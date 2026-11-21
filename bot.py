#!/usr/bin/env python3
"""
Solana Copy-Trade + Pump.fun Snipe Bot
Versione completa aggiornata con:
- Solo filtri deterministici locali (nessuna AI)
- Position sizing dinamico (10% → fino a 20%)
- SL più stretto + stall detection + vendita su picco anomalo
- Fallback WebSocket → RPC + DexScreener
- Report essenziali nel formato richiesto
"""

import asyncio
import base64
import json
import logging
import math
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx
from flask import Flask
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("copybot")

# ---------------------------------------------------------------------------
# CONFIG (variabili d'ambiente su Render / GitHub)
# ---------------------------------------------------------------------------
TOKEN = os.getenv("TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "7935524220"))
RPC_URL = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() != "false"

PCT_PER_TRADE = float(os.getenv("PCT_PER_TRADE", "10"))
MAX_SOL_PER_TRADE = float(os.getenv("MAX_SOL_PER_TRADE", "0"))
MIN_TRADER_SOL = float(os.getenv("MIN_TRADER_SOL", "0.2"))

FILTER_ENABLED = os.getenv("FILTER_ENABLED", "true").lower() != "false"
MIN_LIQ_USD = float(os.getenv("MIN_LIQ_USD", "5000"))
MIN_VOL_H1_USD = float(os.getenv("MIN_VOL_H1_USD", "5000"))
MIN_TXNS_H1 = int(os.getenv("MIN_TXNS_H1", "30"))
MIN_BUY_RATIO = float(os.getenv("MIN_BUY_RATIO", "0.8"))
MAX_PUMP_M5 = float(os.getenv("MAX_PUMP_M5", "150"))
MAX_DUMP_H1 = float(os.getenv("MAX_DUMP_H1", "50"))
MAX_MCAP_USD = float(os.getenv("MAX_MCAP_USD", "3000000"))
RUGCHECK = os.getenv("RUGCHECK", "true").lower() != "false"
SIM_COST_PCT = float(os.getenv("SIM_COST_PCT", "3"))

SLIPPAGE = int(os.getenv("SLIPPAGE", "25"))
PRIORITY_FEE = float(os.getenv("PRIORITY_FEE", "0.0005"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "3"))
STATE_FILE = os.getenv("STATE_FILE", "state.json")

PUMP_PROGRAMS = {
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
}
PUMP_ONLY = os.getenv("PUMP_ONLY", "false").lower() == "true"
WSOL = "So11111111111111111111111111111111111111112"
ADDR_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

KEYPAIR = Keypair.from_base58_string(PRIVATE_KEY) if PRIVATE_KEY else None

GET_TRADERS, GET_BUDGET, GET_DURATION, GET_MODE = range(4)

S: Dict[str, Any] = {
    "active": False,
    "traders": [],
    "budget": 0.0,
    "end": 0.0,
    "positions": {},
    "last": {},
    "trades": 0,
    "skipped": 0,
    "pnl": 0.0,
    "closed": [],
    "mode": "copy",
    "sn": {},
    "by_trader": {},
    "muted": [],
    "tbuy_t": {},
    "tflips": {},
    "bad_creators": [],
    "tsold": {},
    "diag": {},
}

# ---------------------------------------------------------------------------
# Parametri Snipe
# ---------------------------------------------------------------------------
WS_URL = "wss://pumpportal.fun/api/data"
SNIPE_LIVE = os.getenv("SNIPE_LIVE", "false").lower() == "true"
SNIPE_COST_PCT = float(os.getenv("SNIPE_COST_PCT", "8"))
SNIPE_PCT_PER_TRADE = float(os.getenv("SNIPE_PCT_PER_TRADE", "10"))  # base ~10%
SN_MCAP_MIN = float(os.getenv("SN_MCAP_MIN", "4000"))
SN_MCAP_MAX = float(os.getenv("SN_MCAP_MAX", "350000"))
SN_MIN_AGE_MIN = float(os.getenv("SN_MIN_AGE_MIN", "0.3"))  # ~18 secondi
SN_MAX_AGE_MIN = float(os.getenv("SN_MAX_AGE_MIN", "50"))
SN_MIN_BUYERS = int(os.getenv("SN_MIN_BUYERS", "3"))
SN_MIN_BUY_RATIO = float(os.getenv("SN_MIN_BUY_RATIO", "0.5"))
SN_MAX_HOLDER_PCT = float(os.getenv("SN_MAX_HOLDER_PCT", "8"))
SN_MAX_DRAWDOWN_PCT = float(os.getenv("SN_MAX_DRAWDOWN_PCT", "65"))
SN_TP_PCT = float(os.getenv("SN_TP_PCT", "38"))
SN_TP_STRONG_PCT = float(os.getenv("SN_TP_STRONG_PCT", "90"))
SN_STOP_PCT = float(os.getenv("SN_STOP_PCT", "23"))
SN_TRAIL_PCT = float(os.getenv("SN_TRAIL_PCT", "32"))
SN_MAX_HOLD_MIN = float(os.getenv("SN_MAX_HOLD_MIN", "40"))
SN_STALE_MIN = float(os.getenv("SN_STALE_MIN", "2.8"))
SN_EXIT_MCAP_USD = float(os.getenv("SN_EXIT_MCAP_USD", "0"))
SN_MIN_MOMENTUM_PCT = float(os.getenv("SN_MIN_MOMENTUM_PCT", "-90"))
SN_HANDOFF_MCAP_USD = float(os.getenv("SN_HANDOFF_MCAP_USD", "55000"))
SN_MAX_POSITIONS = 10  # massimo posizioni simultanee
SN_MAX_WATCH = int(os.getenv("SN_MAX_WATCH", "500"))
SN_TP_SELL_PCT = float(os.getenv("SN_TP_SELL_PCT", "100"))
SN_BE_ARM_PCT = float(os.getenv("SN_BE_ARM_PCT", "22"))
BE_LOCK_PCT = float(os.getenv("BE_LOCK_PCT", "3"))
SN_FASTDROP_PCT = float(os.getenv("SN_FASTDROP_PCT", "13"))
SN_WHALE_SELL_PCT = float(os.getenv("SN_WHALE_SELL_PCT", "5.5"))
SN_MAX_TOP5_PCT = float(os.getenv("SN_MAX_TOP5_PCT", "24"))
SN_MAX_DEV_PCT = float(os.getenv("SN_MAX_DEV_PCT", "7"))
SN_MAX_TOP1_BUY_PCT = float(os.getenv("SN_MAX_TOP1_BUY_PCT", "20"))
SN_MAX_TOP3_BUY_PCT = float(os.getenv("SN_MAX_TOP3_BUY_PCT", "40"))
SN_MAX_TOP3_TRADES_PCT = float(os.getenv("SN_MAX_TOP3_TRADES_PCT", "45"))
SN_MAX_CLUSTER = int(os.getenv("SN_MAX_CLUSTER", "4"))
SN_MAX_CREATOR_LAUNCHES = int(os.getenv("SN_MAX_CREATOR_LAUNCHES", "3"))
SN_MIN_LIQ_USD = float(os.getenv("SN_MIN_LIQ_USD", "3800"))
SN_MAX_MCAP_LIQ = float(os.getenv("SN_MAX_MCAP_LIQ", "38"))
SN_MAX_PRICE_DIV_PCT = float(os.getenv("SN_MAX_PRICE_DIV_PCT", "25"))
SN_MAX_POS_LIQ_PCT = float(os.getenv("SN_MAX_POS_LIQ_PCT", "3.0"))
SN_MIN_RECENT_BUYS = int(os.getenv("SN_MIN_RECENT_BUYS", "2"))
SN_MAX_SPIKE_PCT = float(os.getenv("SN_MAX_SPIKE_PCT", "220"))
SAFETY_CHECKS = os.getenv("SAFETY_CHECKS", "true").lower() != "false"

SN_MIN_CANDLES = int(os.getenv("SN_MIN_CANDLES", "3"))
SN_MIN_STRUCT_SCORE = float(os.getenv("SN_MIN_STRUCT_SCORE", "20"))
SN_MAX_ENTRY_DRIFT_PCT = float(os.getenv("SN_MAX_ENTRY_DRIFT_PCT", "10"))
SN_STRONG_STRUCT_SCORE = float(os.getenv("SN_STRONG_STRUCT_SCORE", "65"))
SN_STRONG_ORGANIC = float(os.getenv("SN_STRONG_ORGANIC", "68"))

COPY_LOCK = asyncio.Lock()
SN_LOCK = asyncio.Lock()
TASKS: Set = set()

COPY_TP_PCT = float(os.getenv("COPY_TP_PCT", "40"))
COPY_SL_PCT = float(os.getenv("COPY_SL_PCT", "30"))
COPY_BE_ARM_PCT = float(os.getenv("COPY_BE_ARM_PCT", "25"))
COPY_FASTDROP_PCT = float(os.getenv("COPY_FASTDROP_PCT", "15"))
COPY_MAX_HOLD_MIN = float(os.getenv("COPY_MAX_HOLD_MIN", "90"))
COPY_MAX_LAT_SEC = float(os.getenv("COPY_MAX_LAT_SEC", "20"))
COPY_MAX_SLIP_PCT = float(os.getenv("COPY_MAX_SLIP_PCT", "10"))
COPY_MIN_TOKEN_AGE_MIN = float(os.getenv("COPY_MIN_TOKEN_AGE_MIN", "3"))
COPY_MAX_IMPACT_PCT = float(os.getenv("COPY_MAX_IMPACT_PCT", "10"))
COPY_QUICK_FLIP_SEC = float(os.getenv("COPY_QUICK_FLIP_SEC", "30"))
COPY_MAX_FLIPS = int(os.getenv("COPY_MAX_FLIPS", "2"))

CREATORS: Dict[str, List[float]] = {}
TOTAL_SUPPLY = 1_000_000_000
SOL_USD = float(os.getenv("SOL_USD", "150"))
WATCH: Dict[str, Dict] = {}
WS = None
LAST_WS_EVENT = 0.0
WS_STALE_SEC = float(os.getenv("WS_STALE_SEC", "15"))
RPC_DISCOVER_LIMIT = int(os.getenv("RPC_DISCOVER_LIMIT", "40"))
DEX_MAX_RETRIES = int(os.getenv("DEX_MAX_RETRIES", "4"))
DEX_BACKOFF_BASE = float(os.getenv("DEX_BACKOFF_BASE", "2"))
DEX_BACKOFF_MAX = float(os.getenv("DEX_BACKOFF_MAX", "20"))
DEX_BACKOFF = {"delay": DEX_BACKOFF_BASE, "until": 0.0}  # backoff condiviso: un 429 rallenta TUTTE le chiamate DexScreener
DEX_CHUNK_GAP = float(os.getenv("DEX_CHUNK_GAP", "0.5"))  # pausa tra un gruppo di mint e il successivo, per non arrivare a raffica
DEX_CACHE_TTL = float(os.getenv("DEX_CACHE_TTL", "12"))  # secondi: cache per token dei risultati DexScreener
DEX_CACHE: Dict[str, Tuple[float, dict]] = {}
RPC_SEEN_SIGS: Set[str] = set()
RPC_BACKOFF_BASE = float(os.getenv("RPC_BACKOFF_BASE", "2"))  # pausa minima dopo un HTTP 429
RPC_BACKOFF_MAX = float(os.getenv("RPC_BACKOFF_MAX", "20"))  # pausa massima dopo 429 ripetuti
RPC_BACKOFF = {"delay": RPC_BACKOFF_BASE, "until": 0.0, "count429": 0}  # stato condiviso da tutte le chiamate RPC_URL
PENDING_SUB: List[str] = []

# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def save_state() -> None:
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(S, f, default=str)
    except Exception as e:
        log.warning("save_state: %s", e)

def load_state() -> None:
    try:
        with open(STATE_FILE) as f:
            S.update(json.load(f))
    except Exception:
        pass

def exposure() -> float:
    return sum(p["cost"] * p["left"] for p in S["positions"].values())

# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------
DG_DEFAULTS = {
    "polls": 0, "txs": 0, "trades": 0, "small": 0, "sell_nopos": 0,
    "err": "", "last_poll": 0, "sigs": 0, "failed": 0, "null": 0,
    "notrade": 0, "last_nt": "", "muted_skip": 0, "late": 0,
}

def dg(trader: str) -> Dict:
    d = S.setdefault("diag", {}).setdefault(trader, {})
    for k, v in DG_DEFAULTS.items():
        d.setdefault(k, v)
    return d

def short(a: str) -> str:
    return f"{a[:4]}…{a[-4:]}" if a and len(a) > 8 else (a or "?")

async def notify(app, text: str) -> None:
    try:
        await app.bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception as e:
        log.warning("notify: %s", e)

# ---------------------------------------------------------------------------
# RPC
# ---------------------------------------------------------------------------
async def rpc(client: httpx.AsyncClient, method: str, params: list) -> Any:
    """Chiama RPC_URL. In caso di HTTP 429 applica un backoff esponenziale (2s -> 20s) condiviso
    da tutti i chiamanti, così anche il polling rallenta invece di martellare l'RPC a raffica."""
    wait = RPC_BACKOFF["until"] - time.time()
    if wait > 0:
        await asyncio.sleep(wait)
    attempt = 0
    while True:
        r = await client.post(
            RPC_URL,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=25,
        )
        if r.status_code == 429:
            attempt += 1
            RPC_BACKOFF["count429"] += 1
            retry_after = r.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after else RPC_BACKOFF["delay"]
            except ValueError:
                delay = RPC_BACKOFF["delay"]
            delay = min(max(delay, RPC_BACKOFF_BASE), RPC_BACKOFF_MAX)
            RPC_BACKOFF["delay"] = min(delay * 2, RPC_BACKOFF_MAX)
            RPC_BACKOFF["until"] = time.time() + delay
            log.warning("RPC 429 (tentativo %d): pausa %.1fs", attempt, delay)
            if attempt >= 6:
                r.raise_for_status()
            await asyncio.sleep(delay)
            continue
        r.raise_for_status()
        RPC_BACKOFF["delay"] = max(RPC_BACKOFF_BASE, RPC_BACKOFF["delay"] * 0.7)
        j = r.json()
        if "error" in j:
            raise RuntimeError(j["error"])
        return j["result"]


def rpc_poll_interval(base: float) -> float:
    """Intervallo di polling: se siamo in backoff per un 429 su RPC_URL, allunga l'attesa invece del valore base."""
    if time.time() < RPC_BACKOFF["until"]:
        return max(base, RPC_BACKOFF["delay"])
    return base

async def wallet_balance(client: httpx.AsyncClient) -> Optional[float]:
    if not KEYPAIR:
        return None
    res = await rpc(client, "getBalance", [str(KEYPAIR.pubkey()), {"commitment": "confirmed"}])
    return res["value"] / 1e9

# ---------------------------------------------------------------------------
# Parsing trade
# ---------------------------------------------------------------------------
def parse_trades(tx: dict, trader: str) -> List[dict]:
    meta = tx.get("meta")
    if not meta or meta.get("err"):
        return []
    keys = [k["pubkey"] if isinstance(k, dict) else k for k in tx["transaction"]["message"]["accountKeys"]]
    if (PUMP_ONLY and not PUMP_PROGRAMS.intersection(keys)) or trader not in keys:
        return []
    i = keys.index(trader)
    sol_delta = (meta["postBalances"][i] - meta["preBalances"][i]) / 1e9

    def bals(lst):
        out = {}
        for b in lst or []:
            if b.get("owner") == trader and b["mint"] != WSOL:
                out[b["mint"]] = out.get(b["mint"], 0.0) + float(
                    b["uiTokenAmount"].get("uiAmountString") or 0
                )
        return out

    pre = bals(meta.get("preTokenBalances"))
    post = bals(meta.get("postTokenBalances"))
    trades = []
    for mint in set(pre) | set(post):
        d = post.get(mint, 0.0) - pre.get(mint, 0.0)
        if d > 0 and sol_delta < 0:
            trades.append({
                "side": "buy",
                "mint": mint,
                "sol": -sol_delta,
                "bt": tx.get("blockTime"),
                "price_sol": (-sol_delta) / d if d > 0 else None,
            })
        elif d < 0 and sol_delta > 0.01:
            p = pre.get(mint, 0.0)
            pct = min(100.0, (-d / p) * 100) if p > 0 else 100.0
            trades.append({"side": "sell", "mint": mint, "pct": pct, "bt": tx.get("blockTime")})
    return trades

# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------
async def fetch_metrics(client: httpx.AsyncClient, mint: str) -> Optional[dict]:
    r = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}", timeout=15)
    r.raise_for_status()
    pairs = [p for p in (r.json().get("pairs") or []) if p.get("chainId") == "solana"]
    if not pairs:
        return None
    p = max(pairs, key=lambda x: (x.get("liquidity") or {}).get("usd") or 0)
    tx5 = (p.get("txns") or {}).get("m5") or {}
    tx1 = (p.get("txns") or {}).get("h1") or {}
    pc = p.get("priceChange") or {}
    vol = p.get("volume") or {}
    created = p.get("pairCreatedAt")
    return {
        "symbol": (p.get("baseToken") or {}).get("symbol"),
        "price_usd": float(p.get("priceUsd") or 0),
        "price_native": float(p.get("priceNative") or 0),
        "dex": p.get("dexId"),
        "liq_usd": (p.get("liquidity") or {}).get("usd") or 0,
        "mcap_usd": p.get("marketCap") or p.get("fdv") or 0,
        "vol_m5": vol.get("m5") or 0,
        "vol_h1": vol.get("h1") or 0,
        "buys_m5": tx5.get("buys") or 0,
        "sells_m5": tx5.get("sells") or 0,
        "buys_h1": tx1.get("buys") or 0,
        "sells_h1": tx1.get("sells") or 0,
        "chg_m5": pc.get("m5") or 0,
        "chg_h1": pc.get("h1") or 0,
        "age_min": round((time.time() * 1000 - created) / 60000, 1) if created else None,
    }

def rule_check(m: dict) -> List[str]:
    why = []
    if m["liq_usd"] < MIN_LIQ_USD:
        why.append(f"liquidità bassa (${m['liq_usd']:.0f})")
    if m["vol_h1"] < MIN_VOL_H1_USD:
        why.append(f"volume 1h basso (${m['vol_h1']:.0f})")
    if m["buys_h1"] + m["sells_h1"] < MIN_TXNS_H1:
        why.append("poche transazioni")
    if m["buys_m5"] + m["sells_m5"] >= 5 and m["buys_m5"] < MIN_BUY_RATIO * m["sells_m5"]:
        why.append("più vendite che acquisti negli ultimi 5 min")
    if m["chg_m5"] > MAX_PUMP_M5:
        why.append(f"già salito {m['chg_m5']:.0f}% in 5 min")
    if m["chg_h1"] < -MAX_DUMP_H1:
        why.append(f"in calo {m['chg_h1']:.0f}% nell'ultima ora")
    if MAX_MCAP_USD > 0 and m["mcap_usd"] > MAX_MCAP_USD:
        why.append(f"market cap alta (${m['mcap_usd']:.0f})")
    return why

async def rugcheck_dangers(client: httpx.AsyncClient, mint: str) -> List[str]:
    try:
        r = await client.get(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary", timeout=15)
        if r.status_code != 200:
            return []
        return [x.get("name", "?") for x in r.json().get("risks", []) if x.get("level") == "danger"]
    except Exception as e:
        log.warning("rugcheck: %s", e)
        return []

# ---------------------------------------------------------------------------
# Punteggio acquirenti locale (usato solo per il sizing, non blocca l'ingresso)
# ---------------------------------------------------------------------------
def organic_score(w) -> float:
    """Qualità degli acquirenti 0-100 calcolata in locale dai dati on-chain."""
    if not w["stream_trades"]:
        return 50.0
    bq = buyer_quality(w)
    score = 50.0
    score += min(25.0, max(0, bq["acquirenti_unici"] - SN_MIN_BUYERS) * 1.5)
    top1, top3 = bq["quota_volume_top1_pct"], bq["quota_volume_top3_pct"]
    if top1 <= 15:
        score += 10
    elif top1 > SN_MAX_TOP1_BUY_PCT:
        score -= 15
    if top3 <= 30:
        score += 5
    cl = bq["cluster_sincronizzati_max"]
    if cl <= 1:
        score += 5
    elif cl >= SN_MAX_CLUSTER:
        score -= 15
    if bq["quota_acquisti_primi_10s_pct"] > 60:
        score -= 15
    if bq["wallet_che_comprano_e_vendono_pct"] > 30:
        score -= 20
    if bq["nuovi_acquirenti_ultimo_minuto"] >= 3:
        score += 5
    return float(max(0.0, min(100.0, score)))

# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------
def spawn(coro):
    t = asyncio.create_task(coro)
    TASKS.add(t)
    t.add_done_callback(TASKS.discard)
    return t

async def safe(coro, label: str):
    try:
        await coro
    except Exception as e:
        log.warning("%s: %s", label, e)

# ---------------------------------------------------------------------------
# Dynamic position sizing (10% cauti → fino a 20% setup forti)
# ---------------------------------------------------------------------------
def calc_position_size(
    base_pct: float,
    liq_usd: float,
    organic_score: float = 50.0,
    struct_score: float = 50.0,
    impact_pct: float = 0.0,
    is_strong: bool = False,
) -> float:
    """Sizing dinamico: ~10% su setup cauti, fino a 20% su setup forti con alta probabilità."""
    pct = base_pct  # base ~10%
    if is_strong or (organic_score >= SN_STRONG_ORGANIC and struct_score >= SN_STRONG_STRUCT_SCORE and impact_pct < 5):
        pct = min(20.0, base_pct * 2.0)
    elif organic_score >= 62 and struct_score >= 50:
        pct = min(14.0, base_pct * 1.4)
    # modulazione liquidità e impatto
    if liq_usd > 0:
        max_from_liq = (liq_usd * SN_MAX_POS_LIQ_PCT / 100) / max(SOL_USD, 1)
        budget = max(S.get("budget", 0.001), 0.001)
        pct = min(pct, max_from_liq / budget * 100)
    if impact_pct > 7:
        pct = min(pct, 7.5)
    return round(max(4.0, min(20.0, pct)), 1)

# ---------------------------------------------------------------------------
# Order execution
# ---------------------------------------------------------------------------
async def do_trade(client: httpx.AsyncClient, action: str, mint: str, amount) -> Tuple[bool, str]:
    if DRY_RUN:
        return True, "SIMULAZIONE (nessun ordine inviato)"
    if not KEYPAIR:
        return False, "PRIVATE_KEY mancante"
    try:
        data = {
            "publicKey": str(KEYPAIR.pubkey()),
            "action": action,
            "mint": mint,
            "amount": amount,
            "denominatedInSol": "true" if action == "buy" else "false",
            "slippage": SLIPPAGE,
            "priorityFee": PRIORITY_FEE,
            "pool": "auto",
        }
        r = await client.post("https://pumpportal.fun/api/trade-local", data=data, timeout=25)
        if r.status_code != 200:
            return False, f"PumpPortal {r.status_code}: {r.text[:150]}"
        tx = VersionedTransaction.from_bytes(r.content)
        signed = VersionedTransaction(tx.message, [KEYPAIR])
        raw = base64.b64encode(bytes(signed)).decode()
        sig = await rpc(client, "sendTransaction", [raw, {"encoding": "base64", "skipPreflight": True, "maxRetries": 3}])
        for _ in range(20):
            await asyncio.sleep(1.5)
            st = (await rpc(client, "getSignatureStatuses", [[sig]]))["value"][0]
            if st:
                if st.get("err"):
                    return False, f"Transazione fallita: https://solscan.io/tx/{sig}"
                if st.get("confirmationStatus") in ("confirmed", "finalized"):
                    return True, f"https://solscan.io/tx/{sig}"
        return False, f"Non confermata: https://solscan.io/tx/{sig}"
    except Exception as e:
        return False, f"Errore: {e}"

# ---------------------------------------------------------------------------
# PnL e report
# ---------------------------------------------------------------------------
def entry_mid(pos: dict, cost_pct: float) -> Optional[float]:
    if not pos.get("qty") or not pos.get("cost"):
        return None
    return (pos["cost"] / pos["qty"]) / (1 + cost_pct / 100)

def settle(mint: str, pos: dict, pct: float, price_now: float, cost_pct: float = None, mcap_mult: float = None):
    cp = SIM_COST_PCT if cost_pct is None else cost_pct
    mult = TOTAL_SUPPLY if mcap_mult is None else mcap_mult
    f = pct / 100
    cost_sold = pos["cost"] * pos["left"] * f
    if not pos.get("qty") or not price_now:
        return None, cost_sold
    proceeds = pos["qty"] * pos["left"] * f * price_now * (1 - cp / 100)
    pnl = proceeds - cost_sold
    S["pnl"] = S.get("pnl", 0.0) + pnl
    em = entry_mid(pos, cp) or 0.0
    S.setdefault("closed", []).append({
        "sym": pos.get("sym") or mint[:6],
        "pnl": round(pnl, 5),
        "cost": round(cost_sold, 5),
        "in": round(em * mult),
        "out": round(price_now * mult),
        "pct": round((price_now / em - 1) * 100, 1) if em else 0.0,
        "reason": pos.get("exit_reason", ""),
    })
    S["closed"] = S["closed"][-60:]
    return pnl, cost_sold

def fmt_closed(c: dict) -> str:
    if c.get("in"):
        return (
            f"{c['sym']}: Entrato {c['in']:,.0f}, uscito {c['out']:,.0f} "
            f"({c.get('pct', 0):+.0f}%, {c['pnl']:+.4f} SOL)"
        )
    return f"{c['sym']}: {c['pnl']:+.4f} SOL"

def pnl_summary() -> str:
    cl = S.get("closed", [])
    txt = f"Risultato stimato delle vendite: {S.get('pnl', 0.0):+.4f} SOL su {len(cl)} chiusure"
    if cl:
        txt += "\n" + "\n".join("• " + fmt_closed(c) for c in cl[-25:])
    return txt

def mark_bad_creator(c: str) -> None:
    if not c:
        return
    bc = S.setdefault("bad_creators", [])
    if c not in bc:
        bc.append(c)
        del bc[:-400]

def pnl_text(pnl: Optional[float], cost: float) -> str:
    if pnl is None:
        return "\nRisultato: non calcolabile"
    pct = f" ({pnl / cost * 100:+.0f}%)" if cost > 0 else ""
    return f"\nRisultato stimato: {pnl:+.4f} SOL{pct}"

# ---------------------------------------------------------------------------
# Copy trading core
# ---------------------------------------------------------------------------
async def evaluate_buy(client, mint, trader_sol, exec_price=None, trader=None, latency=None, app=None):
    try:
        m = await fetch_metrics(client, mint)
        if m is None:
            await asyncio.sleep(4)
            m = await fetch_metrics(client, mint)
    except Exception as e:
        return False, f"errore dati di mercato: {e}", None
    if m is None:
        return False, "nessun dato di mercato ancora", None

    if FILTER_ENABLED:
        why = rule_check(m)
        if why:
            return False, "; ".join(why), m
        if m.get("age_min") is not None and m["age_min"] < COPY_MIN_TOKEN_AGE_MIN:
            return False, f"token troppo giovane ({m['age_min']} min)", m
        if m["liq_usd"] > 0 and m["mcap_usd"] / m["liq_usd"] > SN_MAX_MCAP_LIQ:
            return False, "prezzo e liquidità incoerenti", m

    impact = trader_sol * SOL_USD / m["liq_usd"] * 100 if m["liq_usd"] > 0 else None
    if FILTER_ENABLED and impact is not None and impact > COPY_MAX_IMPACT_PCT:
        return False, "trade del trader troppo grande rispetto alla liquidità", m

    slip = None
    if exec_price and m.get("price_native"):
        slip = (m["price_native"] / exec_price - 1) * 100
        if slip > COPY_MAX_SLIP_PCT:
            return False, f"prezzo già salito del {slip:.0f}%", m

    if RUGCHECK:
        dangers = await rugcheck_dangers(client, mint)
        if dangers:
            return False, "rischi RugCheck: " + ", ".join(dangers[:3]), m

    tag = f"{m['symbol']} liq ${m['liq_usd']:.0f}, vol1h ${m['vol_h1']:.0f}"
    return True, tag, m

async def copy_sell(app, client, mint, pct, reason, price_now=None):
    pos = S["positions"].get(mint)
    if not pos:
        return
    price_now = price_now or (await get_price(client, mint)) or pos.get("last_price")
    ok, info = await do_trade(client, "sell", mint, f"{pct}%")
    extra = ""
    if ok:
        pnl, cs = settle(mint, pos, pct, price_now)
        extra = pnl_text(pnl, cs)
        pos["left"] *= 1 - pct / 100
        if pct >= 100 or pos["left"] < 0.02:
            del S["positions"][mint]
        S["trades"] += 1
        save_state()
    await notify(app, f"{'🟢' if ok else '🔴'} SELL {pct}% {pos.get('sym') or mint[:6]} — {reason}\nhttps://pump.fun/coin/{mint}\n{info}{extra}")

async def get_price(client, mint):
    try:
        m = await fetch_metrics(client, mint)
        if m and m.get("price_usd"):
            return m["price_usd"]
    except Exception as e:
        log.warning("get_price: %s", e)
    return None

async def copy_trade(app, client, trader, t):
    mint, who = t["mint"], short(trader)
    link = f"https://pump.fun/coin/{mint}"
    now = time.time()
    if t["side"] == "buy":
        if t["sol"] < MIN_TRADER_SOL:
            dg(trader)["small"] += 1
            return
        if trader in S.get("muted", []):
            dg(trader)["muted_skip"] += 1
            return
        lat = (now - t["bt"]) if t.get("bt") else None
        if lat is not None and lat > COPY_MAX_LAT_SEC:
            S["skipped"] += 1
            dg(trader)["late"] += 1
            await notify(app, f"SKIP (trade di {who})\n{link}\nMotivo: arrivo in ritardo di {lat:.0f} s")
            return

        amount = S["budget"] * PCT_PER_TRADE / 100
        if MAX_SOL_PER_TRADE > 0:
            amount = min(amount, MAX_SOL_PER_TRADE)
        amount = round(min(amount, S["budget"] - exposure()), 4)
        if amount < 0.005:
            await notify(app, f"⚠️ {who} ha comprato {link}\nBudget esaurito")
            return

        go, verdict, m = await evaluate_buy(
            client, mint, t["sol"], t.get("price_sol"),
            trader=trader, latency=lat, app=app
        )
        if not go:
            S["skipped"] += 1
            save_state()
            await notify(app, f"SKIP (trade di {who}, {t['sol']:.2f} SOL)\n{link}\nMotivo: {verdict}")
            return

        async with COPY_LOCK:
            amount = round(min(amount, S["budget"] - exposure()), 4)
            if amount < 0.005:
                return
            price = (m or {}).get("price_usd") or await get_price(client, mint)
            entry = price * (1 + SIM_COST_PCT / 100) if price else None
            qty_new = amount / entry if entry else 0.0
            ok, info = await do_trade(client, "buy", mint, amount)
            if ok:
                p = S["positions"].get(mint)
                if p:
                    p["qty"] = p.get("qty", 0.0) * p["left"] + qty_new
                    p["cost"] = p["cost"] * p["left"] + amount
                    p["left"] = 1.0
                    p["last_price"] = price or p.get("last_price")
                    p.setdefault("by", {})
                    p["by"][trader] = p["by"].get(trader, 0.0) + amount
                else:
                    S["positions"][mint] = {
                        "cost": amount, "left": 1.0, "qty": qty_new,
                        "t": time.time(), "last_price": price,
                        "sym": (m or {}).get("symbol"), "by": {trader: amount}
                    }
                S["trades"] += 1
                save_state()
            await notify(app, f"{'🟢' if ok else '🔴'} BUY {amount} SOL (copia di {who})\n{link}\n✓ {verdict}\n{info}")
    else:
        pos = S["positions"].get(mint)
        if not pos:
            dg(trader)["sell_nopos"] += 1
            return
        pct = 100 if t["pct"] >= 95 else max(1, int(round(t["pct"])))
        await copy_sell(app, client, mint, pct, f"vende {who}")

async def copy_guard(app, client):
    if not S["positions"]:
        return
    data = await dex_fetch_many(client, list(S["positions"]))
    now = time.time()
    for mint in list(S["positions"]):
        pos = S["positions"].get(mint)
        p = data.get(mint)
        if not pos or not p:
            continue
        price = float(p.get("priceUsd") or 0)
        if price <= 0:
            continue
        pos["last_price"] = price
        pos.setdefault("t", now)
        entry = entry_mid(pos, SIM_COST_PCT)
        if not entry:
            continue
        gain = (price / entry - 1) * 100
        if gain >= COPY_BE_ARM_PCT:
            pos["be"] = True
        if gain <= -COPY_SL_PCT:
            pos["sl_t"] = pos.get("sl_t") or now
        else:
            pos["sl_t"] = None
        recent = [x for ts, x in pos.get("px", []) if now - ts <= 30]
        drop = (recent[-1] / max(recent) - 1) * 100 if recent else 0
        reason = None
        if drop <= -COPY_FASTDROP_PCT:
            reason = f"calo improvviso ({drop:.0f}% in 30 s)"
        elif gain >= COPY_TP_PCT:
            reason = f"take profit (+{gain:.0f}%)"
        elif gain <= -COPY_SL_PCT and (gain <= -60 or now - pos.get("sl_t", now) >= 4):
            reason = f"stop loss ({gain:.0f}%)"
        elif pos.get("be") and gain <= BE_LOCK_PCT:
            reason = "protezione: tornato al prezzo d'ingresso"
        elif now - pos["t"] > COPY_MAX_HOLD_MIN * 60:
            reason = "tempo massimo di permanenza"
        if reason:
            await copy_sell(app, client, mint, 100, reason, price)

async def close_all(app, client):
    if S.get("mode") == "snipe":
        async with SN_LOCK:
            await snipe_close_all(app, client)
        return
    for mint in list(S["positions"]):
        await copy_sell(app, client, mint, 100, "chiusura sessione")
    save_state()

# ---------------------------------------------------------------------------
# Monitor copy
# ---------------------------------------------------------------------------
async def get_tx(client, sig):
    for ver in (1, 0):
        try:
            return await rpc(client, "getTransaction", [sig, {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": ver,
                "commitment": "confirmed",
            }])
        except RuntimeError as e:
            if ver == 0 or "version" not in str(e).lower():
                raise
    return None

async def check_trader(app, client, trader):
    d = dg(trader)
    d["polls"] += 1
    d["last_poll"] = int(time.time())
    until = S["last"].get(trader)
    sigs, before = [], None
    for _ in range(5):
        params = {"limit": 1000 if until else 5, "commitment": "confirmed"}
        if until:
            params["until"] = until
        if before:
            params["before"] = before
        page = await rpc(client, "getSignaturesForAddress", [trader, params])
        sigs += page
        if len(page) < params["limit"] or not until:
            break
        before = page[-1]["signature"]
    d["err"] = ""
    d["sigs"] += len(sigs)
    for s in reversed(sigs):
        sig = s["signature"]
        if s.get("err"):
            d["failed"] += 1
            S["last"][trader] = sig
            continue
        try:
            tx = None
            for _ in range(3):
                tx = await get_tx(client, sig)
                if tx:
                    break
                await asyncio.sleep(1)
            if not tx:
                d["null"] += 1
            else:
                d["txs"] += 1
                found = parse_trades(tx, trader)
                if not found:
                    d["notrade"] += 1
                for t in found:
                    d["trades"] += 1
                    if t["side"] == "buy":
                        spawn(safe(copy_trade(app, client, trader, t), "copy_trade"))
                    else:
                        await copy_trade(app, client, trader, t)
        except Exception as e:
            log.warning("tx %s: %s", sig[:8], e)
            d["err"] = str(e)[:150]
        S["last"][trader] = sig
    save_state()

async def monitor(app):
    async with httpx.AsyncClient() as client:
        try:
            for t in S["traders"]:
                if not S["last"].get(t):
                    sigs = await rpc(client, "getSignaturesForAddress", [t, {"limit": 1, "commitment": "confirmed"}])
                    S["last"][t] = sigs[0]["signature"] if sigs else None
            save_state()
            await update_sol_price(client)
            last_guard, last_sol = 0.0, time.time()
            while S["active"]:
                if time.time() >= S["end"]:
                    S["active"] = False
                    await notify(app, "📉 Durata terminata. Chiudo le posizioni aperte.")
                    await close_all(app, client)
                    await notify(app, "Sessione chiusa.\n" + pnl_summary())
                    save_state()
                    return
                for t in S["traders"]:
                    try:
                        await check_trader(app, client, t)
                    except Exception as e:
                        log.warning("check_trader %s: %s", short(t), e)
                        dg(t)["err"] = str(e)[:150]
                now = time.time()
                if now - last_sol > 300:
                    await update_sol_price(client)
                    last_sol = now
                if now - last_guard > 10:
                    last_guard = now
                    try:
                        await copy_guard(app, client)
                    except Exception as e:
                        log.warning("copy_guard: %s", e)
                await asyncio.sleep(rpc_poll_interval(POLL_SECONDS))
        except Exception as e:
            log.exception("monitor")
            await notify(app, f"🔴 Errore nel monitor: {e}")

# ---------------------------------------------------------------------------
# Snipe helpers
# ---------------------------------------------------------------------------
def sn():
    d = S.setdefault("sn", {})
    for k, v in {
        "seen": 0, "entered": 0, "events": 0, "last_event": 0,
        "ws_err": "", "reasons": {}, "types": {}, "subs": 0, "send_err": "",
        "msgs": 0, "last_msg": "", "dex_polls": 0, "dex_hits": 0, "incoherent": 0,
        "fallback_on": False, "fallback_cycles": 0, "dex_429": 0,
    }.items():
        d.setdefault(k, v)
    return d

async def ws_send(payload):
    if WS is None:
        sn()["send_err"] = "connessione non pronta"
        return
    try:
        await WS.send(json.dumps(payload))
        if payload.get("method") == "subscribeTokenTrade":
            sn()["subs"] += len(payload.get("keys", []))
    except Exception as e:
        log.warning("ws_send: %s", e)
        sn()["send_err"] = str(e)[:150]

async def flush_subs():
    global PENDING_SUB
    while PENDING_SUB:
        batch, PENDING_SUB = PENDING_SUB[:50], PENDING_SUB[50:]
        await ws_send({"method": "subscribeTokenTrade", "keys": batch})

def price_from(ev):
    v, t = ev.get("vSolInBondingCurve"), ev.get("vTokensInBondingCurve")
    try:
        return float(v) / float(t) if v and t else None
    except Exception:
        return None

def new_watch(ev=None):
    ev = ev or {}
    mc = float(ev.get("marketCapSol") or 0)
    now = time.time()
    pr = price_from(ev)
    return {
        "name": ev.get("name"), "symbol": ev.get("symbol") or "?",
        "creator": ev.get("traderPublicKey"),
        "t0": now, "last_t": now, "mcap_sol": mc, "peak_sol": mc,
        "last_price": pr, "buyers": set(), "sellers": set(), "first_buy": {},
        "buys": 0, "sells": 0, "vol": 0.0, "holders": {},
        "creator_sold": False, "blocked": False, "evaluating": False,
        "why": "nessuno scambio dopo la creazione",
        "stream_trades": 0, "dex_t": 0.0, "dex_buys": 0, "hist": [(now, mc)],
        "src": "stream", "px": [(now, pr)] if pr else [], "outl": 0,
        "buy_vol": {}, "tcount": {}, "buys_log": [], "tlog": [],
        "vsol": float(ev.get("vSolInBondingCurve") or 0), "dump_flag": False,
        "next_chk": 0.0, "xchk": 0, "liq_usd": 0.0, "dex_id": None,
        "candles": [], "cache": {},
    }

def cached(w, key, ttl, fn):
    now = time.time()
    c = w["cache"].get(key)
    if c and now - c[0] < ttl:
        return c[1]
    v = fn(w)
    w["cache"][key] = (now, v)
    return v

def push_candle(w):
    now = time.time()
    b = int(now // 15) * 15
    v = w["mcap_sol"]
    if v <= 0:
        return
    cs = w["candles"]
    if cs and cs[-1][0] == b:
        c = cs[-1]
        c[2], c[3], c[4] = max(c[2], v), min(c[3], v), v
    else:
        cs.append([b, v, v, v, v])
        if len(cs) > 200:
            del cs[:-200]

def zigzag(vals, thr=0.12):
    if len(vals) < 2:
        return []
    piv, direction = [], 0
    ext_i, ext_v = 0, vals[0]
    for i in range(1, len(vals)):
        v = vals[i]
        if direction == 0:
            if v >= ext_v * (1 + thr):
                piv.append((0, vals[0], "L"))
                direction, ext_i, ext_v = 1, i, v
            elif v <= ext_v * (1 - thr):
                piv.append((0, vals[0], "H"))
                direction, ext_i, ext_v = -1, i, v
        elif direction == 1:
            if v > ext_v:
                ext_i, ext_v = i, v
            elif v <= ext_v * (1 - thr):
                piv.append((ext_i, ext_v, "H"))
                direction, ext_i, ext_v = -1, i, v
        else:
            if v < ext_v:
                ext_i, ext_v = i, v
            elif v >= ext_v * (1 + thr):
                piv.append((ext_i, ext_v, "L"))
                direction, ext_i, ext_v = 1, i, v
    piv.append((ext_i, ext_v, "H" if direction >= 0 else "L"))
    return piv

def structure_features(w):
    cs = w["candles"]
    f = {"candele_15s": len(cs)}
    if len(cs) < SN_MIN_CANDLES:
        f["insufficienti"] = True
        return f
    k = SOL_USD
    closes = [c[4] * k for c in cs]
    highs = [c[2] * k for c in cs]
    lows = [c[3] * k for c in cs]
    cur, peak, floor = closes[-1], max(highs), max(1e-9, min(lows))
    since_peak = cs[-1][0] - cs[highs.index(peak)][0]
    piv = zigzag(closes, 0.12)
    pl = [p for p in piv if p[2] == "L"]
    ph = [p for p in piv if p[2] == "H"]
    lows_rising = len(pl) >= 2 and all(pl[i][1] >= pl[i - 1][1] * 0.97 for i in range(1, len(pl)))
    highs_rising = len(ph) >= 2 and all(ph[i][1] >= ph[i - 1][1] * 0.97 for i in range(1, len(ph)))
    up_legs = healthy = 0
    max_pb = 0.0
    for i in range(1, len(piv)):
        a, b = piv[i - 1], piv[i]
        if a[2] == "L" and b[2] == "H" and b[1] >= a[1] * 1.25:
            up_legs += 1
        if a[2] == "H" and b[2] == "L":
            depth = (a[1] - b[1]) / a[1] * 100
            max_pb = max(max_pb, depth)
            nxt = next((p[1] for p in piv[i + 1:] if p[2] == "H"), None)
            if 8 <= depth <= 50 and nxt and nxt > a[1]:
                healthy += 1
    total = math.log(cur / floor) if cur > floor else 0.0
    best60 = max([math.log(closes[i + 4] / closes[i]) for i in range(len(closes) - 4)
                  if closes[i] > 0 and closes[i + 4] > closes[i]], default=0.0)
    best15 = max([math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))
                  if closes[i - 1] > 0 and closes[i] > closes[i - 1]], default=0.0)
    share60 = best60 / total if total > 0.05 else 0.0
    share15 = best15 / total if total > 0.05 else 0.0
    vertical = cur / floor >= 3 and (share60 >= 0.7 or share15 >= 0.55)
    spent = peak / floor >= 6 and since_peak >= 120 and cur <= peak * 0.8
    last = closes[-12:]
    mean = sum(last) / len(last)
    rng = (max(last) - min(last)) / mean if mean > 0 else 0
    net = abs(last[-1] / last[0] - 1) if last[0] > 0 else 0
    diffs = [last[i] - last[i - 1] for i in range(1, len(last))]
    changes = sum(1 for i in range(1, len(diffs)) if diffs[i] * diffs[i - 1] < 0)
    dirty = len(last) >= 8 and rng >= 0.25 and net <= 0.10 and changes >= 6
    score = (50 + (15 if lows_rising else 0) + (10 if highs_rising else 0) + 10 * min(2, healthy)
             + 5 * min(2, max(0, up_legs - 1)) - (35 if vertical else 0) - (35 if spent else 0) - (25 if dirty else 0))
    f.update({
        "mcap_attuale": round(cur), "picco": round(peak), "minimo": round(floor),
        "x_dal_minimo": round(cur / floor, 2), "calo_dal_picco_pct": round((peak - cur) / peak * 100, 1),
        "minimi_crescenti": lows_rising, "massimi_crescenti": highs_rising,
        "gradini_di_salita": up_legs, "ritracciamenti_sani": healthy,
        "ritracciamento_massimo_pct": round(max_pb, 1),
        "quota_salita_nella_candela_migliore_60s": round(share60, 2),
        "verticale": vertical, "spike_sparato": spent, "laterale_sporco": dirty,
        "punteggio": max(0, min(100, round(score))),
    })
    return f

def buyer_quality(w):
    if not w["stream_trades"]:
        return {"disponibile": False, "acquisti_5min": w["buys"], "vendite_5min": w["sells"],
                "nota": "dati sui singoli acquirenti assenti (fonte DexScreener)"}
    now = time.time()
    bl = w["buys_log"]
    vols = [x for _, _, x in bl]
    tot, n = sum(vols), len(vols)
    mean = tot / n if n else 0.0
    disp = ((sum((v - mean) ** 2 for v in vols) / n) ** 0.5 / mean) if n and mean > 0 else 0.0
    bv = sorted(w["buy_vol"].values(), reverse=True)
    early = sum(x for t, _, x in bl if t - w["t0"] <= 10)
    fb = w["first_buy"]
    return {
        "disponibile": True, "acquirenti_unici": len(w["buyers"]),
        "nuovi_acquirenti_ultimo_minuto": sum(1 for t in fb.values() if now - t <= 60),
        "nuovi_acquirenti_ultimi_5_min": sum(1 for t in fb.values() if now - t <= 300),
        "quota_volume_top1_pct": round(bv[0] / tot * 100, 1) if tot and bv else 0,
        "quota_volume_top3_pct": round(sum(bv[:3]) / tot * 100, 1) if tot else 0,
        "cluster_sincronizzati_max": cluster_size(w),
        "quota_acquisti_primi_10s_pct": round(early / tot * 100, 1) if tot else 0,
        "wallet_che_comprano_e_vendono_pct": round(len(w["buyers"] & w["sellers"]) / max(1, len(w["buyers"])) * 100, 1),
        "dispersione_importi": round(disp, 2), "importo_medio_sol": round(mean, 3),
        "scambi_ultimo_minuto": sum(1 for t, _ in w["tlog"] if now - t <= 60),
    }

def push_price(w, price):
    now = time.time()
    px = w.setdefault("px", [])
    recent = sorted(p for _, p in px[-5:])
    if len(recent) >= 3:
        med = recent[len(recent) // 2]
        if med > 0 and abs(price / med - 1) > 0.6:
            w["outl"] = w.get("outl", 0) + 1
            if w["outl"] < 2:
                return False
    w["outl"] = 0
    px.append((now, price))
    w["px"] = [(t, p) for t, p in px if now - t <= 120][-60:]
    return True

def robust_price(w):
    now = time.time()
    vals = [p for t, p in w.get("px", []) if now - t <= 20][-5:]
    if not vals:
        return w.get("last_price")
    return sorted(vals)[len(vals) // 2]

def cluster_size(w):
    ev = w["buys_log"]
    best = 0
    for t, tr, sol in ev:
        grp = {tr2 for t2, tr2, s2 in ev if abs(t2 - t) <= 2 and abs(s2 - sol) <= max(0.005, sol * 0.01)}
        best = max(best, len(grp))
    return best

def concentration_issue(w):
    hold = sorted(w["holders"].items(), key=lambda kv: -kv[1])
    top1 = (hold[0][1] if hold else 0) / TOTAL_SUPPLY * 100
    top5 = sum(v for _, v in hold[:5]) / TOTAL_SUPPLY * 100
    dev = w["holders"].get(w["creator"], 0) / TOTAL_SUPPLY * 100
    if top1 > SN_MAX_HOLDER_PCT:
        return f"un wallet detiene il {top1:.0f}% della supply"
    if top5 > SN_MAX_TOP5_PCT:
        return f"i primi 5 wallet detengono il {top5:.0f}%"
    if dev > SN_MAX_DEV_PCT:
        return f"il creatore detiene il {dev:.0f}%"
    bv = sorted(w["buy_vol"].values(), reverse=True)
    tot = sum(bv)
    if tot > 0:
        if bv[0] / tot * 100 > SN_MAX_TOP1_BUY_PCT:
            return "acquisti dominati da un singolo wallet"
        if sum(bv[:3]) / tot * 100 > SN_MAX_TOP3_BUY_PCT:
            return "acquisti concentrati su 3 wallet"
    tc = sorted(w["tcount"].values(), reverse=True)
    n = sum(tc)
    if n >= 20 and sum(tc[:3]) / n * 100 > SN_MAX_TOP3_TRADES_PCT:
        return "pochi wallet fanno quasi tutti gli scambi (wash trading)"
    if cluster_size(w) >= SN_MAX_CLUSTER:
        return "acquisti sincronizzati di più wallet (bundle)"
    return None

def recent_pressure(w):
    now = time.time()
    b = sum(1 for t, x in w["tlog"] if now - t <= 60 and x == "buy")
    s_ = sum(1 for t, x in w["tlog"] if now - t <= 60 and x == "sell")
    return b, s_

async def check_authorities(client, mint):
    try:
        res = await rpc(client, "getAccountInfo", [mint, {"encoding": "jsonParsed", "commitment": "confirmed"}])
        v = (res or {}).get("value")
        if not v:
            return False, "mint non trovato"
        info = ((v.get("data") or {}).get("parsed") or {}).get("info") or {}
        if info.get("mintAuthority"):
            return False, "mint authority non revocata"
        if info.get("freezeAuthority"):
            return False, "freeze authority non revocata"
        bad = {"transferFeeConfig", "permanentDelegate", "transferHook", "nonTransferable", "pausableConfig"}
        exts = {e.get("extension") for e in info.get("extensions", []) if isinstance(e, dict)}
        if exts & bad:
            return False, "estensioni del token rischiose: " + ", ".join(sorted(exts & bad))
        return True, ""
    except Exception as e:
        return False, f"autorità non verificabili ({e})"

async def onchain_concentration(client, mint, creator=None):
    try:
        sup = await rpc(client, "getTokenSupply", [mint, {"commitment": "confirmed"}])
        total = float(sup["value"].get("uiAmountString") or sup["value"].get("uiAmount") or 0)
        big = await rpc(client, "getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
        accs = (big or {}).get("value", [])[:20]
        if total <= 0 or not accs:
            return None, "distribuzione holder non verificabile"
        info = await rpc(client, "getMultipleAccounts",
                         [[a["address"] for a in accs], {"encoding": "jsonParsed", "commitment": "confirmed"}])
        try:
            from solders.pubkey import Pubkey
        except Exception:
            Pubkey = None
        wallets = []
        for a, acc in zip(accs, (info or {}).get("value", [])):
            owner = ((((acc or {}).get("data") or {}).get("parsed") or {}).get("info") or {}).get("owner")
            if not owner:
                continue
            on_curve = True
            if Pubkey is not None:
                try:
                    on_curve = Pubkey.from_string(owner).is_on_curve()
                except Exception:
                    on_curve = True
            if on_curve:
                wallets.append((owner, float(a.get("uiAmountString") or a.get("uiAmount") or 0)))
        wallets.sort(key=lambda x: -x[1])
        def pct(items):
            return sum(v for _, v in items) / total * 100
        res = {
            "top1": pct(wallets[:1]), "top5": pct(wallets[:5]), "top10": pct(wallets[:10]),
            "dev": next((v / total * 100 for o, v in wallets if o == creator), 0.0) if creator else 0.0
        }
        return res, ""
    except Exception as e:
        return None, f"distribuzione holder non verificabile ({e})"

async def safety_checks(client, mint, creator=None):
    if not SAFETY_CHECKS:
        return True, "", {}
    ok, why = await check_authorities(client, mint)
    if not ok:
        return False, why, {}
    res, why = await onchain_concentration(client, mint, creator)
    if res is None:
        return False, why, {}
    info = {"mint_e_freeze_revocati": True, **{k: round(v, 1) for k, v in res.items()}}
    if res["top1"] > SN_MAX_HOLDER_PCT:
        return False, f"un wallet detiene il {res['top1']:.0f}% della supply", info
    if res["top5"] > SN_MAX_TOP5_PCT:
        return False, f"i primi 5 wallet detengono il {res['top5']:.0f}%", info
    if res["dev"] > SN_MAX_DEV_PCT:
        return False, f"il creatore detiene il {res['dev']:.0f}%", info
    return True, "", info

def liquidity_usd(w):
    if w.get("src") == "dexscreener" and w.get("liq_usd"):
        return w["liq_usd"]
    return max(0.0, w.get("vsol", 0.0) - 30) * SOL_USD

async def deep_checks(client, mint, w):
    ok, why, info = await safety_checks(client, mint, w.get("creator"))
    if not ok:
        return False, why, "non verificabil" in why, info
    liq = liquidity_usd(w)
    info["liquidita_usd"] = round(liq)
    if liq < SN_MIN_LIQ_USD:
        return False, f"liquidità bassa (${liq:,.0f})", False, info
    mcap = w["mcap_sol"] * SOL_USD
    info["mcap_su_liquidita"] = round(mcap / liq, 1) if liq > 0 else None
    if w.get("dex_id") not in (None, "pumpfun") and liq > 0 and mcap / liq > SN_MAX_MCAP_LIQ:
        return False, "prezzo e liquidità incoerenti", False, info
    data = await dex_fetch_many(client, [mint])
    p = data.get(mint)
    if p:
        pn, rp = float(p.get("priceNative") or 0), robust_price(w)
        if pn > 0 and rp:
            div = abs(pn / rp - 1) * 100
            info["scarto_prezzo_tra_fonti_pct"] = round(div, 1)
            if div > SN_MAX_PRICE_DIV_PCT:
                w["xchk"] += 1
                return False, "prezzo incoerente tra le fonti", w["xchk"] < 3, info
    return True, "", False, info

def push_hist(w):
    now = time.time()
    w["hist"].append((now, w["mcap_sol"]))
    w["hist"] = [(t, m) for t, m in w["hist"] if now - t <= 150]

def momentum_pct(w):
    now = time.time()
    older = [m for t, m in w["hist"] if now - t >= 45]
    base = older[-1] if older else (w["hist"][0][1] if w["hist"] else 0)
    return (w["mcap_sol"] / base - 1) * 100 if base > 0 else 0.0

async def update_sol_price(client):
    global SOL_USD
    try:
        r = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{WSOL}", timeout=15)
        pairs = [p for p in (r.json().get("pairs") or [])
                 if p.get("chainId") == "solana" and (p.get("baseToken") or {}).get("address") == WSOL
                 and (p.get("quoteToken") or {}).get("symbol") in ("USDC", "USDT")]
        if pairs:
            p = max(pairs, key=lambda x: (x.get("liquidity") or {}).get("usd") or 0)
            v = float(p.get("priceUsd") or 0)
            if v > 10:
                SOL_USD = v
    except Exception as e:
        log.warning("update_sol_price: %s", e)

async def snipe_trade(client, action, mint, amount):
    if DRY_RUN or not SNIPE_LIVE:
        return True, "SIMULAZIONE (nessun ordine inviato)"
    return await do_trade(client, action, mint, amount)

def entry_check(w):
    age = (time.time() - w["t0"]) / 60
    if age < SN_MIN_AGE_MIN:
        return "troppo giovane"
    if age > SN_MAX_AGE_MIN:
        return "troppo vecchio"
    mcap = w["mcap_sol"] * SOL_USD
    if mcap < SN_MCAP_MIN:
        return "market cap sotto soglia"
    if mcap > SN_MCAP_MAX:
        return "market cap sopra soglia"
    nb = len(w["buyers"]) if w["stream_trades"] else int(w["dex_buys"] / 1.5)
    if nb < SN_MIN_BUYERS:
        return "pochi compratori"
    tot = w["buys"] + w["sells"]
    if tot and w["buys"] / tot < SN_MIN_BUY_RATIO:
        return "troppe vendite"
    if w["creator_sold"]:
        return "il creatore ha venduto"
    if w["dump_flag"]:
        return "vendita pesante di un wallet"
    if w["stream_trades"]:
        why = cached(w, "conc", 5, concentration_issue)
        if why:
            return why
        b, s_ = recent_pressure(w)
        if b < SN_MIN_RECENT_BUYS or b / (b + s_) < 0.5:
            return "attività recente debole"
    if w["peak_sol"] > 0 and (w["peak_sol"] - w["mcap_sol"]) / w["peak_sol"] * 100 > SN_MAX_DRAWDOWN_PCT:
        return "già in forte calo dal picco"
    mom = momentum_pct(w)
    if mom < SN_MIN_MOMENTUM_PCT:
        return "senza slancio"
    if mom > SN_MAX_SPIKE_PCT:
        return "salita troppo verticale"
    if not w["last_price"]:
        return "prezzo non disponibile"
    f = cached(w, "struct", 5, structure_features)
    if f.get("insufficienti"):
        return "dati di prezzo insufficienti"
    if f["verticale"]:
        return "candela verticale unica"
    if f["spike_sparato"]:
        return "spike già sparato con lateralità sporca"
    if f["laterale_sporco"]:
        return "lateralità sporca senza trazione organica"
    if f["punteggio"] < SN_MIN_STRUCT_SCORE:
        return "struttura di prezzo debole"
    return None

def maybe_enter(app, client, mint, w):
    if w["blocked"] or w.get("evaluating") or time.time() < w["next_chk"]:
        return
    if not S["active"] or S.get("mode") != "snipe":
        return
    why = entry_check(w)
    if why:
        w["why"] = why
        return
    w["evaluating"] = True
    spawn(try_enter(app, client, mint, w))

async def try_enter(app, client, mint, w):
    try:
        await _try_enter(app, client, mint, w)
    except Exception as e:
        log.warning("try_enter %s: %s", mint[:8], e)
        w["next_chk"] = time.time() + 30
    finally:
        w["evaluating"] = False

async def _try_enter(app, client, mint, w):
    if len(S["positions"]) >= SN_MAX_POSITIONS:
        w["why"] = "posizioni piene"
        return
    ok, why, retry, info = await deep_checks(client, mint, w)
    if not ok:
        w["why"] = why
        if retry:
            w["next_chk"] = time.time() + 20
        else:
            w["blocked"] = True
        return
    liq = liquidity_usd(w)
    price0 = robust_price(w) or w["last_price"]
    # Setup forte (sizing e target di uscita) deciso solo da dati on-chain locali
    struct = cached(w, "struct", 5, structure_features)
    struct_score = float(struct.get("punteggio", 0))
    organic = organic_score(w)
    is_strong = (
        organic >= SN_STRONG_ORGANIC
        and struct_score >= SN_STRONG_STRUCT_SCORE
        and not struct.get("verticale")
        and not struct.get("spike_sparato")
        and not struct.get("laterale_sporco")
    )
    # Sizing dinamico 10% → 20%
    impact = 0.0
    if liq > 0 and S.get("budget"):
        impact = (S["budget"] * 0.15 * SOL_USD / liq) * 100  # stima impatto
    pct_size = calc_position_size(
        base_pct=SNIPE_PCT_PER_TRADE,
        liq_usd=liq,
        organic_score=organic,
        struct_score=struct_score,
        impact_pct=impact,
        is_strong=is_strong,
    )
    amount = round(min(S["budget"] * pct_size / 100, S["budget"] - exposure()), 4)
    amount = round(min(amount, liq * SN_MAX_POS_LIQ_PCT / 100 / SOL_USD), 4)
    if amount < 0.005:
        w["why"] = "budget o liquidità insufficiente"
        return

    # Target TP differenziato
    tp_target = SN_TP_STRONG_PCT if is_strong else SN_TP_PCT

    async with SN_LOCK:
        if mint in S["positions"] or not S["active"] or w["blocked"]:
            return
        why = entry_check(w)
        if why:
            w["why"] = why
            return
        price = robust_price(w) or w["last_price"]
        if price0 and abs(price / price0 - 1) * 100 > SN_MAX_ENTRY_DRIFT_PCT:
            w["why"], w["next_chk"] = "prezzo cambiato durante l'analisi", time.time() + 10
            return
        amount = round(min(amount, S["budget"] - exposure()), 4)
        if amount < 0.005 or len(S["positions"]) >= SN_MAX_POSITIONS:
            w["why"] = "budget o posizioni esauriti"
            return
        entry = price * (1 + SNIPE_COST_PCT / 100)
        ok, info2 = await snipe_trade(client, "buy", mint, amount)
        if ok:
            S["positions"][mint] = {
                "cost": amount, "left": 1.0, "qty": amount / entry,
                "last_price": price, "sym": w["symbol"], "entry": price,
                "peak": price, "t": time.time(), "tp_done": False,
                "creator": w["creator"],
                "is_strong": is_strong,
                "tp_target": tp_target,
                "size_pct": pct_size,
            }
            S["trades"] += 1
            sn()["entered"] += 1
            save_state()
        else:
            w["blocked"], w["why"] = True, "ordine fallito"
        tot = max(1, w["buys"] + w["sells"])
        strength = "FORTE" if is_strong else "cauto"
        dev_pct = w["holders"].get(w["creator"], 0) / TOTAL_SUPPLY * 100
        await notify(app, (
            f"{'🟢' if ok else '🔴'} BUY {amount} SOL ({pct_size:.0f}% — {strength}) — {w['symbol']}\n"
            f"https://pump.fun/coin/{mint}\n"
            f"mcap ${w['mcap_sol'] * SOL_USD:,.0f}, "
            f"{len(w['buyers']) if w['stream_trades'] else int(w['dex_buys']/1.5)} compratori, "
            f"{len(w['candles'])} candele 15s\n"
            f"{w['buys']/tot*100:.0f}% acquisti, età {(time.time()-w['t0'])/60:.1f} min, Dev {dev_pct:.1f}%\n"
            f"TP target: +{tp_target:.0f}%\n"
            f"{info2}"
        ))

async def snipe_sell(app, client, mint, pct, reason):
    pos = S["positions"].get(mint)
    if not pos:
        return
    w = WATCH.get(mint) or {}
    rp, lp = (robust_price(w) if w else None), w.get("last_price")
    price_now = min([p for p in (rp, lp) if p], default=pos.get("last_price"))
    ok, info = await snipe_trade(client, "sell", mint, f"{pct}%")
    extra = ""
    if ok:
        pnl, cs = settle(mint, pos, pct, price_now, SNIPE_COST_PCT, TOTAL_SUPPLY * SOL_USD)
        extra = pnl_text(pnl, cs)
        if pnl is not None and cs > 0 and pnl / cs <= -0.6:
            mark_bad_creator(pos.get("creator"))
        pos["left"] *= 1 - pct / 100
        S["trades"] += 1
        if pct >= 100 or pos["left"] < 0.02:
            del S["positions"][mint]
            WATCH.pop(mint, None)
            await ws_send({"method": "unsubscribeTokenTrade", "keys": [mint]})
        save_state()
    await notify(app, f"{'🟢' if ok else '🔴'} SELL {pct}% {pos.get('sym') or mint[:6]} — {reason}\nhttps://pump.fun/coin/{mint}\n{info}{extra}")

async def snipe_close_all(app, client):
    for mint in list(S["positions"]):
        await snipe_sell(app, client, mint, 100, "chiusura sessione")
    save_state()

async def manage_position(app, client, mint):
    pos, w = S["positions"].get(mint), WATCH.get(mint)
    if not pos or not w:
        return
    now = time.time()
    price = robust_price(w) or pos["last_price"]
    pos["last_price"] = price
    pos["peak"] = max(pos.get("peak") or price, price)
    gain = (price / pos["entry"] - 1) * 100
    if gain >= SN_BE_ARM_PCT:
        pos["be"] = True
    if gain <= -SN_STOP_PCT:
        pos["sl_t"] = pos.get("sl_t") or now
    else:
        pos["sl_t"] = None
    sl_hit = gain <= -SN_STOP_PCT and (gain <= -60 or now - pos.get("sl_t", now) >= 4)
    recent = [p for t, p in w["px"] if now - t <= 30]
    drop = (recent[-1] / max(recent) - 1) * 100 if recent else 0
    mcap_usd = w["mcap_sol"] * SOL_USD
    # TP target differenziato: setup forti → +100%, setup cauti → +30/40%
    tp_target = pos.get("tp_target", SN_TP_PCT)
    is_strong = pos.get("is_strong", False)
    reason, pct = None, 100
    if w["creator_sold"]:
        reason = "il creatore ha venduto"
    elif w.get("dump_flag"):
        reason = "vendita pesante di un wallet"
    elif drop <= -SN_FASTDROP_PCT:
        reason = f"calo improvviso ({drop:.0f}% in 30 s)"
    elif not pos.get("tp_done") and gain >= tp_target:
        # sui forti lasciamo correre di più (trailing dopo il primo TP parziale se configurato)
        reason, pct = f"take profit (+{gain:.0f}% — {'forte' if is_strong else 'cauto'})", SN_TP_SELL_PCT
    elif sl_hit:
        reason = f"stop loss ({gain:.0f}%)"
    elif pos.get("be") and gain <= BE_LOCK_PCT:
        reason = "protezione: tornato al prezzo d'ingresso"
    elif SN_EXIT_MCAP_USD > 0 and mcap_usd >= SN_EXIT_MCAP_USD:
        reason = "market cap obiettivo raggiunto"
    elif now - pos["t"] > SN_MAX_HOLD_MIN * 60:
        reason = "tempo massimo di permanenza"
    elif now - w["last_t"] > SN_STALE_MIN * 60:
        reason = "nessuno scambia più (stall)"
    elif pos.get("tp_done") and (price / pos["peak"] - 1) * 100 <= -SN_TRAIL_PCT:
        reason = f"trailing stop (guadagno {gain:+.0f}%)"
    if not reason:
        return
    if pct < 100:
        pos["tp_done"] = True
    await snipe_sell(app, client, mint, pct, reason)

async def on_event(app, client, ev):
    mint = ev.get("mint")
    st = sn()
    if not mint:
        st["msgs"] += 1
        st["last_msg"] = json.dumps(ev, ensure_ascii=False)[:200]
        return
    st["events"] += 1
    st["last_event"] = int(time.time())
    tx = ev.get("txType")
    st["types"][str(tx)] = st["types"].get(str(tx), 0) + 1
    now = time.time()
    if tx == "create":
        st["seen"] += 1
        if len(WATCH) >= SN_MAX_WATCH:
            for k in sorted(WATCH, key=lambda k: WATCH[k]["t0"]):
                if k not in S["positions"]:
                    WATCH.pop(k)
                    await ws_send({"method": "unsubscribeTokenTrade", "keys": [k]})
                    break
        cr = ev.get("traderPublicKey")
        lst = CREATORS.setdefault(cr, [])
        lst[:] = [x for x in lst if now - x < 21600]
        lst.append(now)
        w = new_watch(ev)
        w["holders"][cr] = float(ev.get("initialBuy") or 0)
        if cr in S.get("bad_creators", []):
            w["blocked"], w["why"] = True, "creatore già visto fare rug"
        elif len(lst) > SN_MAX_CREATOR_LAUNCHES:
            w["blocked"], w["why"] = True, "creatore seriale"
        WATCH[mint] = w
        if not w["blocked"]:
            PENDING_SUB.append(mint)
        return
    w = WATCH.get(mint)
    if not w or tx not in ("buy", "sell"):
        return
    price, mc = price_from(ev), ev.get("marketCapSol")
    if ev.get("vSolInBondingCurve"):
        w["vsol"] = float(ev["vSolInBondingCurve"])
    accepted = False
    if price and mc:
        if abs(float(mc) / TOTAL_SUPPLY / price - 1) > 0.05:
            st["incoherent"] += 1
        elif push_price(w, price):
            w["last_price"] = price
            w["mcap_sol"] = float(mc)
            w["peak_sol"] = max(w["peak_sol"], w["mcap_sol"])
            accepted = True
    w["last_t"] = now
    w["stream_trades"] += 1
    w["src"] = "stream"
    if accepted:
        push_hist(w)
        push_candle(w)
    trader = ev.get("traderPublicKey")
    sol = float(ev.get("solAmount") or 0)
    w["vol"] += sol
    if ev.get("newTokenBalance") is not None:
        w["holders"][trader] = float(ev["newTokenBalance"])
    w["tlog"] = (w["tlog"] + [(now, tx)])[-300:]
    if trader != w["creator"]:
        w["tcount"][trader] = w["tcount"].get(trader, 0) + 1
    if tx == "buy":
        w["buys"] += 1
        if trader != w["creator"]:
            w["buyers"].add(trader)
            w["first_buy"].setdefault(trader, now)
            w["buy_vol"][trader] = w["buy_vol"].get(trader, 0.0) + sol
            w["buys_log"] = (w["buys_log"] + [(now, trader, sol)])[-150:]
    else:
        w["sells"] += 1
        w["sellers"].add(trader)
        if trader == w["creator"]:
            w["creator_sold"] = True
            if now - w["t0"] < 900:
                mark_bad_creator(trader)
        if w["vsol"] and sol / w["vsol"] * 100 >= SN_WHALE_SELL_PCT:
            w["dump_flag"] = True
    if mint in S["positions"]:
        async with SN_LOCK:
            await manage_position(app, client, mint)
    elif S["active"] and S.get("mode") == "snipe":
        maybe_enter(app, client, mint, w)

async def dex_fetch_many(client, mints):
    """Prezzi/liquidità da DexScreener a gruppi. Un 429 attiva un backoff condiviso (2s -> 20s) che rallenta
    anche i gruppi e le chiamate successive, invece di far ripartire ogni gruppo da zero per conto suo."""
    out = {}
    now0 = time.time()
    todo = []
    for m in mints:
        c = DEX_CACHE.get(m)
        if c and now0 - c[0] < DEX_CACHE_TTL:
            out[m] = c[1]
        else:
            todo.append(m)
    cached_n = len(out)
    mints = todo
    for i in range(0, len(mints), 30):
        chunk = mints[i:i + 30]
        wait = DEX_BACKOFF["until"] - time.time()
        if wait > 0:
            await asyncio.sleep(min(wait, DEX_BACKOFF_MAX))
        for attempt in range(DEX_MAX_RETRIES):
            try:
                r = await client.get(
                    "https://api.dexscreener.com/latest/dex/tokens/" + ",".join(chunk),
                    timeout=15
                )
                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    try:
                        d = float(retry_after) if retry_after else DEX_BACKOFF["delay"]
                    except ValueError:
                        d = DEX_BACKOFF["delay"]
                    d = min(max(d, DEX_BACKOFF_BASE), DEX_BACKOFF_MAX)
                    sn()["dex_429"] = sn().get("dex_429", 0) + 1
                    DEX_BACKOFF["delay"] = min(d * 2, DEX_BACKOFF_MAX)
                    DEX_BACKOFF["until"] = time.time() + d
                    await asyncio.sleep(d)
                    continue
                r.raise_for_status()
                DEX_BACKOFF["delay"] = max(DEX_BACKOFF_BASE, DEX_BACKOFF["delay"] * 0.7)
                for p in r.json().get("pairs") or []:
                    addr = (p.get("baseToken") or {}).get("address")
                    if p.get("chainId") != "solana" or addr not in chunk:
                        continue
                    cur = out.get(addr)
                    if not cur or ((p.get("liquidity") or {}).get("usd") or 0) > ((cur.get("liquidity") or {}).get("usd") or 0):
                        out[addr] = p
                break
            except Exception as e:
                log.warning("dex_fetch_many: %s", e)
                if attempt < DEX_MAX_RETRIES - 1:
                    await asyncio.sleep(DEX_BACKOFF["delay"])
        if i + 30 < len(mints):
            await asyncio.sleep(DEX_CHUNK_GAP)
    tnow = time.time()
    for m in mints:
        if m in out:
            DEX_CACHE[m] = (tnow, out[m])
    if len(DEX_CACHE) > 2000:
        for k in [k for k, (t_, _) in DEX_CACHE.items() if tnow - t_ > DEX_CACHE_TTL * 4]:
            DEX_CACHE.pop(k, None)
    return out

async def dex_search_new(client):
    wait = DEX_BACKOFF["until"] - time.time()
    if wait > 0:
        await asyncio.sleep(min(wait, DEX_BACKOFF_MAX))
    try:
        r = await client.get("https://api.dexscreener.com/latest/dex/search", params={"q": "pump.fun"}, timeout=15)
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            try:
                d = float(retry_after) if retry_after else DEX_BACKOFF["delay"]
            except ValueError:
                d = DEX_BACKOFF["delay"]
            d = min(max(d, DEX_BACKOFF_BASE), DEX_BACKOFF_MAX)
            sn()["dex_429"] = sn().get("dex_429", 0) + 1
            DEX_BACKOFF["delay"] = min(d * 2, DEX_BACKOFF_MAX)
            DEX_BACKOFF["until"] = time.time() + d
            await asyncio.sleep(d)
            return []
        r.raise_for_status()
        DEX_BACKOFF["delay"] = max(DEX_BACKOFF_BASE, DEX_BACKOFF["delay"] * 0.7)
        now = time.time()
        out = []
        for p in r.json().get("pairs") or []:
            if p.get("chainId") != "solana":
                continue
            created = p.get("pairCreatedAt")
            if not created or (now - created / 1000) / 60 > SN_MAX_AGE_MIN:
                continue
            addr = (p.get("baseToken") or {}).get("address")
            if addr:
                out.append((addr, p))
        return out
    except Exception as e:
        log.warning("dex_search_new: %s", e)
        return []

def apply_dex(w, p, stats):
    now = time.time()
    pn = float(p.get("priceNative") or 0)
    if pn > 0 and push_price(w, pn):
        w["last_price"] = pn
        w["mcap_sol"] = pn * TOTAL_SUPPLY
        w["peak_sol"] = max(w["peak_sol"], w["mcap_sol"])
        w["last_t"] = now
        push_hist(w)
        push_candle(w)
    w["liq_usd"] = float((p.get("liquidity") or {}).get("usd") or 0)
    w["dex_id"] = p.get("dexId")
    if stats:
        tx = (p.get("txns") or {}).get("m5") or {}
        w["buys"] = int(tx.get("buys") or 0)
        w["sells"] = int(tx.get("sells") or 0)
        w["dex_buys"] = w["buys"]
        w["vol"] = float((p.get("volume") or {}).get("m5") or 0) / max(SOL_USD, 1)
        w["src"] = "dexscreener"

async def rpc_discover_new(app, client):
    for prog in PUMP_PROGRAMS:
        try:
            sigs = await rpc(client, "getSignaturesForAddress", [prog, {"limit": RPC_DISCOVER_LIMIT, "commitment": "confirmed"}])
        except Exception as e:
            log.warning("rpc_discover_new sigs: %s", e)
            continue
        new_sigs = [s["signature"] for s in sigs if not s.get("err") and s["signature"] not in RPC_SEEN_SIGS]
        for sig in new_sigs[:RPC_DISCOVER_LIMIT]:
            RPC_SEEN_SIGS.add(sig)
            try:
                tx = await get_tx(client, sig)
            except Exception as e:
                log.warning("rpc_discover_new tx: %s", e)
                continue
            if not tx:
                continue
            try:
                await rpc_extract_tokens(app, client, tx)
            except Exception as e:
                log.warning("rpc_extract_tokens: %s", e)
    if len(RPC_SEEN_SIGS) > 4000:
        for s_ in list(RPC_SEEN_SIGS)[:2000]:
            RPC_SEEN_SIGS.discard(s_)

async def rpc_extract_tokens(app, client, tx):
    meta = tx.get("meta")
    if not meta or meta.get("err"):
        return
    keys = [k["pubkey"] if isinstance(k, dict) else k for k in tx["transaction"]["message"]["accountKeys"]]
    if not PUMP_PROGRAMS.intersection(keys):
        return
    signer = keys[0] if keys else None
    mints = {b["mint"] for b in (meta.get("postTokenBalances") or []) if b.get("mint") and b["mint"] != WSOL}
    now = time.time()
    for mint in mints:
        w = WATCH.get(mint)
        if not w:
            w = new_watch()
            w["creator"] = signer
            w["src"] = "rpc"
            WATCH[mint] = w
        pre = next((b for b in (meta.get("preTokenBalances") or []) if b.get("mint") == mint and b.get("owner") == signer), None)
        post = next((b for b in (meta.get("postTokenBalances") or []) if b.get("mint") == mint and b.get("owner") == signer), None)
        pre_amt = float((pre or {}).get("uiTokenAmount", {}).get("uiAmountString") or 0)
        post_amt = float((post or {}).get("uiTokenAmount", {}).get("uiAmountString") or 0)
        if post_amt > pre_amt and signer:
            w["buyers"].add(signer)
            w["first_buy"].setdefault(signer, now)
            w["buys"] += 1
            w["tlog"] = (w["tlog"] + [(now, "buy")])[-300:]
        elif post_amt < pre_amt and signer:
            w["sellers"].add(signer)
            w["sells"] += 1
            w["tlog"] = (w["tlog"] + [(now, "sell")])[-300:]
        w["last_t"] = now
        w["stream_trades"] += 1
    if not S["active"] or S.get("mode") != "snipe":
        return
    for mint in mints:
        if mint not in S["positions"]:
            maybe_enter(app, client, mint, WATCH[mint])

async def dex_discover_new(app, client):
    found = await dex_search_new(client)
    for addr, p in found:
        w = WATCH.get(addr)
        if not w:
            w = new_watch()
            w["symbol"] = (p.get("baseToken") or {}).get("symbol") or "?"
            w["name"] = (p.get("baseToken") or {}).get("name")
            created = p.get("pairCreatedAt")
            if created:
                w["t0"] = created / 1000
            w["src"] = "dexscreener"
            WATCH[addr] = w
        apply_dex(w, p, stats=True)
    if not S["active"] or S.get("mode") != "snipe":
        return
    for addr, _ in found:
        if addr not in S["positions"]:
            maybe_enter(app, client, addr, WATCH[addr])

def ws_is_stale():
    return WS is None or (time.time() - LAST_WS_EVENT > WS_STALE_SEC)

async def fallback_cycle(app, client):
    if not ws_is_stale():
        return
    st = sn()
    st["fallback_on"] = True
    st["fallback_cycles"] = st.get("fallback_cycles", 0) + 1
    await safe(rpc_discover_new(app, client), "rpc_discover_new")
    await safe(dex_discover_new(app, client), "dex_discover_new")

async def dex_cycle(app, client):
    now = time.time()
    if now < DEX_BACKOFF["until"]:
        return
    held = [m for m in S["positions"] if m in WATCH and now - WATCH[m]["dex_t"] > 10
            and (now - WATCH[m]["last_t"] > 15 or WATCH[m]["mcap_sol"] * SOL_USD >= SN_HANDOFF_MCAP_USD)]
    cand = [k for k, w in WATCH.items() if k not in S["positions"] and not w["blocked"]
            and (w["stream_trades"] == 0 or w["src"] != "stream")
            and SN_MIN_AGE_MIN <= (now - w["t0"]) / 60 <= SN_MAX_AGE_MIN and now - w["dex_t"] > 15]
    cand = sorted(cand, key=lambda k: WATCH[k]["dex_t"])[:150]
    mints = held + cand
    if not mints:
        return
    data = await dex_fetch_many(client, mints)
    st = sn()
    st["dex_polls"] += 1
    st["dex_hits"] += len(data)
    for m in mints:
        w = WATCH.get(m)
        if not w:
            continue
        w["dex_t"] = now
        if m in data:
            apply_dex(w, data[m], stats=(m in cand))
    for m in cand:
        w = WATCH.get(m)
        if w and m in data and m not in S["positions"] and S["active"]:
            maybe_enter(app, client, m, w)

async def snipe_housekeeping(app, client):
    last_sol = time.time()
    while S["active"] and S.get("mode") == "snipe":
        now = time.time()
        await flush_subs()
        try:
            await fallback_cycle(app, client)
        except Exception as e:
            log.warning("fallback_cycle: %s", e)
        try:
            await dex_cycle(app, client)
        except Exception as e:
            log.warning("dex_cycle: %s", e)
        if now - last_sol > 300:
            await update_sol_price(client)
            last_sol = now
        if now >= S["end"]:
            S["active"] = False
            await notify(app, "📉 Durata terminata. Chiudo le posizioni aperte.")
            async with SN_LOCK:
                await snipe_close_all(app, client)
            await notify(app, "Sessione chiusa.\n" + pnl_summary())
            save_state()
            return
        async with SN_LOCK:
            for mint in list(S["positions"]):
                await manage_position(app, client, mint)
        expired = [k for k, w in WATCH.items() if k not in S["positions"] and (now - w["t0"]) / 60 > SN_MAX_AGE_MIN]
        if expired:
            reasons = sn()["reasons"]
            for k in expired:
                why = WATCH.pop(k)["why"]
                reasons[why] = reasons.get(why, 0) + 1
            for i in range(0, len(expired), 100):
                await ws_send({"method": "unsubscribeTokenTrade", "keys": expired[i:i + 100]})
        await asyncio.sleep(rpc_poll_interval(2))

async def snipe_run(app):
    global WS, LAST_WS_EVENT
    try:
        import websockets
    except ImportError:
        S["active"] = False
        await notify(app, "Manca la libreria 'websockets'. Aggiungi websockets>=12 a requirements.txt")
        return
    for mint, pos in S["positions"].items():
        w = new_watch()
        w["last_price"] = pos.get("last_price")
        WATCH.setdefault(mint, w)
    async with httpx.AsyncClient() as client:
        await update_sol_price(client)
        hk = asyncio.create_task(snipe_housekeeping(app, client))
        backoff = 2
        try:
            while S["active"] and S.get("mode") == "snipe":
                try:
                    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                        WS = ws
                        LAST_WS_EVENT = time.time()
                        await ws.send(json.dumps({"method": "subscribeNewToken"}))
                        keys = list(WATCH.keys())
                        for i in range(0, len(keys), 100):
                            await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": keys[i:i + 100]}))
                        backoff = 2
                        sn()["ws_err"] = ""
                        while S["active"] and S.get("mode") == "snipe":
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=10)
                            except asyncio.TimeoutError:
                                continue
                            try:
                                ev = json.loads(raw)
                            except Exception:
                                continue
                            LAST_WS_EVENT = time.time()
                            try:
                                await on_event(app, client, ev)
                            except Exception as e:
                                log.warning("on_event: %s", e)
                except Exception as e:
                    log.warning("websocket: %s", e)
                    sn()["ws_err"] = str(e)[:150]
                WS = None
                sn()["fallback_on"] = True
                if S["active"]:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)
        finally:
            WS = None
            try:
                await asyncio.wait_for(hk, timeout=90)
            except Exception:
                hk.cancel()

# ---------------------------------------------------------------------------
# Telegram commands
# ---------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("Non sei autorizzato.")
        return ConversationHandler.END
    if S["active"]:
        await update.message.reply_text("C'è già una sessione attiva. Usa /stop per chiuderla.")
        return ConversationHandler.END
    sim = "SIMULAZIONE (nessun soldo reale)" if DRY_RUN else "REALE ⚠️"
    kb = ReplyKeyboardMarkup([["Copia trader"], ["Cerca nuovi token"]], one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text(
        f"Stato: {sim}\n\nCosa vuoi fare?\n"
        "• Copia trader: copio i trade dei wallet che scegli tu\n"
        "• Cerca nuovi token: cerco da solo token appena nati",
        reply_markup=kb,
    )
    return GET_MODE

async def get_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip().lower()
    if "copia" in t:
        context.user_data["mode"] = "copy"
        await update.message.reply_text(
            "Mandami gli indirizzi wallet dei trader da copiare (max 5), separati da spazio o virgola.",
            reply_markup=ReplyKeyboardRemove()
        )
        return GET_TRADERS
    if "cerca" in t or "token" in t:
        context.user_data["mode"] = "snipe"
        await update.message.reply_text(
            f"Budget massimo in SOL (es. 0.5)? Sizing dinamico: ~{SNIPE_PCT_PER_TRADE:.0f}% base, fino a 20% su setup ad alto potenziale.",
            reply_markup=ReplyKeyboardRemove()
        )
        return GET_BUDGET
    await update.message.reply_text("Scegli con i pulsanti: Copia trader oppure Cerca nuovi token.")
    return GET_MODE

async def get_traders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    items = [t for t in re.split(r"[,\s]+", update.message.text.strip()) if t]
    bad = [t for t in items if not ADDR_RE.match(t)]
    if not items or bad or len(items) > 5:
        await update.message.reply_text("Indirizzi non validi (servono da 1 a 5 indirizzi Solana). Riprova:")
        return GET_TRADERS
    context.user_data["traders"] = list(dict.fromkeys(items))
    await update.message.reply_text(
        f"Budget massimo in SOL da usare (es. 0.1)? Per ogni trade copiato userò il {PCT_PER_TRADE:.0f}% del budget."
    )
    return GET_BUDGET

async def get_budget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        budget = float(update.message.text.strip().replace(",", "."))
        if budget <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Inserisci un numero valido (es. 0.1):")
        return GET_BUDGET
    if not DRY_RUN and (context.user_data.get("mode", "copy") == "copy" or SNIPE_LIVE):
        try:
            async with httpx.AsyncClient() as c:
                bal = await wallet_balance(c)
        except Exception as e:
            await update.message.reply_text(f"Non riesco a leggere il saldo del wallet: {e}")
            return ConversationHandler.END
        if bal is None or budget > bal - 0.02:
            await update.message.reply_text(
                f"Saldo wallet: {bal} SOL. Mi servono budget + circa 0.02 SOL per le fee. Sessione interrotta."
            )
            return ConversationHandler.END
    context.user_data["budget"] = budget
    await update.message.reply_text("Per quante ore tengo attiva la sessione? (es. 0.5 o 3)")
    return GET_DURATION

async def get_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        hours = float(update.message.text.strip().replace(",", "."))
        if hours <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Inserisci un numero valido di ore (es. 0.5):")
        return GET_DURATION
    if context.user_data.get("mode") == "snipe":
        S.update({
            "active": True, "mode": "snipe", "traders": [],
            "budget": context.user_data["budget"],
            "end": time.time() + hours * 3600,
            "positions": {}, "last": {}, "trades": 0, "skipped": 0,
            "pnl": 0.0, "closed": [], "diag": {}, "sn": {}, "by_trader": {},
        })
        WATCH.clear()
        save_state()
        context.application.create_task(snipe_run(context.application))
        live = SNIPE_LIVE and not DRY_RUN
        await update.message.reply_text(
            f"✅ Caccia ai nuovi token avviata ({'REALE ⚠️' if live else 'SIMULAZIONE'})\n"
            f"Budget: {S['budget']} SOL (sizing dinamico 10-20%, max {SN_MAX_POSITIONS} posizioni)\n"
            f"Durata: {hours} ore\n\n"
            "Ti avviso a ogni acquisto e vendita. /resoconto per il riepilogo, /diagnostica, /stop per chiudere."
        )
        return ConversationHandler.END
    S.update({
        "active": True, "mode": "copy",
        "traders": context.user_data["traders"],
        "budget": context.user_data["budget"],
        "end": time.time() + hours * 3600,
        "positions": {}, "last": {}, "trades": 0, "skipped": 0,
        "pnl": 0.0, "closed": [], "by_trader": {},
        "muted": [], "tbuy_t": {}, "tflips": {}, "diag": {}, "sn": {},
    })
    save_state()
    context.application.create_task(monitor(context.application))
    await update.message.reply_text(
        f"✅ Sessione avviata ({'SIMULAZIONE' if DRY_RUN else 'REALE'})\n"
        f"Trader: {', '.join(short(t) for t in S['traders'])}\n"
        f"Budget: {S['budget']} SOL ({PCT_PER_TRADE:.0f}% per trade)\n"
        f"Durata: {hours} ore\n\n"
        "Ti avviso a ogni trade. /resoconto, /stop per chiudere."
    )
    return ConversationHandler.END

async def resoconto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    is_sn = S.get("mode") == "snipe"
    sim = DRY_RUN or (is_sn and not SNIPE_LIVE)
    cost_pct = SNIPE_COST_PCT if is_sn else SIM_COST_PCT
    lines = [
        f"Stato: {'ATTIVA' if S['active'] else 'ferma'} — {'SIMULAZIONE' if sim else 'REALE'} — "
        f"{'caccia token' if is_sn else 'copia trader'}"
    ]
    if S["active"]:
        lines.append(f"Tempo rimasto: {max(0, int((S['end'] - time.time()) / 60))} min")
    if not is_sn:
        lines.append(f"Trader: {', '.join(short(t) for t in S['traders'])}")
    if is_sn:
        st = sn()
        lines.append(f"Token visti: {st['seen']} — in osservazione: {len(WATCH)} — entrati: {st['entered']}")
    lines.append(f"Esposizione: {exposure():.4f} / {S['budget']} SOL")
    lines.append(f"Ordini eseguiti: {S['trades']} — trade scartati: {S.get('skipped', 0)}")
    lines.append(f"Vendite chiuse (stimato): {S.get('pnl', 0.0):+.4f} SOL")
    unreal = 0.0
    async with httpx.AsyncClient() as pc:
        for mint, p in list(S["positions"].items())[:10]:
            name = p.get("sym") or mint[:6]
            cost = p["cost"] * p["left"]
            pr = p.get("last_price") if is_sn else (await get_price(pc, mint) or p.get("last_price"))
            if pr and p.get("qty"):
                val = p["qty"] * p["left"] * pr * (1 - cost_pct / 100)
                unreal += val - cost
                lines.append(f"• {name}: costo {cost:.4f}, valore ora {val:.4f} ({(val / cost - 1) * 100:+.0f}%)")
            else:
                lines.append(f"• {name}: costo {cost:.4f}, valore non disponibile")
    lines.append(f"Posizioni aperte (stimato): {unreal:+.4f} SOL")
    lines.append(f"TOTALE stimato: {S.get('pnl', 0.0) + unreal:+.4f} SOL")
    for c in S.get("closed", [])[-10:]:
        lines.append("  " + fmt_closed(c))
    lines.append(f"(calcolo con costo {cost_pct:.0f}% per lato)")
    if KEYPAIR and not DRY_RUN:
        try:
            async with httpx.AsyncClient() as c:
                lines.append(f"Saldo wallet: {await wallet_balance(c):.4f} SOL")
        except Exception:
            pass
    await update.message.reply_text("\n".join(lines))

async def diagnostica(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if S.get("mode") == "snipe":
        st = sn()
        ago = int(time.time() - st["last_event"]) if st["last_event"] else None
        top = sorted(st["reasons"].items(), key=lambda x: -x[1])[:6]
        lines = [
            "Diagnostica caccia token:",
            f"• stato: {'ATTIVA' if S['active'] else 'ferma'}",
            f"• eventi ricevuti: {st['events']} (ultimo {ago}s fa)",
            f"• token visti: {st['seen']}, in osservazione ora: {len(WATCH)}",
            f"• entrati: {st['entered']}",
            f"• fallback attivo: {'sì' if ws_is_stale() else 'no'} (cicli: {st.get('fallback_cycles', 0)})",
            f"• HTTP 429 DexScreener: {st.get('dex_429', 0)}",
            f"• ultimo errore WS: {st['ws_err'] or 'nessuno'}",
            "• motivi scarto:",
        ] + [f"   {n}× {r}" for r, n in top]
        await update.message.reply_text("\n".join(lines))
        return
    if not S["traders"]:
        await update.message.reply_text("Nessuna sessione avviata.")
        return
    lines = ["Diagnostica per trader:"]
    for t in S["traders"]:
        d = dg(t)
        ago = int(time.time() - d["last_poll"]) if d["last_poll"] else None
        lines.append(
            f"\n{short(t)}\n"
            f"• controlli: {d['polls']} (ultimo {ago}s fa)\n"
            f"• firme viste: {d['sigs']} (fallite: {d['failed']}, non scaricate: {d['null']})\n"
            f"• trade riconosciuti: {d['trades']}\n"
            f"• acquisti ignorati (piccoli): {d['small']}\n"
            f"• ultimo errore: {d['err'] or 'nessuno'}"
        )
    await update.message.reply_text("\n".join(lines))

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not S["active"] and not S["positions"]:
        await update.message.reply_text("Nessuna sessione attiva.")
        return
    S["active"] = False
    await update.message.reply_text("Fermo la sessione e vendo le posizioni aperte...")
    async with httpx.AsyncClient() as c:
        await close_all(context.application, c)
    await update.message.reply_text("Fatto.\n" + pnl_summary())

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Operazione annullata.")
    return ConversationHandler.END

# ---------------------------------------------------------------------------
# Flask + main
# ---------------------------------------------------------------------------
web = Flask(__name__)

@web.route("/")
def home():
    return "Bot is running"

def run_flask():
    web.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), use_reloader=False)

async def post_init(app):
    load_state()
    if S.get("active"):
        await notify(app, "Bot riavviato: riprendo la sessione attiva.")
        app.create_task(snipe_run(app) if S.get("mode") == "snipe" else monitor(app))

def main():
    if not TOKEN:
        print("ERRORE: variabile TOKEN mancante")
        return
    if not DRY_RUN and not KEYPAIR:
        print("ERRORE: DRY_RUN=false ma PRIVATE_KEY mancante")
        return
    threading.Thread(target=run_flask, daemon=True).start()
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("inizia", start),
            MessageHandler(filters.Regex("(?i)^(inizia|ciao|start|parti|ricomincia)$"), start),
        ],
        states={
            GET_MODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_mode)],
            GET_TRADERS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_traders)],
            GET_BUDGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_budget)],
            GET_DURATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_duration)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    app.add_handler(conv)
    app.add_handler(CommandHandler("resoconto", resoconto))
    app.add_handler(MessageHandler(filters.Regex("(?i)^resoconto$"), resoconto))
    app.add_handler(CommandHandler("diagnostica", diagnostica))
    app.add_handler(MessageHandler(filters.Regex("(?i)^diagnostica$"), diagnostica))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(MessageHandler(filters.Regex("(?i)^(stop|esci|chiudi)$"), stop))
    print("Bot avviato — DRY_RUN =", DRY_RUN)
    app.run_polling()

if __name__ == "__main__":
    main()
