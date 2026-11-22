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
from html import escape as hesc
from solders.pubkey import Pubkey
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
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


def _env(name: str, default):
    """Legge una variabile d'ambiente con lo stesso tipo del valore di default."""
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        if isinstance(default, bool):
            return v.strip().lower() in ("1", "true", "yes", "on")
        return type(default)(v)
    except Exception:
        return default


class CONFIG:
    """TUTTI i parametri dei filtri in un punto solo.
    Ogni valore ha un default qui sotto e può essere sovrascritto dalla variabile d'ambiente indicata
    (attenzione: una variabile già impostata su Render vince sempre sul default del codice)."""

    # --- 1) Fase bonding curve (WebSocket/RPC) -> fase DEX (DexScreener) --------------------------
    # DexScreener si interroga solo se il token ha almeno uno di questi due requisiti (altrimenti si usano
    # solo i dati ricevuti via WebSocket/RPC) ed è più vecchio di dex_min_age_sec.
    dex_phase_mcap_usd = _env("DEX_PHASE_MCAP_USD", 8000.0)
    dex_phase_min_txs = _env("DEX_PHASE_MIN_TXS", 25)
    dex_min_age_sec = _env("DEX_MIN_AGE_SEC", 60.0)
    dex_cache_ttl_sec = _env("DEX_CACHE_TTL", 8.0)               # cache per mint (15-20 s)
    dex_cache_ttl_positions_sec = _env("DEX_CACHE_TTL_POS", 5.0)  # posizioni aperte: dati più freschi
    dex_neg_ttl_sec = _env("DEX_NEG_TTL", 20.0)                    # cache anche per "nessuna pool trovata"
    dex_min_interval_sec = _env("DEX_MIN_INTERVAL", 1.5)           # rate limiter globale: 1 richiesta ogni N s
    dex_429_cooldown_min_sec = _env("DEX_429_COOLDOWN_MIN", 15.0)  # dopo un HTTP 429: stop a TUTTE le chiamate
    dex_429_cooldown_max_sec = _env("DEX_429_COOLDOWN_MAX", 30.0)  # (raddoppia a ogni 429 consecutivo)
    dex_batch_window_sec = _env("DEX_BATCH_WINDOW", 0.4)           # raccoglie i mint di più chiamanti in 1 batch
    dex_wait_timeout_sec = _env("DEX_WAIT_TIMEOUT", 30.0)
    dex_repoll_sec = _env("SN_DEX_REPOLL_SEC", 12.0)               # ogni token al massimo 1 volta ogni N s
    dex_max_per_cycle = _env("SN_DEX_MAX_PER_CYCLE", 60)           # max token per ciclo (2 chiamate da 30)
    dex_search_interval_sec = _env("DEX_SEARCH_INTERVAL", 30.0)    # ricerca nuovi token (solo in fallback)

    # --- 2) Filtri di mercato (deterministici) -------------------------------------------------
    mcap_min_usd = _env("SN_MCAP_MIN", 6000.0)
    mcap_max_usd = _env("SN_MCAP_MAX", 50000.0)
    min_age_min = _env("SN_MIN_AGE_MIN", 0.2)
    min_buyers = _env("SN_MIN_BUYERS", 2)
    min_tx_5m = _env("SN_MIN_TX_5M", 8)
    min_buy_sell_ratio = _env("SN_MIN_BUY_SELL_RATIO", 1.0)
    min_liq_usd = _env("SN_MIN_LIQ_USD", 400.0)

    # --- 3) Distribuzione holder / sicurezza ---------------------------------------------------
    max_top10_pct = _env("SN_MAX_TOP10_PCT", 40.0)   # top 10 wallet (bonding curve/LP esclusi)
    max_dev_pct = _env("SN_MAX_DEV_PCT", 10.0)        # il creatore deve detenere meno di questo %
    max_single_wallet_pct = _env("SN_MAX_HOLDER_PCT", 20.0)
    max_top1_buy_pct = _env("SN_MAX_TOP1_BUY_PCT", 30.0)   # quota acquisti del wallet principale
    max_top3_buy_pct = _env("SN_MAX_TOP3_BUY_PCT", 55.0)
    safety_checks = _env("SAFETY_CHECKS", True)      # Mint Authority e Freeze Authority devono essere None
    verify_retries = _env("SN_MAX_VERIFY_RETRIES", 3)
    verify_retry_delay_sec = _env("SN_VERIFY_RETRY_DELAY", 20.0)

    # --- 4) Candele da 15s (anti micro-candele) ------------------------------------------------
    min_candles = _env("SN_MIN_CANDLES", 3)
    min_candle_range_pct = _env("SN_MIN_CANDLE_RANGE_PCT", 0.8)
    min_active_candle_ratio = _env("SN_MIN_ACTIVE_CANDLE_RATIO", 0.5)
    min_total_range_pct = _env("SN_MIN_TOTAL_RANGE_PCT", 6.0)

    trade_silent_sec = _env("SN_TRADE_SILENT_SEC", 120.0)   # nessun buy/sell ricevuto da N s = feed scambi muto
    removed_memory_sec = _env("SN_REMOVED_MEMORY", 1800.0)

    # --- Modalità DexScreener (senza chiave PumpPortal) ----------------------------------------
    dex_mode_min_age_sec = _env("DEX_MODE_MIN_AGE", 15.0)    # primo controllo DexScreener dopo N s di vita
    dex_held_repoll_sec = _env("DEX_HELD_REPOLL", 6.0)       # posizioni aperte: prezzo ogni N s
    min_vol_m5_usd = _env("SN_MIN_VOL_M5_USD", 250.0)        # volume ultimi 5 min (più permissivo)
    min_trend_pct = _env("SN_MIN_TREND_PCT", 0.5)            # variazione prezzo minima
    max_drawdown_pct = _env("SN_DEX_MAX_DRAWDOWN", 35.0)     # già in calo dal picco oltre questo %
    dex_dead_after_min = _env("SN_DEX_DEAD_AFTER_MIN", 4.0)  # senza scambi dopo N min = fuori dalla coda
    dex_dead_min_tx = _env("SN_DEX_DEAD_MIN_TX", 5)
    dex_not_indexed_after_min = _env("SN_DEX_NOT_INDEXED_MIN", 4.0)
    allow_unverified_in_sim = _env("SN_ALLOW_UNVERIFIED_SIM", True)   # se l'RPC non risponde: in simulazione si entra

    # --- 5) Coda "in osservazione" -------------------------------------------------------------
    max_watch = _env("SN_MAX_WATCH", 60)                      # token contemporanei (50-80)
    watch_ttl_min = _env("SN_MAX_AGE_MIN", 15.0)              # TTL: oltre questo età il token esce per sempre
    # Trazione. Un token NON viene giudicato finché non ha avuto una finestra minima di osservazione:
    # almeno momentum_min_window_sec di vita E almeno momentum_min_events scambi (oppure momentum_check_sec
    # di vita, a prescindere dagli scambi). Poi deve mostrare pressione d'acquisto (più buy che sell
    # negli ultimi 10 scambi) E una di queste due cose:
    #   a) variazione >= momentum_min_gain_pct nella finestra, con candele non piatte
    #   b) variazione positiva + attività recente (>= momentum_recent_events scambi negli ultimi
    #      momentum_recent_sec secondi) con più buy che sell
    # (un token già conforme ai requisiti completi min_tx_5m + rapporto acquisti/vendite resta sempre)
    momentum_min_window_sec = _env("SN_MOMENTUM_MIN_WINDOW", 45.0)
    momentum_min_events = _env("SN_MOMENTUM_MIN_EVENTS", 6)
    momentum_check_sec = _env("SN_MOMENTUM_CHECK_SEC", 90.0)
    momentum_window_sec = _env("SN_MOMENTUM_WINDOW_SEC", 300.0)   # finestra della variazione di prezzo (1-5 min)
    momentum_min_gain_pct = _env("SN_MOMENTUM_MIN_GAIN", 1.5)
    momentum_min_range_pct = _env("SN_MOMENTUM_MIN_RANGE", 1.0)   # escursione minima delle candele
    momentum_recent_sec = _env("SN_MOMENTUM_RECENT_SEC", 30.0)
    momentum_recent_events = _env("SN_MOMENTUM_RECENT_EVENTS", 3)
    inactive_sec = _env("SN_INACTIVE_SEC", 30.0)                  # nessuno scambio da N s = token fermo
    new_token_grace_sec = _env("SN_NEW_TOKEN_GRACE", 45.0)        # protegge i token appena nati (solo se serve)

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
# PumpPortal Data API (docs: https://pumpportal.fun/data-api/real-time)
# - wss://pumpportal.fun/api/data  → subscribeNewToken / subscribeMigration (gratis)
# - con ?api-key=...              → anche subscribeTokenTrade / subscribeAccountTrade (a pagamento)
#   richiede wallet collegato con ≥0.02 SOL; costo ~0.01 SOL / 10000 messaggi trade
# Una sola connessione WebSocket per volta (aprire molte connessioni porta a ban temporanei).
PUMPPORTAL_API_KEY = os.getenv("PUMPPORTAL_API_KEY", "").strip()
FEED_STREAM = bool(PUMPPORTAL_API_KEY)
WS_URL = "wss://pumpportal.fun/api/data" + (f"?api-key={PUMPPORTAL_API_KEY}" if FEED_STREAM else "")
SNIPE_LIVE = os.getenv("SNIPE_LIVE", "false").lower() == "true"
SNIPE_COST_PCT = float(os.getenv("SNIPE_COST_PCT", "4"))
SNIPE_PCT_PER_TRADE = float(os.getenv("SNIPE_PCT_PER_TRADE", "10"))  # base ~10%
SN_MCAP_MIN = CONFIG.mcap_min_usd
SN_MCAP_MAX = CONFIG.mcap_max_usd
SN_MIN_AGE_MIN = CONFIG.min_age_min
SN_MAX_AGE_MIN = CONFIG.watch_ttl_min
SN_MIN_BUYERS = CONFIG.min_buyers
SN_MAX_HOLDER_PCT = CONFIG.max_single_wallet_pct
SN_MAX_DRAWDOWN_PCT = float(os.getenv("SN_MAX_DRAWDOWN_PCT", "65"))
SN_STOP_PCT = float(os.getenv("SN_STOP_PCT", "30"))            # stop solo a -30%
SN_TRAIL_PCT = float(os.getenv("SN_TRAIL_PCT", "25"))           # trailing dopo TP1
SN_MAX_HOLD_MIN = float(os.getenv("SN_MAX_HOLD_MIN", "20"))
SN_STALE_MIN = float(os.getenv("SN_STALE_MIN", "3.5"))
SN_EXIT_MCAP_USD = float(os.getenv("SN_EXIT_MCAP_USD", "0"))
SN_MIN_MOMENTUM_PCT = float(os.getenv("SN_MIN_MOMENTUM_PCT", "-90"))
SN_HANDOFF_MCAP_USD = float(os.getenv("SN_HANDOFF_MCAP_USD", "55000"))
SN_MAX_POSITIONS = 10
SN_MAX_WATCH = CONFIG.max_watch
# TP a scalini: metà a +40%, resto punta a +100% (fallback ~+60% se si ferma)
SN_STD_TP1_PCT = float(os.getenv("SN_STD_TP1_PCT", "40"))
SN_STD_TP1_SELL_PCT = float(os.getenv("SN_STD_TP1_SELL_PCT", "50"))   # vende SOLO metà
SN_STD_TP2_PCT = float(os.getenv("SN_STD_TP2_PCT", "100"))
SN_STD_TP2_SELL_PCT = float(os.getenv("SN_STD_TP2_SELL_PCT", "100"))
SN_STRONG_TP1_PCT = float(os.getenv("SN_STRONG_TP1_PCT", "40"))
SN_STRONG_TP1_SELL_PCT = float(os.getenv("SN_STRONG_TP1_SELL_PCT", "50"))
SN_STRONG_TP2_PCT = float(os.getenv("SN_STRONG_TP2_PCT", "100"))
SN_STRONG_TP2_SELL_PCT = float(os.getenv("SN_STRONG_TP2_SELL_PCT", "100"))
# Fallback: se dopo TP1 resta ~+50% senza salire verso 100%, incassa (anti-rug)
SN_MID_TP_PCT = float(os.getenv("SN_MID_TP_PCT", "50"))
SN_MID_TP_HOLD_MIN = float(os.getenv("SN_MID_TP_HOLD_MIN", "5"))
# Uscita anticipata: meno aggressiva, non vende in piccolo rosso
SN_EARLY_ARM_PCT = float(os.getenv("SN_EARLY_ARM_PCT", "25"))
SN_EARLY_TRAIL_PCT = float(os.getenv("SN_EARLY_TRAIL_PCT", "22"))
SN_WEAK_REV_PCT = float(os.getenv("SN_WEAK_REV_PCT", "15"))
SN_WEAK_LIQ_DROP_PCT = float(os.getenv("SN_WEAK_LIQ_DROP_PCT", "40"))
SN_SLOW_MIN_GAIN_PCT = float(os.getenv("SN_SLOW_MIN_GAIN_PCT", "55"))  # solo dopo ~mid TP
SN_SLOW_MIN_HOLD_MIN = float(os.getenv("SN_SLOW_MIN_HOLD_MIN", "8"))
SN_SLOW_SPEED_PCT_MIN = float(os.getenv("SN_SLOW_SPEED_PCT_MIN", "2"))
SN_SLOW_STALE_SEC = float(os.getenv("SN_SLOW_STALE_SEC", "180"))
# NON vendere in perdita prima dello stop -30%
SN_BLEED_MIN = float(os.getenv("SN_BLEED_MIN", "999"))          # di fatto disattivato
SN_BLEED_LOSS_PCT = float(os.getenv("SN_BLEED_LOSS_PCT", "30"))
# Filtro anti micro-candele (candele da 15s)
SN_MIN_CANDLE_RANGE_PCT = CONFIG.min_candle_range_pct
SN_MIN_ACTIVE_CANDLE_RATIO = CONFIG.min_active_candle_ratio
SN_MIN_TOTAL_RANGE_PCT = CONFIG.min_total_range_pct
SN_MAX_VERIFY_RETRIES = CONFIG.verify_retries
SN_BE_ARM_PCT = float(os.getenv("SN_BE_ARM_PCT", "40"))  # BE solo dopo TP1
BE_LOCK_PCT = float(os.getenv("BE_LOCK_PCT", "5"))
SN_FASTDROP_PCT = float(os.getenv("SN_FASTDROP_PCT", "18"))  # meno sensibile ai micro-cali
SN_WHALE_SELL_PCT = float(os.getenv("SN_WHALE_SELL_PCT", "5.5"))
SN_MAX_DEV_PCT = CONFIG.max_dev_pct
SN_MAX_TOP1_BUY_PCT = CONFIG.max_top1_buy_pct
SN_MAX_TOP3_BUY_PCT = CONFIG.max_top3_buy_pct
SN_MAX_TOP3_TRADES_PCT = float(os.getenv("SN_MAX_TOP3_TRADES_PCT", "45"))
SN_MAX_CLUSTER = int(os.getenv("SN_MAX_CLUSTER", "4"))
SN_MAX_CREATOR_LAUNCHES = int(os.getenv("SN_MAX_CREATOR_LAUNCHES", "3"))
SN_MIN_LIQ_USD = CONFIG.min_liq_usd
SN_MAX_MCAP_LIQ = float(os.getenv("SN_MAX_MCAP_LIQ", "38"))
SN_MAX_PRICE_DIV_PCT = float(os.getenv("SN_MAX_PRICE_DIV_PCT", "25"))
SN_MAX_POS_LIQ_PCT = float(os.getenv("SN_MAX_POS_LIQ_PCT", "3.0"))
SN_MIN_RECENT_BUYS = int(os.getenv("SN_MIN_RECENT_BUYS", "2"))
SN_MAX_SPIKE_PCT = float(os.getenv("SN_MAX_SPIKE_PCT", "220"))
SAFETY_CHECKS = CONFIG.safety_checks

SN_MIN_CANDLES = CONFIG.min_candles
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
WS_STALE_SEC = float(os.getenv("WS_STALE_SEC", "90"))
RPC_DISCOVER_LIMIT = int(os.getenv("RPC_DISCOVER_LIMIT", "10"))
DEX_BACKOFF = {"until": 0.0, "streak": 0}  # cooldown condiviso: un 429 ferma TUTTE le chiamate DexScreener
DEX_CACHE_TTL = CONFIG.dex_cache_ttl_sec
DEX_CACHE: Dict[str, Tuple[float, Optional[dict]]] = {}
DEX_NEG_TTL = CONFIG.dex_neg_ttl_sec
DEX_MIN_INTERVAL = CONFIG.dex_min_interval_sec
SN_DEX_REPOLL_SEC = CONFIG.dex_repoll_sec
SN_DEX_MAX_PER_CYCLE = CONFIG.dex_max_per_cycle
DEX_QUEUE = asyncio.Lock()
DEX_LAST = {"t": 0.0}
DEX_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
               "Accept": "application/json"}
DEX_SEARCH_LAST = {"t": 0.0}
REMOVED: Dict[str, float] = {}   # token già tolti dalla coda in questa sessione: non si riconteggiano né si riaggiungono
DEX_PENDING: Dict[str, "asyncio.Future"] = {}   # mint -> future condivisa (richiesta in coda o in volo)
DEX_BATCH: List[str] = []                       # mint in attesa del prossimo batch
DEX_FLUSHER: Dict[str, Any] = {"task": None}
DEX_FAILED = object()
RPC_SEEN_SIGS: Set[str] = set()
RPC_BACKOFF_BASE = float(os.getenv("RPC_BACKOFF_BASE", "2"))  # pausa minima dopo un HTTP 429
RPC_BACKOFF_MAX = float(os.getenv("RPC_BACKOFF_MAX", "20"))  # pausa massima dopo 429 ripetuti
RPC_BACKOFF = {"delay": RPC_BACKOFF_BASE, "until": 0.0, "count429": 0}  # stato condiviso da tutte le chiamate RPC_URL
PENDING_SUB: List[str] = []
SUBSCRIBED: Set[str] = set()  # mint già sottoscritti su questa connessione (anti-duplicati)

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
        for p in (S.get("positions") or {}).values():
            if isinstance(p, dict) and not p.get("entry_price"):
                ep = entry_price_of(p)
                if ep:
                    p["entry_price"] = ep
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

async def notify(app, text: str, parse_mode: Optional[str] = None, reply_markup=None) -> None:
    try:
        await app.bot.send_message(ADMIN_ID, text, disable_web_page_preview=True,
                                   parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        log.warning("notify: %s", e)
        if parse_mode:
            try:
                await app.bot.send_message(ADMIN_ID, re.sub(r"<[^>]+>", "", text), disable_web_page_preview=True)
            except Exception as e2:
                log.warning("notify (plain): %s", e2)

# ---------------------------------------------------------------------------
# Validazione mint / link / formattazione notifiche
# ---------------------------------------------------------------------------
NOT_FOUND_MARKERS = (
    "could not find", "invalid param", "invalid mint", "account not found", "not found",
    "does not exist", "wrongsize", "invalid public key", "invalid base58", "non trovato",
)

def is_valid_mint(mint) -> bool:
    try:
        return bool(mint) and len(bytes(Pubkey.from_string(str(mint)))) == 32
    except Exception:
        return False

def is_not_found_error(err) -> bool:
    t = str(err).lower()
    return any(k in t for k in NOT_FOUND_MARKERS)

def log_skipped_not_found(mint, detail: str = "") -> None:
    log.warning("SKIPPED: Token not found / invalid mint (%s) %s", str(mint)[:12], detail[:100])

def coin_link(mint) -> str:
    """Link pump.fun solo se la mint è un indirizzo valido; altrimenti mostra il solo CA."""
    return f"https://pump.fun/coin/{mint}" if is_valid_mint(mint) else f"CA: {mint}"

def trade_buttons(mint, verified: bool):
    """Pulsanti chart/explorer solo se mint valida e token verificato (visto su pool/RPC)."""
    if not (verified and is_valid_mint(mint)):
        return None
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Pump.fun", url=f"https://pump.fun/coin/{mint}"),
        InlineKeyboardButton("DexScreener", url=f"https://dexscreener.com/solana/{mint}"),
        InlineKeyboardButton("Solscan", url=f"https://solscan.io/token/{mint}"),
    ]])

def entry_price_of(pos: dict) -> Optional[float]:
    v = pos.get("entry_price") or pos.get("entry")
    if v:
        return float(v)
    return entry_mid(pos, SIM_COST_PCT)

def fmt_px(p, unit: str, mcap_mult: Optional[float] = None) -> str:
    if not p:
        return "n/d"
    txt = (f"{p:.12f}" if p < 0.01 else f"{p:.6f}").rstrip("0").rstrip(".") + f" {unit}"
    if mcap_mult:
        txt += f" (mcap ${p * mcap_mult:,.0f})"
    return txt

def ca_line(mint) -> str:
    return f"CA: <code>{hesc(str(mint))}</code>"

async def notify_buy(app, mint, sym, ok, amount, price, qty, unit, mcap_mult, size_txt, extra, info, verified=True):
    lines = [
        f"{'🟢' if ok else '🔴'} BUY{'' if ok else ' FALLITO'} — {hesc(str(sym or mint[:6]))}",
        ca_line(mint),
        f"Entry Price: {hesc(fmt_px(price, unit, mcap_mult))}",
        f"Size: {hesc(size_txt)}",
        f"Quantità: {qty:,.0f} token (stima)" if qty else "Quantità: n/d",
    ] + [hesc(x) for x in (extra or [])] + [hesc(str(info))]
    await notify(app, "\n".join(lines), parse_mode="HTML", reply_markup=trade_buttons(mint, verified))

async def notify_sell(app, mint, pos, ok, pct, reason, price_now, pnl, cs, info, unit, mcap_mult):
    closed = pct >= 100 or pos.get("left", 0) < 0.02
    if reason.startswith("uscita anticipata"):
        kind = "USCITA ANTICIPATA"
    else:
        kind = "TOTALE" if closed else "PARZIALE"
    if pnl is None:
        pnl_txt = "PnL: non calcolabile"
    elif cs and cs > 0:
        pnl_txt = f"PnL: {pnl / cs * 100:+.1f}% ({pnl:+.4f} SOL)"
    else:
        pnl_txt = f"PnL: {pnl:+.4f} SOL"
    sym = pos.get("sym") or mint[:6]
    lines = [
        f"{'🟢' if ok else '🔴'} SELL {pct}% ({kind}){'' if ok else ' FALLITA'} — {hesc(str(sym))}",
        ca_line(mint),
        f"Entry Price: {hesc(fmt_px(entry_price_of(pos), unit, mcap_mult))}",
        f"Exit Price: {hesc(fmt_px(price_now, unit, mcap_mult))}",
        pnl_txt,
        f"Motivo: {hesc(reason)}",
        hesc(str(info)),
    ]
    await notify(app, "\n".join(lines), parse_mode="HTML", reply_markup=trade_buttons(mint, True))

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
    r = await dex_get(client, f"https://api.dexscreener.com/latest/dex/tokens/{mint}")
    if r is None:
        raise RuntimeError("DexScreener rate limit (HTTP 429) dopo i retry")
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
    if not is_valid_mint(mint):
        log_skipped_not_found(mint, "mint non valida")
        return False, "token non trovato / mint non valida", None
    try:
        m = await fetch_metrics(client, mint)
        if m is None:
            await asyncio.sleep(4)
            m = await fetch_metrics(client, mint)
    except Exception as e:
        if is_not_found_error(e) or (isinstance(e, httpx.HTTPStatusError) and e.response.status_code in (400, 404)):
            log_skipped_not_found(mint, str(e))
            return False, "token non trovato / mint non valida", None
        return False, f"errore dati di mercato: {e}", None
    if m is None:
        log_skipped_not_found(mint, "nessuna pool indicizzata")
        return False, "token non trovato (nessuna pool indicizzata)", None

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
    pnl = cs = None
    if ok:
        pos["exit_reason"] = reason
        pnl, cs = settle(mint, pos, pct, price_now)
        pos["left"] *= 1 - pct / 100
        if pct >= 100 or pos["left"] < 0.02:
            del S["positions"][mint]
        S["trades"] += 1
        save_state()
    await notify_sell(app, mint, pos, ok, pct, reason, price_now, pnl, cs, info, "USD", None)

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
    link = coin_link(mint)
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
                    oq, oe = p.get("qty", 0.0) * p["left"], entry_price_of(p)
                    if price and oe and oq + qty_new > 0:
                        p["entry_price"] = (oq * oe + qty_new * price) / (oq + qty_new)
                    elif price and not oe:
                        p["entry_price"] = price
                    p["qty"] = p.get("qty", 0.0) * p["left"] + qty_new
                    p["cost"] = p["cost"] * p["left"] + amount
                    p["left"] = 1.0
                    p["last_price"] = price or p.get("last_price")
                    p.setdefault("by", {})
                    p["by"][trader] = p["by"].get(trader, 0.0) + amount
                else:
                    S["positions"][mint] = {
                        "cost": amount, "left": 1.0, "qty": qty_new,
                        "t": time.time(), "last_price": price, "entry_price": price,
                        "sym": (m or {}).get("symbol"), "by": {trader: amount}
                    }
                S["trades"] += 1
                save_state()
            await notify_buy(
                app, mint, (m or {}).get("symbol"), ok, amount, price, qty_new if ok and price else None,
                "USD", None, f"{amount} SOL (copia di {who})", [f"✓ {verdict}"], info, verified=bool(m),
            )
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
    data = await dex_fetch_many(client, list(S["positions"]), fresh=set(S["positions"]))
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
        "ws_err": "", "ws_auth_err": "", "ws_connected": False, "reconnects": 0,
        "reasons": {}, "types": {}, "subs": 0, "subs_active": 0, "send_err": "",
        "msgs": 0, "last_msg": "", "dex_polls": 0, "dex_hits": 0, "incoherent": 0,
        "fallback_on": False, "fallback_cycles": 0, "dex_429": 0,
        "discard": {}, "dex_reqs": 0, "dex_cache_hits": 0,
        "trade_events": 0, "buy_events": 0, "sell_events": 0,
        "last_trade_t": 0.0, "last_buy_t": 0.0, "last_sell_t": 0.0,
        "start_t": time.time(),
        "ws_resets": 0, "last_resub": 0.0, "hk_err": "",
    }.items():
        d.setdefault(k, v)
    return d

async def ws_send(payload):
    method = payload.get("method") or ""
    if method in ("subscribeTokenTrade", "unsubscribeTokenTrade") and not FEED_STREAM:
        return  # trade stream a pagamento: senza API key non si invia
    if WS is None:
        sn()["send_err"] = "connessione non pronta"
        return
    try:
        await WS.send(json.dumps(payload))
        keys = payload.get("keys") or []
        if method == "subscribeTokenTrade":
            new_keys = [k for k in keys if k not in SUBSCRIBED]
            for k in new_keys:
                SUBSCRIBED.add(k)
            sn()["subs"] += len(new_keys)
            sn()["subs_active"] = len(SUBSCRIBED)
        elif method == "unsubscribeTokenTrade":
            for k in keys:
                SUBSCRIBED.discard(k)
            sn()["subs_active"] = len(SUBSCRIBED)
        sn()["send_err"] = ""
    except Exception as e:
        log.warning("ws_send: %s", e)
        sn()["send_err"] = str(e)[:150]

async def flush_subs():
    """Invia subscribeTokenTrade solo per mint non ancora sottoscritti (anti-duplicati)."""
    global PENDING_SUB
    if not FEED_STREAM:
        PENDING_SUB = []
        return
    while PENDING_SUB:
        batch = []
        while PENDING_SUB and len(batch) < 50:
            m = PENDING_SUB.pop(0)
            if m not in SUBSCRIBED and m not in batch:
                batch.append(m)
        if batch:
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
        "src": "stream", "px": [(now, pr)] if pr else [], "outl": 0, "mom_hist": [(now, mc)],
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

def candle_quality(w) -> Optional[str]:
    """Scarta micro-candele / grafici piatti / scambi non continui (candele da 15s)."""
    cs = w["candles"][-10:]
    if len(cs) < SN_MIN_CANDLES:
        return None  # già gestito da structure_features
    rngs = [(c[2] - c[3]) / c[3] * 100 for c in cs if c[3] > 0]
    if not rngs:
        return "candele non valide"
    active = sum(1 for r in rngs if r >= SN_MIN_CANDLE_RANGE_PCT)
    if active / len(rngs) < SN_MIN_ACTIVE_CANDLE_RATIO:
        return "candele minuscole / grafico piatto"
    lo = min(c[3] for c in cs if c[3] > 0)
    total = (max(c[2] for c in cs) / lo - 1) * 100
    if total < SN_MIN_TOTAL_RANGE_PCT:
        return "escursione di prezzo insufficiente"
    gaps = sum(1 for a, b in zip(cs, cs[1:]) if b[0] - a[0] > 45)
    if gaps > len(cs) // 2:
        return "scambi non continui (token fermo)"
    return None

def weakness_signal(w, pos, price, now) -> Optional[str]:
    """Segnali di cedimento prima del target alto: trailing, inversione candele, volume, liquidità, supporto."""
    if now - pos["t"] < 20:
        return None
    entry = entry_price_of(pos) or price
    peak = pos.get("peak") or price
    gain = (price / entry - 1) * 100
    held = (now - pos["t"]) / 60
    if gain >= SN_SLOW_MIN_GAIN_PCT and held >= SN_SLOW_MIN_HOLD_MIN:
        speed = gain / max(held, 1.0)
        if speed < SN_SLOW_SPEED_PCT_MIN or now - pos.get("peak_t", pos["t"]) >= SN_SLOW_STALE_SEC:
            return f"salita lenta, incasso il profitto (+{gain:.0f}% in {held:.0f} min)"
    if held >= SN_BLEED_MIN and gain <= -SN_BLEED_LOSS_PCT:
        return f"in perdita da {held:.0f} min senza ripresa ({gain:.0f}%)"
    if peak > 0 and (peak / entry - 1) * 100 >= SN_EARLY_ARM_PCT:
        dd = (price / peak - 1) * 100
        if dd <= -SN_EARLY_TRAIL_PCT:
            return f"trailing stop ({dd:.0f}% dal picco)"
    cs = w["candles"]
    if len(cs) >= 4:
        last = cs[-3:]
        if last[0][1] > 0 and all(c[4] < c[1] for c in last):
            rev = (last[-1][4] / last[0][1] - 1) * 100
            if rev <= -SN_WEAK_REV_PCT:
                return f"inversione netta delle candele ({rev:.0f}%)"
    liq0 = pos.get("liq0")
    if liq0:
        liq = liquidity_usd(w)
        if liq > 0 and liq < liq0 * (1 - SN_WEAK_LIQ_DROP_PCT / 100):
            return f"perdita di liquidità (${liq:,.0f} da ${liq0:,.0f})"
    tl = w["tlog"]
    recent = sum(1 for t, _ in tl if now - t <= 30)
    prior = sum(1 for t, _ in tl if 30 < now - t <= 150)
    if prior / 4 >= 3 and recent == 0 and peak > 0 and price <= peak * 0.97:
        return "perdita di volume (scambi fermi)"
    if len(cs) >= 8:
        sup = min(c[3] for c in cs[-10:-2])
        if sup > 0 and cs[-1][4] < sup * 0.97:
            return "rottura del supporto"
    return None

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
    if w.get("src") == "dexscreener" and w.get("last_price"):
        return w["last_price"]   # dati DexScreener già filtrati e campionati ogni pochi secondi
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
    top10 = sum(v for _, v in hold[:10]) / TOTAL_SUPPLY * 100
    dev = w["holders"].get(w["creator"], 0) / TOTAL_SUPPLY * 100
    if top1 > SN_MAX_HOLDER_PCT:
        return f"un wallet detiene il {top1:.0f}% della supply"
    if top10 > CONFIG.max_top10_pct:
        return f"i primi 10 wallet detengono il {top10:.0f}%"
    if dev >= SN_MAX_DEV_PCT:
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
        if is_not_found_error(e):
            return False, f"mint non trovato ({str(e)[:60]})"
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
        if is_not_found_error(e):
            return None, f"mint non trovato ({str(e)[:60]})"
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
    if res["top10"] > CONFIG.max_top10_pct:
        return False, f"i primi 10 wallet detengono il {res['top10']:.0f}%", info
    if res["dev"] >= SN_MAX_DEV_PCT:
        return False, f"il creatore detiene il {res['dev']:.0f}%", info
    return True, "", info

def liquidity_usd(w):
    if w.get("src") == "dexscreener" and w.get("liq_usd"):
        return w["liq_usd"]
    return max(0.0, w.get("vsol", 0.0) - 30) * SOL_USD

async def deep_checks(client, mint, w):
    ok, why, info = await safety_checks(client, mint, w.get("creator"))
    if not ok:
        soft = ("non verificabil" in why) or ("wallet detengono" in why) or ("un wallet detiene" in why)
        return False, why, soft, info
    liq = liquidity_usd(w)
    info["liquidita_usd"] = round(liq)
    if liq < SN_MIN_LIQ_USD:
        return False, f"liquidità bassa (${liq:,.0f})", True, info   # cresce col token: si riprova
    mcap = w["mcap_sol"] * SOL_USD
    info["mcap_su_liquidita"] = round(mcap / liq, 1) if liq > 0 else None
    if w.get("dex_id") not in (None, "pumpfun") and liq > 0 and mcap / liq > SN_MAX_MCAP_LIQ:
        return False, "prezzo e liquidità incoerenti", False, info
    data = await dex_fetch_many(client, [mint])
    p = data.get(mint)
    if p and w.get("src") != "dexscreener":
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
    mh = w.setdefault("mom_hist", [])
    mh.append((now, w["mcap_sol"]))
    w["mom_hist"] = [(t, m) for t, m in mh if now - t <= CONFIG.momentum_window_sec]

def momentum_pct(w):
    now = time.time()
    older = [m for t, m in w["hist"] if now - t >= 45]
    base = older[-1] if older else (w["hist"][0][1] if w["hist"] else 0)
    return (w["mcap_sol"] / base - 1) * 100 if base > 0 else 0.0

async def update_sol_price(client):
    global SOL_USD
    try:
        r = await dex_get(client, f"https://api.dexscreener.com/latest/dex/tokens/{WSOL}")
        if r is None:
            return
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

def trade_feed_silent() -> bool:
    """True se sono state inviate sottoscrizioni agli scambi ma non arriva NESSUN buy/sell da trade_silent_sec."""
    st = sn()
    if st["subs"] < 20:
        return False
    return time.time() - max(st["last_trade_t"], st["start_t"]) > CONFIG.trade_silent_sec

async def trade_watchdog() -> None:
    """Feed scambi muto: riapre la connessione WebSocket (ripete tutte le sottoscrizioni), al massimo ogni 5 minuti."""
    st = sn()
    now = time.time()
    if WS is not None and trade_feed_silent() and now - st["last_resub"] > 300:
        st["last_resub"] = now
        st["ws_resets"] += 1
        log.warning("Nessun buy/sell ricevuto da %.0fs con %d sottoscrizioni: riapro il WebSocket",
                    now - max(st["last_trade_t"], st["start_t"]), st["subs"])
        try:
            await WS.close()
        except Exception as e:
            log.warning("WS.close: %s", e)

def dex_mode() -> bool:
    """True quando i dati di mercato arrivano da DexScreener (nessuna chiave PumpPortal o feed scambi muto)."""
    return (not FEED_STREAM) or trade_feed_silent()

def dex_strong(w) -> bool:
    """Setup ad alto potenziale in modalità DexScreener: forte trend, molti scambi e più acquisti che vendite."""
    b5, s5 = w["buys"], w["sells"]
    return (w.get("pc_m5", 0) >= 8 and b5 + s5 >= 30 and b5 / max(1, s5) >= 1.5
            and w["vol"] * SOL_USD >= 1000)

def tx_stats_5m(w):
    """(acquisti, vendite) degli ultimi 5 minuti: dal flusso WebSocket/RPC, altrimenti dal m5 di DexScreener."""
    if w["stream_trades"]:
        now = time.time()
        b = sum(1 for t, x in w["tlog"] if now - t <= 300 and x == "buy")
        s_ = sum(1 for t, x in w["tlog"] if now - t <= 300 and x == "sell")
        return b, s_
    return w["buys"], w["sells"]

def window_gain_pct(w, now) -> float:
    """Variazione % del market cap nella finestra di trazione (default 5 minuti)."""
    h = [(t, m) for t, m in w.get("mom_hist", w["hist"]) if now - t <= CONFIG.momentum_window_sec]
    if not h or h[0][1] <= 0:
        return 0.0
    return (w["mcap_sol"] / h[0][1] - 1) * 100

def candle_excursion_pct(w) -> float:
    cs = w["candles"][-10:]
    lows = [c[3] for c in cs if c[3] > 0]
    if len(cs) < 2 or not lows:
        return 0.0
    return (max(c[2] for c in cs) / min(lows) - 1) * 100

def momentum_snapshot(w, now) -> dict:
    """Valori letti per giudicare (e per spiegare nel log) il momentum di un token."""
    b5, s5 = tx_stats_5m(w)
    tl = w["tlog"]
    last = [x for _, x in tl[-10:]]
    rec = [x for t, x in tl if now - t <= CONFIG.momentum_recent_sec]
    return {
        "gain": window_gain_pct(w, now),
        "window": min(now - w["t0"], CONFIG.momentum_window_sec),
        "events": w["buys"] + w["sells"],
        "b5": b5, "s5": s5,
        "b10": sum(1 for x in last if x == "buy") if last else b5,
        "s10": sum(1 for x in last if x == "sell") if last else s5,
        "ev_recent": len(rec),
        "b_recent": sum(1 for x in rec if x == "buy"),
        "s_recent": sum(1 for x in rec if x == "sell"),
        "range": candle_excursion_pct(w),
    }

def ready_to_judge(w, now) -> bool:
    """Finestra minima di osservazione prima di giudicare il momentum."""
    age = now - w["t0"]
    if age < CONFIG.momentum_min_window_sec:
        return False
    if w["stream_trades"] == 0 and trade_feed_silent() and not w.get("dex_t"):
        return False   # feed scambi muto: si aspettano i dati DexScreener prima di giudicare
    return (w["buys"] + w["sells"]) >= CONFIG.momentum_min_events or age >= CONFIG.momentum_check_sec

def has_traction(w, now):
    """(ok, snapshot). Vedi CONFIG: pressione d'acquisto + (variazione >= soglia con candele non piatte
    oppure variazione positiva con attività recente a favore dei buy)."""
    sn_ = momentum_snapshot(w, now)
    if sn_["b5"] + sn_["s5"] >= CONFIG.min_tx_5m and sn_["b5"] / max(1, sn_["s5"]) >= CONFIG.min_buy_sell_ratio:
        return True, sn_
    if sn_["b10"] <= sn_["s10"]:
        return False, sn_
    if sn_["gain"] >= CONFIG.momentum_min_gain_pct and sn_["range"] >= CONFIG.momentum_min_range_pct:
        return True, sn_
    if sn_["gain"] > 0 and sn_["ev_recent"] >= CONFIG.momentum_recent_events and sn_["b_recent"] > sn_["s_recent"]:
        return True, sn_
    return False, sn_

def momentum_log(k, sn_) -> None:
    log.info(
        "no_momentum %s: delta=%+.1f%% (soglia +%.1f%%) finestra=%.0fs scambi=%d | 5m acq/vend=%d/%d | "
        "ultimi10 acq/vend=%d/%d | ultimi%.0fs=%d (acq %d/vend %d) | range candele=%.1f%% (min %.1f%%)",
        k[:8], sn_["gain"], CONFIG.momentum_min_gain_pct, sn_["window"], sn_["events"], sn_["b5"], sn_["s5"],
        sn_["b10"], sn_["s10"], CONFIG.momentum_recent_sec, sn_["ev_recent"], sn_["b_recent"], sn_["s_recent"],
        sn_["range"], CONFIG.momentum_min_range_pct,
    )

def prune_candidates(now):
    """Token da togliere dalla coda: bloccati, senza trazione (dopo la finestra minima), oppure oltre il TTL."""
    out = []
    for k, w in WATCH.items():
        if k in S["positions"] or w.get("evaluating"):
            continue
        if w["blocked"]:
            out.append((k, w["why"]))
        elif (now - w["t0"]) / 60 > CONFIG.watch_ttl_min:
            out.append((k, w["why"]))     # TTL scaduto: conta il motivo che l'ha frenato
        elif w["stream_trades"] == 0 and dex_mode():
            age_s = now - w["t0"]
            if w.get("src") != "dexscreener":
                if age_s >= CONFIG.dex_not_indexed_after_min * 60 and w.get("dex_t"):
                    out.append((k, "nessuna pool trovata su DexScreener"))
            elif (age_s >= CONFIG.dex_dead_after_min * 60 and (w["buys"] + w["sells"]) < CONFIG.dex_dead_min_tx
                  and w["mcap_sol"] * SOL_USD < CONFIG.mcap_min_usd):
                out.append((k, "nessun momentum (pochi scambi e market cap fermo)"))
        elif ready_to_judge(w, now):
            ok, snap = has_traction(w, now)
            if not ok:
                momentum_log(k, snap)
                out.append((k, "nessun momentum"))
    return out

async def make_room() -> bool:
    """Coda piena: sostituisce SEMPRE un token (mai scarta il nuovo arrivato). Ordine di sfratto:
    1) bloccati, 2) fermi (nessuno scambio da inactive_sec) dal più vecchio, 3) meno attivi, 4) i più vecchi
    anche fra i giovani. Le posizioni aperte e i token in valutazione non si toccano."""
    cap = CONFIG.max_watch
    if len(WATCH) < cap:
        return True
    now = time.time()
    cands = []
    for k, w in WATCH.items():
        if k in S["positions"] or w.get("evaluating"):
            continue
        b, s_ = tx_stats_5m(w)
        young = now - w["t0"] < CONFIG.new_token_grace_sec
        if w["blocked"]:
            cands.append((0, 0.0, w["t0"], k))
        elif now - w["last_t"] >= CONFIG.inactive_sec and now - w["t0"] >= CONFIG.momentum_min_window_sec:
            cands.append((1, 0.0, w["t0"], k))
        elif not young:
            cands.append((2, b + s_ + w["mcap_sol"] * SOL_USD / 1000, w["t0"], k))
        else:
            cands.append((3, 0.0, w["t0"], k))
    cands.sort()
    need = len(WATCH) - cap + 1
    gone = []
    reasons = sn()["reasons"]
    for group, _, _, k in cands[:need]:
        w_ = WATCH.pop(k)
        gone.append(k)
        reasons["coda piena (sostituito)"] = reasons.get("coda piena (sostituito)", 0) + 1
        REMOVED[k] = now
        count_discard(categorize_reason(w_["why"]) if group == 0 else ("no_momentum" if group == 1 else "queue_full"))
    if gone:
        await ws_send({"method": "unsubscribeTokenTrade", "keys": gone})
    return len(WATCH) < cap

def entry_check(w):
    """Sceglie le regole: dati DexScreener (nessun flusso scambi) oppure flusso scambi PumpPortal."""
    if w["stream_trades"] == 0 and dex_mode():
        return entry_check_dex(w)
    return entry_check_stream(w)

def entry_check_dex(w):
    """Regole con dati DexScreener: token < 15 min, market cap con margine per +40%, scambi e acquisti in
    crescita. (Mint/freeze authority, holder e dev si controllano dopo, solo sui candidati.)"""
    age = (time.time() - w["t0"]) / 60
    if age > SN_MAX_AGE_MIN:
        return "troppo vecchio"
    if w.get("src") != "dexscreener":
        return "in attesa dati DexScreener"
    mcap = w["mcap_sol"] * SOL_USD
    if mcap < SN_MCAP_MIN:
        return "market cap sotto soglia"
    if mcap > SN_MCAP_MAX:
        return "market cap sopra soglia (poco margine per +40%)"
    b5, s5 = w["buys"], w["sells"]
    if b5 + s5 < CONFIG.min_tx_5m:
        return f"poche transazioni ({b5 + s5}/{CONFIG.min_tx_5m:.0f} in 5 min)"
    if b5 / max(1, s5) < CONFIG.min_buy_sell_ratio:
        return f"rapporto acquisti/vendite basso ({b5}/{s5})"
    if w["vol"] * SOL_USD < CONFIG.min_vol_m5_usd:
        return f"volume basso (${w['vol'] * SOL_USD:,.0f} in 5 min)"
    pc5, mom = w.get("pc_m5", 0.0), momentum_pct(w)
    if abs(pc5) < 0.3 and abs(mom) < 0.3 and age > 2:
        return "grafico piatto"
    if max(pc5, mom) < CONFIG.min_trend_pct:
        return "senza slancio"
    if mom < -3.0:
        return "in calo adesso"
    if pc5 > SN_MAX_SPIKE_PCT:
        return "salita troppo verticale"
    if w["peak_sol"] > 0 and (w["peak_sol"] - w["mcap_sol"]) / w["peak_sol"] * 100 > CONFIG.max_drawdown_pct:
        return "già in forte calo dal picco"
    if not w["last_price"]:
        return "prezzo non disponibile"
    return None

def entry_check_stream(w):
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
    b5, s5 = tx_stats_5m(w)
    if b5 + s5 < CONFIG.min_tx_5m:
        return f"poche transazioni ({b5 + s5}/{CONFIG.min_tx_5m:.0f} in 5 min)"
    if b5 / max(1, s5) < CONFIG.min_buy_sell_ratio:
        return f"rapporto acquisti/vendite basso ({b5}/{s5})"
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
    cq = candle_quality(w)
    if cq:
        return cq
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
    if (not ok and "non verificabil" in why and CONFIG.allow_unverified_in_sim
            and not (SNIPE_LIVE and not DRY_RUN) and w.get("vf_retries", 0) >= SN_MAX_VERIFY_RETRIES):
        ok, info = True, {"verifica_holder": "non riuscita (RPC): ingresso solo in simulazione"}
    if not ok:
        w["why"] = why
        if "non trovato" in why:
            log_skipped_not_found(mint, why)
            w["blocked"] = True
            return
        if retry:
            w["vf_retries"] = w.get("vf_retries", 0) + 1
            if w["vf_retries"] > SN_MAX_VERIFY_RETRIES:
                log.info("SKIPPED %s dopo %d verifiche: %s", mint[:8], SN_MAX_VERIFY_RETRIES, why)
                w["blocked"] = True
            else:
                w["next_chk"] = time.time() + CONFIG.verify_retry_delay_sec
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
    if w["stream_trades"] == 0 and dex_mode():
        is_strong = dex_strong(w)
        organic = 60.0 if is_strong else 50.0
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
                "entry_price": price, "entry_fill": entry, "liq0": liq,
                "tp_stage": 0,
                "size_pct": pct_size,
            }
            S["trades"] += 1
            sn()["entered"] += 1
            save_state()
        else:
            w["blocked"], w["why"] = True, "ordine fallito"
        tot = max(1, w["buys"] + w["sells"])
        strength = "FORTE" if is_strong else "cauto"
        dev_pct = info["dev"] if info.get("dev") is not None else w["holders"].get(w["creator"], 0) / TOTAL_SUPPLY * 100
        nb = len(w["buyers"]) if w["stream_trades"] else int(w["dex_buys"] / 1.5)
        t1, t1s, t2 = ((SN_STRONG_TP1_PCT, SN_STRONG_TP1_SELL_PCT, SN_STRONG_TP2_PCT) if is_strong
                       else (SN_STD_TP1_PCT, SN_STD_TP1_SELL_PCT, SN_STD_TP2_PCT))
        tp_txt = f"+{t1:.0f}% (vendo tutto)" if t1s >= 100 else f"+{t1:.0f}% ({t1s:.0f}%) / +{t2:.0f}% (resto)"
        await notify_buy(
            app, mint, w["symbol"], ok, amount, price, (amount / entry) if ok else None,
            "SOL", TOTAL_SUPPLY * SOL_USD, f"{amount} SOL ({pct_size:.0f}% del budget — {strength})",
            [
                f"mcap ${w['mcap_sol'] * SOL_USD:,.0f}, {nb} compratori, {len(w['candles'])} candele 15s",
                f"{w['buys'] / tot * 100:.0f}% acquisti, età {(time.time() - w['t0']) / 60:.1f} min, Dev {dev_pct:.1f}%",
                f"TP: {tp_txt}",
            ],
            info2,
        )

async def snipe_sell(app, client, mint, pct, reason) -> bool:
    pos = S["positions"].get(mint)
    if not pos:
        return False
    w = WATCH.get(mint) or {}
    rp, lp = (robust_price(w) if w else None), w.get("last_price")
    price_now = min([p for p in (rp, lp) if p], default=pos.get("last_price"))
    ok, info = await snipe_trade(client, "sell", mint, f"{pct}%")
    pnl = cs = None
    if ok:
        pos["exit_reason"] = reason
        pnl, cs = settle(mint, pos, pct, price_now, SNIPE_COST_PCT, TOTAL_SUPPLY * SOL_USD)
        if pnl is not None and cs > 0 and pnl / cs <= -0.6:
            mark_bad_creator(pos.get("creator"))
        pos["left"] *= 1 - pct / 100
        S["trades"] += 1
        if pct >= 100 or pos["left"] < 0.02:
            del S["positions"][mint]
            WATCH.pop(mint, None)
            await ws_send({"method": "unsubscribeTokenTrade", "keys": [mint]})
        save_state()
    await notify_sell(app, mint, pos, ok, pct, reason, price_now, pnl, cs, info, "SOL", TOTAL_SUPPLY * SOL_USD)
    return ok

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
    if price > (pos.get("peak") or 0):
        pos["peak_t"] = now
    pos["peak"] = max(pos.get("peak") or price, price)
    gain = (price / (entry_price_of(pos) or price) - 1) * 100
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
    is_strong = pos.get("is_strong", False)
    stage = int(pos.get("tp_stage", 1 if pos.get("tp_done") else 0))
    if is_strong:
        tp1, tp1_sell, tp2, tp2_sell = SN_STRONG_TP1_PCT, SN_STRONG_TP1_SELL_PCT, SN_STRONG_TP2_PCT, SN_STRONG_TP2_SELL_PCT
    else:
        tp1, tp1_sell, tp2, tp2_sell = SN_STD_TP1_PCT, SN_STD_TP1_SELL_PCT, SN_STD_TP2_PCT, SN_STD_TP2_SELL_PCT
    kind = "forte" if is_strong else "standard"
    weak = weakness_signal(w, pos, price, now) if stage < 2 else None
    reason, pct, new_stage = None, 100, stage
    if w["creator_sold"]:
        reason = "il creatore ha venduto"
    elif w.get("dump_flag"):
        reason = "vendita pesante di un wallet"
    elif drop <= -SN_FASTDROP_PCT:
        reason = f"calo improvviso ({drop:.0f}% in 30 s)"
    elif stage == 0 and gain >= tp1:
        # vende metà a +40%
        reason, pct, new_stage = f"take profit 1 metà (+{gain:.0f}%, target +{tp1:.0f}%)", tp1_sell, 1
        pos["tp1_t"] = now
    elif stage == 1 and gain >= tp2:
        # resto a +100%
        reason, pct, new_stage = f"take profit 2 resto (+{gain:.0f}%, target +{tp2:.0f}%)", tp2_sell, 2
    elif stage == 1 and gain >= SN_MID_TP_PCT and (now - pos.get("tp1_t", pos["t"])) >= SN_MID_TP_HOLD_MIN * 60:
        # non arriva a 100%: se sta ~+60% da un po', incassa il resto
        reason, pct, new_stage = f"take profit intermedio resto (+{gain:.0f}%, target mid +{SN_MID_TP_PCT:.0f}%)", 100, 2
    elif sl_hit:
        reason = f"stop loss ({gain:.0f}%)"
    elif stage >= 1 and pos.get("be") and gain <= BE_LOCK_PCT:
        reason = "protezione BE dopo TP1"
    elif stage >= 1 and weak:
        # debolezza solo sulla metà rimasta dopo TP1
        reason = f"uscita anticipata sul resto — {weak}"
    elif SN_EXIT_MCAP_USD > 0 and mcap_usd >= SN_EXIT_MCAP_USD:
        reason = "market cap obiettivo raggiunto"
    elif now - pos["t"] > SN_MAX_HOLD_MIN * 60:
        reason = "tempo massimo di permanenza"
    elif now - w["last_t"] > SN_STALE_MIN * 60:
        reason = "nessuno scambia più (stall)"
    elif stage >= 1 and (price / pos["peak"] - 1) * 100 <= -SN_TRAIL_PCT:
        reason = f"trailing stop sul resto (guadagno {gain:+.0f}%)"
    if not reason:
        return
    partial = pct < 100
    if partial:
        pos["tp_stage"], pos["tp_done"] = new_stage, True
    ok = await snipe_sell(app, client, mint, pct, reason)
    if partial and not ok:
        pos["tp_stage"], pos["tp_done"] = stage, stage >= 1

def _looks_like_auth_error(text: str) -> bool:
    t = (text or "").lower()
    return any(x in t for x in (
        "unauthorized", "invalid api", "api key", "forbidden", "not authorized",
        "authentication", "auth failed", "invalid key", "missing api",
    ))

async def on_event(app, client, ev):
    mint = ev.get("mint")
    st = sn()
    if not mint:
        st["msgs"] += 1
        msg = json.dumps(ev, ensure_ascii=False)[:200]
        st["last_msg"] = msg
        # messaggi server (errori auth, rate limit, ecc.) senza mint
        if _looks_like_auth_error(msg):
            st["ws_auth_err"] = msg[:150]
            log.warning("PumpPortal possibile errore auth (dettaglio non loggato per sicurezza chiave)")
        return
    if not is_valid_mint(mint):
        log_skipped_not_found(mint, "evento con mint non valida")
        count_discard("token_not_found")
        return
    st["events"] += 1
    st["last_event"] = int(time.time())
    tx = ev.get("txType")
    st["types"][str(tx)] = st["types"].get(str(tx), 0) + 1
    now = time.time()
    if tx == "buy":
        st["trade_events"] += 1
        st["buy_events"] = st.get("buy_events", 0) + 1
        st["last_trade_t"] = now
        st["last_buy_t"] = now
    elif tx == "sell":
        st["trade_events"] += 1
        st["sell_events"] = st.get("sell_events", 0) + 1
        st["last_trade_t"] = now
        st["last_sell_t"] = now
    if tx == "create":
        st["seen"] += 1
        if not await make_room():
            return  # coda piena di token giovani o con posizione: il nuovo token viene ignorato
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
        if not w["blocked"] and FEED_STREAM:
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

DISCARD_CORE = ("flat_chart", "low_tx", "low_volume", "fake_holders", "low_liquidity", "token_not_found", "other")
DISCARD_CATEGORIES = [
    ("token_not_found", ("non trovato", "mint non valida", "nessuna pool")),
    ("waiting_data", ("in attesa dati",)),
    ("no_momentum", ("nessun momentum",)),
    ("queue_full", ("coda piena",)),
    ("flat_chart", ("candele minuscole", "grafico piatto", "escursione", "non continui", "token fermo", "candele non valide", "lateralità sporca")),
    ("fake_holders", ("detiene", "primi 10 wallet", "wallet detengono", "singolo wallet", "3 wallet", "wash trading", "bundle",
                      "sincronizzat", "creatore seriale", "creatore già visto", "distribuzione holder")),
    ("rug_risk", ("authority", "creatore ha venduto", "vendita pesante", "estensioni del token", "rug")),
    ("low_liquidity", ("liquidità bassa", "liquidità insufficiente")),
    ("price_incoherent", ("incoerent", "prezzo cambiato")),
    ("mcap_range", ("market cap",)),
    ("age_range", ("troppo giovane", "troppo vecchio")),
    ("low_volume", ("volume",)),
    ("low_tx", ("poche transazioni", "rapporto acquisti", "nessuno scambio", "pochi compratori", "attività recente debole", "troppe vendite",
                "dati di prezzo insufficienti", "prezzo non disponibile", "senza slancio")),
    ("weak_structure", ("struttura", "verticale", "spike", "calo dal picco", "forte calo")),
    ("positions_full", ("posizioni piene", "budget o posizioni", "budget o liquidità")),
]

def categorize_reason(why: str) -> str:
    t = (why or "").lower()
    for key, words in DISCARD_CATEGORIES:
        if any(x in t for x in words):
            return key
    return "other"

def count_discard(key: str) -> None:
    d = sn().setdefault("discard", {})
    d[key] = d.get(key, 0) + 1

def discard_counts() -> Dict[str, int]:
    """Token scartati per categoria: già usciti dall'osservazione + quelli in osservazione ora."""
    c = dict(sn().get("discard", {}))
    for k, w in WATCH.items():
        if k in S["positions"]:
            continue
        why = w.get("why") or ""
        if "troppo giovane" in why or "in attesa dati" in why:
            continue  # sta solo aspettando, non è uno scarto
        key = categorize_reason(why)
        c[key] = c.get(key, 0) + 1
    return c

def discard_lines(counts: Dict[str, int]) -> List[str]:
    keys = list(DISCARD_CORE) + [k for k in counts if k not in DISCARD_CORE]
    rows = sorted(((k, counts.get(k, 0)) for k in keys if k in DISCARD_CORE or counts.get(k, 0) > 0),
                  key=lambda kv: -kv[1])
    return [f"   {n}× {k}" for k, n in rows]

async def dex_get(client, url, params=None, timeout=15):
    """Unica porta d'accesso a DexScreener. Rate limiter globale (almeno dex_min_interval_sec fra due richieste)
    e cooldown globale di 15-30 s dopo un HTTP 429: durante il cooldown NESSUNA richiesta parte (ritorna None
    e i chiamanti riprovano al ciclo successivo)."""
    async with DEX_QUEUE:
        now = time.time()
        if now < DEX_BACKOFF["until"]:
            return None
        wait = DEX_LAST["t"] + DEX_MIN_INTERVAL - now
        if wait > 0:
            await asyncio.sleep(wait)
        DEX_LAST["t"] = time.time()
        sn()["dex_reqs"] = sn().get("dex_reqs", 0) + 1
        r = await client.get(url, params=params, timeout=timeout, headers=DEX_HEADERS)
        if r.status_code == 429:
            try:
                ra = float(r.headers.get("Retry-After") or 0)
            except ValueError:
                ra = 0.0
            streak = DEX_BACKOFF.get("streak", 0) + 1
            cd = ra or CONFIG.dex_429_cooldown_min_sec * (2 ** (streak - 1))
            cd = min(max(cd, CONFIG.dex_429_cooldown_min_sec), CONFIG.dex_429_cooldown_max_sec)
            DEX_BACKOFF["streak"] = streak
            DEX_BACKOFF["until"] = time.time() + cd
            sn()["dex_429"] = sn().get("dex_429", 0) + 1
            log.warning("DexScreener HTTP 429: cooldown di %.0fs su tutte le chiamate", cd)
            return None
        DEX_BACKOFF["streak"] = 0
        return r

async def _dex_flush(client):
    """Svuota la coda dei mint in batch da 30 (una sola richiesta HTTP per batch, via rate limiter globale)."""
    try:
        await asyncio.sleep(CONFIG.dex_batch_window_sec)   # raccoglie i mint di più chiamanti
        while DEX_BATCH:
            chunk = DEX_BATCH[:30]
            del DEX_BATCH[:30]
            res, ok = {}, False
            try:
                r = await dex_get(client, "https://api.dexscreener.com/latest/dex/tokens/" + ",".join(chunk))
                if r is not None:
                    r.raise_for_status()
                    for p in r.json().get("pairs") or []:
                        addr = (p.get("baseToken") or {}).get("address")
                        if p.get("chainId") != "solana" or addr not in chunk:
                            continue
                        cur = res.get(addr)
                        if not cur or ((p.get("liquidity") or {}).get("usd") or 0) > ((cur.get("liquidity") or {}).get("usd") or 0):
                            res[addr] = p
                    ok = True
            except Exception as e:
                log.warning("dex batch: %s", e)
            tnow = time.time()
            for m in chunk:
                if ok:
                    DEX_CACHE[m] = (tnow, res.get(m))       # chiave = mint; None = nessuna pool trovata
                fut = DEX_PENDING.pop(m, None)
                if fut is not None and not fut.done():
                    fut.set_result(res.get(m) if ok else DEX_FAILED)
    finally:
        DEX_FLUSHER["task"] = None
        for m in list(DEX_PENDING):
            fut = DEX_PENDING.pop(m)
            if not fut.done():
                fut.set_result(DEX_FAILED)
        DEX_BATCH.clear()

def norm_mint(m) -> str:
    """Chiave di cache: mint esatto senza spazi. NON si usa il minuscolo: gli indirizzi Solana (base58)
    distinguono maiuscole e minuscole, quindi 'abc' e 'ABC' sarebbero due token diversi."""
    return str(m).strip()

async def dex_fetch_many(client, mints, fresh=()):
    """Dati DexScreener per mint: prima la cache (TTL 15-20 s, 10 s per le posizioni), poi i mint mancanti
    di TUTTI i chiamanti vengono raggruppati in batch da 30 e le richieste identiche in volo vengono condivise.
    Il risultato è indicizzato con le chiavi originali passate dal chiamante."""
    orig, waits, found = {}, {}, {}
    fresh_n = {norm_mint(m) for m in fresh}
    now0 = time.time()
    saved = 0
    for m in mints:
        k = norm_mint(m)
        if not k or k in orig:
            continue
        orig[k] = m
        c = DEX_CACHE.get(k)
        if c and c[1] is not None:
            ttl = CONFIG.dex_cache_ttl_positions_sec if k in fresh_n else DEX_CACHE_TTL
        else:
            ttl = DEX_NEG_TTL
        if c and now0 - c[0] < ttl:
            saved += 1                      # dato ancora valido: nessuna richiesta HTTP
            if c[1] is not None:
                found[k] = c[1]
            continue
        fut = DEX_PENDING.get(k)
        if fut is None:
            fut = asyncio.get_running_loop().create_future()
            DEX_PENDING[k] = fut
            DEX_BATCH.append(k)
        else:
            saved += 1                      # richiesta già in coda/in volo per un altro chiamante
        waits[k] = fut
    if saved:
        sn()["dex_cache_hits"] = sn().get("dex_cache_hits", 0) + saved
    if waits and DEX_FLUSHER["task"] is None:
        DEX_FLUSHER["task"] = asyncio.create_task(_dex_flush(client))
    for k, fut in waits.items():
        try:
            v = await asyncio.wait_for(asyncio.shield(fut), timeout=CONFIG.dex_wait_timeout_sec)
        except asyncio.TimeoutError:
            v = DEX_FAILED
        if v is not DEX_FAILED and v is not None:
            found[k] = v
    tnow = time.time()
    if len(DEX_CACHE) > 2000:
        for k in [k for k, (t_, _) in DEX_CACHE.items() if tnow - t_ > max(DEX_CACHE_TTL, DEX_NEG_TTL) * 4]:
            DEX_CACHE.pop(k, None)
    return {orig[k]: v for k, v in found.items()}

async def dex_search_new(client):
    try:
        r = await dex_get(client, "https://api.dexscreener.com/latest/dex/search", params={"q": "pump.fun"})
        if r is None:
            return []
        r.raise_for_status()
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
    if stats and w.get("src") != "dexscreener":
        # primo dato DexScreener: si riparte da qui (lo storico del create non è confrontabile)
        w["hist"], w["mom_hist"], w["px"], w["candles"], w["peak_sol"] = [], [], [], [], 0.0
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
        pc = p.get("priceChange") or {}
        w["pc_m5"] = float(pc.get("m5") or 0)
        w["pc_h1"] = float(pc.get("h1") or 0)

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
            if mint in REMOVED:
                continue
            if not await make_room():
                continue
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
        if mint not in S["positions"] and mint in WATCH:
            maybe_enter(app, client, mint, WATCH[mint])

async def dex_discover_new(app, client):
    found = await dex_search_new(client)
    for addr, p in found:
        w = WATCH.get(addr)
        if not w:
            if addr in REMOVED:
                continue
            if not await make_room():
                continue
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
        if addr not in S["positions"] and addr in WATCH:
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
    if time.time() - DEX_SEARCH_LAST["t"] >= CONFIG.dex_search_interval_sec:
        DEX_SEARCH_LAST["t"] = time.time()
        await safe(dex_discover_new(app, client), "dex_discover_new")

def dex_prefilter(w, now) -> bool:
    """Fase DEX: finché il token è sulla bonding curve 'fredda' si usano solo i dati WebSocket/RPC.
    DexScreener entra in gioco solo con mcap >= dex_phase_mcap_usd oppure >= dex_phase_min_txs transazioni."""
    if w["blocked"]:
        return False
    age_s = now - w["t0"]
    if age_s / 60 > SN_MAX_AGE_MIN:
        return False
    if w["stream_trades"] == 0 and dex_mode():
        return age_s >= CONFIG.dex_mode_min_age_sec    # DexScreener è l'unica fonte dati
    if age_s < CONFIG.dex_min_age_sec:
        return False
    mcap = w["mcap_sol"] * SOL_USD
    if mcap > SN_MCAP_MAX * 2:
        return False
    return mcap >= CONFIG.dex_phase_mcap_usd or (w["buys"] + w["sells"]) >= CONFIG.dex_phase_min_txs

async def dex_cycle(app, client):
    now = time.time()
    if now < DEX_BACKOFF["until"]:
        return
    if dex_mode():
        held = [m for m in S["positions"] if m in WATCH and now - WATCH[m]["dex_t"] > CONFIG.dex_held_repoll_sec]
    else:
        held = [m for m in S["positions"] if m in WATCH and now - WATCH[m]["dex_t"] > 10
                and (now - WATCH[m]["last_t"] > 15 or WATCH[m]["mcap_sol"] * SOL_USD >= SN_HANDOFF_MCAP_USD)]
    # i token con dati già ricevuti via WebSocket/RPC non vengono interrogati su DexScreener
    cand = [k for k, w in WATCH.items() if k not in S["positions"]
            and (w["stream_trades"] == 0 or w["src"] != "stream")
            and dex_prefilter(w, now) and now - w["dex_t"] > SN_DEX_REPOLL_SEC]
    cand = sorted(cand, key=lambda k: WATCH[k]["dex_t"])[:SN_DEX_MAX_PER_CYCLE]
    mints = held + cand
    if not mints:
        return
    data = await dex_fetch_many(client, mints, fresh=set(held))
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
        st = sn()
        try:
            await flush_subs()
            await trade_watchdog()
        except Exception as e:
            st["hk_err"] = f"sottoscrizioni: {type(e).__name__}: {e}"[:150]
            log.warning("housekeeping subs: %s", e)
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
        try:
            async with SN_LOCK:
                for mint in list(S["positions"]):
                    await manage_position(app, client, mint)
        except Exception as e:
            st["hk_err"] = f"posizioni: {type(e).__name__}: {e}"[:150]
            log.warning("housekeeping positions: %s", e)
        try:
            dead = prune_candidates(now)
            if dead:
                reasons = st["reasons"]
                for k, why in dead:
                    if WATCH.pop(k, None) is None:
                        continue
                    REMOVED[k] = now
                    reasons[why] = reasons.get(why, 0) + 1
                    count_discard(categorize_reason(why))
                keys = [k for k, _ in dead]
                for i in range(0, len(keys), 100):
                    await ws_send({"method": "unsubscribeTokenTrade", "keys": keys[i:i + 100]})
            for k in [k for k, t in REMOVED.items() if now - t > CONFIG.removed_memory_sec]:
                REMOVED.pop(k, None)
        except Exception as e:
            st["hk_err"] = f"coda: {type(e).__name__}: {e}"[:150]
            log.warning("housekeeping prune: %s", e)
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
                    # URL senza loggare la API key
                    log.info("WebSocket connect (api_key=%s)", "SI" if FEED_STREAM else "NO")
                    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                        WS = ws
                        LAST_WS_EVENT = time.time()
                        st = sn()
                        st["ws_connected"] = True
                        st["ws_err"] = ""
                        # nuova connessione: le sottoscrizioni trade vanno rifatte
                        SUBSCRIBED.clear()
                        st["subs_active"] = 0
                        # subscribeNewToken è gratuito anche senza API key
                        await ws.send(json.dumps({"method": "subscribeNewToken"}))
                        if FEED_STREAM:
                            keys = list(WATCH.keys())
                            for i in range(0, len(keys), 100):
                                batch = keys[i:i + 100]
                                await ws_send({"method": "subscribeTokenTrade", "keys": batch})
                            # eventuali create arrivate mentre WS era giù
                            await flush_subs()
                        backoff = 2
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
                    err = str(e)[:150]
                    log.warning("websocket: %s", err)
                    st = sn()
                    st["ws_err"] = err
                    if _looks_like_auth_error(err):
                        st["ws_auth_err"] = err
                    st["reconnects"] = st.get("reconnects", 0) + 1
                WS = None
                sn()["ws_connected"] = False
                sn()["fallback_on"] = True
                SUBSCRIBED.clear()
                if S["active"]:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)
        finally:
            WS = None
            sn()["ws_connected"] = False
            SUBSCRIBED.clear()
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
            f"Obiettivo: +{SN_STD_TP1_PCT:.0f}% su token con meno di {SN_MAX_AGE_MIN:.0f} minuti di vita\n"
            f"Dati: {'PumpPortal' if not dex_mode() else 'DexScreener'}\n"
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
        now = time.time()
        ago = int(now - st["last_event"]) if st["last_event"] else None
        buy_ago = int(now - st["last_buy_t"]) if st.get("last_buy_t") else None
        sell_ago = int(now - st["last_sell_t"]) if st.get("last_sell_t") else None
        counts = discard_counts()
        fonte = ("PumpPortal trade stream" if FEED_STREAM and not trade_feed_silent()
                 else ("DexScreener (feed trade muto/assente)" if FEED_STREAM
                       else "DexScreener (nessuna API key PumpPortal)"))
        lines = [
            "Diagnostica caccia token:",
            f"• stato: {'ATTIVA' if S['active'] else 'ferma'}",
            f"• API key PumpPortal: {'SI' if FEED_STREAM else 'NO'}",
            f"• WebSocket: {'connesso' if st.get('ws_connected') else 'disconnesso'} | "
            f"riconnessioni: {st.get('reconnects', 0)} | reset feed: {st.get('ws_resets', 0)}",
            f"• fonte dati: {fonte}",
            f"• sottoscrizioni trade inviate: {st.get('subs', 0)} | attive ora: {st.get('subs_active', 0)}",
            f"• eventi totali: {st['events']} (ultimo {ago}s fa)" if ago is not None else
            f"• eventi totali: {st['events']} (nessuno ancora)",
            f"• create: {st.get('seen', 0)} | buy: {st.get('buy_events', 0)} | sell: {st.get('sell_events', 0)}",
            f"• ultimo buy: {buy_ago}s fa" if buy_ago is not None else "• ultimo buy: nessuno",
            f"• ultimo sell: {sell_ago}s fa" if sell_ago is not None else "• ultimo sell: nessuno",
            f"• tipi evento: " + (", ".join(f"{k}={v}" for k, v in st["types"].items()) or "nessuno"),
            f"• in osservazione: {len(WATCH)}/{CONFIG.max_watch} | entrati: {st.get('entered', 0)}",
            f"• DexScreener: {st.get('dex_reqs', 0)} req, {st.get('dex_cache_hits', 0)} cache hit, "
            f"{st.get('dex_429', 0)} HTTP 429",
            f"• errore WS: {st.get('ws_err') or 'nessuno'}",
            f"• errore auth WS: {st.get('ws_auth_err') or 'nessuno'}",
            f"• errore invio: {st.get('send_err') or 'nessuno'} | coda: {st.get('hk_err') or 'nessuno'}",
            f"• ultimo msg server: {st.get('last_msg') or 'nessuno'}",
            "• motivi scarto:",
        ] + discard_lines(counts) + [f"   (totale: {sum(counts.values())})"]
        best = sorted(((k, w_) for k, w_ in WATCH.items()),
                      key=lambda kv: -(kv[1].get("buys", 0) + kv[1].get("sells", 0)))[:4]
        if best:
            lines.append("• candidati top ora:")
            for _, w_ in best:
                lines.append(
                    f"   {str(w_.get('symbol', '?'))[:8]} {(now - w_['t0']) / 60:.0f}m "
                    f"${w_.get('mcap_sol', 0) * SOL_USD:,.0f} tx={w_.get('buys', 0) + w_.get('sells', 0)} "
                    f"→ {w_.get('why') or 'ok'}"
                )
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
