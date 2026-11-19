import asyncio
import base64
import json
import logging
import os
import re
import threading
import time

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

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
log = logging.getLogger("runner_hunter")

TOKEN = os.getenv("TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "7935524220"))
RPC_URL = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() != "false"
SLIPPAGE = int(os.getenv("SLIPPAGE", "25"))
PRIORITY_FEE = float(os.getenv("PRIORITY_FEE", "0.0005"))
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
STATE_FILE = os.getenv("STATE_FILE", "state.json")

WS_URL = "wss://pumpportal.fun/api/data"
SNIPE_LIVE = os.getenv("SNIPE_LIVE", "false").lower() == "true"
SNIPE_COST_PCT = float(os.getenv("SNIPE_COST_PCT", "8"))
SNIPE_PCT_PER_TRADE = float(os.getenv("SNIPE_PCT_PER_TRADE", "15"))

# Target: intercettare token forti intorno ai 20k$ con potenziale verso 70k$+
SN_MCAP_MIN = float(os.getenv("SN_MCAP_MIN", "18000"))
SN_MCAP_MAX = float(os.getenv("SN_MCAP_MAX", "35000"))
SN_MIN_AGE_MIN = float(os.getenv("SN_MIN_AGE_MIN", "0.4"))
SN_MAX_AGE_MIN = float(os.getenv("SN_MAX_AGE_MIN", "6.0"))
SN_DEAD_AFTER_SEC = float(os.getenv("SN_DEAD_AFTER_SEC", "75"))
SN_MIN_BUYERS = int(os.getenv("SN_MIN_BUYERS", "35"))
SN_MIN_TXNS = int(os.getenv("SN_MIN_TXNS", "15"))
SN_MIN_BUY_RATIO = float(os.getenv("SN_MIN_BUY_RATIO", "0.68"))
SN_MAX_HOLDER_PCT = float(os.getenv("SN_MAX_HOLDER_PCT", "18"))
SN_MAX_DRAWDOWN_PCT = float(os.getenv("SN_MAX_DRAWDOWN_PCT", "18"))
SN_MIN_MOMENTUM_PCT = float(os.getenv("SN_MIN_MOMENTUM_PCT", "25"))
SN_MIN_BURST_PCT = float(os.getenv("SN_MIN_BURST_PCT", "10"))  # spinta ultimi ~25s

# Gestione uscita pensata per lasciare correre verso 70k$+
SN_TP_PCT = float(os.getenv("SN_TP_PCT", "130"))
SN_TP_SELL_PCT = float(os.getenv("SN_TP_SELL_PCT", "35"))
SN_STOP_PCT = float(os.getenv("SN_STOP_PCT", "28"))
SN_TRAIL_PCT = float(os.getenv("SN_TRAIL_PCT", "38"))
SN_MAX_HOLD_MIN = float(os.getenv("SN_MAX_HOLD_MIN", "30"))
SN_STALE_MIN = float(os.getenv("SN_STALE_MIN", "2.5"))
SN_EXIT_MCAP_USD = float(os.getenv("SN_EXIT_MCAP_USD", "72000"))  # 0 = disattivato
SN_HANDOFF_MCAP_USD = float(os.getenv("SN_HANDOFF_MCAP_USD", "55000"))
SN_MAX_POSITIONS = int(os.getenv("SN_MAX_POSITIONS", "4"))
SN_MAX_WATCH = int(os.getenv("SN_MAX_WATCH", "600"))

TOTAL_SUPPLY = 1_000_000_000
WSOL = "So11111111111111111111111111111111111111112"
SOL_USD = float(os.getenv("SOL_USD", "150"))

KEYPAIR = Keypair.from_base58_string(PRIVATE_KEY) if PRIVATE_KEY else None
GET_BUDGET, GET_DURATION = range(2)

S = {
    "active": False,
    "budget": 0.0,
    "end": 0.0,
    "positions": {},
    "trades": 0,
    "pnl": 0.0,
    "closed": [],
    "sn": {},
}

WATCH = {}
WS = None
SN_LOCK = asyncio.Lock()
PENDING_SUB = []


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(S, f)
    except Exception as e:
        log.warning("save_state: %s", e)


def load_state():
    try:
        with open(STATE_FILE) as f:
            S.update(json.load(f))
    except Exception:
        pass


def exposure():
    return sum(p["cost"] * p["left"] for p in S["positions"].values())


def sn():
    d = S.setdefault("sn", {})
    defaults = {
        "seen": 0, "entered": 0, "vetoed": 0, "pruned": 0,
        "events": 0, "last_event": 0, "ws_err": "", "reasons": {},
        "subs": 0, "send_err": "", "dex_polls": 0, "dex_hits": 0,
    }
    for k, v in defaults.items():
        d.setdefault(k, v)
    return d


async def notify(app, text):
    try:
        await app.bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception as e:
        log.warning("notify: %s", e)


async def rpc(client, method, params):
    r = await client.post(RPC_URL, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=25)
    r.raise_for_status()
    j = r.json()
    if "error" in j:
        raise RuntimeError(j["error"])
    return j["result"]


async def wallet_balance(client):
    if not KEYPAIR:
        return None
    res = await rpc(client, "getBalance", [str(KEYPAIR.pubkey()), {"commitment": "confirmed"}])
    return res["value"] / 1e9


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
        batch, PENDING_SUB = PENDING_SUB[:80], PENDING_SUB[80:]
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
    return {
        "name": ev.get("name"),
        "symbol": ev.get("symbol") or "?",
        "creator": ev.get("traderPublicKey"),
        "t0": now,
        "last_t": now,
        "mcap_sol": mc,
        "peak_sol": mc,
        "last_price": price_from(ev),
        "buyers": set(),
        "buys": 0,
        "sells": 0,
        "vol": 0.0,
        "holders": {},
        "creator_sold": False,
        "ai_done": False,
        "blocked": False,
        "why": "nessuno scambio dopo la creazione",
        "ai_reason": "",
        "stream_trades": 0,
        "dex_t": 0.0,
        "hist": [(now, mc)],
        "src": "stream",
    }


def push_hist(w):
    now = time.time()
    w["hist"].append((now, w["mcap_sol"]))
    w["hist"] = [(t, m) for t, m in w["hist"] if now - t <= 150]


def pct_change_window(w, min_age_sec, max_age_sec=None):
    now = time.time()
    candidates = [
        m for t, m in w["hist"]
        if (now - t) >= min_age_sec and (max_age_sec is None or (now - t) <= max_age_sec)
    ]
    if not candidates:
        return None
    base = candidates[-1]
    return (w["mcap_sol"] / base - 1) * 100 if base > 0 else 0.0


def momentum_pct(w):
    m60 = pct_change_window(w, 45)
    if m60 is not None:
        return m60
    if len(w["hist"]) >= 2 and w["hist"][0][1] > 0:
        return (w["mcap_sol"] / w["hist"][0][1] - 1) * 100
    return 0.0


def burst_pct(w):
    m25 = pct_change_window(w, 18, 60)
    return m25 if m25 is not None else momentum_pct(w)


async def update_sol_price(client):
    global SOL_USD
    try:
        r = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{WSOL}", timeout=15)
        pairs = [
            p for p in (r.json().get("pairs") or [])
            if p.get("chainId") == "solana"
            and (p.get("baseToken") or {}).get("address") == WSOL
            and (p.get("quoteToken") or {}).get("symbol") in ("USDC", "USDT")
        ]
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


def settle(mint, pos, pct, price_now):
    f = pct / 100
    cost_sold = pos["cost"] * pos["left"] * f
    if not pos.get("qty") or not price_now:
        return None, cost_sold
    proceeds = pos["qty"] * pos["left"] * f * price_now * (1 - SNIPE_COST_PCT / 100)
    pnl = proceeds - cost_sold
    S["pnl"] = S.get("pnl", 0.0) + pnl
    S.setdefault("closed", []).append({
        "sym": pos.get("sym") or mint[:6],
        "pnl": round(pnl, 5),
        "cost": round(cost_sold, 5),
    })
    S["closed"] = S["closed"][-40:]
    return pnl, cost_sold


def pnl_text(pnl, cost):
    if pnl is None:
        return "\nRisultato: non calcolabile"
    pct = f" ({pnl / cost * 100:+.0f}%)" if cost > 0 else ""
    return f"\nRisultato stimato: {pnl:+.4f} SOL{pct}"


def pnl_summary():
    n = len(S.get("closed", []))
    return f"Risultato stimato delle vendite: {S.get('pnl', 0.0):+.4f} SOL su {n} chiusure"


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
    tot = w["buys"] + w["sells"]
    if tot < SN_MIN_TXNS:
        return "pochi scambi totali"
    if len(w["buyers"]) < SN_MIN_BUYERS:
        return "pochi compratori unici"
    if w["buys"] / tot < SN_MIN_BUY_RATIO:
        return "troppe vendite"
    if w["creator_sold"]:
        return "il creatore ha venduto"
    top = max(w["holders"].values(), default=0) / TOTAL_SUPPLY * 100
    if top > SN_MAX_HOLDER_PCT:
        return "un wallet detiene troppo"
    if w["peak_sol"] > 0 and (w["peak_sol"] - w["mcap_sol"]) / w["peak_sol"] * 100 > SN_MAX_DRAWDOWN_PCT:
        return "già in calo dal picco"
    if momentum_pct(w) < SN_MIN_MOMENTUM_PCT:
        return "slancio insufficiente"
    if burst_pct(w) < SN_MIN_BURST_PCT:
        return "nessuna spinta recente"
    if not w["last_price"]:
        return "prezzo non disponibile"
    return None


async def snipe_ai(client, w):
    if not GEMINI_API_KEY:
        return True, ""
    tot = w["buys"] + w["sells"]
    stats = {
        "nome": w["name"],
        "simbolo": w["symbol"],
        "eta_minuti": round((time.time() - w["t0"]) / 60, 1),
        "market_cap_usd": round(w["mcap_sol"] * SOL_USD),
        "compratori_diversi": len(w["buyers"]),
        "quota_acquisti": round(w["buys"] / tot, 2) if tot else None,
        "volume_sol": round(w["vol"], 1),
        "max_wallet_percento_supply": round(max(w["holders"].values(), default=0) / TOTAL_SUPPLY * 100, 1),
        "creatore_ha_venduto": w["creator_sold"],
        "crescita_1m_pct": round(momentum_pct(w), 1),
        "spinta_recente_pct": round(burst_pct(w), 1),
    }
    prompt = (
        "Valuta se questo token su pump.fun ha trazione reale intorno ai 20k$ e può arrivare a 70k$+. "
        "Cerca compratori distribuiti, forte momentum e nessun dump del creatore. "
        "Rispondi SOLO con JSON: {\"decision\": \"buy\" oppure \"skip\", \"reason\": \"max 15 parole in italiano\"}.\n"
        f"Statistiche: {json.dumps(stats, ensure_ascii=False)}"
    )
    try:
        r = await client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
            headers={"x-goog-api-key": GEMINI_API_KEY},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"responseMimeType": "application/json", "temperature": 0.2},
            },
            timeout=25,
        )
        r.raise_for_status()
        d = json.loads(r.json()["candidates"][0]["content"]["parts"][0]["text"])
        return str(d.get("decision", "")).lower() == "buy", str(d.get("reason", ""))[:200]
    except Exception as e:
        log.warning("snipe_ai: %s", e)
        return True, "AI non disponibile"


async def try_enter(app, client, mint, w):
    why = entry_check(w)
    if why:
        w["why"] = why
        return
    if len(S["positions"]) >= SN_MAX_POSITIONS:
        w["why"] = "posizioni piene"
        return
    amount = round(min(S["budget"] * SNIPE_PCT_PER_TRADE / 100, S["budget"] - exposure()), 4)
    if amount < 0.005:
        w["why"] = "budget esaurito"
        return
    if GEMINI_API_KEY and not w["ai_done"]:
        w["ai_done"] = True
        ok, reason = await snipe_ai(client, w)
        if not ok:
            w["blocked"], w["why"] = True, "veto dell'AI"
            sn()["vetoed"] += 1
            await notify(app, f"⏭ SKIP {w['symbol']} (mcap ${w['mcap_sol'] * SOL_USD:,.0f})\nhttps://pump.fun/coin/{mint}\nAI: {reason}")
            return
        w["ai_reason"] = reason
    price = w["last_price"]
    entry = price * (1 + SNIPE_COST_PCT / 100)
    ok, info = await snipe_trade(client, "buy", mint, amount)
    if ok:
        S["positions"][mint] = {
            "cost": amount, "left": 1.0, "qty": amount / entry,
            "last_price": price, "sym": w["symbol"], "entry": price,
            "peak": price, "t": time.time(), "tp_done": False,
        }
        S["trades"] += 1
        sn()["entered"] += 1
        save_state()
    else:
        w["blocked"], w["why"] = True, "ordine fallito"
    tot = w["buys"] + w["sells"]
    await notify(
        app,
        f"{'🟢' if ok else '🔴'} BUY {amount} SOL — {w['symbol']}\n"
        f"https://pump.fun/coin/{mint}\n"
        f"mcap ${w['mcap_sol'] * SOL_USD:,.0f}, {len(w['buyers'])} compratori, "
        f"{w['buys'] / tot * 100:.0f}% buy, momentum +{momentum_pct(w):.0f}%, età {(time.time() - w['t0']) / 60:.1f} min"
        + (f"\nAI: {w['ai_reason']}" if w["ai_reason"] else "")
        + f"\n{info}",
    )


async def snipe_sell(app, client, mint, pct, reason):
    pos = S["positions"].get(mint)
    if not pos:
        return
    w = WATCH.get(mint) or {}
    price_now = w.get("last_price") or pos.get("last_price")
    ok, info = await snipe_trade(client, "sell", mint, f"{pct}%")
    extra = ""
    if ok:
        pnl, cs = settle(mint, pos, pct, price_now)
        extra = pnl_text(pnl, cs)
        pos["left"] *= 1 - pct / 100
        S["trades"] += 1
        if pct >= 100 or pos["left"] < 0.02:
            del S["positions"][mint]
            WATCH.pop(mint, None)
            await ws_send({"method": "unsubscribeTokenTrade", "keys": [mint]})
        save_state()
    await notify(
        app,
        f"{'🟠' if ok else '🔴'} SELL {pct}% {pos.get('sym') or mint[:6]} — {reason}\n"
        f"https://pump.fun/coin/{mint}\n{info}{extra}",
    )


async def snipe_close_all(app, client):
    for mint in list(S["positions"]):
        await snipe_sell(app, client, mint, 100, "chiusura sessione")
    save_state()


async def manage_position(app, client, mint):
    pos, w = S["positions"].get(mint), WATCH.get(mint)
    if not pos or not w:
        return
    now = time.time()
    price = w["last_price"] or pos["last_price"]
    pos["last_price"] = price
    pos["peak"] = max(pos.get("peak") or price, price)
    gain = (price / pos["entry"] - 1) * 100
    mcap_usd = w["mcap_sol"] * SOL_USD
    reason = None
    if w["creator_sold"]:
        reason = "il creatore ha venduto"
    elif gain <= -SN_STOP_PCT:
        reason = f"stop loss ({gain:.0f}%)"
    elif SN_EXIT_MCAP_USD > 0 and mcap_usd >= SN_EXIT_MCAP_USD:
        reason = f"target mcap ${mcap_usd:,.0f} raggiunto"
    elif now - pos["t"] > SN_MAX_HOLD_MIN * 60:
        reason = "tempo massimo raggiunto"
    elif now - w["last_t"] > SN_STALE_MIN * 60:
        reason = "nessuno scambia più"
    elif pos.get("tp_done") and (price / pos["peak"] - 1) * 100 <= -SN_TRAIL_PCT:
        reason = f"trailing stop ({gain:+.0f}%)"
    if reason:
        await snipe_sell(app, client, mint, 100, reason)
        return
    if not pos.get("tp_done") and gain >= SN_TP_PCT:
        pos["tp_done"] = True
        await snipe_sell(app, client, mint, SN_TP_SELL_PCT, f"take profit parziale (+{gain:.0f}%)")


async def on_event(app, client, ev):
    mint = ev.get("mint")
    st = sn()
    if not mint:
        return
    st["events"] += 1
    st["last_event"] = int(time.time())
    tx = ev.get("txType")
    if tx == "create":
        st["seen"] += 1
        if len(WATCH) >= SN_MAX_WATCH:
            for k in sorted(WATCH, key=lambda k: WATCH[k]["t0"]):
                if k not in S["positions"]:
                    WATCH.pop(k)
                    await ws_send({"method": "unsubscribeTokenTrade", "keys": [k]})
                    break
        w = new_watch(ev)
        w["holders"][w["creator"]] = float(ev.get("initialBuy") or 0)
        WATCH[mint] = w
        PENDING_SUB.append(mint)
        return
    w = WATCH.get(mint)
    if not w or tx not in ("buy", "sell"):
        return
    price = price_from(ev)
    if price:
        w["last_price"] = price
    mc = ev.get("marketCapSol")
    if mc:
        w["mcap_sol"] = float(mc)
        w["peak_sol"] = max(w["peak_sol"], w["mcap_sol"])
    w["last_t"] = time.time()
    w["stream_trades"] += 1
    w["src"] = "stream"
    push_hist(w)
    trader = ev.get("traderPublicKey")
    w["vol"] += float(ev.get("solAmount") or 0)
    if ev.get("newTokenBalance") is not None:
        w["holders"][trader] = float(ev["newTokenBalance"])
    if tx == "buy":
        w["buys"] += 1
        if trader != w["creator"]:
            w["buyers"].add(trader)
    else:
        w["sells"] += 1
        if trader == w["creator"]:
            w["creator_sold"] = True
    async with SN_LOCK:
        if mint in S["positions"]:
            await manage_position(app, client, mint)
        elif S["active"] and not w["blocked"]:
            await try_enter(app, client, mint, w)


async def dex_fetch_many(client, mints):
    out = {}
    for i in range(0, len(mints), 30):
        chunk = mints[i:i + 30]
        try:
            r = await client.get("https://api.dexscreener.com/latest/dex/tokens/" + ",".join(chunk), timeout=15)
            r.raise_for_status()
            for p in r.json().get("pairs") or []:
                addr = (p.get("baseToken") or {}).get("address")
                if p.get("chainId") != "solana" or addr not in chunk:
                    continue
                cur = out.get(addr)
                if not cur or ((p.get("liquidity") or {}).get("usd") or 0) > ((cur.get("liquidity") or {}).get("usd") or 0):
                    out[addr] = p
        except Exception as e:
            log.warning("dex_fetch_many: %s", e)
    return out


async def dex_cycle(client):
    now = time.time()
    held = [
        m for m in S["positions"]
        if m in WATCH and now - WATCH[m]["dex_t"] > 8
        and (now - WATCH[m]["last_t"] > 12 or WATCH[m]["mcap_sol"] * SOL_USD >= SN_HANDOFF_MCAP_USD)
    ]
    if not held:
        return
    data = await dex_fetch_many(client, held)
    st = sn()
    st["dex_polls"] += 1
    st["dex_hits"] += len(data)
    for m in held:
        w = WATCH.get(m)
        if not w:
            continue
        w["dex_t"] = now
        p = data.get(m)
        if p:
            pn = float(p.get("priceNative") or 0)
            if pn > 0:
                w["last_price"] = pn
                w["mcap_sol"] = pn * TOTAL_SUPPLY
                w["peak_sol"] = max(w["peak_sol"], w["mcap_sol"])
                w["last_t"] = now
                push_hist(w)


async def snipe_housekeeping(app, client):
    last_sol = time.time()
    while S["active"]:
        now = time.time()
        await flush_subs()
        try:
            await dex_cycle(client)
        except Exception as e:
            log.warning("dex_cycle: %s", e)
        if now - last_sol > 60:
            await update_sol_price(client)
            last_sol = now
        if now >= S["end"]:
            S["active"] = False
            await notify(app, "⏱ Durata terminata. Chiudo le posizioni aperte.")
            async with SN_LOCK:
                await snipe_close_all(app, client)
            await notify(app, "Sessione chiusa.\n" + pnl_summary())
            save_state()
            return
        async with SN_LOCK:
            for mint in list(S["positions"]):
                await manage_position(app, client, mint)

        to_remove = []
        for k, w in WATCH.items():
            if k in S["positions"]:
                continue
            age_sec = now - w["t0"]
            if age_sec / 60 > SN_MAX_AGE_MIN:
                to_remove.append(k)
            elif age_sec >= SN_DEAD_AFTER_SEC and (w["buys"] < 4 or len(w["buyers"]) < 4):
                w["why"] = "senza trazione iniziale"
                to_remove.append(k)

        if to_remove:
            reasons = sn()["reasons"]
            for k in to_remove:
                why = WATCH.pop(k)["why"]
                reasons[why] = reasons.get(why, 0) + 1
                sn()["pruned"] += 1
            for i in range(0, len(to_remove), 100):
                await ws_send({"method": "unsubscribeTokenTrade", "keys": to_remove[i:i + 100]})
        await asyncio.sleep(2)


async def snipe_run(app):
    global WS
    try:
        import websockets
    except ImportError:
        S["active"] = False
        await notify(app, "Manca la libreria 'websockets' in requirements.txt.")
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
            while S["active"]:
                try:
                    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                        WS = ws
                        await ws.send(json.dumps({"method": "subscribeNewToken"}))
                        keys = list(WATCH.keys())
                        for i in range(0, len(keys), 100):
                            await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": keys[i:i + 100]}))
                        backoff = 2
                        sn()["ws_err"] = ""
                        while S["active"]:
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=10)
                            except asyncio.TimeoutError:
                                continue
                            try:
                                ev = json.loads(raw)
                            except Exception:
                                continue
                            try:
                                await on_event(app, client, ev)
                            except Exception as e:
                                log.warning("on_event: %s", e)
                except Exception as e:
                    log.warning("websocket: %s", e)
                    sn()["ws_err"] = str(e)[:150]
                WS = None
                if S["active"]:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)
        finally:
            WS = None
            try:
                await asyncio.wait_for(hk, timeout=30)
            except Exception:
                hk.cancel()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END
    if S["active"]:
        await update.message.reply_text("Sessione già attiva. Usa /stop per chiuderla.")
        return ConversationHandler.END
    sim = "SIMULAZIONE" if (DRY_RUN or not SNIPE_LIVE) else "REALE ⚠️"
    await update.message.reply_text(
        f"Modalità: Caccia Runner 20k -> 70k+ ({sim})\n"
        f"Budget massimo in SOL (es. 0.5)? Uso il {SNIPE_PCT_PER_TRADE:.0f}% per posizione.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return GET_BUDGET


async def get_budget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        budget = float(update.message.text.strip().replace(",", "."))
        if budget <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Inserisci un numero valido (es. 0.5):")
        return GET_BUDGET
    if not DRY_RUN and SNIPE_LIVE:
        try:
            async with httpx.AsyncClient() as c:
                bal = await wallet_balance(c)
        except Exception as e:
            await update.message.reply_text(f"Errore lettura saldo: {e}")
            return ConversationHandler.END
        if bal is None or budget > bal - 0.02:
            await update.message.reply_text(f"Saldo insufficiente ({bal} SOL).")
            return ConversationHandler.END
    context.user_data["budget"] = budget
    await update.message.reply_text("Per quante ore tengo attiva la sessione? (es. 1 o 3)")
    return GET_DURATION


async def get_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        hours = float(update.message.text.strip().replace(",", "."))
        if hours <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Inserisci un numero valido di ore:")
        return GET_DURATION

    S.update({
        "active": True,
        "budget": context.user_data["budget"],
        "end": time.time() + hours * 3600,
        "positions": {},
        "trades": 0,
        "pnl": 0.0,
        "closed": [],
        "sn": {},
    })
    WATCH.clear()
    save_state()
    context.application.create_task(snipe_run(context.application))
    await update.message.reply_text(
        f"✅ Caccia runner avviata\n"
        f"Range entrata: ${SN_MCAP_MIN:,.0f} - ${SN_MCAP_MAX:,.0f} | età {SN_MIN_AGE_MIN}-{SN_MAX_AGE_MIN} min\n"
        f"Requisiti: >= {SN_MIN_BUYERS} buyer, >= {SN_MIN_TXNS} tx, >= {SN_MIN_BUY_RATIO * 100:.0f}% buy, "
        f"momentum >= +{SN_MIN_MOMENTUM_PCT:.0f}%, burst >= +{SN_MIN_BURST_PCT:.0f}%\n"
        f"Target mcap: ${SN_EXIT_MCAP_USD:,.0f} | TP {SN_TP_SELL_PCT:.0f}% a +{SN_TP_PCT:.0f}% | Stop -{SN_STOP_PCT:.0f}% | Trail -{SN_TRAIL_PCT:.0f}%\n"
        f"Durata: {hours}h"
    )
    return ConversationHandler.END


async def resoconto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    st = sn()
    unreal = 0.0
    lines = [
        f"Stato: {'ATTIVA' if S['active'] else 'ferma'}",
        f"Visti: {st['seen']} | In watch: {len(WATCH)} | Potati: {st['pruned']} | Entrati: {st['entered']} | Veti AI: {st['vetoed']}",
        f"Esposizione: {exposure():.4f} / {S['budget']} SOL | Ordini: {S['trades']}",
        f"Realizzato (stimato): {S.get('pnl', 0.0):+.4f} SOL",
    ]
    for mint, p in list(S["positions"].items())[:10]:
        cost = p["cost"] * p["left"]
        pr = p.get("last_price")
        if pr and p.get("qty"):
            val = p["qty"] * p["left"] * pr * (1 - SNIPE_COST_PCT / 100)
            unreal += val - cost
            lines.append(f"• {p.get('sym') or mint[:6]}: costo {cost:.4f}, ora {val:.4f} ({(val / cost - 1) * 100:+.0f}%)")
    lines.append(f"Aperto (stimato): {unreal:+.4f} SOL | TOTALE: {S.get('pnl', 0.0) + unreal:+.4f} SOL")
    await update.message.reply_text("\n".join(lines))


async def diagnostica(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    st = sn()
    ago = int(time.time() - st["last_event"]) if st["last_event"] else None
    top = sorted(st["reasons"].items(), key=lambda x: -x[1])[:8]
    lines = [
        "Diagnostica runner:",
        f"• eventi: {st['events']} (ultimo {ago}s fa) | SOL_USD: ${SOL_USD:.1f}",
        f"• visti: {st['seen']} | watch: {len(WATCH)} | potati: {st['pruned']}",
        f"• entrati: {st['entered']} | veti AI: {st['vetoed']} | ws_err: {st['ws_err'] or 'nessuno'}",
        "• scarti principali:",
    ] + [f"   {n}× {r}" for r, n in top]
    await update.message.reply_text("\n".join(lines))


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    S["active"] = False
    async with httpx.AsyncClient() as c:
        async with SN_LOCK:
            await snipe_close_all(context.application, c)
    await update.message.reply_text("Sessione fermata.\n" + pnl_summary())


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Annullato.")
    return ConversationHandler.END


web = Flask(__name__)


@web.route("/")
def home():
    return "Bot is running"


def run_flask():
    web.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), use_reloader=False)


async def post_init(app):
    load_state()
    if S.get("active"):
        await notify(app, "♻️ Bot riavviato: riprendo la caccia.")
        app.create_task(snipe_run(app))


def main():
    if not TOKEN:
        print("ERRORE: TOKEN mancante")
        return
    if not DRY_RUN and SNIPE_LIVE and not KEYPAIR:
        print("ERRORE: PRIVATE_KEY mancante")
        return
    threading.Thread(target=run_flask, daemon=True).start()
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start), CommandHandler("inizia", start)],
        states={
            GET_BUDGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_budget)],
            GET_DURATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_duration)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    app.add_handler(conv)
    app.add_handler(CommandHandler("resoconto", resoconto))
    app.add_handler(CommandHandler("diagnostica", diagnostica))
    app.add_handler(CommandHandler("stop", stop))
    app.run_polling()


if __name__ == "__main__":
    main()
