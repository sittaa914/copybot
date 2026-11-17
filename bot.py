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
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
log = logging.getLogger("copybot")

# ---------------- CONFIG (variabili d'ambiente su Render) ----------------
TOKEN = os.getenv("TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "7935524220"))
RPC_URL = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")  # meglio Helius
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")  # chiave privata base58 del wallet DEDICATO
DRY_RUN = os.getenv("DRY_RUN", "true").lower() != "false"  # default: simulazione
PCT_PER_TRADE = float(os.getenv("PCT_PER_TRADE", "20"))  # % del budget per ogni trade copiato
MAX_SOL_PER_TRADE = float(os.getenv("MAX_SOL_PER_TRADE", "0"))  # tetto assoluto per trade (0 = nessuno)
MIN_TRADER_SOL = float(os.getenv("MIN_TRADER_SOL", "0.2"))  # ignora acquisti del trader più piccoli di questo

# Filtro "hype" (tutto regolabile da Render)
FILTER_ENABLED = os.getenv("FILTER_ENABLED", "true").lower() != "false"
MIN_LIQ_USD = float(os.getenv("MIN_LIQ_USD", "5000"))
MIN_VOL_H1_USD = float(os.getenv("MIN_VOL_H1_USD", "5000"))
MIN_TXNS_H1 = int(os.getenv("MIN_TXNS_H1", "30"))
MIN_BUY_RATIO = float(os.getenv("MIN_BUY_RATIO", "0.8"))  # acquisti/vendite minimi negli ultimi 5 min
MAX_PUMP_M5 = float(os.getenv("MAX_PUMP_M5", "150"))  # % max salita negli ultimi 5 min (evita di comprare in cima)
MAX_DUMP_H1 = float(os.getenv("MAX_DUMP_H1", "50"))  # % max calo nell'ultima ora
MAX_MCAP_USD = float(os.getenv("MAX_MCAP_USD", "3000000"))
RUGCHECK = os.getenv("RUGCHECK", "true").lower() != "false"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")  # opzionale: secondo parere dell'AI
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
SLIPPAGE = int(os.getenv("SLIPPAGE", "25"))  # %
PRIORITY_FEE = float(os.getenv("PRIORITY_FEE", "0.0005"))  # SOL
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "3"))
STATE_FILE = os.getenv("STATE_FILE", "state.json")

PUMP_PROGRAMS = {
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",  # pump.fun bonding curve
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",  # pump AMM (dopo migrazione)
}
PUMP_ONLY = os.getenv("PUMP_ONLY", "false").lower() == "true"  # false = rileva anche swap non-pump (Raydium, Jupiter...)
WSOL = "So11111111111111111111111111111111111111112"
ADDR_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

KEYPAIR = Keypair.from_base58_string(PRIVATE_KEY) if PRIVATE_KEY else None

GET_TRADERS, GET_BUDGET, GET_DURATION = range(3)

S = {"active": False, "traders": [], "budget": 0.0, "end": 0.0, "positions": {}, "last": {}, "trades": 0, "skipped": 0}


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


DG_DEFAULTS = {"polls": 0, "txs": 0, "trades": 0, "small": 0, "sell_nopos": 0, "err": "", "last_poll": 0,
               "sigs": 0, "failed": 0, "null": 0, "notrade": 0, "last_nt": ""}


def dg(trader):
    d = S.setdefault("diag", {}).setdefault(trader, {})
    for k, v in DG_DEFAULTS.items():
        d.setdefault(k, v)
    return d


def describe_tx(tx, trader):
    try:
        meta = tx["meta"]
        keys = [k["pubkey"] if isinstance(k, dict) else k for k in tx["transaction"]["message"]["accountKeys"]]
        if trader not in keys:
            return "trader non presente nella transazione"
        i = keys.index(trader)
        sol = (meta["postBalances"][i] - meta["preBalances"][i]) / 1e9

        def bals(lst):
            out = {}
            for b in lst or []:
                if b.get("owner") == trader:
                    out[b["mint"]] = out.get(b["mint"], 0.0) + float(b["uiTokenAmount"].get("uiAmountString") or 0)
            return out

        pre, post = bals(meta.get("preTokenBalances")), bals(meta.get("postTokenBalances"))
        n = sum(1 for m in set(pre) | set(post) if abs(post.get(m, 0.0) - pre.get(m, 0.0)) > 0)
        pump = "sì" if PUMP_PROGRAMS.intersection(keys) else "no"
        return f"{tx['transaction']['signatures'][0][:8]}… SOL {sol:+.3f}, token cambiati {n}, pump {pump}"
    except Exception as e:
        return f"errore analisi: {e}"


def short(a):
    return f"{a[:4]}…{a[-4:]}"


async def notify(app, text):
    try:
        await app.bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception as e:
        log.warning("notify: %s", e)


# ---------------- RPC ----------------
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


# ---------------- PARSING DEI TRADE DEL TRADER ----------------
def parse_trades(tx, trader):
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
                out[b["mint"]] = out.get(b["mint"], 0.0) + float(b["uiTokenAmount"].get("uiAmountString") or 0)
        return out

    pre, post = bals(meta.get("preTokenBalances")), bals(meta.get("postTokenBalances"))
    trades = []
    for mint in set(pre) | set(post):
        d = post.get(mint, 0.0) - pre.get(mint, 0.0)
        if d > 0 and sol_delta < 0:
            trades.append({"side": "buy", "mint": mint, "sol": -sol_delta})
        elif d < 0 and sol_delta > 0.01:
            p = pre.get(mint, 0.0)
            pct = min(100.0, (-d / p) * 100) if p > 0 else 100.0
            trades.append({"side": "sell", "mint": mint, "pct": pct})
    return trades


# ---------------- FILTRO HYPE / QUALITA' TOKEN ----------------
async def fetch_metrics(client, mint):
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


def rule_check(m):
    why = []
    if m["liq_usd"] < MIN_LIQ_USD:
        why.append(f"liquidità bassa (${m['liq_usd']:.0f})")
    if m["vol_h1"] < MIN_VOL_H1_USD:
        why.append(f"volume 1h basso (${m['vol_h1']:.0f})")
    if m["buys_h1"] + m["sells_h1"] < MIN_TXNS_H1:
        why.append("poche transazioni (poco interesse)")
    if m["buys_m5"] + m["sells_m5"] >= 5 and m["buys_m5"] < MIN_BUY_RATIO * m["sells_m5"]:
        why.append("più vendite che acquisti negli ultimi 5 min")
    if m["chg_m5"] > MAX_PUMP_M5:
        why.append(f"già salito {m['chg_m5']:.0f}% in 5 min")
    if m["chg_h1"] < -MAX_DUMP_H1:
        why.append(f"in calo {m['chg_h1']:.0f}% nell'ultima ora")
    if MAX_MCAP_USD > 0 and m["mcap_usd"] > MAX_MCAP_USD:
        why.append(f"market cap alta (${m['mcap_usd']:.0f})")
    return why


async def rugcheck_dangers(client, mint):
    try:
        r = await client.get(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary", timeout=15)
        if r.status_code != 200:
            return []
        return [x.get("name", "?") for x in r.json().get("risks", []) if x.get("level") == "danger"]
    except Exception as e:
        log.warning("rugcheck: %s", e)
        return []


async def ai_check(client, m, trader_sol):
    if not GEMINI_API_KEY:
        return True, ""
    prompt = (
        "Sei un filtro di rischio per un bot che copia trade di meme coin su Solana. "
        "Ti do le metriche di mercato di un token che un trader esperto ha appena comprato "
        f"({trader_sol:.2f} SOL). Decidi se conviene copiare l'acquisto con una piccola somma. "
        "Sii prudente: se i segnali sono deboli o sospetti, scegli skip. "
        'Rispondi SOLO con JSON: {"decision": "buy" oppure "skip", "reason": "max 15 parole in italiano"}.\n'
        f"Metriche: {json.dumps(m)}"
    )
    try:
        r = await client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
            headers={"x-goog-api-key": GEMINI_API_KEY},
            json={"contents": [{"parts": [{"text": prompt}]}],
                  "generationConfig": {"responseMimeType": "application/json", "temperature": 0.2}},
            timeout=25,
        )
        r.raise_for_status()
        d = json.loads(r.json()["candidates"][0]["content"]["parts"][0]["text"])
        return str(d.get("decision", "")).lower() == "buy", str(d.get("reason", ""))[:200]
    except Exception as e:
        log.warning("ai_check: %s", e)
        return True, "AI non disponibile, valgono solo le regole"


async def evaluate_buy(client, mint, trader_sol):
    """Ritorna (copiare?, motivo)."""
    if not FILTER_ENABLED:
        return True, "filtri disattivati"
    try:
        m = await fetch_metrics(client, mint)
        if m is None:  # token appena nato: riprova una volta
            await asyncio.sleep(4)
            m = await fetch_metrics(client, mint)
    except Exception as e:
        return False, f"errore dati di mercato: {e}"
    if m is None:
        return False, "nessun dato di mercato ancora"
    why = rule_check(m)
    if why:
        return False, "; ".join(why)
    if RUGCHECK:
        dangers = await rugcheck_dangers(client, mint)
        if dangers:
            return False, "rischi RugCheck: " + ", ".join(dangers[:3])
    ok, reason = await ai_check(client, m, trader_sol)
    if not ok:
        return False, "AI: " + reason
    tag = f"{m['symbol']} liq ${m['liq_usd']:.0f}, vol1h ${m['vol_h1']:.0f}, {m['buys_h1']}/{m['sells_h1']} acq/vend 1h"
    return True, tag + (f" — AI: {reason}" if reason else "")


# ---------------- ESECUZIONE ORDINI (PumpPortal, firma locale) ----------------
async def do_trade(client, action, mint, amount):
    """amount: SOL (float) per buy, stringa tipo '50%' per sell. Ritorna (ok, testo)."""
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
        return False, f"Non confermata (controlla): https://solscan.io/tx/{sig}"
    except Exception as e:
        return False, f"Errore: {e}"


async def copy_trade(app, client, trader, t):
    mint, who = t["mint"], short(trader)
    link = f"https://pump.fun/coin/{mint}"
    if t["side"] == "buy":
        if t["sol"] < MIN_TRADER_SOL:
            dg(trader)["small"] += 1
            return  # acquisto di prova / troppo piccolo: ignoro
        amount = S["budget"] * PCT_PER_TRADE / 100
        if MAX_SOL_PER_TRADE > 0:
            amount = min(amount, MAX_SOL_PER_TRADE)
        amount = round(min(amount, S["budget"] - exposure()), 4)
        if amount < 0.005:
            await notify(app, f"⚠️ {who} ha comprato {link}\nBudget esaurito, salto.")
            return
        go, verdict = await evaluate_buy(client, mint, t["sol"])
        if not go:
            S["skipped"] += 1
            save_state()
            await notify(app, f"⏭ SKIP (trade di {who}, {t['sol']:.2f} SOL)\n{link}\nMotivo: {verdict}")
            return
        ok, info = await do_trade(client, "buy", mint, amount)
        if ok:
            p = S["positions"].get(mint)
            if p:
                p["cost"] = p["cost"] * p["left"] + amount
                p["left"] = 1.0
            else:
                S["positions"][mint] = {"cost": amount, "left": 1.0}
            S["trades"] += 1
            save_state()
        await notify(app, f"{'🟢' if ok else '🔴'} BUY {amount} SOL (copia di {who})\n{link}\n✔ {verdict}\n{info}")
    else:
        pos = S["positions"].get(mint)
        if not pos:
            dg(trader)["sell_nopos"] += 1
            return
        pct = 100 if t["pct"] >= 95 else max(1, int(round(t["pct"])))
        ok, info = await do_trade(client, "sell", mint, f"{pct}%")
        if ok:
            pos["left"] *= 1 - pct / 100
            if pos["left"] < 0.02:
                del S["positions"][mint]
            S["trades"] += 1
            save_state()
        await notify(app, f"{'🟠' if ok else '🔴'} SELL {pct}% (copia di {who})\n{link}\n{info}")


async def close_all(app, client):
    for mint in list(S["positions"]):
        ok, info = await do_trade(client, "sell", mint, "100%")
        if ok:
            del S["positions"][mint]
        await notify(app, f"{'🟠' if ok else '🔴'} Chiusura {mint}\n{info}")
    save_state()


# ---------------- MONITOR ----------------
async def get_tx(client, sig):
    """Scarica una transazione; prova prima la versione 1 (nuove transazioni Solana), poi la 0."""
    for ver in (1, 0):
        try:
            return await rpc(client, "getTransaction", [sig, {
                "encoding": "jsonParsed", "maxSupportedTransactionVersion": ver, "commitment": "confirmed"}])
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
    for _ in range(5):  # fino a 5 pagine: evita di perdere trade quando il wallet fa molte transazioni
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
                    d["last_nt"] = describe_tx(tx, trader)
                for t in found:
                    d["trades"] += 1
                    await copy_trade(app, client, trader, t)
        except Exception as e:  # una transazione problematica non deve bloccare le altre
            log.warning("tx %s: %s", sig[:8], e)
            d["err"] = str(e)[:150]
        S["last"][trader] = sig  # avanza sempre, così non rileggo all'infinito le stesse firme
    save_state()


async def monitor(app):
    async with httpx.AsyncClient() as client:
        try:
            for t in S["traders"]:
                if not S["last"].get(t):
                    sigs = await rpc(client, "getSignaturesForAddress", [t, {"limit": 1, "commitment": "confirmed"}])
                    S["last"][t] = sigs[0]["signature"] if sigs else None
            save_state()
            while S["active"]:
                if time.time() >= S["end"]:
                    S["active"] = False
                    await notify(app, "⏱ Durata terminata. Chiudo le posizioni aperte.")
                    await close_all(app, client)
                    await notify(app, "Sessione chiusa.")
                    save_state()
                    return
                for t in S["traders"]:
                    try:
                        await check_trader(app, client, t)
                    except Exception as e:
                        log.warning("check_trader %s: %s", short(t), e)
                        dg(t)["err"] = str(e)[:150]
                await asyncio.sleep(POLL_SECONDS)
        except Exception as e:
            log.exception("monitor")
            await notify(app, f"🔴 Errore nel monitor: {e}")


# ---------------- COMANDI TELEGRAM ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("Non sei autorizzato a usare questo bot.")
        return ConversationHandler.END
    if S["active"]:
        await update.message.reply_text("C'è già una sessione attiva. Usa /stop per chiuderla.")
        return ConversationHandler.END
    mode = "SIMULAZIONE (nessun soldo reale)" if DRY_RUN else "REALE ⚠️"
    await update.message.reply_text(
        f"Bot Copy-Trading — modalità: {mode}\n\n"
        "Mandami gli indirizzi wallet dei trader da copiare (max 5), separati da spazio o virgola."
    )
    return GET_TRADERS


async def get_traders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    items = [t for t in re.split(r"[,\s]+", update.message.text.strip()) if t]
    bad = [t for t in items if not ADDR_RE.match(t)]
    if not items or bad or len(items) > 5:
        await update.message.reply_text("Indirizzi non validi (servono da 1 a 5 indirizzi Solana). Riprova:")
        return GET_TRADERS
    context.user_data["traders"] = list(dict.fromkeys(items))
    await update.message.reply_text(f"Budget massimo in SOL da usare (es. 0.1)? Per ogni trade copiato userò il {PCT_PER_TRADE:.0f}% del budget.")
    return GET_BUDGET


async def get_budget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        budget = float(update.message.text.strip().replace(",", "."))
        if budget <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Inserisci un numero valido (es. 0.1):")
        return GET_BUDGET
    if not DRY_RUN:
        try:
            async with httpx.AsyncClient() as c:
                bal = await wallet_balance(c)
        except Exception as e:
            await update.message.reply_text(f"Non riesco a leggere il saldo del wallet: {e}")
            return ConversationHandler.END
        if bal is None or budget > bal - 0.02:
            await update.message.reply_text(
                f"Saldo wallet: {bal} SOL. Mi servono budget + circa 0.02 SOL per le fee. Sessione interrotta.")
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
    S.update({
        "active": True,
        "traders": context.user_data["traders"],
        "budget": context.user_data["budget"],
        "end": time.time() + hours * 3600,
        "positions": {},
        "last": {},
        "trades": 0,
        "skipped": 0,
        "diag": {},
    })
    save_state()
    context.application.create_task(monitor(context.application))
    await update.message.reply_text(
        f"✅ Sessione avviata ({'SIMULAZIONE' if DRY_RUN else 'REALE'})\n"
        f"Trader: {', '.join(short(t) for t in S['traders'])}\n"
        f"Budget: {S['budget']} SOL ({PCT_PER_TRADE:.0f}% per trade)\n"
        f"Filtro hype: {'ATTIVO' if FILTER_ENABLED else 'spento'}{' + AI' if GEMINI_API_KEY else ''}\n"
        f"Durata: {hours} ore\n\n"
        "Ti avviso a ogni trade copiato. /resoconto per il riepilogo, /stop per chiudere tutto."
    )
    return ConversationHandler.END


async def resoconto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    lines = [f"Stato: {'ATTIVA' if S['active'] else 'ferma'} — {'SIMULAZIONE' if DRY_RUN else 'REALE'}"]
    if S["active"]:
        lines.append(f"Tempo rimasto: {max(0, int((S['end'] - time.time()) / 60))} min")
        lines.append(f"Trader: {', '.join(short(t) for t in S['traders'])}")
    lines.append(f"Esposizione: {exposure():.4f} / {S['budget']} SOL")
    lines.append(f"Ordini eseguiti: {S['trades']} — trade scartati dal filtro: {S.get('skipped', 0)}")
    for mint, p in list(S["positions"].items())[:10]:
        lines.append(f"• {mint[:6]}… costo {p['cost'] * p['left']:.4f} SOL")
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
            f"• transazioni lette: {d['txs']}\n"
            f"• lette ma non riconosciute come trade: {d['notrade']}\n"
            f"• ultima non riconosciuta: {d['last_nt'] or '-'}\n"
            f"• trade riconosciuti: {d['trades']}\n"
            f"• acquisti ignorati perché piccoli: {d['small']}\n"
            f"• vendite ignorate (token non posseduto): {d['sell_nopos']}\n"
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
    await update.message.reply_text("Fermo la sessione e vendo le posizioni aperte…")
    async with httpx.AsyncClient() as c:
        await close_all(context.application, c)
    await update.message.reply_text("Fatto.")


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Operazione annullata.")
    return ConversationHandler.END


# ---------------- Flask per Render ----------------
web = Flask(__name__)


@web.route("/")
def home():
    return "Bot is running"


def run_flask():
    web.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), use_reloader=False)


async def post_init(app):
    load_state()
    if S.get("active"):
        await notify(app, "♻️ Bot riavviato: riprendo la sessione attiva.")
        app.create_task(monitor(app))


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
