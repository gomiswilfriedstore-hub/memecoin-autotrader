"""
Bot Telegram - pump.fun "Top movers" auto-trader
Filtres = ceux des captures d'écran (Audit 4 + Métriques 4 = 8 filtres).
Achat : tout le solde SOL dispo (moins une réserve de frais). Le scan continue
        tant qu'il reste assez de SOL : plusieurs positions peuvent coexister.
Sortie : vend 80% du restant à chaque palier de +50% (calculé depuis le prix de
         la dernière vente), stop loss -25%, et vente totale si aucun nouveau
         plus haut pendant 3 min (stagnation).

Commandes Telegram : /on /off /status /sellall
Lance d'abord en DRY_RUN=true pour vérifier que tout fonctionne.
"""
import asyncio
import base64
import logging
import os
import time

import aiohttp
import base58
from dotenv import load_dotenv
from solders.commitment_config import CommitmentLevel
from solders.keypair import Keypair
from solders.rpc.config import RpcSendTransactionConfig
from solders.rpc.requests import SendVersionedTransaction
from solders.transaction import VersionedTransaction
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("pumpbot")

# ----------------------------- CONFIG ---------------------------------
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])          # seul ton compte peut commander le bot
RPC_URL = os.environ["RPC_URL"]                 # idéalement un RPC Helius (holders via DAS)
KP = Keypair.from_bytes(base58.b58decode(os.environ["PRIVATE_KEY"]))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# Filtres lus sur tes captures (None = pas de filtre)
FILTERS = dict(
    holders_min=200,
    age_max_min=60,
    top10_max_pct=14,
    dev_max_pct=14,
    mcap_min_usd=10_000,
    fees_max_sol=0.5,
    volume24h_min_usd=40_000,
    buys_min=200,
)

# Stratégie de sortie
TIER_MULT = 1.5          # palier = +50%
TIER_SELL_PCT = 80       # on vend 80% du restant à chaque palier
STOP_LOSS = 0.25         # -25%
STOP_FROM_PEAK = True    # True = trailing (depuis le plus haut) / False = depuis le prix d'entrée

# Exécution
SOL_RESERVE = 0.02       # SOL gardés pour frais/rent (ne pas mettre 0)
MIN_BUY_SOL = 0.05       # en dessous, le bot n'achète pas (mais continue de scanner)
STAGNATION_SEC = 180     # pas de nouveau plus haut pendant 3 min -> vente totale
CONFIRM_WAIT = 8         # s d'attente après un achat pour que le solde se mette à jour
SLIPPAGE = 25            # %
PRIORITY_FEE = 0.001     # SOL
SCAN_EVERY = 5           # s
MONITOR_EVERY = 2        # s

S = {"on": False, "pos": {}, "seen": {}, "bought": set(), "sampled": False, "virtual": None}
RECHECK_AFTER = 60  # s : un coin refusé est réévalué après 60 s (volume/txs évoluent)


# ----------------------------- DONNÉES --------------------------------
# ⚠️ L'onglet "Top movers" n'a pas d'API publique documentée. Cette partie
# utilise l'API frontend de pump.fun (non officielle, peut changer) : adapte
# CANDIDATES_URL et parse_coin() si les noms de champs ne correspondent pas.
CANDIDATES_URL = ("https://frontend-api-v3.pump.fun/coins"
                  "?offset=0&limit=50&sort=last_trade_timestamp&order=DESC&includeNsfw=false")
COIN_URL = "https://frontend-api-v3.pump.fun/coins/{mint}"


def parse_coin(c: dict) -> dict:
    g = c.get
    return dict(
        mint=g("mint"),
        creator=g("creator"),
        curve_ata=g("associated_bonding_curve"),
        age_min=(time.time() * 1000 - g("created_timestamp", 0)) / 60000 if g("created_timestamp") else None,
        mcap=g("usd_market_cap"),
        volume24h=g("volume_24h") or g("volume"),     # à vérifier
        buys=g("buy_count") or g("buys"),             # à vérifier
        fees_sol=g("total_fees_sol") or g("fees"),    # à vérifier
        symbol=g("symbol"),
    )


async def get_json(s, url):
    async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
        r.raise_for_status()
        return await r.json()


async def rpc(s, method, params):
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    async with s.post(RPC_URL, json=body) as r:
        return (await r.json()).get("result")


async def top10_pct(s, coin):
    res = await rpc(s, "getTokenLargestAccounts", [coin["mint"]])
    if not res:
        return None
    accs = [a for a in res["value"] if a["address"] != coin["curve_ata"]]  # exclut la bonding curve
    return sum(float(a["uiAmount"] or 0) for a in accs[:10]) / 1e9 * 100   # supply pump.fun = 1B


async def dev_pct(s, coin):
    res = await rpc(s, "getTokenAccountsByOwner",
                    [coin["creator"], {"mint": coin["mint"]}, {"encoding": "jsonParsed"}])
    if res is None:
        return None
    tot = sum(float(a["account"]["data"]["parsed"]["info"]["tokenAmount"]["uiAmount"] or 0)
              for a in res["value"])
    return tot / 1e9 * 100


async def holders_ok(s, coin, minimum):
    """Helius DAS : on a juste besoin de savoir si holders >= minimum."""
    res = await rpc(s, "getTokenAccounts", {"mint": coin["mint"], "limit": 1000})
    if not res:
        return None
    return sum(1 for a in res["token_accounts"] if int(a.get("amount", 0)) > 0) >= minimum


async def passes(s, c) -> bool:
    f = FILTERS
    # filtres rapides d'abord (fail closed : donnée absente = rejet)
    quick = [
        (c["age_min"], lambda v: v <= f["age_max_min"]),
        (c["mcap"], lambda v: v >= f["mcap_min_usd"]),
        (c["volume24h"], lambda v: v >= f["volume24h_min_usd"]),
        (c["buys"], lambda v: v >= f["buys_min"]),
        (c["fees_sol"], lambda v: v <= f["fees_max_sol"]),
    ]
    for val, ok in quick:
        if val is None or not ok(val):
            return False
    # filtres coûteux (appels RPC)
    t10 = await top10_pct(s, c)
    if t10 is None or t10 > f["top10_max_pct"]:
        return False
    dv = await dev_pct(s, c)
    if dv is None or dv > f["dev_max_pct"]:
        return False
    h = await holders_ok(s, c, f["holders_min"])
    return bool(h)


# ----------------------------- TRADING --------------------------------
async def sol_balance(s) -> float:
    res = await rpc(s, "getBalance", [str(KP.pubkey())])
    return res["value"] / 1e9


async def available(s) -> float:
    """Solde utilisable. En DRY_RUN : portefeuille virtuel (achats/ventes simulés)."""
    real = await sol_balance(s)
    if not DRY_RUN:
        return real
    if S["virtual"] is None:
        S["virtual"] = real
    return S["virtual"]


async def fetch_mc(s, mint):
    return parse_coin(await get_json(s, COIN_URL.format(mint=mint)))["mcap"]


async def trade(s, action, mint, amount, in_sol):
    if DRY_RUN:
        log.info("[DRY_RUN] %s %s amount=%s", action, mint, amount)
        return "dry-run"
    payload = dict(publicKey=str(KP.pubkey()), action=action, mint=mint, amount=amount,
                   denominatedInSol="true" if in_sol else "false",
                   slippage=SLIPPAGE, priorityFee=PRIORITY_FEE, pool="auto")
    async with s.post("https://pumpportal.fun/api/trade-local", data=payload) as r:
        if r.status != 200:
            raise RuntimeError(await r.text())
        raw = await r.read()
    tx = VersionedTransaction(VersionedTransaction.from_bytes(raw).message, [KP])
    body = SendVersionedTransaction(
        tx, RpcSendTransactionConfig(preflight_commitment=CommitmentLevel.Confirmed)).to_json()
    async with s.post(RPC_URL, data=body, headers={"Content-Type": "application/json"}) as r:
        return (await r.json()).get("result")


async def notify(app, text):
    log.info(text)
    await app.bot.send_message(OWNER_ID, text)


# ----------------------------- BOUCLES --------------------------------
async def scanner(app):
    s = app.bot_data["session"]
    while True:
        await asyncio.sleep(SCAN_EVERY)
        if not S["on"]:
            continue
        try:
            if await available(s) - SOL_RESERVE < MIN_BUY_SOL:
                continue  # pas assez de SOL : on attend qu'une vente en libère
            coins = await get_json(s, CANDIDATES_URL)
            if coins and not S["sampled"]:
                log.info("SAMPLE COIN (brut): %s", coins[0])
                log.info("SAMPLE COIN (parsé): %s", parse_coin(coins[0]))
                S["sampled"] = True
            for raw in coins:
                c = parse_coin(raw)
                m = c["mint"]
                if not m or m in S["bought"] or time.time() - S["seen"].get(m, 0) < RECHECK_AFTER:
                    continue
                S["seen"][m] = time.time()
                if not await passes(s, c):
                    continue
                size = await available(s) - SOL_RESERVE
                if size < MIN_BUY_SOL:
                    break
                size = round(size, 4)
                sig = await trade(s, "buy", m, size, True)
                if DRY_RUN:
                    S["virtual"] -= size
                S["bought"].add(m)
                S["pos"][m] = dict(symbol=c["symbol"], entry=c["mcap"], peak=c["mcap"],
                                   next_tier=c["mcap"] * TIER_MULT, cost=size, frac=1.0,
                                   last_high=time.time())
                await notify(app, f"🟢 ACHAT {c['symbol']} ({m})\n{size} SOL @ mcap ${c['mcap']:,.0f}\ntx: {sig}")
                await asyncio.sleep(CONFIRM_WAIT)
                break
        except Exception as e:
            log.warning("scanner: %s", e)


async def sell(app, s, mint, pct, reason, mc):
    p = S["pos"][mint]
    sig = await trade(s, "sell", mint, f"{pct}%", False)
    proceeds = p["cost"] * p["frac"] * (pct / 100) * (mc / p["entry"])  # estimation
    p["frac"] *= 1 - pct / 100
    if DRY_RUN:
        S["virtual"] += proceeds
    await notify(app, f"🔴 {reason}: vente {pct}% {p['symbol']} @ ${mc:,.0f} (≈{proceeds:.3f} SOL)\ntx: {sig}")
    if pct == 100:
        del S["pos"][mint]


async def monitor(app):
    s = app.bot_data["session"]
    while True:
        await asyncio.sleep(MONITOR_EVERY)
        for mint, p in list(S["pos"].items()):
            try:
                mc = await fetch_mc(s, mint)
                if not mc:
                    continue
                now = time.time()
                if mc > p["peak"]:
                    p["peak"], p["last_high"] = mc, now
                ref = p["peak"] if STOP_FROM_PEAK else p["entry"]
                if mc <= ref * (1 - STOP_LOSS):
                    await sell(app, s, mint, 100, "STOP LOSS", mc)
                elif mc >= p["next_tier"]:
                    await sell(app, s, mint, TIER_SELL_PCT, "PALIER +50%", mc)
                    p["next_tier"] = mc * TIER_MULT  # +50% depuis le prix de cette vente
                elif now - p["last_high"] >= STAGNATION_SEC:
                    await sell(app, s, mint, 100, f"STAGNATION {STAGNATION_SEC // 60} min", mc)
            except Exception as e:
                log.warning("monitor %s: %s", mint, e)


# ----------------------------- TELEGRAM -------------------------------
def owner_only(fn):
    async def w(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if u.effective_user.id == OWNER_ID:
            await fn(u, ctx)
    return w


@owner_only
async def cmd_on(u, ctx):
    S["on"] = True
    await u.message.reply_text(f"✅ Bot ON ({'DRY_RUN' if DRY_RUN else 'RÉEL'})")


@owner_only
async def cmd_off(u, ctx):
    S["on"] = False
    await u.message.reply_text("⏸ Bot OFF (la position ouverte reste surveillée)")


@owner_only
async def cmd_status(u, ctx):
    s = ctx.application.bot_data["session"]
    txt = f"{'ON' if S['on'] else 'OFF'} | {'DRY_RUN' if DRY_RUN else 'RÉEL'} | solde {await available(s):.4f} SOL\n"
    if not S["pos"]:
        txt += "Aucune position"
    for m, p in S["pos"].items():
        txt += f"• {p['symbol']}: entrée ${p['entry']:,.0f}, pic ${p['peak']:,.0f}, reste {p['frac'] * 100:.0f}%\n"
    await u.message.reply_text(txt)


@owner_only
async def cmd_sellall(u, ctx):
    app, s = ctx.application, ctx.application.bot_data["session"]
    for m, p in list(S["pos"].items()):
        try:
            await sell(app, s, m, 100, "VENTE MANUELLE", await fetch_mc(s, m) or p["entry"])
        except Exception as e:
            await u.message.reply_text(f"Échec vente {p['symbol']}: {e}")


async def post_init(app):
    app.bot_data["session"] = aiohttp.ClientSession()
    asyncio.create_task(scanner(app))
    asyncio.create_task(monitor(app))


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    for name, fn in [("on", cmd_on), ("off", cmd_off), ("status", cmd_status), ("sellall", cmd_sellall)]:
        app.add_handler(CommandHandler(name, fn))
    app.run_polling()


if __name__ == "__main__":
    main()
