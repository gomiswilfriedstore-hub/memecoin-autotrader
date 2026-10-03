"""
Bot Telegram - pump.fun "Top movers" auto-trader
Filtres = ceux des captures d'écran (Audit 4 + Métriques 4 = 8 filtres).
Achat : 30% du solde SOL dispo (hors réserve de frais) à chaque achat. Le scan
        continue tant qu'il reste assez de SOL : plusieurs positions coexistent.
Sortie : vend 80% du restant à chaque palier de +50% (calculé depuis le prix de
         la dernière vente), stop loss -25%, et vente totale si aucun nouveau
         plus haut pendant 3 min (stagnation).

Commandes Telegram : /on /off /status /sellall
Lance d'abord en DRY_RUN=true pour vérifier que tout fonctionne.
"""
import asyncio
import json
import logging
import os
import time

import aiohttp
import base58
from aiohttp import web
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
logging.getLogger("httpx").setLevel(logging.WARNING)  # évite d'écrire le token Telegram dans les logs
logging.getLogger("httpcore").setLevel(logging.WARNING)
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
    mcap_max_usd=100_000,
    volume24h_min_usd=15_000,
    buys_min=200,
)

# Stratégie de sortie
TIER_MULT = 1.5          # palier = +50%
TIER_SELL_PCT = 80       # on vend 80% du restant à chaque palier
STOP_LOSS = 0.25         # -25%
STOP_FROM_PEAK = True    # True = trailing (depuis le plus haut) / False = depuis le prix d'entrée

# Exécution
SOL_RESERVE = 0.02       # SOL gardés pour frais/rent (ne pas mettre 0)
BUY_FRACTION = 0.30      # chaque achat utilise 30% des fonds disponibles (hors réserve)
MIN_BUY_SOL = 0.02       # taille d'achat minimale ; en dessous, le bot n'achète pas (mais continue de scanner)
STAGNATION_SEC = 180     # pas de nouveau plus haut pendant 3 min -> vente totale
CONFIRM_WAIT = 2         # s d'attente après un achat confirmé
CONFIRM_TIMEOUT = 40     # s max pour qu'une transaction soit confirmée sur la blockchain
TX_RETRIES = 3           # nouvelles tentatives si la transaction a échoué (slippage, etc.)
SLIPPAGE = 25            # %
PRIORITY_FEE = 0.001     # SOL
SCAN_EVERY = 10          # s
MIN_AGE_MIN = 3          # on n'évalue un coin qu'après 3 min (il faut le temps d'avoir du volume)
MAX_PER_SCAN = 60        # coins confirmés par DexScreener par cycle (2 appels max)
STREAM_FRESH = 20        # s : un prix du flux est "frais" s'il a moins de 20 s
PRESELECT = 0.8          # pré-filtre local : seuils x0.8 (tolérance), DexScreener confirme ensuite
WS_URL = "wss://pumpportal.fun/api/data"   # flux gratuit des nouveaux tokens
MONITOR_EVERY = 3        # s
BLIND_SELL_SEC = 90      # pas de prix pendant 90 s -> vente totale de sécurité

S = {"on": False, "pos": {}, "seen": {}, "bought": set(), "sampled": False, "fail": {}, "tracked": {}, "scan_pause": 0, "sol_usd": 0.0, "sol_ts": 0, "ws": None, "trades_n": 0, "virtual": None, "stats": {}, "stats_ts": time.time()}
RECHECK_AFTER = 60  # s : un coin refusé est réévalué après 60 s (volume/txs évoluent)


# ----------------------------- DONNÉES --------------------------------
# ⚠️ L'onglet "Top movers" n'a pas d'API publique documentée. Cette partie
# utilise l'API frontend de pump.fun (non officielle, peut changer) : adapte
# CANDIDATES_URL et parse_coin() si les noms de champs ne correspondent pas.
CANDIDATES_URL = ("https://frontend-api-v3.pump.fun/coins"
                  "?offset={off}&limit=50&sort=created_timestamp&order=DESC&includeNsfw=false")
PAGES = (0, 50, 100)   # utilisé une seule fois au démarrage (150 coins les plus récents)
DEX_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"  # volume 24h + nb d'achats
COIN_URL = "https://frontend-api-v3.pump.fun/coins/{mint}"


def parse_coin(c: dict) -> dict:
    g = c.get
    return dict(
        mint=g("mint"),
        creator=g("creator"),
        curve_ata=g("associated_bonding_curve"),
        bonding_curve=g("bonding_curve"),
        pool=g("pool_address") or g("pump_swap_pool"),
        age_min=(time.time() * 1000 - g("created_timestamp", 0)) / 60000 if g("created_timestamp") else None,
        mcap=g("usd_market_cap"),
        volume24h=None,   # rempli par dex_ok() (DexScreener)
        buys=None,        # rempli par dex_ok() (DexScreener)
        fees_sol=None,    # non utilisé
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


DEX_BATCH = "https://api.dexscreener.com/tokens/v1/solana/{mints}"   # jusqu'à 30 mints par appel


async def dex_batch(s, mints):
    """DexScreener (public) -> {mint: {price, mcap, volume24h, buys}} ; 1 appel pour 30 coins."""
    out = {}
    for i in range(0, len(mints), 30):
        chunk = mints[i:i + 30]
        async with s.get(DEX_BATCH.format(mints=",".join(chunk)), timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 429:
                S["scan_pause"] = time.time() + 30   # le scanner fait une pause de 30 s
                raise RuntimeError("DexScreener 429 (trop de requêtes) : pause 30 s")
            r.raise_for_status()
            data = await r.json()
        await asyncio.sleep(0.3)
        pairs = data if isinstance(data, list) else (data.get("pairs") or [])
        for p in pairs:
            m = (p.get("baseToken") or {}).get("address")
            if m not in chunk or p.get("chainId") != "solana":
                continue
            d = out.setdefault(m, dict(price=0.0, mcap=0.0, volume24h=0.0, buys=0, liq=-1))
            d["volume24h"] += (p.get("volume") or {}).get("h24") or 0
            d["buys"] += ((p.get("txns") or {}).get("h24") or {}).get("buys") or 0
            liq = (p.get("liquidity") or {}).get("usd") or 0
            if liq > d["liq"]:   # prix = celui du pool le plus liquide
                d["liq"] = liq
                d["price"] = float(p.get("priceUsd") or 0)
                pn = float(p.get("priceNative") or 0)
                d["sol_usd"] = d["price"] / pn if pn else 0   # cours SOL/USD implicite
                d["mcap"] = p.get("marketCap") or p.get("fdv") or 0
                d["pair"] = p.get("pairAddress")
                if d["sol_usd"] and time.time() - S["sol_ts"] > 300:
                    S["sol_usd"] = d["sol_usd"]   # repli si Coinbase est indisponible
    return out


async def top10_pct(s, coin):
    """% détenu par les 10 plus gros holders, hors bonding curve / pool de liquidité."""
    res = await rpc(s, "getTokenLargestAccounts", [coin["mint"]])
    if not res:
        return None
    accs = res["value"]
    info = await rpc(s, "getMultipleAccounts", [[a["address"] for a in accs], {"encoding": "jsonParsed"}])
    owners = (info or {}).get("value") or [None] * len(accs)
    skip = {coin["bonding_curve"], coin["pool"]} - {None}
    kept = []
    for a, acc in zip(accs, owners):
        try:
            owner = acc["data"]["parsed"]["info"]["owner"]
        except Exception:
            owner = None
        if a["address"] == coin["curve_ata"] or owner in skip:
            continue
        kept.append(float(a["uiAmount"] or 0))
    return sum(kept[:10]) / 1e9 * 100   # supply pump.fun = 1B


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


def reject(why):
    S["stats"][why] = S["stats"].get(why, 0) + 1
    return False


def dex_ok(c, d) -> bool:
    """Filtres DexScreener : market cap, volume 24h, achats."""
    f = FILTERS
    if not d or not d["price"]:
        return reject("pas_sur_dexscreener")
    c["price"], c["volume24h"], c["buys"] = d["price"], d["volume24h"], d["buys"]
    c["mcap"], c["pool"] = d["mcap"], d.get("pair")
    if not (f["mcap_min_usd"] <= c["mcap"] <= f["mcap_max_usd"]):
        return reject("mcap")
    if d["volume24h"] < f["volume24h_min_usd"]:
        return reject("volume")
    if d["buys"] < f["buys_min"]:
        return reject("achats")
    return True


async def onchain_ok(s, c) -> bool:
    """Filtres RPC (les plus coûteux, donc en dernier)."""
    f = FILTERS
    t10 = await top10_pct(s, c)
    if t10 is None or t10 > f["top10_max_pct"]:
        return reject("top10")
    dv = await dev_pct(s, c)
    if dv is None or dv > f["dev_max_pct"]:
        return reject("dev")
    if not await holders_ok(s, c, f["holders_min"]):
        return reject("holders")
    return True


# ----------------------------- TRADING --------------------------------
async def sol_balance(s) -> float:
    res = await rpc(s, "getBalance", [str(KP.pubkey())])
    return res["value"] / 1e9


async def available(s) -> float:
    """Solde utilisable. En DRY_RUN : portefeuille virtuel (DRY_RUN_BALANCE, 1 SOL par défaut)."""
    if DRY_RUN:
        if S["virtual"] is None:
            S["virtual"] = float(os.getenv("DRY_RUN_BALANCE", "1"))
        return S["virtual"]
    return await sol_balance(s)


class TxUncertain(Exception):
    """Transaction envoyée mais statut inconnu après le délai : on vérifie via le solde de tokens."""
    def __init__(self, sig):
        super().__init__(f"transaction non confirmée après {CONFIRM_TIMEOUT}s ({sig})")
        self.sig = sig


async def confirm(s, sig):
    """-> 'ok' | 'failed:<erreur>' | 'unknown'  (vérifie vraiment sur la blockchain)."""
    t0 = time.time()
    while time.time() - t0 < CONFIRM_TIMEOUT:
        res = await rpc(s, "getSignatureStatuses", [[sig], {"searchTransactionHistory": True}])
        st = ((res or {}).get("value") or [None])[0]
        if st:
            if st.get("err"):
                return "failed:" + json.dumps(st["err"])
            if st.get("confirmationStatus") in ("confirmed", "finalized"):
                return "ok"
        await asyncio.sleep(1.5)
    return "unknown"


async def token_balance(s, mint) -> float:
    res = await rpc(s, "getTokenAccountsByOwner", [str(KP.pubkey()), {"mint": mint}, {"encoding": "jsonParsed"}])
    return sum(float(a["account"]["data"]["parsed"]["info"]["tokenAmount"]["uiAmount"] or 0)
               for a in ((res or {}).get("value") or []))


async def tx_sol_delta(s, sig):
    """Variation réelle du solde SOL du wallet causée par la transaction (frais inclus), ou None."""
    try:
        res = await rpc(s, "getTransaction", [sig, {"encoding": "json", "commitment": "confirmed",
                                                    "maxSupportedTransactionVersion": 0}])
        meta = res["meta"]
        return (meta["postBalances"][0] - meta["preBalances"][0]) / 1e9   # index 0 = notre wallet (payeur)
    except Exception:
        return None


async def trade(s, action, mint, amount, in_sol):
    """Envoie l'ordre ET attend sa confirmation. Retourne la signature, ou lève une erreur."""
    if DRY_RUN:
        log.info("[DRY_RUN] %s %s amount=%s", action, mint, amount)
        return "dry-run"
    last = "?"
    for attempt in range(1, TX_RETRIES + 1):
        payload = dict(publicKey=str(KP.pubkey()), action=action, mint=mint, amount=amount,
                       denominatedInSol="true" if in_sol else "false",
                       slippage=SLIPPAGE, priorityFee=PRIORITY_FEE, pool="auto")
        async with s.post("https://pumpportal.fun/api/trade-local", data=payload) as r:
            if r.status != 200:
                last = f"PumpPortal {r.status}: {(await r.text())[:150]}"
                await asyncio.sleep(1)
                continue
            raw = await r.read()
        tx = VersionedTransaction(VersionedTransaction.from_bytes(raw).message, [KP])
        body = SendVersionedTransaction(
            tx, RpcSendTransactionConfig(preflight_commitment=CommitmentLevel.Confirmed)).to_json()
        async with s.post(RPC_URL, data=body, headers={"Content-Type": "application/json"}) as r:
            resp = await r.json()
        sig = resp.get("result")
        if not sig:
            last = f"envoi refusé: {str(resp.get('error'))[:150]}"   # rien n'est parti : on peut réessayer
            await asyncio.sleep(1)
            continue
        status = await confirm(s, sig)
        if status == "ok":
            return sig
        if status == "unknown":
            raise TxUncertain(sig)          # peut-être passée plus tard : ne PAS renvoyer (risque de doublon)
        last = f"{status} (tentative {attempt}/{TX_RETRIES})"   # échec confirmé on-chain : on réessaie
        log.warning("%s %s: %s", action, mint, last)
    raise RuntimeError(f"échec après {TX_RETRIES} tentatives: {last}")


async def buy(s, mint, size):
    """Achat vérifié. -> (signature, coût réel en SOL)."""
    try:
        sig = await trade(s, "buy", mint, size, True)
    except TxUncertain as e:
        if await token_balance(s, mint) <= 0:
            raise RuntimeError(f"achat non confirmé et aucun token reçu ({e.sig})")
        sig = e.sig                         # les tokens sont bien arrivés
    delta = await tx_sol_delta(s, sig) if not DRY_RUN else None
    return sig, (-delta if delta and delta < 0 else size)


async def notify(app, text):
    log.info(text)
    await app.bot.send_message(OWNER_ID, text)


# ----------------------------- BOUCLES --------------------------------
def throttled_warn(key, msg):
    if time.time() - S.setdefault("warn", {}).get(key, 0) > 30:
        S["warn"][key] = time.time()
        log.warning(msg)


async def refresh_sol_usd(s):
    """Cours SOL/USD (Coinbase, public). Repli : cours implicite de DexScreener."""
    if time.time() - S["sol_ts"] < 60:
        return
    try:
        data = await get_json(s, "https://api.coinbase.com/v2/prices/SOL-USD/spot")
        S["sol_usd"], S["sol_ts"] = float(data["data"]["amount"]), time.time()
    except Exception as e:
        throttled_warn("sol", f"cours SOL/USD: {e}")


def local_ok(t) -> bool:
    """Pré-filtre gratuit à partir du flux de trades : évite d'appeler DexScreener pour rien."""
    f, sol = FILTERS, S["sol_usd"]
    mc = t["mcap_sol"] * sol
    return (t["buys"] >= f["buys_min"] * PRESELECT
            and t["vol_sol"] * sol >= f["volume24h_min_usd"] * PRESELECT
            and f["mcap_min_usd"] * PRESELECT <= mc <= f["mcap_max_usd"] / PRESELECT)


async def ws_subscribe(ws, mints):
    for i in range(0, len(mints), 50):
        await ws.send_json({"method": "subscribeTokenTrade", "keys": mints[i:i + 50]})


async def new_token_feed(app):
    """Flux PumpPortal : nouveaux tokens + leurs trades (volume, achats, market cap) en temps réel."""
    s = app.bot_data["session"]
    while True:
        try:
            async with s.ws_connect(WS_URL, heartbeat=30) as ws:
                S["ws"] = ws
                await ws.send_json({"method": "subscribeNewToken"})
                if S["tracked"]:
                    await ws_subscribe(ws, list(S["tracked"]))   # après une reconnexion
                log.info("Flux nouveaux tokens connecté")
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    d = json.loads(msg.data)
                    m, tx = d.get("mint"), d.get("txType")
                    if not m:
                        continue
                    sol = d.get("solAmount") or 0
                    if tx == "create":
                        S["tracked"][m] = dict(created=time.time(), creator=d.get("traderPublicKey"),
                                               bonding_curve=d.get("bondingCurveKey"),
                                               symbol=d.get("symbol") or "?", pool=None, wait=RECHECK_AFTER,
                                               buys=1 if sol else 0, sells=0, vol_sol=float(sol),
                                               mcap_sol=float(d.get("marketCapSol") or 0), last_trade=time.time())
                        await ws_subscribe(ws, [m])
                    elif tx in ("buy", "sell") and m in S["tracked"]:
                        t = S["tracked"][m]
                        t["buys" if tx == "buy" else "sells"] += 1
                        t["vol_sol"] += float(sol)
                        if d.get("marketCapSol"):
                            t["mcap_sol"] = float(d["marketCapSol"])
                        t["last_trade"] = time.time()
                        S["trades_n"] += 1
        except Exception as e:
            throttled_warn("ws", f"flux tokens: {e}")
        S["ws"] = None
        await asyncio.sleep(5)


async def scanner(app):
    s = app.bot_data["session"]
    while True:
        await asyncio.sleep(SCAN_EVERY)
        if not S["on"]:
            continue
        try:
            await refresh_sol_usd(s)
            bal = await available(s)
            if (bal - SOL_RESERVE) * BUY_FRACTION < MIN_BUY_SOL:
                if time.time() - S.get("lowlog", 0) > 300:
                    log.info("Solde insuffisant (%.4f SOL, il faut > %.2f) : scan en pause", bal, SOL_RESERVE + MIN_BUY_SOL / BUY_FRACTION)
                    S["lowlog"] = time.time()
                continue  # on attend qu'une vente en libère
            now = time.time()
            if now < S["scan_pause"] or not S["sol_usd"]:
                continue
            old = [m for m, t in S["tracked"].items()
                   if now - t["created"] > (FILTERS["age_max_min"] + 5) * 60 and m not in S["pos"]]
            for m in old:
                del S["tracked"][m]   # trop vieux : on arrête de le suivre
            if old and S["ws"] is not None:
                try:
                    await S["ws"].send_json({"method": "unsubscribeTokenTrade", "keys": old[:50]})
                except Exception:
                    pass
            due = []
            for m, t in S["tracked"].items():
                age = (now - t["created"]) / 60
                if MIN_AGE_MIN <= age <= FILTERS["age_max_min"] and m not in S["bought"] \
                        and now - S["seen"].get(m, 0) >= t["wait"] and local_ok(t):
                    due.append((S["seen"].get(m, 0), m, age))
            due.sort()
            cands = []
            for _, m, age in due[:MAX_PER_SCAN]:
                t = S["tracked"][m]
                cands.append(dict(mint=m, creator=t["creator"], bonding_curve=t["bonding_curve"], curve_ata=None,
                                  pool=t["pool"], age_min=age, mcap=None, volume24h=None, buys=None,
                                  fees_sol=None, symbol=t["symbol"]))
            S["stats"]["évalués"] = S["stats"].get("évalués", 0) + len(cands)
            if cands:
                info = await dex_batch(s, [c["mint"] for c in cands])
                for c in cands:
                    m = c["mint"]
                    S["seen"][m] = time.time()
                    d = info.get(m)
                    S["tracked"][m]["wait"] = 180 if not d else RECHECK_AFTER
                    if not dex_ok(c, d) or not await onchain_ok(s, c):
                        continue
                    S["stats"]["OK"] = S["stats"].get("OK", 0) + 1
                    metric = S["tracked"][m]["mcap_sol"] or (c["mcap"] / S["sol_usd"])
                    size = (await available(s) - SOL_RESERVE) * BUY_FRACTION
                    if size < MIN_BUY_SOL or not metric:
                        break
                    size = round(size, 4)
                    try:
                        sig, size = await buy(s, m, size)   # attend la confirmation on-chain
                    except Exception as e:
                        S["fail"][m] = S["fail"].get(m, 0) + 1
                        if S["fail"][m] >= 2:
                            S["bought"].add(m)              # 2 échecs : on abandonne ce coin
                        await notify(app, f"⚠️ Achat ÉCHOUÉ {c['symbol']} ({m}): {e}\nAucune position ouverte.")
                        continue
                    if DRY_RUN:
                        S["virtual"] -= size
                    S["bought"].add(m)
                    S["pos"][m] = dict(symbol=c["symbol"], entry=metric, peak=metric, last=metric,
                                       next_tier=metric * TIER_MULT, cost=size, frac=1.0,
                                       last_high=time.time(), blind=None)
                    await notify(app, f"🟢 ACHAT confirmé {c['symbol']} ({m})\n{size:.4f} SOL @ mcap ${c['mcap']:,.0f}\n{tx_link(sig)}")
                    await asyncio.sleep(CONFIRM_WAIT)
                    break
        except Exception as e:
            throttled_warn("scanner", f"scanner: {e}")
        if time.time() - S["stats_ts"] > 60:
            log.info("Bilan 60s | suivis: %d | trades reçus: %d | SOL=$%.0f | rejets: %s",
                     len(S["tracked"]), S["trades_n"], S["sol_usd"], S["stats"])
            S["stats"], S["stats_ts"], S["trades_n"] = {}, time.time(), 0


def tx_link(sig):
    return "tx: simulation (DRY_RUN)" if sig == "dry-run" else f"tx: https://solscan.io/tx/{sig}"


async def sell(app, s, mint, pct, reason, price):
    """Vente vérifiée : l'état de la position ne change qu'après confirmation."""
    p = S["pos"][mint]
    before = await token_balance(s, mint) if not DRY_RUN else None
    if before is not None and before <= 0:
        del S["pos"][mint]
        await notify(app, f"ℹ️ {p['symbol']}: aucun token restant, position retirée.")
        return
    try:
        sig = await trade(s, "sell", mint, f"{pct}%", False)
    except TxUncertain as e:
        after = await token_balance(s, mint)
        if after >= before * 0.999:
            raise RuntimeError(f"vente non confirmée, tokens inchangés ({e.sig}) : nouvelle tentative")
        sig = e.sig                         # les tokens ont bien diminué : la vente est passée
    delta = await tx_sol_delta(s, sig) if not DRY_RUN else None
    proceeds = delta if delta is not None else p["cost"] * p["frac"] * (pct / 100) * (price / p["entry"])
    p["frac"] *= 1 - pct / 100
    if DRY_RUN:
        S["virtual"] += proceeds
    await notify(app, f"🔴 {reason}: vente {pct}% {p['symbol']} à x{price / p['entry']:.2f} "
                      f"({'+' if proceeds >= 0 else ''}{proceeds:.3f} SOL {'reçus' if delta is not None else 'estimés'})\n{tx_link(sig)}")
    if pct == 100:
        del S["pos"][mint]


async def monitor(app):
    s = app.bot_data["session"]
    while True:
        await asyncio.sleep(MONITOR_EVERY)
        if not S["pos"]:
            continue
        now, sol = time.time(), S["sol_usd"]
        prices, stale = {}, []
        for m in S["pos"]:
            t = S["tracked"].get(m)
            if t and t["mcap_sol"] and now - t["last_trade"] < STREAM_FRESH:
                prices[m] = t["mcap_sol"]          # prix frais du flux (market cap en SOL)
            else:
                stale.append(m)
        if stale and sol:                           # coin silencieux ou migré : on interroge DexScreener
            try:
                for m, d in (await dex_batch(s, stale)).items():
                    if d["mcap"]:
                        prices[m] = d["mcap"] / sol
            except Exception as e:
                throttled_warn("monitor", f"monitor: {e}")
        for m in stale:                             # repli : dernier prix connu du flux (< 2 min)
            t = S["tracked"].get(m)
            if m not in prices and t and t["mcap_sol"] and now - t["last_trade"] < 120:
                prices[m] = t["mcap_sol"]
        for mint, p in list(S["pos"].items()):
            try:
                price = prices.get(mint)
                if not price:
                    p["blind"] = p["blind"] or now
                    if now - p["blind"] >= BLIND_SELL_SEC:   # plus de prix : on sort par sécurité
                        await sell(app, s, mint, 100, f"PRIX INDISPONIBLE {BLIND_SELL_SEC}s", p["last"])
                    continue
                p["blind"], p["last"] = None, price
                if price > p["peak"]:
                    p["peak"], p["last_high"] = price, now
                ref = p["peak"] if STOP_FROM_PEAK else p["entry"]
                if price <= ref * (1 - STOP_LOSS):
                    await sell(app, s, mint, 100, "STOP LOSS", price)
                elif price >= p["next_tier"]:
                    await sell(app, s, mint, TIER_SELL_PCT, "PALIER +50%", price)
                    p["next_tier"] = price * TIER_MULT  # +50% depuis le prix de cette vente
                elif now - p["last_high"] >= STAGNATION_SEC:
                    await sell(app, s, mint, 100, f"STAGNATION {STAGNATION_SEC // 60} min", price)
            except Exception as e:
                throttled_warn("monitor_" + mint, f"monitor {mint}: {e}")
                if time.time() - p.get("fail_notif", 0) > 60:
                    p["fail_notif"] = time.time()
                    await notify(app, f"⚠️ Vente en échec {p['symbol']}: {e}\nNouvelle tentative automatique.")


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
        txt += (f"• {p['symbol']}: actuel x{p['last'] / p['entry']:.2f}, pic x{p['peak'] / p['entry']:.2f}, "
                f"reste {p['frac'] * 100:.0f}%\n")
    await u.message.reply_text(txt)


@owner_only
async def cmd_sellall(u, ctx):
    app, s = ctx.application, ctx.application.bot_data["session"]
    for m, p in list(S["pos"].items()):
        try:
            await sell(app, s, m, 100, "VENTE MANUELLE", p["last"])
        except Exception as e:
            await u.message.reply_text(f"Échec vente {p['symbol']}: {e}")


def find_fee_fields(obj, path="", out=None, depth=0):
    out = {} if out is None else out
    if depth > 4:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            if "fee" in str(k).lower() and not isinstance(v, (dict, list)):
                out[path + k] = v
            find_fee_fields(v, path + str(k) + ".", out, depth + 1)
    elif isinstance(obj, list) and obj:
        find_fee_fields(obj[0], path + "[0].", out, depth + 1)
    return out


@owner_only
async def cmd_fees(u, ctx):
    """/fees <mint> : cherche un champ « frais » dans l'API pump.fun pour ce coin (diagnostic)."""
    if not ctx.args:
        await u.message.reply_text("Usage : /fees <adresse du coin>")
        return
    s, mint, lines = ctx.application.bot_data["session"], ctx.args[0], []
    for url in (f"https://frontend-api-v3.pump.fun/coins/{mint}?sync=true",
                f"https://frontend-api-v3.pump.fun/coins/{mint}"):
        try:
            async with s.get(url, headers={"Origin": "https://pump.fun"}, timeout=aiohttp.ClientTimeout(total=10)) as r:
                status = r.status
                data = await r.json(content_type=None) if status == 200 else None
        except Exception as e:
            lines.append(f"{url.split('/coins/')[1][-12:]} : erreur {e}")
            continue
        if data is None:
            lines.append(f"HTTP {status}")
            continue
        lines.append(f"HTTP 200 | champs 'fee' : {find_fee_fields(data) or 'aucun'}")
        break
    await u.message.reply_text("\n".join(lines)[:3500])


async def start_health():
    """Mini serveur HTTP : Render (Web Service) exige un port ouvert, et sert aussi au ping anti-veille."""
    srv = web.Application()
    srv.router.add_get("/", lambda r: web.Response(text="ok"))
    runner = web.AppRunner(srv)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "10000"))).start()


async def post_init(app):
    await start_health()
    app.bot_data["session"] = aiohttp.ClientSession()
    asyncio.create_task(new_token_feed(app))
    asyncio.create_task(scanner(app))
    asyncio.create_task(monitor(app))


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    for name, fn in [("on", cmd_on), ("off", cmd_off), ("status", cmd_status), ("sellall", cmd_sellall), ("fees", cmd_fees)]:
        app.add_handler(CommandHandler(name, fn))
    app.run_polling()


if __name__ == "__main__":
    main()
