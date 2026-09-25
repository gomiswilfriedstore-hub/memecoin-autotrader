import asyncio
import json
import logging
import os
import sys
import time
from typing import Dict, Any, Optional

import aiohttp
from aiohttp import web
import base58
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

# ==========================================
# CONFIGURATION & SÉCURITÉ
# ==========================================

PRIVATE_KEY_B58 = os.getenv("SOLANA_PRIVATE_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Utilisation du RPC public gratuit officiel de Solana
SOLANA_PUBLIC_RPC_URL = "https://api.mainnet-beta.solana.com"

if not PRIVATE_KEY_B58:
    sys.exit("[ERREUR FATALE] La variable SOLANA_PRIVATE_KEY est absente. Arrêt du bot pour sécurité.")

try:
    KEYPAIR = Keypair.from_base58_string(PRIVATE_KEY_B58)
    PUBLIC_KEY_STR = str(KEYPAIR.pubkey())
except Exception as e:
    sys.exit(f"[ERREUR FATALE] Clé privée invalide : {e}")

# ==========================================
# REGLAGES STRATÉGIQUES OPTIMISÉS
# ==========================================
TOKEN_EVALUATION_DELAY = 10      # Temps d'observation (sec)
MIN_LIQUIDITY_USD = 2000.0       # Liquidité min ($)
MIN_MARKET_CAP_USD = 8000.0      # MCap min ($)
MAX_MARKET_CAP_USD = 25000.0     # MCap max ($)
MIN_VOLUME_5M_USD = 1200.0       # Volume min ($)

TRADE_AMOUNT_SOL = 0.01          # Taille de position par trade
SLIPPAGE_PCT = 20.0              # Slippage augmenté à 20% pour compenser la lenteur du RPC
PRIORITY_FEE_SOL = 0.003         # Frais de priorité ajustés pour passer sur RPC public
TRAILING_STOP_PCT = 15.0         # Trailing Stop (%)
MAX_POSITIONS = 3                # Positions simultanées max
STAGNATION_SECONDS = 300         # Vente si stagnation > 5 min

# ==========================================
# ÉTAT GLOBAL & RESSOURCES SYSTEME
# ==========================================
positions: Dict[str, Dict[str, Any]] = {}
scanned_tokens: Dict[str, float] = {}
scanned_count = 0
HTTP_SESSION: Optional[aiohttp.ClientSession] = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# ==========================================
# CLIENT HTTP & TELEGRAM
# ==========================================

async def get_http_session() -> aiohttp.ClientSession:
    global HTTP_SESSION
    if HTTP_SESSION is None or HTTP_SESSION.closed:
        timeout = aiohttp.ClientTimeout(total=12)
        HTTP_SESSION = aiohttp.ClientSession(timeout=timeout)
    return HTTP_SESSION

async def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        session = await get_http_session()
        async with session.post(url, json=payload) as resp:
            pass
    except Exception as e:
        logging.error(f"Erreur d'envoi Telegram : {e}")

# ==========================================
# ENVOI ROBUSTE VERS RPC PUBLIC (AVEC RETRY)
# ==========================================

async def send_raw_tx_to_public_rpc(raw_tx: bytes) -> Optional[str]:
    """Envoie la transaction signée au RPC public avec gestion de réessais."""
    session = await get_http_session()
    rpc_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendTransaction",
        "params": [
            base58.b58encode(raw_tx).decode("utf-8"),
            {"encoding": "base58", "skipPreflight": True, "maxRetries": 5}
        ]
    }
    
    # 3 tentatives en cas de blocage du RPC public
    for attempt in range(3):
        try:
            async with session.post(SOLANA_PUBLIC_RPC_URL, json=rpc_payload) as rpc_resp:
                res_json = await rpc_resp.json()
                if "result" in res_json:
                    return res_json['result']
                elif "error" in res_json:
                    logging.warning(f"Avertissement RPC (tentative {attempt + 1}) : {res_json['error']}")
        except Exception as e:
            logging.warning(f"Erreur réseau RPC (tentative {attempt + 1}) : {e}")
        await asyncio.sleep(1)
    return None

# ==========================================
# EXÉCUTION DES TRANSACTIONS ON-CHAIN
# ==========================================

async def execute_real_buy(mint: str, sol_amount: float) -> bool:
    url = "https://pumpportal.fun/api/trade-local"
    payload = {
        "publicKey": PUBLIC_KEY_STR,
        "action": "buy",
        "mint": mint,
        "denominatedInSol": "true",
        "amount": sol_amount,
        "slippage": SLIPPAGE_PCT,
        "priorityFee": PRIORITY_FEE_SOL,
        "pool": "pump"
    }

    try:
        session = await get_http_session()
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                tx_bytes = await resp.read()
                tx = VersionedTransaction.deserialize(tx_bytes)
                tx.sign([KEYPAIR])

                tx_hash = await send_raw_tx_to_public_rpc(bytes(tx))
                if tx_hash:
                    logging.info(f"✅ BUY EXÉCUTÉ - Tx: https://solscan.io/tx/{tx_hash}")
                    return True
                else:
                    logging.error("❌ Échec envoi transaction sur le RPC public.")
            else:
                err_text = await resp.text()
                logging.error(f"❌ Échec PumpPortal ({resp.status}) : {err_text}")
    except Exception as e:
        logging.error(f"❌ Exception lors de l'achat de {mint} : {e}")
    return False

async def execute_real_sell(mint: str, percentage_str: str = "100%") -> bool:
    url = "https://pumpportal.fun/api/trade-local"
    payload = {
        "publicKey": PUBLIC_KEY_STR,
        "action": "sell",
        "mint": mint,
        "denominatedInSol": "false",
        "amount": percentage_str,
        "slippage": SLIPPAGE_PCT,
        "priorityFee": PRIORITY_FEE_SOL,
        "pool": "auto"
    }

    try:
        session = await get_http_session()
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                tx_bytes = await resp.read()
                tx = VersionedTransaction.deserialize(tx_bytes)
                tx.sign([KEYPAIR])

                tx_hash = await send_raw_tx_to_public_rpc(bytes(tx))
                if tx_hash:
                    logging.info(f"✅ SELL EXÉCUTÉ - Tx: https://solscan.io/tx/{tx_hash}")
                    return True
                else:
                    logging.error("❌ Échec envoi vente sur le RPC public.")
            else:
                err_text = await resp.text()
                logging.error(f"❌ Échec PumpPortal Vente ({resp.status}) : {err_text}")
    except Exception as e:
        logging.error(f"❌ Exception lors de la vente de {mint} : {e}")
    return False

# ==========================================
# ANALYSE ET MONITORING
# ==========================================

async def fetch_dexscreener_data(mint: str) -> Optional[Dict[str, Any]]:
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        session = await get_http_session()
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                pairs = data.get("pairs")
                if pairs:
                    return pairs[0]
    except Exception:
        pass
    return None

async def evaluate_after_delay(mint: str, symbol: str):
    global scanned_count
    await asyncio.sleep(TOKEN_EVALUATION_DELAY)
    
    scanned_count += 1
    scanned_tokens[mint] = time.time()

    dex_data = await fetch_dexscreener_data(mint)
    if not dex_data:
        return

    try:
        liquidity_usd = float(dex_data.get("liquidity", {}).get("usd", 0))
        market_cap = float(dex_data.get("marketCap", dex_data.get("fdv", 0)))
        volume_5m = float(dex_data.get("volume", {}).get("m5", 0))
        txns_5m = dex_data.get("txns", {}).get("m5", {})
        buys = int(txns_5m.get("buys", 0))
        sells = int(txns_5m.get("sells", 0))
        price_usd = float(dex_data.get("priceUsd", 0))
    except (ValueError, TypeError):
        return

    if liquidity_usd < MIN_LIQUIDITY_USD:
        return
    if not (MIN_MARKET_CAP_USD <= market_cap <= MAX_MARKET_CAP_USD):
        return
    if volume_5m < MIN_VOLUME_5M_USD:
        return
    if buys < (sells * 1.2):
        return

    if len(positions) >= MAX_POSITIONS or mint in positions:
        return

    logging.info(f"🎯 Opportunité détectée : {symbol} ({mint[:6]}...)")
    
    buy_success = await execute_real_buy(mint, TRADE_AMOUNT_SOL)
    if buy_success:
        now = time.time()
        positions[mint] = {
            "symbol": symbol,
            "entry_price": price_usd,
            "highest_price": price_usd,
            "entry_time": now,
            "last_peak_time": now
        }

        msg = (
            f"🚀 **ACHAT LIVE (RPC PUBLIC)**\n"
            f"• **Token :** {symbol}\n"
            f"• **Prix Entrée :** ${price_usd:.8f}\n"
            f"• **Liq :** ${liquidity_usd:,.0f} \vert{} **MCap :**${market_cap:,.0f}"
        )
        await send_telegram(msg)

async def update_position_price(mint: str, current_price: float):
    if mint not in positions:
        return

    pos = positions[mint]
    now = time.time()

    if current_price > pos["highest_price"]:
        pos["highest_price"] = current_price
        pos["last_peak_time"] = now

    highest = pos["highest_price"]
    entry = pos["entry_price"]
    
    stop_level = highest * (1 - (TRAILING_STOP_PCT / 100.0))

    should_sell_stop = (current_price <= stop_level) and (highest > entry)
    should_sell_stagnation = (now - pos["last_peak_time"] > STAGNATION_SECONDS) and (current_price > entry)

    if should_sell_stop or should_sell_stagnation:
        reason = "Trailing Stop (-15%)" if should_sell_stop else "Stagnation (+5 min)"
        logging.info(f"⚠️ Vente ({reason}) sur {pos['symbol']}...")
        
        sell_success = await execute_real_sell(mint, "100%")
        if sell_success:
            pnl_pct = ((current_price - entry) / entry) * 100
            pnl_sol = TRADE_AMOUNT_SOL * (pnl_pct / 100)

            msg = (
                f"🔴 **VENTE LIVE (RPC PUBLIC)**\n"
                f"• **Token :** {pos['symbol']}\n"
                f"• **Raison :** {reason}\n"
                f"• **PnL :** `{pnl_pct:+.2f}%` ({pnl_sol:+.4f} SOL)"
            )
            await send_telegram(msg)
            del positions[mint]

async def position_monitor():
    while True:
        await asyncio.sleep(2)
        for mint in list(positions.keys()):
            dex_data = await fetch_dexscreener_data(mint)
            if dex_data:
                try:
                    price = float(dex_data.get("priceUsd", 0))
                    if price > 0:
                        await update_position_price(mint, price)
                except (ValueError, TypeError):
                    continue

async def memory_cleanup_loop():
    while True:
        await asyncio.sleep(3600)
        now = time.time()
        for mint, ts in list(scanned_tokens.items()):
            if now - ts > 3600:
                del scanned_tokens[mint]

# ==========================================
# FLUX WEBSOCKET PUMP.FUN
# ==========================================

async def websocket_loop():
    uri = "wss://pumpportal.fun/api/data"
    while True:
        try:
            session = await get_http_session()
            async with session.ws_connect(uri, heartbeat=15) as ws:
                logging.info("⚡ Connecté au WebSocket (Mode RPC Public)")
                await ws.send_json({"method": "subscribeNewToken"})

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if data.get("txType") == "create":
                            mint = data.get("mint")
                            symbol = data.get("symbol", "N/A")

                            if mint and mint not in scanned_tokens:
                                scanned_tokens[mint] = time.time()
                                asyncio.create_task(evaluate_after_delay(mint, symbol))

        except Exception as e:
            logging.error(f"Reconnexion WS dans 3s : {e}")
            await asyncio.sleep(3)

# ==========================================
# WEBSERVER HEALTHCHECK
# ==========================================

async def handle_health(request):
    return web.json_response({
        "status": "online",
        "rpc_type": "public_free",
        "account": PUBLIC_KEY_STR,
        "active_positions": len(positions)
    })

async def start_web_server():
    app = web.Application()
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 10000)
    await site.start()

# ==========================================
# POINT D'ENTRÉE MAIN
# ==========================================

async def main():
    logging.info(f"Bot démarré avec RPC Public. Compte : {PUBLIC_KEY_STR}")
    await start_web_server()
    asyncio.create_task(position_monitor())
    asyncio.create_task(memory_cleanup_loop())
    await websocket_loop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot arrêté.")
