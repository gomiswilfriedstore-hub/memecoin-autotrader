import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, Any, Optional

import aiohttp
from aiohttp import web
import base58

# Solana SDKs
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

# ============================================================
# CONFIGURATION & ENV
# ============================================================

PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"
PUMPPORTAL_TRADE_API = "https://pumpportal.fun/api/trade-local"
DEXSCREENER_URL = "https://api.dexscreener.com/latest/dex/tokens/{}"

RPC_ENDPOINT = os.getenv("RPC_ENDPOINT", "https://api.mainnet-beta.solana.com")
PRIVATE_KEY_STR = os.getenv("SOLANA_PRIVATE_KEY", "")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

STATE_FILE = Path("bot_live_state.json")

# ---------------- STRATÉGIE ----------------

TOKEN_EVALUATION_DELAY = 5

MIN_LIQUIDITY_USD = 2000.0
MIN_MARKET_CAP_USD = 8000.0
MAX_MARKET_CAP_USD = 25000.0
MIN_VOLUME_5M_USD = 1200.0
MIN_BUY_SELL_RATIO = 1.20

TRADE_AMOUNT_SOL = 0.01
MAX_POSITIONS = 3

TRAILING_STOP_PCT = 15.0
STAGNATION_SECONDS = 300

# ---------------- PARAMÈTRES LIVE TRADING ----------------

SLIPPAGE_PCT = 10.0          # 10% de slippage recommandé pour les memecoins
PRIORITY_FEE_SOL = 0.0005     # Priority Fee pour accélérer l'inclusion du bloc

# ---------------- SYSTÈME ----------------

PRICE_REFRESH_SECONDS = 3
HTTP_CONCURRENCY = 8
TOKEN_MEMORY_SECONDS = 3600
HEALTH_PORT = 10000

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("live-trader")

# ============================================================
# SOLANA WALLET INITIALIZATION
# ============================================================

WALLET: Optional[Keypair] = None

if PRIVATE_KEY_STR:
    try:
        WALLET = Keypair.from_bytes(base58.b58decode(PRIVATE_KEY_STR))
        logger.info(f"Portefeuille chargé : {WALLET.pubkey()}")
    except Exception as err:
        logger.critical(f"Erreur lors du chargement de la clé privée: {err}")
        sys.exit(1)
else:
    logger.warning("AUCUNE CLÉ PRIVÉE FOURNIE. Le bot démarrera mais ne pourra pas executer de swaps réels.")

# ============================================================
# GLOBAL STATE
# ============================================================

positions: Dict[str, Dict[str, Any]] = {}
scanned_tokens: Dict[str, float] = {}

paused = False
running = True

scanned_count = 0
winning_trades = 0
losing_trades = 0

HTTP_SESSION: Optional[aiohttp.ClientSession] = None
position_lock = asyncio.Lock()
http_semaphore = asyncio.Semaphore(HTTP_CONCURRENCY)

# ============================================================
# HTTP SESSION
# ============================================================

async def get_http_session() -> aiohttp.ClientSession:
    global HTTP_SESSION
    if HTTP_SESSION is None or HTTP_SESSION.closed:
        timeout = aiohttp.ClientTimeout(total=12, connect=5, sock_read=8)
        HTTP_SESSION = aiohttp.ClientSession(timeout=timeout)
    return HTTP_SESSION

# ============================================================
# SOLANA ON-CHAIN ACTIONS (PUMPPORTAL / RPC)
# ============================================================

async def get_wallet_balance_sol() -> float:
    """Récupère le solde réel SOL du portefeuille via RPC."""
    if not WALLET:
        return 0.0
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [str(WALLET.pubkey())]
    }
    
    try:
        session = await get_http_session()
        async with session.post(RPC_ENDPOINT, json=payload) as resp:
            data = await resp.json()
            lamports = data.get("result", {}).get("value", 0)
            return lamports / 1_000_000_000.0
    except Exception as e:
        logger.error(f"Erreur lecture solde SOL: {e}")
        return 0.0


async def execute_pump_swap(
    mint: str,
    action: str,  # "buy" ou "sell"
    amount: float # En SOL pour un "buy", ou en "%" pour un "sell" (ex: "100%")
) -> Optional[str]:
    """Génère, signe et envoie une transaction de swap via l'API Trade-Local de PumpPortal."""
    if not WALLET:
        logger.error("Achat/Vente impossible: Clé privée manquante.")
        return None

    session = await get_http_session()
    
    # 1. Demande de la transaction non signée à PumpPortal
    payload = {
        "publicKey": str(WALLET.pubkey()),
        "action": action,
        "mint": mint,
        "denominatedInSol": "true" if action == "buy" else "false",
        "amount": amount,
        "slippage": SLIPPAGE_PCT,
        "priorityFee": PRIORITY_FEE_SOL,
        "pool": "pump"
    }

    try:
        async with session.post(PUMPPORTAL_TRADE_API, json=payload) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.error(f"Échec API PumpPortal [{resp.status}]: {body}")
                return None
            
            tx_bytes = await resp.read()

        # 2. Reconstitution et signature de la transaction
        tx = VersionedTransaction.from_bytes(tx_bytes)
        signature = WALLET.sign_message(tx.message)
        signed_tx = VersionedTransaction.populate(tx.message, [signature])

        # 3. Diffusion de la transaction sur la blockchain via RPC
        encoded_tx = base58.b58encode(bytes(signed_tx)).decode("utf-8")
        rpc_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                encoded_tx,
                {"encoding": "base58", "maxRetries": 3, "skipPreflight": True}
            ]
        }

        async with session.post(RPC_ENDPOINT, json=rpc_payload) as rpc_resp:
            res_json = await rpc_resp.json()
            if "error" in res_json:
                logger.error(f"Erreur RPC sendTransaction: {res_json['error']}")
                return None
            
            tx_hash = res_json.get("result")
            logger.info(f"Transaction envoyée ({action.upper()}): https://solscan.io/tx/{tx_hash}")
            return tx_hash

    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du Swap: {e}")
        return None

# ============================================================
# PERSISTENCE
# ============================================================

def save_state():
    state = {
        "positions": positions,
        "scanned_tokens": scanned_tokens,
        "scanned_count": scanned_count,
        "winning_trades": winning_trades,
        "losing_trades": losing_trades,
        "paused": paused,
    }
    temp_file = STATE_FILE.with_suffix(".tmp")
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        temp_file.replace(STATE_FILE)
    except Exception as e:
        logger.error(f"Impossible de sauvegarder l'état: {e}")


def load_state():
    global positions, scanned_tokens, scanned_count, winning_trades, losing_trades, paused
    if not STATE_FILE.exists():
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        positions = state.get("positions", {})
        scanned_tokens = state.get("scanned_tokens", {})
        scanned_count = int(state.get("scanned_count", 0))
        winning_trades = int(state.get("winning_trades", 0))
        losing_trades = int(state.get("losing_trades", 0))
        paused = bool(state.get("paused", False))
        logger.info(f"État restauré: {len(positions)} positions actives.")
    except Exception as e:
        logger.error(f"Erreur chargement état: {e}")

# ============================================================
# TELEGRAM
# ============================================================

async def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    try:
        session = await get_http_session()
        async with session.post(url, json=payload) as response:
            if response.status != 200:
                logger.warning(f"Telegram HTTP {response.status}")
    except Exception as e:
        logger.error(f"Telegram: {e}")

# ============================================================
# DEXSCREENER & ANALYSIS
# ============================================================

async def fetch_dexscreener_data(mint: str) -> Optional[Dict[str, Any]]:
    url = DEXSCREENER_URL.format(mint)
    try:
        async with http_semaphore:
            session = await get_http_session()
            async with session.get(url) as response:
                if response.status != 200:
                    return None
                data = await response.json()
                pairs = data.get("pairs") or []
                valid_pairs = [p for p in pairs if p.get("liquidity")]
                if not valid_pairs:
                    return None
                valid_pairs.sort(key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0), reverse=True)
                return valid_pairs[0]
    except Exception as e:
        logger.debug(f"DexScreener {mint}: {e}")
        return None


def extract_market_data(dex_data: Dict[str, Any]) -> Optional[Dict[str, float]]:
    try:
        liquidity = float(dex_data.get("liquidity", {}).get("usd", 0) or 0)
        market_cap = float(dex_data.get("marketCap", dex_data.get("fdv", 0)) or 0)
        volume_5m = float(dex_data.get("volume", {}).get("m5", 0) or 0)
        price = float(dex_data.get("priceUsd", 0) or 0)
        txns = dex_data.get("txns", {}).get("m5", {})
        buys = int(txns.get("buys", 0) or 0)
        sells = int(txns.get("sells", 0) or 0)
        return {
            "liquidity": liquidity,
            "market_cap": market_cap,
            "volume_5m": volume_5m,
            "price": price,
            "buys": buys,
            "sells": sells
        }
    except (ValueError, TypeError):
        return None


def passes_filters(data: Dict[str, float]) -> bool:
    if data["liquidity"] < MIN_LIQUIDITY_USD:
        return False
    if not (MIN_MARKET_CAP_USD <= data["market_cap"] <= MAX_MARKET_CAP_USD):
        return False
    if data["volume_5m"] < MIN_VOLUME_5M_USD:
        return False
    
    if data["sells"] == 0:
        if data["buys"] < 2:
            return False
    else:
        if (data["buys"] / data["sells"]) < MIN_BUY_SELL_RATIO:
            return False
    return True

# ============================================================
# LIVE TRADING (BUY & SELL)
# ============================================================

async def real_buy(mint: str, symbol: str, market_data: Dict[str, float]) -> bool:
    async with position_lock:
        if paused or mint in positions or len(positions) >= MAX_POSITIONS:
            return False

        balance = await get_wallet_balance_sol()
        if balance < (TRADE_AMOUNT_SOL + PRIORITY_FEE_SOL + 0.002): # Marge pour frais de réseau
            logger.warning(f"Solde SOL réel insuffisant: {balance:.4f} SOL")
            return False

        entry_price = market_data["price"]
        if entry_price <= 0:
            return False

        # Exécution du Swap On-Chain
        tx_hash = await execute_pump_swap(mint, "buy", TRADE_AMOUNT_SOL)
        if not tx_hash:
            logger.error(f"Échec de l'achat On-Chain pour {symbol}")
            return False

        now = time.time()
        positions[mint] = {
            "symbol": symbol,
            "entry_price": entry_price,
            "highest_price": entry_price,
            "entry_time": now,
            "last_peak_time": now,
            "invested_sol": TRADE_AMOUNT_SOL,
            "tx_buy": tx_hash
        }
        save_state()

    logger.info(f"REAL BUY EFFECTUÉ : {symbol} @ ${entry_price:.10f}")
    await send_telegram(
        f"🟢 REAL BUY\n\n"
        f"Token: {symbol}\n"
        f"Prix: ${entry_price:.10f}\n"
        f"Montant: {TRADE_AMOUNT_SOL:.4f} SOL\n"
        f"Tx: https://solscan.io/tx/{tx_hash}"
    )
    return True


async def real_sell(mint: str, current_price: float, reason: str) -> bool:
    global winning_trades, losing_trades

    async with position_lock:
        if mint not in positions:
            return False

        pos = positions[mint]
        symbol = pos["symbol"]
        entry_price = float(pos["entry_price"])

        # Vente de 100% des tokens possédés sur cette adresse
        tx_hash = await execute_pump_swap(mint, "sell", "100%")
        if not tx_hash:
            logger.error(f"Échec de la vente On-Chain pour {symbol}")
            return False

        pnl_pct = ((current_price - entry_price) / entry_price) * 100
        if pnl_pct >= 0:
            winning_trades += 1
        else:
            losing_trades += 1

        del positions[mint]
        save_state()

    logger.info(f"REAL SELL EFFECTUÉ : {symbol} ({pnl_pct:+.2f}%)")
    await send_telegram(
        f"🔴 REAL SELL\n\n"
        f"Token: {symbol}\n"
        f"Raison: {reason}\n"
        f"Prix: ${current_price:.10f}\n"
        f"PnL estimé: {pnl_pct:+.2f}%\n"
        f"Tx: https://solscan.io/tx/{tx_hash}"
    )
    return True

# ============================================================
# MONITORING & EVALUATION
# ============================================================

async def update_position(mint: str, current_price: float):
    if mint not in positions or current_price <= 0:
        return

    pos = positions[mint]
    now = time.time()

    if current_price > pos["highest_price"]:
        pos["highest_price"] = current_price
        pos["last_peak_time"] = now
        save_state()

    highest = pos["highest_price"]
    entry = pos["entry_price"]
    stop_level = highest * (1 - TRAILING_STOP_PCT / 100)

    should_trail = (highest > entry) and (current_price <= stop_level)
    should_stagnate = (current_price > entry) and ((now - pos["last_peak_time"]) >= STAGNATION_SECONDS)

    if should_trail:
        await real_sell(mint, current_price, f"Trailing stop {TRAILING_STOP_PCT:.1f}%")
    elif should_stagnate:
        await real_sell(mint, current_price, "Stagnation")


async def evaluate_after_delay(mint: str, symbol: str):
    global scanned_count
    await asyncio.sleep(TOKEN_EVALUATION_DELAY)
    scanned_count += 1

    dex_data = await fetch_dexscreener_data(mint)
    if not dex_data:
        return
    market_data = extract_market_data(dex_data)
    if not market_data or not passes_filters(market_data):
        return

    await real_buy(mint, symbol, market_data)


async def position_monitor():
    while running:
        await asyncio.sleep(PRICE_REFRESH_SECONDS)
        if not positions:
            continue
        for mint in list(positions.keys()):
            try:
                dex_data = await fetch_dexscreener_data(mint)
                if not dex_data:
                    continue
                market_data = extract_market_data(dex_data)
                if market_data:
                    await update_position(mint, market_data["price"])
            except Exception as e:
                logger.error(f"Position monitor {mint}: {e}")

# ============================================================
# WEBSOCKET & MEMORY CLEANUP
# ============================================================

async def memory_cleanup_loop():
    while running:
        await asyncio.sleep(3600)
        now = time.time()
        for mint, timestamp in list(scanned_tokens.items()):
            if (now - timestamp) > TOKEN_MEMORY_SECONDS:
                del scanned_tokens[mint]
        save_state()


async def websocket_loop():
    while running:
        try:
            session = await get_http_session()
            async with session.ws_connect(PUMPPORTAL_WS, heartbeat=15) as ws:
                logger.info("WebSocket PumpPortal connecté.")
                await ws.send_json({"method": "subscribeNewToken"})

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue

                        if data.get("txType") != "create":
                            continue

                        mint = data.get("mint")
                        symbol = data.get("symbol", "N/A")

                        if mint and mint not in scanned_tokens:
                            scanned_tokens[mint] = time.time()
                            asyncio.create_task(evaluate_after_delay(mint, symbol))

                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
        except Exception as e:
            logger.error(f"WebSocket: {e}")
            await asyncio.sleep(3)

# ============================================================
# TELEGRAM COMMANDS
# ============================================================

async def telegram_api(method: str, params: Optional[Dict[str, Any]] = None):
    if not TELEGRAM_BOT_TOKEN:
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        session = await get_http_session()
        async with session.post(url, json=params or {}) as response:
            return await response.json() if response.status == 200 else None
    except Exception:
        return None


async def telegram_command_loop():
    if not TELEGRAM_BOT_TOKEN:
        return
    offset = 0
    while running:
        try:
            result = await telegram_api("getUpdates", {"timeout": 20, "offset": offset})
            if not result:
                await asyncio.sleep(2)
                continue

            for update in result.get("result", []):
                offset = update["update_id"] + 1
                message = update.get("message")
                if not message or str(message["chat"]["id"]) != str(TELEGRAM_CHAT_ID):
                    continue

                text = message.get("text", "").strip().lower()
                if text == "/status":
                    balance = await get_wallet_balance_sol()
                    await send_telegram(f"🟢 LIVE TRADING RUNNING\nSolde: {balance:.4f} SOL\nPositions: {len(positions)}/{MAX_POSITIONS}")
                elif text == "/balance":
                    balance = await get_wallet_balance_sol()
                    await send_telegram(f"💰 SOLDE RÉEL : {balance:.5f} SOL")
                elif text == "/pause":
                    global paused
                    paused = True
                    save_state()
                    await send_telegram("⏸ Bot mis en pause.")
                elif text == "/resume":
                    paused = False
                    save_state()
                    await send_telegram("▶️ Bot repris.")
        except Exception as e:
            logger.error(f"Telegram loop: {e}")
            await asyncio.sleep(3)

# ============================================================
# HEALTH SERVER & MAIN
# ============================================================

async def handle_health(request):
    balance = await get_wallet_balance_sol()
    return web.json_response({
        "status": "online",
        "mode": "REAL_TRADING",
        "wallet": str(WALLET.pubkey()) if WALLET else "None",
        "positions": len(positions),
        "balance_sol": balance,
        "paused": paused
    })


async def start_web_server():
    app = web.Application()
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()


async def main():
    load_state()
    logger.info("=== LIVE TRADING BOT SOLANA V2 STARTED ===")
    await start_web_server()

    tasks = [
        asyncio.create_task(position_monitor()),
        asyncio.create_task(memory_cleanup_loop()),
        asyncio.create_task(websocket_loop()),
        asyncio.create_task(telegram_command_loop())
    ]

    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        if HTTP_SESSION:
            await HTTP_SESSION.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Arrêt demandé.")
