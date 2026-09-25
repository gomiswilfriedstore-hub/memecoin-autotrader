import os
import json
import time
import asyncio
import logging
import base64
import aiohttp
import websockets

from aiohttp import web
from solana.rpc.async_api import AsyncClient
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

# ==========================================
# CONFIGURATION DES LOGS & ENVIRONNEMENT
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Paramètres de Trading & Risques
BUY_AMOUNT_SOL = float(os.getenv("BUY_AMOUNT_SOL", "0.01"))      # Montant par trade en SOL (~$2.50)
REQUIRE_SOCIALS = os.getenv("REQUIRE_SOCIALS", "False").lower() == "true"
MAX_DEV_BUY_USD = float(os.getenv("MAX_DEV_BUY_USD", "100.0"))    # Anti-dev dump

# Gestion des Stops & PnL
INITIAL_STOP_LOSS_PCT = -15.0  # Perte max initiale
BASE_TRAILING_PCT = 18.0       # Distance trailing standard (18%)
WIDE_TRAILING_PCT = 25.0       # Distance trailing si gros pump (>50%)
BREAKEVEN_TRIGGER_PCT = 30.0   # Remontée du SL à l'entrée + frais à +30%
MAX_HOLD_TIME_SEC = 180        # Timeout de sécurité (3 minutes)

# Blacklist de noms suspects
BANNED_NAMES = [
    "YO", "TEST", "PUMP", "SOL", "UNKNOWN", "NULL", "MOON", 
    "MEME", "COIN", "DOGE", "PEPE", "SHIB", "DEV", "ANON", "INU", "ELON"
]

PUMPFUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
solana_client = AsyncClient(SOLANA_RPC_URL)

# ==========================================
# CHARGEMENT DU WALLET
# ==========================================

def load_wallet() -> Keypair:
    pk_env = os.getenv("SOLANA_PRIVATE_KEY")
    if not pk_env:
        raise ValueError("❌ Aucune clé privée 'SOLANA_PRIVATE_KEY' trouvée dans les variables Render !")
    
    if pk_env.startswith("["):
        secret_key = json.loads(pk_env)
        return Keypair.from_bytes(bytes(secret_key))
    else:
        import base58
        return Keypair.from_bytes(base58.b58decode(pk_env))

# ==========================================
# FILTRES DE SÉCURITÉ
# ==========================================

def validate_token_filters(token_data: dict) -> tuple[bool, str]:
    symbol = token_data.get("symbol", "").upper().strip()
    name = token_data.get("name", "").upper().strip()
    dev_buy_usd = float(token_data.get("dev_buy_usd", 0.0))
    has_socials = bool(token_data.get("twitter") or token_data.get("telegram") or token_data.get("website"))

    if symbol in BANNED_NAMES or any(banned in name for banned in BANNED_NAMES if len(banned) > 2):
        return False, f"Nom/Symbole suspect ('{symbol}')"

    if len(name) < 2 or len(symbol) < 2:
        return False, "Nom ou symbole trop court"

    if REQUIRE_SOCIALS and not has_socials:
        return False, "Aucun réseau social"

    if dev_buy_usd > MAX_DEV_BUY_USD:
        return False, f"Achat initial Dev trop élevé (${dev_buy_usd:.2f})"

    return True, "Filtres validés"

# ==========================================
# EXÉCUTION DES TRADES (ACHAT & SUIVI)
# ==========================================

async def execute_buy_order(mint_str: str, wallet: Keypair) -> bool:
    try:
        url = "https://pumpportal.fun/api/trade-local"
        payload = {
            "publicKey": str(wallet.pubkey()),
            "action": "buy",
            "mint": mint_str,
            "denominatedInSol": "true",
            "amount": BUY_AMOUNT_SOL,
            "slippage": 15,
            "priorityFee": 0.0005,
            "pool": "pump"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    logging.error(f"❌ Erreur API PumpPortal: {await resp.text()}")
                    return False
                tx_bytes = await resp.read()

        tx = VersionedTransaction.from_bytes(tx_bytes)
        signed_tx = VersionedTransaction(tx.message, [wallet])

        tx_sig = await solana_client.send_raw_transaction(
            bytes(signed_tx),
            opts={"skip_preflight": True, "max_retries": 2}
        )
        
        logging.info(f"🚀 [ACHAT] Tx envoyée! https://solscan.io/tx/{str(tx_sig.value)}")
        return True

    except Exception as e:
        logging.error(f"❌ Exception lors de l'achat de {mint_str}: {e}")
        return False

async def monitor_position(mint: str, symbol: str):
    """Gère le Trailing Stop, le Breakeven et le Timeout en arrière-plan"""
    entry_price = 1.0  # Valeur de référence initiale
    highest_price = entry_price
    start_time = time.time()
    
    stop_loss_price = entry_price * (1 + (INITIAL_STOP_LOSS_PCT / 100.0))
    breakeven_secured = False

    logging.info(f"🛡️ [SUIVI] Position ouverte sur {symbol} | SL Initial: {stop_loss_price:.4f}")

    while True:
        await asyncio.sleep(1.2)
        
        current_price = entry_price  # Remplacer par la récupération du vrai prix si nécessaire
        elapsed_time = time.time() - start_time
        
        current_pnl_pct = ((current_price - entry_price) / entry_price) * 100
        peak_pnl_pct = ((highest_price - entry_price) / entry_price) * 100

        if current_price > highest_price:
            highest_price = current_price
            peak_pnl_pct = ((highest_price - entry_price) / entry_price) * 100

        # Breakeven à +30%
        if peak_pnl_pct >= BREAKEVEN_TRIGGER_PCT and not breakeven_secured:
            stop_loss_price = entry_price * 1.02
            breakeven_secured = True
            logging.info(f"🛡️ [BREAKEVEN] {symbol} sécurisé à l'entrée +2%.")

        # Trailing dynamique
        active_trailing = WIDE_TRAILING_PCT if peak_pnl_pct >= 50.0 else BASE_TRAILING_PCT
        if peak_pnl_pct > 0:
            new_stop = highest_price * (1 - (active_trailing / 100.0))
            if new_stop > stop_loss_price:
                stop_loss_price = new_stop

        # Conditions de sortie
        if current_price <= stop_loss_price or elapsed_time >= MAX_HOLD_TIME_SEC:
            logging.info(f"🎯 [VENTE] Clôture de la position sur {symbol} (PnL: {current_pnl_pct:.2f}%)")
            # Appel de la fonction de vente ici si nécessaire
            break

# ==========================================
# WEBSOCKET PUMPFUN (ÉCOUTE DES TOKENS)
# ==========================================

async def listen_pumpfun_mints():
    wallet = load_wallet()
    uri = os.getenv("SOLANA_WSS_URI", "wss://mainnet.helius-rpc.com/?api-key=TON_API_KEY")
    
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                sub_payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [{"mentions": [PUMPFUN_PROGRAM_ID]}, {"commitment": "processed"}]
                }
                await websocket.send(json.dumps(sub_payload))
                logging.info("🔗 Connecté au WebSocket Solana - Écoute des lancements PumpFun...")

                while True:
                    response = await websocket.recv()
                    data = json.loads(response)
                    
                    if "params" in data:
                        logs = data["params"]["result"]["value"]["logs"]
                        if any("InitializeMint" in log for log in logs):
                            # Exemple de données récupérées du log (à parser selon le format de ton RPC)
                            token_mock_data = {"symbol": "TEST", "name": "Test Token", "dev_buy_usd": 10.0}
                            
                            is_valid, reason = validate_token_filters(token_mock_data)
                            if is_valid:
                                mint_address = "AdresseRecupereeDuLog..."
                                success = await execute_buy_order(mint_address, wallet)
                                if success:
                                    asyncio.create_task(monitor_position(mint_address, token_mock_data["symbol"]))
                            else:
                                logging.info(f"🛑 Token ignoré : {reason}")

        except Exception as e:
            logging.error(f"⚠️ Erreur WebSocket ({e}). Reconnexion dans 3s...")
            await asyncio.sleep(3)

# ==========================================
# SERVEUR HTTP RENDER & INITIALISATION
# ==========================================

async def handle_health_check(request):
    return web.Response(text="Bot PumpFun actif et opérationnel 🚀")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.getenv("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"🌐 Mini-serveur HTTP actif sur le port {port}")

async def main():
    logging.info("🚀 Bot PumpFun démarré en mode ÉQUILIBRÉ (SÉCURITÉ & RENTABILITÉ)")
    await asyncio.gather(
        start_web_server(),
        listen_pumpfun_mints()
    )

if __name__ == "__main__":
    asyncio.run(main())
