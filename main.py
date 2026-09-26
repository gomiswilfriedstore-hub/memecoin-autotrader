import os
import json
import time
import asyncio
import logging
import base64
import struct
import aiohttp
from aiohttp import web
import websockets
import base58
from collections import OrderedDict

from solana.rpc.async_api import AsyncClient
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction

# ==========================================
# NETTOYAGE GLOBAL DES VARIABLES D'ENVIRONNEMENT
# ==========================================
for env_key, env_val in list(os.environ.items()):
    if isinstance(env_val, str):
        os.environ[env_key] = env_val.strip().replace("\n", "").replace("\r", "")

# ==========================================
# CONFIGURATION DES LOGS & ENVIRONNEMENT
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Vos paramètres inchangés
BUY_AMOUNT_SOL = 0.02  
MIN_HOLDERS_REQUIRED = 100  
INITIAL_STOP_LOSS_PCT = -10.0  
BASE_TRAILING_PCT = 10.0       
WIDE_TRAILING_PCT = 20.0       
BREAKEVEN_TRIGGER_PCT = 25.0   
MAX_HOLD_TIME_SEC = 180        

# Blacklist affinée
BANNED_NAMES = [
    "YO", "TEST", "PUMP", "SOL", "UNKNOWN", "NULL", "MOON", 
    "MEME", "COIN", "DOGE", "PEPE", "SHIB", "DEV", "ANON", "INU", "ELON",
    "ETF", "AI", "AIRDROP", "CLAIM", "SAFE", "BABY", "CAT", "DDOS", "PUMPFUN"
]

PUMPFUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=7d50ec7c-921b-4281-8eb7-4d1b1e5f61a2")
solana_client = AsyncClient(SOLANA_RPC_URL)

trade_semaphore = asyncio.Semaphore(2)

# Optimisation : Cache intelligent à taille limitée pour éviter les fuites de mémoire
processed_tokens = OrderedDict()
MAX_PROCESSED_CACHE = 1000

active_positions_count = 0

# ==========================================
# CHARGEMENT DU WALLET
# ==========================================

def load_wallet() -> Keypair:
    pk_env = os.getenv("SOLANA_PRIVATE_KEY")
    if not pk_env:
        raise ValueError("❌ Aucune clé privée 'SOLANA_PRIVATE_KEY' trouvée dans l'environnement !")
    
    if pk_env.startswith("["):
        secret_key = json.loads(pk_env)
        if len(secret_key) == 32:
            return Keypair.from_seed(bytes(secret_key))
        return Keypair.from_bytes(bytes(secret_key))
    else:
        try:
            decoded = base58.b58decode(pk_env)
            if len(decoded) == 32:
                return Keypair.from_seed(decoded)
            elif len(decoded) == 64:
                return Keypair.from_bytes(decoded)
            else:
                return Keypair.from_base58_string(pk_env)
        except Exception:
            return Keypair.from_base58_string(pk_env)

async def check_wallet_balance(wallet: Keypair) -> float:
    try:
        balance_resp = await solana_client.get_balance(wallet.pubkey())
        lamports = balance_resp.value if hasattr(balance_resp, "value") else balance_resp.get("result", {}).get("value", 0)
        sol_balance = lamports / 1e9
        return sol_balance
    except Exception as e:
        logging.error(f"❌ Impossible de récupérer le solde du wallet : {e}")
        return 0.0

# ==========================================
# PARSEUR DES LOGS PUMPFUN
# ==========================================

def read_length_prefixed_string(data, offset):
    length = struct.unpack('<I', data[offset:offset + 4])[0]
    offset += 4
    string_data = data[offset:offset + length]
    offset += length
    return string_data.decode('utf-8', errors='ignore').strip('\x00').strip().replace("\n", "").replace("\r", ""), offset

def read_pubkey(data, offset):
    pubkey_data = data[offset:offset + 32]
    offset += 32
    pubkey = str(Pubkey.from_bytes(pubkey_data))
    return pubkey.strip().replace("\n", "").replace("\r", ""), offset

def parse_pumpfun_event(program_data_hex):
    try:
        data_bytes = bytes.fromhex(program_data_hex)
        offset = 8  
        
        event_data = {}
        event_data['name'], offset = read_length_prefixed_string(data_bytes, offset)
        event_data['symbol'], offset = read_length_prefixed_string(data_bytes, offset)
        event_data['uri'], offset = read_length_prefixed_string(data_bytes, offset)
        event_data['mint'], offset = read_pubkey(data_bytes, offset)
        event_data['bonding_curve'], offset = read_pubkey(data_bytes, offset)
        event_data['user'], offset = read_pubkey(data_bytes, offset)
        
        return event_data
    except Exception:
        return None

# ==========================================
# RÉCUPÉRATION DES MÉTRIQUES DE MARCHÉ (ROBUSTE)
# ==========================================

async def fetch_token_market_data(mint: str) -> dict:
    clean_mint = mint.strip().replace("\n", "").replace("\r", "")
    url = f"https://pumpportal.fun/api/data/token/{clean_mint}"
    
    for attempt in range(4): # 4 essais pour contrer les micro-latences réseau
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=3.5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data and isinstance(data, dict) and len(data) > 0:
                            return data
        except Exception:
            pass
        await asyncio.sleep(0.6)
        
    return {}

# ==========================================
# FILTRE RAPIDE (ANTI-BLACKLIST)
# ==========================================

def validate_token_quick(token_data: dict) -> tuple[bool, str]:
    symbol = token_data.get("symbol", "").strip()
    name = token_data.get("name", "").strip()
    mint = token_data.get("mint", "").strip()

    if mint in processed_tokens:
        return False, "Token déjà traité"

    upper_symbol = symbol.upper()
    upper_name = name.upper()
    if upper_symbol in BANNED_NAMES or any(banned in upper_name for banned in BANNED_NAMES if len(banned) > 2):
        return False, f"Nom/Symbole suspect ('{symbol}')"

    # Gestion de la taille du cache (mémoire optimisée)
    processed_tokens[mint] = True
    if len(processed_tokens) > MAX_PROCESSED_CACHE:
        processed_tokens.popitem(last=False)

    return True, "Nom valide"

# ==========================================
# EXÉCUTION DES TRADES (ACHAT / VENTE)
# ==========================================

async def execute_trade(mint_str: str, wallet: Keypair, action: str, amount_val=None) -> bool:
    async with trade_semaphore:
        try:
            current_balance = await check_wallet_balance(wallet)
            if current_balance < BUY_AMOUNT_SOL and action == "buy":
                logging.error(f"❌ Achat annulé : Solde de SOL insuffisant ({current_balance:.4f} SOL < {BUY_AMOUNT_SOL} SOL).")
                return False

            clean_mint = mint_str.strip().replace("\n", "").replace("\r", "")
            clean_pubkey = str(wallet.pubkey()).strip().replace("\n", "").replace("\r", "")
            
            url = "https://pumpportal.fun/api/trade-local"
            
            payload = {
                "publicKey": clean_pubkey,
                "action": action,
                "mint": clean_mint,
                "denominatedInSol": "true" if action == "buy" else "false",
                "amount": amount_val if action == "buy" else "100%",
                "slippage": 20,        
                "priorityFee": 0.001,  
                "pool": "pump"
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        logging.error(f"❌ Erreur API PumpPortal ({action.upper()}) : {error_text}")
                        return False
                    raw_data = await resp.read()

            tx = VersionedTransaction.from_bytes(raw_data)
            signed_tx = VersionedTransaction(tx.message, [wallet])
            
            rpc_payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sendTransaction",
                "params": [
                    base64.b64encode(bytes(signed_tx)).decode('utf-8'),
                    {"encoding": "base64", "skipPreflight": True, "maxRetries": 3}
                ]
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(SOLANA_RPC_URL, json=rpc_payload) as resp:
                    res_json = await resp.json()
                    if "error" in res_json:
                        logging.error(f"❌ Erreur RPC Solana : {res_json['error']}")
                        return False
                    sig_str = res_json.get("result")

            logging.info(f"🎯 [{action.upper()}] Succès ! https://solscan.io/tx/{sig_str}")
            return True

        except Exception as e:
            logging.error(f"❌ Exception lors de l'ordre {action} sur {mint_str}: {e}")
            return False

async def fetch_token_price(mint: str) -> float:
    data = await fetch_token_market_data(mint)
    return float(data.get("price", 1.0) or 1.0)

async def monitor_position(mint: str, symbol: str, wallet: Keypair):
    global active_positions_count
    active_positions_count += 1
    clean_mint = mint.strip().replace("\n", "").replace("\r", "")
    
    # Attente de 5 secondes (inchangée)
    await asyncio.sleep(5.0)
    
    market_data = await fetch_token_market_data(clean_mint)
    
    # Sécurité anti-bug : si l'API est brièvement injoignable, on retente une fois avant de rejeter
    if not market_data:
        await asyncio.sleep(1.0)
        market_data = await fetch_token_market_data(clean_mint)

    holders_count = int(market_data.get("holders", 0) or 0)
    
    # Sécurité Holders (100 min)
    if holders_count < MIN_HOLDERS_REQUIRED:
        logging.warning(f"🛑 [SÉCURITÉ HOLDERS] {symbol} rejeté : Seulement {holders_count} détenteur(s) (Minimum requis : {MIN_HOLDERS_REQUIRED}). Vente immédiate...")
        await execute_trade(clean_mint, wallet, "sell")
        active_positions_count = max(0, active_positions_count - 1)
        return

    entry_price = float(market_data.get("price", 1.0) or 1.0)
    if entry_price <= 0:
        entry_price = 1.0
        
    highest_price = entry_price
    start_time = time.time()
    
    stop_loss_price = entry_price * (1 + (INITIAL_STOP_LOSS_PCT / 100.0))
    breakeven_secured = False

    logging.info(f"🛡️ [SUIVI] Position active sur {symbol} | Holders: {holders_count} | Entrée: {entry_price} | SL: {stop_loss_price:.4f}")

    try:
        while True:
            await asyncio.sleep(1.0) 
            current_price = await fetch_token_price(clean_mint)
            elapsed_time = time.time() - start_time
            
            if current_price <= 0:
                continue

            current_pnl_pct = ((current_price - entry_price) / entry_price) * 100
            peak_pnl_pct = ((highest_price - entry_price) / entry_price) * 100

            if current_price > highest_price:
                highest_price = current_price
                peak_pnl_pct = ((highest_price - entry_price) / entry_price) * 100

            if peak_pnl_pct >= BREAKEVEN_TRIGGER_PCT and not breakeven_secured:
                stop_loss_price = entry_price * 1.01  
                breakeven_secured = True
                logging.info(f"🛡️ [BREAKEVEN] {symbol} sécurisé à l'entrée.")

            active_trailing = WIDE_TRAILING_PCT if peak_pnl_pct >= 100.0 else BASE_TRAILING_PCT
            if peak_pnl_pct > 0:
                new_stop = highest_price * (1 - (active_trailing / 100.0))
                if new_stop > stop_loss_price:
                    stop_loss_price = new_stop

            if current_price <= stop_loss_price or elapsed_time >= MAX_HOLD_TIME_SEC:
                logging.info(f"⚡ [CLÔTURE] {symbol} - PnL: {current_pnl_pct:.2f}% | Temps: {elapsed_time:.1f}s")
                await execute_trade(clean_mint, wallet, "sell")
                break
    finally:
        active_positions_count = max(0, active_positions_count - 1)
        new_balance = await check_wallet_balance(wallet)
        logging.info(f"💼 Position fermée. Solde actuel du wallet : {new_balance:.4f} SOL")

# ==========================================
# WEBSOCKET PUMPFUN (AVEC BACKOFF EXPONENTIEL)
# ==========================================

async def listen_pumpfun_mints():
    wallet = load_wallet()
    uri = os.getenv("SOLANA_WSS_URI", "wss://mainnet.helius-rpc.com/?api-key=7d50ec7c-921b-4281-8eb7-4d1b1e5f61a2")
    
    reconnect_delay = 3
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                reconnect_delay = 3 # Réinitialise le délai si la connexion réussit
                sub_payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [{"mentions": [PUMPFUN_PROGRAM_ID]}, {"commitment": "processed"}]
                }
                await websocket.send(json.dumps(sub_payload))
                logging.info("🔗 Connecté au WebSocket Solana - Mode Robuste Actif...")

                while True:
                    response = await websocket.recv()
                    data = json.loads(response)
                    
                    if "params" in data:
                        logs = data["params"]["result"]["value"]["logs"]
                        logs_str = "".join(logs)
                        
                        if "Instruction: InitializeMint" in logs_str or "Instruction: Create" in logs_str:
                            current_sol_balance = await check_wallet_balance(wallet)
                            if current_sol_balance < BUY_AMOUNT_SOL:
                                continue

                            for log_entry in logs:
                                if "Program data: " in log_entry:
                                    try:
                                        base64_data = log_entry.split("Program data: ")[1].strip()
                                        hex_data = base64.b64decode(base64_data).hex()
                                        token_info = parse_pumpfun_event(hex_data)
                                        
                                        if token_info and 'mint' in token_info:
                                            mint_address = token_info['mint'].strip()
                                            symbol = token_info['symbol'].strip()
                                            name = token_info['name'].strip()
                                            
                                            is_valid, reason = validate_token_quick(token_info)
                                            if is_valid:
                                                logging.info(f"🚀 NOUVEAU TOKEN DÉTECTÉ ({name} / {symbol}) - Achat immédiat à {BUY_AMOUNT_SOL} SOL !")
                                                success = await execute_trade(mint_address, wallet, "buy", BUY_AMOUNT_SOL)
                                                if success:
                                                    asyncio.create_task(monitor_position(mint_address, symbol, wallet))
                                    except Exception:
                                        pass

        except Exception as e:
            logging.error(f"⚠️ Erreur WebSocket ({e}). Reconnexion dans {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30) # Backoff exponentiel plafonné à 30s

# ==========================================
# SERVEUR HTTP & MAIN
# ==========================================

async def handle_health_check(request):
    return web.Response(text="Bot PumpFun Haute Fiabilité Actif 🚀")

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
    logging.info("🚀 Bot PumpFun démarré - Version Ultra-Fiable sans changement de paramètres")
    await asyncio.gather(
        start_web_server(),
        listen_pumpfun_mints()
    )

if __name__ == "__main__":
    asyncio.run(main())
