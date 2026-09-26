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

# Paramètres de Trading : Montant fixé à 0.02 SOL
BUY_AMOUNT_SOL = 0.02  

# Filtres de Marché Avancés (Liquidité & MarketCap en USD)
MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD", "1000.0"))  
MAX_MARKET_CAP_USD = float(os.getenv("MAX_MARKET_CAP_USD", "45000.0")) 
MIN_VOLUME_USD = float(os.getenv("MIN_VOLUME_USD", "200.0"))           

# Gestion des Stops & PnL
INITIAL_STOP_LOSS_PCT = -10.0  
BASE_TRAILING_PCT = 10.0       
WIDE_TRAILING_PCT = 20.0       
BREAKEVEN_TRIGGER_PCT = 25.0   
MAX_HOLD_TIME_SEC = 90         

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
processed_tokens = set()

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
        logging.info(f"💰 Solde du Wallet ({wallet.pubkey()}) : {sol_balance:.4f} SOL")
        if sol_balance < BUY_AMOUNT_SOL:
            logging.warning(f"⚠️ ATTENTION : Solde insuffisant (< {BUY_AMOUNT_SOL} SOL). Rechargez votre wallet !")
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
# RÉCUPÉRATION DES MÉTRIQUES DE MARCHÉ (AVEC RETRY)
# ==========================================

async def fetch_token_market_data(mint: str) -> dict:
    clean_mint = mint.strip().replace("\n", "").replace("\r", "")
    url = f"https://pumpportal.fun/api/data/token/{clean_mint}"
    
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=3) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data and isinstance(data, dict) and len(data) > 0:
                            return data
        except Exception:
            pass
        await asyncio.sleep(0.8)
        
    return {}

# ==========================================
# FILTRES DE SÉCURITÉ, MARCHÉ & ANTI-DOUBLONS
# ==========================================

async def validate_token_filters(token_data: dict) -> tuple[bool, str]:
    symbol = token_data.get("symbol", "").strip()
    name = token_data.get("name", "").strip()
    mint = token_data.get("mint", "").strip()

    # 1. RÈGLE ANTI-DOUBLON
    if mint in processed_tokens:
        return False, "Token déjà traité ou acheté (Anti-Doublon)"

    # 2. RÈGLE BLACKLIST / MOTS SUSPECTS
    upper_symbol = symbol.upper()
    upper_name = name.upper()
    if upper_symbol in BANNED_NAMES or any(banned in upper_name for banned in BANNED_NAMES if len(banned) > 2):
        return False, f"Nom/Symbole suspect ('{symbol}')"

    # 3. RÉCUPÉRATION DES DONNÉES DE MARCHÉ & RÉSEAUX VIA API (Avec Retry)
    market_data = await fetch_token_market_data(mint)
    if not market_data:
        return False, "Impossible de récupérer les métriques du token (API non indexée après retries)"

    # Vérification des réseaux sociaux (Sécurité Anti-Rug)
    twitter = market_data.get("twitter")
    telegram = market_data.get("telegram")
    website = market_data.get("website")
    if not (twitter or telegram or website):
        return False, "Aucun réseau social détecté (Potentiel Rug)"

    # 4. FILTRES DE RENTABILITÉ : MARKET CAP & VOLUME
    market_cap = float(market_data.get("marketCap", market_data.get("usd_market_cap", 0)) or 0)
    volume = float(market_data.get("v_usd", market_data.get("volume", 0)) or 0)

    if market_cap > 0:
        if market_cap < MIN_MARKET_CAP_USD:
            return False, f"Market Cap trop faible ({market_cap}$ < {MIN_MARKET_CAP_USD}$)"
        if market_cap > MAX_MARKET_CAP_USD:
            return False, f"Market Cap trop élevée ({market_cap}$ > {MAX_MARKET_CAP_USD}$)"

    # Marquer comme traité pour l'anti-doublon
    processed_tokens.add(mint)
    return True, f"Validé (MCap: {market_cap}$, Vol: {volume}$)"

# ==========================================
# EXÉCUTION DES TRADES (ACHAT / VENTE)
# ==========================================

async def execute_trade(mint_str: str, wallet: Keypair, action: str, amount_val=None) -> bool:
    async with trade_semaphore:
        try:
            current_balance = await check_wallet_balance(wallet)
            if current_balance < BUY_AMOUNT_SOL and action == "buy":
                logging.error(f"❌ Achat annulé : Solde de SOL insuffisant (< {BUY_AMOUNT_SOL} SOL).")
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
    clean_mint = mint.strip().replace("\n", "").replace("\r", "")
    entry_price = await fetch_token_price(clean_mint)
    if entry_price <= 0:
        entry_price = 1.0
        
    highest_price = entry_price
    start_time = time.time()
    
    stop_loss_price = entry_price * (1 + (INITIAL_STOP_LOSS_PCT / 100.0))
    breakeven_secured = False

    logging.info(f"🛡️ [SUIVI] Position active sur {symbol} | Entrée: {entry_price} | SL Initial: {stop_loss_price:.4f}")

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

# ==========================================
# WEBSOCKET PUMPFUN
# ==========================================

async def listen_pumpfun_mints():
    wallet = load_wallet()
    await check_wallet_balance(wallet)
    
    uri = os.getenv("SOLANA_WSS_URI", "wss://mainnet.helius-rpc.com/?api-key=7d50ec7c-921b-4281-8eb7-4d1b1e5f61a2")
    
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
                logging.info("🔗 Connecté au WebSocket Solana - Achat à 0.02 SOL & Filtres Actifs...")

                while True:
                    response = await websocket.recv()
                    data = json.loads(response)
                    
                    if "params" in data:
                        logs = data["params"]["result"]["value"]["logs"]
                        logs_str = "".join(logs)
                        
                        if "Instruction: InitializeMint" in logs_str or "Instruction: Create" in logs_str:
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
                                            
                                            is_valid, reason = await validate_token_filters(token_info)
                                            if is_valid:
                                                logging.info(f"🚀 TOKEN VALIDÉ ({reason}) ! {name} ({symbol})")
                                                success = await execute_trade(mint_address, wallet, "buy", BUY_AMOUNT_SOL)
                                                if success:
                                                    asyncio.create_task(monitor_position(mint_address, symbol, wallet))
                                            else:
                                                logging.info(f"🛑 Token ignoré ({symbol}) : {reason}")
                                    except Exception:
                                        pass

        except Exception as e:
            logging.error(f"⚠️ Erreur WebSocket ({e}). Reconnexion dans 3s...")
            await asyncio.sleep(3)

# ==========================================
# SERVEUR HTTP & MAIN
# ==========================================

async def handle_health_check(request):
    return web.Response(text="Bot PumpFun (0.02 SOL) Actif 🚀")

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
    logging.info("🚀 Bot PumpFun démarré - Mode Achat 0.02 SOL & Sécurité")
    await asyncio.gather(
        start_web_server(),
        listen_pumpfun_mints()
    )

if __name__ == "__main__":
    asyncio.run(main())
