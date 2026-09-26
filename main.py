import os
import json
import time
import asyncio
import logging
import base64
import struct
import aiohttp
import websockets
import base58

from aiohttp import web
from solana.rpc.async_api import AsyncClient
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction

# ==========================================
# CONFIGURATION DES LOGS & ENVIRONNEMENT
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Paramètres de Trading & Optimisation Marché
BUY_AMOUNT_SOL = float(os.getenv("BUY_AMOUNT_SOL", "0.02"))  
REQUIRE_SOCIALS = os.getenv("REQUIRE_SOCIALS", "False").lower() == "true"
MAX_DEV_BUY_USD = float(os.getenv("MAX_DEV_BUY_USD", "50.0"))  

# Gestion des Stops & PnL (Sécurité maximale + Scalping agressif)
INITIAL_STOP_LOSS_PCT = -10.0  
BASE_TRAILING_PCT = 10.0       
WIDE_TRAILING_PCT = 20.0       
BREAKEVEN_TRIGGER_PCT = 25.0   
MAX_HOLD_TIME_SEC = 90         

# Blacklist affinée
BANNED_NAMES = [
    "YO", "TEST", "PUMP", "SOL", "UNKNOWN", "NULL", "MOON", 
    "MEME", "COIN", "DOGE", "PEPE", "SHIB", "DEV", "ANON", "INU", "ELON",
    "ETF", "AI", "AIRDROP", "CLAIM", "SAFE", "BABY", "CAT"
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
        raise ValueError("❌ Aucune clé privée 'SOLANA_PRIVATE_KEY' trouvée dans l'environnement !")
    
    # Nettoyage de la clé privée contre les espaces ou retours à la ligne cachés
    pk_env = pk_env.strip().replace("\n", "").replace("\r", "")

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
# FILTRES DE SÉCURITÉ AVANCÉS
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
# EXÉCUTION DES TRADES (ACHAT / VENTE)
# ==========================================

async def execute_trade(mint_str: str, wallet: Keypair, action: str, amount_val=None) -> bool:
    try:
        # Nettoyage rigoureux anti-caractères invisibles et \n
        clean_mint = mint_str.strip().replace("\n", "").replace("\r", "")
        clean_pubkey = str(wallet.pubkey()).strip().replace("\n", "").replace("\r", "")
        
        url = "https://pumpportal.fun/api/trade-local"
        
        payload = {
            "publicKey": clean_pubkey,
            "action": action,
            "mint": clean_mint,
            "denominatedInSol": "true" if action == "buy" else "false",
            "amount": amount_val if action == "buy" else "100%",
            "slippage": 15,        
            "priorityFee": 0.0006, 
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
        tx_sig = await solana_client.send_raw_transaction(bytes(signed_tx))
        
        sig_str = tx_sig.get("result") if isinstance(tx_sig, dict) else getattr(tx_sig, "value", tx_sig)
        logging.info(f"🎯 [{action.upper()}] Succès ! https://solscan.io/tx/{sig_str}")
        return True

    except Exception as e:
        logging.error(f"❌ Exception lors de l'ordre {action} sur {mint_str}: {e}")
        return False

async def fetch_token_price(mint: str) -> float:
    try:
        clean_mint = mint.strip().replace("\n", "").replace("\r", "")
        url = f"https://pumpportal.fun/api/data/token/{clean_mint}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=2) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("price", 1.0))
    except Exception:
        pass
    return 1.0

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
                logging.info("🔗 Connecté au WebSocket Solana - Mode Sécurité & Rentabilité Actif...")

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
                                            mint_address = token_info['mint'].strip().replace("\n", "").replace("\r", "")
                                            symbol = token_info['symbol'].strip().replace("\n", "").replace("\r", "")
                                            name = token_info['name'].strip().replace("\n", "").replace("\r", "")
                                            
                                            is_valid, reason = validate_token_filters(token_info)
                                            if is_valid:
                                                logging.info(f"🚀 TOKEN VALIDÉ ! {name} ({symbol})")
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
    return web.Response(text="Bot PumpFun Sécurité/Rentabilité Actif 🚀")

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
    logging.info("🚀 Bot PumpFun démarré - Paramètres Marché Optimisés")
    await asyncio.gather(
        start_web_server(),
        listen_pumpfun_mints()
    )

if __name__ == "__main__":
    asyncio.run(main())
