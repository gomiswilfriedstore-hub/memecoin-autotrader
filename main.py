import os
import json
import asyncio
import requests
import websockets
import http.server
import socketserver
import threading
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solana.rpc.async_api import AsyncClient
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# --- 1. SERVEUR KEEP-ALIVE POUR RENDER ---
class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True

def keep_alive():
    port = int(os.environ.get("PORT", 10000))
    handler = http.server.SimpleHTTPRequestHandler
    try:
        httpd = ReusableTCPServer(("0.0.0.0", port), handler)
        httpd.serve_forever()
    except Exception as e:
        print(f"Note Keep-Alive: {e}")

threading.Thread(target=keep_alive, daemon=True).start()

# --- 2. CONFIGURATION & VARIABLES D'ENVIRONNEMENT ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID_TARGET = os.environ.get("CHAT_ID")
SOLANA_PRIVATE_KEY = os.environ.get("SOLANA_PRIVATE_KEY")

BUY_AMOUNT_SOL = float(os.environ.get("BUY_AMOUNT_SOL", "0.05"))
SLIPPAGE_PCT = float(os.environ.get("SLIPPAGE_PCT", "20"))
PRIORITY_FEE = float(os.environ.get("PRIORITY_FEE", "0.003"))

STOP_LOSS_PCT = 0.15
MIN_LIQUIDITY_USD = 1000
MIN_VOLUME_5M = 500

RPC_URL = "https://api.mainnet-beta.solana.com"
PUMP_FUN_WS = "wss://pumpportal.fun/api/data"
PUMP_TRADE_API = "https://pumpportal.fun/api/trade-local"

BOT_ACTIVE = True

# --- 3. INITIALISATION DU PORTEFEUILLE ---
signer_keypair = None
if SOLANA_PRIVATE_KEY:
    try:
        clean_key = SOLANA_PRIVATE_KEY.strip()
        signer_keypair = Keypair.from_base58_string(clean_key)
        print(f"🔑 Wallet Connecté : {signer_keypair.pubkey()}")
    except Exception as e:
        print(f"❌ Erreur Clé Privée : {e}")

# --- 4. SÉCURITÉ ET FILTRAGE DE TOKEN ---
def check_security_and_score(token_address):
    try:
        dex_res = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{token_address}", timeout=4).json()
        pairs = dex_res.get("pairs", [])
        if not pairs:
            return None
        
        pair = pairs[0]
        liquidity = pair.get("liquidity", {}).get("usd", 0)
        volume_5m = pair.get("volume", {}).get("m5", 0)
        price_usd = float(pair.get("priceUsd", 0))

        if liquidity < MIN_LIQUIDITY_USD or volume_5m < MIN_VOLUME_5M: 
            return None

        rug_res = requests.get(f"https://api.rugcheck.xyz/v1/tokens/{token_address}/report/summary", timeout=4)
        if rug_res.status_code != 200:
            return None
        
        rug_data = rug_res.json()
        risks = [r.get("name", "") for r in rug_data.get("risks", [])]

        if "Mint Authority Enabled" in risks or "Freeze Authority Enabled" in risks: 
            return None

        dev_risks = [
            "Single holder ownership", 
            "High holder concentration", 
            "Creator balance high",
            "Large Amount of LP Unlocked"
        ]
        for risk in risks:
            if any(dev_risk.lower() in risk.lower() for dev_risk in dev_risks):
                return None

        return {
            "name": pair["baseToken"]["name"],
            "symbol": pair["baseToken"]["symbol"],
            "address": token_address,
            "price_usd": price_usd,
            "liquidity": liquidity,
            "volume_5m": volume_5m
        }
    except Exception:
        return None

# --- 5. EXECUTION DES TRANSACTIONS ---
async def send_solana_transaction(tx_bytes):
    try:
        tx = VersionedTransaction.from_bytes(tx_bytes)
        signature = signer_keypair.sign_message(tx.message)
        signed_tx = VersionedTransaction(tx.message, [signature])

        async with AsyncClient(RPC_URL) as client:
            res = await client.send_raw_transaction(bytes(signed_tx))
            return str(res.value)
    except Exception as e:
        print(f"❌ Erreur Tx: {e}")
        return None

async def execute_trade(action, token_address, amount):
    if not signer_keypair:
        print(f"⚠️ Impossible d'exécuter {action} : Wallet manquant.")
        return None
    try:
        payload = {
            "publicKey": str(signer_keypair.pubkey()),
            "action": action,
            "mint": token_address,
            "denominatedInSol": "true" if action == "buy" else "false",
            "amount": amount,
            "slippage": SLIPPAGE_PCT,
            "priorityFee": PRIORITY_FEE,
            "pool": "auto"
        }
        res = requests.post(PUMP_TRADE_API, json=payload, timeout=5)
        if res.status_code == 200:
            return await send_solana_transaction(res.content)
    except Exception as e:
        print(f"❌ Erreur {action}: {e}")
    return None

# --- 6. SUIVI DU PRIX ET REVENTE AUTO (STOP-LOSS) ---
async def monitor_and_auto_sell(app, token_address, symbol, entry_price):
    peak_price = entry_price
    sl_trigger_price = entry_price * (1 - STOP_LOSS_PCT)

    for _ in range(144):  # Surveille pendant ~12 minutes max
        await asyncio.sleep(5)
        try:
            res = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{token_address}", timeout=3).json()
            pairs = res.get("pairs", [])
            if not pairs: continue

            current_price = float(pairs[0].get("priceUsd", 0))
            if current_price <= 0: continue

            if current_price > peak_price:
                peak_price = current_price

            gain_pct = ((current_price - entry_price) / entry_price) * 100
            peak_gain_pct = ((peak_price - entry_price) / entry_price) * 100

            if current_price <= sl_trigger_price:
                tx_hash = await execute_trade("sell", token_address, "100%")
                
                msg = (
                    f"🛑 **STOP-LOSS EXÉCUTÉ !**\n\n"
                    f"🔴 **Token:** ${symbol}\n"
                    f"🚀 **Plus haut:** +{peak_gain_pct:.1f}%\n"
                    f"📉 **Perte revente:** {gain_pct:.1f}%\n"
                    f"🔗 [Solscan](https://solscan.io/tx/{tx_hash})"
                )

                await app.bot.send_message(chat_id=CHAT_ID_TARGET, text=msg, parse_mode="Markdown")
                break
        except Exception:
            continue

# --- 7. ECOUTE WEBSOCKET ---
async def listen_new_launches(app):
    global BOT_ACTIVE
    while True:
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            async with websockets.connect(
                PUMP_FUN_WS, 
                additional_headers=headers,
                ping_interval=20, 
                ping_timeout=10, 
                close_timeout=5
            ) as ws:
                subscribe_payload = {"method": "subscribeNewToken"}
                await ws.send(json.dumps(subscribe_payload))
                print("⚡ SURVEILLANCE ACTIVE ET STABLE...")

                while True:
                    try:
                        message = await ws.recv()
                        data = json.loads(message)
                        
                        if isinstance(data, dict) and "mint" in data and BOT_ACTIVE:
                            token_address = data["mint"]
                            print(f"🎯 Nouveau Token Détecté : {token_address}")
                            await asyncio.sleep(1.5)
                            
                            setup = await asyncio.to_thread(check_security_and_score, token_address)

                            if setup and signer_keypair:
                                tx_hash = await execute_trade("buy", token_address, BUY_AMOUNT_SOL)

                                if tx_hash:
                                    msg = (
                                        f"🤖 **AUTO-BUY EXÉCUTÉ !**\n\n"
                                        f"💎 **Token:** {setup['name']} (${setup['symbol']})\n"
                                        f"💵 **Montant:** {BUY_AMOUNT_SOL} SOL\n"
                                        f"🔒 **Sécurité:** Dev Lock & RugCheck Validés\n"
                                        f"📈 **Stratégie:** Hold / Vente à -15%\n"
                                        f"🔗 [Solscan](https://solscan.io/tx/{tx_hash})"
                                    )
                                    await app.bot.send_message(chat_id=CHAT_ID_TARGET, text=msg, parse_mode="Markdown")
                                    asyncio.create_task(monitor_and_auto_sell(app, token_address, setup['symbol'], setup['price_usd']))
                    except websockets.exceptions.ConnectionClosed:
                        print("🔄 Déconnexion WebSocket, reconnexion dans 3s...")
                        break
                    except asyncio.CancelledError:
                        raise
                    except Exception as inner_e:
                        print(f"Erreur traitement token: {inner_e}")
                        continue

        except asyncio.CancelledError:
            print("🛑 Arrêt de la surveillance WebSocket.")
            break
        except Exception as e:
            print(f"Erreur connexion WebSocket: {e}")
            try:
                await asyncio.sleep(3)
            except asyncio.CancelledError:
                break

# --- 8. COMMANDES TELEGRAM & CICLE DE VIE ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global BOT_ACTIVE
    BOT_ACTIVE = True
    await update.message.reply_text("🟢 **Bot Activé !** Surveillance en cours.")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global BOT_ACTIVE
    BOT_ACTIVE = False
    await update.message.reply_text("🔴 **Bot en Pause !** Aucun achat automatique ne sera effectué.")

async def post_init(app):
    app.bot_data["ws_task"] = asyncio.create_task(listen_new_launches(app))

async def post_shutdown(app):
    ws_task = app.bot_data.get("ws_task")
    if ws_task:
        ws_task.cancel()

# --- 9. POINT D'ENTRÉE ---
if __name__ == "__main__":
    app = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop))
    print("🤖 Démarrage du bot...")
    app.run_polling()
