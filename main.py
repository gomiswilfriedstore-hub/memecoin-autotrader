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

# Serveur web factice pour compatibilité Render
def keep_alive():
    port = int(os.environ.get("PORT", 10000))
    server = socketserver.TCPServer(("0.0.0.0", port), http.server.SimpleHTTPRequestHandler)
    server.serve_forever()

threading.Thread(target=keep_alive, daemon=True).start()

# CONFIGURATION DU BOT
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID_TARGET = os.environ.get("CHAT_ID")
SOLANA_PRIVATE_KEY = os.environ.get("SOLANA_PRIVATE_KEY")

BUY_AMOUNT_SOL = float(os.environ.get("BUY_AMOUNT_SOL", "0.05"))
SLIPPAGE_PCT = float(os.environ.get("SLIPPAGE_PCT", "20"))
PRIORITY_FEE = float(os.environ.get("PRIORITY_FEE", "0.003"))

# PARAMÈTRES DU TRAILING STOP
INITIAL_SL_PCT = 0.20  # Stop-Loss initial à -20%
TRAILING_STOP_PCT = 0.15  # Vente si le prix chute de 15% depuis son sommet le plus haut

MIN_LIQUIDITY_USD = 10000
MIN_VOLUME_5M = 3000

RPC_URL = "https://api.mainnet-beta.solana.com"
PUMP_FUN_WS = "wss://pumpportal.fun/api/data"
PUMP_TRADE_API = "https://pumpportal.fun/api/trade-local"

BOT_ACTIVE = True

signer_keypair = None
if SOLANA_PRIVATE_KEY:
    try:
        signer_keypair = Keypair.from_base58_string(SOLANA_PRIVATE_KEY)
        print(f"🔑 Wallet Connecté : {signer_keypair.pubkey()}")
    except Exception as e:
        print(f"❌ Erreur Clé Privée : {e}")

def check_security_and_score(token_address):
    try:
        dex_res = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{token_address}", timeout=4).json()
        pairs = dex_res.get("pairs", [])
        if not pairs: return None
        
        pair = pairs[0]
        liquidity = pair.get("liquidity", {}).get("usd", 0)
        volume_5m = pair.get("volume", {}).get("m5", 0)
        price_usd = float(pair.get("priceUsd", 0))

        rug_res = requests.get(f"https://api.rugcheck.xyz/v1/tokens/{token_address}/report/summary", timeout=4)
        if rug_res.status_code != 200: return None
        
        rug_data = rug_res.json()
        risks = [r.get("name") for r in rug_data.get("risks", [])]

        if liquidity < MIN_LIQUIDITY_USD or volume_5m < MIN_VOLUME_5M: return None
        if "Mint Authority Enabled" in risks or "Freeze Authority Enabled" in risks: return None

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

async def monitor_and_auto_sell(app, token_address, symbol, entry_price):
    peak_price = entry_price
    initial_sl_price = entry_price * (1 - INITIAL_SL_PCT)

    # Surveillance sur 12 minutes (144 cycles de 5 sec)
    for _ in range(144):
        await asyncio.sleep(5)
        try:
            res = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{token_address}", timeout=3).json()
            pairs = res.get("pairs", [])
            if not pairs: continue

            current_price = float(pairs[0].get("priceUsd", 0))
            if current_price <= 0: continue

            # Mise à jour du prix sommet
            if current_price > peak_price:
                peak_price = current_price

            # Calcul du Stop-Loss suiveur (15% sous le sommet, ou le SL initial si le prix n'a pas monté)
            trailing_sl_price = max(initial_sl_price, peak_price * (1 - TRAILING_STOP_PCT))

            gain_pct = ((current_price - entry_price) / entry_price) * 100
            peak_gain_pct = ((peak_price - entry_price) / entry_price) * 100

            # Déclenchement de la revente si le prix repasse sous le Trailing SL
            if current_price <= trailing_sl_price:
                tx_hash = await execute_trade("sell", token_address, "100%")
                
                if peak_gain_pct >= 20:
                    msg = (
                        f"🎯 **TRAILING STOP : PROFIT SÉCURISÉ !**\n\n"
                        f"💎 **Token:** ${symbol}\n"
                        f"🚀 **Sommet Atteint:** +{peak_gain_pct:.1f}%\n"
                        f"💰 **Vendu à:** +{gain_pct:.1f}%\n"
                        f"🔗 [Solscan](https://solscan.io/tx/{tx_hash})"
                    )
                else:
                    msg = (
                        f"🛑 **STOP-LOSS DÉCLENCHÉ !**\n\n"
                        f"🔴 **Token:** ${symbol}\n"
                        f"📉 **Perte:** {gain_pct:.1f}%\n"
                        f"🔗 [Solscan](https://solscan.io/tx/{tx_hash})"
                    )

                await app.bot.send_message(chat_id=CHAT_ID_TARGET, text=msg, parse_mode="Markdown")
                break
        except Exception as e:
            print(f"Erreur monitoring: {e}")
            continue

async def listen_new_launches(app):
    global BOT_ACTIVE
    async with websockets.connect(PUMP_FUN_WS) as ws:
        await ws.send(json.dumps({"method": "subscribeNewToken"}))
        print("⚡ SURVEILLANCE ACTIVE...")

        while True:
            try:
                message = await ws.recv()
                data = json.loads(message)
                
                if "mint" in data and BOT_ACTIVE:
                    token_address = data["mint"]
                    await asyncio.sleep(4)
                    
                    setup = check_security_and_score(token_address)

                    if setup and signer_keypair:
                        tx_hash = await execute_trade("buy", token_address, BUY_AMOUNT_SOL)

                        if tx_hash:
                            msg = (
                                f"🤖 **AUTO-BUY EXÉCUTÉ !**\n\n"
                                f"💎 **Token:** {setup['name']} (${setup['symbol']})\n"
                                f"💵 **Montant:** {BUY_AMOUNT_SOL} SOL\n"
                                f"📈 **Stratégie:** Trailing Stop Active (Suivi des sommets)\n"
                                f"🔗 [Solscan](https://solscan.io/tx/{tx_hash})"
                            )
                            await app.bot.send_message(chat_id=CHAT_ID_TARGET, text=msg, parse_mode="Markdown")
                            asyncio.create_task(monitor_and_auto_sell(app, token_address, setup['symbol'], setup['price_usd']))
            except Exception as e:
                print(f"Erreur WS: {e}")
                await asyncio.sleep(2)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global BOT_ACTIVE
    BOT_ACTIVE = True
    await update.message.reply_text("🟢 **Bot Activé !** Le bot surveille le marché avec la stratégie Trailing Stop.")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global BOT_ACTIVE
    BOT_ACTIVE = False
    await update.message.reply_text("🔴 **Bot en Pause !** Aucun nouvel achat automatique ne sera effectué.")

async def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop))

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    asyncio.create_task(listen_new_launches(app))
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
