import asyncio
import json
import time
import requests
import websockets
from solders.keypair import Keypair
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
TELEGRAM_BOT_TOKEN = "VOTRE_TELEGRAM_BOT_TOKEN_ICI"
PRIVATE_KEY_SECRET = "VOTRE_CLE_PRIVEE_SOLANA_BASE58_ICI" 

AUTO_BUY_AMOUNT_USD = 10.0
MIN_LIQUIDITY_USD = 3000
MIN_MARKET_CAP_USD = 6000
MAX_MARKET_CAP_USD = 30000

# ==========================================
# 🛡️ LOGIQUE DU BOT
# ==========================================
class SimplePumpBot:
    def __init__(self):
        self.is_running = False
        self.positions = {}
        self.deja_vus = set()

    async def filter_token(self, mint_address):
        try:
            url = "https://api.dexscreener.com/latest/dex/tokens/" + str(mint_address)
            res = requests.get(url, timeout=5).json()
            pairs = res.get("pairs")
            
            if not pairs:
                return False, "Token introuvable"

            liquidity = pairs[0].get("liquidity", {}).get("usd", 0)
            market_cap = pairs[0].get("fdv", 0)
            buys = pairs[0].get("txns", {}).get("m5", {}).get("buys", 0)

            if liquidity < MIN_LIQUIDITY_USD:
                return False, "Liquidite trop basse"
            if market_cap < MIN_MARKET_CAP_USD or market_cap > MAX_MARKET_CAP_USD:
                return False, "Market Cap hors limites"
            if buys < 5:
                return False, "Pas assez d'achats"

            # LIGNE ULTRA-SIMPLIFIÉE (Plus aucun risque d'erreur de syntaxe)
            message_succes = "MC: " + str(int(market_cap)) + "$ --- Liq: " + str(int(liquidity)) + "$"
            return True, message_succes
            
        except Exception as e:
            return False, "Erreur API DexScreener"

    async def scanner_loop(self, update_obj: Update):
        self.is_running = True
        await update_obj.message.reply_text("📡 Scanner Pump.fun Activé !")
        
        uri = "wss://pumpportal.fun/api/data"
        
        while self.is_running:
            try:
                async with websockets.connect(uri) as websocket:
                    await websocket.send(json.dumps({"method": "subscribeNewToken"}))
                    
                    async for message in websocket:
                        if not self.is_running:
                            break

                        data = json.loads(message)
                        mint = data.get("mint")

                        if mint and mint not in self.deja_vus:
                            self.deja_vus.add(mint)
                            
                            # On lance l'analyse en arrière-plan
                            asyncio.create_task(self.analyser_et_acheter(mint, update_obj))

            except Exception:
                await asyncio.sleep(5) # Pause avant de se reconnecter

    async def analyser_et_acheter(self, mint, update_obj: Update):
        await asyncio.sleep(10) # Attente de sécurité
        
        is_valid, reason = await self.filter_token(mint)
        if is_valid:
            msg = "🟢 ACHAT VALIDE !\nToken: " + str(mint) + "\nRaison: " + reason
            await update_obj.message.reply_text(msg)

bot = SimplePumpBot()

# ==========================================
# 🚀 COMMANDES TELEGRAM
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 Bot prêt ! Tappe /on pour lancer le scan, ou /off pour stopper.")

async def cmd_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if bot.is_running:
        await update.message.reply_text("Le scanner tourne déjà.")
        return
    asyncio.create_task(bot.scanner_loop(update))

async def cmd_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot.is_running = False
    await update.message.reply_text("🛑 Scanner arrêté.")

def main():
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("on", cmd_on))
    app.add_handler(CommandHandler("off", cmd_off))

    print("Bot en ligne...")
    app.run_polling()

if __name__ == "__main__":
    main()
