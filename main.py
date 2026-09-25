import asyncio
import json
import logging
import time
import requests
import websockets
from typing import Dict, Optional, Set

from solders.keypair import Keypair
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# =====================================================================
# ⚙️ CONFIGURATION GLOBALE
# =====================================================================
# Remplacer par vos vraies clés
TELEGRAM_BOT_TOKEN = "VOTRE_TELEGRAM_BOT_TOKEN_ICI"
PRIVATE_KEY_SECRET = "VOTRE_CLE_PRIVEE_SOLANA_BASE58_ICI" 

# Stratégie de Trading
AUTO_BUY_AMOUNT_USD = 10.0      # Achat automatique (10 $)
MIN_LIQUIDITY_USD = 3000.0      # Liquidité minimale (3 000 $)
MIN_MARKET_CAP_USD = 6000.0     # Market Cap minimal (6 000 $)
MAX_MARKET_CAP_USD = 30000.0    # Market Cap maximal (30 000 $)
MAX_CONCURRENT_POSITIONS = 3    # Limite de positions simultanées

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# =====================================================================
# 🛡️ CLASSES MÉTIER
# =====================================================================
class SolanaWalletManager:
    """Gère le portefeuille Solana."""
    def __init__(self, private_key_str: str):
        try:
            if private_key_str.startswith("["):
                self.keypair = Keypair.from_bytes(bytes(json.loads(private_key_str)))
            else:
                self.keypair = Keypair.from_base58_string(private_key_str)
            logging.info(f"✅ Portefeuille connecté : {self.keypair.pubkey()}")
        except Exception as e:
            logging.error(f"❌ Erreur clé privée : {e}")
            self.keypair = None

    def get_pubkey(self) -> str:
        return str(self.keypair.pubkey()) if self.keypair else "Non connecté"


class PositionTracker:
    """Gère le Stop Loss (-15%) et le Take Profit par stagnation (5 min)."""
    def __init__(self, mint_address: str, entry_price: float):
        self.mint_address = mint_address
        self.entry_price = entry_price
        
        # Stop loss fixe à -15% du prix d'entrée
        self.stop_loss_price = entry_price * 0.85
        
        # Suivi du plus haut pour la stagnation
        self.peak_price = entry_price
        self.last_high_time = time.time()

    def update_price(self, current_price: float) -> tuple[bool, str]:
        now = time.time()

        # 1. Stop Loss : Vente si le prix tombe à -15%
        if current_price <= self.stop_loss_price:
            return True, f"STOP_LOSS (-15% atteint : ${self.stop_loss_price:.6f})"

        # 2. Mise à jour du sommet
        if current_price > self.peak_price:
            self.peak_price = current_price
            self.last_high_time = now

        # 3. Stagnation : Vente si aucun nouveau sommet depuis 5 minutes (300s)
        if (now - self.last_high_time) >= 300:
            return True, "STAGNATION (Aucun nouveau plus haut depuis 5 min)"

        return False, "HOLD"


class AutoPumpFunBot:
    """Scanner WebSocket et Moteur de Trading."""
    def __init__(self, wallet: SolanaWalletManager):
        self.wallet = wallet
        self.active_positions: Dict[str, PositionTracker] = {}
        self.processed_tokens: Set[str] = set()
        self.is_scanner_running = False

    async def get_sol_price(self) -> float:
        """Récupère le prix actuel du SOL en USD via DexScreener."""
        try:
            url = "https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112"
            res = requests.get(url, timeout=3).json()
            if res.get("pairs"):
                return float(res["pairs"][0].get("priceUsd", 150.0))
        except Exception:
            pass
        return 150.0

    async def start_auto_scanner(self, update_obj: Update):
        """Écoute les nouveaux tokens Pump.fun via WebSocket."""
        uri = "wss://pumpportal.fun/api/data"
        self.is_scanner_running = True
        
        await update_obj.message.reply_text(
            f"📡 **Scanner Pump.fun Activé**\n"
            f"• Achat auto : **{AUTO_BUY_AMOUNT_USD}$**\n"
            f"• Filtres : Liq > **{MIN_LIQUIDITY_USD}$** | MC : **{MIN_MARKET_CAP_USD}$- {MAX_MARKET_CAP_USD}$**\n"
            f"• SL : **-15%** | Sortie : **5 min de stagnation**",
            parse_mode="Markdown"
        )

        while self.is_scanner_running:
            try:
                async with websockets.connect(uri) as websocket:
                    await websocket.send(json.dumps({"method": "subscribeNewToken"}))
                    logging.info("🔌 Connecté au WebSocket PumpPortal.")
                    
                    async for message in websocket:
                        if not self.is_scanner_running:
                            break

                        data = json.loads(message)
                        mint = data.get("mint")

                        if mint and mint not in self.processed_tokens:
                            self.processed_tokens.add(mint)

                            if len(self.active_positions) >= MAX_CONCURRENT_POSITIONS:
                                continue

                            # Lancer l'analyse sans bloquer le WebSocket
                            asyncio.create_task(self.process_token(mint, update_obj))

            except Exception as e:
                logging.error(f"⚠️ Déconnexion WebSocket : {e}. Reconnexion dans 5s...")
                await asyncio.sleep(5)

    async def process_token(self, mint_address: str, update_obj: Update):
        """Pipeline complet : Attente -> Filtre -> Achat -> Surveillance."""
        # 1. Attente obligatoire de 10 secondes
        await asyncio.sleep(10)

        # 2. Filtrage API
        is_valid, reason = await self.filter_token(mint_address)
        if not is_valid:
            return

        # 3. Calcul montant SOL
        sol_price = await self.get_sol_price()
        amount_sol = AUTO_BUY_AMOUNT_USD / sol_price

        # 4. Simulation / Exécution Achat
        entry_price = await self.execute_buy(mint_address, amount_sol)
        if not entry_price:
            return

        tracker = PositionTracker(mint_address, entry_price)
        self.active_positions[mint_address] = tracker

        msg = (
            f"🟢 **ACHAT EXÉCUTÉ**\n"
            f"• Token : `{mint_address}`\n"
            f"• Montant : **{AUTO_BUY_AMOUNT_USD}$** (~{amount_sol:.4f} SOL)\n"
            f"• Prix d'entrée : `${entry_price:.6f}`\n"
            f"• Stop Loss (-15%) : `${tracker.stop_loss_price:.6f}`\n"
            f"ℹ️ {reason}"
        )
        await update_obj.message.reply_text(msg, parse_mode="Markdown")

        # 5. Lancement de la boucle de surveillance
        asyncio.create_task(self.monitor_position(mint_address, update_obj))

    async def filter_token(self, mint_address: str) -> tuple[bool, str]:
        """Vérifie le Market Cap, la liquidité et l'activité."""
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{mint_address}"
            res = requests.get(url, timeout=5).json()
            pairs = res.get("pairs")
            
            if not pairs:
                return False, "Token introuvable sur DexScreener"

            main_pair = pairs[0]
            liquidity = main_pair.get("liquidity", {}).get("usd", 0)
            market_cap = main_pair.get("fdv", 0)
            buys = main_pair.get("txns", {}).get("m5", {}).get("buys", 0)

            if liquidity < MIN_LIQUIDITY_USD:
                return False, "Liquidité insuffisante"
            if market_cap < MIN_MARKET_CAP_USD or market_cap > MAX_MARKET_CAP_USD:
                return False, "Market Cap hors limites"
            if buys < 5:
                return False, "Pas assez d'achats (momentum faible)"

            return True, f"MC: ${market_cap:,.0f} \vert{} Liq:${liquidity:,.0f}"
        except Exception:
            return False, "Erreur API"

    async def execute_buy(self, mint_address: str, amount_sol: float) -> Optional[float]:
        """Envoie l'ordre d'achat (Simulé ici via appel de prix)."""
        # TODO: Remplacer par l'appel API RPC/PumpPortal réel
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{mint_address}"
            res = requests.get(url, timeout=5).json()
            if res.get("pairs"):
                return float(res["pairs"][0].get("priceUsd", 0))
        except Exception:
            pass
        return None

    async def execute_sell(self, mint_address: str, reason: str):
        """Envoie l'ordre de vente (Simulé)."""
        # TODO: Remplacer par l'appel API RPC/PumpPortal réel
        logging.info(f"🚨 Vente de {mint_address} pour motif : {reason}")

    async def monitor_position(self, mint_address: str, update_obj: Update):
        """Surveille le prix de la position toutes les 3 secondes."""
        tracker = self.active_positions.get(mint_address)
        if not tracker:
            return

        while mint_address in self.active_positions:
            await asyncio.sleep(3)
            
            try:
                url = f"https://api.dexscreener.com/latest/dex/tokens/{mint_address}"
                res = requests.get(url, timeout=3).json()
                if not res.get("pairs"):
                    continue
                current_price = float(res["pairs"][0].get("priceUsd", 0))
            except Exception:
                continue

            should_sell, reason = tracker.update_price(current_price)
            if should_sell:
                await self.execute_sell(mint_address, reason)
                del self.active_positions[mint_address]

                profit_loss_pct = ((current_price - tracker.entry_price) / tracker.entry_price) * 100
                emoji = "🟩" if profit_loss_pct > 0 else "🟥"

                msg = (
                    f"🔴 **VENTE EXÉCUTÉE**\n"
                    f"• Token : `{mint_address}`\n"
                    f"• Motif : {reason}\n"
                    f"• Prix Sortie : `${current_price:.6f}`\n"
                    f"• PnL : {emoji} **{profit_loss_pct:.2f}%**"
                )
                await update_obj.message.reply_text(msg, parse_mode="Markdown")
                break


# =====================================================================
# 🚀 INITIALISATION DU BOT TELEGRAM
# =====================================================================
wallet_mgr = SolanaWalletManager(PRIVATE_KEY_SECRET)
bot_engine = AutoPumpFunBot(wallet_mgr)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🤖 **Bot de Trading Automatique Pump.fun**\n\n"
        f"💳 Portefeuille : `{wallet_mgr.get_pubkey()}`\n\n"
        "**Commandes :**\n"
        "▶️ `/on` : Démarrer l'auto-scanner\n"
        "⏸️ `/off` : Arrêter l'auto-scanner\n"
        "📊 `/status` : Voir les positions en cours"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def cmd_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if bot_engine.is_scanner_running:
        await update.message.reply_text("⚠️ Le scanner est déjà actif.")
        return
    asyncio.create_task(bot_engine.start_auto_scanner(update))

async def cmd_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_engine.is_scanner_running = False
    await update.message.reply_text("🛑 **Scanner désactivé.**")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not bot_engine.active_positions:
        await update.message.reply_text("📭 Aucune position ouverte.")
        return

    msg = "📊 **Positions Actives :**\n\n"
    for mint, tracker in bot_engine.active_positions.items():
        msg += (
            f"• `{mint[:8]}...` | Achat : `${tracker.entry_price:.6f}`\n"
            f"  SL : `${tracker.stop_loss_price:.6f}` | Plus Haut : `${tracker.peak_price:.6f}`\n\n"
        )
    await update.message.reply_text(msg, parse_mode="Markdown")

def main():
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("on", cmd_on))
    app.add_handler(CommandHandler("off", cmd_off))
    app.add_handler(CommandHandler("status", cmd_status))

    print("✅ Bot Telegram prêt et en attente de messages...")
    app.run_polling()

if __name__ == "__main__":
    main()
