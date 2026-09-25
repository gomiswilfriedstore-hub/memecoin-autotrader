import os
import sys
import logging
import asyncio
import base58

from typing import Optional
from solders.keypair import Keypair
from aiohttp import web

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ============================================================
# CHARGEMENT DU PORTEFEUILLE SOLANA
# ============================================================

PRIVATE_KEY_STR = os.getenv("SOLANA_PRIVATE_KEY")
WALLET: Optional[Keypair] = None

if PRIVATE_KEY_STR:
    try:
        clean_key = PRIVATE_KEY_STR.strip()
        raw_bytes = base58.b58decode(clean_key)
        
        if len(raw_bytes) == 64:
            WALLET = Keypair.from_bytes(raw_bytes)
        elif len(raw_bytes) == 32:
            WALLET = Keypair.from_seed(raw_bytes)
        else:
            raise ValueError(f"Longueur invalide: {len(raw_bytes)} octets (attendu: 32 ou 64)")

        logger.info(f"Portefeuille connecté avec succès : {WALLET.pubkey()}")

    except Exception as err:
        logger.critical(f"Erreur lors du chargement de la clé privée: {err}")
        sys.exit(1)
else:
    logger.warning("AUCUNE CLÉ PRIVÉE FOURNIE.")

# ============================================================
# DUMMY HTTP SERVER (Pour satisfaire le Port Binding de Render)
# ============================================================

async def handle_health_check(request):
    return web.Response(text="Bot Solana actif et opérationnel !")

async def start_http_server():
    port = int(os.getenv("PORT", 10000))
    app = web.Application()
    app.router.add_get("/", handle_health_check)
    app.router.add_get("/health", handle_health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Serveur Web de contrôle démarré sur le port {port}")

# ============================================================
# BOUCLE PRINCIPALE DU BOT
# ============================================================

async def bot_loop():
    logger.info("Démarrage de la boucle du bot Solana / PumpFun...")
    while True:
        # Insérez votre logique de trading / surveillance ici
        await asyncio.sleep(60)

async def main():
    # Lancement parallèle du serveur HTTP et de la logique du bot
    await asyncio.gather(
        start_http_server(),
        bot_loop()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Arrêt du bot.")
