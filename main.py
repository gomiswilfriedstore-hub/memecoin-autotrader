import os
import sys
import logging
import asyncio
import base58

from typing import Optional
from solders.keypair import Keypair

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ============================================================
# CHARGEMENT DES VARIABLES D'ENVIRONNEMENT ET INITIALISATION
# ============================================================

PRIVATE_KEY_STR = os.getenv("SOLANA_PRIVATE_KEY")
WALLET: Optional[Keypair] = None

if PRIVATE_KEY_STR:
    try:
        # Nettoyage de la clé (suppression des espaces ou retours à la ligne superflus)
        clean_key = PRIVATE_KEY_STR.strip()
        raw_bytes = base58.b58decode(clean_key)
        
        if len(raw_bytes) == 64:
            # Clé privée Solana standard complète (64 octets)
            WALLET = Keypair.from_bytes(raw_bytes)
        elif len(raw_bytes) == 32:
            # Seed / Clé provenant de pump.fun (32 octets)
            WALLET = Keypair.from_seed(raw_bytes)
        else:
            raise ValueError(f"Longueur de clé invalide : {len(raw_bytes)} octets (attendu: 32 ou 64)")

        logger.info(f"Portefeuille connecté avec succès : {WALLET.pubkey()}")

    except Exception as err:
        logger.critical(f"Erreur lors du chargement de la clé privée: {err}")
        sys.exit(1)
else:
    logger.warning("AUCUNE CLÉ PRIVÉE FOURNIE (SOLANA_PRIVATE_KEY non définie).")

# ============================================================
# LOGIQUE PRINCIPALE DU BOT
# ============================================================

async def main():
    logger.info("Démarrage du bot Solana / PumpFun...")
    
    if WALLET:
        logger.info(f"Prêt à exécuter des transactions avec l'adresse : {WALLET.pubkey()}")
    else:
        logger.warning("Le bot tourne en mode lecture seule (aucun portefeuille chargé).")

    # Boucle principale de votre bot
    while True:
        # Insère ici la logique de ton bot (listening, trading, etc.)
        await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Arrêt du bot.")
