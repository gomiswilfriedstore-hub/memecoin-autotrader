import os
import sys
import logging
import asyncio
import base58
import json
import time
from typing import Optional, Dict, Any

import websockets
from solders.keypair import Keypair
from solana.rpc.async_api import AsyncClient
from aiohttp import web

# ---------------------------------------------------------------------------
# CONFIGURATION ET LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("PumpFunSniper")

# Variables d'Environnement (Render)
PRIVATE_KEY_STR = os.getenv("SOLANA_PRIVATE_KEY")
RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
PUMPFUN_WS = os.getenv("PUMPFUN_WS_URL", "wss://pumpportal.fun/api/data")

# Paramètres de Trading & Filtres (Optimisés)
BUY_AMOUNT_SOL = float(os.getenv("BUY_AMOUNT_SOL", "0.05"))          # Montant par trade
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "-15.0"))          # Stop Loss strict à -15% du prix d'achat
STAGNATION_TIMEOUT = int(os.getenv("STAGNATION_TIMEOUT", "300"))    # Vente si pas de nouveau peak en 5 min (300s)

# Filtres de Sécurité
MAX_DEV_BUY_SOL = float(os.getenv("MAX_DEV_BUY_SOL", "1.5"))        # Achat max du Dev au lancement
REQUIRE_SOCIALS = os.getenv("REQUIRE_SOCIALS", "True").lower() == "true" # Réseaux sociaux obligatoires
MIN_VSOL = float(os.getenv("MIN_VSOL", "1.0"))                       # Min SOL dans la courbe
MAX_VSOL = float(os.getenv("MAX_VSOL", "15.0"))                      # Max SOL dans la courbe

WALLET: Optional[Keypair] = None
solana_client = AsyncClient(RPC_URL)

# ---------------------------------------------------------------------------
# INITIALISATION DU PORTEFEUILLE SOLANA
# ---------------------------------------------------------------------------
if PRIVATE_KEY_STR:
    try:
        clean_key = PRIVATE_KEY_STR.strip()
        raw_bytes = base58.b58decode(clean_key)
        if len(raw_bytes) == 64:
            WALLET = Keypair.from_bytes(raw_bytes)
        elif len(raw_bytes) == 32:
            WALLET = Keypair.from_seed(raw_bytes)
        else:
            raise ValueError(f"Longueur de clé invalide : {len(raw_bytes)} octets")
        logger.info(f"Portefeuille connecté : {WALLET.pubkey()}")
    except Exception as e:
        logger.critical(f"Erreur de clé privée : {e}")
        sys.exit(1)
else:
    logger.critical("Variable SOLANA_PRIVATE_KEY manquante !")
    sys.exit(1)

# ---------------------------------------------------------------------------
# FILTRES DE SÉLECTION DU TOKEN
# ---------------------------------------------------------------------------
async def analyze_token_safety(data: Dict[str, Any]) -> bool:
    """
    Analyse le token intercepté sur le WebSocket pour rejeter les scams/rugs.
    """
    mint = data.get("mint")
    symbol = data.get("symbol", "UNKNOWN")
    dev_buy_sol = float(data.get("initialBuy", 0))
    v_sol = float(data.get("vSolInBondingCurve", 0))
    
    # Vérification présence réseaux sociaux (Twitter, Telegram ou Website)
    has_socials = bool(data.get("twitter") or data.get("telegram") or data.get("website"))

    # 1. Filtre sur l'achat du Dev au lancement
    if dev_buy_sol > MAX_DEV_BUY_SOL:
        logger.warning(f" ❌ [FILTRE] {symbol} ({mint[:6]}): Dev buy trop élevé ({dev_buy_sol} SOL > {MAX_DEV_BUY_SOL} SOL)")
        return False

    # 2. Filtre sur les Réseaux Sociaux
    if REQUIRE_SOCIALS and not has_socials:
        logger.warning(f" ❌ [FILTRE] {symbol} ({mint[:6]}): Aucun réseau social renseigné")
        return False

    # 3. Filtre sur la Liquidité de la Bonding Curve
    if v_sol < MIN_VSOL or v_sol > MAX_VSOL:
        logger.warning(f" ❌ [FILTRE] {symbol} ({mint[:6]}): Liquide hors limites ({v_sol:.2f} SOL)")
        return False

    logger.info(f" ✅ [SÉLECTIONNÉ] {symbol} ({mint[:6]}) valide tous les filtres de sécurité !")
    return True

# ---------------------------------------------------------------------------
# EXECUTION ACHAT / VENTE
# ---------------------------------------------------------------------------
async def execute_buy(mint: str) -> Optional[float]:
    """Exécute l'achat et renvoie le prix d'entrée exact."""
    logger.info(f" 🛒 [ACHAT EN COURS] Achat de {BUY_AMOUNT_SOL} SOL sur {mint}...")
    try:
        # TODO: Relier à l'API de Swaps PumpFun/Raydium ou Instruction On-Chain avec WALLET
        await asyncio.sleep(0.5) 
        entry_price = 1.0  # Prix fictif d'entrée (À remplacer par la réponse du Swap)
        logger.info(f" 💸 [ACHAT CONFIRMÉ] {mint} acheté au prix d'entrée : {entry_price:.6f}")
        return entry_price
    except Exception as e:
        logger.error(f" ❌ [ÉCHEC ACHAT] Impossible d'acheter {mint}: {e}")
        return None

async def execute_sell(mint: str, reason: str):
    """Exécute l'ordre de vente sur le réseau Solana."""
    logger.info(f" 🚨 [VENTE EN COURS] Token: {mint} | Raison: {reason}")
    try:
        # TODO: Relier à l'API de Swaps PumpFun/Raydium pour vider la position
        await asyncio.sleep(0.5) 
        logger.info(f" ✅ [VENTE CONFIRMÉE] {mint} vendu avec succès.")
    except Exception as e:
        logger.error(f" ❌ [ÉCHEC VENTE] Erreur lors de la vente de {mint}: {e}")

async def fetch_current_price(mint: str) -> float:
    """Récupère le prix actuel du token en temps réel."""
    # Simulation d'appel prix on-chain / websocket
    await asyncio.sleep(0.1)
    return 1.0

# ---------------------------------------------------------------------------
# STRATÉGIE DE GESTION DE POSITION (RÈGLES 1 & 2)
# ---------------------------------------------------------------------------
async def monitor_and_sell(mint: str, entry_price: float):
    """
    Gestion dynamique de la position :
    1. Garde tant que le prix monte (nouveau sommet). Vente si le PnL stagne pendant 5 min.
    2. Vente immédiate si baisse de 15% par rapport au prix d'achat initial.
    """
    logger.info(f" 📊 [MONITORING ACTIVÉ] {mint} | Prix d'achat d'entrée : {entry_price:.6f}")
    
    peak_price = entry_price
    last_peak_time = time.time()
    
    # Seuil absolu du Stop Loss (-15% du prix initial)
    stop_loss_price = entry_price * (1.0 + (STOP_LOSS_PCT / 100.0))
    logger.info(f" 🛑 [STOP LOSS] Seuil de vente strict à -15% fixé à : {stop_loss_price:.6f}")

    while True:
        await asyncio.sleep(2)  # Vérification du prix toutes les 2 secondes
        
        current_price = await fetch_current_price(mint)
        now = time.time()
        current_pnl_pct = ((current_price - entry_price) / entry_price) * 100

        # RÈGLE 2 : STOP LOSS STRICT (-15% DU PRIX INITIAL D'ACHAT)
        if current_price <= stop_loss_price:
            await execute_sell(
                mint, 
                f"Stop Loss -15% atteint ! (PnL: {current_pnl_pct:.2f}%)"
            )
            break

        # RÈGLE 1 : MISE À JOUR DU PEAK SI LE PRIX MONTE
        if current_price > peak_price:
            peak_price = current_price
            last_peak_time = now  # Réinitialise le délai de 5 minutes
            logger.info(f" 🚀 Nouveau Peak sur {mint} : {peak_price:.6f} (PnL: +{current_pnl_pct:.2f}%)")

        # RÈGLE 1 (SUITE) : VENTE APRÈS 5 MIN (300 SECONDES) DE STAGNATION
        time_since_last_peak = now - last_peak_time
        if time_since_last_peak >= STAGNATION_TIMEOUT:
            await execute_sell(
                mint, 
                f"Stagnation : Aucun nouveau peak depuis 5 min (PnL actuel: {current_pnl_pct:.2f}%)"
            )
            break

# ---------------------------------------------------------------------------
# ECOUTE WEBSOCKET EN DIRECT
# ---------------------------------------------------------------------------
async def listen_pumpfun_new_tokens():
    """Se connecte au flux PumpFun et traite les tokens en temps réel."""
    while True:
        try:
            async with websockets.connect(PUMPFUN_WS) as ws:
                payload = {"method": "subscribeNewToken"}
                await ws.send(json.dumps(payload))
                logger.info(" 📡 Connecté au flux WebSocket PumpFun (Recherche de pépites...)")

                async for message in ws:
                    data = json.loads(message)
                    mint = data.get("mint")
                    
                    if mint:
                        # 1. Application de la batterie de filtres
                        is_safe = await analyze_token_safety(data)
                        if not is_safe:
                            continue

                        # 2. Exécution de l'achat
                        entry_price = await execute_buy(mint)
                        
                        # 3. Lancement du suivi TP/SL si l'achat est validé
                        if entry_price:
                            asyncio.create_task(monitor_and_sell(mint, entry_price))

        except Exception as e:
            logger.error(f"Erreur connexion WebSocket : {e}. Nouvelle tentative dans 3 secondes...")
            await asyncio.sleep(3)

# ---------------------------------------------------------------------------
# SERVEUR WEB HEALTH CHECK (POUR RENDER) & DÉMARRAGE
# ---------------------------------------------------------------------------
async def handle_health(request):
    return web.Response(text="PumpFun Sniper Operational")

async def start_http_server():
    port = int(os.getenv("PORT", 10000))
    app = web.Application()
    app.router.add_get("/", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Serveur HTTP de contrôle démarré sur le port {port}")

async def main():
    await asyncio.gather(
        start_http_server(),
        listen_pumpfun_new_tokens()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Arrêt du Sniper.")
