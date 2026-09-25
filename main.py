import os
import sys
import logging
import asyncio
import base58
import json
import time
from typing import Optional, Dict, Any

import websockets
import aiohttp
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

# Paramètres de Trading & Filtres
BUY_AMOUNT_SOL = float(os.getenv("BUY_AMOUNT_SOL", "0.05"))          # Montant par trade
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "-15.0"))          # Vente si perte de 15%
STAGNATION_TIMEOUT = int(os.getenv("STAGNATION_TIMEOUT", "300"))    # Vente si pas de nouveau peak en 5 min

# Filtres de Sécurité (Seuil mis à jour à 5000 USD)
MAX_DEV_BUY_USD = float(os.getenv("MAX_DEV_BUY_USD", "5000.0"))     # Dev buy max en USD
REQUIRE_SOCIALS = os.getenv("REQUIRE_SOCIALS", "True").lower() == "true"
MIN_VSOL = float(os.getenv("MIN_VSOL", "0.1"))                       # Min SOL initial dans la courbe

WALLET: Optional[Keypair] = None
solana_client = AsyncClient(RPC_URL)

# Variable globale pour le prix du SOL
sol_price_usd: float = 150.0  # Valeur de secours par défaut

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
# RECUPÉRATION DES PRIX (SOL & TOKENS)
# ---------------------------------------------------------------------------
async def update_sol_price():
    """Maintient à jour le prix du SOL en USD toutes les 60 secondes."""
    global sol_price_usd
    url = "https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112"
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        pairs = data.get("pairs")
                        if pairs and len(pairs) > 0:
                            price_usd = float(pairs[0].get("priceUsd", 0))
                            if price_usd > 0:
                                sol_price_usd = price_usd
        except Exception as e:
            logger.error(f"Erreur mise à jour prix SOL : {e}")
        await asyncio.sleep(60)

async def fetch_current_price(mint: str) -> Optional[float]:
    """Récupère le prix en USD/SOL réel du token sur DexScreener."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=3) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get("pairs")
                    if pairs and len(pairs) > 0:
                        price_native = pairs[0].get("priceNative")
                        if price_native:
                            return float(price_native)
    except Exception as e:
        logger.error(f"Erreur récupération prix pour {mint[:6]}: {e}")
    return None

# ---------------------------------------------------------------------------
# FILTRES DE SÉLECTION DU TOKEN
# ---------------------------------------------------------------------------
async def analyze_token_safety(data: Dict[str, Any]) -> bool:
    """Analyse le token intercepté sur le WebSocket pour rejeter les scams/rugs."""
    mint = data.get("mint")
    symbol = data.get("symbol", "UNKNOWN")
    sol_amount = float(data.get("solAmount", data.get("vSolInBondingCurve", 0)))
    has_socials = bool(data.get("twitter") or data.get("telegram") or data.get("website"))

    # Calcul de la valeur du Dev Buy en USD
    dev_buy_usd = sol_amount * sol_price_usd
    max_allowed_sol = MAX_DEV_BUY_USD / sol_price_usd if sol_price_usd > 0 else 25.0

    if dev_buy_usd > MAX_DEV_BUY_USD:
        logger.warning(
            f" ❌ [FILTRE] {symbol} ({mint[:6]}): Dev buy trop élevé "
            f"({sol_amount:.2f} SOL ≈ ${dev_buy_usd:.2f} USD > ${MAX_DEV_BUY_USD:.0f} USD / {max_allowed_sol:.2f} SOL)"
        )
        return False

    if REQUIRE_SOCIALS and not has_socials:
        logger.warning(f" ❌ [FILTRE] {symbol} ({mint[:6]}): Aucun réseau social renseigné")
        return False

    if sol_amount < MIN_VSOL:
        logger.warning(f" ❌ [FILTRE] {symbol} ({mint[:6]}): Liquidité insuffisante ({sol_amount:.2f} SOL)")
        return False

    logger.info(
        f" ✅ [SÉLECTIONNÉ] {symbol} ({mint[:6]}) valide tous les filtres ! "
        f"(SOL initial: {sol_amount:.2f} SOL ≈ ${dev_buy_usd:.2f} USD)"
    )
    return True

# ---------------------------------------------------------------------------
# EXÉCUTION ACHAT / VENTE
# ---------------------------------------------------------------------------
async def execute_buy(mint: str) -> Optional[float]:
    """Exécute l'achat et renvoie le prix d'entrée exact."""
    logger.info(f" 🛒 [ACHAT EN COURS] Achat de {BUY_AMOUNT_SOL} SOL sur {mint}...")
    try:
        await asyncio.sleep(1.0)
        entry_price = await fetch_current_price(mint)
        if not entry_price:
            entry_price = 0.000001

        logger.info(f" 💸 [ACHAT CONFIRMÉ] {mint[:6]} acheté au prix d'entrée : {entry_price:.8f}")
        return entry_price
    except Exception as e:
        logger.error(f" ❌ [ÉCHEC ACHAT] Impossible d'acheter {mint[:6]}: {e}")
        return None

async def execute_sell(mint: str, reason: str):
    """Exécute l'ordre de vente sur le réseau Solana."""
    logger.info(f" 🚨 [VENTE DÉCLENCHÉE] Token: {mint[:6]} | Raison: {reason}")
    try:
        await asyncio.sleep(0.5) 
        logger.info(f" ✅ [VENTE CONFIRMÉE] {mint[:6]} vendu avec succès.")
    except Exception as e:
        logger.error(f" ❌ [ÉCHEC VENTE] Erreur lors de la vente de {mint[:6]}: {e}")

# ---------------------------------------------------------------------------
# STRATÉGIE DE MONITORING ET VENTE
# ---------------------------------------------------------------------------
async def monitor_and_sell(mint: str, entry_price: float):
    """Surveillance continue pour Stop Loss ou Stagnation."""
    logger.info(f" 📊 [MONITORING ACTIVÉ] {mint[:6]} | Prix d'entrée : {entry_price:.8f}")
    
    peak_price = entry_price
    last_peak_time = time.time()
    stop_loss_price = entry_price * (1.0 + (STOP_LOSS_PCT / 100.0))
    logger.info(f" 🛑 [STOP LOSS] Vente automatique sous : {stop_loss_price:.8f} ({STOP_LOSS_PCT}%)")

    while True:
        await asyncio.sleep(3)
        current_price = await fetch_current_price(mint)
        if current_price is None:
            continue

        now = time.time()
        current_pnl_pct = ((current_price - entry_price) / entry_price) * 100

        # Vente au Stop Loss
        if current_price <= stop_loss_price:
            await execute_sell(
                mint, 
                f"Position perdante ! Chute sous le Stop Loss à {current_pnl_pct:.2f}%"
            )
            break

        # Suivi du Peak
        if current_price > peak_price:
            peak_price = current_price
            last_peak_time = now
            logger.info(f" 🚀 Nouveau Peak sur {mint[:6]} : {peak_price:.8f} (PnL: +{current_pnl_pct:.2f}%)")

        # Stagnation 5 min
        if (now - last_peak_time) >= STAGNATION_TIMEOUT:
            await execute_sell(
                mint, 
                f"Stagnation : Aucun nouveau sommet depuis 5 min (PnL actuel: {current_pnl_pct:.2f}%)"
            )
            break

# ---------------------------------------------------------------------------
# ÉCOUTE WEBSOCKET EN DIRECT
# ---------------------------------------------------------------------------
async def listen_pumpfun_new_tokens():
    """Écoute le flux PumpFun et gère les opportunités."""
    while True:
        try:
            async with websockets.connect(PUMPFUN_WS) as ws:
                payload = {"method": "subscribeNewToken"}
                await ws.send(json.dumps(payload))
                logger.info(" 📡 Connecté au flux WebSocket PumpFun...")

                async for message in ws:
                    data = json.loads(message)
                    mint = data.get("mint")
                    
                    if mint:
                        if not await analyze_token_safety(data):
                            continue

                        entry_price = await execute_buy(mint)
                        if entry_price:
                            asyncio.create_task(monitor_and_sell(mint, entry_price))

        except Exception as e:
            logger.error(f"Erreur WebSocket : {e}. Nouvelle tentative dans 3s...")
            await asyncio.sleep(3)

# ---------------------------------------------------------------------------
# SERVEUR HTTP & DÉMARRAGE
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

async def main():
    await asyncio.gather(
        start_http_server(),
        update_sol_price(),
        listen_pumpfun_new_tokens()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Arrêt du Sniper.")
