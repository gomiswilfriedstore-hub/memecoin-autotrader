import os
import time
import asyncio
import logging

# Configuration des Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ==========================================
# CONFIGURATION OPTIMISÉE & ÉQUILIBRÉE
# ==========================================

BUY_AMOUNT_USD = float(os.getenv("BUY_AMOUNT_USD", "2.50"))      # Mise fixe par trade
REQUIRE_SOCIALS = os.getenv("REQUIRE_SOCIALS", "False").lower() == "true"
MAX_DEV_BUY_USD = float(os.getenv("MAX_DEV_BUY_USD", "100.0"))    # Réduit à $100 pour plus de sécurité

# GESTION DES RISQUES & TRAILING PROGRESSIF
INITIAL_STOP_LOSS_PCT = -15.0  # Perte max initiale (~$0.37)
BASE_TRAILING_PCT = 18.0       # Distance sous le peak au début (18%)
WIDE_TRAILING_PCT = 25.0       # Distance élargie à 25% si le token dépasse +50% de hausse
BREAKEVEN_TRIGGER_PCT = 30.0   # À +30%, le stop monte au prix d'entrée + frais (+2%)
MAX_HOLD_TIME_SEC = 180        # Timeout ramené à 3 minutes

# Blacklist renforcée des spams et tokens suspects
BANNED_NAMES = [
    "YO", "TEST", "PUMP", "SOL", "UNKNOWN", "NULL", "MOON", 
    "MEME", "COIN", "DOGE", "PEPE", "SHIB", "DEV", "ANON", "INU", "ELON"
]

# ==========================================
# FILTRES DE SÉCURITÉ
# ==========================================

def validate_token_filters(token_data: dict) -> tuple[bool, str]:
    symbol = token_data.get("symbol", "").upper().strip()
    name = token_data.get("name", "").upper().strip()
    dev_buy_usd = float(token_data.get("dev_buy_usd", 0.0))
    has_socials = bool(token_data.get("twitter") or token_data.get("telegram") or token_data.get("website"))

    # 1. Filtre Spam / Noms génériques
    if symbol in BANNED_NAMES or any(banned in name for banned in BANNED_NAMES if len(banned) > 2):
        return False, f"Nom/Symbole suspect ('{symbol}')"

    if len(name) < 2 or len(symbol) < 2:
        return False, "Nom ou symbole trop court"

    # 2. Filtre Réseaux Sociaux (Optionnel)
    if REQUIRE_SOCIALS and not has_socials:
        return False, "Aucun réseau social"

    # 3. Filtre Anti-Dev Dump ($100 max)
    if dev_buy_usd > MAX_DEV_BUY_USD:
        return False, f"Achat initial Dev trop élevé (${dev_buy_usd:.2f} > ${MAX_DEV_BUY_USD:.2f})"

    return True, "Filtres validés"

# ==========================================
# MOTEUR D'EXÉCUTION & TRAILING ADAPTATIF
# ==========================================

async def execute_trade(token_data: dict):
    mint = token_data.get("mint")
    symbol = token_data.get("symbol")
    
    logging.info(f"🛒 [ACHAT] Ordre de ${BUY_AMOUNT_USD:.2f} sur {symbol} ({mint[:8]}...)")
    
    # Place ton instruction d'achat Web3 / PumpFun SDK ici
    # await buy_token(mint, BUY_AMOUNT_USD)
    
    entry_price = float(token_data.get("initial_price", 1.0))
    highest_price = entry_price
    start_time = time.time()
    
    stop_loss_price = entry_price * (1 + (INITIAL_STOP_LOSS_PCT / 100.0))
    breakeven_secured = False

    logging.info(f"✅ [EXÉCUTÉ] {symbol} | Prix Entrée: {entry_price:.6f} | SL Initial: {stop_loss_price:.6f}")

    while True:
        await asyncio.sleep(1.2)  # Fréquence de scan de 1.2 seconde
        
        # Récupération du prix actuel via ton WebSocket ou RPC
        current_price = entry_price  # Remplace par float(get_current_price(mint))
        elapsed_time = time.time() - start_time
        
        current_pnl_pct = ((current_price - entry_price) / entry_price) * 100
        peak_pnl_pct = ((highest_price - entry_price) / entry_price) * 100

        # 1. Mise à jour du plus haut historique (ATH Local)
        if current_price > highest_price:
            highest_price = current_price
            peak_pnl_pct = ((highest_price - entry_price) / entry_price) * 100

        # 2. Sécurisation "Breakeven" dès qu'on touche +30%
        if peak_pnl_pct >= BREAKEVEN_TRIGGER_PCT and not breakeven_secured:
            breakeven_price = entry_price * 1.02  # Prix d'entrée + 2% pour couvrir les frais de gaz
            if breakeven_price > stop_loss_price:
                stop_loss_price = breakeven_price
                breakeven_secured = True
                logging.info(f"🛡️ [BREAKEVEN] {symbol} a atteint +{peak_pnl_pct:.1f}% ! Capital sécurisé (SL remonté à +2%).")

        # 3. Calcul de la distance du Trailing Stop selon la hausse atteinte
        if peak_pnl_pct >= 50.0:
            active_trailing_distance = WIDE_TRAILING_PCT  # 25% si gros pump
        else:
            active_trailing_distance = BASE_TRAILING_PCT  # 18% par défaut

        # 4. Ajustement dynamique du Trailing Stop
        if peak_pnl_pct > 0:
            new_stop_loss = highest_price * (1 - (active_trailing_distance / 100.0))
            if new_stop_loss > stop_loss_price:
                stop_loss_price = new_stop_loss
                logging.info(f"📈 [TRAILING -> {symbol}] Peak: +{peak_pnl_pct:.1f}% | Stop suiveur (-{active_trailing_distance}%): {stop_loss_price:.6f}")

        # 5. Condition de sortie : Prix passe sous le Stop
        if current_price <= stop_loss_price:
            if current_pnl_pct >= 0:
                logging.info(f"🎯 [PROFIT] Vente de {symbol} ! Gain net: +{current_pnl_pct:.2f}%")
            else:
                logging.warning(f"🛑 [STOP LOSS] Vente de {symbol} ! Perte: {current_pnl_pct:.2f}%")
            
            # Place ton instruction de vente Web3 / PumpFun SDK ici
            # await sell_token(mint)
            break

        # 6. Condition de sortie : Timeout 3 minutes
        if elapsed_time >= MAX_HOLD_TIME_SEC:
            logging.info(f"⏱️ [TIMEOUT 3M] Stagnation sur {symbol} (PnL: {current_pnl_pct:.2f}%). Vente !")
            # await sell_token(mint)
            break

# ==========================================
# BOUCLE PRINCIPALE
# ==========================================

async def process_new_token(token_data: dict):
    mint = token_data.get("mint", "UNKNOWN")
    symbol = token_data.get("symbol", "N/A")

    is_valid, reason = validate_token_filters(token_data)

    if not is_valid:
        logging.warning(f"❌ [FILTRE] {symbol} ({mint[:6]}): {reason}")
        return

    logging.info(f"✅ [ÉLIGIBLE] Token {symbol} ({mint[:6]}) validé !")
    await execute_trade(token_data)

async def main():
    logging.info("🚀 Bot PumpFun démarré en mode ÉQUILIBRÉ (SÉCURITÉ & RENTABILITÉ)")
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
