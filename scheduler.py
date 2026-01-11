import os
import requests
from datetime import datetime, timezone
from typing import Tuple, Dict, Any

# ==========================================
# 1) CONFIG DE AMBIENTE (test vs prod)
# ==========================================
AMADEUS_ENV = os.getenv("AMADEUS_ENV", "test").lower().strip()

BASE_URL = "https://api.amadeus.com" if AMADEUS_ENV == "prod" else "https://test.api.amadeus.com"
TOKEN_URL = f"{BASE_URL}/v1/security/oauth2/token"
FLIGHT_OFFERS_URL = f"{BASE_URL}/v2/shopping/flight-offers"

# ==========================================
# 2) MODO TEMPORÁRIO (APENAS PARA TESTE)
# ==========================================
TEMP_CLIENT_ID = os.getenv("AMADEUS_TEMP_CLIENT_ID", "").strip() or "COLE_SUA_CHAVE_NOVA_AQUI"
TEMP_CLIENT_SECRET = os.getenv("AMADEUS_TEMP_CLIENT_SECRET", "").strip() or "COLE_SEU_SECRET_NOVO_AQUI"

# ==========================================
# 3) FUNÇÃO: pegar credenciais (prioridade TEMP)
# ==========================================
def get_amadeus_creds() -> Tuple[str, str]:
    temp_filled = (
        TEMP_CLIENT_ID
        and TEMP_CLIENT_SECRET
        and "COLE_SUA_CHAVE" not in TEMP_CLIENT_ID
        and "COLE_SEU_SECRET" not in TEMP_CLIENT_SECRET
    )
    if temp_filled:
        return TEMP_CLIENT_ID, TEMP_CLIENT_SECRET

    if AMADEUS_ENV == "prod":
        return os.environ["AMADEUS_CLIENT_ID_PROD"], os.environ["AMADEUS_CLIENT_SECRET_PROD"]
    return os.environ["AMADEUS_CLIENT_ID_TEST"], os.environ["AMADEUS_CLIENT_SECRET_TEST"]

# ==========================================
# 4) DEBUG: prova de vida
# ================================
