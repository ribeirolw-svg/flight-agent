import os
import requests
from datetime import datetime, timezone
from typing import Tuple, Dict, Any, Optional

# ==========================================
# 1) CONFIG DE AMBIENTE (test vs prod)
# ==========================================
AMADEUS_ENV = os.getenv("AMADEUS_ENV", "test").lower().strip()

BASE_URL = "https://api.amadeus.com" if AMADEUS_ENV == "prod" else "https://test.api.amadeus.com"
TOKEN_URL = f"{BASE_URL}/v1/security/oauth2/token"
FLIGHT_OFFERS_URL = f"{BASE_URL}/v2/shopping/flight-offers"

# ==========================================
# 2) MODO TEMPORÁRIO (APENAS PARA TESTE)
#    - Cole suas chaves novas aqui
#    - Depois remova e use apenas ENV
# ==========================================
TEMP_CLIENT_ID = os.getenv("AMADEUS_TEMP_CLIENT_ID", "").strip() or "COLE_SUA_CHAVE_NOVA_AQUI"
TEMP_CLIENT_SECRET = os.getenv("AMADEUS_TEMP_CLIENT_SECRET", "").strip() or "COLE_SEU_SECRET_NOVO_AQUI"

# ==========================================
# 3) FUNÇÃO: pegar credenciais (prioridade TEMP)
# ==========================================
def get_amadeus_creds() -> Tuple[str, str]:
    """
    Prioridade:
    1) Se TEMP_* estiver preenchido (e não for placeholder), usa TEMP.
    2) Caso contrário, usa variáveis de ambiente:
       - TEST: AMADEUS_CLIENT_ID_TEST / AMADEUS_CLIENT_SECRET_TEST
       - PROD: AMADEUS_CLIENT_ID_PROD / AMADEUS_CLIENT_SECRET_PROD
    """
    # Detecta se TEMP foi realmente preenchido
    temp_filled = (
        TEMP_CLIENT_ID
        and TEMP_CLIENT_SECRET
        and "COLE_SUA_CHAVE" not in TEMP_CLIENT_ID
        and "COLE_SEU_SECRET" not in TEMP_CLIENT_SECRET
    )
    if temp_filled:
        return TEMP_CLIENT_ID, TEMP_CLIENT_SECRET

    # ENV mode
    if AMADEUS_ENV == "prod":
        return os.environ["AMADEUS_CLIENT_ID_PROD"], os.environ["AMADEUS_CLIENT_SECRET_PROD"]
    return os.environ["AMADEUS_CLIENT_ID_TEST"], os.environ["AMADEUS_CLIENT_SECRET_TEST"]

# ==========================================
# 4) DEBUG: prova de vida (sem expor segredo)
# ==========================================
def debug_banner(client_id: str) -> None:
    print("=======================================")
    print("RUN_UTC:", datetime.now(timezone.utc).isoformat())
    print("ENV:", AMADEUS_ENV)
    print("BASE_URL:", BASE_URL)
    print("CLIENT_ID_PREFIX:", client_id[:6])
    print("=======================================")

# ==========================================
# 5) TOKEN
# ==========================================
def amadeus_get_token(client_id: str, client_secret: str) -> str:
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )

    # Mensagem mais amigável se der erro
    if resp.status_code >= 400:
        raise RuntimeError(f"Erro ao obter token Amadeus ({resp.status_code}): {resp.text}")

    return resp.json()["access_token"]

# ==========================================
# 6) SEARCH FLIGHTS
# ==========================================
def search_flights(
    origin: str,
    destination: str,
    departure_d_
