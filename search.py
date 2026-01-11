import os
import requests
from datetime import datetime, timezone
from typing import Tuple, Dict, Any, Optional

# ==========================================
# Ambiente (test vs prod)
# ==========================================
AMADEUS_ENV = os.getenv("AMADEUS_ENV", "test").lower().strip()

BASE_URL = "https://api.amadeus.com" if AMADEUS_ENV == "prod" else "https://test.api.amadeus.com"
TOKEN_URL = f"{BASE_URL}/v1/security/oauth2/token"
FLIGHT_OFFERS_URL = f"{BASE_URL}/v2/shopping/flight-offers"

# ==========================================
# Modo TEMP (opcional; só pra teste rápido)
# Se preencher AMADEUS_TEMP_CLIENT_ID/SECRET no ambiente,
# eles têm prioridade.
# ==========================================
TEMP_CLIENT_ID = os.getenv("AMADEUS_TEMP_CLIENT_ID", "").strip()
TEMP_CLIENT_SECRET = os.getenv("AMADEUS_TEMP_CLIENT_SECRET", "").strip()


def _debug_banner(client_id: str) -> None:
    # Não imprime segredo, só prefixo
    print("=======================================")
    print("AMAD_RUN_UTC:", datetime.now(timezone.utc).isoformat())
    print("AMAD_ENV:", AMADEUS_ENV)
    print("AMAD_BASE_URL:", BASE_URL)
    print("AMAD_CLIENT_ID_PREFIX:", (client_id[:6] if client_id else "—"))
    print("=======================================")


def get_amadeus_creds() -> Tuple[str, str]:
    """
    Ordem de prioridade:
    1) AMADEUS_TEMP_CLIENT_ID / AMADEUS_TEMP_CLIENT_SECRET (se existirem)
    2) AMADEUS_CLIENT_ID / AMADEUS_CLIENT_SECRET (padrão do seu Actions)
    3) AMADEUS_CLIENT_ID_TEST/PROD e AMADEUS_CLIENT_SECRET_TEST/PROD (fallback)
    """
    if TEMP_CLIENT_ID and TEMP_CLIENT_SECRET:
        return TEMP_CLIENT_ID, TEMP_CLIENT_SECRET

    if "AMADEUS_CLIENT_ID" in os.environ and "AMADEUS_CLIENT_SECRET" in os.environ:
        return os.environ["AMADEUS_CLIENT_ID"], os.environ["AMADEUS_CLIENT_SECRET"]

    if AMADEUS_ENV == "prod":
        return os.environ["AMADEUS_CLIENT_ID_PROD"], os.environ["AMADEUS_CLIENT_SECRET_PROD"]

    return os.environ["AMADEUS_CLIENT_ID_TEST"], os.environ["AMADEUS_CLIENT_SECRET_TEST"]


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

    if resp.status_code >= 400:
        raise RuntimeError(f"Erro ao obter token Amadeus ({resp.status_code}): {resp.text}")

    j = resp.json()
    token = j.get("access_token")
    if not token:
        raise RuntimeError(f"Token não encontrado na resposta Amadeus: {j}")
    return token


def search_flights(
    origin: str,
    destination: str,
    departure_date: str,
    return_date: Optional[str] = None,
    adults: int = 1,
    children: int = 0,
    travel_class: str = "ECONOMY",
    currency: str = "BRL",
    nonstop: bool = True,
    max_results: int = 10,
) -> Dict[str, Any]:
    """
    Busca ofertas de voo via Amadeus Flight Offers Search.

    departure_date / return_date: 'YYYY-MM-DD'
    travel_class: ECONOMY | PREMIUM_ECONOMY | BUSINESS | FIRST
    """
    client_id, client_secret = get_amadeus_creds()
    _debug_banner(client_id)

    token = amadeus_get_token(client_id, client_secret)

    headers = {"Authorization": f"Bearer {token}"}

    params: Dict[str, Any] = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": departure_date,
        "adults": int(adults),
        "children": int(children),
        "travelClass": travel_class,
        "currencyCode": currency,
        "nonStop": "true" if nonstop else "false",
        "max": int(max_results),
    }

    # round-trip (se aplicável)
    if return_date:
        params["returnDate"] = return_date

    resp = requests.get(
        FLIGHT_OFFERS_URL,
        headers=headers,
        params=params,
        timeout=30,
    )

    if resp.status_code >= 400:
        raise RuntimeError(f"Erro ao buscar ofertas ({resp.status_code}): {resp.text}")

    return resp.json()


if __name__ == "__main__":
    # teste local rápido (não precisa rodar em produção)
    # Ajuste as datas se quiser.
    data = search_flights(
        origin="GRU",
        destination="FCO",
        departure_date="2026-09-01",
        return_date="2026-09-11",
        adults=2,
        children=1,
        travel_class="ECONOMY",
        currency="BRL",
        nonstop=True,
        max_results=5,
    )
    print("OK - resposta possui chaves:", list(data.keys()))
