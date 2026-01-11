import requests
import os
from datetime import datetime, timezone

# ======== CONFIG TEMPORÁRIA ========
# APAGAR ISSO DEPOIS!
TEMP_CLIENT_ID = "COLE_SUA_CHAVE_NOVA_AQUI"
TEMP_CLIENT_SECRET = "COLE_SEU_SECRET_NOVO_AQUI"

# Alternar ambiente
AMADEUS_ENV = os.getenv("AMADEUS_ENV", "test").lower().strip()

BASE_URL = "https://api.amadeus.com" if AMADEUS_ENV == "prod" else "https://test.api.amadeus.com"

TOKEN_URL = f"{BASE_URL}/v1/security/oauth2/token"
FLIGHT_OFFERS = f"{BASE_URL}/v2/shopping/flight-offers"

# ======== DEBUG DE VIDA ========
print("=======================================")
print("RUN:", datetime.now(timezone.utc).isoformat())
print("ENV:", AMADEUS_ENV)
print("BASE_URL:", BASE_URL)
print("CLIENT_ID PREFIX:", TEMP_CLIENT_ID[:6])
print("=======================================")

def amadeus_get_token():
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": TEMP_CLIENT_ID,
            "client_secret": TEMP_CLIENT_SECRET,
        },
        timeout=30,
    )

    response.raise_for_status()
    return response.json()["access_token"]


def search_flights(origin, destination, departure_date):
    token = amadeus_get_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    params = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": departure_date,
        "adults": 1,
        "currencyCode": "BRL",
        "nonStop": "true",
        "max": 5
    }

    response = requests.get(
        FLIGHT_OFFERS,
        headers=headers,
        params=params,
        timeout=30,
    )

    response.raise_for_status()
    return response.json()
