from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import requests

from utilitario.history_store import HistoryStore


BASE_URL = "https://test.api.amadeus.com"
TOKEN_URL = f"{BASE_URL}/v1/security/oauth2/token"
FLIGHT_OFFERS_URL = f"{BASE_URL}/v2/shopping/flight-offers"


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
    resp.raise_for_status()
    return resp.json()["access_token"]


def _extract_best_price_currency(offers_data: Any) -> Tuple[Optional[float], Optional[str], int]:
    """
    Tenta extrair:
    - menor preço (best_price)
    - currency
    - offers_count

    Funciona para payloads padrão do Amadeus: {"data":[...]}
    """
    if not isinstance(offers_data, dict):
        return None, None, 0

    data = offers_data.get("data")
    if not isinstance(data, list):
        return None, None, 0

    best: Optional[float] = None
    cur: Optional[str] = None
    count = len(data)

    for offer in data[:300]:
        if not isinstance(offer, dict):
            continue
        price = offer.get("price")
        if isinstance(price, dict):
            cur = cur or price.get("currency")
            gt = price.get("grandTotal") or price.get("total")
            try:
                if gt is not None:
                    v = float(str(gt).replace(",", "."))
                    if best is None or v < best:
                        best = v
            except Exception:
                pass

    return best, cur, count


def amadeus_search_offers(
    token: str,
    origin: str,
    destination: str,
    departure_date: str,
    return_date: Optional[str],
    adults: int = 1,
    children: int = 0,
    cabin: str = "ECONOMY",
    currency: str = "BRL",
    direct_only: bool = True,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": departure_date,
        "adults": adults,
        "travelClass": cabin,
        "currencyCode": currency,
        "nonStop": "true" if direct_only else "false",
        "max": 50,
    }
    if children:
        params["children"] = children
    if return_date:
        params["returnDate"] = return_date

    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.get(FLIGHT_OFFERS_URL, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def run_search_and_store(
    *,
    store_name: str,
    client_id: str,
    client_secret: str,
    origin: str,
    destination: str,
    departure_date: str,
    return_date: Optional[str],
    adults: int,
    children: int,
    cabin: str,
    currency: str,
    direct_only: bool,
) -> Dict[str, Any]:
    """
    Executa a busca e grava SEMPRE no histórico um payload achatado (útil pro dashboard).
    """
    store = HistoryStore(store_name)
    run_id = uuid.uuid4().hex[:12]

    t0 = time.time()
    try:
        token = amadeus_get_token(client_id, client_secret)
        offers = amadeus_search_offers(
            token=token,
            origin=origin,
            destination=destination,
            departure_date=departure_date,
            return_date=return_date,
            adults=adults,
            children=children,
            cabin=cabin,
            currency=currency,
            direct_only=direct_only,
        )
        best_price, detected_currency, offers_count = _extract_best_price_currency(offers)

        payload = {
            "run_id": run_id,
            "origin": origin,
            "destination": destination,
            "departure_date": departure_date,
            "return_date": return_date,
            "adults": adults,
            "children": children,
            "cabin": cabin,
            "currency": detected_currency or currency,
            "direct_only": direct_only,
            "offers_count": offers_count,
            "best_price": best_price,
            "elapsed_s": round(time.time() - t0, 3),
            "error": None,
        }

        # opcional: guarda o raw (se quiser)
        # payload["raw"] = offers

        store.append("flight_search", payload)
        return payload

    except Exception as e:
        payload = {
            "run_id": run_id,
            "origin": origin,
            "destination": destination,
            "departure_date": departure_date,
            "return_date": return_date,
            "adults": adults,
            "children": children,
            "cabin": cabin,
            "currency": currency,
            "direct_only": direct_only,
            "offers_count": 0,
            "best_price": None,
            "elapsed_s": round(time.time() - t0, 3),
            "error": str(e),
        }
        store.append("flight_search", payload)
        return payload
