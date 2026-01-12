from __future__ import annotations

import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# -------------------------------------------------------------------
# PATH FIX: search.py está na raiz, mas utilitario está em /app
# -------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent
APP_DIR = ROOT_DIR / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from utilitario.history_store import HistoryStore  # noqa: E402


# -------------------------------------------------------------------
# Amadeus endpoints
# -------------------------------------------------------------------
BASE_URL = os.environ.get("AMADEUS_BASE_URL", "https://test.api.amadeus.com").strip()
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


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, str):
            return float(x.strip().replace(",", "."))
        if isinstance(x, (int, float)):
            return float(x)
    except Exception:
        return None
    return None


def _extract_best_price_currency_offerscount(payload: Any) -> Tuple[Optional[float], Optional[str], int]:
    """
    Extrai:
      - best_price (menor grandTotal/total)
      - currency (se houver)
      - offers_count
    Espera payload padrão Amadeus: {"data":[{...offer...}, ...]}
    """
    if not isinstance(payload, dict):
        return None, None, 0

    data = payload.get("data")
    if not isinstance(data, list):
        return None, None, 0

    best: Optional[float] = None
    currency: Optional[str] = None
    offers_count = len(data)

    # Limita iteração (segurança/perf)
    for offer in data[:500]:
        if not isinstance(offer, dict):
            continue

        price = offer.get("price")
        if isinstance(price, dict):
            currency = currency or price.get("currency")
            gt = price.get("grandTotal") or price.get("total")
            v = _to_float(gt)
            if v is not None:
                best = v if best is None else min(best, v)

    return best, currency, offers_count


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
    max_results: int = 50,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": departure_date,
        "adults": adults,
        "travelClass": cabin,
        "currencyCode": currency,
        "nonStop": "true" if direct_only else "false",
        "max": max_results,
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
    max_results: int = 50,
    save_raw: bool = False,
) -> Dict[str, Any]:
    """
    Executa a busca e grava SEMPRE no histórico um payload 'achatado' para o dashboard:
      origin, destination, best_price, offers_count, currency, error, etc.

    Retorna o payload gravado.
    """
    store = HistoryStore(store_name)
    run_id = uuid.uuid4().hex[:12]
    t0 = time.time()

    try:
        token = amadeus_get_token(client_id, client_secret)
        offers_payload = amadeus_search_offers(
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
            max_results=max_results,
        )

        best_price, detected_currency, offers_count = _extract_best_price_currency_offerscount(offers_payload)

        payload: Dict[str, Any] = {
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

        if save_raw:
            payload["raw"] = offers_payload  # opcional (pesa o jsonl)

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
