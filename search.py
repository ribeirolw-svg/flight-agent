# search.py (REAL - Amadeus)
from __future__ import annotations

import os
import time
import hashlib
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import requests


AMADEUS_TEST_BASE = "https://test.api.amadeus.com"
AMADEUS_PROD_BASE = "https://api.amadeus.com"


def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _daterange(start: date, end: date, step_days: int) -> List[date]:
    if step_days < 1:
        step_days = 1
    out = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur = cur.fromordinal(cur.toordinal() + step_days)
    return out


def _stable_key(route: dict[str, Any]) -> str:
    origin = str(route.get("origin", "")).upper()
    dest = str(route.get("destination", "")).upper()
    dep = route.get("departure_window", {}) or {}
    dep_from = str(dep.get("from", ""))
    dep_to = str(dep.get("to", ""))
    return_date = str(route.get("return_date", ""))  # agora é data fixa
    cabin = str(route.get("cabin", "ECONOMY") or "ECONOMY").upper()
    adults = int(route.get("adults", 1) or 1)
    children = int(route.get("children", 0) or 0)
    currency = str(route.get("currency", "") or "")

    raw = f"{origin}-{dest}|{dep_from}:{dep_to}|return:{return_date}|{cabin}|A{adults}|C{children}|{currency}"
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    return f"{origin}-{dest}-{dep_from}-{dep_to}-R{return_date}-{cabin}-A{adults}-C{children}-{currency}-{h}"


class AmadeusClient:
    def __init__(self) -> None:
        self.client_id = _env("AMADEUS_CLIENT_ID")
        self.client_secret = _env("AMADEUS_CLIENT_SECRET")
        env = os.getenv("AMADEUS_ENV", "test").lower().strip()
        self.base = AMADEUS_PROD_BASE if env in ("prod", "production") else AMADEUS_TEST_BASE

        self._token: Optional[str] = None
        self._token_expiry: float = 0.0  # epoch seconds

    def _get_token(self) -> str:
        now = time.time()
        if self._token and now < self._token_expiry - 30:
            return self._token

        url = f"{self.base}/v1/security/oauth2/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        r = requests.post(url, data=data, headers=headers, timeout=30)
        r.raise_for_status()
        payload = r.json()
        self._token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 1799))
        self._token_expiry = now + expires_in
        return self._token

    def flight_offers_search(
        self,
        origin: str,
        destination: str,
        departure_date: str,
        return_date: str,
        adults: int,
        children: int,
        travel_class: str,
        currency_code: str,
        max_results: int = 20,
    ) -> Dict[str, Any]:
        token = self._get_token()
        url = f"{self.base}/v2/shopping/flight-offers"

        params = {
            "originLocationCode": origin,
            "destinationLocationCode": destination,
            "departureDate": departure_date,
            "returnDate": return_date,
            "adults": adults,
            "children": children,
            "travelClass": travel_class,
            "currencyCode": currency_code,
            "max": max_results,
        }

        headers = {"Authorization": f"Bearer {token}"}
        r = requests.get(url, params=params, headers=headers, timeout=45)
        r.raise_for_status()
        return r.json()


def _min_price_from_offers(payload: Dict[str, Any]) -> Optional[float]:
    data = payload.get("data", [])
    if not data:
        return None
    prices = []
    for offer in data:
        # total costuma vir como string
        total = offer.get("price", {}).get("total")
        if total is None:
            continue
        try:
            prices.append(float(total))
        except Exception:
            pass
    return min(prices) if prices else None


def run_search(profile: dict[str, Any]) -> list[dict[str, Any]]:
    routes = profile.get("routes", [])
    if not isinstance(routes, list) or not routes:
        raise ValueError("Profile must contain a non-empty 'routes' list")

    client = AmadeusClient()
    results: list[dict[str, Any]] = []

    for route in routes:
        origin = str(route["origin"]).upper()
        destination = str(route["destination"]).upper()

        dep = route.get("departure_window", {}) or {}
        dep_from = _parse_date(dep["from"])
        dep_to = _parse_date(dep["to"])

        return_date = str(route["return_date"])  # data fixa
        step = int(route.get("departure_step_days", 2) or 2)

        cabin = str(route.get("cabin", "ECONOMY")).upper()
        adults = int(route.get("adults", 1) or 1)
        children = int(route.get("children", 0) or 0)
        currency = str(route.get("currency", "BRL")).upper()

        best_price: Optional[float] = None
        best_dep: Optional[str] = None

        # varre as datas de ida (amostrando para poupar quota)
        for d in _daterange(dep_from, dep_to, step_days=step):
            dep_date_str = d.isoformat()
            try:
                payload = client.flight_offers_search(
                    origin=origin,
                    destination=destination,
                    departure_date=dep_date_str,
                    return_date=return_date,
                    adults=adults,
                    children=children,
                    travel_class=cabin,
                    currency_code=currency,
                    max_results=20,
                )
                p = _min_price_from_offers(payload)
                if p is not None and (best_price is None or p < best_price):
                    best_price = p
                    best_dep = dep_date_str
            except requests.HTTPError as e:
                # se estourar quota/429, você vai ver aqui no log do Actions
                raise RuntimeError(f"Amadeus HTTP error for {origin}-{destination} dep={dep_date_str}: {e}") from e

            # respeitar rate limit (bem conservador)
            time.sleep(0.12)

        key = _stable_key(route)

        if best_price is None:
            # Sem ofertas encontradas (ou rota inválida) — mantém registro pra diagnóstico
            results.append(
                {
                    "key": key,
                    "price": float("inf"),
                    "currency": currency,
                    "summary": f"{origin}→{destination} no offers found in {dep_from}..{dep_to} return={return_date}",
                    "deeplink": "",
                }
            )
        else:
            results.append(
                {
                    "key": key,
                    "price": float(best_price),
                    "currency": currency,
                    "summary": (
                        f"{origin}→{destination} best_dep={best_dep} return={return_date} "
                        f"cabin={cabin} adults={adults} children={children}"
                    ),
                    "deeplink": "",
                }
            )

    return results
