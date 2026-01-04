# search.py (Amadeus real + return until)
from __future__ import annotations

import os
import time
import hashlib
from datetime import date, datetime
from typing import Any, Dict, List, Optional

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


def _add_days(d: date, days: int) -> date:
    return d.fromordinal(d.toordinal() + days)


def _daterange(start: date, end: date, step_days: int) -> List[date]:
    step_days = max(1, int(step_days))
    out = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur = _add_days(cur, step_days)
    return out


def _stable_key(route: dict[str, Any]) -> str:
    origin = str(route.get("origin", "")).upper()
    dest = str(route.get("destination", "")).upper()
    dep = route.get("departure_window", {}) or {}
    dep_from = str(dep.get("from", ""))
    dep_to = str(dep.get("to", ""))

    return_latest = str(route.get("return_latest", ""))  # limit
    cabin = str(route.get("cabin", "ECONOMY") or "ECONOMY").upper()
    adults = int(route.get("adults", 1) or 1)
    children = int(route.get("children", 0) or 0)
    currency = str(route.get("currency", "") or "")

    raw = f"{origin}-{dest}|{dep_from}:{dep_to}|return<= {return_latest}|{cabin}|A{adults}|C{children}|{currency}"
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    return f"{origin}-{dest}-{dep_from}-{dep_to}-RL{return_latest}-{cabin}-A{adults}-C{children}-{currency}-{h}"


class AmadeusClient:
    def __init__(self) -> None:
        self.client_id = _env("AMADEUS_CLIENT_ID")
        self.client_secret = _env("AMADEUS_CLIENT_SECRET")
        env = os.getenv("AMADEUS_ENV", "test").lower().strip()
        self.base = AMADEUS_PROD_BASE if env in ("prod", "production") else AMADEUS_TEST_BASE
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0

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

    def flight_offers_search(self, params: Dict[str, Any]) -> Dict[str, Any]:
        token = self._get_token()
        url = f"{self.base}/v2/shopping/flight-offers"
        headers = {"Authorization": f"Bearer {token}"}

        # retry com backoff para 429/5xx
        backoff = 1.0
        for attempt in range(1, 6):  # até 5 tentativas
            r = requests.get(url, params=params, headers=headers, timeout=45)

            if r.status_code == 429:
                # tenta respeitar Retry-After se existir
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra and ra.isdigit() else backoff
                time.sleep(wait)
                backoff = min(backoff * 2, 16.0)
                continue

            if 500 <= r.status_code <= 599:
                time.sleep(backoff)
                backoff = min(backoff * 2, 16.0)
                continue

            r.raise_for_status()
            return r.json()

        # se estourou tentativas, levanta erro com contexto
        r.raise_for_status()
        return r.json()

def _min_price_from_offers(payload: Dict[str, Any]) -> Optional[float]:
    data = payload.get("data", [])
    if not data:
        return None
    best = None
    for offer in data:
        total = offer.get("price", {}).get("total")
        if total is None:
            continue
        try:
            p = float(total)
        except Exception:
            continue
        best = p if best is None else min(best, p)
    return best

def _min_price_by_carrier(payload: Dict[str, Any]) -> Dict[str, float]:
    """
    Returns a dict: { "AZ": 10742.30, "TP": 10210.00, ... }
    Uses validatingAirlineCodes if present; fallback to first segment carrierCode.
    """
    data = payload.get("data", []) or []
    best: Dict[str, float] = {}

    for offer in data:
        total = offer.get("price", {}).get("total")
        if total is None:
            continue
        try:
            price = float(total)
        except Exception:
            continue

        carriers: List[str] = []

        # Prefer validating airlines (most consistent)
        vac = offer.get("validatingAirlineCodes")
        if isinstance(vac, list) and vac:
            carriers = [str(c) for c in vac if c]
        else:
            # Fallback: first segment marketing carrier
            itins = offer.get("itineraries", []) or []
            if itins:
                segs = itins[0].get("segments", []) or []
                if segs:
                    cc = segs[0].get("carrierCode")
                    if cc:
                        carriers = [str(cc)]

        if not carriers:
            continue

        # offer may have more than one validating carrier; count each
        for c in carriers:
            if c not in best or price < best[c]:
                best[c] = price

    return best

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
        dep_step = int(route.get("departure_step_days", 3) or 3)

        return_latest = _parse_date(str(route["return_latest"]))
        return_offsets = route.get("return_offsets_days", [7, 14])
        if not isinstance(return_offsets, list) or not return_offsets:
            return_offsets = [7, 14]

        cabin = str(route.get("cabin", "ECONOMY")).upper()
        adults = int(route.get("adults", 1) or 1)
        children = int(route.get("children", 0) or 0)
        currency = str(route.get("currency", "BRL")).upper()

        best_price: Optional[float] = None
        best_dep: Optional[str] = None
        best_ret: Optional[str] = None

        for d in _daterange(dep_from, dep_to, step_days=dep_step):
            # monta candidatos de volta: d+offsets (capado no limite) + o próprio limite
            candidates: List[date] = []
            for off in return_offsets:
                try:
                    off_i = int(off)
                except Exception:
                    continue
                ret = _add_days(d, max(1, off_i))
                if ret <= return_latest:
                    candidates.append(ret)
            candidates.append(return_latest)

            # remove duplicadas e ordena
            candidates = sorted({c for c in candidates})

            for ret_d in candidates:
                # segurança: volta sempre depois da ida
                if ret_d <= d:
                    continue

                params = {
                    "originLocationCode": origin,
                    "destinationLocationCode": destination,
                    "departureDate": d.isoformat(),
                    "returnDate": ret_d.isoformat(),
                    "adults": adults,
                    "children": children,
                    "travelClass": cabin,
                    "currencyCode": currency,
                    "max": 20,
                }

                payload = client.flight_offers_search(params)
                p = _min_price_from_offers(payload)
                if p is not None and (best_price is None or p < best_price):
                    best_price = p
                    best_dep = d.isoformat()
                    best_ret = ret_d.isoformat()

                time.sleep(0.12)

        key = _stable_key(route)
        if best_price is None:
            results.append(
                {
                    "key": key,
                    "price": float("inf"),
                    "currency": currency,
                    "summary": f"{origin}→{destination} no offers found dep={dep_from}..{dep_to} return<= {return_latest}",
                    "deeplink": "",
                }
            )
        else:
            results.append(
                {
                    "key": key,
                    "price": float(best_price),
                    "currency": currency,
                    "best_dep": best_dep,
                    "best_ret": best_ret,
                    "origin": origin,
                    "destination": destination,
                    "cabin": cabin,
                    "adults": adults,
                    "children": children,
                    "summary": f"{origin}→{destination} best_dep={best_dep} best_ret={best_ret} cabin={cabin} A={adults} C={children}",
                    "deeplink": "",
                }
            )

    return results
