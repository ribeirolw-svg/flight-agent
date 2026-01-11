from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests


# ----------------------------
# Config
# ----------------------------
AMADEUS_ENV = (os.getenv("AMADEUS_ENV") or "test").strip().lower()
AMADEUS_BASE = "https://test.api.amadeus.com" if AMADEUS_ENV == "test" else "https://api.amadeus.com"

CLIENT_ID = os.getenv("AMADEUS_CLIENT_ID")
CLIENT_SECRET = os.getenv("AMADEUS_CLIENT_SECRET")
DEFAULT_CURRENCY = os.getenv("CURRENCY_CODE", "BRL")


def _require_secrets() -> None:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("AMADEUS_CLIENT_ID / AMADEUS_CLIENT_SECRET não configurados.")


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _parse_iso_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _daterange(start: date, end: date, step_days: int) -> List[date]:
    out = []
    d = start
    while d <= end:
        out.append(d)
        d += timedelta(days=step_days)
    return out


def _carrier_from_offer(offer: Dict[str, Any]) -> Optional[str]:
    codes = offer.get("validatingAirlineCodes")
    if isinstance(codes, list) and codes:
        return str(codes[0])
    try:
        return str(offer["itineraries"][0]["segments"][0]["carrierCode"])
    except Exception:
        return None


def _extract_prices(offer: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    (base, total)
    base: price.base (sem taxas)
    total: price.grandTotal (com taxas) ou price.total
    """
    price_obj = offer.get("price") or {}
    base = _to_float(price_obj.get("base"))
    total = _to_float(price_obj.get("grandTotal"))
    if total is None:
        total = _to_float(price_obj.get("total"))

    # fallbacks
    if base is None and total is not None:
        base = total
    if total is None and base is not None:
        total = base
    return base, total


def _min_update(d: Dict[str, float], k: str, v: float) -> None:
    cur = d.get(k)
    if cur is None or v < cur:
        d[k] = v


# ----------------------------
# Amadeus Client
# ----------------------------
@dataclass
class AmadeusClient:
    access_token: Optional[str] = None
    token_expiry_ts: float = 0.0

    def _token(self) -> str:
        _require_secrets()

        now = time.time()
        if self.access_token and now < self.token_expiry_ts - 30:
            return self.access_token

        url = f"{AMADEUS_BASE}/v1/security/oauth2/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
        r = requests.post(url, data=data, timeout=30)
        r.raise_for_status()
        payload = r.json()
        self.access_token = payload["access_token"]
        self.token_expiry_ts = now + int(payload.get("expires_in", 1800))
        return self.access_token

    def flight_offers_search(self, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{AMADEUS_BASE}/v2/shopping/flight-offers"
        headers = {"Authorization": f"Bearer {self._token()}"}
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        return r.json()


# ----------------------------
# Profile normalization
# ----------------------------
def _normalize_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(profile or {})

    p.setdefault("origin", "GRU")
    p.setdefault("destinations", ["FCO", "CIA"])
    p.setdefault("travelClass", "ECONOMY")
    p.setdefault("currencyCode", DEFAULT_CURRENCY)
    p.setdefault("adults", 2)
    p.setdefault("children", 1)

    # aliases
    if "currency" in p and "currencyCode" not in p:
        p["currencyCode"] = p["currency"]
    if "class" in p and "travelClass" not in p:
        p["travelClass"] = p["class"]

    # ✅ PONTO 1: children (2–11) sempre; nunca enviar infants
    p.pop("infants", None)
    p.pop("infant", None)
    p.pop("child_age", None)
    p.pop("children_ages", None)

    p["adults"] = int(p.get("adults") or 0)
    p["children"] = int(p.get("children") or 0)
    if p["adults"] <= 0:
        raise ValueError("Profile inválido: adults deve ser >= 1.")
    if p["children"] < 0:
        raise ValueError("Profile inválido: children não pode ser negativo.")

    dep_start = p.get("dep_start") or p.get("departure_from") or p.get("departure_start")
    dep_end = p.get("dep_end") or p.get("departure_to") or p.get("departure_end")
    return_by = p.get("return_by") or p.get("return_limit")

    if not dep_start or not dep_end or not return_by:
        raise ValueError("Profile inválido: informe dep_start/dep_end/return_by (YYYY-MM-DD).")

    p["_dep_start"] = _parse_iso_date(str(dep_start))
    p["_dep_end"] = _parse_iso_date(str(dep_end))
    p["_return_by"] = _parse_iso_date(str(return_by))

    p["dep_step_days"] = int(p.get("dep_step_days") or p.get("depStep") or 7)
    p["ret_offset_days"] = int(p.get("ret_offset_days") or p.get("retOff") or 10)

    # ranking: total por padrão
    p["rank_by"] = str(p.get("rank_by") or "total").lower()  # total|base

    return p


# ----------------------------
# ✅ EXPORT PRINCIPAL (scheduler importa isso)
# ----------------------------
def run_search(profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Retorna lista de dicts compatível com scheduler.py:
      key, origin, destination, currency,
      price (compat), price_base, price_total,
      best_dep, best_ret, by_carrier, summary
    """
    p = _normalize_profile(profile)
    client = AmadeusClient()

    origin = str(p["origin"]).upper()
    destinations = [str(x).upper() for x in (p.get("destinations") or ["FCO", "CIA"])]

    dep_dates = _daterange(p["_dep_start"], p["_dep_end"], p["dep_step_days"])
    return_by: date = p["_return_by"]
    ret_offset = p["ret_offset_days"]

    adults = int(p["adults"])
    children = int(p["children"])
    travel_class = str(p["travelClass"]).upper()
    currency = str(p["currencyCode"]).upper()
    rank_by = p["rank_by"]

    results: List[Dict[str, Any]] = []

    for dest in destinations:
        offers_found = False

        best_rank: Optional[float] = None
        best_base: Optional[float] = None
        best_total: Optional[float] = None
        best_dep: Optional[str] = None
        best_ret: Optional[str] = None
        by_carrier: Dict[str, float] = {}

        for dep in dep_dates:
            ret = dep + timedelta(days=ret_offset)
            if ret > return_by:
                ret = return_by

            params: Dict[str, Any] = {
                "originLocationCode": origin,
                "destinationLocationCode": dest,
                "departureDate": dep.isoformat(),
                "returnDate": ret.isoformat(),
                "adults": adults,
                "children": children,  # ✅ criança 2–11
                "travelClass": travel_class,
                "currencyCode": currency,
                "max": 20,
            }

            data = client.flight_offers_search(params)
            offers = data.get("data") or []
            if not offers:
                continue

            offers_found = True

            for offer in offers:
                base, total = _extract_prices(offer)
                rank = total if rank_by == "total" else base
                if rank is None:
                    rank = total or base
                if rank is None:
                    continue

                carrier = _carrier_from_offer(offer) or "??"
                if total is not None:
                    _min_update(by_carrier, carrier, total)
                else:
                    _min_update(by_carrier, carrier, rank)

                if best_rank is None or rank < best_rank:
                    best_rank = rank
                    best_base = base
                    best_total = total
                    best_dep = dep.isoformat()
                    best_ret = ret.isoformat()

        key = (
            f"{origin}-{dest}"
            f"|dep={p['_dep_start'].isoformat()}..{p['_dep_end'].isoformat()}"
            f"|ret<={return_by.isoformat()}"
            f"|class={travel_class}"
            f"|A{adults}|C{children}|{currency}"
            f"|depStep={p['dep_step_days']}|retOff={ret_offset}"
            f"|rank={rank_by}"
        )

        if not offers_found:
            results.append(
                {
                    "key": key,
                    "origin": origin,
                    "destination": dest,
                    "currency": currency,
                    "price": None,
                    "price_base": None,
                    "price_total": None,
                    "best_dep": None,
                    "best_ret": None,
                    "by_carrier": {},
                    "summary": (
                        f"{origin}→{dest} no offers found dep={p['_dep_start'].isoformat()}..{p['_dep_end'].isoformat()} "
                        f"return<= {return_by.isoformat()}"
                    ),
                }
            )
        else:
            price_compat = best_total if rank_by == "total" else best_base
            if price_compat is None:
                price_compat = best_rank

            results.append(
                {
                    "key": key,
                    "origin": origin,
                    "destination": dest,
                    "currency": currency,
                    "price": price_compat,
                    "price_base": best_base,
                    "price_total": best_total,
                    "best_dep": best_dep,
                    "best_ret": best_ret,
                    "by_carrier": by_carrier,
                    "summary": (
                        f"{origin}→{dest} best_dep={best_dep} best_ret={best_ret} "
                        f"cabin={travel_class} A={adults} C={children} rank_by={rank_by} "
                        f"base={best_base} total={best_total}"
                    ),
                }
            )

    return results
