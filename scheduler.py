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

# Remendo 429
AMADEUS_MAX_RETRIES = int(os.getenv("AMADEUS_MAX_RETRIES", "5"))
AMADEUS_BACKOFF_BASE_SECONDS = float(os.getenv("AMADEUS_BACKOFF_BASE_SECONDS", "1.2"))
AMADEUS_THROTTLE_SECONDS = float(os.getenv("AMADEUS_THROTTLE_SECONDS", "0.2"))


# ----------------------------
# Utils
# ----------------------------
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


def _min_update(d: Dict[str, float], k: str, v: float) -> None:
    cur = d.get(k)
    if cur is None or v < cur:
        d[k] = v


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
      base  = sem taxas (price.base)
      total = com taxas (price.grandTotal) ou fallback (price.total)
    """
    price_obj = offer.get("price") or {}

    base = _to_float(price_obj.get("base"))
    total = _to_float(price_obj.get("grandTotal"))
    if total is None:
        total = _to_float(price_obj.get("total"))

    if base is None and total is not None:
        base = total
    if total is None and base is not None:
        total = base

    return base, total


def _pick_first(p: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    for k in keys:
        v = p.get(k)
        if v is not None and str(v).strip() != "":
            return v
    return None


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
        """
        Remendo 429:
          - retry/backoff para 429 e 5xx
          - respeita Retry-After quando existe
          - se estourar retries, retorna {"data": []} (não mata o scheduler)
        """
        url = f"{AMADEUS_BASE}/v2/shopping/flight-offers"
        headers = {"Authorization": f"Bearer {self._token()}"}

        last_err: Optional[Exception] = None

        for attempt in range(1, AMADEUS_MAX_RETRIES + 1):
            if AMADEUS_THROTTLE_SECONDS > 0:
                time.sleep(AMADEUS_THROTTLE_SECONDS)

            try:
                r = requests.get(url, headers=headers, params=params, timeout=60)

                if 200 <= r.status_code < 300:
                    return r.json()

                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait = float(retry_after)
                        except Exception:
                            wait = AMADEUS_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                    else:
                        wait = AMADEUS_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                    time.sleep(wait)
                    last_err = requests.HTTPError(f"429 Too Many Requests (attempt {attempt})", response=r)
                    continue

                if 500 <= r.status_code < 600:
                    wait = AMADEUS_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                    time.sleep(wait)
                    last_err = requests.HTTPError(f"{r.status_code} Server error (attempt {attempt})", response=r)
                    continue

                try:
                    r.raise_for_status()
                except Exception as e:
                    last_err = e
                break

            except Exception as e:
                last_err = e
                wait = AMADEUS_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                time.sleep(wait)
                continue

        return {"data": [], "_error": str(last_err) if last_err else "unknown_error"}


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

    if "currency" in p and "currencyCode" not in p:
        p["currencyCode"] = p["currency"]
    if "class" in p and "travelClass" not in p:
        p["travelClass"] = p["class"]

    # criança 2–11: children; nunca infants
    for k in ("infants", "infant", "child_age", "children_ages"):
        p.pop(k, None)

    p["adults"] = int(p.get("adults") or 0)
    p["children"] = int(p.get("children") or 0)
    if p["adults"] <= 0:
        raise ValueError("Profile inválido: adults deve ser >= 1.")
    if p["children"] < 0:
        raise ValueError("Profile inválido: children não pode ser negativo.")

    dep_start = _pick_first(
        p,
        ["dep_start", "depart_start", "departure_from", "departure_start", "departure_window_start", "departureWindowStart"],
    )
    dep_end = _pick_first(
        p,
        ["dep_end", "depart_end", "departure_to", "departure_end", "departure_window_end", "departureWindowEnd"],
    )
    return_by = _pick_first(
        p,
        ["return_by", "return_limit", "return_latest", "return_max", "returnDateLimit", "return_by_limit", "returnUntil", "return_until"],
    )

    if not dep_start or not dep_end or not return_by:
        raise ValueError("Profile inválido: informe dep_start/dep_end/return_by (YYYY-MM-DD).")

    p["_dep_start"] = _parse_iso_date(str(dep_start))
    p["_dep_end"] = _parse_iso_date(str(dep_end))
    p["_return_by"] = _parse_iso_date(str(return_by))

    p["dep_step_days"] = int(_pick_first(p, ["dep_step_days", "depStep"]) or 10)
    p["ret_offset_days"] = int(_pick_first(p, ["ret_offset_days", "retOff"]) or 10)

    rank_by = str(_pick_first(p, ["rank_by", "rankBy", "rank"]) or "total").lower()
    p["rank_by"] = "base" if rank_by == "base" else "total"

    return p


# ----------------------------
# Export principal
# ----------------------------
def run_search(profile: Dict[str, Any]) -> List[Dict[str, Any]]:
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
                "children": children,
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
