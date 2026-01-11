from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests


# ----------------------------
# Config / Utils
# ----------------------------
AMADEUS_ENV = (os.getenv("AMADEUS_ENV") or "test").strip().lower()
AMADEUS_BASE = "https://test.api.amadeus.com" if AMADEUS_ENV == "test" else "https://api.amadeus.com"

CLIENT_ID = os.getenv("AMADEUS_CLIENT_ID")
CLIENT_SECRET = os.getenv("AMADEUS_CLIENT_SECRET")

DEFAULT_CURRENCY = os.getenv("CURRENCY_CODE", "BRL")


def _require_secrets() -> None:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError(
            "AMADEUS_CLIENT_ID / AMADEUS_CLIENT_SECRET não configurados (env vars). "
            "Configure os secrets no GitHub Actions."
        )


def _parse_iso_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _daterange(start: date, end: date, step_days: int) -> List[date]:
    out = []
    d = start
    while d <= end:
        out.append(d)
        d += timedelta(days=step_days)
    return out


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _min_update(d: Dict[str, float], key: str, val: float) -> None:
    cur = d.get(key)
    if cur is None or val < cur:
        d[key] = val


def _carrier_from_offer(offer: Dict[str, Any]) -> Optional[str]:
    # Amadeus normalmente retorna validatingAirlineCodes
    codes = offer.get("validatingAirlineCodes")
    if isinstance(codes, list) and codes:
        return str(codes[0])
    # fallback: tenta pegar do primeiro segmento
    try:
        seg = offer["itineraries"][0]["segments"][0]
        return str(seg["carrierCode"])
    except Exception:
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
        expires_in = int(payload.get("expires_in", 1800))
        self.token_expiry_ts = now + expires_in
        return self.access_token

    def flight_offers_search(self, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{AMADEUS_BASE}/v2/shopping/flight-offers"
        headers = {"Authorization": f"Bearer {self._token()}"}
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        return r.json()


# ----------------------------
# Core search
# ----------------------------
def _normalize_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza o profile para garantir:
      - children (2–11) sempre usado
      - infants nunca enviado (mesmo se existir no profile)
    Observação: este endpoint GET não aceita idades; só quantidades.
    """
    p = dict(profile or {})

    # valores padrão
    p.setdefault("origin", "GRU")
    p.setdefault("destinations", ["FCO", "CIA"])
    p.setdefault("travelClass", "ECONOMY")
    p.setdefault("currencyCode", DEFAULT_CURRENCY)
    p.setdefault("adults", 2)
    p.setdefault("children", 1)

    # aliases comuns
    if "currency" in p and "currencyCode" not in p:
        p["currencyCode"] = p["currency"]
    if "class" in p and "travelClass" not in p:
        p["travelClass"] = p["class"]

    # 🔥 REMENDO DO PONTO 1:
    # Força children e ignora qualquer coisa relacionada a infants/idade.
    # Se alguém colocou infants por engano, simplesmente não enviamos.
    p.pop("infants", None)
    p.pop("infant", None)
    p.pop("child_age", None)
    p.pop("children_ages", None)

    # garante int
    p["adults"] = int(p.get("adults", 0) or 0)
    p["children"] = int(p.get("children", 0) or 0)

    # limites mínimos
    if p["adults"] <= 0:
        raise ValueError("Profile inválido: adults deve ser >= 1.")
    if p["children"] < 0:
        raise ValueError("Profile inválido: children não pode ser negativo.")

    # datas obrigatórias
    # Aceita tanto:
    #  - dep_start/dep_end/return_by
    #  - departure_from/departure_to/return_by
    dep_start = p.get("dep_start") or p.get("departure_from") or p.get("departure_start")
    dep_end = p.get("dep_end") or p.get("departure_to") or p.get("departure_end")
    return_by = p.get("return_by") or p.get("return_limit")

    if not dep_start or not dep_end or not return_by:
        raise ValueError(
            "Profile inválido: informe dep_start/dep_end/return_by (YYYY-MM-DD). "
            "Ex: dep_start=2026-09-01 dep_end=2026-10-05 return_by=2026-10-05"
        )

    p["_dep_start"] = _parse_iso_date(str(dep_start))
    p["_dep_end"] = _parse_iso_date(str(dep_end))
    p["_return_by"] = _parse_iso_date(str(return_by))

    # steps
    p["dep_step_days"] = int(p.get("dep_step_days") or p.get("depStep") or 7)
    p["ret_offset_days"] = int(p.get("ret_offset_days") or p.get("retOff") or 10)

    return p


def run_search(profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Retorna lista de resultados por destino (FCO/CIA) contendo:
      key, origin, destination, currency, price, best_dep, best_ret, by_carrier, summary
    """
    p = _normalize_profile(profile)
    client = AmadeusClient()

    origin = str(p["origin"]).upper()
    destinations = p.get("destinations") or ["FCO", "CIA"]
    destinations = [str(x).upper() for x in destinations]

    dep_dates = _daterange(p["_dep_start"], p["_dep_end"], p["dep_step_days"])
    return_by: date = p["_return_by"]
    ret_offset_days = p["ret_offset_days"]

    adults = int(p["adults"])
    children = int(p["children"])
    travel_class = str(p["travelClass"]).upper()
    currency = str(p["currencyCode"]).upper()

    results: List[Dict[str, Any]] = []

    for dest in destinations:
        # Agregadores por destino (janela inteira)
        best_price: Optional[float] = None
        best_dep: Optional[str] = None
        best_ret: Optional[str] = None
        by_carrier: Dict[str, float] = {}
        offers_found = False

        for dep in dep_dates:
            # volta: dep + offset, mas nunca passando do return_by
            ret = dep + timedelta(days=ret_offset_days)
            if ret > return_by:
                ret = return_by

            params: Dict[str, Any] = {
                "originLocationCode": origin,
                "destinationLocationCode": dest,
                "departureDate": dep.isoformat(),
                "returnDate": ret.isoformat(),
                "adults": adults,
                # ✅ CRIANÇA 2–11: usar children (e NÃO infants)
                "children": children,
                "travelClass": travel_class,
                "currencyCode": currency,
                "max": 20,
            }

            # ✅ NÃO ENVIAR infants em hipótese alguma (mesmo que children=0)
            # params.pop("infants", None)  # nem existe aqui, só garantindo ideia

            data = client.flight_offers_search(params)
            offers = data.get("data") or []
            if not offers:
                continue

            offers_found = True

            for offer in offers:
                # Preço: aqui usamos "base" (sem taxas) como você falou que tudo bem.
                # Se quiser trocar pra com taxas, use grandTotal/total.
                price_obj = offer.get("price") or {}
                price_base = _to_float(price_obj.get("base"))  # sem taxas
                if price_base is None:
                    # fallback
                    price_base = _to_float(price_obj.get("total")) or _to_float(price_obj.get("grandTotal"))

                if price_base is None:
                    continue

                carrier = _carrier_from_offer(offer) or "??"
                _min_update(by_carrier, carrier, price_base)

                if best_price is None or price_base < best_price:
                    best_price = price_base
                    best_dep = dep.isoformat()
                    best_ret = ret.isoformat()

        key = (
            f"{origin}-{dest}"
            f"|dep={p['_dep_start'].isoformat()}..{p['_dep_end'].isoformat()}"
            f"|ret<={return_by.isoformat()}"
            f"|class={travel_class}"
            f"|A{adults}|C{children}|{currency}"
            f"|depStep={p['dep_step_days']}|retOff={ret_offset_days}"
        )

        if not offers_found:
            results.append(
                {
                    "key": key,
                    "origin": origin,
                    "destination": dest,
                    "currency": currency,
                    "price": None,
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
            results.append(
                {
                    "key": key,
                    "origin": origin,
                    "destination": dest,
                    "currency": currency,
                    "price": best_price,
                    "best_dep": best_dep,
                    "best_ret": best_ret,
                    "by_carrier": by_carrier,
                    "summary": (
                        f"{origin}→{dest} best_dep={best_dep} best_ret={best_ret} "
                        f"cabin={travel_class} A={adults} C={children}"
                    ),
                }
            )

    return results
