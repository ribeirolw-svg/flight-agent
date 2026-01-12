#!/usr/bin/env python3
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import requests

# Base URLs do Amadeus
BASE_TEST = "https://test.api.amadeus.com"
BASE_PROD = "https://api.amadeus.com"

TOKEN_PATH = "/v1/security/oauth2/token"
OFFERS_PATH = "/v2/shopping/flight-offers"

DEFAULT_TIMEOUT = 30


class AmadeusError(RuntimeError):
    pass


def _base_url(env: str) -> str:
    env = (env or "").strip().lower()
    if env in {"prod", "production", "live"}:
        return BASE_PROD
    return BASE_TEST


def _get_env_required(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        raise AmadeusError(f"Missing required env var: {name}")
    return val


def _get_token(client_id: str, client_secret: str, base_url: str) -> str:
    url = f"{base_url}{TOKEN_PATH}"
    resp = requests.post(
        url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=DEFAULT_TIMEOUT,
    )

    if resp.status_code != 200:
        # tenta extrair json de erro
        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text[:500]}
        raise AmadeusError(f"Token error HTTP {resp.status_code}: {payload}")

    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise AmadeusError(f"Token response missing access_token: {data}")
    return token


def _build_params(route: Dict[str, Any], max_results: int) -> Dict[str, Any]:
    """
    Constrói query params do endpoint flight-offers.
    Mantém robusto: só inclui o que existir.
    """
    origin = route.get("origin")
    destination = route.get("destination")
    departure_date = route.get("departure_date")
    return_date = route.get("return_date")

    if not origin or not destination or not departure_date:
        raise AmadeusError("Route missing required fields: origin, destination, departure_date")

    params: Dict[str, Any] = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": departure_date,
        "adults": int(route.get("adults") or 1),
        "max": int(max_results or 10),
    }

    # opcionais
    if return_date:
        params["returnDate"] = return_date

    children = route.get("children")
    if children is not None and str(children).strip() != "":
        params["children"] = int(children)

    cabin = route.get("cabin")
    if cabin:
        # Amadeus: ECONOMY, PREMIUM_ECONOMY, BUSINESS, FIRST
        params["travelClass"] = cabin

    currency = route.get("currency")
    if currency:
        params["currencyCode"] = currency

    direct_only = route.get("direct_only")
    if direct_only is True:
        params["nonStop"] = "true"
    elif direct_only is False:
        params["nonStop"] = "false"

    return params


def _request_offers(token: str, base_url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    url = f"{base_url}{OFFERS_PATH}"
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.get(url, headers=headers, params=params, timeout=DEFAULT_TIMEOUT)

    if resp.status_code != 200:
        # tenta extrair json de erro do Amadeus
        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text[:800]}

        # Alguns erros comuns da sandbox: 429, 400 (params), 401 (token)
        raise AmadeusError(f"Offers error HTTP {resp.status_code}: {payload}")

    data = resp.json()
    offers = data.get("data", [])
    if not isinstance(offers, list):
        raise AmadeusError(f"Unexpected offers payload shape: {type(offers)}")

    return offers


def search_offers_for_route(route: Dict[str, Any], *, max_results: int, env: str) -> List[Dict[str, Any]]:
    """
    Função que o scheduler espera.
    Retorna lista de flight offers (dicts) conforme payload do Amadeus.

    Erros de rede/auth/params levantam exceção (para o scheduler contabilizar err_calls).
    Se não houver oferta, retorna [] (OK, não é erro).
    """
    base_url = _base_url(env or "test")

    client_id = _get_env_required("AMADEUS_CLIENT_ID")
    client_secret = _get_env_required("AMADEUS_CLIENT_SECRET")

    params = _build_params(route, max_results=max_results)

    # token por chamada (simples e robusto). Na Etapa 1 a gente faz cache com expiração.
    token = _get_token(client_id, client_secret, base_url)

    offers = _request_offers(token, base_url, params)

    # Sem ofertas é OK
    return offers
