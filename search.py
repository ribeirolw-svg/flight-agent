from __future__ import annotations

import os
import time
import requests
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple


# -----------------------------
# Env / Config
# -----------------------------
def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def _as_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except Exception:
        return default


def _as_float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except Exception:
        return default


AMADEUS_ENV = _env("AMADEUS_ENV", "test").lower()
IS_PROD = AMADEUS_ENV in {"prod", "production", "live"}

BASE_URL = "https://api.amadeus.com" if IS_PROD else "https://test.api.amadeus.com"
TOKEN_URL = f"{BASE_URL}/v1/security/oauth2/token"
FLIGHT_OFFERS_URL = f"{BASE_URL}/v2/shopping/flight-offers"

CLIENT_ID = _env("AMADEUS_CLIENT_ID", "")
CLIENT_SECRET = _env("AMADEUS_CLIENT_SECRET", "")

MAX_RETRIES = _as_int("AMADEUS_MAX_RETRIES", 5)
BACKOFF_BASE = _as_float("AMADEUS_BACKOFF_BASE_SECONDS", 1.2)
THROTTLE_SECONDS = _as_float("AMADEUS_THROTTLE_SECONDS", 0.35)
TIMEOUT_SECONDS = _as_float("AMADEUS_TIMEOUT_SECONDS", 30.0)


# -----------------------------
# Token cache
# -----------------------------
_token_cache: Dict[str, Any] = {
    "access_token": None,
    "expires_at_epoch": 0.0,  # epoch seconds
}


def _now_epoch() -> float:
    return time.time()


def _token_valid() -> bool:
    tok = _token_cache.get("access_token")
    exp = float(_token_cache.get("expires_at_epoch") or 0.0)
    # margem de segurança (30s)
    return bool(tok) and (_now_epoch() < (exp - 30.0))


def _amadeus_get_token() -> str:
    if _token_valid():
        return str(_token_cache["access_token"])

    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError(
            "Credenciais Amadeus ausentes. Defina AMADEUS_CLIENT_ID e AMADEUS_CLIENT_SECRET como secrets/env vars."
        )

    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=TIMEOUT_SECONDS,
    )

    if resp.status_code >= 400:
        raise RuntimeError(f"Erro ao obter token ({resp.status_code}): {resp.text}")

    payload = resp.json()
    access_token = payload.get("access_token")
    expires_in = payload.get("expires_in", 0)

    if not access_token:
        raise RuntimeError(f"Token inválido/ausente na resposta: {payload}")

    _token_cache["access_token"] = access_token
    _token_cache["expires_at_epoch"] = _now_epoch() + float(expires_in or 0)

    return str(access_token)


# -----------------------------
# Retry helpers
# -----------------------------
def _is_rate_limit(resp: Optional[requests.Response], err: Optional[Exception]) -> bool:
    if resp is not None and resp.status_code == 429:
        return True
    if err is not None:
        s = str(err).lower()
        return ("429" in s) or ("too many requests" in s) or ("rate limit" in s)
    return False


def _is_transient(resp: Optional[requests.Response], err: Optional[Exception]) -> bool:
    if resp is not None and resp.status_code in {429, 500, 502, 503, 504}:
        return True
    if err is not None:
        s = str(err).lower()
        return any(x in s for x in ["timeout", "timed out", "connection", "temporarily", "reset", "429"])
    return False


def _sleep_backoff(attempt: int) -> None:
    # backoff exponencial leve: base^(attempt+1)
    wait = BACKOFF_BASE * (BACKOFF_BASE ** attempt)
    time.sleep(wait)


# -----------------------------
# Public API
# -----------------------------
def search_flights(
    *,
    origin: str,
    destination: str,
    departure_date: str,
    return_date: Optional[str] = None,
    adults: int = 1,
    children: int = 0,
    travel_class: str = "ECONOMY",
    currency: str = "BRL",
    nonstop: bool = True,
    max_results: int = 5,
) -> Dict[str, Any]:
    """
    Wrapper do Amadeus Flight Offers Search (v2).
    Observação: este endpoint aceita quantidade de children, mas não aceita idade da criança.
    """

    # Debug leve (sem vazar secrets)
    print("=======================================")
    print("AMAD_RUN_UTC:", datetime.now(timezone.utc).isoformat())
    print("AMAD_ENV:", AMADEUS_ENV)
    print("AMAD_BASE_URL:", BASE_URL)
    print("AMAD_CLIENT_ID_PREFIX:", (CLIENT_ID[:6] if CLIENT_ID else "EMPTY"))
    print("=======================================")

    params: Dict[str, Any] = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": departure_date,
        "adults": int(adults),
        "currencyCode": currency,
        "nonStop": "true" if nonstop else "false",
        "max": int(max_results),
        "travelClass": travel_class,
    }

    if children and int(children) > 0:
        params["children"] = int(children)

    if return_date:
        params["returnDate"] = return_date

    last_err: Optional[Exception] = None
    last_resp: Optional[requests.Response] = None

    for attempt in range(MAX_RETRIES + 1):
        if THROTTLE_SECONDS > 0:
            time.sleep(THROTTLE_SECONDS)

        try:
            token = _amadeus_get_token()
            headers = {"Authorization": f"Bearer {token}"}

            resp = requests.get(
                FLIGHT_OFFERS_URL,
                headers=headers,
                params=params,
                timeout=TIMEOUT_SECONDS,
            )
            last_resp = resp

            if resp.status_code < 400:
                return resp.json()

            # 401/403 pode ser token ruim -> limpa cache e tenta de novo 1x
            if resp.status_code in {401, 403}:
                _token_cache["access_token"] = None
                _token_cache["expires_at_epoch"] = 0.0

            # Se é transitório, tenta de novo com backoff
            if attempt < MAX_RETRIES and _is_transient(resp, None):
                print(f"[WARN] Amadeus HTTP {resp.status_code} (attempt {attempt+1}/{MAX_RETRIES}) -> retry/backoff")
                _sleep_backoff(attempt)
                continue

            raise RuntimeError(f"Erro ao buscar ofertas ({resp.status_code}): {resp.text}")

        except Exception as e:
            last_err = e

            if attempt < MAX_RETRIES and _is_transient(None, e):
                print(f"[WARN] Amadeus exception (attempt {attempt+1}/{MAX_RETRIES}) -> retry/backoff | {e}")
                _sleep_backoff(attempt)
                continue

            # sem retry
            raise

    # fallback (não deve acontecer)
    if last_resp is not None:
        raise RuntimeError(f"Erro ao buscar ofertas ({last_resp.status_code}): {last_resp.text}")
    if last_err is not None:
        raise last_err
    raise RuntimeError("Erro desconhecido ao buscar ofertas")
