# scheduler.py
from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    import yaml  # type: ignore
except Exception as e:
    raise RuntimeError("PyYAML é necessário para ler routes.yaml. Instale com `pip install pyyaml`.") from e

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # pragma: no cover


# =============================
# Paths
# =============================
REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

ROUTES_FILE = REPO_ROOT / "routes.yaml"

STATE_FILE = DATA_DIR / "state.json"
SUMMARY_FILE = DATA_DIR / "summary.md"
HISTORY_FILE = DATA_DIR / "history.jsonl"
BEST_FILE = DATA_DIR / "best_offers.json"
ALERTS_FILE = DATA_DIR / "alerts.json"
DEBUG_FILE = DATA_DIR / "debug_last_run.json"

TZ_NAME = "America/Sao_Paulo"
DEFAULT_MAX_RESULTS = int(os.getenv("MAX_RESULTS", "10"))

AMADEUS_TEST_BASE = "https://test.api.amadeus.com"
AMADEUS_PROD_BASE = "https://api.amadeus.com"


# =============================
# Helpers
# =============================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def run_id_utc() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ")


def local_today() -> date:
    if ZoneInfo is None:
        return datetime.utcnow().date()
    return datetime.now(ZoneInfo(TZ_NAME)).date()


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str):
            s = x.strip().replace(" ", "")
            if s.count(",") == 1 and s.count(".") == 0:
                s = s.replace(",", ".")
            elif s.count(",") >= 1 and s.count(".") >= 1:
                s = s.replace(",", "")
            x = s
        return float(x)
    except Exception:
        return None


def load_config() -> Dict[str, Any]:
    if not ROUTES_FILE.exists():
        raise FileNotFoundError(f"routes.yaml não encontrado em: {ROUTES_FILE}")
    return yaml.safe_load(ROUTES_FILE.read_text(encoding="utf-8")) or {}


def route_key(route: Dict[str, Any]) -> str:
    rid = route.get("id")
    return str(rid) if rid else f'{route.get("origin","")}-{route.get("destination","")}'


# =============================
# Immutable validation
# =============================
IMMUTABLE_REQUIRED_ORIGINS = ["CGH", "GRU"]
IMMUTABLE_REQUIRED_DESTS = ["CWB", "FCO", "NVT"]


def validate_immutable_rules(routes_expanded: List[Dict[str, Any]]) -> None:
    origins = sorted({r.get("origin") for r in routes_expanded if r.get("origin")})
    dests = sorted({r.get("destination") for r in routes_expanded if r.get("destination")})

    missing_o = [o for o in IMMUTABLE_REQUIRED_ORIGINS if o not in origins]
    missing_d = [d for d in IMMUTABLE_REQUIRED_DESTS if d not in dests]

    if missing_o or missing_d:
        print("[FATAL] routes.yaml violou regras imutáveis.")
        if missing_o:
            print(f"[FATAL] Origens faltando: {missing_o}")
        if missing_d:
            print(f"[FATAL] Destinos faltando: {missing_d}")
        raise SystemExit(1)


# =============================
# Date rules
# =============================
def generate_rome_pairs(
    year: int,
    start_mm_dd: Tuple[int, int],
    latest_return_mm_dd: Tuple[int, int],
    trip_days: int,
) -> List[Tuple[date, date]]:
    start = date(year, start_mm_dd[0], start_mm_dd[1])
    latest_return = date(year, latest_return_mm_dd[0], latest_return_mm_dd[1])
    latest_depart = latest_return - timedelta(days=trip_days)

    pairs: List[Tuple[date, date]] = []
    d = start
    while d <= latest_depart:
        r = d + timedelta(days=trip_days)
        if r <= latest_return:
            pairs.append((d, r))
        d += timedelta(days=1)
    return pairs


def generate_weekend_pairs(
    base: date,
    horizon_days: int,
    depart_dows: Tuple[int, int],
    return_dows: Tuple[int, int],
    max_trip_len_days: int,
) -> List[Tuple[date, date]]:
    end = base + timedelta(days=horizon_days)
    pairs = set()
    d = base
    while d <= end:
        if d.weekday() in depart_dows:
            for k in range(1, max_trip_len_days + 1):
                r = d + timedelta(days=k)
                if r <= end and r.weekday() in return_dows:
                    pairs.add((d, r))
        d += timedelta(days=1)
    return sorted(pairs)


def cap_pairs(pairs: List[Tuple[date, date]], max_pairs: Optional[int]) -> List[Tuple[date, date]]:
    if not max_pairs or max_pairs <= 0:
        return pairs
    # pega os mais “próximos” do começo da janela (boa estratégia pra achar promo cedo)
    return pairs[:max_pairs]


def expand_routes(routes_base: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    today = local_today()
    expanded: List[Dict[str, Any]] = []
    expanded_ranges: Dict[str, Any] = {}

    for r in routes_base:
        rid = route_key(r)
        rule = (r.get("rule") or "").strip().upper()
        params = r.get("rule_params") or {}

        if rule == "ROME_15D_WINDOW":
            trip_days = int(params.get("trip_days", 15))
            start_mm_dd = tuple(params.get("start_mm_dd", [9, 1]))
            latest_return_mm_dd = tuple(params.get("latest_return_mm_dd", [10, 5]))
            max_pairs = params.get("max_pairs")

            pairs = generate_rome_pairs(
                year=today.year,
                start_mm_dd=start_mm_dd,                  # type: ignore
                latest_return_mm_dd=latest_return_mm_dd,  # type: ignore
                trip_days=trip_days,
            )
            if not pairs:
                pairs = generate_rome_pairs(
                    year=today.year + 1,
                    start_mm_dd=start_mm_dd,                  # type: ignore
                    latest_return_mm_dd=latest_return_mm_dd,  # type: ignore
                    trip_days=trip_days,
                )

            pairs = cap_pairs(pairs, int(max_pairs) if max_pairs is not None else None)
            if pairs:
                expanded_ranges[rid] = {"min_dep": pairs[0][0].isoformat(), "max_dep": pairs[-1][0].isoformat(), "count": len(pairs)}

            for dep, ret in pairs:
                rr = dict(r)
                rr["departure_date"] = dep.isoformat()
                rr["return_date"] = ret.isoformat()
                expanded.append(rr)

        elif rule in ("WEEKEND_WINDOW", "WEEKEND_30D"):
            start_offset_days = int(params.get("start_offset_days", 0))
            horizon_days = int(params.get("horizon_days", 30))
            depart_dows = tuple(params.get("depart_dows", [4, 5]))
            return_dows = tuple(params.get("return_dows", [6, 0]))
            max_trip_len_days = int(params.get("max_trip_len_days", 4))
            max_pairs = params.get("max_pairs")

            # ✅ PONTO CRÍTICO: base = hoje + offset
            base = today + timedelta(days=start_offset_days)

            pairs = generate_weekend_pairs(
                base=base,
                horizon_days=horizon_days,
                depart_dows=depart_dows,  # type: ignore
                return_dows=return_dows,  # type: ignore
                max_trip_len_days=max_trip_len_days,
            )
            pairs = cap_pairs(pairs, int(max_pairs) if max_pairs is not None else None)

            if pairs:
                expanded_ranges[rid] = {"min_dep": pairs[0][0].isoformat(), "max_dep": pairs[-1][0].isoformat(), "count": len(pairs), "base": base.isoformat()}

            for dep, ret in pairs:
                rr = dict(r)
                rr["departure_date"] = dep.isoformat()
                rr["return_date"] = ret.isoformat()
                expanded.append(rr)

        else:
            # rota fixa (se alguém quiser)
            if r.get("departure_date") and r.get("return_date"):
                expanded.append(r)

    # logs
    for rid, info in expanded_ranges.items():
        print(f"[INFO] Expanded range for {rid}: {info}")

    return expanded, expanded_ranges


# =============================
# Amadeus with retry/backoff
# =============================
def amadeus_base(env: str) -> str:
    return AMADEUS_TEST_BASE if env.lower() == "test" else AMADEUS_PROD_BASE


def amadeus_get_token(client_id: str, client_secret: str, env: str) -> str:
    base = amadeus_base(env)
    url = f"{base}/v1/security/oauth2/token"
    resp = requests.post(
        url,
        data={"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def request_with_retry(method: str, url: str, *, headers: Dict[str, str], params: Dict[str, Any], retries: int = 4) -> requests.Response:
    delay = 1.0
    last_resp: Optional[requests.Response] = None

    for attempt in range(1, retries + 1):
        resp = requests.request(method, url, headers=headers, params=params, timeout=45)
        last_resp = resp

        # sucesso
        if resp.status_code < 400:
            return resp

        # retry only for 429 / 5xx
        if resp.status_code == 429 or 500 <= resp.status_code <= 599:
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = max(delay, float(retry_after))
                except Exception:
                    pass
            print(f"[WARN] HTTP {resp.status_code} (attempt {attempt}/{retries}) -> sleeping {delay:.1f}s")
            time.sleep(delay)
            delay = min(delay * 2, 12.0)
            continue

        # 4xx não-retry
        return resp

    assert last_resp is not None
    return last_resp


def amadeus_search_offers(
    token: str,
    env: str,
    origin: str,
    destination: str,
    departure_date: str,
    return_date: str,
    adults: int,
    children: int,
    cabin: str,
    currency: str,
    direct_only: bool,
    max_results: int,
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[Dict[str, Any]]]:
    base = amadeus_base(env)
    url = f"{base}/v2/shopping/flight-offers"

    params: Dict[str, Any] = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": departure_date,
        "returnDate": return_date,
        "adults": adults,
        "travelClass": cabin,
        "currencyCode": currency,
        "max": max_results,
    }
    if children and children > 0:
        params["children"] = children
    if direct_only:
        params["nonStop"] = "true"

    headers = {"Authorization": f"Bearer {token}"}
    resp = request_with_retry("GET", url, headers=headers, params=params, retries=4)

    if resp.status_code >= 400:
        try:
            payload = resp.json()
        except Exception:
            payload = {"error": resp.text}
        payload["_status"] = resp.status_code
        return None, payload

    try:
        payload = resp.json()
    except Exception as e:
        return None, {"_status": 200, "error": f"invalid_json: {e}"}

    return payload.get("data", []) or [], None


# =============================
# Offer normalization
# =============================
def extract_price_total(offer: Dict[str, Any]) -> Optional[float]:
    for k in ("price_total", "total_price", "total"):
        v = safe_float(offer.get(k))
        if v is not None:
            return v
    p = offer.get("price")
    if isinstance(p, dict):
        v = safe_float(p.get("grandTotal")) or safe_float(p.get("total"))
        if v is not None:
            return v
    tp = offer.get("travelerPricings")
    if isinstance(tp, list) and tp and isinstance(tp[0], dict):
        pp = tp[0].get("price")
        if isinstance(pp, dict):
            v = safe_float(pp.get("grandTotal")) or safe_float(pp.get("total"))
            if v is not None:
                return v
    return None


def normalize_offer(offer: Dict[str, Any]) -> Dict[str, Any]:
    price = extract_price_total(offer)

    vac = offer.get("validatingAirlineCodes")
    carrier = offer.get("carrier") or offer.get("airline") or offer.get("validating_airline")
    if not carrier:
        if isinstance(vac, list) and vac:
            carrier = vac[0]
        elif isinstance(vac, str) and vac:
            carrier = vac
        else:
            carrier = "?"

    stops = None
    try:
        stops_calc = 0
        for it in offer.get("itineraries", []) or []:
            segs = it.get("segments", []) or []
            stops_calc = max(stops_calc, max(0, len(segs) - 1))
        stops = stops_calc
    except Exception:
        stops = 99

    return {"price_total": price, "carrier": carrier, "stops": int(stops), "raw": offer}


# =============================
# Best/Alerts
# =============================
def load_prev_best() -> Dict[str, Any]:
    if not BEST_FILE.exists():
        return {"by_route": {}}
    try:
        txt = BEST_FILE.read_text(encoding="utf-8").strip()
        if not txt:
            return {"by_route": {}}
        data = json.loads(txt)
        if not isinstance(data, dict):
            return {"by_route": {}}
        if "by_route" not in data or not isinstance(data.get("by_route"), dict):
            data["by_route"] = {}
        return data
    except Exception:
        return {"by_route": {}}


def save_best(run_id: str, best_by_route: Dict[str, Any]) -> None:
    BEST_FILE.write_text(json.dumps({"run_id": run_id, "updated_utc": utc_now_iso(), "by_route": best_by_route}, ensure_ascii=False, indent=2), encoding="utf-8")


def save_alerts(run_id: str, alerts: List[Dict[str, Any]]) -> None:
    ALERTS_FILE.write_text(json.dumps({"run_id": run_id, "updated_utc": utc_now_iso(), "alerts": alerts}, ensure_ascii=False, indent=2), encoding="utf-8")


def pick_best_offer(candidates: List[Dict[str, Any]], watch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    norm = []
    for c in candidates:
        offer = c.get("offer")
        if not isinstance(offer, dict):
            continue
        n = normalize_offer(offer)
        if n["price_total"] is None:
            continue
        n["departure_date"] = c.get("departure_date")
        n["return_date"] = c.get("return_date")
        norm.append(n)

    max_stops = watch.get("max_stops")
    if max_stops is not None:
        try:
            ms = int(max_stops)
            norm = [o for o in norm if o["stops"] <= ms]
        except Exception:
            pass

    if not norm:
        return None
    norm.sort(key=lambda x: x["price_total"])
    return norm[0]


def append_history_line(obj: Dict[str, Any]) -> None:
    with HISTORY_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


# =============================
# Main
# =============================
def main() -> None:
    started = utc_now()
    rid = run_id_utc()

    cfg = load_config()
    routes_base = cfg.get("routes") or []
    routes_expanded, expanded_ranges = expand_routes(routes_base)

    validate_immutable_rules(routes_expanded)

    amadeus_env = (os.getenv("AMADEUS_ENV") or "test").strip().lower()
    client_id = os.getenv("AMADEUS_CLIENT_ID") or ""
    client_secret = os.getenv("AMADEUS_CLIENT_SECRET") or ""
    if not client_id or not client_secret:
        raise RuntimeError("Faltam AMADEUS_CLIENT_ID / AMADEUS_CLIENT_SECRET.")

    max_results = int(os.getenv("MAX_RESULTS", str(DEFAULT_MAX_RESULTS)))

    print(f"[INFO] Run: {rid}")
    print(f"[INFO] Routes expanded: {len(routes_expanded)} | Routes base: {len(routes_base)} | Env: {amadeus_env} | Max results: {max_results}")

    token = amadeus_get_token(client_id, client_secret, amadeus_env)

    offers_by_route: Dict[str, List[Dict[str, Any]]] = {}
    errors_by_route: Dict[str, List[Dict[str, Any]]] = {}

    ok_calls = 0
    err_calls = 0
    offers_saved = 0

    debug = {"run_id": rid, "expanded_ranges": expanded_ranges, "errors_sample": {}, "offers_sample": {}}

    for r in routes_expanded:
        rk = route_key(r)
        offers_by_route.setdefault(rk, [])
        errors_by_route.setdefault(rk, [])

        offers, err = amadeus_search_offers(
            token=token,
            env=amadeus_env,
            origin=r["origin"],
            destination=r["destination"],
            departure_date=r["departure_date"],
            return_date=r["return_date"],
            adults=int(r.get("adults", 1)),
            children=int(r.get("children", 0)),
            cabin=str(r.get("cabin", "ECONOMY")).upper(),
            currency=str(r.get("currency", "BRL")).upper(),
            direct_only=bool(r.get("direct_only", False)),
            max_results=max_results,
        )

        if err is not None:
            err_calls += 1
            errors_by_route[rk].append({"ctx": {k: r.get(k) for k in ("origin","destination","departure_date","return_date","adults","children","direct_only")}, "err": err})
            if rk not in debug["errors_sample"]:
                debug["errors_sample"][rk] = err
            continue

        ok_calls += 1
        assert offers is not None
        offers_saved += len(offers)

        if offers and rk not in debug["offers_sample"]:
            debug["offers_sample"][rk] = {"sample_offer_price": extract_price_total(offers[0]), "sample_offer": offers[0]}

        for offer in offers:
            offers_by_route[rk].append({"offer": offer, "departure_date": r["departure_date"], "return_date": r["return_date"]})
            append_history_line(
                {
                    "run_id": rid,
                    "ts_utc": utc_now_iso(),
                    "route_key": rk,
                    "origin": r["origin"],
                    "destination": r["destination"],
                    "departure_date": r["departure_date"],
                    "return_date": r["return_date"],
                    "adults": int(r.get("adults", 1)),
                    "children": int(r.get("children", 0)),
                    "direct_only": bool(r.get("direct_only", False)),
                    "offer": offer,
                }
            )

    # Best + alerts
    best_by_route: Dict[str, Any] = {}
    alerts: List[Dict[str, Any]] = []

    for r in routes_base:
        rk = route_key(r)
        watch = r.get("watch") or {}
        best = pick_best_offer(offers_by_route.get(rk, []), watch)

        if best is None:
            # motivo simples
            note = "no_offers_after_filters"
            if errors_by_route.get(rk) and not offers_by_route.get(rk):
                note = "all_calls_failed"
            elif not errors_by_route.get(rk) and not offers_by_route.get(rk):
                note = "no_offers_returned"

            best_by_route[rk] = {
                "id": r.get("id"),
                "origin": r.get("origin"),
                "destination": r.get("destination"),
                "adults": int(r.get("adults", 1)),
                "children": int(r.get("children", 0)),
                "carrier": None,
                "stops": None,
                "price_total": None,
                "departure_date": None,
                "return_date": None,
                "note": note,
            }
            continue

        best_by_route[rk] = {
            "id": r.get("id"),
            "origin": r.get("origin"),
            "destination": r.get("destination"),
            "adults": int(r.get("adults", 1)),
            "children": int(r.get("children", 0)),
            "carrier": best["carrier"],
            "stops": best["stops"],
            "price_total": best["price_total"],
            "departure_date": best.get("departure_date"),
            "return_date": best.get("return_date"),
        }

        target = safe_float((watch.get("target_price_total")))
        if target is not None and best["price_total"] <= target:
            alerts.append({"type": "TARGET_PRICE", "route_key": rk, "current_price": best["price_total"], "target_price": target})

    save_best(rid, best_by_route)
    save_alerts(rid, alerts)
    DEBUG_FILE.write_text(json.dumps(debug, ensure_ascii=False, indent=2), encoding="utf-8")

    finished = utc_now()
    state = {
        "run_id": rid,
        "started_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_utc": finished.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_sec": int((finished - started).total_seconds()),
        "total_calls": len(routes_expanded),
        "ok_calls": ok_calls,
        "err_calls": err_calls,
        "success_rate": (ok_calls / len(routes_expanded)) if routes_expanded else 0.0,
        "offers_saved": offers_saved,
        "amadeus_env": amadeus_env,
        "max_results": max_results,
        "expanded_ranges": expanded_ranges,
    }
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    SUMMARY_FILE.write_text(
        f"# Flight Agent — Update Summary\n\n"
        f"- run_id: `{rid}`\n"
        f"- ok_calls: `{ok_calls}`\n"
        f"- err_calls: `{err_calls}`\n"
        f"- offers_saved: `{offers_saved}`\n"
        f"- total_calls: `{len(routes_expanded)}`\n"
        f"- expanded_ranges: `{json.dumps(expanded_ranges, ensure_ascii=False)}`\n",
        encoding="utf-8",
    )

    print("[OK] Run completed successfully.")


if __name__ == "__main__":
    main()
