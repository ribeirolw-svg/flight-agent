from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # pragma: no cover


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

AMADEUS_TEST_BASE = "https://test.api.amadeus.com"
AMADEUS_PROD_BASE = "https://api.amadeus.com"

DEFAULT_MAX_RESULTS = int(os.getenv("MAX_RESULTS", "10"))

# ✅ throttling (ajustável por env)
REQUEST_SLEEP_SEC = float(os.getenv("REQUEST_SLEEP_SEC", "0.35"))

# ✅ se começar a dar 429 em sequência, aborta cedo
MAX_429_BEFORE_ABORT = int(os.getenv("MAX_429_BEFORE_ABORT", "6"))


IMMUTABLE_REQUIRED_ORIGINS = ["CGH", "GRU"]
IMMUTABLE_REQUIRED_DESTS = ["CWB", "FCO", "NVT"]


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
    return yaml.safe_load(ROUTES_FILE.read_text(encoding="utf-8")) or {}


def route_key(route: Dict[str, Any]) -> str:
    return str(route.get("id") or f'{route.get("origin","")}-{route.get("destination","")}')


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


def sample_pairs(pairs: List[Tuple[date, date]], step_days: int, max_pairs: int) -> List[Tuple[date, date]]:
    if not pairs:
        return []
    # pega 1 a cada "step_days" (com base no departure)
    sampled = []
    last_dep: Optional[date] = None
    for dep, ret in pairs:
        if last_dep is None or (dep - last_dep).days >= step_days:
            sampled.append((dep, ret))
            last_dep = dep
        if len(sampled) >= max_pairs:
            break
    return sampled


def cap_pairs(pairs: List[Tuple[date, date]], max_pairs: Optional[int]) -> List[Tuple[date, date]]:
    if not max_pairs or max_pairs <= 0:
        return pairs
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
            max_pairs = int(params.get("max_pairs", 12))
            step_days = int(params.get("step_days", 2))

            pairs = generate_rome_pairs(today.year, start_mm_dd, latest_return_mm_dd, trip_days)  # type: ignore
            if not pairs:
                pairs = generate_rome_pairs(today.year + 1, start_mm_dd, latest_return_mm_dd, trip_days)  # type: ignore

            # ✅ sampling + cap pra não estourar
            pairs = sample_pairs(pairs, step_days=step_days, max_pairs=max_pairs)

            if pairs:
                expanded_ranges[rid] = {
                    "min_dep": pairs[0][0].isoformat(),
                    "max_dep": pairs[-1][0].isoformat(),
                    "count": len(pairs),
                    "step_days": step_days,
                }

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
            max_pairs = int(params.get("max_pairs", 16))

            base = today + timedelta(days=start_offset_days)
            pairs = generate_weekend_pairs(base, horizon_days, depart_dows, return_dows, max_trip_len_days)  # type: ignore
            pairs = cap_pairs(pairs, max_pairs)

            if pairs:
                expanded_ranges[rid] = {
                    "base": base.isoformat(),
                    "min_dep": pairs[0][0].isoformat(),
                    "max_dep": pairs[-1][0].isoformat(),
                    "count": len(pairs),
                }

            for dep, ret in pairs:
                rr = dict(r)
                rr["departure_date"] = dep.isoformat()
                rr["return_date"] = ret.isoformat()
                expanded.append(rr)

        else:
            if r.get("departure_date") and r.get("return_date"):
                expanded.append(r)

    for rid, info in expanded_ranges.items():
        print(f"[INFO] Expanded range for {rid}: {info}")

    return expanded, expanded_ranges


def amadeus_base(env: str) -> str:
    return AMADEUS_TEST_BASE if env.lower() == "test" else AMADEUS_PROD_BASE


def amadeus_get_token(client_id: str, client_secret: str, env: str) -> str:
    url = f"{amadeus_base(env)}/v1/security/oauth2/token"
    resp = requests.post(url, data={"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def request_with_retry(method: str, url: str, *, headers: Dict[str, str], params: Dict[str, Any], retries: int = 3) -> requests.Response:
    delay = 1.0
    last_resp: Optional[requests.Response] = None

    for attempt in range(1, retries + 1):
        resp = requests.request(method, url, headers=headers, params=params, timeout=45)
        last_resp = resp
        if resp.status_code < 400:
            return resp

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
    url = f"{amadeus_base(env)}/v2/shopping/flight-offers"
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

    resp = request_with_retry("GET", url, headers={"Authorization": f"Bearer {token}"}, params=params, retries=3)
    if resp.status_code >= 400:
        try:
            payload = resp.json()
        except Exception:
            payload = {"error": resp.text}
        payload["_status"] = resp.status_code
        return None, payload

    return resp.json().get("data", []) or [], None


def extract_price_total(offer: Dict[str, Any]) -> Optional[float]:
    p = offer.get("price")
    if isinstance(p, dict):
        v = safe_float(p.get("grandTotal")) or safe_float(p.get("total"))
        if v is not None:
            return v
    return None


def normalize_offer(offer: Dict[str, Any]) -> Dict[str, Any]:
    vac = offer.get("validatingAirlineCodes")
    carrier = None
    if isinstance(vac, list) and vac:
        carrier = vac[0]
    elif isinstance(vac, str) and vac:
        carrier = vac
    if not carrier:
        carrier = "?"
    stops = 99
    try:
        stops_calc = 0
        for it in offer.get("itineraries", []) or []:
            segs = it.get("segments", []) or []
            stops_calc = max(stops_calc, max(0, len(segs) - 1))
        stops = stops_calc
    except Exception:
        pass

    return {"price_total": extract_price_total(offer), "carrier": carrier, "stops": int(stops), "raw": offer}


def append_history_line(obj: Dict[str, Any]) -> None:
    with HISTORY_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def save_best(run_id: str, best_by_route: Dict[str, Any]) -> None:
    BEST_FILE.write_text(json.dumps({"run_id": run_id, "updated_utc": utc_now_iso(), "by_route": best_by_route}, ensure_ascii=False, indent=2), encoding="utf-8")


def save_alerts(run_id: str, alerts: List[Dict[str, Any]]) -> None:
    ALERTS_FILE.write_text(json.dumps({"run_id": run_id, "updated_utc": utc_now_iso(), "alerts": alerts}, ensure_ascii=False, indent=2), encoding="utf-8")


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

    token = amadeus_get_token(client_id, client_secret, amadeus_env)

    print(f"[INFO] Run: {rid}")
    print(f"[INFO] total_calls={len(routes_expanded)} | REQUEST_SLEEP_SEC={REQUEST_SLEEP_SEC}")

    offers_by_route: Dict[str, List[Dict[str, Any]]] = {}
    errors_sample: Dict[str, Any] = {}
    offers_sample: Dict[str, Any] = {}

    ok_calls = 0
    err_calls = 0
    offers_saved = 0

    consecutive_429 = 0

    for i, r in enumerate(routes_expanded, start=1):
        rk = route_key(r)
        offers_by_route.setdefault(rk, [])

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

        # throttle SEMPRE entre calls
        time.sleep(REQUEST_SLEEP_SEC)

        if err is not None:
            err_calls += 1
            status = err.get("_status")
            if status == 429:
                consecutive_429 += 1
            else:
                consecutive_429 = 0

            if rk not in errors_sample:
                errors_sample[rk] = {"ctx": {k: r.get(k) for k in ("origin","destination","departure_date","return_date","adults","children","direct_only")}, "err": err}

            if consecutive_429 >= MAX_429_BEFORE_ABORT:
                print(f"[FATAL] Muitos 429 consecutivos ({consecutive_429}). Abortando cedo para não queimar cota.")
                break

            continue

        consecutive_429 = 0
        ok_calls += 1
        assert offers is not None
        offers_saved += len(offers)

        if offers and rk not in offers_sample:
            offers_sample[rk] = {"sample_price": extract_price_total(offers[0]), "sample_offer": offers[0]}

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

        print(f"[OK] ({i}/{len(routes_expanded)}) {r['origin']}->{r['destination']} {r['departure_date']}/{r['return_date']} offers={len(offers)}")

    # best_by_route (mínimo)
    best_by_route: Dict[str, Any] = {}
    for r in routes_base:
        rk = route_key(r)
        watch = r.get("watch") or {}
        best = None
        best_price = None

        for c in offers_by_route.get(rk, []):
            n = normalize_offer(c["offer"])
            if n["price_total"] is None:
                continue
            max_stops = watch.get("max_stops")
            if max_stops is not None:
                try:
                    if n["stops"] > int(max_stops):
                        continue
                except Exception:
                    pass
            if best is None or n["price_total"] < best_price:
                best = (n, c)
                best_price = n["price_total"]

        if best is None:
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
                "note": "no_offers_after_filters",
            }
        else:
            n, c = best
            best_by_route[rk] = {
                "id": r.get("id"),
                "origin": r.get("origin"),
                "destination": r.get("destination"),
                "adults": int(r.get("adults", 1)),
                "children": int(r.get("children", 0)),
                "carrier": n["carrier"],
                "stops": n["stops"],
                "price_total": n["price_total"],
                "departure_date": c.get("departure_date"),
                "return_date": c.get("return_date"),
            }

    save_best(rid, best_by_route)
    save_alerts(rid, [])

    debug = {"run_id": rid, "expanded_ranges": expanded_ranges, "errors_sample": errors_sample, "offers_sample": offers_sample}
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
        "request_sleep_sec": REQUEST_SLEEP_SEC,
        "max_429_before_abort": MAX_429_BEFORE_ABORT,
    }
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    SUMMARY_FILE.write_text(
        f"# Flight Agent — Update Summary\n\n"
        f"- run_id: `{rid}`\n"
        f"- total_calls: `{state['total_calls']}`\n"
        f"- ok_calls: `{state['ok_calls']}`\n"
        f"- err_calls: `{state['err_calls']}`\n"
        f"- offers_saved: `{state['offers_saved']}`\n"
        f"- request_sleep_sec: `{REQUEST_SLEEP_SEC}`\n"
        f"- expanded_ranges: `{json.dumps(expanded_ranges, ensure_ascii=False)}`\n",
        encoding="utf-8",
    )

    print("[OK] Run completed successfully.")


if __name__ == "__main__":
    main()
