from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml


# -----------------------------
# Paths
# -----------------------------
REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

STATE_FILE = DATA_DIR / "state.json"
SUMMARY_FILE = DATA_DIR / "summary.md"
HISTORY_FILE = DATA_DIR / "history.jsonl"
BEST_FILE = DATA_DIR / "best_offers.json"
ALERTS_FILE = DATA_DIR / "alerts.json"
DEBUG_FILE = DATA_DIR / "debug_last_run.json"

ROUTES_FILE = REPO_ROOT / "routes.yaml"


# -----------------------------
# Env knobs
# -----------------------------
AMADEUS_ENV = os.getenv("AMADEUS_ENV", "test").strip().lower()
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "10"))

REQUEST_SLEEP_SEC = float(os.getenv("REQUEST_SLEEP_SEC", "0.6"))
MAX_429_BEFORE_ABORT = int(os.getenv("MAX_429_BEFORE_ABORT", "3"))

AMADEUS_CLIENT_ID = os.getenv("AMADEUS_CLIENT_ID", "").strip()
AMADEUS_CLIENT_SECRET = os.getenv("AMADEUS_CLIENT_SECRET", "").strip()


# -----------------------------
# Utilities
# -----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"routes file not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def append_history_line(obj: Dict[str, Any]) -> None:
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        txt = path.read_text(encoding="utf-8").strip()
        if not txt:
            return default
        return json.loads(txt)
    except Exception:
        return default


# -----------------------------
# Amadeus API
# -----------------------------
def amadeus_base_url(env: str) -> str:
    # test vs production
    return "https://test.api.amadeus.com" if env == "test" else "https://api.amadeus.com"


def amadeus_get_token(env: str, client_id: str, client_secret: str) -> str:
    if not client_id or not client_secret:
        raise RuntimeError("AMADEUS_CLIENT_ID/AMADEUS_CLIENT_SECRET not set")

    url = f"{amadeus_base_url(env)}/v1/security/oauth2/token"
    resp = requests.post(
        url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=45,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["access_token"]


def request_with_retry(
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    params: Dict[str, Any],
    retries: int = 3,
) -> requests.Response:
    delay = 1.0
    last_resp: Optional[requests.Response] = None

    for attempt in range(1, retries + 1):
        resp = requests.request(method, url, headers=headers, params=params, timeout=45)
        last_resp = resp

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

        # 4xx non-retry
        return resp

    assert last_resp is not None
    return last_resp


def amadeus_search_offers(
    *,
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
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    url = f"{amadeus_base_url(env)}/v2/shopping/flight-offers"
    headers = {"Authorization": f"Bearer {token}"}
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

    resp = request_with_retry("GET", url, headers=headers, params=params, retries=3)

    if resp.status_code >= 400:
        body_txt = ""
        try:
            body_txt = resp.text or ""
        except Exception:
            body_txt = ""
        err_payload: Dict[str, Any] = {
            "_status": resp.status_code,
            "body": body_txt[:1200],
        }
        try:
            j = resp.json()
            # Amadeus costuma vir em "errors"
            if isinstance(j, dict) and "errors" in j:
                err_payload["errors"] = j.get("errors")
            else:
                err_payload["json"] = j
        except Exception:
            pass
        return [], err_payload

    try:
        j = resp.json()
    except Exception:
        return [], {"_status": resp.status_code, "body": (resp.text or "")[:1200], "message": "invalid_json_response"}

    data = j.get("data", [])
    if not isinstance(data, list):
        data = []
    return data, None


# -----------------------------
# Offer normalization (best/alerts)
# -----------------------------
def extract_price_total(offer: Dict[str, Any]) -> Optional[float]:
    try:
        p = offer.get("price", {}).get("grandTotal")
        if p is not None:
            return float(p)
    except Exception:
        pass
    try:
        p = offer.get("price", {}).get("total")
        if p is not None:
            return float(p)
    except Exception:
        pass
    return None


def extract_carrier(offer: Dict[str, Any]) -> str:
    try:
        vac = offer.get("validatingAirlineCodes")
        if isinstance(vac, list) and vac:
            return str(vac[0])
        if isinstance(vac, str) and vac:
            return vac
    except Exception:
        pass
    return "?"


def extract_stops(offer: Dict[str, Any]) -> Optional[int]:
    # stops = total segments across itineraries - number of itineraries
    # (roundtrip: 2 itineraries). We'll compute max(segments-1) across itineraries and sum.
    try:
        itins = offer.get("itineraries", [])
        if not isinstance(itins, list) or not itins:
            return None
        total_stops = 0
        for itin in itins:
            segs = itin.get("segments", [])
            if isinstance(segs, list):
                total_stops += max(0, len(segs) - 1)
        return int(total_stops)
    except Exception:
        return None


@dataclass
class OfferMeta:
    offer: Dict[str, Any]
    departure_date: str
    return_date: str


def pick_best_offer(offers_meta: List[OfferMeta], watch: Dict[str, Any]) -> Optional[Tuple[OfferMeta, float, str, Optional[int]]]:
    candidates: List[Tuple[OfferMeta, float, str, Optional[int]]] = []

    max_stops = watch.get("max_stops")
    max_stops_i: Optional[int] = None
    if max_stops is not None:
        try:
            max_stops_i = int(max_stops)
        except Exception:
            max_stops_i = None

    prefer_airlines = set(watch.get("prefer_airlines") or [])

    for om in offers_meta:
        price = extract_price_total(om.offer)
        if price is None:
            continue
        carrier = extract_carrier(om.offer)
        stops = extract_stops(om.offer)

        # filter stops if requested
        if max_stops_i is not None and stops is not None and stops > max_stops_i:
            continue

        candidates.append((om, float(price), carrier, stops))

    if not candidates:
        return None

    # airline preference
    if prefer_airlines:
        preferred = [c for c in candidates if c[2] in prefer_airlines]
        if preferred:
            candidates = preferred

    candidates.sort(key=lambda x: x[1])
    return candidates[0]


def load_prev_best() -> Dict[str, Any]:
    # tolerate empty/invalid file
    payload = read_json(BEST_FILE, {"by_route": {}})
    if not isinstance(payload, dict):
        return {"by_route": {}}
    by_route = payload.get("by_route", {})
    if not isinstance(by_route, dict):
        by_route = {}
    return {"by_route": by_route}


def build_best_and_alerts(
    rid: str,
    routes_base: List[Dict[str, Any]],
    offers_by_route: Dict[str, List[OfferMeta]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    prev_best = load_prev_best().get("by_route", {})
    best_by_route: Dict[str, Any] = {}
    alerts: List[Dict[str, Any]] = []

    for base in routes_base:
        route_id = base["id"]
        watch = base.get("watch") or {}

        best_pick = pick_best_offer(offers_by_route.get(route_id, []), watch)

        if best_pick is None:
            best_by_route[route_id] = {
                "id": route_id,
                "origin": base["origin"],
                "destination": base["destination"],
                "adults": base.get("adults", 1),
                "children": base.get("children", 0),
                "carrier": None,
                "stops": None,
                "price_total": None,
                "departure_date": None,
                "return_date": None,
                "note": "no_offers_after_filters",
            }
            continue

        om, price, carrier, stops = best_pick
        best_payload = {
            "id": route_id,
            "origin": base["origin"],
            "destination": base["destination"],
            "adults": base.get("adults", 1),
            "children": base.get("children", 0),
            "carrier": carrier,
            "stops": stops,
            "price_total": price,
            "departure_date": om.departure_date,
            "return_date": om.return_date,
        }
        best_by_route[route_id] = best_payload

        # Alerts: target
        target = safe_float(watch.get("target_price_total"))
        if target is not None and price <= target:
            alerts.append(
                {
                    "type": "TARGET_PRICE",
                    "route_id": route_id,
                    "message": f'Alvo atingido: {base["origin"]}->{base["destination"]} <= {target:.2f}',
                    "current_price": price,
                    "target_price": target,
                    "carrier": carrier,
                    "stops": stops,
                    "departure_date": om.departure_date,
                    "return_date": om.return_date,
                }
            )

        # Alerts: drop pct vs previous best
        prev = prev_best.get(route_id, {})
        prev_price = safe_float(prev.get("price_total"))
        drop_pct = safe_float(watch.get("alert_drop_pct"))
        if prev_price and drop_pct:
            delta_pct = (prev_price - price) / prev_price * 100.0
            if delta_pct >= drop_pct:
                alerts.append(
                    {
                        "type": "DROP_PCT",
                        "route_id": route_id,
                        "message": f"Queda {delta_pct:.1f}%: {base['origin']}->{base['destination']}",
                        "current_price": price,
                        "prev_best_price": prev_price,
                        "delta_pct": delta_pct,
                        "carrier": carrier,
                        "stops": stops,
                        "departure_date": om.departure_date,
                        "return_date": om.return_date,
                    }
                )

    return best_by_route, alerts


# -----------------------------
# Rules: expand routes
# -----------------------------
def parse_mm_dd(x: Any) -> Tuple[int, int]:
    if isinstance(x, (list, tuple)) and len(x) == 2:
        return int(x[0]), int(x[1])
    raise ValueError("expected [MM, DD]")


def daterange(start: date, end: date) -> List[date]:
    out: List[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def expand_rome_15d_window(base: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rp = base.get("rule_params") or {}
    trip_days = int(rp.get("trip_days", 15))
    max_pairs = int(rp.get("max_pairs", 10))
    step_days = int(rp.get("step_days", 2))

    start_mm_dd = parse_mm_dd(rp.get("start_mm_dd", [9, 1]))
    latest_ret_mm_dd = parse_mm_dd(rp.get("latest_return_mm_dd", [10, 5]))

    # window in next year relative to current year (good enough for your use)
    today = datetime.now().date()
    year = today.year if today.month <= 10 else today.year + 1

    min_dep = date(year, start_mm_dd[0], start_mm_dd[1])
    latest_return = date(year, latest_ret_mm_dd[0], latest_ret_mm_dd[1])

    # dep must satisfy dep + trip_days <= latest_return
    max_dep = latest_return - timedelta(days=trip_days)

    pairs: List[Dict[str, Any]] = []
    cur = min_dep
    while cur <= max_dep and len(pairs) < max_pairs:
        dep = cur
        ret = dep + timedelta(days=trip_days)
        r = dict(base)
        r["departure_date"] = dep.isoformat()
        r["return_date"] = ret.isoformat()
        pairs.append(r)
        cur += timedelta(days=step_days)

    meta = {
        "min_dep": min_dep.isoformat(),
        "max_dep": max_dep.isoformat(),
        "count": len(pairs),
        "step_days": step_days,
    }
    return pairs, meta


def expand_weekend_window(base: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rp = base.get("rule_params") or {}
    start_offset_days = int(rp.get("start_offset_days", 60))
    horizon_days = int(rp.get("horizon_days", 60))
    max_pairs = int(rp.get("max_pairs", 16))
    max_trip_len_days = int(rp.get("max_trip_len_days", 4))
    depart_dows = [int(x) for x in (rp.get("depart_dows") or [4, 5])]  # 0=Mon..6=Sun
    return_dows = [int(x) for x in (rp.get("return_dows") or [6, 0])]

    today = datetime.now().date()
    base_day = today + timedelta(days=start_offset_days)
    end_day = base_day + timedelta(days=horizon_days)

    pairs: List[Dict[str, Any]] = []
    # generate departure candidates within window matching depart_dows
    for dep in daterange(base_day, end_day):
        if dep.weekday() not in depart_dows:
            continue

        # return candidates within trip len constraint matching return_dows
        for d in range(1, max_trip_len_days + 1):
            ret = dep + timedelta(days=d)
            if ret > end_day:
                break
            if ret.weekday() not in return_dows:
                continue

            r = dict(base)
            r["departure_date"] = dep.isoformat()
            r["return_date"] = ret.isoformat()
            pairs.append(r)

            if len(pairs) >= max_pairs:
                meta = {
                    "base": base_day.isoformat(),
                    "min_dep": base_day.isoformat(),
                    "max_dep": (base_day + timedelta(days=min(horizon_days, 999999))).isoformat(),
                    "count": len(pairs),
                }
                return pairs, meta

    meta = {
        "base": base_day.isoformat(),
        "min_dep": base_day.isoformat(),
        "max_dep": end_day.isoformat(),
        "count": len(pairs),
    }
    return pairs, meta


def expand_routes(routes_base: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    expanded: List[Dict[str, Any]] = []
    ranges: Dict[str, Any] = {}

    for base in routes_base:
        rule = (base.get("rule") or "").strip().upper()
        if rule == "ROME_15D_WINDOW":
            pairs, meta = expand_rome_15d_window(base)
            expanded.extend(pairs)
            ranges[base["id"]] = meta
        elif rule == "WEEKEND_WINDOW":
            pairs, meta = expand_weekend_window(base)
            expanded.extend(pairs)
            ranges[base["id"]] = meta
        else:
            # fallback: if explicit dep/ret exists
            if base.get("departure_date") and base.get("return_date"):
                expanded.append(base)
                ranges[base["id"]] = {
                    "min_dep": base["departure_date"],
                    "max_dep": base["departure_date"],
                    "count": 1,
                }
            else:
                ranges[base["id"]] = {"note": "no_rule_and_no_explicit_dates", "count": 0}

    return expanded, ranges


# -----------------------------
# Immutable guardrails
# -----------------------------
IMMUTABLE_REQUIRED_ORIGINS = ["CGH", "GRU"]
IMMUTABLE_REQUIRED_DESTS = ["CWB", "FCO", "NVT"]


def validate_immutable(routes_base: List[Dict[str, Any]]) -> None:
    origins = sorted({r.get("origin") for r in routes_base if r.get("origin")})
    dests = sorted({r.get("destination") for r in routes_base if r.get("destination")})

    missing_o = [x for x in IMMUTABLE_REQUIRED_ORIGINS if x not in origins]
    missing_d = [x for x in IMMUTABLE_REQUIRED_DESTS if x not in dests]

    if missing_o or missing_d:
        print("[FATAL] routes.yaml violou regras imutáveis.")
        if missing_o:
            print(f"[FATAL] Origens faltando: {missing_o}")
        if missing_d:
            print(f"[FATAL] Destinos faltando: {missing_d}")
        raise SystemExit(1)


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    rid = run_id()
    started = utc_now_iso()

    print(f"[INFO] Run: {rid}")
    print(f"[INFO] Repo root: {REPO_ROOT}")
    print(f"[INFO] Data dir:  {DATA_DIR}")
    print(f"[INFO] Store: default | Env: {AMADEUS_ENV} | Max results: {MAX_RESULTS}")

    cfg = load_yaml(ROUTES_FILE)
    routes_base = cfg.get("routes") or []
    if not isinstance(routes_base, list) or not routes_base:
        raise RuntimeError("routes.yaml must contain routes: [ ... ]")

    # ensure each route has an id
    for r in routes_base:
        if "id" not in r or not r["id"]:
            # deterministic fallback
            r["id"] = f'{r.get("destination","UNK")}_{r.get("origin","UNK")}_{r.get("adults",1)}A{r.get("children",0)}C'

    validate_immutable(routes_base)

    # Expand based on rule
    routes_expanded, expanded_ranges = expand_routes(routes_base)

    print(f"[INFO] Routes expanded: {len(routes_expanded)} | Routes base: {len(routes_base)} | Routes file: {ROUTES_FILE}")

    # debug payload
    debug: Dict[str, Any] = {
        "run_id": rid,
        "ts_utc": started,
        "env": AMADEUS_ENV,
        "max_results": MAX_RESULTS,
        "request_sleep_sec": REQUEST_SLEEP_SEC,
        "max_429_before_abort": MAX_429_BEFORE_ABORT,
        "expanded_ranges": expanded_ranges,
        "errors_sample": {},
        "offers_sample": {},
        "status_counts": {},
    }

    # state counters
    total_calls = 0
    ok_calls = 0
    err_calls = 0
    empty_ok_calls = 0
    offers_saved = 0

    status_counts: Dict[str, int] = {}
    consecutive_429 = 0

    offers_by_route: Dict[str, List[OfferMeta]] = {r["id"]: [] for r in routes_base}

    # token
    try:
        token = amadeus_get_token(AMADEUS_ENV, AMADEUS_CLIENT_ID, AMADEUS_CLIENT_SECRET)
    except Exception as e:
        # hard fail: cannot proceed
        err = {"_status": "TOKEN_ERROR", "message": str(e)}
        debug["errors_sample"]["TOKEN"] = err
        write_json(DEBUG_FILE, debug)
        raise

    # execute calls
    for idx, r in enumerate(routes_expanded, start=1):
        route_id = r["id"]
        rk = route_id

        total_calls += 1

        offers, err = amadeus_search_offers(
            token=token,
            env=AMADEUS_ENV,
            origin=str(r["origin"]).upper(),
            destination=str(r["destination"]).upper(),
            departure_date=r["departure_date"],
            return_date=r["return_date"],
            adults=int(r.get("adults", 1)),
            children=int(r.get("children", 0)),
            cabin=str(r.get("cabin", "ECONOMY")).upper(),
            currency=str(r.get("currency", "BRL")).upper(),
            direct_only=bool(r.get("direct_only", False)),
            max_results=MAX_RESULTS,
        )

        time.sleep(REQUEST_SLEEP_SEC)

        if err is not None:
            err_calls += 1
            stc = str(err.get("_status", "unknown"))
            status_counts[stc] = status_counts.get(stc, 0) + 1

            if stc == "429":
                consecutive_429 += 1
            else:
                consecutive_429 = 0

            # save one sample per route_id
            if rk not in debug["errors_sample"]:
                debug["errors_sample"][rk] = {
                    "ctx": {
                        "origin": r.get("origin"),
                        "destination": r.get("destination"),
                        "departure_date": r.get("departure_date"),
                        "return_date": r.get("return_date"),
                        "adults": r.get("adults"),
                        "children": r.get("children"),
                        "direct_only": r.get("direct_only"),
                    },
                    "err": err,
                }

            short = err.get("errors") or err.get("message") or err.get("body") or err
            print(
                f"[ERR] ({idx}/{len(routes_expanded)}) {r.get('origin')}->{r.get('destination')} "
                f"{r.get('departure_date')}/{r.get('return_date')} | status={stc} | {str(short)[:240]}"
            )

            if consecutive_429 >= MAX_429_BEFORE_ABORT:
                print(f"[FATAL] Muitos 429 consecutivos ({consecutive_429}). Abortando cedo.")
                break

            continue

        # status < 400
        ok_calls += 1

        if not offers:
            empty_ok_calls += 1
            status_counts["200_empty"] = status_counts.get("200_empty", 0) + 1
            print(
                f"[OK] ({idx}/{len(routes_expanded)}) {r.get('origin')}->{r.get('destination')} "
                f"{r.get('departure_date')}/{r.get('return_date')} | offers: 0"
            )
            continue

        offers_saved += len(offers)

        if rk not in debug["offers_sample"]:
            debug["offers_sample"][rk] = {
                "count": len(offers),
                "sample_price": extract_price_total(offers[0]),
                "sample_carrier": extract_carrier(offers[0]),
            }

        print(
            f"[OK] ({idx}/{len(routes_expanded)}) {r.get('origin')}->{r.get('destination')} "
            f"{r.get('departure_date')}/{r.get('return_date')} | offers: {len(offers)}"
        )

        for offer in offers:
            # store for best/alerts
            offers_by_route[route_id].append(OfferMeta(offer=offer, departure_date=r["departure_date"], return_date=r["return_date"]))

            # store in history
            append_history_line(
                {
                    "run_id": rid,
                    "ts_utc": utc_now_iso(),
                    "route_key": route_id,
                    "origin": r["origin"],
                    "destination": r["destination"],
                    "departure_date": r["departure_date"],
                    "return_date": r["return_date"],
                    "adults": int(r.get("adults", 1)),
                    "children": int(r.get("children", 0)),
                    "cabin": str(r.get("cabin", "ECONOMY")).upper(),
                    "currency": str(r.get("currency", "BRL")).upper(),
                    "direct_only": bool(r.get("direct_only", False)),
                    "offer": offer,
                }
            )

    finished = utc_now_iso()

    # Best & Alerts
    best_by_route, alerts = build_best_and_alerts(rid, routes_base, offers_by_route)

    write_json(BEST_FILE, {"run_id": rid, "updated_utc": finished, "by_route": best_by_route})
    write_json(ALERTS_FILE, {"run_id": rid, "updated_utc": finished, "alerts": alerts})

    # final state
    duration = int((datetime.fromisoformat(started.replace("Z", "+00:00")) - datetime.fromisoformat(started.replace("Z", "+00:00"))).total_seconds())
    # ^ placeholder; compute safely:
    try:
        dt_start = datetime.fromisoformat(started.replace("Z", "+00:00"))
        dt_end = datetime.fromisoformat(finished.replace("Z", "+00:00"))
        duration = int((dt_end - dt_start).total_seconds())
    except Exception:
        duration = 0

    success_rate = (ok_calls / total_calls) if total_calls else 0.0

    state_payload = {
        "run_id": rid,
        "started_utc": started,
        "finished_utc": finished,
        "duration_sec": duration,
        "total_calls": total_calls,
        "ok_calls": ok_calls,
        "err_calls": err_calls,
        "empty_ok_calls": empty_ok_calls,
        "success_rate": round(success_rate, 3),
        "offers_saved": offers_saved,
        "store": "default",
        "max_results": MAX_RESULTS,
        "amadeus_env": AMADEUS_ENV,
        "immutable_required_origins": IMMUTABLE_REQUIRED_ORIGINS,
        "immutable_required_dests": IMMUTABLE_REQUIRED_DESTS,
        "expanded_ranges": expanded_ranges,
        "request_sleep_sec": REQUEST_SLEEP_SEC,
        "max_429_before_abort": MAX_429_BEFORE_ABORT,
        "status_counts": status_counts,
    }
    write_json(STATE_FILE, state_payload)

    # debug file
    debug["status_counts"] = status_counts
    write_json(DEBUG_FILE, debug)

    # summary
    # pick sample offers from best_by_route
    sample_lines: List[str] = []
    for rid_key, bo in best_by_route.items():
        if bo.get("price_total") is None:
            continue
        sample_lines.append(
            f"- {bo.get('origin')}->{bo.get('destination')} {bo.get('departure_date')}/{bo.get('return_date')} | "
            f"{bo.get('carrier')} | BRL {bo.get('price_total'):.2f} | {bo.get('stops')} stop(s)"
        )
        if len(sample_lines) >= 5:
            break

    summary_md = []
    summary_md.append("# Flight Agent — Update Summary\n")
    summary_md.append(f"- started_utc: `{started}`")
    summary_md.append(f"- finished_utc: `{finished}`")
    summary_md.append(f"- duration_sec: `{duration}`")
    summary_md.append(f"- total_calls: `{total_calls}`")
    summary_md.append(f"- ok_calls: `{ok_calls}`")
    summary_md.append(f"- err_calls: `{err_calls}`")
    summary_md.append(f"- empty_ok_calls: `{empty_ok_calls}`")
    summary_md.append(f"- success_rate: `{success_rate:.3f}`")
    summary_md.append(f"- offers_saved: `{offers_saved}`")
    summary_md.append(f"- store: `default`")
    summary_md.append(f"- max_results: `{MAX_RESULTS}`")
    summary_md.append(f"- amadeus_env: `{AMADEUS_ENV}`")
    summary_md.append(f"- immutable_required_origins: `{IMMUTABLE_REQUIRED_ORIGINS}`")
    summary_md.append(f"- immutable_required_dests: `{IMMUTABLE_REQUIRED_DESTS}`")
    summary_md.append(f"- request_sleep_sec: `{REQUEST_SLEEP_SEC}`")
    summary_md.append(f"- max_429_before_abort: `{MAX_429_BEFORE_ABORT}`")
    summary_md.append(f"- status_counts: `{status_counts}`")
    summary_md.append("\n## Sample best offers (preview)")
    if sample_lines:
        summary_md.extend(sample_lines)
    else:
        summary_md.append("- (none)")

    SUMMARY_FILE.write_text("\n".join(summary_md) + "\n", encoding="utf-8")

    print("\n[OK] Run completed successfully.")


if __name__ == "__main__":
    main()
