# scheduler.py
from __future__ import annotations

import json
import os
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
# Repo-root safe paths
# =============================
# garante que sempre grava em <repo>/data mesmo se o script for chamado de outro cwd
REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

ROUTES_FILE = REPO_ROOT / "routes.yaml"

STATE_FILE = DATA_DIR / "state.json"
SUMMARY_FILE = DATA_DIR / "summary.md"
HISTORY_FILE = DATA_DIR / "history.jsonl"
BEST_FILE = DATA_DIR / "best_offers.json"
ALERTS_FILE = DATA_DIR / "alerts.json"

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
        return float(x)
    except Exception:
        return None


def load_config() -> Dict[str, Any]:
    if not ROUTES_FILE.exists():
        raise FileNotFoundError(f"routes.yaml não encontrado em: {ROUTES_FILE}")
    return yaml.safe_load(ROUTES_FILE.read_text(encoding="utf-8")) or {}


def route_key(route: Dict[str, Any]) -> str:
    rid = route.get("id")
    if rid:
        return str(rid)
    return f'{route.get("origin","")}-{route.get("destination","")}:{route.get("cabin","")}'


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
    start_mm_dd: Tuple[int, int] = (9, 1),
    latest_return_mm_dd: Tuple[int, int] = (10, 5),
    trip_days: int = 15,
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
    horizon_days: int = 30,
    depart_dows: Tuple[int, int] = (4, 5),  # Sex(4) / Sáb(5)
    return_dows: Tuple[int, int] = (6, 0),  # Dom(6) / Seg(0)
    max_trip_len_days: int = 4,
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


def expand_routes(routes_base: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    today = local_today()
    expanded: List[Dict[str, Any]] = []

    for r in routes_base:
        rule = (r.get("rule") or "").strip().upper()

        # rota fixa
        if r.get("departure_date") and r.get("return_date") and not rule:
            expanded.append(r)
            continue

        if rule == "ROME_15D_WINDOW":
            params = r.get("rule_params") or {}
            trip_days = int(params.get("trip_days", 15))
            start_mm_dd = tuple(params.get("start_mm_dd", [9, 1]))
            latest_return_mm_dd = tuple(params.get("latest_return_mm_dd", [10, 5]))

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

            for dep, ret in pairs:
                rr = dict(r)
                rr["departure_date"] = dep.isoformat()
                rr["return_date"] = ret.isoformat()
                expanded.append(rr)

        elif rule == "WEEKEND_30D":
            params = r.get("rule_params") or {}
            horizon_days = int(params.get("horizon_days", 30))
            depart_dows = tuple(params.get("depart_dows", [4, 5]))
            return_dows = tuple(params.get("return_dows", [6, 0]))
            max_trip_len_days = int(params.get("max_trip_len_days", 4))

            pairs = generate_weekend_pairs(
                base=today,
                horizon_days=horizon_days,
                depart_dows=depart_dows,  # type: ignore
                return_dows=return_dows,  # type: ignore
                max_trip_len_days=max_trip_len_days,
            )

            for dep, ret in pairs:
                rr = dict(r)
                rr["departure_date"] = dep.isoformat()
                rr["return_date"] = ret.isoformat()
                expanded.append(rr)

        else:
            # sem datas e sem rule reconhecida
            continue

    return expanded


# =============================
# Amadeus
# =============================
def amadeus_base(env: str) -> str:
    return AMADEUS_TEST_BASE if env.lower() == "test" else AMADEUS_PROD_BASE


def amadeus_get_token(client_id: str, client_secret: str, env: str) -> str:
    base = amadeus_base(env)
    url = f"{base}/v1/security/oauth2/token"
    resp = requests.post(
        url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


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
) -> List[Dict[str, Any]]:
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
    resp = requests.get(url, headers=headers, params=params, timeout=45)

    if resp.status_code >= 400:
        try:
            payload = resp.json()
        except Exception:
            payload = {"error": resp.text}
        return [{"_error": payload, "_status": resp.status_code}]

    payload = resp.json()
    return payload.get("data", []) or []


# =============================
# Offer normalization (fix NULL)
# =============================
def normalize_offer(offer: Dict[str, Any]) -> Dict[str, Any]:
    """
    Aceita:
    - Raw Amadeus: price.grandTotal / validatingAirlineCodes / itineraries.segments
    - Simplificado: price_total/total_price/price/total, carrier/airline, stops/number_of_stops
    """
    price = (
        safe_float(offer.get("price_total"))
        or safe_float(offer.get("total_price"))
        or safe_float(offer.get("price"))
        or safe_float(offer.get("total"))
    )
    if price is None:
        price = safe_float((offer.get("price") or {}).get("grandTotal"))

    carrier = offer.get("carrier") or offer.get("airline") or offer.get("validating_airline")
    if not carrier:
        vac = offer.get("validatingAirlineCodes")
        if isinstance(vac, list) and vac:
            carrier = vac[0]
        elif isinstance(vac, str) and vac:
            carrier = vac
        else:
            carrier = "?"

    stops = offer.get("stops")
    if stops is None:
        stops = offer.get("number_of_stops")
    if stops is None:
        try:
            stops_calc = 0
            for it in offer.get("itineraries", []) or []:
                segs = it.get("segments", []) or []
                stops_calc = max(stops_calc, max(0, len(segs) - 1))
            stops = stops_calc
        except Exception:
            stops = 99

    try:
        stops = int(stops)
    except Exception:
        stops = 99

    return {"price_total": price, "carrier": carrier, "stops": stops, "raw": offer}


# =============================
# Best/Alerts
# =============================
def load_prev_best() -> Dict[str, Any]:
    if not BEST_FILE.exists():
        return {"by_route": {}}
    return json.loads(BEST_FILE.read_text(encoding="utf-8"))


def save_best(run_id: str, best_by_route: Dict[str, Any]) -> None:
    payload = {"run_id": run_id, "updated_utc": utc_now_iso(), "by_route": best_by_route}
    BEST_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def save_alerts(run_id: str, alerts: List[Dict[str, Any]]) -> None:
    payload = {"run_id": run_id, "updated_utc": utc_now_iso(), "alerts": alerts}
    ALERTS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def pick_best_offer(candidates: List[Dict[str, Any]], watch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    normed = []
    for c in candidates:
        offer = c.get("offer")
        if not isinstance(offer, dict) or "_error" in offer:
            continue
        n = normalize_offer(offer)
        if n["price_total"] is None:
            continue
        n["departure_date"] = c.get("departure_date")
        n["return_date"] = c.get("return_date")
        normed.append(n)

    max_stops = watch.get("max_stops")
    if max_stops is not None:
        try:
            ms = int(max_stops)
            normed = [o for o in normed if o["stops"] <= ms]
        except Exception:
            pass

    prefer_airlines = set(watch.get("prefer_airlines") or [])
    if prefer_airlines:
        preferred = [o for o in normed if o["carrier"] in prefer_airlines]
        if preferred:
            normed = preferred

    if not normed:
        return None

    normed.sort(key=lambda x: x["price_total"])
    return normed[0]


def build_best_and_alerts(
    run_id: str,
    routes_base: List[Dict[str, Any]],
    offers_by_route: Dict[str, List[Dict[str, Any]]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    prev_best = load_prev_best().get("by_route", {})
    best_by_route: Dict[str, Any] = {}
    alerts: List[Dict[str, Any]] = []

    for r in routes_base:
        rk = route_key(r)
        watch = r.get("watch") or {}
        best = pick_best_offer(offers_by_route.get(rk, []), watch)

        adults = int(r.get("adults", 1))
        children = int(r.get("children", 0))

        if best is None:
            best_by_route[rk] = {
                "id": r.get("id"),
                "origin": r.get("origin"),
                "destination": r.get("destination"),
                "adults": adults,
                "children": children,
                "carrier": None,
                "stops": None,
                "price_total": None,
                "departure_date": None,
                "return_date": None,
                "note": "no_offers_after_filters",
            }
            continue

        best_payload = {
            "id": r.get("id"),
            "origin": r.get("origin"),
            "destination": r.get("destination"),
            "adults": adults,
            "children": children,
            "carrier": best["carrier"],
            "stops": best["stops"],
            "price_total": best["price_total"],
            "departure_date": best.get("departure_date"),
            "return_date": best.get("return_date"),
        }
        best_by_route[rk] = best_payload

        target = safe_float(watch.get("target_price_total"))
        if target is not None and best["price_total"] <= target:
            alerts.append(
                {
                    "type": "TARGET_PRICE",
                    "route_key": rk,
                    "message": f'Alvo atingido: {r["origin"]}->{r["destination"]} <= {target:.2f}',
                    "current_price": best["price_total"],
                    "target_price": target,
                    "carrier": best["carrier"],
                    "stops": best["stops"],
                    "departure_date": best.get("departure_date"),
                    "return_date": best.get("return_date"),
                    "adults": adults,
                    "children": children,
                }
            )

        prev = prev_best.get(rk, {})
        prev_price = safe_float(prev.get("price_total"))
        drop_pct = safe_float(watch.get("alert_drop_pct"))
        if prev_price and drop_pct:
            delta_pct = (prev_price - best["price_total"]) / prev_price * 100.0
            if delta_pct >= drop_pct:
                alerts.append(
                    {
                        "type": "DROP_PCT",
                        "route_key": rk,
                        "message": f'Queda {delta_pct:.1f}%: {r["origin"]}->{r["destination"]}',
                        "current_price": best["price_total"],
                        "prev_best_price": prev_price,
                        "delta_pct": delta_pct,
                        "carrier": best["carrier"],
                        "stops": best["stops"],
                        "departure_date": best.get("departure_date"),
                        "return_date": best.get("return_date"),
                        "adults": adults,
                        "children": children,
                    }
                )

    return best_by_route, alerts


# =============================
# Persistence
# =============================
def append_history_line(obj: Dict[str, Any]) -> None:
    with HISTORY_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def save_summary_md(summary: str) -> None:
    SUMMARY_FILE.write_text(summary, encoding="utf-8")


# =============================
# Main
# =============================
def main() -> None:
    started = utc_now()
    rid = run_id_utc()

    cfg = load_config()
    routes_base = cfg.get("routes") or []
    routes_expanded = expand_routes(routes_base)

    validate_immutable_rules(routes_expanded)

    amadeus_env = (os.getenv("AMADEUS_ENV") or "test").strip().lower()
    client_id = os.getenv("AMADEUS_CLIENT_ID") or ""
    client_secret = os.getenv("AMADEUS_CLIENT_SECRET") or ""
    if not client_id or not client_secret:
        raise RuntimeError("Faltam secrets AMADEUS_CLIENT_ID / AMADEUS_CLIENT_SECRET no ambiente.")

    max_results = int(os.getenv("MAX_RESULTS", str(DEFAULT_MAX_RESULTS)))

    print(f"[INFO] Run: {rid}")
    print(f"[INFO] Repo root: {REPO_ROOT}")
    print(f"[INFO] Data dir:  {DATA_DIR}")
    print(f"[INFO] Store: default | Env: {amadeus_env} | Max results: {max_results}")
    print(f"[INFO] Routes expanded: {len(routes_expanded)} | Routes base: {len(routes_base)} | Routes file: {ROUTES_FILE}")

    token = amadeus_get_token(client_id, client_secret, amadeus_env)

    offers_by_route: Dict[str, List[Dict[str, Any]]] = {}
    ok_calls = 0
    err_calls = 0
    offers_saved = 0
    total_calls = len(routes_expanded)

    for idx, r in enumerate(routes_expanded, start=1):
        origin = r["origin"]
        dest = r["destination"]
        dep = r["departure_date"]
        ret = r["return_date"]
        adults = int(r.get("adults", 1))
        children = int(r.get("children", 0))
        cabin = str(r.get("cabin", "ECONOMY")).upper()
        currency = str(r.get("currency", "BRL")).upper()
        direct_only = bool(r.get("direct_only", False))

        rk = route_key(r)
        offers_by_route.setdefault(rk, [])

        data = amadeus_search_offers(
            token=token,
            env=amadeus_env,
            origin=origin,
            destination=dest,
            departure_date=dep,
            return_date=ret,
            adults=adults,
            children=children,
            cabin=cabin,
            currency=currency,
            direct_only=direct_only,
            max_results=max_results,
        )

        if data and isinstance(data, list) and isinstance(data[0], dict) and "_error" in data[0]:
            err_calls += 1
            print(f"[ERR] ({idx}/{total_calls}) {origin}->{dest} {dep}/{ret} | error_status={data[0].get('_status')}")
            continue

        ok_calls += 1
        offers_saved += len(data)
        print(f"[OK] ({idx}/{total_calls}) {origin}->{dest} {dep}/{ret} | offers: {len(data)}")

        # acumula candidatos para best por ID
        for offer in data:
            offers_by_route[rk].append({"offer": offer, "departure_date": dep, "return_date": ret})

            append_history_line(
                {
                    "run_id": rid,
                    "ts_utc": utc_now_iso(),
                    "route_id": r.get("id"),
                    "route_key": rk,
                    "origin": origin,
                    "destination": dest,
                    "departure_date": dep,
                    "return_date": ret,
                    "adults": adults,
                    "children": children,
                    "cabin": cabin,
                    "currency": currency,
                    "direct_only": direct_only,
                    "offer": offer,
                }
            )

    # best + alerts (por rota base/id)
    best_by_route, alerts = build_best_and_alerts(rid, routes_base, offers_by_route)

    # >>> garante que os arquivos sempre existam
    print(f"[INFO] Writing best offers: {BEST_FILE}")
    save_best(rid, best_by_route)
    print(f"[INFO] Writing alerts:     {ALERTS_FILE}")
    save_alerts(rid, alerts)

    finished = utc_now()
    duration_sec = int((finished - started).total_seconds())

    state = {
        "run_id": rid,
        "started_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_utc": finished.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_sec": duration_sec,
        "total_calls": total_calls,
        "ok_calls": ok_calls,
        "err_calls": err_calls,
        "success_rate": (ok_calls / total_calls) if total_calls else 0.0,
        "offers_saved": offers_saved,
        "store": "default",
        "max_results": max_results,
        "amadeus_env": amadeus_env,
        "immutable_required_origins": IMMUTABLE_REQUIRED_ORIGINS,
        "immutable_required_dests": IMMUTABLE_REQUIRED_DESTS,
    }
    save_state(state)

    sample_lines = []
    for rk, b in list(best_by_route.items())[:3]:
        if b and b.get("price_total") is not None:
            sample_lines.append(
                f'- {b["origin"]}->{b["destination"]} {b.get("departure_date")}/{b.get("return_date")} | '
                f'{b.get("carrier","?")} | BRL {b["price_total"]:.2f} | {b.get("stops", "?")} stop(s)'
            )

    summary = f"""# Flight Agent — Update Summary

- started_utc: `{state["started_utc"]}`
- finished_utc: `{state["finished_utc"]}`
- duration_sec: `{state["duration_sec"]}`
- total_calls: `{state["total_calls"]}`
- ok_calls: `{state["ok_calls"]}`
- err_calls: `{state["err_calls"]}`
- success_rate: `{state["success_rate"]:.3f}`
- offers_saved: `{state["offers_saved"]}`
- store: `{state["store"]}`
- max_results: `{state["max_results"]}`
- amadeus_env: `{state["amadeus_env"]}`
- immutable_required_origins: `{state["immutable_required_origins"]}`
- immutable_required_dests: `{state["immutable_required_dests"]}`

## Sample offers (preview)
{chr(10).join(sample_lines) if sample_lines else "- (no samples)"}

[OK] Run completed successfully.
"""
    save_summary_md(summary)
    print(summary)


if __name__ == "__main__":
    main()
