import os
import json
import yaml
from datetime import datetime, timezone, date, timedelta
from typing import Dict, Any, List, Tuple, Optional

from search import search_flights

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(PROJECT_DIR, "routes.yaml")

DATA_DIR = os.getenv("DATA_DIR", os.path.join(PROJECT_DIR, "data"))
os.makedirs(DATA_DIR, exist_ok=True)

STATE_PATH = os.path.join(DATA_DIR, "state.json")
SUMMARY_PATH = os.path.join(DATA_DIR, "summary.md")
HISTORY_PATH = os.path.join(DATA_DIR, "history.jsonl")

DOW = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}


# -----------------------------
# IO helpers
# -----------------------------
def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def append_jsonl(path: str, obj: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


# -----------------------------
# date utils
# -----------------------------
def parse_yyyy_mm_dd(s: str) -> date:
    y, m, d = [int(x) for x in s.split("-")]
    return date(y, m, d)


def daterange(start: date, end: date) -> List[date]:
    out = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


# -----------------------------
# offer utils
# -----------------------------
def pick_price_total(offer: Dict[str, Any]) -> Optional[float]:
    try:
        price_obj = offer.get("price") or {}
        val = price_obj.get("grandTotal")
        return float(val) if val is not None else None
    except Exception:
        return None


# -----------------------------
# Date rules
# -----------------------------
def build_pairs_fixed(rule: Dict[str, Any]) -> List[Tuple[str, str]]:
    depart_start = parse_yyyy_mm_dd(rule["depart_start"])
    depart_end = parse_yyyy_mm_dd(rule["depart_end"])
    trip_len = int(rule["trip_length_days"])
    return_deadline = parse_yyyy_mm_dd(rule["return_deadline"])

    pairs = []
    for dep in daterange(depart_start, depart_end):
        ret = dep + timedelta(days=trip_len)
        if ret <= return_deadline:
            pairs.append((dep.isoformat(), ret.isoformat()))
    return pairs


def build_pairs_rolling_weekend(rule: Dict[str, Any], today: date) -> List[Tuple[str, str]]:
    lookahead_days = int(rule.get("lookahead_days", 30))
    dep_dows = [DOW[x] for x in (rule.get("depart_dows") or ["FRI", "SAT"])]
    ret_dows = [DOW[x] for x in (rule.get("return_dows") or ["SUN", "MON"])]

    # Opcional: limitar duração
    min_stay = int(rule.get("min_stay_days", 1))
    max_stay = int(rule.get("max_stay_days", 7))

    start = today
    end = today + timedelta(days=lookahead_days)

    pairs: List[Tuple[str, str]] = []
    for dep in daterange(start, end):
        if dep.weekday() not in dep_dows:
            continue

        for delta in range(min_stay, max_stay + 1):
            ret = dep + timedelta(days=delta)
            if ret > end:
                continue
            if ret.weekday() in ret_dows:
                pairs.append((dep.isoformat(), ret.isoformat()))

    # evita explosão
    max_pairs = int(rule.get("max_pairs", 120))
    return pairs[:max_pairs]


def build_pairs_for_route(route_cfg: Dict[str, Any], today: date) -> List[Tuple[str, str]]:
    rule = route_cfg.get("date_rule") or {}
    rtype = (rule.get("type") or "fixed").strip().lower()

    if rtype == "fixed":
        return build_pairs_fixed(rule)
    if rtype in {"rolling_weekend", "rolling"}:
        return build_pairs_rolling_weekend(rule, today)

    raise ValueError(f"date_rule.type inválido: {rule.get('type')}")


# -----------------------------
# Config merge (defaults + route)
# -----------------------------
def merged_int(route_cfg: Dict[str, Any], defaults: Dict[str, Any], key: str, fallback: int) -> int:
    v = route_cfg.get(key, defaults.get(key, fallback))
    try:
        return int(v)
    except Exception:
        return fallback


def merged_str(route_cfg: Dict[str, Any], defaults: Dict[str, Any], key: str, fallback: str) -> str:
    v = route_cfg.get(key, defaults.get(key, fallback))
    return (str(v) if v is not None else fallback).strip()


def merged_bool(route_cfg: Dict[str, Any], defaults: Dict[str, Any], key: str, fallback: bool) -> bool:
    v = route_cfg.get(key, defaults.get(key, fallback))
    return bool(v) if v is not None else fallback


def merged_list(route_cfg: Dict[str, Any], defaults: Dict[str, Any], key: str) -> List[Any]:
    v = route_cfg.get(key, defaults.get(key, []))
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


# -----------------------------
# Keys & state
# -----------------------------
def route_key(route_name: str, origin: str, destination: str, cabin: str, adults: int, children: int, currency: str, rule_type: str) -> str:
    return f"{route_name}|{origin}-{destination}|{rule_type}|class={cabin}|A{adults}|C{children}|{currency}"


def run_for_destination(
    origin: str,
    destination: str,
    direct_only: bool,
    adults: int,
    children: int,
    cabin: str,
    currency: str,
    pairs: List[Tuple[str, str]],
    run_id: str,
    ts_utc: str,
) -> Tuple[Optional[float], Optional[str], Optional[str], Dict[str, float]]:
    best_price_run: Optional[float] = None
    best_dep_run: Optional[str] = None
    best_ret_run: Optional[str] = None
    by_carrier_best: Dict[str, float] = {}

    max_results = int(os.getenv("AMADEUS_MAX_RESULTS", "10"))

    for dep, ret in pairs:
        raw = search_flights(
            origin=origin,
            destination=destination,
            departure_date=dep,
            return_date=ret,
            adults=adults,
            children=children,
            travel_class=cabin,
            currency=currency,
            nonstop=direct_only,
            max_results=max_results,
        )

        offers = raw.get("data", []) or []

        append_jsonl(HISTORY_PATH, {
            "run_id": run_id,
            "ts_utc": ts_utc,
            "origin": origin,
            "destination": destination,
            "departure_date": dep,
            "return_date": ret,
            "offers_count": len(offers),
            "adults": adults,
            "children": children,
            "cabin": cabin,
            "currency": currency,
            "direct_only": direct_only,
        })

        for offer in offers:
            price = pick_price_total(offer)
            if price is None:
                continue

            validating = offer.get("validatingAirlineCodes")
            carrier = validating[0] if isinstance(validating, list) and validating else "—"

            if carrier != "—":
                cur = by_carrier_best.get(carrier)
                if cur is None or price < cur:
                    by_carrier_best[carrier] = price

            if best_price_run is None or price < best_price_run:
                best_price_run = price
                best_dep_run = dep
                best_ret_run = ret

        print(f"[OK] {origin}->{destination} {dep}->{ret} offers={len(offers)}")

    return best_price_run, best_dep_run, best_ret_run, by_carrier_best


def main() -> None:
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Não achei routes.yaml em: {CONFIG_FILE}")

    cfg = load_yaml(CONFIG_FILE)

    sources = cfg.get("sources") or ["amadeus"]
    use_amadeus = "amadeus" in [s.lower() for s in sources]
    if not use_amadeus:
        print("[INFO] 'amadeus' não está em sources. Nada a fazer.")
        return

    routes = cfg.get("routes") or []
    if not isinstance(routes, list) or not routes:
        raise ValueError("routes.yaml precisa ter 'routes:' como lista não-vazia")

    defaults = cfg.get("defaults") or {}
    origin_domestic = (cfg.get("origin_domestic") or "CGH").strip().upper()

    state = load_json(STATE_PATH, default={"best": {}, "meta": {"previous_run_id": None, "latest_run_id": None}})
    best_map = state.get("best", {}) if isinstance(state.get("best"), dict) else {}
    meta = state.get("meta", {}) if isinstance(state.get("meta"), dict) else {}
    prev_latest = meta.get("latest_run_id")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ts_utc = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).date()

    print("=======================================")
    print("RUN_ID:", run_id)
    print("TS_UTC:", ts_utc)
    print("routes_count:", len(routes))
    print("origin_domestic:", origin_domestic)
    print("defaults:", defaults)
    print("sources:", sources)
    print("=======================================")

    headline_lines: List[str] = []

    for route_cfg in routes:
        route_name = merged_str(route_cfg, defaults, "name", "Route")
        is_domestic = bool(route_cfg.get("domestic", False))

        # Origem: domestic usa origin_domestic; internacional usa origin explícito (ou default GRU)
        origin = origin_domestic if is_domestic else merged_str(route_cfg, defaults, "origin", "GRU").upper()
        origin = origin.strip().upper()

        destinations = merged_list(route_cfg, defaults, "destinations")
        destinations = [str(d).strip().upper() for d in destinations if str(d).strip()]

        direct_only = merged_bool(route_cfg, defaults, "direct_only", True)
        adults = merged_int(route_cfg, defaults, "adults", 1)
        children = merged_int(route_cfg, defaults, "children", 0)
        cabin = merged_str(route_cfg, defaults, "cabin", "ECONOMY").upper()
        currency = merged_str(route_cfg, defaults, "currency", "BRL").upper()

        # child ages: metadado (endpoint não usa), mas guardamos no summary/state
        children_ages = merged_list(route_cfg, defaults, "children_ages")

        pairs = build_pairs_for_route(route_cfg, today=today)

        rule = route_cfg.get("date_rule") or {}
        rule_type = (rule.get("type") or "fixed").strip().upper()

        print("---------------------------------------")
        print("ROUTE:", route_name)
        print("origin:", origin, "| destinations:", destinations, "| domestic:", is_domestic)
        print("pax:", f"A{adults} C{children}", "| children_ages:", children_ages, "| cabin:", cabin)
        print("rule_type:", rule_type, "| pairs_count:", len(pairs))
        print("---------------------------------------")

        for destination in destinations:
            best_price, best_dep, best_ret, by_carrier = run_for_destination(
                origin=origin,
                destination=destination,
                direct_only=direct_only,
                adults=adults,
                children=children,
                cabin=cabin,
                currency=currency,
                pairs=pairs,
                run_id=run_id,
                ts_utc=ts_utc,
            )

            k = route_key(
                route_name=route_name,
                origin=origin,
                destination=destination,
                cabin=cabin,
                adults=adults,
                children=children,
                currency=currency,
                rule_type=rule_type,
            )

            if best_price is None:
                best_map[k] = {
                    "price_total": None,
                    "price": float("inf"),
                    "currency": currency,
                    "run_id": run_id,
                    "ts_utc": ts_utc,
                    "best_dep": None,
                    "best_ret": None,
                    "origin": origin,
                    "destination": destination,
                    "by_carrier": {},
                    "pax": {"adults": adults, "children": children, "children_ages": children_ages},
                    "summary": f"{route_name}: {origin}→{destination} sem ofertas (A={adults} C={children})",
                }
                headline_lines.append(f"- {route_name} — {origin}→{destination}: **N/A** (A{adults} C{children})")
            else:
                best_map[k] = {
                    "price_total": best_price,
                    "price": best_price,
                    "currency": currency,
                    "run_id": run_id,
                    "ts_utc": ts_utc,
                    "best_dep": best_dep,
                    "best_ret": best_ret,
                    "origin": origin,
                    "destination": destination,
                    "by_carrier": by_carrier,
                    "pax": {"adults": adults, "children": children, "children_ages": children_ages},
                    "summary": f"{route_name}: {origin}→{destination} best_dep={best_dep} best_ret={best_ret} cabin={cabin} A={adults} C={children}",
                }
                headline_lines.append(
                    f"- {route_name} — {origin}→{destination}: **{currency} {best_price:,.2f}** ({best_dep} → {best_ret}) (A{adults} C{children})"
                )

    meta["previous_run_id"] = prev_latest
    meta["latest_run_id"] = run_id

    save_json(STATE_PATH, {"best": best_map, "meta": meta})

    updated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    summary_md = f"""# Flight Agent — Daily Summary

- Updated: **{updated}**
- Latest run_id: `{run_id}`
- Previous run_id: `{prev_latest or "—"}`

## Headline

{chr(10).join(headline_lines)}
"""
    with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
        f.write(summary_md)

    print("=======================================")
    print("FINALIZADO")
    print("state.json atualizado:", STATE_PATH)
    print("summary.md atualizado:", SUMMARY_PATH)
    print("history.jsonl atualizado:", HISTORY_PATH)
    print("=======================================")


if __name__ == "__main__":
    main()
