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


def build_trip_pairs(date_rule: Dict[str, Any]) -> List[Tuple[str, str]]:
    depart_start = parse_yyyy_mm_dd(date_rule["depart_start"])
    depart_end = parse_yyyy_mm_dd(date_rule["depart_end"])
    trip_len = int(date_rule["trip_length_days"])
    return_deadline = parse_yyyy_mm_dd(date_rule["return_deadline"])

    pairs = []
    for dep in daterange(depart_start, depart_end):
        ret = dep + timedelta(days=trip_len)
        if ret <= return_deadline:
            pairs.append((dep.isoformat(), ret.isoformat()))
    return pairs


def route_key(origin: str, destination: str, cfg: Dict[str, Any]) -> str:
    r = cfg.get("route") or {}
    dr = cfg.get("date_rule") or {}
    adults = int(r.get("adults", 1))
    children = int(r.get("children", 0))
    cabin = (r.get("cabin") or "ECONOMY").strip().upper()
    currency = "BRL"

    dep_start = dr.get("depart_start")
    ret_deadline = dr.get("return_deadline")
    return (
        f"{origin}-{destination}"
        f"|dep={dep_start}..{ret_deadline}"
        f"|ret<={ret_deadline}"
        f"|class={cabin}"
        f"|A{adults}|C{children}|{currency}"
    )


def pick_price_total(offer: Dict[str, Any]) -> Optional[float]:
    try:
        price_obj = offer.get("price") or {}
        val = price_obj.get("grandTotal")
        return float(val) if val is not None else None
    except Exception:
        return None


def run_for_destination(
    cfg: Dict[str, Any],
    origin: str,
    destination: str,
    direct_only: bool,
    adults: int,
    children: int,
    cabin: str,
    pairs: List[Tuple[str, str]],
    run_id: str,
    ts_utc: str,
) -> Tuple[Optional[float], Optional[str], Optional[str], Dict[str, float]]:
    """
    Retorna: (best_price, best_dep, best_ret, by_carrier_best)
    """
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
            currency="BRL",
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
    route = cfg.get("route") or {}
    date_rule = cfg.get("date_rule") or {}
    sources = cfg.get("sources") or ["amadeus"]
    use_amadeus = "amadeus" in [s.lower() for s in sources]

    origin = (route.get("origin") or "GRU").strip().upper()
    base_destination = (route.get("destination") or "FCO").strip().upper()

    # Aqui está a mudança principal: vamos sempre atualizar Roma (FCO/CIA).
    # Se você quiser manter flexível, dá pra ler do YAML depois.
    destinations_to_run = ["FCO", "CIA"] if base_destination in {"FCO", "CIA"} else [base_destination]

    direct_only = bool(route.get("direct_only", True))
    adults = int(route.get("adults", 1))
    children = int(route.get("children", 0))
    cabin = (route.get("cabin") or "ECONOMY").strip().upper()

    pairs = build_trip_pairs(date_rule)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ts_utc = datetime.now(timezone.utc).isoformat()

    print("=======================================")
    print("RUN_ID:", run_id)
    print("TS_UTC:", ts_utc)
    print("origin:", origin)
    print("destinations_to_run:", destinations_to_run)
    print("direct_only:", direct_only, "| adults:", adults, "| children:", children, "| cabin:", cabin)
    print("pairs_count:", len(pairs))
    print("sources:", sources)
    print("=======================================")

    if not use_amadeus:
        print("[INFO] 'amadeus' não está em sources. Nada a fazer.")
        return

    state = load_json(STATE_PATH, default={"best": {}, "meta": {"previous_run_id": None, "latest_run_id": None}})
    best_map = state.get("best", {}) if isinstance(state.get("best"), dict) else {}
    meta = state.get("meta", {}) if isinstance(state.get("meta"), dict) else {}
    prev_latest = meta.get("latest_run_id")

    # Para o summary
    headline_lines = []

    for destination in destinations_to_run:
        best_price_run, best_dep_run, best_ret_run, by_carrier_best = run_for_destination(
            cfg=cfg,
            origin=origin,
            destination=destination,
            direct_only=direct_only,
            adults=adults,
            children=children,
            cabin=cabin,
            pairs=pairs,
            run_id=run_id,
            ts_utc=ts_utc,
        )

        k = route_key(origin, destination, cfg)

        if best_price_run is None:
            best_map[k] = {
                "price_total": None,
                "price": float("inf"),  # app antigo pode cair aqui; vamos melhorar no app depois
                "currency": "BRL",
                "run_id": run_id,
                "ts_utc": ts_utc,
                "destination": destination,
                "by_carrier": {},  # garante que existe
                "summary": f"{origin}→{destination} no offers found dep={date_rule.get('depart_start')}..{date_rule.get('return_deadline')}",
            }
            headline_lines.append(f"- {origin}→{destination}: **N/A** (sem ofertas)")
        else:
            best_map[k] = {
                "price_total": best_price_run,
                "price": best_price_run,
                "currency": "BRL",
                "run_id": run_id,
                "ts_utc": ts_utc,
                "best_dep": best_dep_run,
                "best_ret": best_ret_run,
                "destination": destination,
                "by_carrier": by_carrier_best,
                "summary": f"{origin}→{destination} best_dep={best_dep_run} best_ret={best_ret_run} cabin={cabin} A={adults} C={children}",
            }
            headline_lines.append(f"- {origin}→{destination}: **BRL {best_price_run:,.2f}** ({best_dep_run} → {best_ret_run})")

    meta["previous_run_id"] = prev_latest
    meta["latest_run_id"] = run_id
    save_json(STATE_PATH, {"best": best_map, "meta": meta})

    updated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    summary_md = f"""# Flight Agent — Daily Summary

- Updated: **{updated}**
- Latest run_id: `{run_id}`
- Previous run_id: `{prev_latest or "—"}`

## Headline — {origin} → Roma (FCO/CIA)

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
