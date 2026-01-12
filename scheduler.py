# === alerts_and_best.py (pode ficar dentro do scheduler.py) ===
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Any, Dict, List, Optional, Tuple


DATA_DIR = Path("data")
BEST_FILE = DATA_DIR / "best_offers.json"
ALERTS_FILE = DATA_DIR / "alerts.json"
STATE_FILE = DATA_DIR / "state.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def route_key(route: Dict[str, Any]) -> str:
    return f'{route["origin"]}-{route["destination"]}:{route["departure_date"]}:{route["return_date"]}:{route.get("cabin","")}'


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def normalize_offer(offer: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ajuste aqui conforme o seu schema de offer.
    Esperado no retorno:
      price_total (float), carrier (str), stops (int), raw (dict)
    """
    # EXEMPLOS de caminhos comuns (ajuste conforme seu payload real):
    price = (
        safe_float(offer.get("price_total"))
        or safe_float(offer.get("total_price"))
        or safe_float(offer.get("price"))
        or safe_float(offer.get("total"))
    )

    carrier = (
        offer.get("carrier")
        or offer.get("validating_airline")
        or offer.get("airline")
        or "?"
    )

    stops = offer.get("stops")
    if stops is None:
        # se você tiver segments/itineraries, calcula aqui
        stops = offer.get("number_of_stops")
    try:
        stops = int(stops) if stops is not None else 99
    except Exception:
        stops = 99

    return {
        "price_total": price,
        "carrier": carrier,
        "stops": stops,
        "raw": offer,
    }


def pick_best_offer(offers: List[Dict[str, Any]], watch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    norm = [normalize_offer(o) for o in offers]
    norm = [o for o in norm if o["price_total"] is not None]

    # filtros opcionais
    max_stops = watch.get("max_stops")
    if max_stops is not None:
        try:
            ms = int(max_stops)
            norm = [o for o in norm if o["stops"] <= ms]
        except Exception:
            pass

    prefer_airlines = set(watch.get("prefer_airlines") or [])
    if prefer_airlines:
        preferred = [o for o in norm if o["carrier"] in prefer_airlines]
        if preferred:
            norm = preferred

    if not norm:
        return None

    norm.sort(key=lambda x: x["price_total"])
    return norm[0]


def load_prev_best() -> Dict[str, Any]:
    if not BEST_FILE.exists():
        return {"by_route": {}}
    return json.loads(BEST_FILE.read_text(encoding="utf-8"))


def save_best(run_id: str, best_by_route: Dict[str, Any]) -> None:
    payload = {
        "run_id": run_id,
        "updated_utc": utc_now_iso(),
        "by_route": best_by_route,  # route_key -> best offer payload
    }
    BEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    BEST_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def save_alerts(run_id: str, alerts: List[Dict[str, Any]]) -> None:
    payload = {
        "run_id": run_id,
        "updated_utc": utc_now_iso(),
        "alerts": alerts,
    }
    ALERTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    ALERTS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_alerts(
    run_id: str,
    routes: List[Dict[str, Any]],
    offers_by_route: Dict[str, List[Dict[str, Any]]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    prev_best = load_prev_best().get("by_route", {})
    best_by_route: Dict[str, Any] = {}
    alerts: List[Dict[str, Any]] = []

    for r in routes:
        rk = route_key(r)
        watch = r.get("watch") or {}
        best = pick_best_offer(offers_by_route.get(rk, []), watch)

        if best is None:
            best_by_route[rk] = {
                "origin": r["origin"],
                "destination": r["destination"],
                "departure_date": r["departure_date"],
                "return_date": r["return_date"],
                "best": None,
                "note": "no_offers_after_filters",
            }
            continue

        # best offer “sempre”
        best_payload = {
            "origin": r["origin"],
            "destination": r["destination"],
            "departure_date": r["departure_date"],
            "return_date": r["return_date"],
            "carrier": best["carrier"],
            "stops": best["stops"],
            "price_total": best["price_total"],
        }
        best_by_route[rk] = best_payload

        # Alertas
        target = safe_float(watch.get("target_price_total"))
        if target is not None and best["price_total"] <= target:
            alerts.append({
                "type": "TARGET_PRICE",
                "route_key": rk,
                "message": f'Alvo atingido: {r["origin"]}->{r["destination"]} <= {target:.2f}',
                "current_price": best["price_total"],
                "target_price": target,
                "carrier": best["carrier"],
                "stops": best["stops"],
            })

        prev = prev_best.get(rk, {})
        prev_price = safe_float(prev.get("price_total"))
        drop_pct = safe_float(watch.get("alert_drop_pct"))
        if prev_price and drop_pct:
            delta_pct = (prev_price - best["price_total"]) / prev_price * 100.0
            if delta_pct >= drop_pct:
                alerts.append({
                    "type": "DROP_PCT",
                    "route_key": rk,
                    "message": f'Queda {delta_pct:.1f}%: {r["origin"]}->{r["destination"]}',
                    "current_price": best["price_total"],
                    "prev_best_price": prev_price,
                    "delta_pct": delta_pct,
                    "carrier": best["carrier"],
                    "stops": best["stops"],
                })

    return best_by_route, alerts
