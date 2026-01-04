# search.py
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


def _stable_key(route: dict[str, Any]) -> str:
    origin = str(route.get("origin", "")).upper()
    dest = str(route.get("destination", "")).upper()

    dep = route.get("departure_window", {}) or {}
    dep_from = str(dep.get("from", ""))
    dep_to = str(dep.get("to", ""))

    return_by = str(route.get("return_by", ""))
    cabin = str(route.get("cabin", "ECONOMY") or "ECONOMY").upper()

    adults = int(route.get("adults", 1) or 1)
    children = int(route.get("children", 0) or 0)

    raw = (
        f"{origin}-{dest}|{dep_from}:{dep_to}|return_by:{return_by}|{cabin}"
        f"|adults:{adults}|children:{children}"
    )
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    return f"{origin}-{dest}-{dep_from}-{dep_to}-{return_by}-{cabin}-A{adults}-C{children}-{h}"


def run_search(profile: dict[str, Any]) -> list[dict[str, Any]]:
    routes = profile.get("routes", [])
    if not isinstance(routes, list) or not routes:
        raise ValueError("Profile must contain a non-empty 'routes' list")

    day_seed = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    results: list[dict[str, Any]] = []
    for route in routes:
        key = _stable_key(route)

        seed = (key + "|" + day_seed).encode("utf-8")
        h = hashlib.sha1(seed).hexdigest()
        base = int(h[:6], 16)
        price = 1500 + (base % 4500)  # 1500..5999 (mock)

        origin = str(route.get("origin", "")).upper()
        dest = str(route.get("destination", "")).upper()
        dep = route.get("departure_window", {}) or {}

        results.append(
            {
                "key": key,
                "price": float(price),
                "currency": str(route.get("currency", "BRL")),
                "summary": (
                    f"{origin}→{dest} "
                    f"{dep.get('from','')}..{dep.get('to','')} "
                    f"return_by={route.get('return_by','')} "
                    f"cabin={route.get('cabin','ECONOMY')} "
                    f"adults={route.get('adults',1)} "
                    f"children={route.get('children',0)}"
                ),
                "deeplink": "",
            }
        )

    return results
