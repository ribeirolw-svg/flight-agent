# search.py
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


def _stable_key(route: dict[str, Any]) -> str:
    """
    Generates a stable identifier for a route/search criterion so we can
    track best prices over time.
    """
    origin = route.get("origin", "").upper()
    dest = route.get("destination", "").upper()
    dep = route.get("departure_window", {}) or {}
    dep_from = dep.get("from", "")
    dep_to = dep.get("to", "")
    return_by = route.get("return_by", "")
    cabin = (route.get("cabin", "ECONOMY") or "ECONOMY").upper()
    adults = int(route.get("adults", 1) or 1)

    raw = f"{origin}-{dest}|{dep_from}:{dep_to}|return_by:{return_by}|{cabin}|adults:{adults}"
    # compact hash to avoid huge keys
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    return f"{origin}-{dest}-{dep_from}-{dep_to}-{return_by}-{cabin}-A{adults}-{h}"


def run_search(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Minimal implementation to validate the weekly scheduler pipeline.
    Replace the 'price' logic with Amadeus API calls later.

    Expected profile structure:
    {
      "routes": [
        {
          "origin": "GRU",
          "destination": "JFK",
          "departure_window": {"from":"2026-09-01","to":"2026-09-30"},
          "return_by": "2026-10-15",
          "cabin": "ECONOMY",
          "adults": 1
        }
      ]
    }
    """
    routes = profile.get("routes", [])
    if not isinstance(routes, list) or not routes:
        raise ValueError("Profile must contain a non-empty 'routes' list")

    # Deterministic pseudo-price: stable per day + route (so you see changes over weeks)
    # This is just to validate automation; swap to real Amadeus pricing later.
    day_seed = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    results: list[dict[str, Any]] = []
    for r in routes:
        key = _stable_key(r)

        # pseudo price based on hash(route+day)
        seed = (key + "|" + day_seed).encode("utf-8")
        h = hashlib.sha1(seed).hexdigest()
        base = int(h[:6], 16)  # 0..16M
        price = 1500 + (base % 4500)  # 1500..5999

        results.append(
            {
                "key": key,
                "price": float(price),
                "currency": r.get("currency", "BRL"),
                "summary": f"{r.get('origin','').upper()}→{r.get('destination','').upper()} "
                           f"{r.get('departure_window',{}).get('from','')}..{r.get('departure_window',{}).get('to','')} "
                           f"return_by={r.get('return_by','')} cabin={r.get('cabin','ECONOMY')}",
                "deeplink": "",
            }
        )

    return results
