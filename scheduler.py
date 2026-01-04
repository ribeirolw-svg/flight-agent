# backend/scheduler.py
"""
Scheduler entrypoint: runs weekly search and triggers notification if price improves.

How it works:
- Calls search.run_search() to fetch current best options for saved routes/criteria.
- Calls storage to load previous best price per route.
- If improvement >= threshold -> notifier sends alert.
- Always stores run results for history.

This file is designed to run in GitHub Actions (no daemon).
"""

from __future__ import annotations

import os
import sys
import json
from datetime import datetime, timezone

# Local imports (expected to exist in your backend/)
from search import run_search  # you already have/terá o search.py
from storage import (
    load_state,
    save_state,
    append_history,
)
from notifier import notify_price_drop


def _env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None or val == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def _env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if val is None or val.strip() == "":
        return default
    try:
        return float(val)
    except ValueError as e:
        raise RuntimeError(f"Env var {name} must be a float. Got: {val}") from e


def main() -> int:
    # Threshold: notify only if current best price <= (1 - threshold) * previous_best
    threshold_pct = _env_float("ALERT_DROP_PCT", 0.10)  # default 10%

    # Optional: for logging/metadata
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # Load saved search criteria (JSON) from env or fallback file
    # You can keep it in env (GHA secrets) as SEARCH_PROFILE_JSON
    profile_json = os.getenv("SEARCH_PROFILE_JSON", "").strip()
    profile_path = os.getenv("SEARCH_PROFILE_PATH", "backend/search_profile.json").strip()

    if profile_json:
        try:
            profile = json.loads(profile_json)
        except json.JSONDecodeError as e:
            raise RuntimeError("SEARCH_PROFILE_JSON is not valid JSON") from e
    else:
        if not os.path.exists(profile_path):
            raise RuntimeError(
                "No search profile found. Provide SEARCH_PROFILE_JSON env var "
                f"or create file at: {profile_path}"
            )
        with open(profile_path, "r", encoding="utf-8") as f:
            profile = json.load(f)

    # Load prior state (best prices per route/query)
    state = load_state()

    # Run search (Amadeus)
    # Expected run_search(profile) -> list[dict] results with keys:
    # - "key": stable identifier for the route/query (e.g. "GRU-JFK-2026-09-flex")
    # - "price": numeric
    # - "currency": "BRL"/"USD"/etc
    # - "summary": human friendly (optional)
    results = run_search(profile)

    # Save history always
    append_history(run_id=run_id, profile=profile, results=results)

    # Compare to previous best and notify if improved
    alerts = []
    for r in results:
        key = r.get("key")
        price = r.get("price")
        currency = r.get("currency", "")
        if key is None or price is None:
            continue

        prev_best = state.get("best", {}).get(key)
        improved = False

        if prev_best is None:
            # first time: store as best (no alert by default)
            improved = True
            reason = "primeiro registro"
        else:
            try:
                prev_price = float(prev_best["price"])
            except Exception:
                prev_price = float(prev_best)

            # improvement if price dropped enough
            improved = price <= (1.0 - threshold_pct) * prev_price
            reason = f"queda >= {threshold_pct:.0%}"

        # Update best if new price is lower than stored (even if not enough to alert)
        should_update_best = prev_best is None or price < float(prev_best["price"])
        if should_update_best:
            state.setdefault("best", {})[key] = {
                "price": float(price),
                "currency": currency,
                "run_id": run_id,
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "summary": r.get("summary", ""),
            }

        if improved and prev_best is not None:
            alerts.append(
                {
                    "key": key,
                    "previous": prev_best,
                    "current": {
                        "price": float(price),
                        "currency": currency,
                        "summary": r.get("summary", ""),
                        "deeplink": r.get("deeplink", ""),
                    },
                    "reason": reason,
                }
            )

    # Persist state
    save_state(state)

    # Notify if needed
    if alerts:
        notify_price_drop(run_id=run_id, alerts=alerts, profile=profile)
        print(f"[{run_id}] Alerts sent: {len(alerts)}")
    else:
        print(f"[{run_id}] No alerts. Search completed.")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"Scheduler failed: {e}", file=sys.stderr)
        raise
