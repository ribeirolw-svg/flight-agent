from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from search import run_search

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
STATE_PATH = DATA_DIR / "state.json"
HISTORY_PATH = DATA_DIR / "history.jsonl"
SUMMARY_PATH = DATA_DIR / "summary.md"


# ----------------------------
# Utils
# ----------------------------
def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, obj: Any) -> None:
    _ensure_data_dir()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _append_jsonl(path: Path, obj: Any) -> None:
    _ensure_data_dir()
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _fmt_money(v: Optional[float], currency: str) -> str:
    if v is None:
        return "N/A"
    return f"{currency} {v:,.2f}"


def _now_utc_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")


def _run_id() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


# ----------------------------
# State handling
# ----------------------------
def load_state() -> Dict[str, Any]:
    return _load_json(STATE_PATH, {"best": {}, "last_run_id": None})


def save_state(state: Dict[str, Any]) -> None:
    _save_json(STATE_PATH, state)


def append_history(run_id: str, profile: Dict[str, Any], results: List[Dict[str, Any]]) -> None:
    record = {"run_id": run_id, "profile": profile, "results": results}
    _append_jsonl(HISTORY_PATH, record)


# ----------------------------
# Best-state update
# ----------------------------
def _pick_price_for_compare(r: Dict[str, Any]) -> Optional[float]:
    """
    Prefer total (com taxas) -> fallback price -> fallback base
    """
    v = _to_float(r.get("price_total"))
    if v is not None:
        return v
    v = _to_float(r.get("price"))
    if v is not None:
        return v
    return _to_float(r.get("price_base"))


def update_best_state_rich(prev_state: Dict[str, Any], results: List[Dict[str, Any]]) -> Dict[str, Any]:
    best_map = dict(prev_state.get("best", {}))

    for r in results:
        key = r["key"]
        new_price = _pick_price_for_compare(r)

        prev = best_map.get(key)
        prev_price = None
        if isinstance(prev, dict):
            prev_price = _to_float(prev.get("price_total"))
            if prev_price is None:
                prev_price = _to_float(prev.get("price"))
            if prev_price is None:
                prev_price = _to_float(prev.get("price_base"))

        is_better = False
        if prev is None:
            is_better = True
        elif new_price is not None and prev_price is not None and new_price < prev_price:
            is_better = True
        elif prev_price is None and new_price is not None:
            is_better = True

        if is_better:
            best_map[key] = {
                "price": r.get("price"),
                "price_base": r.get("price_base"),
                "price_total": r.get("price_total"),
                "currency": r.get("currency"),
                "summary": r.get("summary", "") or "",
                "origin": r.get("origin"),
                "destination": r.get("destination"),
                "best_dep": r.get("best_dep"),
                "best_ret": r.get("best_ret"),
                "by_carrier": r.get("by_carrier", {}) or {},
            }

    return {
        "best": best_map,
        "last_run_id": prev_state.get("last_run_id"),
    }


# ----------------------------
# Summary
# ----------------------------
def _extract_rome_rows(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in results:
        if r.get("destination") in ("FCO", "CIA"):
            out.append(r)
    return out


def _best_rome_this_run(rome_rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    best = None
    best_price = None

    for r in rome_rows:
        p = _pick_price_for_compare(r)
        if p is None:
            continue
        if best is None or (best_price is not None and p < best_price):
            best = r
            best_price = p

    return best


def _render_summary(
    run_id: str,
    prev_run_id: Optional[str],
    state: Dict[str, Any],
    results: List[Dict[str, Any]],
) -> str:
    lines: List[str] = []

    lines.append("# Flight Agent — Weekly Summary")
    lines.append(f"Updated: {_now_utc_str()}")
    lines.append(f"Latest run_id: {run_id}")
    lines.append(f"Previous run_id: {prev_run_id}")
    lines.append("")

    rome_rows = _extract_rome_rows(results)
    best_rome = _best_rome_this_run(rome_rows)

    lines.append("## Headline — São Paulo → Roma (FCO/CIA)")

    if best_rome is None:
        lines.append("No Rome results found in latest run.")
    else:
        currency = best_rome.get("currency", "")
        best_price = _pick_price_for_compare(best_rome)
        lines.append(f"Best this run: {best_rome.get('origin')}→{best_rome.get('destination')} — {_fmt_money(best_price, currency)}")
        lines.append(f"Dates: depart {best_rome.get('best_dep')} · return {best_rome.get('best_ret')}")
        lines.append(f"Key: {best_rome.get('key')}")
    lines.append("")

    # By airline (global Rome)
    by_carrier: Dict[str, float] = {}
    for r in rome_rows:
        bc = r.get("by_carrier") or {}
        for k, v in bc.items():
            try:
                fv = float(v)
            except Exception:
                continue
            cur = by_carrier.get(k)
            if cur is None or fv < cur:
                by_carrier[k] = fv

    if by_carrier:
        lines.append("## Roma — by Airline (Top 5)")
        lines.append("Airline\tBest Price")
        for carrier, price in sorted(by_carrier.items(), key=lambda x: x[1])[:5]:
            currency = best_rome.get("currency", "") if best_rome else ""
            lines.append(f"{carrier}\t{_fmt_money(price, currency)}")
        lines.append("")

    # Current best
    lines.append("## Current Best (from state.json)")
    best_map = state.get("best", {})

    if not best_map:
        lines.append("No best prices recorded yet.")
    else:
        lines.append("Route Key\tBest Price\tNotes")
        for k, v in best_map.items():
            currency = v.get("currency", "")
            p = _to_float(v.get("price_total"))
            if p is None:
                p = _to_float(v.get("price"))
            if p is None:
                p = _to_float(v.get("price_base"))
            lines.append(f"{k}\t{_fmt_money(p, currency)}\t{v.get('summary', '')}")
    lines.append("")

    # Snapshot
    lines.append("## Latest Run — Snapshot")
    if not results:
        lines.append("No snapshot rows available yet.")
    else:
        lines.append("Route Key\tThis Run Best")
        for r in results:
            currency = r.get("currency", "")
            p = _pick_price_for_compare(r)
            lines.append(f"{r.get('key')}\t{_fmt_money(p, currency)}")

    return "\n".join(lines)


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    try:
        profile_json = os.getenv("SEARCH_PROFILE_JSON")
        if profile_json:
            profile = json.loads(profile_json)
        else:
            # fallback file
            p1 = Path("backend/search_profile.json")
            p2 = Path("search_profile.json")
            if p1.exists():
                profile = _load_json(p1, {})
            elif p2.exists():
                profile = _load_json(p2, {})
            else:
                raise RuntimeError("Nenhum profile encontrado (SEARCH_PROFILE_JSON ou backend/search_profile.json).")

        results = run_search(profile)

        run_id = _run_id()
        prev_state = load_state()
        prev_run_id = prev_state.get("last_run_id")

        new_state = update_best_state_rich(prev_state, results)
        new_state["last_run_id"] = run_id

        save_state(new_state)
        append_history(run_id, profile, results)

        summary = _render_summary(run_id, prev_run_id, new_state, results)
        _ensure_data_dir()
        with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
            f.write(summary)

        print(summary)
        return 0

    except Exception as e:
        print(f"Scheduler failed: {e}", file=sys.stderr)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
