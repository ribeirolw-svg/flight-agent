# report.py
from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

DATA_DIR = Path("data")
STATE_PATH = DATA_DIR / "state.json"
HISTORY_PATH = DATA_DIR / "history.jsonl"
SUMMARY_PATH = DATA_DIR / "summary.md"


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _read_history_last(n: int = 2) -> List[Dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return []
    lines = HISTORY_PATH.read_text(encoding="utf-8").splitlines()
    tail = lines[-n:] if len(lines) >= n else lines
    out = []
    for line in tail:
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            pass
    return out


def _fmt_money(price: float, currency: str) -> str:
    if price == float("inf"):
        return "N/A"
    return f"{currency} {price:,.2f}"


def _extract_best_from_results(results: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    # Map key -> best result (min price) inside one run
    best: Dict[str, Dict[str, Any]] = {}
    for r in results or []:
        key = r.get("key")
        price = r.get("price")
        if key is None or price is None:
            continue
        try:
            p = float(price)
        except Exception:
            continue
        if key not in best or p < float(best[key].get("price", float("inf"))):
            best[key] = r
    return best


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    state = _read_json(STATE_PATH)
    best_map: Dict[str, Any] = state.get("best", {}) if isinstance(state.get("best", {}), dict) else {}

    history = _read_history_last(2)
    curr_run = history[-1] if len(history) >= 1 else None
    prev_run = history[-2] if len(history) >= 2 else None

    curr_best = _extract_best_from_results(curr_run.get("results", []) if curr_run else [])
    prev_best = _extract_best_from_results(prev_run.get("results", []) if prev_run else [])

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    md = []
    md.append("# Flight Agent — Weekly Summary")
    md.append("")
    md.append(f"- Updated: **{now}**")
    if curr_run:
        md.append(f"- Latest run_id: `{curr_run.get('run_id','')}`")
    if prev_run:
        md.append(f"- Previous run_id: `{prev_run.get('run_id','')}`")
    md.append("")

    md.append("## Current Best (from state.json)")
    md.append("")
    if not best_map:
        md.append("_No best prices recorded yet._")
    else:
        md.append("| Route Key | Best Price | Notes |")
        md.append("|---|---:|---|")
        for key, info in best_map.items():
            try:
                price = float(info.get("price", float("inf")))
            except Exception:
                price = float("inf")
            currency = str(info.get("currency", "") or "")
            summary = str(info.get("summary", "") or "")
            md.append(f"| `{key}` | {_fmt_money(price, currency)} | {summary} |")
    md.append("")

    md.append("## Latest Run — Snapshot")
    md.append("")
    if not curr_run:
        md.append("_No run history yet._")
    else:
        md.append("| Route Key | This Run Best | Change vs Prev |")
        md.append("|---|---:|---:|")
        for key, r in curr_best.items():
            currency = str(r.get("currency", "") or "")
            p_now = float(r.get("price", float("inf")))
            p_prev = float(prev_best.get(key, {}).get("price", float("inf"))) if prev_best else float("inf")
            if p_prev == float("inf") or p_now == float("inf"):
                delta = "N/A"
            else:
                delta_val = p_now - p_prev
                delta = f"{delta_val:,.2f}"
                if currency:
                    delta = f"{currency} {delta}"
            md.append(f"| `{key}` | {_fmt_money(p_now, currency)} | {delta} |")

    md.append("")
    SUMMARY_PATH.write_text("\n".join(md) + "\n", encoding="utf-8")
    print(f"Wrote {SUMMARY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
