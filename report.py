# report.py
from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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


def _pick_best_rome(curr_results: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Choose best among Rome destinations (FCO/CIA), preferring valid numeric prices.
    """
    candidates = []
    for r in curr_results or []:
        dest = str(r.get("destination", "") or "")
        if dest not in ("FCO", "CIA"):
            # fallback: sometimes destination not present; infer from summary
            s = str(r.get("summary", "") or "")
            if "→FCO" not in s and "→CIA" not in s:
                continue
        try:
            p = float(r.get("price", float("inf")))
        except Exception:
            p = float("inf")
        candidates.append((p, r))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    best_price, best_r = candidates[0]
    if best_price == float("inf"):
        return best_r  # will render as N/A
    return best_r


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    state = _read_json(STATE_PATH)
    import re
    KEEP_REGEX = re.compile(r"^GRU-(FCO|CIA)-.*-RL2026-10-05-.*$")
    
    best_map: Dict[str, Any] = state.get("best", {}) if isinstance(state.get("best", {}), dict) else {}
    best_map = {k: v for k, v in best_map.items() if KEEP_REGEX.match(k)}


    history = _read_history_last(2)
    curr_run = history[-1] if len(history) >= 1 else None
    prev_run = history[-2] if len(history) >= 2 else None

    curr_results = curr_run.get("results", []) if curr_run else []
    prev_results = prev_run.get("results", []) if prev_run else []

    curr_best = _extract_best_from_results(curr_results)
    prev_best = _extract_best_from_results(prev_results)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    md: List[str] = []
    md.append("# Flight Agent — Weekly Summary")
    md.append("")
    md.append(f"- Updated: **{now}**")
    if curr_run:
        md.append(f"- Latest run_id: `{curr_run.get('run_id','')}`")
    if prev_run:
        md.append(f"- Previous run_id: `{prev_run.get('run_id','')}`")
    md.append("")

    # Headline: best Rome
    best_rome = _pick_best_rome(curr_results)
    md.append("## Headline — São Paulo → Roma (FCO/CIA)")
    md.append("")
    if not best_rome:
        md.append("_No Rome results found in latest run._")
    else:
        currency = str(best_rome.get("currency", ""))
        try:
            p = float(best_rome.get("price", float("inf")))
        except Exception:
            p = float("inf")

        origin = str(best_rome.get("origin", "GRU") or "GRU")
        dest = str(best_rome.get("destination", "") or "")
        # try infer destination if missing
        if not dest:
            s = str(best_rome.get("summary", "") or "")
            if "→FCO" in s:
                dest = "FCO"
            elif "→CIA" in s:
                dest = "CIA"
            else:
                dest = "ROM"

        dep = str(best_rome.get("best_dep", "") or "")
        ret = str(best_rome.get("best_ret", "") or "")

        md.append(f"- **Best this run:** {origin}→{dest} — **{_fmt_money(p, currency)}**")
        if dep or ret:
            md.append(f"- Dates: depart **{dep or '—'}** · return **{ret or '—'}** (≤ 2026-10-05)")
        md.append(f"- Key: `{best_rome.get('key','')}`")
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
                delta = f"{currency} {delta_val:,.2f}" if currency else f"{delta_val:,.2f}"
            md.append(f"| `{key}` | {_fmt_money(p_now, currency)} | {delta} |")

    md.append("")
    SUMMARY_PATH.write_text("\n".join(md) + "\n", encoding="utf-8")
    print(f"Wrote {SUMMARY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
