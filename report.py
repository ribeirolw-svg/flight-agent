# report.py
from __future__ import annotations

import json
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

DATA_DIR = Path("data")
STATE_PATH = DATA_DIR / "state.json"
HISTORY_PATH = DATA_DIR / "history.jsonl"
SUMMARY_PATH = DATA_DIR / "summary.md"

# New no-hash keys look like: GRU-FCO|dep=...|ret<=2026-10-05|...
KEEP_REGEX = re.compile(r"^GRU-(FCO|CIA)\|.*\|ret<=2026-10-05\|.*$")

# Minimal IATA -> Airline name mapping (extend as new codes show up)
IATA_AIRLINE_NAMES = {
    "AF": "Air France",
    "LH": "Lufthansa",
    "UX": "Air Europa",
    "ET": "Ethiopian Airlines",
    "AT": "Royal Air Maroc",
    "TP": "TAP Air Portugal",
    "AZ": "ITA Airways",
    "IB": "Iberia",
    "KL": "KLM",
    "LX": "SWISS",
    "BA": "British Airways",
    "LA": "LATAM",
    "TK": "Turkish Airlines",
    "QR": "Qatar Airways",
    "EK": "Emirates",
}


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _read_history_last(n: int = 2) -> List[Dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return []
    lines = HISTORY_PATH.read_text(encoding="utf-8").splitlines()
    tail = lines[-n:] if len(lines) >= n else lines
    out: List[Dict[str, Any]] = []
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


def _md_table_escape(s: str) -> str:
    return (s or "").replace("|", "\\|").replace("\n", " ")


def _airline_label(code: str) -> str:
    code = (code or "").strip().upper()
    name = IATA_AIRLINE_NAMES.get(code)
    return f"{code} ({name})" if name else code


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


def _infer_destination(r: Dict[str, Any]) -> str:
    dest = str(r.get("destination", "") or "")
    if dest:
        return dest
    s = str(r.get("summary", "") or "")
    if "→FCO" in s:
        return "FCO"
    if "→CIA" in s:
        return "CIA"
    k = str(r.get("key", "") or "")
    if k.startswith("GRU-FCO|"):
        return "FCO"
    if k.startswith("GRU-CIA|"):
        return "CIA"
    return "ROM"


def _pick_best_rome(curr_results: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    candidates: List[Tuple[float, Dict[str, Any]]] = []
    for r in curr_results or []:
        dest = _infer_destination(r)
        if dest not in ("FCO", "CIA"):
            continue
        try:
            p = float(r.get("price", float("inf")))
        except Exception:
            p = float("inf")
        candidates.append((p, r))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _render_carrier_table(md: List[str], by_carrier: Any, currency: str) -> None:
    if not isinstance(by_carrier, dict) or not by_carrier:
        md.append("_No airline split available for this run._")
        return

    rows: List[Tuple[str, float]] = []
    for c, v in by_carrier.items():
        try:
            p = float(v)
        except Exception:
            continue
        rows.append((str(c), p))

    if not rows:
        md.append("_No airline split available for this run._")
        return

    rows.sort(key=lambda x: x[1])

    md.append("| Airline | Best Price |")
    md.append("|---|---:|")
    for c, p in rows[:5]:
        md.append(f"| `{_md_table_escape(_airline_label(c))}` | {_fmt_money(p, currency)} |")


def _find_result_for_dest(results: List[Dict[str, Any]], dest: str) -> Optional[Dict[str, Any]]:
    dest = dest.upper().strip()
    for r in results or []:
        if _infer_destination(r) == dest:
            return r
    return None


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    state = _read_json(STATE_PATH)
    best_map: Dict[str, Any] = state.get("best", {}) if isinstance(state.get("best", {}), dict) else {}
    best_map = {k: v for k, v in best_map.items() if KEEP_REGEX.match(k)}

    history = _read_history_last(2)
    curr_run = history[-1] if len(history) >= 1 else None
    prev_run = history[-2] if len(history) >= 2 else None

    curr_results = curr_run.get("results", []) if curr_run else []
    prev_results = prev_run.get("results", []) if prev_run else []

    curr_results_filtered = [r for r in curr_results if KEEP_REGEX.match(str(r.get("key", "")))]
    prev_results_filtered = [r for r in prev_results if KEEP_REGEX.match(str(r.get("key", "")))]

    curr_best = _extract_best_from_results(curr_results_filtered)
    prev_best = _extract_best_from_results(prev_results_filtered)

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

    # Headline — best overall Rome
    md.append("## Headline — São Paulo → Roma (FCO/CIA)")
    md.append("")
    best_rome = _pick_best_rome(curr_results_filtered)
    if not best_rome:
        md.append("_No Rome results found in latest run._")
        md.append("")
    else:
        currency = str(best_rome.get("currency", "") or "BRL")
        try:
            p = float(best_rome.get("price", float("inf")))
        except Exception:
            p = float("inf")

        origin = str(best_rome.get("origin", "GRU") or "GRU")
        dest = _infer_destination(best_rome)

        dep = str(best_rome.get("best_dep", "") or "")
        ret = str(best_rome.get("best_ret", "") or "")

        md.append(f"- **Best this run:** {origin}→{dest} — **{_fmt_money(p, currency)}**")
        md.append(f"- Dates: depart **{dep or '—'}** · return **{ret or '—'}** (≤ 2026-10-05)")
        md.append(f"- Key: `{_md_table_escape(str(best_rome.get('key','') or ''))}`")
        md.append("")

        md.append("### Roma — by Airline (Top 5)")
        md.append("")
        _render_carrier_table(md, best_rome.get("by_carrier", {}), currency)
        md.append("")

    # Per-destination tables
    md.append("## Per Destination — Airline Split")
    md.append("")
    for dest in ("FCO", "CIA"):
        r = _find_result_for_dest(curr_results_filtered, dest)
        md.append(f"### {dest} — by Airline (Top 5)")
        md.append("")
        if not r:
            md.append("_No result for this destination in latest run._")
            md.append("")
            continue

        currency = str(r.get("currency", "") or "BRL")
        try:
            p = float(r.get("price", float("inf")))
        except Exception:
            p = float("inf")

        dep = str(r.get("best_dep", "") or "")
        ret = str(r.get("best_ret", "") or "")

        md.append(f"- Best {dest} this run: **{_fmt_money(p, currency)}**")
        md.append(f"- Dates: depart **{dep or '—'}** · return **{ret or '—'}** (≤ 2026-10-05)")
        md.append("")
        _render_carrier_table(md, r.get("by_carrier", {}), currency)
        md.append("")

    # Current Best
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
            md.append(
                f"| `{_md_table_escape(key)}` | {_fmt_money(price, currency)} | {_md_table_escape(summary)} |"
            )
    md.append("")

    # Snapshot
    md.append("## Latest Run — Snapshot")
    md.append("")
    if not curr_best:
        md.append("_No snapshot rows available yet._")
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

            md.append(f"| `{_md_table_escape(key)}` | {_fmt_money(p_now, currency)} | {delta} |")

    md.append("")
    SUMMARY_PATH.write_text("\n".join(md) + "\n", encoding="utf-8")
    print(f"Wrote {SUMMARY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
