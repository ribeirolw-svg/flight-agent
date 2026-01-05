from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# =========================
# Paths / IO
# =========================
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
STATE_PATH = DATA_DIR / "state.json"
HISTORY_PATH = DATA_DIR / "history.jsonl"
SUMMARY_PATH = DATA_DIR / "summary.md"


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _utc_now_stamp() -> str:
    # ex: 20260104T215041Z
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utc_now_human() -> str:
    # ex: 2026-01-04 21:59 UTC
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def load_state() -> Dict[str, Any]:
    _ensure_data_dir()
    if not STATE_PATH.exists():
        return {"best": {}, "meta": {}}
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))


def save_state(state: Dict[str, Any]) -> None:
    _ensure_data_dir()
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def append_history(run_id: str, profile: Dict[str, Any], results: List[Dict[str, Any]]) -> None:
    _ensure_data_dir()
    record = {"run_id": run_id, "profile": profile, "results": results}
    with open(HISTORY_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_last_history_record() -> Optional[Dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return None
    try:
        lines = HISTORY_PATH.read_text(encoding="utf-8").splitlines()
        if not lines:
            return None
        return json.loads(lines[-1])
    except Exception:
        return None


# =========================
# Profile loading
# =========================
def load_profile() -> Dict[str, Any]:
    # 1) env var (string JSON)
    raw = (os.getenv("SEARCH_PROFILE_JSON") or "").strip()
    if raw:
        try:
            return json.loads(raw)
        except Exception as e:
            raise RuntimeError(f"SEARCH_PROFILE_JSON inválido (não é JSON): {e}")

    # 2) arquivo padrão (compat com seu fluxo anterior)
    candidates = [
        Path("backend") / "search_profile.json",
        Path("search_profile.json"),
    ]
    for p in candidates:
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception as e:
                raise RuntimeError(f"Arquivo {p} existe mas está inválido: {e}")

    raise RuntimeError(
        "No search profile found. Provide SEARCH_PROFILE_JSON env var or create file at: backend/search_profile.json"
    )


# =========================
# Best persistence (rich)
# =========================
def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def update_best_state_rich(state: Dict[str, Any], results: List[Dict[str, Any]]) -> None:
    """
    Atualiza state["best"] com campos completos para o dashboard:
      price, currency, summary, origin, destination, best_dep, best_ret, by_carrier
    Regra de overwrite:
      - grava se não existia
      - ou se preço novo é menor que o anterior
      - não sobrescreve preço bom com N/A / None
    """
    best_map: Dict[str, Any] = state.setdefault("best", {})

    for r in results:
        key = r.get("key")
        if not key:
            continue

        new_price = _to_float(r.get("price"))
        new_currency = r.get("currency") or "BRL"

        prev = best_map.get(key)
        prev_price = _to_float(prev.get("price")) if isinstance(prev, dict) else None

        should_write = False
        if prev is None:
            # primeira vez, grava mesmo que seja N/A (para registrar "no offers")
            should_write = True
        elif prev_price is None and new_price is not None:
            should_write = True
        elif prev_price is not None and new_price is not None and new_price < prev_price:
            should_write = True
        else:
            # se novo é None e já tinha preço -> não sobrescreve
            should_write = False

        if should_write:
            best_map[key] = {
                "price": r.get("price"),  # mantem como veio (float/str/None)
                "currency": new_currency,
                "summary": r.get("summary", "") or "",
                "origin": r.get("origin", "GRU"),
                "destination": r.get("destination"),      # "FCO"/"CIA" quando aplicável
                "best_dep": r.get("best_dep"),            # ✅ datas
                "best_ret": r.get("best_ret"),            # ✅ datas
                "by_carrier": r.get("by_carrier", {}) or {},  # ✅ split por cia
            }


# =========================
# Summary generation (robusto)
# =========================
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


def _carrier_label(code: str) -> str:
    code = (code or "").upper().strip()
    name = IATA_AIRLINE_NAMES.get(code)
    return f"{code} ({name})" if name else code


def _money(currency: str, price: Any) -> str:
    p = _to_float(price)
    if p is None:
        return "N/A"
    return f"{currency} {p:,.2f}"


def _infer_dest_from_key(key: str) -> str:
    if key.startswith("GRU-FCO|"):
        return "FCO"
    if key.startswith("GRU-CIA|"):
        return "CIA"
    return ""


def _extract_pax_from_key(key: str) -> str:
    # ...|A2|C1|...
    import re as _re

    ma = _re.search(r"\|A(\d+)\|", key)
    mc = _re.search(r"\|C(\d+)\|", key)
    a = int(ma.group(1)) if ma else 0
    c = int(mc.group(1)) if mc else 0
    parts = []
    if a:
        parts.append(f"{a} adulto" + ("s" if a != 1 else ""))
    if c:
        parts.append(f"{c} criança" + ("s" if c != 1 else ""))
    return " · ".join(parts) if parts else "—"


def _best_carriers(by_carrier: Dict[str, Any]) -> List[Tuple[str, float]]:
    rows: List[Tuple[str, float]] = []
    if not isinstance(by_carrier, dict):
        return rows
    for c, v in by_carrier.items():
        p = _to_float(v)
        if p is None:
            continue
        rows.append((str(c), p))
    rows.sort(key=lambda x: x[1])
    return rows


def _pick_best_rome_from_state(best_map: Dict[str, Any]) -> Optional[Tuple[str, Dict[str, Any]]]:
    candidates: List[Tuple[float, str, Dict[str, Any]]] = []
    for key, info in (best_map or {}).items():
        key = str(key)
        if not (key.startswith("GRU-FCO|") or key.startswith("GRU-CIA|")):
            continue
        p = _to_float(info.get("price") if isinstance(info, dict) else None)
        candidates.append((p if p is not None else float("inf"), key, info if isinstance(info, dict) else {}))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1], candidates[0][2]


def _format_table(rows: List[List[str]]) -> str:
    # markdown table
    if not rows:
        return ""
    header = rows[0]
    body = rows[1:]
    out = []
    out.append("| " + " | ".join(header) + " |")
    out.append("| " + " | ".join(["---"] * len(header)) + " |")
    for r in body:
        out.append("| " + " | ".join(r) + " |")
    return "\n".join(out)


def write_summary(
    run_id: str,
    prev_run_id: Optional[str],
    state: Dict[str, Any],
    results: List[Dict[str, Any]],
    prev_results: Optional[List[Dict[str, Any]]],
) -> None:
    _ensure_data_dir()

    best_map: Dict[str, Any] = state.get("best", {}) if isinstance(state.get("best", {}), dict) else {}
    best_rome = _pick_best_rome_from_state(best_map)

    lines: List[str] = []
    lines.append("# Flight Agent — Weekly Summary")
    lines.append(f"Updated: {_utc_now_human()}")
    lines.append(f"Latest run_id: {run_id}")
    lines.append(f"Previous run_id: {prev_run_id or '—'}")
    lines.append("")

    # Headline Roma
    lines.append("## Headline — São Paulo → Roma (FCO/CIA)")
    if best_rome:
        key, info = best_rome
        currency = str(info.get("currency", "BRL") or "BRL")
        price_txt = _money(currency, info.get("price"))
        dep = info.get("best_dep") or "—"
        ret = info.get("best_ret") or "—"
        dest = info.get("destination") or _infer_dest_from_key(key) or "ROM"
        pax = _extract_pax_from_key(key)

        lines.append(f"Best (state): GRU→{dest} — {price_txt}")
        lines.append(f"Dates: depart {dep} · return {ret}")
        lines.append(f"Pax: {pax}")
        lines.append(f"Key: `{key}`")
    else:
        lines.append("No Rome results found in state.json yet.")
    lines.append("")

    # Roma by airline (usa by_carrier do melhor item FCO/CIA se existir)
    if best_rome:
        _, info = best_rome
        carriers = _best_carriers(info.get("by_carrier", {}) if isinstance(info, dict) else {})
        if carriers:
            lines.append("## Roma — by Airline (Top 5)")
            table_rows = [["Airline", "Best Price"]]
            currency = str(info.get("currency", "BRL") or "BRL")
            for code, price in carriers[:5]:
                table_rows.append([_carrier_label(code), _money(currency, price)])
            lines.append(_format_table(table_rows))
            lines.append("")

    # Current Best
    lines.append("## Current Best (from state.json)")
    if not best_map:
        lines.append("No best prices recorded yet.")
        lines.append("")
    else:
        table_rows = [["Route Key", "Best Price", "Notes"]]
        # ordena por destino e preço
        def _sort_key(item: Tuple[str, Dict[str, Any]]):
            k, inf = item
            dest = (inf.get("destination") or _infer_dest_from_key(str(k)) or "ZZZ")
            p = _to_float(inf.get("price")) if isinstance(inf, dict) else None
            return (dest, p if p is not None else float("inf"))

        for k, inf in sorted(best_map.items(), key=_sort_key):
            inf = inf if isinstance(inf, dict) else {}
            currency = str(inf.get("currency", "BRL") or "BRL")
            table_rows.append([str(k), _money(currency, inf.get("price")), (inf.get("summary") or "")[:200]])
        lines.append(_format_table(table_rows))
        lines.append("")

    # Snapshot do run atual vs anterior (se houver prev_results)
    lines.append("## Latest Run — Snapshot")
    if not results:
        lines.append("No snapshot rows available yet.")
    else:
        prev_by_key = {}
        if prev_results:
            for r in prev_results:
                if r.get("key"):
                    prev_by_key[str(r["key"])] = r

        snap_rows = [["Route Key", "This Run Best", "Change vs Prev"]]
        for r in results:
            key = str(r.get("key", ""))
            if not key:
                continue
            currency = str(r.get("currency", "BRL") or "BRL")
            this_p = _to_float(r.get("price"))
            this_txt = _money(currency, r.get("price"))

            prev = prev_by_key.get(key)
            prev_p = _to_float(prev.get("price")) if prev else None

            if this_p is None or prev_p is None:
                ch = "N/A"
            else:
                ch = _money(currency, this_p - prev_p)
            snap_rows.append([key, this_txt, ch])

        lines.append(_format_table(snap_rows))
    lines.append("")

    SUMMARY_PATH.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


# =========================
# Main
# =========================
def main() -> int:
    try:
        from search import run_search  # seu search.py na raiz
    except Exception as e:
        print(f"Scheduler failed: could not import run_search from search.py: {e}", file=sys.stderr)
        return 2

    run_id = _utc_now_stamp()
    state = load_state()
    prev_history = load_last_history_record()
    prev_run_id = prev_history.get("run_id") if prev_history else None
    prev_results = prev_history.get("results") if prev_history else None

    try:
        profile = load_profile()
    except Exception as e:
        print(f"Scheduler failed: {e}", file=sys.stderr)
        return 1

    try:
        results = run_search(profile)
        if results is None:
            results = []
        if not isinstance(results, list):
            raise RuntimeError("run_search(profile) deve retornar uma lista de dicts.")
    except Exception as e:
        print(f"Scheduler failed: {e}", file=sys.stderr)
        raise  # mantém stacktrace no Actions

    # Atualiza meta (run ids)
    meta = state.setdefault("meta", {})
    meta["previous_run_id"] = meta.get("latest_run_id")
    meta["latest_run_id"] = run_id

    # ✅ Persistência rica (corrige seu dashboard)
    update_best_state_rich(state, results)

    # Salva state + history + summary
    save_state(state)
    append_history(run_id, profile, results)
    write_summary(run_id, prev_run_id, state, results, prev_results)

    print(f"OK: run_id={run_id} results={len(results)} state_best={len(state.get('best', {}))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
