from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml

# ------------------------------------------------------------
# PATHS (repo root + app for utilitario)
# ------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent
APP_DIR = ROOT_DIR / "app"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

# search.py (raiz)
from search import run_search_and_store  # noqa: E402

# ------------------------------------------------------------
# State.json helpers (persistência de execução)
# ------------------------------------------------------------
def _utc_now_iso() -> str:
    """Ex: 2026-01-12T09:15:22Z"""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def update_state(data_dir: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    p = Path(data_dir) / "state.json"
    cur = _read_json(p)
    cur.update(patch)
    _write_json(p, cur)
    return cur


def mark_run_start(data_dir: str) -> None:
    update_state(
        data_dir,
        {
            "last_run_utc": _utc_now_iso(),
            "last_status": "running",
            "last_error": "",
        },
    )


def mark_run_success(data_dir: str, summary: str) -> None:
    update_state(
        data_dir,
        {
            "last_success_utc": _utc_now_iso(),
            "last_status": "success",
            "last_error": "",
            "last_summary": summary,
        },
    )


def mark_run_error(data_dir: str, err: str) -> None:
    update_state(
        data_dir,
        {
            "last_status": "error",
            "last_error": (err or "").strip(),
        },
    )


# ------------------------------------------------------------
# Date helpers
# ------------------------------------------------------------
DOW_MAP = {
    "MON": 0,
    "TUE": 1,
    "WED": 2,
    "THU": 3,
    "FRI": 4,
    "SAT": 5,
    "SUN": 6,
}


def normalize_date(s: Optional[str]) -> Optional[str]:
    """
    Normaliza datas comuns para YYYY-MM-DD:
      - YYYY-MM-DD
      - DD-MM-YYYY
      - YYYY/MM/DD
      - DD/MM/YYYY
    """
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None

    fmts = ["%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    return s  # deixa estourar na API e cair no erro gravado


def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def daterange(d0: date, d1: date) -> Iterable[date]:
    cur = d0
    while cur <= d1:
        yield cur
        cur += timedelta(days=1)


def next_date_with_dow(start: date, allowed_dows: List[int], max_ahead_days: int = 10) -> Optional[date]:
    for i in range(1, max_ahead_days + 1):
        d = start + timedelta(days=i)
        if d.weekday() in allowed_dows:
            return d
    return None


def generate_date_pairs_fixed(rule: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    fixed:
      depart_start, depart_end, trip_length_days, return_deadline
    Estratégia: gerar algumas datas dentro do range (não todas),
    pegando 1 a cada 3 dias, e respeitando return_deadline.
    """
    depart_start = parse_ymd(normalize_date(rule["depart_start"]) or rule["depart_start"])
    depart_end = parse_ymd(normalize_date(rule["depart_end"]) or rule["depart_end"])
    trip_len = int(rule.get("trip_length_days", 7))
    return_deadline = parse_ymd(normalize_date(rule["return_deadline"]) or rule["return_deadline"])

    pairs: List[Tuple[str, str]] = []
    step = int(rule.get("depart_step_days", 3))  # opcional

    cur = depart_start
    while cur <= depart_end:
        ret = cur + timedelta(days=trip_len)
        if ret <= return_deadline:
            pairs.append((cur.strftime("%Y-%m-%d"), ret.strftime("%Y-%m-%d")))
        cur += timedelta(days=step)

    # garante pelo menos 1
    if not pairs:
        cur = depart_start
        ret = min(cur + timedelta(days=trip_len), return_deadline)
        pairs.append((cur.strftime("%Y-%m-%d"), ret.strftime("%Y-%m-%d")))

    return pairs


def generate_date_pairs_rolling_weekend(rule: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    rolling_weekend:
      lookahead_days
      depart_dows: [FRI, SAT]
      return_dows: [SUN, MON]
    Estratégia: para cada data de ida permitida,
    pega a primeira data de volta permitida logo após (até +10 dias).
    """
    lookahead_days = int(rule.get("lookahead_days", 30))
    depart_dows = [DOW_MAP[x.upper()] for x in rule.get("depart_dows", ["FRI", "SAT"])]
    return_dows = [DOW_MAP[x.upper()] for x in rule.get("return_dows", ["SUN", "MON"])]

    today = datetime.now().date()
    end = today + timedelta(days=lookahead_days)

    pairs: List[Tuple[str, str]] = []
    for d in daterange(today, end):
        if d.weekday() not in depart_dows:
            continue
        ret = next_date_with_dow(d, return_dows, max_ahead_days=10)
        if ret:
            pairs.append((d.strftime("%Y-%m-%d"), ret.strftime("%Y-%m-%d")))

    # limita volume (evita spam e lentidão)
    max_pairs = int(rule.get("max_pairs", 6))
    return pairs[:max_pairs]


def generate_date_pairs(rule: Dict[str, Any]) -> List[Tuple[str, str]]:
    t = str(rule.get("type", "")).strip().lower()
    if t == "fixed":
        return generate_date_pairs_fixed(rule)
    if t == "rolling_weekend":
        return generate_date_pairs_rolling_weekend(rule)
    # fallback: nenhum par
    return []


# ------------------------------------------------------------
# History.jsonl writer (consolidado)
# ------------------------------------------------------------
def append_history_jsonl(data_dir: str, event: Dict[str, Any]) -> None:
    p = Path(data_dir) / "history.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)

    # garante alguns campos padrão
    event = dict(event)
    event.setdefault("ts_utc", _utc_now_iso())
    event.setdefault("type", "flight_search")

    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


# ------------------------------------------------------------
# Core scheduler
# ------------------------------------------------------------
@dataclass
class RunStats:
    total_calls: int = 0
    ok_calls: int = 0
    err_calls: int = 0

    def add(self, ok: bool) -> None:
        self.total_calls += 1
        if ok:
            self.ok_calls += 1
        else:
            self.err_calls += 1


def load_routes_config(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"routes config not found: {path}")
    return yaml.safe_load(p.read_text(encoding="utf-8"))


def main() -> None:
    data_dir = os.environ.get("DATA_DIR", "data")
    routes_file = os.environ.get("ROUTES_FILE", "routes.yaml")
    store_name = os.environ.get("STORE_NAME", "default").strip() or "default"

    # marca início
    mark_run_start(data_dir)

    stats = RunStats()
    started_utc = _utc_now_iso()

    try:
        cfg = load_routes_config(routes_file)

        defaults = cfg.get("defaults", {}) or {}
        adults = int(os.environ.get("ADULTS", defaults.get("adults", 1)))
        children = int(os.environ.get("CHILDREN", defaults.get("children", 0)))
        cabin = str(os.environ.get("CABIN", defaults.get("cabin", "ECONOMY"))).strip().upper()
        currency = str(os.environ.get("CURRENCY", defaults.get("currency", "BRL"))).strip().upper()
        direct_only = str(os.environ.get("DIRECT_ONLY", defaults.get("direct_only", True))).strip().lower() in ["1", "true", "yes", "y"]
        max_results = int(os.environ.get("AMADEUS_MAX_RESULTS", "10"))

        client_id = os.environ.get("AMADEUS_CLIENT_ID", "").strip()
        client_secret = os.environ.get("AMADEUS_CLIENT_SECRET", "").strip()
        if not client_id or not client_secret:
            raise RuntimeError("Missing AMADEUS_CLIENT_ID / AMADEUS_CLIENT_SECRET (secrets/env)")

        origin_domestic = str(cfg.get("origin_domestic", "CGH")).strip().upper()

        routes = cfg.get("routes", []) or []
        if not routes:
            raise RuntimeError("No routes found in routes.yaml")

        # Loop por rota -> destinos -> datas
        for route in routes:
            route_name = str(route.get("name", "route")).strip()
            domestic = bool(route.get("domestic", False))

            origin = str(route.get("origin", origin_domestic if domestic else "")).strip().upper()
            if not origin:
                raise RuntimeError(f"Route '{route_name}' missing origin")

            destinations: List[str] = [str(x).strip().upper() for x in (route.get("destinations", []) or [])]
            if not destinations:
                continue

            rule = route.get("date_rule", {}) or {}
            pairs = generate_date_pairs(rule)
            if not pairs:
                # se não gerou datas, pula
                continue

            for dest in destinations:
                for depart_date, return_date in pairs:
                    # roda busca
                    result = run_search_and_store(
                        store_name=store_name,
                        client_id=client_id,
                        client_secret=client_secret,
                        origin=origin,
                        destination=dest,
                        departure_date=depart_date,
                        return_date=return_date,
                        adults=adults,
                        children=children,
                        cabin=cabin,
                        currency=currency,
                        direct_only=direct_only,
                        max_results=max_results,
                        save_raw=False,
                    )

                    ok = not bool(result.get("error"))
                    stats.add(ok)

                    # também escreve consolidado em history.jsonl (pra persistência padronizada)
                    event = dict(result)
                    event["route_name"] = route_name
                    event["store_name"] = store_name
                    event["source"] = "amadeus"
                    append_history_jsonl(data_dir, event)

        # summary.md
        summary = (
            f"# Flight Agent — Update Summary\n\n"
            f"- started_utc: `{started_utc}`\n"
            f"- finished_utc: `{_utc_now_iso()}`\n"
            f"- total_calls: `{stats.total_calls}`\n"
            f"- ok_calls: `{stats.ok_calls}`\n"
            f"- err_calls: `{stats.err_calls}`\n"
            f"- store: `{store_name}`\n"
            f"- max_results: `{max_results}`\n"
        )
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        (Path(data_dir) / "summary.md").write_text(summary, encoding="utf-8")

        mark_run_success(
            data_dir,
            summary=f"calls={stats.total_calls} ok={stats.ok_calls} err={stats.err_calls}",
        )

    except Exception as e:
        mark_run_error(data_dir, str(e))
        # também escreve summary.md com erro
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        (Path(data_dir) / "summary.md").write_text(
            f"# Flight Agent — Update Summary (ERROR)\n\n- finished_utc: `{_utc_now_iso()}`\n- error: `{str(e)}`\n",
            encoding="utf-8",
        )
        raise


if __name__ == "__main__":
    main()
