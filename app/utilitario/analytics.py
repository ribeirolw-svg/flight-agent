from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from utilitario.history_store import HistoryStore


# -----------------------------
# Helpers de data/hora
# -----------------------------

def _parse_ts_utc(ts: str) -> Optional[datetime]:
    """
    Aceita ISO8601 (ex: '2026-01-11T22:43:40.699464+00:00').
    Retorna datetime timezone-aware em UTC.
    """
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get_nested(d: Dict[str, Any], path: str) -> Any:
    """
    Lê chave aninhada via 'a.b.c'. Se não existir, retorna None.
    """
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        if part not in cur:
            return None
        cur = cur[part]
    return cur


def _as_number(x: Any) -> Optional[float]:
    """
    Converte int/float/str numérica em float.
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip().replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None
    return None


# -----------------------------
# Modelinho (opcional)
# -----------------------------

@dataclass
class EventRow:
    ts_utc: datetime
    type: str
    payload: Dict[str, Any]


def load_events(store: HistoryStore) -> List[EventRow]:
    """
    Carrega todos os eventos do store e normaliza ts_utc.
    Ignora linhas com ts inválido.
    """
    rows: List[EventRow] = []
    for e in store.all():
        ts = e.get("ts_utc")
        et = e.get("type")
        payload = e.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {"value": payload}

        if not isinstance(ts, str) or not isinstance(et, str):
            continue

        dt = _parse_ts_utc(ts)
        if dt is None:
            continue

        rows.append(EventRow(ts_utc=dt, type=et, payload=payload))

    rows.sort(key=lambda r: r.ts_utc)
    return rows


# -----------------------------
# Filtros
# -----------------------------

def filter_events(
    events: List[EventRow],
    event_types: Optional[Union[str, Iterable[str]]] = None,
    *,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    payload_equals: Optional[Dict[str, Any]] = None,
    payload_contains: Optional[Dict[str, str]] = None,
) -> List[EventRow]:
    """
    - event_types: str ou lista/iterável
    - since/until: datetime (naive ou tz-aware) -> tratado como UTC
    - payload_equals: {"payload.path": valor_exato}
    - payload_contains: {"payload.path": "trecho"} (case-insensitive)
    """
    if isinstance(event_types, str):
        types_set = {event_types}
    elif event_types is None:
        types_set = None
    else:
        types_set = set(event_types)

    since_utc = _to_utc(since) if since else None
    until_utc = _to_utc(until) if until else None

    out: List[EventRow] = []
    for r in events:
        if types_set is not None and r.type not in types_set:
            continue
        if since_utc and r.ts_utc < since_utc:
            continue
        if until_utc and r.ts_utc > until_utc:
            continue

        if payload_equals:
            ok = True
            for k, v in payload_equals.items():
                got = _get_nested(r.payload, k)
                if got != v:
                    ok = False
                    break
            if not ok:
                continue

        if payload_contains:
            ok = True
            for k, needle in payload_contains.items():
                got = _get_nested(r.payload, k)
                if got is None:
                    ok = False
                    break
                s = str(got).lower()
                if str(needle).lower() not in s:
                    ok = False
                    break
            if not ok:
                continue

        out.append(r)

    return out


def last_n_days(events: List[EventRow], days: int) -> List[EventRow]:
    if days <= 0:
        return []
    now = datetime.now(timezone.utc)
    return filter_events(events, since=now - timedelta(days=days))


# -----------------------------
# Métricas / Cálculos
# -----------------------------

def count_by_type(events: List[EventRow]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in events:
        out[r.type] = out.get(r.type, 0) + 1
    return out


def count_by_key(events: List[EventRow], key_path: str) -> Dict[str, int]:
    """
    Conta ocorrências pelo valor de payload[key_path].
    Ex: key_path="origin" ou "flight.origin" (se seu payload for aninhado)
    """
    out: Dict[str, int] = {}
    for r in events:
        v = _get_nested(r.payload, key_path)
        if v is None:
            continue
        k = str(v)
        out[k] = out.get(k, 0) + 1
    return out


def time_series_daily_count(
    events: List[EventRow],
    event_type: Optional[str] = None,
) -> List[Tuple[str, int]]:
    """
    Retorna lista de (YYYY-MM-DD, count) em UTC.
    """
    out: Dict[str, int] = {}
    for r in events:
        if event_type and r.type != event_type:
            continue
        day = r.ts_utc.date().isoformat()
        out[day] = out.get(day, 0) + 1
    return sorted(out.items(), key=lambda x: x[0])


def numeric_summary(
    events: List[EventRow],
    numeric_path: str,
    *,
    event_type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Calcula sum/avg/min/max/count para um campo numérico do payload.
    """
    vals: List[float] = []
    for r in events:
        if event_type and r.type != event_type:
            continue
        v = _get_nested(r.payload, numeric_path)
        n = _as_number(v)
        if n is None:
            continue
        vals.append(n)

    if not vals:
        return {"count": 0, "sum": 0.0, "avg": None, "min": None, "max": None}

    s = sum(vals)
    return {
        "count": len(vals),
        "sum": s,
        "avg": s / len(vals),
        "min": min(vals),
        "max": max(vals),
    }


def group_numeric_by_key(
    events: List[EventRow],
    group_key_path: str,
    numeric_path: str,
    *,
    event_type: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Agrupa por uma chave do payload e calcula resumo numérico dentro de cada grupo.
    """
    buckets: Dict[str, List[float]] = {}
    for r in events:
        if event_type and r.type != event_type:
            continue

        g = _get_nested(r.payload, group_key_path)
        if g is None:
            continue
        gk = str(g)

        v = _get_nested(r.payload, numeric_path)
        n = _as_number(v)
        if n is None:
            continue

        buckets.setdefault(gk, []).append(n)

    out: Dict[str, Dict[str, Any]] = {}
    for gk, vals in buckets.items():
        s = sum(vals)
        out[gk] = {
            "count": len(vals),
            "sum": s,
            "avg": s / len(vals) if vals else None,
            "min": min(vals) if vals else None,
            "max": max(vals) if vals else None,
        }
    return out


# -----------------------------
# Funções “prontas pro Streamlit”
# -----------------------------

def build_dashboard_snapshot(
    store_name: str = "default",
    *,
    days: int = 30,
    type_filter: Optional[Union[str, Iterable[str]]] = None,
) -> Dict[str, Any]:
    """
    Carrega store, filtra últimos N dias, e retorna um pacotinho de métricas.
    """
    store = HistoryStore(store_name)
    events = load_events(store)
    events = last_n_days(events, days)

    if type_filter is not None:
        events = filter_events(events, event_types=type_filter)

    snapshot = {
        "store_name": store_name,
        "days": days,
        "total_events": len(events),
        "count_by_type": count_by_type(events),
        "daily_counts": time_series_daily_count(events),
    }
    return snapshot


def query_events_for_table(
    store_name: str = "default",
    *,
    event_types: Optional[Union[str, Iterable[str]]] = None,
    days: Optional[int] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    payload_equals: Optional[Dict[str, Any]] = None,
    payload_contains: Optional[Dict[str, str]] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """
    Retorna uma lista de dicts “achatada” pra jogar numa tabela do Streamlit.
    """
    store = HistoryStore(store_name)
    events = load_events(store)

    if days is not None:
        events = last_n_days(events, days)

    events = filter_events(
        events,
        event_types=event_types,
        since=since,
        until=until,
        payload_equals=payload_equals,
        payload_contains=payload_contains,
    )

    # Limite e formato final
    out: List[Dict[str, Any]] = []
    for r in events[-limit:]:
        out.append(
            {
                "ts_utc": r.ts_utc.isoformat(),
                "type": r.type,
                **r.payload,  # espalha payload no nível de coluna (bom p/ dataframe)
            }
        )
    return out


# -----------------------------
# Exemplo rápido (pra testar local)
# -----------------------------

if __name__ == "__main__":
    snap = build_dashboard_snapshot("default", days=7)
    print("Snapshot:", snap)

    rows = query_events_for_table("default", days=7, limit=5)
    print("Sample rows:", rows)
