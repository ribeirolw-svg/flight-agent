from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from utilitario.history_store import HistoryStore


@dataclass
class EventRow:
    ts_utc: datetime
    type: str
    payload: Dict[str, Any]


def _parse_ts(ts: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def load_events(store: HistoryStore) -> List[EventRow]:
    rows: List[EventRow] = []
    for e in store.all():
        ts = e.get("ts_utc")
        et = e.get("type")
        payload = e.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {"value": payload}
        if not isinstance(ts, str) or not isinstance(et, str):
            continue
        dt = _parse_ts(ts)
        if dt is None:
            continue
        rows.append(EventRow(ts_utc=dt, type=et, payload=payload))
    rows.sort(key=lambda r: r.ts_utc)
    return rows


def filter_events(
    events: List[EventRow],
    event_types: Optional[Union[str, Iterable[str]]] = None,
    *,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> List[EventRow]:
    types_set = None
    if isinstance(event_types, str):
        types_set = {event_types}
    elif event_types is not None:
        types_set = set(event_types)

    if since and since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    if until and until.tzinfo is None:
        until = until.replace(tzinfo=timezone.utc)

    out: List[EventRow] = []
    for r in events:
        if types_set and r.type not in types_set:
            continue
        if since and r.ts_utc < since.astimezone(timezone.utc):
            continue
        if until and r.ts_utc > until.astimezone(timezone.utc):
            continue
        out.append(r)
    return out


def last_n_days(events: List[EventRow], days: int) -> List[EventRow]:
    now = datetime.now(timezone.utc)
    return filter_events(events, since=now - timedelta(days=days))


def count_by_type(events: List[EventRow]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in events:
        out[r.type] = out.get(r.type, 0) + 1
    return out


def time_series_daily_count(events: List[EventRow], event_type: Optional[str] = None) -> List[Tuple[str, int]]:
    out: Dict[str, int] = {}
    for r in events:
        if event_type and r.type != event_type:
            continue
        day = r.ts_utc.date().isoformat()
        out[day] = out.get(day, 0) + 1
    return sorted(out.items(), key=lambda x: x[0])


def build_dashboard_snapshot(
    store_name: str = "default",
    *,
    days: int = 30,
    type_filter: Optional[Union[str, Iterable[str]]] = None,
) -> Dict[str, Any]:
    store = HistoryStore(store_name)
    events = load_events(store)
    events = last_n_days(events, days)
    if type_filter:
        events = filter_events(events, event_types=type_filter)
    return {
        "store_name": store_name,
        "days": days,
        "total_events": len(events),
        "count_by_type": count_by_type(events),
        "daily_counts": time_series_daily_count(events),
    }


def query_events_for_table(
    store_name: str = "default",
    *,
    event_types: Optional[Union[str, Iterable[str]]] = None,
    days: Optional[int] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    store = HistoryStore(store_name)
    events = load_events(store)
    if days is not None:
        events = last_n_days(events, days)
    if event_types:
        events = filter_events(events, event_types=event_types)

    out: List[Dict[str, Any]] = []
    for r in events[-limit:]:
        out.append(
            {
                "ts_utc": r.ts_utc.isoformat(),
                "type": r.type,
                **r.payload,  # payload “achatado” vira colunas
            }
        )
    return out
