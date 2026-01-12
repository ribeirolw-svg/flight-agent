"""
utilitario package

Exports:
- HistoryStore
- analytics helpers
"""

# Import "suave" (não mata o app se algo estiver faltando)
try:
    from .history_store import HistoryStore
except Exception as e:
    HistoryStore = None  # type: ignore
    _history_store_import_error = e  # noqa

try:
    from .analytics import (
        load_events,
        filter_events,
        last_n_days,
        count_by_type,
        count_by_key,
        time_series_daily_count,
        numeric_summary,
        group_numeric_by_key,
        build_dashboard_snapshot,
        query_events_for_table,
    )
except Exception as e:
    # Se analytics também quebrar, o app ainda sobe e você vê o erro no Streamlit
    load_events = None  # type: ignore
    filter_events = None  # type: ignore
    last_n_days = None  # type: ignore
    count_by_type = None  # type: ignore
    count_by_key = None  # type: ignore
    time_series_daily_count = None  # type: ignore
    numeric_summary = None  # type: ignore
    group_numeric_by_key = None  # type: ignore
    build_dashboard_snapshot = None  # type: ignore
    query_events_for_table = None  # type: ignore
    _analytics_import_error = e  # noqa

__all__ = [
    "HistoryStore",
    "load_events",
    "filter_events",
    "last_n_days",
    "count_by_type",
    "count_by_key",
    "time_series_daily_count",
    "numeric_summary",
    "group_numeric_by_key",
    "build_dashboard_snapshot",
    "query_events_for_table",
]
