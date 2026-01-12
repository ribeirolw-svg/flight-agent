# app/streamlit_app.py
from __future__ import annotations

import json
import sys
from pathlib import Path
import importlib

import streamlit as st

# ----------------------------
# PATH FIX (Streamlit Cloud safe)
# ----------------------------
APP_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

st.set_page_config(page_title="Flight Agent - Debug", layout="wide")
st.title("✈️ Flight Agent — Debug de Imports")

# ----------------------------
# Debug de filesystem (antes de importar qualquer coisa)
# ----------------------------
st.subheader("1) Debug do filesystem")
st.write("APP_DIR:", str(APP_DIR))
st.write("sys.path[0:5]:", sys.path[:5])

# Lista o que existe dentro de /app
try:
    app_items = sorted([p.name for p in APP_DIR.iterdir()])
    st.write("Conteúdo de /app:", app_items)
except Exception as e:
    st.error("Não consegui listar /app")
    st.exception(e)
    st.stop()

util_dir = APP_DIR / "utilitario"
st.write("Existe /app/utilitario ?", util_dir.exists())
if util_dir.exists():
    util_items = sorted([p.name for p in util_dir.iterdir()])
    st.write("Conteúdo de /app/utilitario:", util_items)
else:
    st.error("❌ /app/utilitario NÃO existe no deploy. Isso explica tudo.")
    st.info("Garanta que a pasta utilitario está dentro da pasta app e commit/push no GitHub.")
    st.stop()

st.divider()

# ----------------------------
# Import dinâmico (pra não morrer antes de mostrar debug)
# ----------------------------
st.subheader("2) Tentando importar módulos")

def _import_or_stop(module_name: str):
    try:
        m = importlib.import_module(module_name)
        st.success(f"✅ Import OK: {module_name}")
        return m
    except Exception as e:
        st.error(f"❌ Falha ao importar: {module_name}")
        st.exception(e)
        st.stop()

history_store_mod = _import_or_stop("utilitario.history_store")
analytics_mod = _import_or_stop("utilitario.analytics")

# pega as funções/classes do módulo
HistoryStore = getattr(history_store_mod, "HistoryStore", None)
build_dashboard_snapshot = getattr(analytics_mod, "build_dashboard_snapshot", None)
query_events_for_table = getattr(analytics_mod, "query_events_for_table", None)
load_events = getattr(analytics_mod, "load_events", None)
last_n_days = getattr(analytics_mod, "last_n_days", None)
filter_events = getattr(analytics_mod, "filter_events", None)
numeric_summary = getattr(analytics_mod, "numeric_summary", None)

missing = [name for name, obj in {
    "HistoryStore": HistoryStore,
    "build_dashboard_snapshot": build_dashboard_snapshot,
    "query_events_for_table": query_events_for_table,
    "load_events": load_events,
    "last_n_days": last_n_days,
    "filter_events": filter_events,
    "numeric_summary": numeric_summary,
}.items() if obj is None]

if missing:
    st.error("Módulos importaram, mas faltam símbolos:")
    st.write(missing)
    st.stop()

st.divider()

# ----------------------------
# App normal (se chegou até aqui, import tá OK)
# ----------------------------
st.subheader("3) App (funcional)")

store_name = st.sidebar.text_input("Nome do store", value="default").strip() or "default"
days = st.sidebar.slider("Janela (últimos N dias)", 1, 365, 30)

type_filter_str = st.sidebar.text_input("Filtrar por type (vírgula, opcional)", value="").strip()
type_filter = [t.strip() for t in type_filter_str.split(",") if t.strip()] if type_filter_str else None

payload_key = st.sidebar.text_input("Payload key (opcional)", value="").strip()
payload_value = st.sidebar.text_input("Payload contém (opcional)", value="").strip()
payload_contains = {payload_key: payload_value} if payload_key and payload_value else None

store = HistoryStore(store_name)

st.sidebar.subheader("Teste rápido: Append")
event_type = st.sidebar.text_input("type", value="manual_test").strip() or "manual_test"
payload_text = st.sidebar.text_area(
    "payload JSON",
    value=json.dumps({"run_id": "test", "value": 123.45}, ensure_ascii=False, indent=2),
    height=120,
)

c1, c2 = st.sidebar.columns(2)
with c1:
    if st.button("➕ Append", use_container_width=True):
        try:
            payload = json.loads(payload_text) if payload_text.strip() else {}
            if not isinstance(payload, dict):
                st.sidebar.error("Payload precisa ser JSON objeto (dict).")
            else:
                store.append(event_type=event_type, payload=payload)
                st.sidebar.success("Gravado ✅")
        except Exception as e:
            st.sidebar.error("Erro ao gravar")
            st.sidebar.exception(e)

with c2:
    if st.button("🧹 Clear", use_container_width=True):
        store.clear()
        st.sidebar.warning("Apagado.")

# Snapshot
snap = build_dashboard_snapshot(store_name=store_name, days=days, type_filter=type_filter)

col1, col2 = st.columns([1, 2], gap="large")
with col1:
    st.metric("Total eventos", snap.get("total_events", 0))
    st.write("**Count by type**")
    st.json(snap.get("count_by_type", {}))

with col2:
    daily = snap.get("daily_counts", [])
    if daily:
        st.line_chart({d: c for d, c in daily})
    else:
        st.info("Sem dados na janela.")

# Tabela
st.subheader("Eventos")
rows = query_events_for_table(
    store_name=store_name,
    event_types=type_filter,
    days=days,
    payload_contains=payload_contains,
    limit=1000,
)
st.dataframe(rows, use_container_width=True) if rows else st.info("Nada encontrado.")
