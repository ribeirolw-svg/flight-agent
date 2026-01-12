# app/streamlit_app.py
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import streamlit as st

# -------------------------------------------------------------------
# PATH FIX: garante que /app esteja no sys.path (Streamlit Cloud safe)
# -------------------------------------------------------------------
APP_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

# -------------------------------------------------------------------
# Imports do seu pacote (NÃO depende do __init__.py)
# -------------------------------------------------------------------
try:
    from utilitario.history_store import HistoryStore
except Exception as e:
    st.error("Falha ao importar utilitario.history_store")
    st.exception(e)
    st.stop()

try:
    from utilitario.analytics import (
        build_dashboard_snapshot,
        query_events_for_table,
        load_events,
        last_n_days,
        filter_events,
        numeric_summary,
    )
except Exception as e:
    st.error("Falha ao importar utilitario.analytics")
    st.exception(e)
    st.stop()


# -------------------------------------------------------------------
# UI
# -------------------------------------------------------------------
st.set_page_config(page_title="Flight Agent - History & Analytics", layout="wide")

st.title("✈️ Flight Agent — Histórico & Analytics")
st.caption("Persistência em JSONL + filtros + métricas. (Streamlit Cloud-safe)")

# Sidebar - config
st.sidebar.header("Configuração")

store_name = st.sidebar.text_input("Nome do store", value="default").strip() or "default"
days = st.sidebar.slider("Janela (últimos N dias)", min_value=1, max_value=365, value=30)

type_filter_str = st.sidebar.text_input(
    "Filtrar por type (vírgula, opcional)", value=""
).strip()
type_filter = None
if type_filter_str:
    type_filter = [t.strip() for t in type_filter_str.split(",") if t.strip()]

st.sidebar.subheader("Filtro por texto no payload (opcional)")
payload_key = st.sidebar.text_input("Chave do payload", value="").strip()
payload_value = st.sidebar.text_input("Contém (texto)", value="").strip()
payload_contains = None
if payload_key and payload_value:
    payload_contains = {payload_key: payload_value}

st.sidebar.divider()

# Ações: append / limpar
st.sidebar.subheader("Teste rápido: gravar evento")

event_type = st.sidebar.text_input("type", value="manual_test").strip() or "manual_test"

default_payload = {
    "run_id": "test-run",
    "origin": "CGH",
    "destination": "CWB",
    "value": 123.45,
    "note": "evento inserido manualmente",
}

payload_text = st.sidebar.text_area(
    "payload (JSON objeto)",
    value=json.dumps(default_payload, ensure_ascii=False, indent=2),
    height=190,
)

store = HistoryStore(store_name)

col_a, col_b = st.sidebar.columns(2)
with col_a:
    if st.button("➕ Append", use_container_width=True):
        try:
            payload = json.loads(payload_text) if payload_text.strip() else {}
            if not isinstance(payload, dict):
                st.sidebar.error("Payload precisa ser um JSON objeto (dict).")
            else:
                store.append(event_type=event_type, payload=payload)
                st.sidebar.success("Evento gravado ✅")
        except json.JSONDecodeError as e:
            st.sidebar.error(f"JSON inválido: {e}")

with col_b:
    if st.button("🧹 Clear", use_container_width=True):
        store.clear()
        st.sidebar.warning("Histórico apagado.")

# -------------------------------------------------------------------
# Snapshot
# -------------------------------------------------------------------
top1, top2 = st.columns([1, 2], gap="large")

with top1:
    st.subheader("📌 Snapshot")
    try:
        snap = build_dashboard_snapshot(store_name=store_name, days=days, type_filter=type_filter)
        st.metric("Total (janela)", snap.get("total_events", 0))
        st.write("**Por type**")
        st.json(snap.get("count_by_type", {}))
    except Exception as e:
        st.error("Erro ao calcular snapshot")
        st.exception(e)

with top2:
    st.subheader("📈 Série diária (count)")
    try:
        daily = snap.get("daily_counts", []) if "snap" in locals() else []
        if daily:
            series_dict = {d: c for d, c in daily}
            st.line_chart(series_dict)
        else:
            st.info("Sem eventos na janela.")
    except Exception as e:
        st.error("Erro ao plotar série diária")
        st.exception(e)

st.divider()

# -------------------------------------------------------------------
# Tabela detalhada
# -------------------------------------------------------------------
st.subheader("🧾 Eventos (tabela)")

try:
    rows = query_events_for_table(
        store_name=store_name,
        event_types=type_filter,
        days=days,
        payload_contains=payload_contains,
        limit=1000,
    )

    st.caption(
        f"Store: '{store_name}' | janela: {days} dias"
        + (f" | type={type_filter}" if type_filter else "")
        + (f" | contains {payload_key}~'{payload_value}'" if payload_contains else "")
    )

    if rows:
        st.dataframe(rows, use_container_width=True)
        st.download_button(
            "⬇️ Baixar JSONL filtrado",
            data="\n".join(json.dumps(r, ensure_ascii=False) for r in rows),
            file_name=f"{store_name}_events_{days}d.jsonl",
            mime="application/json",
        )
    else:
        st.info("Nada encontrado com os filtros atuais.")
except Exception as e:
    st.error("Erro ao carregar tabela de eventos")
    st.exception(e)

st.divider()

# -------------------------------------------------------------------
# Resumo numérico opcional (por chave)
# -------------------------------------------------------------------
st.subheader("🧮 Resumo numérico (opcional)")
st.write(
    "Se você tiver um campo numérico no payload (ex: `value`, `price`, `amount`), "
    "ele calcula soma/média/min/max na janela."
)

numeric_key = st.text_input("Chave numérica no payload", value="value").strip()

if st.button("Calcular resumo numérico"):
    try:
        events = load_events(store)
        events = last_n_days(events, days)
        if type_filter:
            events = filter_events(events, event_types=type_filter)

        summary = numeric_summary(events, numeric_key)
        st.json(summary)
    except Exception as e:
        st.error("Erro ao calcular resumo numérico")
        st.exception(e)

# -------------------------------------------------------------------
# Debug opcional (pra você ver paths no Cloud)
# -------------------------------------------------------------------
with st.expander("🛠 Debug (paths)"):
    st.write("APP_DIR:", str(APP_DIR))
    st.write("sys.path (primeiros 5):", sys.path[:5])
    st.write("Existe pasta utilitario?", (APP_DIR / "utilitario").exists())
    st.write("Arquivos em utilitario:", [p.name for p in (APP_DIR / "utilitario").glob("*")] if (APP_DIR / "utilitario").exists() else [])
