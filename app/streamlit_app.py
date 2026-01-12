import json
from datetime import datetime, timedelta, timezone

import streamlit as st

from utilitario import (
    HistoryStore,
    build_dashboard_snapshot,
    query_events_for_table,
)

st.set_page_config(page_title="Histórico & Analytics", layout="wide")

st.title("📚 Histórico & Analytics (JSONL)")

# -----------------------------
# Sidebar - Config geral
# -----------------------------
st.sidebar.header("Config")

store_name = st.sidebar.text_input("Nome do store", value="default")
days = st.sidebar.slider("Janela (últimos N dias)", min_value=1, max_value=365, value=30)

# Filtro por type (opcional)
type_filter_str = st.sidebar.text_input(
    "Filtrar por type (separar por vírgula, opcional)", value=""
).strip()
type_filter = None
if type_filter_str:
    type_filter = [t.strip() for t in type_filter_str.split(",") if t.strip()]

# Busca textual simples no payload
payload_search_key = st.sidebar.text_input(
    "Buscar no payload - chave (ex: origin, destination, run_id, error)", value=""
).strip()
payload_search_value = st.sidebar.text_input(
    "Buscar no payload - contém (texto)", value=""
).strip()

payload_contains = None
if payload_search_key and payload_search_value:
    # query_events_for_table usa paths diretos no payload (não prefixar com payload.)
    payload_contains = {payload_search_key: payload_search_value}

st.sidebar.divider()

# -----------------------------
# Área de teste - gravar evento manual
# -----------------------------
st.sidebar.subheader("Teste rápido: gravar evento")

event_type = st.sidebar.text_input("type do evento", value="manual_test").strip()

default_payload = {
    "run_id": "test-run",
    "note": "evento inserido manualmente",
    "value": 123.45,
}
payload_text = st.sidebar.text_area(
    "payload (JSON)",
    value=json.dumps(default_payload, ensure_ascii=False, indent=2),
    height=180,
)

store = HistoryStore(store_name)

if st.sidebar.button("➕ Append evento no histórico"):
    try:
        payload = json.loads(payload_text) if payload_text.strip() else {}
        if not isinstance(payload, dict):
            st.sidebar.error("Payload precisa ser um JSON objeto (dict).")
        else:
            store.append(event_type=event_type, payload=payload)
            st.sidebar.success("Evento gravado! ✅")
    except json.JSONDecodeError as e:
        st.sidebar.error(f"JSON inválido: {e}")

if st.sidebar.button("🧹 Limpar histórico (CUIDADO)"):
    store.clear()
    st.sidebar.warning("Histórico apagado.")

# -----------------------------
# Snapshot (métricas rápidas)
# -----------------------------
col1, col2 = st.columns([1, 2], gap="large")

with col1:
    st.subheader("📌 Snapshot")

    snap = build_dashboard_snapshot(
        store_name=store_name,
        days=days,
        type_filter=type_filter,
    )

    st.metric("Total de eventos (janela)", snap["total_events"])

    st.write("**Contagem por type**")
    if snap["count_by_type"]:
        st.json(snap["count_by_type"])
    else:
        st.info("Sem eventos no período.")

with col2:
    st.subheader("📈 Série diária (count)")
    daily = snap["daily_counts"]  # lista de (YYYY-MM-DD, count)
    if daily:
        # Streamlit aceita dict ou dataframe-like.
        # Vamos montar um dict com datas como índice.
        series_dict = {d: c for d, c in daily}
        st.line_chart(series_dict)
    else:
        st.info("Sem dados pra plotar.")

st.divider()

# -----------------------------
# Tabela detalhada + filtros avançados
# -----------------------------
st.subheader("🧾 Eventos (tabela)")

rows = query_events_for_table(
    store_name=store_name,
    event_types=type_filter,
    days=days,
    payload_contains=payload_contains,
    limit=1000,
)

st.caption(
    f"Mostrando até {min(len(rows), 1000)} evento(s) do store '{store_name}' "
    f"na janela de {days} dias"
    + (f" | type={type_filter}" if type_filter else "")
    + (f" | contains {payload_search_key}~'{payload_search_value}'" if payload_contains else "")
)

if rows:
    st.dataframe(rows, use_container_width=True)

    # Download
    st.download_button(
        "⬇️ Baixar JSON (linhas)",
        data="\n".join(json.dumps(r, ensure_ascii=False) for r in rows),
        file_name=f"{store_name}_events_{days}d.jsonl",
        mime="application/json",
    )
else:
    st.info("Nada encontrado com os filtros atuais.")

st.divider()

# -----------------------------
# Resumo numérico opcional (se existir campo no payload)
# -----------------------------
st.subheader("🧮 Resumo numérico (opcional)")

st.write(
    "Se você tiver um campo numérico no payload (ex: `price_total`, `value`, `amount`), "
    "dá pra tirar soma/média/min/max por janela e type."
)

numeric_key = st.text_input("Chave numérica no payload (ex: value, price, amount)", value="value").strip()

# Import só aqui pra manter o topo limpo
from utilitario.analytics import numeric_summary

if st.button("Calcular resumo numérico"):
    # Reaproveita a mesma janela/filtro
    # Carregamos os eventos via query_events_for_table é “achatado”, então vamos usar o store direto:
    from utilitario.analytics import load_events, last_n_days, filter_events

    events = load_events(store)
    events = last_n_days(events, days)
    if type_filter:
        events = filter_events(events, event_types=type_filter)

    summary = numeric_summary(events, numeric_key)
    st.json(summary)
