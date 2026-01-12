# app.py
from __future__ import annotations

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone

from history_store import ensure_jsonl, load_records, append_search_record, SearchRecord, utc_now_iso
from analytics import to_dataframe, apply_filters, summary_metrics, group_views

st.set_page_config(page_title="Flight Tracker", layout="wide")


@st.cache_data(show_spinner=False)
def load_df(limit: int | None = None) -> pd.DataFrame:
    ensure_jsonl()
    recs = load_records(limit=limit)
    return to_dataframe(recs)


def format_brl(v: float | None) -> str:
    if v is None:
        return "-"
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


st.title("✈️ Flight Tracker — Histórico & Insights")

with st.sidebar:
    st.header("⚙️ Config")

    limit = st.number_input("Carregar últimos N registros (0 = tudo)", min_value=0, value=2000, step=500)
    limit_val = None if limit == 0 else int(limit)

    refresh = st.button("🔄 Recarregar histórico")

    st.divider()
    st.header("🔎 Filtros")

df = load_df(limit=limit_val)

if refresh:
    st.cache_data.clear()
    df = load_df(limit=limit_val)

if df.empty:
    st.info("Sem histórico ainda. Assim que você salvar registros, eles aparecem aqui.")
    st.stop()

# opções para filtros
routes = ["(todas)"] + sorted(df["route"].dropna().unique().tolist()) if "route" in df.columns else ["(todas)"]
airlines = ["(todas)"] + sorted(df["best_airline"].dropna().unique().tolist()) if "best_airline" in df.columns else ["(todas)"]

with st.sidebar:
    sel_route = st.selectbox("Rota", routes, index=0)
    sel_airline = st.selectbox("Cia", airlines, index=0)

    if "direct_only" in df.columns:
        direct_opt = st.selectbox("Somente direto", ["(tanto faz)", "Sim", "Não"], index=0)
        direct_only = None if direct_opt == "(tanto faz)" else (direct_opt == "Sim")
    else:
        direct_only = None

    # janela de tempo
    days = st.slider("Janela (dias)", min_value=1, max_value=180, value=30, step=1)
    date_from = datetime.now(timezone.utc) - timedelta(days=int(days))
    date_to = datetime.now(timezone.utc)

route_val = None if sel_route == "(todas)" else sel_route
airline_val = None if sel_airline == "(todas)" else sel_airline

df_f = apply_filters(
    df,
    route=route_val,
    airline=airline_val,
    direct_only=direct_only,
    date_from=date_from,
    date_to=date_to,
)

m = summary_metrics(df_f)

# métricas topo
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Registros", m["rows"])
c2.metric("Menor preço", format_brl(m["best_price_min"]))
c3.metric("Preço médio", format_brl(m["best_price_avg"]))
c4.metric("Maior preço", format_brl(m["best_price_max"]))
trend = m["trend_pct"]
trend_txt = "-" if trend is None else f"{trend:+.1f}%"
c5.metric("Tendência", trend_txt)

if m["last_seen"]:
    st.caption(f"Última consulta no filtro: {m['last_seen']} (UTC)")

st.divider()

left, right = st.columns([1.2, 1])

with left:
    st.subheader("📋 Histórico filtrado (tabela calculável)")
    show_cols = [c for c in [
        "ts_utc", "origin", "destination", "route", "departure_date", "return_date",
        "best_airline", "best_price", "best_stops", "offers_count", "direct_only", "cabin", "currency", "provider"
    ] if c in df_f.columns]

    dshow = df_f[show_cols].sort_values("ts_utc", ascending=False)
    st.dataframe(dshow, use_container_width=True, height=520)

with right:
    st.subheader("📈 Visões rápidas")
    by_route, by_airline = group_views(df_f)

    if not by_route.empty:
        st.write("**Por rota**")
        st.dataframe(by_route, use_container_width=True, height=240)

    if not by_airline.empty:
        st.write("**Por cia**")
        st.dataframe(by_airline, use_container_width=True, height=240)

st.divider()

st.subheader("🧪 Exemplo: botão para gravar um registro (teste)")
st.caption("Use isso só pra validar persistência. Depois você substitui pela gravação real após a consulta no Amadeus.")

if st.button("➕ Adicionar registro fake"):
    fake = SearchRecord(
        ts_utc=utc_now_iso(),
        origin="CGH",
        destination="CWB",
        departure_date="2026-01-30",
        return_date="2026-02-02",
        adults=2,
        children=1,
        cabin="ECONOMY",
        currency="BRL",
        direct_only=True,
        best_price=412.00,
        best_airline="LATAM",
        best_stops=0,
        offers_count=10,
        run_id="demo",
        extra={"note": "registro fake para teste"},
    )
    append_search_record(fake)
    st.success("Gravado! Clica em 'Recarregar histórico'.")
