from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

APP_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from utilitario.history_store import HistoryStore
from utilitario.analytics import build_dashboard_snapshot, query_events_for_table


st.set_page_config(page_title="Flight Agent", layout="wide")
st.title("✈️ Flight Agent — Dashboard")

st.sidebar.header("Filtros")
store_name = st.sidebar.text_input("Store", "default").strip() or "default"
days = st.sidebar.slider("Janela (dias)", 1, 365, 30)

type_filter_str = st.sidebar.text_input("Type (vírgula, opcional)", "flight_search").strip()
type_filter = [t.strip() for t in type_filter_str.split(",") if t.strip()] if type_filter_str else None

origin = st.sidebar.text_input("Origin", "").strip().upper()
destination = st.sidebar.text_input("Destination", "").strip().upper()
only_errors = st.sidebar.checkbox("Somente erros", False)

# Carrega dados
rows = query_events_for_table(store_name=store_name, event_types=type_filter, days=days, limit=5000)
df = pd.DataFrame(rows) if rows else pd.DataFrame()

if not df.empty:
    # normaliza colunas esperadas
    for c in ["origin", "destination", "currency", "offers_count", "best_price", "error", "direct_only", "run_id", "ts_utc"]:
        if c not in df.columns:
            df[c] = None

    df["offers_count"] = pd.to_numeric(df["offers_count"], errors="coerce")
    df["best_price"] = pd.to_numeric(df["best_price"], errors="coerce")
    df["has_error"] = df["error"].apply(lambda x: bool(x) and str(x).strip().lower() not in ["none", "null", ""])

    if origin:
        df = df[df["origin"].astype(str).str.upper() == origin]
    if destination:
        df = df[df["destination"].astype(str).str.upper() == destination]
    if only_errors:
        df = df[df["has_error"] == True]

    df["ts_utc_dt"] = pd.to_datetime(df["ts_utc"], errors="coerce", utc=True)
    df = df.sort_values("ts_utc_dt", ascending=False)

# KPIs
st.subheader("📌 Resumo")
c1, c2, c3, c4, c5 = st.columns(5)

total = int(df.shape[0]) if not df.empty else 0
errors = int(df["has_error"].sum()) if not df.empty else 0
last_ts = df["ts_utc"].iloc[0] if not df.empty else "-"
offers_sum = int(df["offers_count"].fillna(0).sum()) if not df.empty else 0
best_min = float(df["best_price"].min()) if (not df.empty and df["best_price"].notna().any()) else None

c1.metric("Eventos", total)
c2.metric("Erros", errors)
c3.metric("Último evento (UTC)", last_ts)
c4.metric("Ofertas (soma)", offers_sum)
c5.metric("Melhor preço (min)", "-" if best_min is None else f"{best_min:,.2f}")

# Gráficos
st.subheader("📈 Evolução do melhor preço")
if df.empty or df["best_price"].dropna().empty:
    st.info("Sem preços ainda. Rode o scheduler/busca para gravar eventos com best_price.")
else:
    price_ts = (
        df.dropna(subset=["ts_utc_dt", "best_price"])
          .sort_values("ts_utc_dt")
          .set_index("ts_utc_dt")["best_price"]
    )
    st.line_chart(price_ts)

# Melhor por rota
st.subheader("💸 Melhor preço por rota")
if df.empty or df["best_price"].dropna().empty:
    st.info("Sem dados suficientes.")
else:
    df["route"] = df["origin"].astype(str) + " → " + df["destination"].astype(str)
    best_routes = (
        df.dropna(subset=["best_price"])
          .groupby("route", as_index=False)
          .agg(best_price=("best_price", "min"), samples=("best_price", "count"))
          .sort_values("best_price")
    )
    st.dataframe(best_routes, use_container_width=True)

# Tabela limpa
st.subheader("🧾 Eventos (limpo)")
if df.empty:
    st.info("Nada encontrado com os filtros atuais.")
else:
    cols = ["ts_utc", "origin", "destination", "currency", "offers_count", "best_price", "direct_only", "error", "run_id"]
    cols = [c for c in cols if c in df.columns]
    st.dataframe(df[cols].head(1000), use_container_width=True)

    st.download_button(
        "⬇️ Baixar JSONL filtrado",
        data="\n".join(json.dumps(r, ensure_ascii=False) for r in df[cols].to_dict(orient="records")),
        file_name=f"{store_name}_filtered_{days}d.jsonl",
        mime="application/json",
    )

# Teste manual (opcional)
st.sidebar.divider()
st.sidebar.subheader("Teste manual: append")
store = HistoryStore(store_name)
if st.sidebar.button("Append evento fake de exemplo"):
    store.append(
        "flight_search",
        {
            "run_id": "manual-example",
            "origin": "CGH",
            "destination": "CWB",
            "currency": "BRL",
            "offers_count": 10,
            "best_price": 399.90,
            "direct_only": True,
            "error": None,
        },
    )
    st.sidebar.success("Gravado.")
