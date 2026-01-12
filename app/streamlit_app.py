from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

# --- garantir que /app está no sys.path (cloud-safe)
APP_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from utilitario.history_store import HistoryStore
from utilitario.analytics import (
    load_events,
    filter_events,
    last_n_days,
    count_by_type,
    query_events_for_table,
)

st.set_page_config(page_title="Flight Agent", layout="wide")

st.title("✈️ Flight Agent — Histórico & Insights")

# -----------------------------
# Helpers
# -----------------------------
PRICE_KEYS = ["best_price", "price", "total_price", "min_price", "amount", "valor", "preco", "price_total"]

def pick_price(row: Dict[str, Any]) -> Optional[float]:
    for k in PRICE_KEYS:
        v = row.get(k)
        if v is None:
            continue
        try:
            if isinstance(v, str):
                v2 = v.strip().replace(",", ".")
                return float(v2)
            return float(v)
        except Exception:
            continue
    return None

def normalize_table(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Garantir colunas mínimas
    for c in ["ts_utc", "type"]:
        if c not in df.columns:
            df[c] = None

    # Criar preço “best_price” se não existir
    if "best_price" not in df.columns:
        df["best_price"] = df.apply(lambda r: pick_price(r.to_dict()), axis=1)

    # Padronizar algumas colunas comuns
    for c in ["origin", "destination", "currency", "run_id", "offers_count", "direct_only", "error"]:
        if c not in df.columns:
            df[c] = None

    # ofertas_count para numérico
    if "offers_count" in df.columns:
        df["offers_count"] = pd.to_numeric(df["offers_count"], errors="coerce")

    # best_price para numérico
    if "best_price" in df.columns:
        df["best_price"] = pd.to_numeric(df["best_price"], errors="coerce")

    # erro booleano
    df["has_error"] = df["error"].apply(lambda x: bool(x) and str(x).strip().lower() not in ["none", "null", ""])

    # rota
    df["route"] = df.apply(
        lambda r: f"{r.get('origin') or '-'} → {r.get('destination') or '-'}",
        axis=1,
    )

    # ts_utc parse
    df["ts_utc_dt"] = pd.to_datetime(df["ts_utc"], errors="coerce", utc=True)

    # Ordena por data
    df = df.sort_values("ts_utc_dt", ascending=False)

    return df

# -----------------------------
# Sidebar - Filtros
# -----------------------------
st.sidebar.header("Filtros")

store_name = st.sidebar.text_input("Store", value="default").strip() or "default"
days = st.sidebar.slider("Janela (dias)", 1, 365, 30)

# type
type_filter_str = st.sidebar.text_input("Type (vírgula, opcional)", value="").strip()
type_filter = [t.strip() for t in type_filter_str.split(",") if t.strip()] if type_filter_str else None

# rota
origin = st.sidebar.text_input("Origin (ex: CGH)", value="").strip().upper()
destination = st.sidebar.text_input("Destination (ex: CWB)", value="").strip().upper()

# erros
only_errors = st.sidebar.checkbox("Somente com erro", value=False)
hide_errors = st.sidebar.checkbox("Ocultar com erro", value=False)

st.sidebar.divider()

# -----------------------------
# Carregar dados
# -----------------------------
store = HistoryStore(store_name)

# usando query_events_for_table (mais simples pro dataframe)
rows = query_events_for_table(
    store_name=store_name,
    event_types=type_filter,
    days=days,
    payload_contains=None,  # filtros finos fazemos via pandas abaixo
    limit=5000,
)

df = normalize_table(rows)

# aplica filtros de rota
if not df.empty:
    if origin:
        df = df[df["origin"].astype(str).str.upper() == origin]
    if destination:
        df = df[df["destination"].astype(str).str.upper() == destination]

    if only_errors:
        df = df[df["has_error"] == True]
    if hide_errors:
        df = df[df["has_error"] == False]

# -----------------------------
# KPIs
# -----------------------------
st.subheader("📌 Resumo")

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

total_events = int(df.shape[0]) if not df.empty else 0
total_errors = int(df["has_error"].sum()) if not df.empty else 0
last_ts = df["ts_utc"].iloc[0] if not df.empty else None

total_offers = int(df["offers_count"].fillna(0).sum()) if (not df.empty and "offers_count" in df.columns) else 0
avg_best_price = float(df["best_price"].dropna().mean()) if (not df.empty and df["best_price"].notna().any()) else None

kpi1.metric("Eventos", f"{total_events}")
kpi2.metric("Erros", f"{total_errors}")
kpi3.metric("Último evento (UTC)", last_ts or "-")
kpi4.metric("Ofertas (soma)", f"{total_offers}")
kpi5.metric("Preço médio", "-" if avg_best_price is None else f"{avg_best_price:,.2f}")

# -----------------------------
# Charts
# -----------------------------
c1, c2 = st.columns([1, 1], gap="large")

with c1:
    st.write("**Eventos por type**")
    if df.empty:
        st.info("Sem dados com os filtros atuais.")
    else:
        counts = df["type"].value_counts().reset_index()
        counts.columns = ["type", "count"]
        st.bar_chart(counts.set_index("type"))

with c2:
    st.write("**Eventos por dia**")
    if df.empty or df["ts_utc_dt"].isna().all():
        st.info("Sem datas válidas para plotar.")
    else:
        per_day = (
            df.dropna(subset=["ts_utc_dt"])
              .assign(day=lambda x: x["ts_utc_dt"].dt.date.astype(str))
              .groupby("day")["type"]
              .count()
              .reset_index()
              .rename(columns={"type": "count"})
              .sort_values("day")
        )
        st.line_chart(per_day.set_index("day"))

st.divider()

# -----------------------------
# Melhores preços por rota (se tiver preço)
# -----------------------------
st.subheader("💸 Melhor preço por rota (se disponível)")
if df.empty:
    st.info("Sem dados.")
elif df["best_price"].dropna().empty:
    st.info("Não encontrei campo de preço no payload. Se você me disser qual chave guarda o preço, eu ajusto.")
else:
    best = (
        df.dropna(subset=["best_price"])
          .groupby("route", as_index=False)
          .agg(best_price=("best_price", "min"), samples=("best_price", "count"))
          .sort_values("best_price")
    )
    st.dataframe(best, use_container_width=True)

st.divider()

# -----------------------------
# Tabela principal (limpa)
# -----------------------------
st.subheader("🧾 Eventos (tabela limpa)")

if df.empty:
    st.info("Nada encontrado com os filtros atuais.")
else:
    # seleciona colunas úteis (se existirem)
    cols = [
        "ts_utc",
        "type",
        "run_id",
        "origin",
        "destination",
        "currency",
        "offers_count",
        "best_price",
        "direct_only",
        "error",
    ]
    cols = [c for c in cols if c in df.columns]

    st.dataframe(df[cols].head(1000), use_container_width=True)

    st.download_button(
        "⬇️ Baixar JSONL filtrado",
        data="\n".join(json.dumps(r, ensure_ascii=False) for r in df.drop(columns=["ts_utc_dt"], errors="ignore").to_dict(orient="records")),
        file_name=f"{store_name}_filtered_{days}d.jsonl",
        mime="application/json",
    )
