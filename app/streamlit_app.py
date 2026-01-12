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
from utilitario.analytics import query_events_for_table

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
                return float(v.strip().replace(",", "."))
            return float(v)
        except Exception:
            continue
    return None

def normalize_table(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Mínimas
    for c in ["ts_utc", "type"]:
        if c not in df.columns:
            df[c] = None

    # Colunas úteis esperadas (cria se não existir)
    for c in [
        "origin", "destination", "currency", "run_id",
        "offers_count", "direct_only", "error",
        "best_price", "adults", "children",
        "carrier_main", "carriers",
        "departure_date", "return_date", "cabin",
    ]:
        if c not in df.columns:
            df[c] = None

    # best_price fallback se não existir
    if df["best_price"].isna().all():
        df["best_price"] = df.apply(lambda r: pick_price(r.to_dict()), axis=1)

    # Tipos
    df["offers_count"] = pd.to_numeric(df["offers_count"], errors="coerce")
    df["best_price"] = pd.to_numeric(df["best_price"], errors="coerce")
    df["adults"] = pd.to_numeric(df["adults"], errors="coerce")
    df["children"] = pd.to_numeric(df["children"], errors="coerce")

    df["has_error"] = df["error"].apply(lambda x: bool(x) and str(x).strip().lower() not in ["none", "null", ""])
    df["route"] = df.apply(lambda r: f"{r.get('origin') or '-'} → {r.get('destination') or '-'}", axis=1)

    df["ts_utc_dt"] = pd.to_datetime(df["ts_utc"], errors="coerce", utc=True)
    df = df.sort_values("ts_utc_dt", ascending=False)

    return df

# -----------------------------
# Sidebar - Filtros
# -----------------------------
st.sidebar.header("Filtros")

store_name = st.sidebar.text_input("Store", value="default").strip() or "default"
days = st.sidebar.slider("Janela (dias)", 1, 365, 30)

# Default vazio pra não esconder tudo
type_filter_str = st.sidebar.text_input("Type (vírgula, opcional)", value="").strip()
type_filter = [t.strip() for t in type_filter_str.split(",") if t.strip()] if type_filter_str else None

origin = st.sidebar.text_input("Origin (ex: CGH)", value="").strip().upper()
destination = st.sidebar.text_input("Destination (ex: CWB)", value="").strip().upper()

# novos filtros
adults_filter = st.sidebar.selectbox("Adultos (opcional)", options=["(todos)", "1", "2", "3", "4"], index=0)
children_filter = st.sidebar.selectbox("Crianças (opcional)", options=["(todos)", "0", "1", "2", "3"], index=0)
carrier_filter = st.sidebar.text_input("CIA aérea (opcional, ex: G3)", value="").strip().upper()

only_errors = st.sidebar.checkbox("Somente com erro", value=False)
hide_errors = st.sidebar.checkbox("Ocultar com erro", value=False)

st.sidebar.divider()
st.sidebar.subheader("Teste manual")

store = HistoryStore(store_name)
if st.sidebar.button("Append exemplo (CGH→CWB)"):
    store.append(
        "flight_search",
        {
            "run_id": "manual-test",
            "origin": "CGH",
            "destination": "CWB",
            "departure_date": "2026-01-30",
            "return_date": None,
            "adults": 2,
            "children": 1,
            "cabin": "ECONOMY",
            "currency": "BRL",
            "offers_count": 12,
            "best_price": 399.90,
            "direct_only": True,
            "carriers": ["G3"],
            "carrier_main": "G3",
            "error": None,
        },
    )
    st.sidebar.success("Evento de teste gravado.")

# -----------------------------
# Carregar dados
# -----------------------------
rows = query_events_for_table(
    store_name=store_name,
    event_types=type_filter,
    days=days,
    limit=5000,
)
df = normalize_table(rows)

# aplica filtros
if not df.empty:
    if origin:
        df = df[df["origin"].astype(str).str.upper() == origin]
    if destination:
        df = df[df["destination"].astype(str).str.upper() == destination]

    if adults_filter != "(todos)":
        df = df[df["adults"].fillna(-1).astype(int) == int(adults_filter)]
    if children_filter != "(todos)":
        df = df[df["children"].fillna(-1).astype(int) == int(children_filter)]

    if carrier_filter:
        df = df[df["carrier_main"].astype(str).str.upper() == carrier_filter]

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
total_offers = int(df["offers_count"].fillna(0).sum()) if not df.empty else 0
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
    st.write("**Eventos por CIA aérea (carrier_main)**")
    if df.empty:
        st.info("Sem dados com os filtros atuais.")
    else:
        counts = df["carrier_main"].fillna("(sem)").value_counts().reset_index()
        counts.columns = ["carrier_main", "count"]
        st.bar_chart(counts.set_index("carrier_main"))

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
# Melhor preço por rota
# -----------------------------
st.subheader("💸 Melhor preço por rota")
if df.empty:
    st.info("Sem dados.")
elif df["best_price"].dropna().empty:
    st.info("Sem best_price ainda (rode o scheduler/search pra gravar).")
else:
    best = (
        df.dropna(subset=["best_price"])
          .groupby(["route", "carrier_main"], as_index=False)
          .agg(best_price=("best_price", "min"), samples=("best_price", "count"))
          .sort_values("best_price")
    )
    st.dataframe(best, use_container_width=True)

st.divider()

# -----------------------------
# Tabela limpa
# -----------------------------
st.subheader("🧾 Eventos (tabela limpa)")
if df.empty:
    st.info("Nada encontrado com os filtros atuais.")
else:
    cols = [
        "ts_utc",
        "run_id",
        "origin",
        "destination",
        "departure_date",
        "return_date",
        "adults",
        "children",
        "cabin",
        "carrier_main",
        "currency",
        "offers_count",
        "best_price",
        "direct_only",
        "error",
    ]
    cols = [c for c in cols if c in df.columns]
    st.dataframe(df[cols].head(2000), use_container_width=True)

    st.download_button(
        "⬇️ Baixar JSONL filtrado",
        data="\n".join(json.dumps(r, ensure_ascii=False) for r in df[cols].to_dict(orient="records")),
        file_name=f"{store_name}_filtered_{days}d.jsonl",
        mime="application/json",
    )

st.divider()

# -----------------------------
# Diagnóstico (data/jsonl)
# -----------------------------
st.subheader("🔎 Diagnóstico rápido (data/jsonl)")

data_dir = Path("data")
st.write("Diretório data existe?", data_dir.exists())
if data_dir.exists():
    files = sorted([p.name for p in data_dir.glob("*.jsonl")])
    st.write("Arquivos JSONL em /data:", files)

    store_path = data_dir / f"{store_name}.jsonl"
    st.write("Arquivo do store atual:", str(store_path))
    st.write("Existe?", store_path.exists())

    if store_path.exists():
        st.write("Tamanho (bytes):", store_path.stat().st_size)
        try:
            lines = store_path.read_text(encoding="utf-8").strip().splitlines()
            st.write("Linhas no store:", len(lines))
            if lines:
                st.write("Última linha (raw):")
                st.code(lines[-1][:2000])
        except Exception as e:
            st.exception(e)
else:
    st.warning("Sem pasta data — seu app ainda não gravou nada.")
