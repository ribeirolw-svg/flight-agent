from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

# --- paths
APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# utilitário (mantemos, mas agora o principal é ler data/history.jsonl)
from utilitario.history_store import HistoryStore
from utilitario.analytics import query_events_for_table

st.set_page_config(page_title="Flight Agent", layout="wide")
st.title("✈️ Flight Agent — Histórico & Insights")

DATA_DIR = Path("data")
STATE_PATH = DATA_DIR / "state.json"
HISTORY_PATH = DATA_DIR / "history.jsonl"


# -----------------------------
# Timezone helpers (execuções)
# -----------------------------
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

TZ_SP = ZoneInfo("America/Sao_Paulo") if ZoneInfo else timezone(timedelta(hours=-3))
SCHEDULE_HOUR_LOCAL = 6
SCHEDULE_MIN_LOCAL = 15
CRON_EXPECTED = "15 9 * * *"  # 09:15 UTC = 06:15 BRT


def parse_iso_any(s: Any) -> Optional[datetime]:
    if not s:
        return None
    try:
        s = str(s).strip()
        if not s:
            return None
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def next_runs_local(n: int = 7) -> List[datetime]:
    now_local = datetime.now(TZ_SP)
    candidate = now_local.replace(hour=SCHEDULE_HOUR_LOCAL, minute=SCHEDULE_MIN_LOCAL, second=0, microsecond=0)
    if candidate <= now_local:
        candidate += timedelta(days=1)
    runs = []
    for _ in range(n):
        runs.append(candidate)
        candidate += timedelta(days=1)
    return runs


# -----------------------------
# Load persisted state/history
# -----------------------------
def read_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def read_history_jsonl(limit_lines: int = 20000) -> List[Dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        # lê as últimas N linhas para não explodir memória
        lines = HISTORY_PATH.read_text(encoding="utf-8").splitlines()
        if len(lines) > limit_lines:
            lines = lines[-limit_lines:]
        for ln in lines:
            ln = ln.strip()
            if not ln:
                continue
            try:
                rows.append(json.loads(ln))
            except Exception:
                continue
    except Exception:
        return []
    return rows


def normalize_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # colunas esperadas
    expected = [
        "ts_utc", "type", "source", "store_name", "route_name",
        "run_id", "origin", "destination",
        "departure_date", "return_date",
        "adults", "children", "cabin",
        "currency", "direct_only",
        "offers_count", "best_price",
        "carriers", "carrier_main",
        "elapsed_s", "error",
    ]
    for c in expected:
        if c not in df.columns:
            df[c] = None

    df["ts_utc_dt"] = pd.to_datetime(df["ts_utc"], errors="coerce", utc=True)
    df["offers_count"] = pd.to_numeric(df["offers_count"], errors="coerce")
    df["best_price"] = pd.to_numeric(df["best_price"], errors="coerce")
    df["elapsed_s"] = pd.to_numeric(df["elapsed_s"], errors="coerce")
    df["adults"] = pd.to_numeric(df["adults"], errors="coerce")
    df["children"] = pd.to_numeric(df["children"], errors="coerce")

    df["has_error"] = df["error"].apply(lambda x: bool(x) and str(x).strip().lower() not in ["none", "null", ""])
    df["route"] = df.apply(lambda r: f"{(r.get('origin') or '-')} → {(r.get('destination') or '-')}", axis=1)

    df = df.sort_values("ts_utc_dt", ascending=False)
    return df


# -----------------------------
# Sidebar controls
# -----------------------------
st.sidebar.header("Filtros")

days = st.sidebar.slider("Janela (dias)", 1, 365, 30)

route_name = st.sidebar.text_input("Route name (ex: Roma, Curitiba)", value="").strip()
origin = st.sidebar.text_input("Origin (ex: CGH)", value="").strip().upper()
destination = st.sidebar.text_input("Destination (ex: CWB, NVT, FCO)", value="").strip().upper()
carrier = st.sidebar.text_input("CIA aérea (ex: G3)", value="").strip().upper()

adults_filter = st.sidebar.selectbox("Adultos", ["(todos)", "1", "2", "3", "4", "5", "6", "7", "8", "9"], 0)
children_filter = st.sidebar.selectbox("Crianças", ["(todos)", "0", "1", "2", "3", "4", "5"], 0)

only_errors = st.sidebar.checkbox("Somente com erro", value=False)
hide_errors = st.sidebar.checkbox("Ocultar com erro", value=False)

st.sidebar.divider()

# botão opcional (manual) para gravar no store local (não persistente no GitHub automaticamente)
st.sidebar.subheader("Teste manual (store local)")
store_name = st.sidebar.text_input("Store (local)", value="default").strip() or "default"
store = HistoryStore(store_name)
if st.sidebar.button("Append exemplo local"):
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
            "elapsed_s": 0.0,
            "error": None,
        },
    )
    st.sidebar.success("Evento local gravado.")

st.sidebar.divider()

# -----------------------------
# Execuções (state.json + cron)
# -----------------------------
st.subheader("🗓️ Execuções do Scheduler")

state = read_state()
last_run_utc = parse_iso_any(state.get("last_run_utc"))
last_success_utc = parse_iso_any(state.get("last_success_utc"))
last_status = state.get("last_status") or "-"
last_error = state.get("last_error") or ""
last_summary = state.get("last_summary") or ""

c1, c2, c3, c4 = st.columns(4)
c1.metric("Status", str(last_status))
c2.metric("Último run (BRT)", last_run_utc.astimezone(TZ_SP).strftime("%Y-%m-%d %H:%M") if last_run_utc else "-")
c3.metric("Último sucesso (BRT)", last_success_utc.astimezone(TZ_SP).strftime("%Y-%m-%d %H:%M") if last_success_utc else "-")
c4.metric("Cron", CRON_EXPECTED)

if last_summary:
    st.caption(f"Resumo: {last_summary}")
if last_error.strip():
    st.error(f"Último erro: {last_error}")

runs = next_runs_local(7)
st.dataframe(
    pd.DataFrame(
        [
            {
                "Execução (BRT)": r.strftime("%Y-%m-%d %H:%M"),
                "Execução (UTC)": r.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            }
            for r in runs
        ]
    ),
    use_container_width=True,
)

st.divider()

# -----------------------------
# Dados persistidos (history.jsonl)
# -----------------------------
rows = read_history_jsonl()
df = normalize_df(rows)

# filtra por janela
if not df.empty and df["ts_utc_dt"].notna().any():
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    df = df[df["ts_utc_dt"] >= cutoff]

# aplica filtros
if not df.empty:
    if route_name:
        df = df[df["route_name"].astype(str).str.contains(route_name, case=False, na=False)]
    if origin:
        df = df[df["origin"].astype(str).str.upper() == origin]
    if destination:
        df = df[df["destination"].astype(str).str.upper() == destination]
    if carrier:
        df = df[df["carrier_main"].astype(str).str.upper() == carrier]

    if adults_filter != "(todos)":
        df = df[df["adults"].fillna(-1).astype(int) == int(adults_filter)]
    if children_filter != "(todos)":
        df = df[df["children"].fillna(-1).astype(int) == int(children_filter)]

    if only_errors:
        df = df[df["has_error"] == True]
    if hide_errors:
        df = df[df["has_error"] == False]

# KPIs
st.subheader("📌 Resumo (histórico persistido)")

k1, k2, k3, k4, k5 = st.columns(5)
total_events = int(df.shape[0]) if not df.empty else 0
total_errors = int(df["has_error"].sum()) if not df.empty else 0
last_event = df["ts_utc_dt"].iloc[0] if (not df.empty and df["ts_utc_dt"].notna().any()) else None
offers_sum = int(df["offers_count"].fillna(0).sum()) if not df.empty else 0
best_seen = float(df["best_price"].min()) if (not df.empty and df["best_price"].notna().any()) else None

k1.metric("Eventos", f"{total_events}")
k2.metric("Erros", f"{total_errors}")
k3.metric("Último evento (BRT)", last_event.astimezone(TZ_SP).strftime("%Y-%m-%d %H:%M") if last_event else "-")
k4.metric("Ofertas (soma)", f"{offers_sum}")
k5.metric("Menor preço", "-" if best_seen is None else f"{best_seen:,.2f}")

# Tabela por rota/destino/cia
st.subheader("💸 Melhor preço por rota / CIA")
if df.empty or df["best_price"].dropna().empty:
    st.info("Sem preços no período/filtros.")
else:
    best = (
        df.dropna(subset=["best_price"])
          .groupby(["route_name", "origin", "destination", "carrier_main"], as_index=False)
          .agg(best_price=("best_price", "min"), samples=("best_price", "count"))
          .sort_values("best_price")
    )
    st.dataframe(best, use_container_width=True)

# Eventos recentes
st.subheader("🧾 Eventos recentes")
if df.empty:
    st.info("Sem eventos (verifique se data/history.jsonl existe e está sendo commitado).")
else:
    cols = [
        "ts_utc",
        "route_name",
        "origin",
        "destination",
        "departure_date",
        "return_date",
        "adults",
        "children",
        "carrier_main",
        "offers_count",
        "best_price",
        "elapsed_s",
        "error",
    ]
    cols = [c for c in cols if c in df.columns]
    st.dataframe(df[cols].head(2000), use_container_width=True)

    st.download_button(
        "⬇️ Baixar CSV filtrado",
        data=df[cols].to_csv(index=False),
        file_name=f"flight_history_{days}d.csv",
        mime="text/csv",
    )

# Diagnóstico (arquivos)
st.divider()
st.subheader("🔎 Diagnóstico (arquivos persistidos)")
st.write("state.json existe?", STATE_PATH.exists(), str(STATE_PATH))
st.write("history.jsonl existe?", HISTORY_PATH.exists(), str(HISTORY_PATH))
if HISTORY_PATH.exists():
    try:
        size = HISTORY_PATH.stat().st_size
        st.write("history.jsonl tamanho (bytes):", size)
    except Exception:
        pass
