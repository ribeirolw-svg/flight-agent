from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st


# -----------------------------
# Paths
# -----------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"

STATE_FILE = DATA_DIR / "state.json"
BEST_FILE = DATA_DIR / "best_offers.json"
ALERTS_FILE = DATA_DIR / "alerts.json"
HISTORY_FILE = DATA_DIR / "history.jsonl"


# -----------------------------
# Helpers
# -----------------------------
def read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        txt = path.read_text(encoding="utf-8").strip()
        if not txt:
            return default
        return json.loads(txt)
    except Exception:
        return default


def read_jsonl(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
    except Exception:
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    return pd.json_normalize(rows)


def money(x: Any) -> str:
    try:
        if x is None:
            return "-"
        v = float(x)
        return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "-"


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def offer_price_from_history_row(row: Dict[str, Any]) -> Optional[float]:
    """
    Tenta extrair preço da linha do history.jsonl.
    Você salva a offer crua, então aqui tentamos o padrão do Amadeus: offer.price.grandTotal.
    """
    try:
        p = row.get("offer.price.grandTotal")
        if p is not None:
            return float(p)
    except Exception:
        pass
    try:
        p = row.get("offer.price.total")
        if p is not None:
            return float(p)
    except Exception:
        pass
    return None


def carrier_from_offer(row: Dict[str, Any]) -> str:
    # Amadeus: validatingAirlineCodes[0]
    try:
        vac = row.get("offer.validatingAirlineCodes")
        if isinstance(vac, list) and vac:
            return str(vac[0])
        if isinstance(vac, str) and vac:
            return vac
    except Exception:
        pass
    return "?"


def stops_from_offer(row: Dict[str, Any]) -> Optional[int]:
    # tenta inferir stops: itineraries[0].segments length - 1
    try:
        itins = row.get("offer.itineraries")
        if isinstance(itins, list) and itins:
            segs = itins[0].get("segments", [])
            if isinstance(segs, list):
                return max(0, len(segs) - 1)
    except Exception:
        pass
    return None


def dedupe_offers_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicatas comuns quando MAX_RESULTS=10 traz muitos resultados idênticos
    (ou quando a UI repete).
    """
    cols = [c for c in ["run_id", "route_key", "departure_date", "return_date", "offer.id", "offer.price.grandTotal"] if c in df.columns]
    if cols:
        return df.drop_duplicates(subset=cols, keep="first")
    return df.drop_duplicates(keep="first")


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Flight Agent — Dashboard", layout="wide")
st.title("✈️ Flight Agent — Dashboard")

state = read_json(STATE_FILE, {})
best = read_json(BEST_FILE, {})
alerts = read_json(ALERTS_FILE, {})
history_df = read_jsonl(HISTORY_FILE)

# Header metrics
run_id = (state or {}).get("run_id")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Run ID", str(run_id) if run_id else "-")
c2.metric("Offers saved", safe_int((state or {}).get("offers_saved"), 0))
c3.metric("OK calls", safe_int((state or {}).get("ok_calls"), 0))
c4.metric("Errors", safe_int((state or {}).get("err_calls"), 0))
c5.metric("Duration (sec)", safe_int((state or {}).get("duration_sec"), 0))

st.caption(
    f"Data dir: `{DATA_DIR}` | "
    f"best_offers.json: {'OK' if BEST_FILE.exists() else 'MISSING'} | "
    f"alerts.json: {'OK' if ALERTS_FILE.exists() else 'MISSING'}"
)

# Sidebar controls
st.sidebar.header("Filtros")

only_latest = st.sidebar.checkbox("Mostrar apenas o último run", value=True)

available_run_ids: List[str] = []
if not history_df.empty and "run_id" in history_df.columns:
    available_run_ids = sorted([x for x in history_df["run_id"].dropna().astype(str).unique().tolist()], reverse=True)

latest_run_from_state = str(run_id) if run_id else (available_run_ids[0] if available_run_ids else None)

selected_run = None
if available_run_ids:
    default_idx = 0
    if latest_run_from_state and latest_run_from_state in available_run_ids:
        default_idx = available_run_ids.index(latest_run_from_state)
    selected_run = st.sidebar.selectbox("Run ID", available_run_ids, index=default_idx)
else:
    selected_run = latest_run_from_state

# Apply run filter
filtered_history = history_df.copy()
if not filtered_history.empty and "run_id" in filtered_history.columns:
    if only_latest and latest_run_from_state:
        filtered_history = filtered_history[filtered_history["run_id"].astype(str) == str(latest_run_from_state)].copy()
    elif selected_run:
        filtered_history = filtered_history[filtered_history["run_id"].astype(str) == str(selected_run)].copy()

# Route filter options
route_options: List[str] = []
if not filtered_history.empty and "route_key" in filtered_history.columns:
    route_options = sorted([x for x in filtered_history["route_key"].dropna().astype(str).unique().tolist()])
route_filter = st.sidebar.selectbox("Rota (route_key)", ["(Todas)"] + route_options, index=0)

if route_filter != "(Todas)" and not filtered_history.empty and "route_key" in filtered_history.columns:
    filtered_history = filtered_history[filtered_history["route_key"].astype(str) == route_filter].copy()

# -----------------------------
# Alerts section
# -----------------------------
st.subheader("🔔 Alertas de preço")

alerts_list = (alerts or {}).get("alerts") or []
if not alerts_list:
    st.info("Nenhum alerta disparado no momento.")
else:
    adf = pd.json_normalize(alerts_list)
    st.dataframe(adf, width="stretch", height=220)

# -----------------------------
# Best offers section
# -----------------------------
st.subheader("🏆 Best Offer (sempre mostra)")

best_by_route = (best or {}).get("by_route") or {}
best_rows = []
for k, v in best_by_route.items():
    if not isinstance(v, dict):
        continue
    best_rows.append({
        "route_key": k,
        "origin": v.get("origin"),
        "destination": v.get("destination"),
        "adults": v.get("adults"),
        "children": v.get("children"),
        "departure_date": v.get("departure_date"),
        "return_date": v.get("return_date"),
        "carrier": v.get("carrier"),
        "stops": v.get("stops"),
        "price_total": v.get("price_total"),
        "note": v.get("note"),
    })

best_df = pd.DataFrame(best_rows)
if best_df.empty:
    st.warning("Nenhuma best offer disponível ainda (best_offers.json vazio ou sem rotas).")
else:
    # filtro destino no best
    dests = sorted([x for x in best_df["destination"].dropna().astype(str).unique().tolist()])
    dest_sel = st.selectbox("Destino", ["(Todos)"] + dests, index=0)
    view = best_df.copy()
    if dest_sel != "(Todos)":
        view = view[view["destination"].astype(str) == dest_sel].copy()

    view["price_total_fmt"] = view["price_total"].apply(money)
    cols = ["route_key","origin","destination","adults","children","departure_date","return_date","carrier","stops","price_total_fmt","note"]
    cols = [c for c in cols if c in view.columns]
    st.dataframe(view[cols], width="stretch", height=240)

# -----------------------------
# History section (offers)
# -----------------------------
st.subheader("📈 Histórico (amostra do history.jsonl)")

if filtered_history.empty:
    st.warning("Sem linhas no histórico para o filtro atual (ou history.jsonl vazio).")
else:
    # extrair um "price" útil pro gráfico/tabela
    df = filtered_history.copy()

    # garantir colunas básicas
    for col in ["departure_date","return_date","origin","destination","route_key","ts_utc"]:
        if col not in df.columns:
            df[col] = None

    # preço / cia / stops
    # (usa apply em dict-like via to_dict por linha; é mais robusto com normalize)
    as_dicts = df.to_dict(orient="records")
    prices = [offer_price_from_history_row(r) for r in as_dicts]
    carriers = [carrier_from_offer(r) for r in as_dicts]
    stops = [stops_from_offer(r) for r in as_dicts]

    df["price_total"] = prices
    df["carrier"] = carriers
    df["stops"] = stops

    # dedupe para não repetir 10x igual
    df = dedupe_offers_table(df)

    # tabela “ofertas”
    show_cols = [
        "ts_utc",
        "origin",
        "destination",
        "departure_date",
        "return_date",
        "carrier",
        "stops",
        "price_total",
    ]
    show_cols = [c for c in show_cols if c in df.columns]
    tdf = df[show_cols].copy()
    tdf["price_total"] = tdf["price_total"].apply(money)

    st.dataframe(tdf.sort_values(by=["ts_utc"], ascending=False).head(200), width="stretch", height=300)

    # gráfico (por departure/return)
    st.markdown("**Preço por data (menor preço por par ida/volta)**")
    g = df.dropna(subset=["price_total"]).copy()
    if g.empty:
        st.info("Sem preços válidos no histórico filtrado (price_total não encontrado nas offers).")
    else:
        g["pair"] = g["departure_date"].astype(str) + " → " + g["return_date"].astype(str)
        agg = g.groupby("pair", as_index=False)["price_total"].min().sort_values("price_total", ascending=True)
        st.bar_chart(agg.set_index("pair")["price_total"], width="stretch", height=220)

# -----------------------------
# Debug section
# -----------------------------
with st.expander("🧪 Debug — JSON carregados"):
    st.write(f"STATE_FILE: {STATE_FILE} exists? {STATE_FILE.exists()}")
    st.write(f"BEST_FILE: {BEST_FILE} exists? {BEST_FILE.exists()}")
    st.write(f"ALERTS_FILE: {ALERTS_FILE} exists? {ALERTS_FILE.exists()}")
    st.write(f"HISTORY_FILE: {HISTORY_FILE} exists? {HISTORY_FILE.exists()}")

    payload = {"state": state, "best": best, "alerts": alerts}
    st.json(payload)
