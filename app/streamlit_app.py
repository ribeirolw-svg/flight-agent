# app/streamlit_app.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import streamlit as st


# =========================
# Paths (sempre repo root)
# =========================
REPO_ROOT = Path(__file__).resolve().parents[1]  # .../flight-agent
DATA_DIR = REPO_ROOT / "data"

STATE_FILE = DATA_DIR / "state.json"
SUMMARY_FILE = DATA_DIR / "summary.md"
BEST_FILE = DATA_DIR / "best_offers.json"
ALERTS_FILE = DATA_DIR / "alerts.json"
HISTORY_FILE = DATA_DIR / "history.jsonl"


# =========================
# Helpers
# =========================
def load_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        txt = path.read_text(encoding="utf-8").strip()
        if not txt:
            return default
        return json.loads(txt)
    except Exception:
        return default


def fmt_money(x: Any, currency: str = "BRL") -> str:
    try:
        if x is None:
            return "—"
        v = float(x)
        # formato BR simples
        s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"{currencycurrency(currency)} {s}"
    except Exception:
        return "—"


def RQ(x: str) -> str:
    # small helper for markdown code rendering
    return f"`{x}`"


def parse_history_tail(path: Path, max_lines: int = 20000) -> List[Dict[str, Any]]:
    """Lê o history.jsonl (últimas N linhas) para gráficos básicos, sem explodir memória."""
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as f:
            # lê tudo (em repos pequenos é ok); se ficar grande, a gente otimiza depois
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
        if len(rows) > max_lines:
            rows = rows[-max_lines:]
        return rows
    except Exception:
        return []


def currency_symbol(cur: str) -> str:
    cur = (cur or "").upper()
    if cur == "BRL":
        return "R$"
    if cur == "USD":
        return "$"
    if cur == "EUR":
        return "€"
    return cur


def get_price_from_offer(offer: Dict[str, Any]) -> float | None:
    """
    Ajuda o gráfico com o mínimo de dependência do normalize do scheduler.
    """
    # schema simplificado
    for k in ("price_total", "total_price", "price", "total"):
        if k in offer and offer.get(k) is not None:
            try:
                return float(offer[k])
            except Exception:
                pass
    # schema amadeus raw
    try:
        p = offer.get("price") or {}
        if isinstance(p, dict) and p.get("grandTotal") is not None:
            return float(p["grandTotal"])
    except Exception:
        pass
    return None


# =========================
# Page
# =========================
st.set_page_config(
    page_title="Flight Agent Dashboard",
    layout="wide",
)

st.title("✈️ Flight Agent — Dashboard")

# Sidebar
st.sidebar.header("Filtros")
show_debug = st.sidebar.toggle("Mostrar debug (JSON)", value=False)
only_alerts = st.sidebar.toggle("Mostrar apenas rotas com alerta", value=False)

# Load data
state = load_json(STATE_FILE, {})
best = load_json(BEST_FILE, {"by_route": {}})
alerts_payload = load_json(ALERTS_FILE, {"alerts": []})

by_route: Dict[str, Dict[str, Any]] = best.get("by_route") or {}
alerts: List[Dict[str, Any]] = alerts_payload.get("alerts") or []

# =========================
# Status / Run summary
# =========================
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Run ID", state.get("run_id", "—"))
with col2:
    st.metric("Offers saved", str(state.get("offers_saved", "—")))
with col3:
    st.metric("OK calls", str(state.get("ok_calls", "—")))
with col4:
    st.metric("Errors", str(state.get("err_calls", "—")))

st.caption(
    f"Data dir: {DATA_DIR} | best_offers.json: {'OK' if BEST_FILE.exists() else 'MISSING'} | alerts.json: {'OK' if ALERTS_FILE.exists() else 'MISSING'}"
)

# =========================
# Alerts
# =========================
st.subheader("🔔 Alertas de preço")

if not alerts:
    st.info("Nenhum alerta disparado no momento.")
else:
    rows_a = []
    for a in alerts:
        rows_a.append(
            {
                "Tipo": a.get("type"),
                "Rota": a.get("route_key"),
                "Mensagem": a.get("message"),
                "Preço atual": a.get("current_price"),
                "Alvo": a.get("target_price"),
                "Queda %": a.get("delta_pct"),
                "Cia": a.get("carrier"),
                "Stops": a.get("stops"),
                "Ida": a.get("departure_date"),
                "Volta": a.get("return_date"),
                "Pax": f'{a.get("adults","—")}A + {a.get("children","—")}C' if a.get("adults") is not None else "—",
            }
        )
    dfa = pd.DataFrame(rows_a)
    # formata valores
    if "Preço atual" in dfa.columns:
        dfa["Preço atual"] = dfa["Preço atual"].apply(lambda x: fmt_money(x, "BRL") if pd.notna(x) else "—")
    if "Alvo" in dfa.columns:
        dfa["Alvo"] = dfa["Alvo"].apply(lambda x: fmt_money(x, "BRL") if pd.notna(x) else "—")
    st.dataframe(dfa, width="stretch", hide_index=True)

# =========================
# Best Offers
# =========================
st.subheader("🏆 Best Offer (sempre mostra)")

# destinos disponíveis (com base no best_offers.json)
all_dests = sorted(
    {b.get("destination") for b in by_route.values() if isinstance(b, dict) and b.get("destination")}
)
dest_options = ["(Todos)"] + all_dests
sel_dest = st.selectbox("Destino", dest_options, index=0)

# mapa rápido: quais route_keys estão em alerta (se o toggle estiver ligado)
alert_route_keys = {a.get("route_key") for a in alerts if isinstance(a, dict)}

rows = []
for rk, b in by_route.items():
    if not isinstance(b, dict):
        continue

    dest = b.get("destination")
    if sel_dest != "(Todos)" and dest != sel_dest:
        continue

    if only_alerts and rk not in alert_route_keys:
        continue

    adults = b.get("adults")
    children = b.get("children")
    pax = "—"
    if adults is not None:
        pax = f'{int(adults)}A + {int(children or 0)}C'

    price = b.get("price_total")
    has_price = price is not None

    rows.append(
        {
            "ID": b.get("id") or rk,
            "Origem": b.get("origin") or "—",
            "Destino": b.get("destination") or "—",
            "Pax": pax,
            "Ida": b.get("departure_date") or "—",
            "Volta": b.get("return_date") or "—",
            "Cia": b.get("carrier") or "—",
            "Stops": b.get("stops") if b.get("stops") is not None else "—",
            "Preço (Total)": float(price) if has_price else None,
            "Status": "OK" if has_price else (b.get("note") or "SEM BEST"),
        }
    )

if not rows:
    st.info("Nenhuma rota para exibir com o filtro atual.")
else:
    df = pd.DataFrame(rows)

    # ordena: com preço primeiro, depois sem preço
    df["__has_price"] = df["Preço (Total)"].notna()
    df = df.sort_values(["__has_price", "Preço (Total)"], ascending=[False, True]).drop(columns=["__has_price"])

    # moeda
    df["Preço (Total)"] = df["Preço (Total)"].apply(lambda x: fmt_money(x, "BRL") if pd.notna(x) else "—")

    st.dataframe(df, width="stretch", hide_index=True)

    # Observações úteis
    st.caption(
        "Nota: para rotas com `no_offers_after_filters`, o problema normalmente é normalização de preço no scheduler (não é inexistência de voo)."
    )

# =========================
# Histórico (opcional)
# =========================
st.subheader("📈 Histórico (amostra do history.jsonl)")

hist = parse_history_tail(HISTORY_FILE, max_lines=15000)
if not hist:
    st.info("history.jsonl não encontrado (ou vazio).")
else:
    # seleciona rota pelo mesmo route_key (id)
    keys = sorted({r.get("route_key") for r in hist if isinstance(r, dict) and r.get("route_key")})
    if keys:
        sel_key = st.selectbox("Rota (route_key)", keys, index=0)
        filtered = [r for r in hist if r.get("route_key") == sel_key]

        # extrai preços e timestamps
        rows_h = []
        for r in filtered:
            offer = r.get("offer") or {}
            if not isinstance(offer, dict):
                continue
            p = get_price_from_offer(offer)
            if p is None:
                continue
            rows_h.append(
                {
                    "ts_utc": r.get("ts_utc"),
                    "origin": r.get("origin"),
                    "destination": r.get("destination"),
                    "departure_date": r.get("departure_date"),
                    "return_date": r.get("return_date"),
                    "price_total": p,
                }
            )
        if not rows_h:
            st.warning("Sem preços legíveis no histórico para essa rota (provável schema/normalização).")
        else:
            dfh = pd.DataFrame(rows_h)
            dfh["ts_utc"] = pd.to_datetime(dfh["ts_utc"], errors="coerce")
            dfh = dfh.dropna(subset=["ts_utc"]).sort_values("ts_utc")

            st.line_chart(dfh.set_index("ts_utc")["price_total"])
            st.dataframe(dfh.tail(50), width="stretch", hide_index=True)
    else:
        st.info("Nenhuma rota encontrada no history.jsonl.")

# =========================
# Debug
# =========================
if show_debug:
    st.subheader("🧪 Debug — JSON carregados")
    st.write("STATE_FILE:", str(STATE_FILE), "exists?", STATE_FILE.exists())
    st.write("BEST_FILE:", str(BEST_FILE), "exists?", BEST_FILE.exists())
    st.write("ALERTS_FILE:", str(ALERTS_FILE), "exists?", ALERTS_FILE.exists())
    st.write("HISTORY_FILE:", str(HISTORY_FILE), "exists?", HISTORY_FILE.exists())
    st.json({"state": state, "best": best, "alerts": alerts_payload})
