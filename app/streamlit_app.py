from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

import pandas as pd
import streamlit as st


st.set_page_config(page_title="Flight Agent", layout="wide")

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

STATE_PATH = DATA_DIR / "state.json"
SUMMARY_PATH = DATA_DIR / "summary.md"
HISTORY_PATH = DATA_DIR / "history.jsonl"


IATA_AIRLINE_NAMES = {
    "AF": "Air France",
    "LH": "Lufthansa",
    "UX": "Air Europa",
    "ET": "Ethiopian Airlines",
    "AT": "Royal Air Maroc",
    "TP": "TAP Air Portugal",
    "AZ": "ITA Airways",
    "IB": "Iberia",
    "KL": "KLM",
    "LX": "SWISS",
    "BA": "British Airways",
    "LA": "LATAM",
    "TK": "Turkish Airlines",
    "QR": "Qatar Airways",
    "EK": "Emirates",
    # domésticas comuns (se aparecerem)
    "G3": "GOL",
    "AD": "Azul",
    "JJ": "LATAM (antigo)",
}


def carrier_label(code: str) -> str:
    c = (code or "").strip().upper()
    if not c or c == "—":
        return "—"
    name = IATA_AIRLINE_NAMES.get(c)
    return f"{c} ({name})" if name else c


def load_json(path: Path, default: Any):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def load_text(path: Path, default: str = "") -> str:
    if not path.exists():
        return default
    return path.read_text(encoding="utf-8")


def to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isinf(v) or math.isnan(v):
            return None
        return v
    except Exception:
        return None


def fmt_money(currency: str, value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{currency} {value:,.2f}"


def parse_route_name(key: str) -> str:
    # novo key: "Roma|GRU-FCO|FIXED|..."
    if "|" in key:
        return key.split("|", 1)[0].strip() or "—"
    return "—"


def parse_origin_dest(info: Dict[str, Any], key: str) -> Tuple[str, str]:
    # Preferir o que o scheduler grava no state
    o = (info.get("origin") or "").strip().upper()
    d = (info.get("destination") or "").strip().upper()
    if o and d:
        return o, d

    # fallback: tenta ler "XXX-YYY" do key
    m = re.search(r"\|([A-Z]{3})-([A-Z]{3})\|", key)
    if m:
        return m.group(1), m.group(2)
    m2 = re.search(r"^([A-Z]{3})-([A-Z]{3})\|", key)
    if m2:
        return m2.group(1), m2.group(2)
    return "—", "—"


def best_price_for_display(info: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    p_total = to_float(info.get("price_total"))
    if p_total is not None:
        return p_total, "total"
    p = to_float(info.get("price"))
    if p is not None:
        return p, "price"
    p_base = to_float(info.get("price_base"))
    if p_base is not None:
        return p_base, "base"
    return None, None


def min_carrier_from_by_carrier(by_carrier: Dict[str, Any]) -> Tuple[str, Optional[float]]:
    best_code = ""
    best_price = None
    if not isinstance(by_carrier, dict):
        return "—", None
    for k, v in by_carrier.items():
        fv = to_float(v)
        if fv is None:
            continue
        if best_price is None or fv < best_price:
            best_price = fv
            best_code = str(k)
    return (best_code or "—"), best_price


# ----------------------------
# UI
# ----------------------------
st.title("✈️ Flight Agent")

if not DATA_DIR.exists():
    st.error("Pasta `data/` não existe no repo. Rode o GitHub Actions 1x para gerar os arquivos.")
    st.stop()

state = load_json(STATE_PATH, default={"best": {}, "meta": {}})
best_map = state.get("best", {}) if isinstance(state.get("best", {}), dict) else {}
meta = state.get("meta", {}) if isinstance(state.get("meta", {}), dict) else {}

last_run = meta.get("latest_run_id") or state.get("last_run_id")

top = st.columns([1, 1, 1])
top[0].metric("Último run_id (state)", last_run or "—")
top[1].metric("Entradas no state.best", str(len(best_map)))
top[2].metric("Atualização (summary)", "OK" if SUMMARY_PATH.exists() else "—")

st.divider()

# Sidebar filters
route_names = sorted({parse_route_name(k) for k in best_map.keys() if k})
sel_route = st.sidebar.selectbox("Rota", options=["(todas)"] + route_names)

# Build rows for table
rows: List[Dict[str, Any]] = []
for k, info in best_map.items():
    if not isinstance(info, dict):
        continue

    rname = parse_route_name(k)
    if sel_route != "(todas)" and rname != sel_route:
        continue

    origin, dest = parse_origin_dest(info, k)
    currency = str(info.get("currency") or "BRL")

    price_val, _kind = best_price_for_display(info)
    dep = info.get("best_dep") or "—"
    ret = info.get("best_ret") or "—"

    best_carrier_code, best_carrier_price = min_carrier_from_by_carrier(info.get("by_carrier") or {})
    best_carrier = carrier_label(best_carrier_code)

    rows.append(
        {
            "Rota": rname,
            "Origem": origin,
            "Destino": dest,
            "TOTAL": fmt_money(currency, price_val),
            "Ida": dep,
            "Volta": ret,
            "Cia + barata": best_carrier,
            "Notas": info.get("summary", "") or "",
            "Key": k,
        }
    )

if not rows:
    st.warning("Nada para mostrar com esse filtro. Verifique se `data/state.json` tem entradas.")
else:
    df = pd.DataFrame(rows)

    def _money_to_num(s: str) -> float:
        try:
            if s == "N/A":
                return 1e18
            return float(s.split(" ", 1)[1].replace(",", ""))
        except Exception:
            return 1e18

    df["_p"] = df["TOTAL"].map(_money_to_num)
    df = df.sort_values(["Rota", "Destino", "_p"]).drop(columns=["_p"])

    st.subheader("📌 Melhores preços por destino (state.json)")
    st.dataframe(
        df[["Rota", "Origem", "Destino", "TOTAL", "Ida", "Volta", "Cia + barata", "Notas"]],
        width="stretch",
        hide_index=True,
    )

    with st.expander("Ver Keys (avançado)"):
        st.dataframe(df[["Rota", "Origem", "Destino", "Key"]], width="stretch", hide_index=True)

st.divider()

st.subheader("🏷️ Top 5 por companhia (por rota/destino)")

carrier_rows = []
for k, info in best_map.items():
    if not isinstance(info, dict):
        continue

    rname = parse_route_name(k)
    if sel_route != "(todas)" and rname != sel_route:
        continue

    origin, dest = parse_origin_dest(info, k)
    currency = str(info.get("currency") or "BRL")
    by_carrier = info.get("by_carrier") or {}

    for code, price in by_carrier.items():
        pv = to_float(price)
        if pv is None:
            continue
        carrier_rows.append(
            {
                "Rota": rname,
                "Origem": origin,
                "Destino": dest,
                "Airline": carrier_label(str(code)),
                "Preço (TOTAL)": fmt_money(currency, pv),
            }
        )

if carrier_rows:
    cdf = pd.DataFrame(carrier_rows)

    def _money_to_num2(s: str) -> float:
        try:
            if s == "N/A":
                return 1e18
            return float(s.split(" ", 1)[1].replace(",", ""))
        except Exception:
            return 1e18

    cdf["_p"] = cdf["Preço (TOTAL)"].map(_money_to_num2)
    cdf = cdf.sort_values(["Rota", "Destino", "_p"]).drop(columns=["_p"])

    out = []
    for (rname, dest), g in cdf.groupby(["Rota", "Destino"], sort=False):
        out.append(g.head(5))
    cdf2 = pd.concat(out, ignore_index=True) if out else cdf.head(0)

    st.dataframe(cdf2, width="stretch", hide_index=True)
else:
    st.info("Sem by_carrier no state ainda (ou sem ofertas nas rotas filtradas).")

st.divider()

st.subheader("📝 Summary (data/summary.md)")
summary_text = load_text(SUMMARY_PATH, default="")
if summary_text.strip():
    st.markdown(summary_text)
else:
    st.info("Ainda não há summary.md (ou está vazio). Rode Actions 1x.")

with st.expander("🔧 Debug", expanded=False):
    st.write("ROOT:", str(ROOT))
    st.write("DATA_DIR:", str(DATA_DIR))
    st.write("Files:", [p.name for p in sorted(DATA_DIR.glob("*"))])
    st.write("STATE exists:", STATE_PATH.exists())
    st.write("SUMMARY exists:", SUMMARY_PATH.exists())
    st.write("HISTORY exists:", HISTORY_PATH.exists())
    st.write("meta:", meta)
