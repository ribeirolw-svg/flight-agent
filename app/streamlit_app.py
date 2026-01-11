from __future__ import annotations

import json
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
}


def carrier_label(code: str) -> str:
    c = (code or "").strip().upper()
    if not c:
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
        return float(x)
    except Exception:
        return None


def fmt_money(currency: str, value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{currency} {value:,.2f}"


def parse_key_pax(key: str) -> str:
    # ...|A2|C1|...
    m_a = re.search(r"\|A(\d+)\|", key)
    m_c = re.search(r"\|C(\d+)\|", key)
    a = int(m_a.group(1)) if m_a else 0
    c = int(m_c.group(1)) if m_c else 0
    parts = []
    if a:
        parts.append(f"{a} adulto" + ("s" if a != 1 else ""))
    if c:
        parts.append(f"{c} criança" + ("s" if c != 1 else ""))
    return " · ".join(parts) if parts else "—"


def best_price_for_display(info: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    """
    retorna (price_total_preferencial, "total|price|base")
    """
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


def dest_from_key(key: str) -> str:
    # "GRU-FCO|..." ou "GRU-CIA|..."
    if key.startswith("GRU-FCO|") or key.startswith("GRU-FCO"):
        return "FCO"
    if key.startswith("GRU-CIA|") or key.startswith("GRU-CIA"):
        return "CIA"
    return "—"


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


def pick_best_rome(best_map: Dict[str, Any]) -> Optional[Tuple[str, Dict[str, Any]]]:
    # escolhe menor preço entre FCO/CIA
    best_key = None
    best_info = None
    best_val = None
    for k, info in (best_map or {}).items():
        if not (k.startswith("GRU-FCO") or k.startswith("GRU-CIA")):
            continue
        v, _ = best_price_for_display(info if isinstance(info, dict) else {})
        if v is None:
            continue
        if best_val is None or v < best_val:
            best_val = v
            best_key = k
            best_info = info
    if best_key and best_info:
        return best_key, best_info
    return None


# ----------------------------
# UI
# ----------------------------
st.title("✈️ Flight Agent — São Paulo ↔ Roma")

if not DATA_DIR.exists():
    st.error("Pasta `data/` não existe no repo. Rode o GitHub Actions 1x para gerar os arquivos.")
    st.stop()

# load state
state = load_json(STATE_PATH, default={"best": {}, "last_run_id": None})
best_map = state.get("best", {}) if isinstance(state.get("best", {}), dict) else {}
last_run = state.get("last_run_id")

top = st.columns([1, 1, 1])
top[0].metric("Último run_id (state)", last_run or "—")
top[1].metric("Arquivos em data/", str(len(list(DATA_DIR.glob("*")))))
top[2].metric("Atualização (summary)", "OK" if SUMMARY_PATH.exists() else "—")

st.divider()

# KPI Card Roma
st.subheader("🇮🇹 Cartão Roma (melhor preço atual)")

best_rome = pick_best_rome(best_map)
if not best_rome:
    st.warning("Ainda não encontrei Roma em `data/state.json` (ou está sem preço).")
else:
    key, info = best_rome
    currency = str(info.get("currency") or "BRL")

    price_val, price_kind = best_price_for_display(info)
    price_total = to_float(info.get("price_total"))
    price_base = to_float(info.get("price_base"))

    dep = info.get("best_dep") or "—"
    ret = info.get("best_ret") or "—"
    pax = parse_key_pax(key)
    dest = str(info.get("destination") or dest_from_key(key))

    best_carrier_code, best_carrier_price = min_carrier_from_by_carrier(info.get("by_carrier") or {})
    best_carrier_txt = carrier_label(best_carrier_code)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Preço (TOTAL)", fmt_money(currency, price_total if price_total is not None else price_val))
    c2.metric("Ida", dep)
    c3.metric("Volta", ret)
    c4.metric("Cia + barata", best_carrier_txt)

    st.caption(
        f"Pax: **{pax}** · Destino: **{dest}** · "
        f"BASE (sem taxas): **{fmt_money(currency, price_base)}** · "
        f"Key: `{key}`"
    )

st.divider()

# Table: Destinos FCO/CIA
st.subheader("📌 Roma — por destino (FCO / CIA)")

rows: List[Dict[str, Any]] = []
for k, info in best_map.items():
    if not (k.startswith("GRU-FCO") or k.startswith("GRU-CIA")):
        continue
    if not isinstance(info, dict):
        continue

    currency = str(info.get("currency") or "BRL")
    dest = str(info.get("destination") or dest_from_key(k))
    pax = parse_key_pax(k)

    price_total = to_float(info.get("price_total"))
    price_base = to_float(info.get("price_base"))
    price_val, _kind = best_price_for_display(info)

    dep = info.get("best_dep") or "—"
    ret = info.get("best_ret") or "—"

    best_carrier_code, best_carrier_price = min_carrier_from_by_carrier(info.get("by_carrier") or {})
    best_carrier = carrier_label(best_carrier_code)

    rows.append(
        {
            "Destino": dest,
            "TOTAL": fmt_money(currency, price_total if price_total is not None else price_val),
            "BASE": fmt_money(currency, price_base),
            "Ida": dep,
            "Volta": ret,
            "Cia + barata": best_carrier,
            "Pax": pax,
            "Notas": info.get("summary", "") or "",
            "Key": k,
        }
    )

if not rows:
    st.info("Sem entradas GRU→(FCO/CIA) no state ainda.")
else:
    df = pd.DataFrame(rows)
    # ordenar por destino e preço total
    def _money_to_num(s: str) -> float:
        try:
            if s == "N/A":
                return 1e18
            return float(s.split(" ", 1)[1].replace(",", ""))
        except Exception:
            return 1e18

    df["_p"] = df["TOTAL"].map(_money_to_num)
    df = df.sort_values(["Destino", "_p"]).drop(columns=["_p"])

    st.dataframe(
        df[["Destino", "TOTAL", "BASE", "Ida", "Volta", "Cia + barata", "Pax", "Notas"]],
        use_container_width=True,
        hide_index=True,
    )

    with st.expander("Ver Keys (avançado)"):
        st.dataframe(df[["Destino", "Key"]], use_container_width=True, hide_index=True)

st.divider()

# Top 5 por companhia (do melhor estado)
st.subheader("🏷️ Roma — Top 5 por companhia (do state)")

carrier_rows = []
for k, info in best_map.items():
    if not isinstance(info, dict):
        continue
    if not (k.startswith("GRU-FCO") or k.startswith("GRU-CIA")):
        continue
    currency = str(info.get("currency") or "BRL")
    dest = str(info.get("destination") or dest_from_key(k))
    by_carrier = info.get("by_carrier") or {}

    for code, price in by_carrier.items():
        pv = to_float(price)
        if pv is None:
            continue
        carrier_rows.append({"Destino": dest, "Airline": carrier_label(str(code)), "Preço (TOTAL)": fmt_money(currency, pv)})

if carrier_rows:
    cdf = pd.DataFrame(carrier_rows)
    # rank por destino
    def _money_to_num2(s: str) -> float:
        try:
            return float(s.split(" ", 1)[1].replace(",", ""))
        except Exception:
            return 1e18

    cdf["_p"] = cdf["Preço (TOTAL)"].map(_money_to_num2)
    cdf = cdf.sort_values(["Destino", "_p"]).drop(columns=["_p"])
    # top5 por destino
    out = []
    for dest, g in cdf.groupby("Destino", sort=False):
        out.append(g.head(5))
    cdf2 = pd.concat(out, ignore_index=True)
    st.dataframe(cdf2, use_container_width=True, hide_index=True)
else:
    st.info("Sem by_carrier no state ainda (rode Actions ao menos 1x com ofertas).")

st.divider()

# Summary.md
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
