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


# ----------------------------
# Helpers
# ----------------------------
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
    "G3": "GOL",
    "AD": "Azul",
    "JJ": "LATAM (legacy)",
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
        if isinstance(x, str) and x.strip().lower() in {"inf", "infinity"}:
            return None
        return float(x)
    except Exception:
        return None


def fmt_money(currency: str, value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{currency} {value:,.2f}"


def best_price_for_display(info: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    """
    retorna (price_preferencial, "price_total|price|base")
    """
    p_total = to_float(info.get("price_total"))
    if p_total is not None:
        return p_total, "price_total"
    p = to_float(info.get("price"))
    if p is not None:
        return p, "price"
    p_base = to_float(info.get("price_base"))
    if p_base is not None:
        return p_base, "price_base"
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


def parse_key_fields(key: str) -> Dict[str, str]:
    """
    Espera key no formato:
      "<route_name>|<ORIGIN>-<DEST>|<RULE>|class=...|A2|C1|BRL"
    Mas tenta ser tolerante se algo variar.
    """
    out = {"route_name": "—", "origin": "—", "destination": "—", "rule": "—"}
    if not key:
        return out

    parts = key.split("|")
    if len(parts) >= 1:
        out["route_name"] = parts[0].strip() or "—"
    if len(parts) >= 2 and "-" in parts[1]:
        od = parts[1].strip()
        try:
            o, d = od.split("-", 1)
            out["origin"] = o.strip().upper() or "—"
            out["destination"] = d.strip().upper() or "—"
        except Exception:
            pass
    if len(parts) >= 3:
        out["rule"] = parts[2].strip() or "—"

    return out


def pax_from_info_or_key(info: Dict[str, Any], key: str) -> str:
    pax = info.get("pax")
    if isinstance(pax, dict):
        a = pax.get("adults")
        c = pax.get("children")
        try:
            a_i = int(a) if a is not None else None
            c_i = int(c) if c is not None else None
            if a_i is not None or c_i is not None:
                return f"A{a_i or 0} C{c_i or 0}"
        except Exception:
            pass

    # fallback: tenta A\d e C\d no key
    m_a = re.search(r"\|A(\d+)\|", key)
    m_c = re.search(r"\|C(\d+)\|", key)
    a = int(m_a.group(1)) if m_a else 0
    c = int(m_c.group(1)) if m_c else 0
    return f"A{a} C{c}"


def load_history_jsonl(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    if "ts_utc" in df.columns:
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], errors="coerce", utc=True)

    for col in ["offers_count", "adults", "children"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "origin" in df.columns:
        df["origin"] = df["origin"].astype(str).str.upper()
    if "destination" in df.columns:
        df["destination"] = df["destination"].astype(str).str.upper()
    if "cabin" in df.columns:
        df["cabin"] = df["cabin"].astype(str).str.upper()

    return df


def pick_latest_history(
    dfh: pd.DataFrame,
    *,
    origin: str,
    destination: str,
    adults: int,
    children: int,
    cabin: Optional[str] = None,
    direct_only: Optional[bool] = None,
) -> Optional[Dict[str, Any]]:
    if dfh is None or dfh.empty:
        return None

    d = dfh.copy()

    if "origin" in d.columns:
        d = d[d["origin"].astype(str).str.upper() == str(origin).upper()]
    if "destination" in d.columns:
        d = d[d["destination"].astype(str).str.upper() == str(destination).upper()]

    if "adults" in d.columns:
        d = d[d["adults"].fillna(-1).astype(int) == int(adults)]
    if "children" in d.columns:
        d = d[d["children"].fillna(-1).astype(int) == int(children)]

    if cabin and "cabin" in d.columns:
        d = d[d["cabin"].astype(str).str.upper() == str(cabin).upper()]

    if direct_only is not None and "direct_only" in d.columns:
        # direct_only pode ter vindo como bool ou str
        d = d[d["direct_only"].astype(bool) == bool(direct_only)]

    # remove erros
    if "error" in d.columns:
        d = d[d["error"].isna() | (d["error"].astype(str).str.strip() == "")]

    if d.empty:
        return None

    if "ts_utc" in d.columns:
        d = d.sort_values("ts_utc", ascending=False)

    return d.iloc[0].to_dict()


# ----------------------------
# UI
# ----------------------------
st.title("✈️ Flight Agent")

if not DATA_DIR.exists():
    st.error("Pasta `data/` não existe no repo. Rode o GitHub Actions 1x para gerar os arquivos.")
    st.stop()

state = load_json(STATE_PATH, default={"best": {}, "meta": {"previous_run_id": None, "latest_run_id": None}})
best_map = state.get("best", {}) if isinstance(state.get("best", {}), dict) else {}
meta = state.get("meta", {}) if isinstance(state.get("meta", {}), dict) else {}

history_df = load_history_jsonl(HISTORY_PATH)
summary_text = load_text(SUMMARY_PATH, default="")

top = st.columns([1, 1, 1, 1])
top[0].metric("latest_run_id", meta.get("latest_run_id") or "—")
top[1].metric("previous_run_id", meta.get("previous_run_id") or "—")
top[2].metric("Arquivos em data/", str(len(list(DATA_DIR.glob("*")))))
top[3].metric("History rows", str(len(history_df)) if not history_df.empty else "0")

st.divider()

# ----------------------------
# Build table rows from state
# ----------------------------
rows: List[Dict[str, Any]] = []
for k, info in (best_map or {}).items():
    if not isinstance(info, dict):
        continue

    kf = parse_key_fields(str(k))
    route_name = kf["route_name"]
    origin = str(info.get("origin") or kf["origin"] or "—").upper()
    destination = str(info.get("destination") or kf["destination"] or "—").upper()
    rule = kf["rule"]

    currency = str(info.get("currency") or "BRL").upper()
    pax = pax_from_info_or_key(info, str(k))

    price_total = to_float(info.get("price_total"))
    price_val, _kind = best_price_for_display(info)
    price_base = to_float(info.get("price_base"))

    dep = info.get("best_dep") or "—"
    ret = info.get("best_ret") or "—"

    best_carrier_code, best_carrier_price = min_carrier_from_by_carrier(info.get("by_carrier") or {})
    best_carrier_txt = carrier_label(best_carrier_code)

    rows.append(
        {
            "Rota": route_name,
            "Origem": origin,
            "Destino": destination,
            "Regra": rule,
            "Pax": pax,
            "TOTAL": fmt_money(currency, price_total if price_total is not None else price_val),
            "BASE": fmt_money(currency, price_base),
            "Ida": dep,
            "Volta": ret,
            "Cia + barata": best_carrier_txt,
            "Notas": info.get("summary", "") or "",
            "Key": str(k),
        }
    )

if not rows:
    st.warning("Ainda não há entradas em `data/state.json`. Rode o workflow 1x.")
    st.stop()

df = pd.DataFrame(rows)

# sort by route then price
def _money_to_num(s: str) -> float:
    try:
        if not isinstance(s, str) or s.strip() in {"N/A", ""}:
            return 1e18
        # "BRL 1,234.56"
        return float(s.split(" ", 1)[1].replace(",", ""))
    except Exception:
        return 1e18

df["_p"] = df["TOTAL"].map(_money_to_num)
df = df.sort_values(["Rota", "_p", "Destino"]).drop(columns=["_p"])

# ----------------------------
# Sidebar controls (Etapa 1)
# ----------------------------
st.sidebar.header("Filtros")

route_opts = ["(todas)"] + sorted(df["Rota"].dropna().unique().tolist())
sel_route = st.sidebar.selectbox("Rota", options=route_opts, index=0)

df_view = df.copy()
if sel_route != "(todas)":
    df_view = df_view[df_view["Rota"] == sel_route]

dest_opts = ["(auto)"] + sorted(df_view["Destino"].dropna().unique().tolist())
sel_dest = st.sidebar.selectbox("Destino", options=dest_opts, index=0)

st.sidebar.markdown("---")
st.sidebar.subheader("Configuração (simulação)")

ui_adults = st.sidebar.number_input("Adultos", min_value=1, max_value=9, value=2, step=1)
ui_children = st.sidebar.number_input("Crianças", min_value=0, max_value=9, value=1, step=1)
ui_cabin = st.sidebar.selectbox("Cabine", options=["ECONOMY", "PREMIUM_ECONOMY", "BUSINESS", "FIRST"], index=0)
ui_direct = st.sidebar.checkbox("Somente direto", value=True)

# ----------------------------
# KPI / selection reference
# ----------------------------
st.subheader("📌 Melhores preços (state.json)")

if df_view.empty:
    st.info("Nenhuma rota encontrada com os filtros atuais.")
else:
    st.dataframe(
        df_view[["Rota", "Origem", "Destino", "TOTAL", "BASE", "Ida", "Volta", "Cia + barata", "Pax", "Notas"]],
        width="stretch",
        hide_index=True,
    )

    with st.expander("Ver Keys (avançado)"):
        st.dataframe(df_view[["Rota", "Origem", "Destino", "Key"]], width="stretch", hide_index=True)

st.divider()

# Reference origin/destination for history lookup (Etapa 1)
ref_origin = None
ref_dest = None
ref_route = None

if not df_view.empty:
    if sel_dest != "(auto)":
        sub = df_view[df_view["Destino"] == sel_dest]
        if not sub.empty:
            ref = sub.iloc[0].to_dict()
        else:
            ref = df_view.iloc[0].to_dict()
    else:
        ref = df_view.iloc[0].to_dict()

    ref_origin = str(ref.get("Origem") or "—").upper()
    ref_dest = str(ref.get("Destino") or "—").upper()
    ref_route = str(ref.get("Rota") or "—")

# ----------------------------
# Etapa 1: "última consulta" do histórico para a config escolhida
# ----------------------------
st.subheader("🧪 Etapa 1 — Última consulta no histórico (history.jsonl) para a config escolhida")

if not ref_origin or not ref_dest or ref_origin == "—" or ref_dest == "—":
    st.info("Selecione uma rota/destino para consultar o histórico.")
else:
    latest = pick_latest_history(
        history_df,
        origin=ref_origin,
        destination=ref_dest,
        adults=int(ui_adults),
        children=int(ui_children),
        cabin=str(ui_cabin),
        direct_only=bool(ui_direct),
    )

    if not latest:
        st.warning(
            f"Não encontrei consulta no histórico para **{ref_route} — {ref_origin}→{ref_dest}** com "
            f"**A{ui_adults} C{ui_children}**, cabine **{ui_cabin}**, direto={ui_direct}. "
            f"Isso *não é erro*: só significa que essa combinação ainda não foi consultada pelo batch. "
            f"Rode o workflow manual (workflow_dispatch) ou espere o próximo agendamento."
        )
    else:
        ts = latest.get("ts_utc")
        dep = latest.get("departure_date", "—")
        ret = latest.get("return_date", "—")
        offers = latest.get("offers_count", "—")
        run_id = latest.get("run_id", "—")

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Rota", ref_route)
        c2.metric("Origem→Destino", f"{ref_origin}→{ref_dest}")
        c3.metric("Pax", f"A{ui_adults} C{ui_children}")
        c4.metric("Última consulta (UTC)", str(ts)[:19] if ts is not None else "—")
        try:
            c5.metric("Offers", str(int(offers)) if offers is not None else "—")
        except Exception:
            c5.metric("Offers", str(offers))

        st.caption(f"Dep: **{dep}** · Ret: **{ret}** · run_id: `{run_id}`")

st.divider()

# ----------------------------
# By-carrier (do state) para rota selecionada
# ----------------------------
st.subheader("🏷️ Top companhias (do state.json) — rota selecionada")

carrier_rows: List[Dict[str, Any]] = []
for k, info in (best_map or {}).items():
    if not isinstance(info, dict):
        continue

    kf = parse_key_fields(str(k))
    route_name = kf["route_name"]
    origin = str(info.get("origin") or kf["origin"] or "—").upper()
    destination = str(info.get("destination") or kf["destination"] or "—").upper()

    if sel_route != "(todas)" and route_name != sel_route:
        continue
    if sel_dest != "(auto)" and destination != sel_dest:
        continue

    currency = str(info.get("currency") or "BRL").upper()
    by_carrier = info.get("by_carrier") or {}
    if not isinstance(by_carrier, dict):
        continue

    for code, price in by_carrier.items():
        pv = to_float(price)
        if pv is None:
            continue
        carrier_rows.append(
            {
                "Rota": route_name,
                "Origem": origin,
                "Destino": destination,
                "Airline": carrier_label(str(code)),
                "Preço (TOTAL)": fmt_money(currency, pv),
            }
        )

if carrier_rows:
    cdf = pd.DataFrame(carrier_rows)

    def _money_to_num2(s: str) -> float:
        try:
            return float(s.split(" ", 1)[1].replace(",", ""))
        except Exception:
            return 1e18

    cdf["_p"] = cdf["Preço (TOTAL)"].map(_money_to_num2)
    cdf = cdf.sort_values(["Rota", "Destino", "_p"]).drop(columns=["_p"])

    # top5 por rota+dest
    out = []
    for (r, d), g in cdf.groupby(["Rota", "Destino"], sort=False):
        out.append(g.head(5))
    cdf2 = pd.concat(out, ignore_index=True)

    st.dataframe(cdf2, width="stretch", hide_index=True)
else:
    st.info("Sem by_carrier disponível no state para os filtros atuais (ou o batch não retornou ofertas).")

st.divider()

# ----------------------------
# Summary
# ----------------------------
st.subheader("📝 Summary (data/summary.md)")
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
    st.write("best_map size:", len(best_map) if isinstance(best_map, dict) else 0)
