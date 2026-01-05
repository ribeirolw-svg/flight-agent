import json
import re
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Flight Agent", layout="wide")

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

SUMMARY = DATA_DIR / "summary.md"
STATE = DATA_DIR / "state.json"
HISTORY = DATA_DIR / "history.jsonl"

# (opcional) mapeamento simples IATA -> nome, pra ficar mais humano
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


# ----------------------------
# Helpers
# ----------------------------
def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _fmt_money(currency: str, price) -> str:
    try:
        p = float(price)
    except Exception:
        return "N/A"
    if p == float("inf"):
        return "N/A"
    return f"{currency} {p:,.2f}"


def _price_num_from_str(s: str) -> float:
    try:
        return float(s.split(" ", 1)[1].replace(",", ""))
    except Exception:
        return float("inf")


def _infer_dest_from_key(key: str) -> str:
    if key.startswith("GRU-FCO|"):
        return "FCO"
    if key.startswith("GRU-CIA|"):
        return "CIA"
    return ""


def _parse_pax_from_key(key: str) -> str:
    # key contém ...|A2|C1|...
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


def _best_carriers(by_carrier: dict) -> list[tuple[str, float]]:
    """retorna lista ordenada [(code, price), ...]"""
    if not isinstance(by_carrier, dict) or not by_carrier:
        return []
    rows = []
    for c, v in by_carrier.items():
        try:
            rows.append((str(c), float(v)))
        except Exception:
            continue
    rows.sort(key=lambda x: x[1])
    return rows


def _carrier_label(code: str) -> str:
    code = (code or "").upper().strip()
    name = IATA_AIRLINE_NAMES.get(code)
    return f"{code} ({name})" if name else code


def _pick_best_rome_from_state(best_map: dict) -> dict | None:
    """
    best_map: state["best"] dict
    escolhe o menor preço entre GRU-FCO e GRU-CIA (se houver)
    """
    candidates = []
    for key, info in (best_map or {}).items():
        if not (str(key).startswith("GRU-FCO|") or str(key).startswith("GRU-CIA|")):
            continue
        try:
            p = float(info.get("price", float("inf")))
        except Exception:
            p = float("inf")
        candidates.append((p, key, info))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    _, key, info = candidates[0]
    return {"key": key, "info": info}


# ----------------------------
# UI
# ----------------------------
st.title("✈️ Agente de Voo — Painel de Controle")

top_left, top_right = st.columns([1, 1])
with top_left:
    if st.button("🔄 Recarregar agora"):
        st.rerun()
with top_right:
    st.caption(f"📁 Fonte: `{DATA_DIR}`")

state_best = {}
if STATE.exists():
    state = _load_json(STATE)
    state_best = state.get("best", {}) if isinstance(state.get("best", {}), dict) else {}

# --- KPI Card (Roma) ---
st.markdown("### 🇮🇹 Roma — Cartão (KPI)")

best_rome = _pick_best_rome_from_state(state_best)

if best_rome:
    key = str(best_rome["key"])
    info = best_rome["info"] or {}

    currency = str(info.get("currency", "BRL") or "BRL")
    price_txt = _fmt_money(currency, info.get("price"))

    dep = info.get("best_dep") or "—"
    ret = info.get("best_ret") or "—"

    dest = info.get("destination") or _infer_dest_from_key(key) or "ROM"
    pax = _parse_pax_from_key(key)

    carriers = _best_carriers(info.get("by_carrier", {}))
    best_carrier = _carrier_label(carriers[0][0]) if carriers else "—"
    top3 = [_carrier_label(c) for (c, _) in carriers[:3]]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Melhor preço", price_txt)
    c2.metric("Ida", dep)
    c3.metric("Volta", ret)
    c4.metric("Cia + barata", best_carrier)

    # contexto (pax/rota)
    st.caption(f"Rota: **GRU → {dest}**  |  Pax: **{pax}**  |  Classe: **ECONOMY**  |  Key: `{key}`")

    if price_txt != "N/A":
        st.success("✅ Tem resultado Roma no estado atual (state.json).")
    else:
        st.warning("⚠️ Roma está sem preço (N/A) no state.json.")

    if top3:
        st.caption("Top 3 cias (Roma): " + " · ".join(top3))

else:
    st.warning("⚠️ Não achei Roma no state.json ainda (rode o Actions para gerar state/history).")

st.divider()

# --- Current Best table ---
st.subheader("✅ Current Best (state.json)")

if STATE.exists():
    best = state_best

    rows = []
    for key, info in (best or {}).items():
        currency = str(info.get("currency", "BRL") or "BRL")
        dest = str(info.get("destination") or _infer_dest_from_key(str(key)) or "—")
        price = info.get("price", None)
        carriers = _best_carriers(info.get("by_carrier", {}))
        best_carrier = _carrier_label(carriers[0][0]) if carriers else "—"

        rows.append(
            {
                "Destino": dest,
                "Melhor preço": _fmt_money(currency, price),
                "Ida (best_dep)": info.get("best_dep") or "—",
                "Volta (best_ret)": info.get("best_ret") or "—",
                "Cia + barata": best_carrier,
                "Pax": _parse_pax_from_key(str(key)),
                "Notas": info.get("summary", "") or "",
                "Key": str(key),
            }
        )

    if rows:
        df = pd.DataFrame(rows)
        df["_p"] = df["Melhor preço"].apply(_price_num_from_str)
        df = df.sort_values(["Destino", "_p"]).drop(columns=["_p"])

        st.dataframe(
            df[["Destino", "Melhor preço", "Ida (best_dep)", "Volta (best_ret)", "Cia + barata", "Pax", "Notas"]],
            use_container_width=True,
            hide_index=True,
        )

        with st.expander("Ver keys (avançado)"):
            st.dataframe(df[["Destino", "Key"]], use_container_width=True, hide_index=True)
    else:
        st.info("state.json existe, mas ainda não tem registros em best.")
else:
    st.warning("Não encontrei data/state.json no repo.")

st.divider()

# --- Weekly Summary ---
st.subheader("📝 Weekly Summary (summary.md)")
if SUMMARY.exists():
    st.markdown(SUMMARY.read_text(encoding="utf-8"))
else:
    st.warning("Não encontrei data/summary.md no repo. Rode o GitHub Actions ao menos 1x para gerar.")

st.divider()

# --- History (compact) ---
st.subheader("🧾 History (últimos 5 runs)")
if HISTORY.exists():
    lines = HISTORY.read_text(encoding="utf-8").splitlines()
    tail = lines[-5:] if len(lines) > 5 else lines

    if not tail:
        st.info("history.jsonl está vazio.")
    else:
        for line in reversed(tail):
            try:
                rec = json.loads(line)
            except Exception:
                continue
            st.markdown(f"**run_id:** `{rec.get('run_id','')}`")
            results = rec.get("results", []) or []

            small = []
            for r in results:
                dest = r.get("destination") or _infer_dest_from_key(str(r.get("key", "")))
                small.append(
                    {
                        "dest": dest,
                        "price": _fmt_money(str(r.get("currency", "BRL") or "BRL"), r.get("price", float("inf"))),
                        "dep": r.get("best_dep"),
                        "ret": r.get("best_ret"),
                    }
                )

            st.dataframe(pd.DataFrame(small), use_container_width=True, hide_index=True)
            st.divider()
else:
    st.warning("Não encontrei data/history.jsonl no repo.")

with st.expander("🔧 Debug", expanded=False):
    st.write("ROOT:", str(ROOT))
    st.write("DATA_DIR exists:", DATA_DIR.exists())
    st.write("Files:", [p.name for p in sorted(DATA_DIR.glob("*"))] if DATA_DIR.exists() else [])
