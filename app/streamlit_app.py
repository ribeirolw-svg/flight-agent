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
    # expects "BRL 10,742.30" or "N/A"
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


def _best_airline_code(by_carrier: dict) -> str:
    if not isinstance(by_carrier, dict) or not by_carrier:
        return "—"
    best_code, best_price = None, None
    for c, v in by_carrier.items():
        try:
            p = float(v)
        except Exception:
            continue
        if best_price is None or p < best_price:
            best_price = p
            best_code = str(c)
    return best_code or "—"


def _extract_rome_kpi_from_summary(text: str) -> dict:
    """
    Extract from summary.md:
      - Updated: <...>
      - Best this run: <route> — <money>
      - Dates: depart YYYY-mm-dd · return YYYY-mm-dd
      - Roma — by Airline (Top 5): collect first 3 airlines
    """
    out = {
        "updated": "—",
        "best_price": "N/A",
        "dep": "—",
        "ret": "—",
        "best_airline": "—",
        "top3": [],
        "has_results": False,
    }

    if not text:
        return out

    m = re.search(r"Updated:\s*(.+)", text)
    if m:
        out["updated"] = m.group(1).strip()

    # Best this run: GRU→FCO — BRL 10,742.30
    m = re.search(r"Best this run:\s*(.+?)\s+—\s+(.+)", text)
    if m:
        out["best_price"] = m.group(2).strip()
        out["has_results"] = out["best_price"] not in ("N/A", "NA", "—", "")

    # Dates: depart 2026-09-01 · return 2026-09-11 (≤ 2026-10-05)
    m = re.search(r"Dates:\s*depart\s+(\d{4}-\d{2}-\d{2})\s*·\s*return\s+(\d{4}-\d{2}-\d{2})", text)
    if m:
        out["dep"] = m.group(1)
        out["ret"] = m.group(2)

    # Parse "Roma — by Airline (Top 5)" block: take first 3 airlines
    # Works with your current summary format (plain lines)
    after = re.split(r"Roma\s+—\s+by Airline.*?\n", text, maxsplit=1)
    if len(after) == 2:
        block = after[1]
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        # skip the header line "Airline Best Price" if present
        cleaned = []
        for ln in lines:
            if ln.lower().startswith("airline"):
                continue
            if "best price" in ln.lower():
                continue
            # stop if we reach next section
            if ln.startswith("Current Best") or ln.startswith("Per Destination") or ln.startswith("Latest Run"):
                break
            cleaned.append(ln)

        for ln in cleaned[:5]:
            # line like: AT (Royal Air Maroc)   BRL 10,742.30
            parts = re.split(r"\s{2,}|\t", ln)
            if parts:
                out["top3"].append(parts[0].strip())

        if out["top3"]:
            out["best_airline"] = out["top3"][0]

    return out


# ----------------------------
# UI
# ----------------------------
st.title("✈️ Flight Agent — Dashboard")

top_left, top_right = st.columns([1, 1])
with top_left:
    if st.button("🔄 Recarregar agora"):
        st.rerun()
with top_right:
    st.caption(f"📁 Fonte: `{DATA_DIR}`")

summary_text = SUMMARY.read_text(encoding="utf-8") if SUMMARY.exists() else ""
kpi = _extract_rome_kpi_from_summary(summary_text)

# --- KPI Card ---
st.markdown("### 🇮🇹 Roma — Cartão (KPI)")
c1, c2, c3, c4 = st.columns(4)

# Color-ish behavior via status messages (Streamlit metrics don't accept colors directly)
if kpi["has_results"]:
    c1.metric("Melhor preço", kpi["best_price"])
else:
    c1.metric("Melhor preço", "N/A")

c2.metric("Ida", kpi["dep"])
c3.metric("Volta", kpi["ret"])
c4.metric("Cia + barata", kpi["best_airline"])

if kpi["has_results"]:
    st.success("✅ Tem resultado Roma no último run.")
else:
    st.warning("⚠️ Sem resultado Roma no último run (ou summary ainda não gerado).")

if kpi["top3"]:
    st.caption("Top 3 cias (Roma): " + " · ".join(kpi["top3"][:3]))

st.caption(f"Última atualização do summary: {kpi['updated']}")
st.divider()

# --- Current Best table ---
st.subheader("✅ Current Best (state.json)")

if STATE.exists():
    state = _load_json(STATE)
    best = state.get("best", {}) if isinstance(state.get("best", {}), dict) else {}

    rows = []
    for key, info in best.items():
        currency = str(info.get("currency", "BRL") or "BRL")
        dest = str(info.get("destination") or _infer_dest_from_key(str(key)) or "—")
        price = info.get("price", None)

        rows.append(
            {
                "Destino": dest,
                "Melhor preço": _fmt_money(currency, price),
                "Ida (best_dep)": info.get("best_dep") or "—",
                "Volta (best_ret)": info.get("best_ret") or "—",
                "Companhia + barata (code)": _best_airline_code(info.get("by_carrier", {})),
                "Notas": info.get("summary", "") or "",
                "Key": key,
            }
        )

    if rows:
        df = pd.DataFrame(rows)
        df["_p"] = df["Melhor preço"].apply(_price_num_from_str)
        df = df.sort_values(["Destino", "_p"]).drop(columns=["_p"])

        st.dataframe(
            df[["Destino", "Melhor preço", "Ida (best_dep)", "Volta (best_ret)", "Companhia + barata (code)", "Notas"]],
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
    st.markdown(summary_text)
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
                small.append(
                    {
                        "dest": r.get("destination") or _infer_dest_from_key(str(r.get("key", ""))),
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
