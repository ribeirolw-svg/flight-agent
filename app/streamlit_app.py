import json
from pathlib import Path
import streamlit as st

st.set_page_config(page_title="Flight Agent", layout="wide")

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

SUMMARY = DATA_DIR / "summary.md"
STATE = DATA_DIR / "state.json"
HISTORY = DATA_DIR / "history.jsonl"

st.title("✈️ Flight Agent — Dashboard")

with st.expander("🔧 Debug", expanded=False):
    st.write("ROOT:", str(ROOT))
    st.write("DATA_DIR:", str(DATA_DIR))
    st.write("DATA_DIR exists:", DATA_DIR.exists())
    st.write("Files:", [p.name for p in sorted(DATA_DIR.glob("*"))] if DATA_DIR.exists() else [])

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Weekly Summary")
    if SUMMARY.exists():
        st.markdown(SUMMARY.read_text(encoding="utf-8"))
    else:
        st.warning("Não encontrei data/summary.md no repo. Rode o GitHub Actions ao menos 1x para gerar.")

with col2:
    st.subheader("Current Best (state.json)")
    if STATE.exists():
        state = json.loads(STATE.read_text(encoding="utf-8"))
        best = state.get("best", {})
        if best:
            st.json(best)
        else:
            st.info("state.json existe, mas ainda não tem best.")
    else:
        st.warning("Não encontrei data/state.json no repo.")

st.divider()
st.subheader("History (últimos 10 runs)")

if HISTORY.exists():
    lines = HISTORY.read_text(encoding="utf-8").splitlines()
    tail = lines[-10:] if len(lines) > 10 else lines
    if not tail:
        st.info("history.jsonl está vazio.")
    for line in reversed(tail):
        try:
            rec = json.loads(line)
        except Exception:
            continue
        st.markdown(f"**run_id:** `{rec.get('run_id','')}`")
        st.json(rec.get("results", []))
        st.divider()
else:
    st.warning("Não encontrei data/history.jsonl no repo.")
