import json
from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Flight Agent", layout="wide")

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

SUMMARY = DATA_DIR / "summary.md"
STATE = DATA_DIR / "state.json"
HISTORY = DATA_DIR / "history.jsonl"


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


def _infer_dest_from_key(key: str) -> str:
    if key.startswith("GRU-FCO|"):
        return "FCO"
    if key.startswith("GRU-CIA|"):
        return "CIA"
    return ""


def _best_airline_label(by_carrier: dict) -> str:
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
    if best_code is None:
        return "—"
    return best_code


st.title("✈️ Flight Agent — Dashboard")

top_left, top_right = st.columns([1, 1])
with top_left:
    if st.button("🔄 Recarregar agora"):
        st.rerun()

with top_right:
    st.caption(f"📁 Lendo arquivos de: `{DATA_DIR}`")

# --- Current Best as a nice table ---
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
                "Companhia + barata": _best_airline_label(info.get("by_carrier", {})),
                "Notas": info.get("summary", "") or "",
                "Key": key,
            }
        )

    if rows:
        df = pd.DataFrame(rows)

        # ordena: primeiro por destino, depois por preço (convertendo quando possível)
        def _price_num(x: str) -> float:
            try:
                return float(x.split(" ", 1)[1].replace(",", ""))
            except Exception:
                return float("inf")

        df["_p"] = df["Melhor preço"].apply(_price_num)
        df = df.sort_values(["Destino", "_p"]).drop(columns=["_p"])

        st.dataframe(
            df[["Destino", "Melhor preço", "Ida (best_dep)", "Volta (best_ret)", "Companhia + barata", "Notas"]],
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
            # mostra só um resumo compacto
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
