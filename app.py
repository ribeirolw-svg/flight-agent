import streamlit as st
from collector import collect
import pandas as pd
from io import BytesIO

st.set_page_config(page_title="Flight Agent", layout="wide")

st.title("✈️ Monitor de Passagens — São Paulo ↔ Roma")
st.markdown("""
**Rota:** GRU → FCO  
**Pax:** 2 adultos + 1 criança (3 anos)  
**Direto:** Sim  
**Janela:** ida 01/09/2026–20/09/2026 | duração 15 dias | volta ≤ 05/10/2026  
**Moeda:** como vier da fonte
""")

if "df" not in st.session_state:
    st.session_state["df"] = None

col1, col2 = st.columns([1, 1])

with col1:
    if st.button("🔄 Rodar busca agora"):
        with st.spinner("Coletando dados..."):
            df = collect()
            st.session_state["df"] = df
        st.success("Busca concluída.")

with col2:
    df = st.session_state.get("df")
    if df is not None and isinstance(df, pd.DataFrame) and len(df) > 0:
        # Gera excel em memória
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Resultados", index=False)
        output.seek(0)

        st.download_button(
            label="📥 Baixar Excel (Resultados)",
            data=output,
            file_name="flight_prices_latest.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.caption("Rode a busca para habilitar o download do Excel.")

df = st.session_state.get("df")
if df is not None:
    st.dataframe(df, use_container_width=True)
