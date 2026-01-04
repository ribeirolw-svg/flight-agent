import streamlit as st
from collector import collect

st.set_page_config(page_title="Flight Agent", layout="wide")

st.title("✈️ Monitor de Passagens — São Paulo ↔ Roma")

st.markdown("""
**Rota:** GRU → FCO  
**Pax:** 2 adultos + 1 criança  
**Direto:** Sim  
**Período:** Setembro/2026 (15 dias)
""")

if st.button("🔄 Rodar busca agora"):
    with st.spinner("Coletando dados..."):
        df = collect()
        st.success("Busca concluída")
        st.dataframe(df, use_container_width=True)
