import streamlit as st

def render():
    st.title("Análises de Portfólio")

    if "portfolio_salvo" not in st.session_state:
        st.warning("Execute primeiro a Criação de Portfólio.")
        return

    portfolio = st.session_state["portfolio_salvo"]

    st.subheader("Portfólio Carregado")
    st.json(portfolio)

    if st.button("Executar Análise LLM"):
        st.info("Simulação de análise executada com sucesso.")
