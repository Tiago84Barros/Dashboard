import streamlit as st
from datetime import datetime

def salvar_portfolio(tickers, pesos, selic):
    st.session_state["portfolio_salvo"] = {
        "tickers": tickers,
        "pesos": pesos,
        "selic": selic,
        "timestamp": datetime.now().isoformat()
    }

def render():
    st.title("Criação de Portfólio")

    tickers = st.text_input("Tickers (separados por vírgula)", "PETR4, VALE3")
    selic = st.number_input("Selic considerada (%)", value=10.75)

    if st.button("Criar Portfólio"):
        lista = [t.strip().upper() for t in tickers.split(",")]
        pesos = {t: round(100/len(lista),2) for t in lista}

        salvar_portfolio(lista, pesos, selic)

        st.success("Portfólio criado com sucesso!")
        st.json(pesos)
