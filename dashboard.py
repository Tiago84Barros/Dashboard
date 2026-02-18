import streamlit as st
import importlib

st.set_page_config(page_title="Dashboard Financeiro", layout="wide")

PAGES = {
    "Básica": "page.basica",
    "Avançada": "page.advanced",
    "Criação de Portfólio": "page.criacao_portfolio",
    "Análises de Portfólio": "page.analises_portfolio",
    "Configurações": "page.configuracoes",
}

st.sidebar.title("Análises")
pagina_escolhida = st.sidebar.radio("Escolha a seção:", list(PAGES.keys()))

try:
    module = importlib.import_module(PAGES[pagina_escolhida])
    module.render()
except Exception as e:
    st.error("Falha ao carregar a página selecionada.")
    st.exception(e)
