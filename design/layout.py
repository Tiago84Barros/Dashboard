import streamlit as st

def configurar_pagina(titulo="Dashboard Financeiro"):
    st.set_page_config(page_title=titulo, layout="wide")

def aplicar_estilos_css():
    st.markdown("""
    <style>
    .main {
        background-color: var(--background-color);
        color: var(--text-color);
        padding: 0;
    }
    .stApp {
        background-color: var(--background-color);
        color: var(--text-color);
    }
    div[data-testid="metric-container"] {
        background-color: var(--block-background-color);
        border: 1px solid var(--block-border-color);
        padding: 5% 5% 5% 10%;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0, 0, 0, 0.1);
    }
    div[data-testid="metric-container"] > label {
        color: var(--metric-text-color);
        font-size: 18px;
    }
    div[data-testid="metric-container"] > div > p {
        color: var(--positive-color);
        font-size: 18px;
    }
    button {
        background-color: var(--button-background-color);
        color: var(--button-text-color);
        border-radius: 5px;
        padding: 5px 10px;
        border: none;
    }
    button:hover {
        background-color: var(--button-hover-background-color);
        color: var(--button-hover-text-color);
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <style>
    .reportview-container .main .block-container {
        max-width: 100% !important;
        padding-left: 1rem;
        padding-right: 1rem;
        padding-bottom: 3rem;
    }
    .sector-box {
        border: 1px solid #ccc;
        padding: 20px;
        border-radius: 12px;
        margin-bottom: 20px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        min-height: 140px;
        background: #f9f9f9;
        box-shadow: 0 2px 5px rgba(0, 0, 0, 0.05);
        transition: background-color 0.3s ease, transform 0.2s ease;
    }
    .sector-box:hover {
        background-color: #f0f0f0;
        transform: translateY(-2px);
    }
    .sector-info {
        font-size: 14px;
        color: #333;
        text-align: left;
        flex: 1;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: normal;
    }
    .sector-info strong {
        font-size: 16px;
        color: #000;
    }
    .sector-logo {
        width: 60px;
        height: auto;
        margin-left: 20px;
    }
    </style>
    """, unsafe_allow_html=True)
