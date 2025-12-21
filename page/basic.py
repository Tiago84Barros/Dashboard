from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from core.db_loader import (
    load_data_from_db,
    load_multiplos_limitado_from_db,
)
from core.helpers import get_logo_url
from page.empresa_view import render_empresa_view as exibir_detalhes_empresa

pd.set_option("display.float_format", "{:.2f}".format)

# HTML do bloco de exibição por setor -----------------------------------------------------------------------------------------------------------------------------
def _sector_box_html(row: pd.Series) -> str:
    return f"""
    <div class=\"sector-box\">
      <div class=\"sector-info\">
        <strong>{row['ticker']}</strong><br>
        Subsetor: {row['SUBSETOR']}<br>
        Segmento: {row['SEGMENTO']}
      </div>
      <img src=\"{get_logo_url(row['ticker'])}\" class=\"sector-logo\">
    </div>
    """


def render() -> None:
    st.header("Análise Básica de Ações")

    with st.sidebar:
        if st.button("Atualizar dados", key="refresh_button"):
            st.cache_data.clear()
            st.experimental_rerun()

        ticker_input = st.text_input("Buscar ticker (ex.: PETR4)", key="ticker_box")
        if ticker_input.strip():
            ticker = ticker_input.upper()
            if not ticker.endswith(".SA"):
                ticker += ".SA"
            st.session_state["ticker"] = ticker
        elif "ticker" in st.session_state:
            del st.session_state["ticker"]

    ticker = st.session_state.get("ticker", None)
    setores_df = st.session_state.get("setores_df", None)

    # Se houver ticker, exibe os detalhes da empresa ------------------------------------------------------------------------------------------------------------------------------
    if ticker:
        exibir_detalhes_empresa(ticker)
        return

    st.subheader("Empresas distribuídas por setor")
    if setores_df is None or setores_df.empty:
        st.info("Base de setores não carregada.")
        return

    df = setores_df.sort_values(["SETOR", "ticker"])
    for setor, grupo in df.groupby("SETOR"):
        st.markdown(f"### {setor}")
        grupo = grupo.reset_index(drop=True)
        for i in range(0, len(grupo), 3):
            cols = st.columns(3, gap="large")
            for j in range(3):
                if i + j < len(grupo):
                    row = grupo.iloc[i + j]
                    with cols[j]:
                        st.markdown(_sector_box_html(row), unsafe_allow_html=True)
