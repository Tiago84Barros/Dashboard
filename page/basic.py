from __future__ import annotations

import pandas as pd
import streamlit as st

from core.data_access import load_data_from_db
from core.helpers import get_logo_url
from page.empresa_view import render_empresa_view as exibir_detalhes_empresa

pd.set_option("display.float_format", "{:.2f}".format)


def _sector_box_html(row: pd.Series) -> str:
    return f"""
    <div class="sector-box">
      <div class="sector-info">
        <strong>{row['ticker']}</strong><br>
        Subsetor: {row.get('SUBSETOR','-')}<br>
        Segmento: {row.get('SEGMENTO','-')}
      </div>
      <img src="{get_logo_url(row['ticker'])}" class="sector-logo">
    </div>
    """


def render() -> None:
    st.header("Análise Básica de Ações")

    # Lê ticker do sidebar global (dashboard.py)
    ticker = st.session_state.get("ticker", None)
    setores_df = st.session_state.get("setores_df", None)

    # Se houver ticker, exibe detalhes
    if ticker:
        exibir_detalhes_empresa(ticker)
        return

    st.subheader("Empresas distribuídas por setor")

    # Carrega base de setores sob demanda (se ainda não existir)
    if setores_df is None:
        try:
            setores_df = load_data_from_db("setores")
            st.session_state["setores_df"] = setores_df
        except Exception:
            setores_df = None

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
