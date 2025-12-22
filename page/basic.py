from __future__ import annotations

import pandas as pd
import streamlit as st

from core.helpers import get_logo_url
from page.empresa_view import render_empresa_view as exibir_detalhes_empresa

pd.set_option("display.float_format", "{:.2f}".format)


def _sector_box_html(row: pd.Series) -> str:
    return f"""
    <div class="sector-box">
      <div class="sector-info">
        <strong>{row['ticker']}</strong><br>
        Subsetor: {row.get('SUBSETOR', '—')}<br>
        Segmento: {row.get('SEGMENTO', '—')}
      </div>
      <img src="{get_logo_url(row['ticker'])}" class="sector-logo">
    </div>
    """


def render() -> None:
    st.header("Análise Básica de Ações")

    # O ticker vem do dashboard.py (campo "Buscar ticker" no sidebar)
    ticker = st.session_state.get("ticker", None)

    # Base de setores deve estar em session_state (carregada pelo seu fluxo)
    setores_df = st.session_state.get("setores_df", None)

    # Se houver ticker, exibe os detalhes da empresa
    if ticker:
        exibir_detalhes_empresa(ticker)
        return

    st.subheader("Empresas distribuídas por setor")
    if setores_df is None or getattr(setores_df, "empty", True):
        st.info("Base de setores não carregada. Vá em Configurações e execute a atualização/ingest.")
        return

    # Render por setor
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
