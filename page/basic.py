"""
basic.py
~~~~~~~~
Página de Análise Básica – Streamlit

Objetivo:
- Visão inicial das empresas por setor
- Busca rápida por ticker
- Acesso direto ao detalhe da empresa
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from core.db_loader import (
    load_data_from_db,
    load_multiplos_limitado_from_db,
)
from core.helpers import get_logo_url
from page.empresa_view import render_empresa_view as exibir_detalhes_empresa

pd.set_option("display.float_format", "{:.2f}".format)


# ─────────────────────────────────────────────────────────────
# HTML helper – caixa por empresa
# ─────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────
# Render principal
# ─────────────────────────────────────────────────────────────
def render() -> None:
    st.header("Análise Básica de Ações")

    # ───────────────────────── Sidebar ─────────────────────────
    with st.sidebar:
        if st.button("Atualizar dados", key="refresh_button"):
            st.cache_data.clear()
            st.rerun()

        ticker_input = st.text_input(
            "Buscar ticker (ex.: PETR4)",
            key="ticker_box",
        )

        if ticker_input.strip():
            ticker = ticker_input.upper()
            if not ticker.endswith(".SA"):
                ticker += ".SA"
            st.session_state["ticker"] = ticker
        else:
            st.session_state.pop("ticker", None)

    ticker = st.session_state.get("ticker")
    setores_df = st.session_state.get("setores_df")

    # ───────────────────────── Visão por ticker ─────────────────────────
    if ticker:
        exibir_detalhes_empresa(ticker)
        return

    # ───────────────────────── Visão por setor ─────────────────────────
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
                        st.markdown(
                            _sector_box_html(row),
                            unsafe_allow_html=True,
                        )
