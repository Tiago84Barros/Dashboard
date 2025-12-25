from __future__ import annotations

import pandas as pd
import streamlit as st

from core.helpers import get_logo_url
from page.empresa_view import render_empresa_view as exibir_detalhes_empresa

pd.set_option("display.float_format", "{:.2f}".format)


def _sector_box_html(row: pd.Series) -> str:
    ticker = row.get("ticker", "—")
    subsetor = row.get("SUBSETOR", "—")
    segmento = row.get("SEGMENTO", "—")

    return f"""
    <div class="sector-box">
      <div class="sector-info">
        <strong>{ticker}</strong><br>
        Subsetor: {subsetor}<br>
        Segmento: {segmento}
      </div>
      <img src="{get_logo_url(ticker)}" class="sector-logo">
    </div>
    """


def _get_ticker_from_state() -> str | None:
    """
    Compatível com o dashboard:
      - ticker_selecionado / ticker_filtrado (padrão novo)
      - ticker (legado)
    Retorna ticker normalizado (ex: PETR4) sem sufixo .SA.
    """
    t = (
        st.session_state.get("ticker_selecionado")
        or st.session_state.get("ticker_filtrado")
        or st.session_state.get("ticker")
    )

    if not t:
        return None

    t = str(t).strip().upper()
    # Se vier no formato PETR4.SA, normaliza para PETR4
    if t.endswith(".SA"):
        t = t[:-3]
    return t or None


def render() -> None:
    st.header("Análise Básica de Ações")

    # Botão opcional para atualizar (sem sidebar local)
    col1, col2 = st.columns([1, 6])
    with col1:
        if st.button("Atualizar dados", key="basic_refresh"):
            st.cache_data.clear()
            st.rerun()

    ticker = _get_ticker_from_state()
    setores_df = st.session_state.get("setores_df")

    # Se houver ticker, exibe detalhes da empresa
    if ticker:
        exibir_detalhes_empresa(ticker)
        return

    st.subheader("Empresas distribuídas por setor")
    if setores_df is None or getattr(setores_df, "empty", True):
        st.info("Base de setores não carregada.")
        return

    # Garantias mínimas para não quebrar caso alguma coluna não exista
    df = setores_df.copy()
    for col in ["SETOR", "SUBSETOR", "SEGMENTO", "ticker"]:
        if col not in df.columns:
            df[col] = "—"

    df = df.sort_values(["SETOR", "ticker"])

    for setor, grupo in df.groupby("SETOR", dropna=False):
        st.markdown(f"### {setor}")
        grupo = grupo.reset_index(drop=True)

        for i in range(0, len(grupo), 3):
            cols = st.columns(3, gap="large")
            for j in range(3):
                idx = i + j
                if idx < len(grupo):
                    row = grupo.iloc[idx]
                    with cols[j]:
                        st.markdown(_sector_box_html(row), unsafe_allow_html=True)
