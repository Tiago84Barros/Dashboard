from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from core.db.engine import get_engine
from core.db.loader import (
    load_demonstracoes_financeiras,
    load_multiplos,
    load_setores,
)
from analytics.helpers import get_logo_url
from page.empresa_view import render_empresa_view as exibir_detalhes_empresa

pd.set_option("display.float_format", "{:.2f}".format)


# -------------------------------
# Engine e carregamentos com cache
# -------------------------------
@st.cache_resource
def _engine():
    return get_engine()


@st.cache_data(ttl=3600)
def _load_setores_cached() -> pd.DataFrame:
    return load_setores(engine=_engine())


@st.cache_data(ttl=3600)
def _load_dre_cached(ticker: str) -> pd.DataFrame:
    return load_demonstracoes_financeiras(ticker, engine=_engine())


@st.cache_data(ttl=3600)
def _load_multiplos_cached(ticker: str) -> pd.DataFrame:
    return load_multiplos(ticker, engine=_engine())


def _load_multiplos_limitado(ticker: str, anos: int = 12) -> pd.DataFrame:
    """
    Substitui o legado load_multiplos_limitado_from_db.
    Mantém compatibilidade funcional: carrega tudo e aplica um filtro de anos se houver coluna de data.
    """
    df = _load_multiplos_cached(ticker)
    if df is None or df.empty:
        return df

    # tenta achar uma coluna de data padrão do seu pipeline (data/Data)
    col_data = None
    for c in ("data", "Data"):
        if c in df.columns:
            col_data = c
            break

    if not col_data:
        return df

    df = df.copy()
    df[col_data] = pd.to_datetime(df[col_data], errors="coerce")
    df = df.dropna(subset=[col_data])

    if df.empty:
        return df

    cutoff = pd.Timestamp.today().normalize() - pd.DateOffset(years=anos)
    return df[df[col_data] >= cutoff].sort_values(col_data)


# HTML do bloco de exibição por setor
def _sector_box_html(row: pd.Series) -> str:
    return f"""
    <div class="sector-box">
      <div class="sector-info">
        <strong>{row['ticker']}</strong><br>
        Subsetor: {row['SUBSETOR']}<br>
        Segmento: {row['SEGMENTO']}
      </div>
      <img src="{get_logo_url(row['ticker'])}" class="sector-logo">
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

    # garante setores no session_state (padrão novo: Supabase via loader)
    setores_df = st.session_state.get("setores_df")
    if setores_df is None or (isinstance(setores_df, pd.DataFrame) and setores_df.empty):
        try:
            setores_df = _load_setores_cached()
            st.session_state["setores_df"] = setores_df
        except Exception as e:
            st.error(f"Falha ao carregar setores no Supabase: {e}")
            return

    # Se houver ticker, exibe os detalhes da empresa
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
