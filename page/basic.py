from __future__ import annotations

import pandas as pd
import streamlit as st

from core.helpers import get_logo_url
from page.empresa_view import render_empresa_view as exibir_detalhes_empresa

pd.set_option("display.float_format", "{:.2f}".format)


# HTML do bloco de exibição por setor
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


def _norm_ticker_input(raw: str) -> str | None:
    """
    Normaliza o ticker digitado.
    Aceita: petr4 | PETR4 | PETR4.SA
    Retorna: PETR4.SA (padrão do seu algoritmo) ou None.
    """
    if not raw:
        return None

    t = str(raw).strip().upper()
    if not t:
        return None

    if not t.endswith(".SA"):
        t += ".SA"
    return t


def _safe_exibir_detalhes_empresa(ticker: str) -> None:
    """
    Renderiza a visão da empresa com tolerância a variações na assinatura.
    """
    try:
        # forma posicional
        exibir_detalhes_empresa(ticker)
        return
    except TypeError:
        # forma keyword (caso a assinatura tenha mudado)
        try:
            exibir_detalhes_empresa(ticker=ticker)  # type: ignore[call-arg]
            return
        except Exception as e:
            st.error("Falha inesperada ao renderizar os detalhes da empresa.")
            st.exception(e)
            return
    except Exception as e:
        st.error("Falha inesperada ao renderizar os detalhes da empresa.")
        st.exception(e)
        return


def render() -> None:
    st.header("Análise Básica de Ações")

    # Sidebar (seguindo seu algoritmo)
    with st.sidebar:
        if st.button("Atualizar dados", key="refresh_button"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()

        ticker_input = st.text_input("Buscar ticker (ex.: PETR4)", key="ticker_box")

        ticker_norm = _norm_ticker_input(ticker_input)
        if ticker_norm:
            st.session_state["ticker"] = ticker_norm
        else:
            # se o usuário apagou o campo, remove o ticker
            if "ticker" in st.session_state:
                del st.session_state["ticker"]

    ticker = st.session_state.get("ticker", None)
    setores_df = st.session_state.get("setores_df", None)

    # Se houver ticker, exibe os detalhes da empresa
    if ticker:
        _safe_exibir_detalhes_empresa(ticker)
        return

    # Grid por setor
    st.subheader("Empresas distribuídas por setor")
    if setores_df is None or getattr(setores_df, "empty", True):
        st.info("Base de setores não carregada.")
        return

    df = setores_df.copy()

    # Garantias mínimas para evitar KeyError
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
