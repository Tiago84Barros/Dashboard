"""
advanced.py
~~~~~~~~~~~
Página de Análise Avançada – Streamlit

Responsável por:
- Análise fundamentalista agregada
- Comparações interanuais
- Contexto macroeconômico
- Visão estratégica (não operacional)

Usada pelo dashboard.py
"""

from __future__ import annotations

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px

from core.db_loader import (
    load_macro_summary,
    load_data_from_db,
    load_multiplos_from_db,
    load_multiplos_tri_from_db,
)

pd.set_option("display.float_format", "{:.2f}".format)


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
def _metric_card(label: str, value, help_text: str | None = None):
    st.metric(label, value=value, help=help_text)


def _safe_last(series: pd.Series):
    if series is None or series.dropna().empty:
        return None
    return series.dropna().iloc[-1]


# ─────────────────────────────────────────────────────────────
# Render principal
# ─────────────────────────────────────────────────────────────
def render() -> None:
    st.header("Análise Avançada")

    st.markdown(
        """
        Esta seção consolida **fundamentos, múltiplos e macroeconomia**.
        O objetivo não é timing, mas **entendimento estrutural e risco**.
        """
    )

    # ───────────────────────── Sidebar ─────────────────────────
    with st.sidebar:
        st.markdown("## Parâmetros avançados")

        ticker_input = st.text_input(
            "Ticker para análise avançada (ex: PETR4)",
            key="adv_ticker_input",
        )

        if ticker_input.strip():
            ticker = ticker_input.upper()
            if not ticker.endswith(".SA"):
                ticker += ".SA"
            st.session_state["adv_ticker"] = ticker
        else:
            st.session_state.pop("adv_ticker", None)

    ticker = st.session_state.get("adv_ticker")

    if not ticker:
        st.info("Informe um ticker para iniciar a análise avançada.")
        return

    # ───────────────────────── Dados base ─────────────────────────
    df_fin = load_data_from_db(ticker)
    df_mult = load_multiplos_from_db(ticker)
    df_mult_tri = load_multiplos_tri_from_db(ticker)
    df_macro = load_macro_summary()

    if df_fin is None or df_fin.empty:
        st.warning("Dados financeiros não encontrados para o ticker.")
        return

    # ───────────────────────── Visão Fundamental ─────────────────────────
    st.subheader("Fundamentos – Visão Estrutural")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        _metric_card(
            "ROE",
            _safe_last(df_fin.get("ROE")),
            "Retorno sobre patrimônio líquido",
        )

    with col2:
        _metric_card(
            "ROIC",
            _safe_last(df_fin.get("ROIC")),
            "Retorno sobre capital investido",
        )

    with col3:
        _metric_card(
            "Margem Líquida",
            _safe_last(df_fin.get("Margem_Liquida")),
            "Eficiência final do negócio",
        )

    with col4:
        _metric_card(
            "Dívida Líquida",
            _safe_last(df_fin.get("Divida_Liquida")),
            "Endividamento líquido",
        )

    # ───────────────────────── Gráfico – Crescimento ─────────────────────────
    st.subheader("Crescimento Histórico")

    growth_cols = [
        c
        for c in ["Receita_Liquida", "Lucro_Liquido"]
        if c in df_fin.columns
    ]

    if growth_cols:
        fig = px.line(
            df_fin,
            x="Data",
            y=growth_cols,
            title="Evolução de Receita e Lucro",
            markers=True,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ───────────────────────── Múltiplos ─────────────────────────
    st.subheader("Múltiplos de Mercado")

    if df_mult is not None and not df_mult.empty:
        mult_cols = [
            c for c in ["P_L", "P_VP", "EV_EBITDA"] if c in df_mult.columns
        ]

        if mult_cols:
            fig = px.line(
                df_mult,
                x="Data",
                y=mult_cols,
                title="Múltiplos Históricos",
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Múltiplos históricos indisponíveis.")

    # ───────────────────────── TTM / TRI ─────────────────────────
    st.subheader("Indicadores Recentes (TRI / TTM)")

    if df_mult_tri is not None and not df_mult_tri.empty:
        st.dataframe(df_mult_tri, use_container_width=True)
    else:
        st.info("Dados trimestrais não disponíveis.")

    # ───────────────────────── Macro Contexto ─────────────────────────
    st.subheader("Contexto Macroeconômico")

    if df_macro is not None and not df_macro.empty:
        macro_cols = ["selic", "ipca", "cambio", "pib"]

        available = [c for c in macro_cols if c in df_macro.columns]

        if available:
            fig = px.line(
                df_macro,
                x="Data",
                y=available,
                title="Indicadores Macroeconômicos",
            )
            st.plotly_chart(fig, use_container_width=True)

    # ───────────────────────── Diagnóstico Final ─────────────────────────
    st.subheader("Leitura Estratégica")

    st.markdown(
        """
        **Perguntas que esta análise ajuda a responder:**
        - O negócio gera retorno acima do custo de capital?
        - Cresce com consistência ou depende de ciclo?
        - O valuation atual faz sentido frente ao histórico?
        - O ambiente macro favorece ou pressiona o ativo?

        👉 Esta página **não sugere compra ou venda**.  
        Ela fornece **base racional** para decisões de portfólio.
        """
    )
