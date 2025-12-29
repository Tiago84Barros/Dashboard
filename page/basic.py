from __future__ import annotations

import pandas as pd
import streamlit as st

from core.db_supabase import get_engine
from core.helpers import get_logo_url

# ✅ IMPORT CORRETO (Opção A)
from empresa_view import render_empresa_view as exibir_detalhes_empresa


# =============================================================================
# HTML do card de empresa (setor)
# =============================================================================

def _sector_box_html(row: pd.Series) -> str:
    return f"""
    <div style="
        border:1px solid #e0e0e0;
        border-radius:10px;
        padding:12px;
        margin-bottom:12px;
        display:flex;
        justify-content:space-between;
        align-items:center;
        background-color:#fafafa;
    ">
        <div>
            <strong style="font-size:16px;">{row['ticker']}</strong><br>
            <span style="font-size:13px;">
                Setor: {row['SETOR']}<br>
                Subsetor: {row['SUBSETOR']}<br>
                Segmento: {row['SEGMENTO']}
            </span>
        </div>
        <img src="{get_logo_url(row['ticker'])}"
             style="width:48px;height:48px;object-fit:contain;">
    </div>
    """


# =============================================================================
# Render principal
# =============================================================================

def render() -> None:
    st.header("📌 Análise Básica de Ações")

    engine = get_engine()

    # -------------------------------------------------------------------------
    # Sidebar — busca por ticker
    # -------------------------------------------------------------------------
    with st.sidebar:
        st.subheader("Buscar empresa")

        ticker_input = st.text_input(
            "Digite o ticker (ex.: PETR4)",
            key="ticker_input",
        )

        if ticker_input:
            ticker = ticker_input.upper().strip()
            if not ticker.endswith(".SA"):
                ticker += ".SA"
            st.session_state["ticker_selecionado"] = ticker
        else:
            st.session_state.pop("ticker_selecionado", None)

    ticker = st.session_state.get("ticker_selecionado")

    # -------------------------------------------------------------------------
    # Se houver ticker selecionado → exibir detalhes da empresa
    # -------------------------------------------------------------------------
    if ticker:
        exibir_detalhes_empresa(ticker)
        return

    # -------------------------------------------------------------------------
    # Tela inicial — empresas por setor
    # -------------------------------------------------------------------------
    st.subheader("Empresas distribuídas por setor")

    try:
        setores_df = pd.read_sql(
            """
            select
                ticker,
                setor as "SETOR",
                subsetor as "SUBSETOR",
                segmento as "SEGMENTO"
            from cvm.setores
            order by setor, ticker
            """,
            engine,
        )
    except Exception as e:
        st.error("Erro ao carregar a base de setores.")
        st.exception(e)
        return

    if setores_df.empty:
        st.warning("Base de setores não encontrada ou vazia.")
        return

    # -------------------------------------------------------------------------
    # Renderização por setor (grid 3 colunas)
    # -------------------------------------------------------------------------
    for setor, grupo in setores_df.groupby("SETOR"):
        st.markdown(f"### {setor}")

        grupo = grupo.reset_index(drop=True)

        for i in range(0, len(grupo), 3):
            cols = st.columns(3, gap="large")

            for j in range(3):
                if i + j < len(grupo):
                    row = grupo.iloc[i + j]
                    with cols[j]:
                        if st.button(
                            row["ticker"],
                            key=f"btn_{row['ticker']}",
                            use_container_width=True,
                        ):
                            st.session_state["ticker_selecionado"] = row["ticker"] + ".SA"
                            st.rerun()

                        st.markdown(
                            _sector_box_html(row),
                            unsafe_allow_html=True,
                        )
