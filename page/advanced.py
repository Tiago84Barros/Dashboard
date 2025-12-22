from __future__ import annotations

import pandas as pd
import streamlit as st

from core.data_access import (
    load_data_from_db,
    load_multiplos_limitado_from_db,
)
from page.empresa_view import render_empresa_view as exibir_detalhes_empresa

pd.set_option("display.float_format", "{:.2f}".format)


def render() -> None:
    """
    Página de Análise Avançada.

    Regras:
    - NÃO usa sidebar (sidebar é responsabilidade exclusiva do dashboard.py)
    - Se houver ticker em session_state -> abre página da empresa
    - Caso contrário -> mostra filtros por setor/segmento/subsetor + ranking
    """

    st.header("Análise Avançada de Ações")

    # ─────────────────────────────────────────────────────────────
    # 1) Se existir ticker selecionado no sidebar global, prioriza empresa
    # ─────────────────────────────────────────────────────────────
    ticker = st.session_state.get("ticker")
    if ticker:
        exibir_detalhes_empresa(ticker)
        return

    # ─────────────────────────────────────────────────────────────
    # 2) Carrega base de setores (cache em session_state)
    # ─────────────────────────────────────────────────────────────
    setores_df = st.session_state.get("setores_df")

    if setores_df is None:
        try:
            setores_df = load_data_from_db("setores")
            st.session_state["setores_df"] = setores_df
        except Exception:
            setores_df = None

    if setores_df is None or setores_df.empty:
        st.info("Base de setores não carregada.")
        return

    # Garantia de colunas esperadas
    df = setores_df.copy()
    for col in ["SETOR", "SEGMENTO", "SUBSETOR", "ticker"]:
        if col not in df.columns:
            df[col] = None

    # ─────────────────────────────────────────────────────────────
    # 3) Filtros (sempre visíveis na tela)
    # ─────────────────────────────────────────────────────────────
    st.subheader("Filtros avançados")

    f1, f2, f3 = st.columns(3)

    setores = ["Todos"] + sorted(
        [x for x in df["SETOR"].dropna().unique() if str(x).strip()]
    )
    with f1:
        setor_sel = st.selectbox("Setor", setores, index=0)

    df_setor = df if setor_sel == "Todos" else df[df["SETOR"] == setor_sel]

    segmentos = ["Todos"] + sorted(
        [x for x in df_setor["SEGMENTO"].dropna().unique() if str(x).strip()]
    )
    with f2:
        segmento_sel = st.selectbox("Segmento", segmentos, index=0)

    df_segmento = (
        df_setor if segmento_sel == "Todos" else df_setor[df_setor["SEGMENTO"] == segmento_sel]
    )

    subsetores = ["Todos"] + sorted(
        [x for x in df_segmento["SUBSETOR"].dropna().unique() if str(x).strip()]
    )
    with f3:
        subsetor_sel = st.selectbox("Subsetor", subsetores, index=0)

    df_final = (
        df_segmento
        if subsetor_sel == "Todos"
        else df_segmento[df_segmento["SUBSETOR"] == subsetor_sel]
    )

    tickers = sorted(df_final["ticker"].dropna().unique().tolist())

    if not tickers:
        st.warning("Nenhuma empresa encontrada com os filtros selecionados.")
        return

    st.caption(f"{len(tickers)} empresas encontradas.")

    # ─────────────────────────────────────────────────────────────
    # 4) Ranking por múltiplos / indicadores
    # ─────────────────────────────────────────────────────────────
    st.subheader("Ranking por múltiplos e indicadores")

    try:
        multiplos_df = load_multiplos_limitado_from_db(tickers)
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos: {e}")
        return

    if multiplos_df is None or multiplos_df.empty:
        st.info("Nenhum dado fundamental disponível para os filtros selecionados.")
        return

    # Ordenação padrão (ajuste fácil depois)
    if "ROE" in multiplos_df.columns:
        multiplos_df = multiplos_df.sort_values("ROE", ascending=False)

    st.dataframe(
        multiplos_df,
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "💡 Dica: digite um ticker no campo de busca do sidebar para abrir a página detalhada da empresa."
    )
