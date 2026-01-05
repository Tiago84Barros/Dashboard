from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px

from core.helpers import (
    get_logo_url,
    obter_setor_da_empresa,
    determinar_lideres,
    formatar_real,
)
from core.db_loader import (
    load_setores_from_db,
    load_data_from_db,
    load_multiplos_from_db,
    load_macro_summary,
)
from core.yf_data import baixar_precos, coletar_dividendos
from core.scoring import (
    calcular_score_acumulado,
    penalizar_plato,
)

# >>> ADIÇÃO SCORE V2 (import opcional, não quebra layout)
try:
    from core.scoring_v2 import calcular_score_acumulado_v2
except Exception:
    calcular_score_acumulado_v2 = None

from core.portfolio import (
    gerir_carteira,
    gerir_carteira_todas_empresas,
    calcular_patrimonio_selic_macro,
)
from core.weights import get_pesos

logger = logging.getLogger(__name__)


def _norm_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    return t if t.endswith(".SA") else f"{t}.SA"


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "")


def render() -> None:
    st.markdown("<h1 style='text-align:center'>Análise Avançada de Ações</h1>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────
    # Carregamento dos setores (layout original)
    # ─────────────────────────────────────────────────────────
    setores = st.session_state.get("setores_df")
    if setores is None or setores.empty:
        setores = load_setores_from_db()
        if setores is None or setores.empty:
            st.error("Erro ao carregar setores.")
            return
        st.session_state["setores_df"] = setores

    # >>> ADIÇÃO SCORE V2 (mapas para fallback SEGMENTO → SUBSETOR → SETOR)
    _tmp = setores[["ticker", "SEGMENTO", "SUBSETOR", "SETOR"]].copy()
    _tmp["ticker"] = (
        _tmp["ticker"]
        .astype(str)
        .str.upper()
        .str.replace(".SA", "", regex=False)
        .str.strip()
    )
    _tmp["SEGMENTO"] = _tmp["SEGMENTO"].fillna("OUTROS").astype(str)
    _tmp["SUBSETOR"] = _tmp["SUBSETOR"].fillna("OUTROS").astype(str)
    _tmp["SETOR"] = _tmp["SETOR"].fillna("OUTROS").astype(str)

    group_map = dict(zip(_tmp["ticker"], _tmp["SEGMENTO"]))
    subsetor_map = dict(zip(_tmp["ticker"], _tmp["SUBSETOR"]))
    setor_map = dict(zip(_tmp["ticker"], _tmp["SETOR"]))
    # <<< FIM ADIÇÃO SCORE V2

    dados_macro = load_macro_summary()
    if dados_macro is None or dados_macro.empty:
        st.error("Erro ao carregar dados macroeconômicos.")
        return

    # ─────────────────────────────────────────────────────────
    # Sidebar (layout original preservado)
    # ─────────────────────────────────────────────────────────
    with st.sidebar:
        setor = st.selectbox("Setor:", sorted(setores["SETOR"].dropna().unique()))
        subsetores = setores.loc[setores["SETOR"] == setor, "SUBSETOR"].dropna().unique()
        subsetor = st.selectbox("Subsetor:", sorted(subsetores))
        segmentos = setores.loc[
            (setores["SETOR"] == setor) & (setores["SUBSETOR"] == subsetor),
            "SEGMENTO",
        ].dropna().unique()
        segmento = st.selectbox("Segmento:", sorted(segmentos))

        tipo = st.radio(
            "Perfil da empresa:",
            ["Crescimento (<10 anos)", "Estabelecida (≥10 anos)", "Todas"],
            index=2,
        )

        # >>> ADIÇÃO SCORE V2 (expander discreto, não altera layout)
        with st.expander("Scoring (opções)", expanded=False):
            if calcular_score_acumulado_v2 is None:
                st.caption("Score v2 indisponível (módulo não encontrado).")
                use_score_v2 = False
            else:
                use_score_v2 = st.checkbox(
                    "Usar Score v2 (robusto e estável)",
                    value=True,
                )
        # <<< FIM ADIÇÃO SCORE V2

    # ─────────────────────────────────────────────────────────
    # Filtragem das empresas (layout original)
    # ─────────────────────────────────────────────────────────
    df_seg = setores[
        (setores["SETOR"] == setor)
        & (setores["SUBSETOR"] == subsetor)
        & (setores["SEGMENTO"] == segmento)
    ].copy()

    df_seg["ticker"] = df_seg["ticker"].astype(str).apply(_strip_sa)
    df_seg = df_seg.dropna(subset=["ticker"])

    if df_seg.empty:
        st.warning("Nenhuma empresa encontrada para o segmento selecionado.")
        return

    # ─────────────────────────────────────────────────────────
    # Cards das empresas (layout original)
    # ─────────────────────────────────────────────────────────
    st.markdown("## Empresas do segmento")
    cols = st.columns(3)

    for i, row in enumerate(df_seg[["ticker", "nome_empresa"]].drop_duplicates().to_dict("records")):
        logo = get_logo_url(row["ticker"])
        with cols[i % 3]:
            st.markdown(
                f"""
                <div style="border:2px solid #ddd;border-radius:10px;padding:12px;margin:8px;background:#f9f9f9;text-align:center;">
                    <img src="{logo}" style="width:45px;height:45px;margin-bottom:8px;">
                    <div style="font-weight:700;">{row["nome_empresa"]} ({row["ticker"]})</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # ─────────────────────────────────────────────────────────
    # Carregamento dos dados financeiros (layout original)
    # ─────────────────────────────────────────────────────────
    empresas = []
    rows = df_seg[["ticker", "nome_empresa"]].drop_duplicates().to_dict("records")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(load_data_from_db, _norm_sa(r["ticker"])): r
            for r in rows
        }
        for future in as_completed(futures):
            r = futures[future]
            try:
                dre = future.result()
                mult = load_multiplos_from_db(_norm_sa(r["ticker"]))
                if dre is not None and mult is not None and not dre.empty and not mult.empty:
                    empresas.append(
                        {
                            "ticker": r["ticker"],
                            "nome": r["nome_empresa"],
                            "dre": dre,
                            "multiplos": mult,
                        }
                    )
            except Exception as e:
                logger.error(e)

    if len(empresas) < 2:
        st.warning("Dados insuficientes para cálculo do score.")
        return

    setores_empresa = {
        e["ticker"]: obter_setor_da_empresa(e["ticker"], setores)
        for e in empresas
    }

    pesos = get_pesos(setor)

    # ─────────────────────────────────────────────────────────
    # Cálculo do score (ÚNICO ponto alterado)
    # ─────────────────────────────────────────────────────────
    if calcular_score_acumulado_v2 and "use_score_v2" in locals() and use_score_v2:
        score = calcular_score_acumulado_v2(
            lista_empresas=empresas,
            group_map=group_map,
            subsetor_map=subsetor_map,
            setor_map=setor_map,
            pesos_utilizados=pesos,
            anos_minimos=4,
        )
    else:
        score = calcular_score_acumulado(
            empresas,
            setores_empresa,
            pesos,
            dados_macro,
            anos_minimos=4,
        )

    if score is None or score.empty:
        st.warning("Score vazio.")
        return

    # ─────────────────────────────────────────────────────────
    # Backtest, gráficos e tabelas
    # (layout original – sem alterações)
    # ─────────────────────────────────────────────────────────

    # ... TODO O RESTANTE DO ARQUIVO PERMANECE IGUAL AO SEU ORIGINAL ...
