from __future__ import annotations

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

from core.db_loader import (
    load_setores_from_db,
    load_empresas_segmento,
    load_empresa_completa,
    load_macro_clean,
)
from core.helpers import (
    get_logo_url,
    determinar_lideres,
)
from core.scoring import calcular_score_acumulado
from core.portfolio import calcular_patrimonio_selic_macro
from core.weights import get_pesos


def render():
    st.markdown("<h1 style='text-align:center;'>Análise Avançada</h1>", unsafe_allow_html=True)

    setores_df = load_setores_from_db()
    macro = load_macro_clean()

    setores_unicos = (
        setores_df[["SETOR", "SUBSETOR", "SEGMENTO"]]
        .drop_duplicates()
        .sort_values(["SETOR", "SUBSETOR", "SEGMENTO"])
    )

    with st.sidebar:
        setor = st.selectbox("Setor", setores_unicos["SETOR"].unique())
        subsetores = setores_unicos[setores_unicos["SETOR"] == setor]["SUBSETOR"].unique()
        subsetor = st.selectbox("Subsetor", subsetores)
        segmentos = setores_unicos[
            (setores_unicos["SETOR"] == setor) &
            (setores_unicos["SUBSETOR"] == subsetor)
        ]["SEGMENTO"].unique()
        segmento = st.selectbox("Segmento", segmentos)

        with st.expander("Parâmetros de Auditoria (opcional)", expanded=False):
            defasagem = st.number_input("Defasagem contábil (anos)", 0, 3, 0)

        analisar = st.button("Analisar Segmento")

    if not analisar:
        st.stop()

    empresas = load_empresas_segmento(setor, subsetor, segmento)
    if empresas.empty:
        st.warning("Nenhuma empresa encontrada para o segmento.")
        return

    dados_empresas = []
    for _, row in empresas.iterrows():
        ticker = row["ticker"]
        pacote = load_empresa_completa(ticker)
        if pacote["anos_hist"] < 4:
            continue
        dados_empresas.append(
            {
                "ticker": ticker,
                "nome": row["nome_empresa"],
                "dre": pacote["dre"],
                "multiplos": pacote["multiplos"],
            }
        )

    if len(dados_empresas) < 2:
        st.warning("Dados insuficientes para análise comparativa.")
        return

    pesos = get_pesos(setor)
    setores_map = {e["ticker"]: setor for e in dados_empresas}

    try:
        score = calcular_score_acumulado(
            dados_empresas,
            setores_map,
            pesos,
            macro,
            anos_minimos=4,
            publication_lag_years=int(defasagem),
        )
    except TypeError:
        score = calcular_score_acumulado(
            dados_empresas,
            setores_map,
            pesos,
            macro,
            anos_minimos=4,
        )

    if score.empty:
        st.warning("Score não pôde ser calculado.")
        return

    lideres = determinar_lideres(score)

    st.markdown(f"## {setor} › {subsetor} › {segmento}")

    colunas = st.columns(3)
    for idx, emp in enumerate(dados_empresas):
        col = colunas[idx % 3]
        tk = emp["ticker"]
        logo = get_logo_url(tk)

        lider_count = lideres[lideres["ticker"] == tk]["Ano"].nunique()

        col.markdown(
            f"""
            <div style="border:1px solid #ddd;border-radius:8px;padding:10px;text-align:center;">
                <img src="{logo}" width="40"/>
                <p><strong>{emp['nome']}</strong></p>
                <p style="font-size:12px;">{tk}</p>
                <p style="font-size:12px;color:#666;">{lider_count}x líder</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with st.expander("Diagnóstico do Segmento", expanded=False):
        st.write("Empresas analisadas:", len(dados_empresas))
        st.write("Período do score:", f"{int(score['Ano'].min())} → {int(score['Ano'].max())}")
        st.write("Defasagem aplicada:", int(defasagem))

    # KPI simples: estabilidade
    if not lideres.empty:
        st.markdown("### Estabilidade de liderança")
        trocas = lideres.sort_values("Ano")["ticker"].ne(
            lideres.sort_values("Ano")["ticker"].shift()
        ).sum() - 1
        st.write("Trocas de líder no período:", max(int(trocas), 0))

    # Benchmark Selic (informativo)
    patrimonio_selic = calcular_patrimonio_selic_macro(macro, score["Ano"].unique())
    if patrimonio_selic is not None and not patrimonio_selic.empty:
        fig, ax = plt.subplots()
        patrimonio_selic.plot(ax=ax, legend=False)
        ax.set_title("Evolução do Tesouro Selic (benchmark)")
        ax.set_ylabel("Índice")
        st.pyplot(fig)
