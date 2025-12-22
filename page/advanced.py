from __future__ import annotations

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px

from core.db_loader import load_setores_from_db, load_data_from_db, load_multiplos_from_db
from core.helpers import obter_setor_da_empresa
from core.yf_data import baixar_precos, coletar_dividendos
from core.scoring import calcular_score_acumulado
from core.portfolio import gerir_carteira, calcular_patrimonio_selic_macro
from core.weights import get_pesos
from core.macro import load_macro_summary


def render():
    st.title("Análise Avançada")

    setores_df = load_setores_from_db()
    if setores_df is None or setores_df.empty:
        st.error("Não foi possível carregar os setores.")
        return

    setor = st.selectbox("Setor", sorted(setores_df["SETOR"].unique()))
    subsetor = st.selectbox(
        "Subsetor",
        sorted(setores_df[setores_df["SETOR"] == setor]["SUBSETOR"].unique()),
    )
    segmento = st.selectbox(
        "Segmento",
        sorted(
            setores_df[
                (setores_df["SETOR"] == setor)
                & (setores_df["SUBSETOR"] == subsetor)
            ]["SEGMENTO"].unique()
        ),
    )

    empresas_df = setores_df[
        (setores_df["SETOR"] == setor)
        & (setores_df["SUBSETOR"] == subsetor)
        & (setores_df["SEGMENTO"] == segmento)
    ]

    if empresas_df.empty:
        st.warning("Nenhuma empresa encontrada para o filtro selecionado.")
        return

    dados_empresas = []
    for _, row in empresas_df.iterrows():
        ticker = f"{row['ticker']}.SA"

        dre = load_data_from_db(ticker)
        mult = load_multiplos_from_db(ticker)

        if dre is None or mult is None:
            continue

        dados_empresas.append(
            {
                "ticker": row["ticker"],
                "nome": row.get("nome_empresa", row["ticker"]),
                "dre": dre,
                "multiplos": mult,
            }
        )

    if not dados_empresas:
        st.warning("Dados insuficientes para análise.")
        return

    setores_empresa = {
        e["ticker"]: obter_setor_da_empresa(e["ticker"], setores_df)
        for e in dados_empresas
    }

    pesos = get_pesos(setor)
    macro = load_macro_summary()

    score = calcular_score_acumulado(
        dados_empresas,
        setores_empresa,
        pesos,
        macro,
        anos_minimos=4,
    )

    if score is None or score.empty:
        st.warning("Score vazio.")
        return

    tickers = [f"{t}.SA" for t in score["ticker"].unique()]
    precos = baixar_precos(tickers)
    dividendos = coletar_dividendos(tickers)

    patrimonio = gerir_carteira(precos, score, dividendos)

    patrimonio_selic = calcular_patrimonio_selic_macro(macro, patrimonio.index)

    fig, ax = plt.subplots()
    patrimonio.plot(ax=ax, label="Estratégia")
    patrimonio_selic.plot(ax=ax, label="Tesouro Selic")
    ax.legend()
    ax.set_title("Evolução do Patrimônio")
    ax.grid(True)

    st.pyplot(fig)

    st.subheader("Score por Empresa")
    st.dataframe(score)

    fig2 = px.bar(
        score,
        x="ticker",
        y="score_final",
        title="Score Final por Empresa",
    )
    st.plotly_chart(fig2, use_container_width=True)
