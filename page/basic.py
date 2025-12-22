from __future__ import annotations

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

from core.db_loader import load_setores_from_db
from core.helpers import obter_setor_da_empresa
from core.yf_data import baixar_precos
from core.portfolio import gerir_carteira_simples


def render():
    st.title("Análise Básica")

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

    empresas = setores_df[
        (setores_df["SETOR"] == setor)
        & (setores_df["SUBSETOR"] == subsetor)
        & (setores_df["SEGMENTO"] == segmento)
    ]["ticker"].dropna().unique().tolist()

    if not empresas:
        st.warning("Nenhuma empresa encontrada para o filtro selecionado.")
        return

    tickers = [f"{e}.SA" for e in empresas]
    precos = baixar_precos(tickers)

    if precos is None or precos.empty:
        st.error("Não foi possível baixar os preços.")
        return

    patrimonio = gerir_carteira_simples(precos)

    fig, ax = plt.subplots()
    patrimonio.plot(ax=ax)
    ax.set_title("Evolução do Patrimônio")
    ax.set_xlabel("Data")
    ax.set_ylabel("Valor (R$)")
    ax.grid(True)

    st.pyplot(fig)
