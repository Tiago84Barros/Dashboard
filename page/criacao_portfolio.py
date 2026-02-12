
from __future__ import annotations

import streamlit as st
import pandas as pd
import numpy as np

from core.db_loader import carregar_dados
from core.scoring_v2 import calcular_score
from core.portfolio import (
    determinar_lideres,
    gerir_carteira,
    gerir_carteira_modulada,
)

def filtrar_por_tipo(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    if df.empty:
        return df

    if tipo == "Crescimento (<10 anos)":
        return df[(df["anos_historico"] > 0) & (df["anos_historico"] < 10)]

    if tipo == "Estabelecida (≥10 anos)":
        return df[df["anos_historico"] >= 10]

    return df[df["anos_historico"] > 0]


def render():

    st.title("Criação de Portfólio")

    with st.sidebar.form("form_criacao"):

        tipo_empresa = st.selectbox(
            "Perfil de empresa (histórico DRE)",
            [
                "Todas",
                "Crescimento (<10 anos)",
                "Estabelecida (≥10 anos)",
            ],
        )

        margem_minima = st.number_input(
            "Margem mínima vs Tesouro Selic (%)",
            min_value=0.0,
            max_value=500.0,
            value=150.0,
            step=1.0,
        )

        executar = st.form_submit_button(
            "Rodar Criação de Portfólio",
            type="primary",
            use_container_width=True,
        )

    if not executar:
        return

    with st.spinner("Gerando portfólio..."):

        df_base = carregar_dados()

        segmentos_aprovados = 0
        carteira_final = []

        for segmento in df_base["segmento"].unique():

            df_seg = df_base[df_base["segmento"] == segmento].copy()
            df_seg = filtrar_por_tipo(df_seg, tipo_empresa)

            if df_seg.empty:
                continue

            score = calcular_score(df_seg)
            lideres = determinar_lideres(score)

            if lideres.empty:
                continue

            n_empresas = len(df_seg)

            if n_empresas <= 4:
                patrimonio = gerir_carteira(lideres)
                modo = "Padrão"
            else:
                patrimonio = gerir_carteira_modulada(
                    lideres,
                    policy={"mode": "heuristica_calibrada", "eps": 0.35},
                )
                modo = "Calibrado"

            if patrimonio.empty:
                continue

            patrimonio_final = patrimonio["Patrimônio"].iloc[-1]
            patrimonio_inicial = patrimonio["Patrimônio"].iloc[0]

            retorno_estrategia = patrimonio_final / patrimonio_inicial - 1

            benchmark_final = patrimonio["Benchmark"].iloc[-1]
            benchmark_inicial = patrimonio["Benchmark"].iloc[0]

            retorno_benchmark = benchmark_final / benchmark_inicial - 1

            diff = (retorno_estrategia - retorno_benchmark) * 100

            if diff < margem_minima:
                continue

            segmentos_aprovados += 1

            ultimo_ano = score["Ano"].max()
            lider_ultimo_ano = lideres[lideres["Ano"] == ultimo_ano]["Ticker"].iloc[0]

            ultima_linha = patrimonio.iloc[-1].drop(
                labels=["Patrimônio", "Benchmark"], errors="ignore"
            )

            ticker_maior_participacao = ultima_linha.sort_values(ascending=False).index[0]

            if lider_ultimo_ano == ticker_maior_participacao:
                tickers_escolhidos = [lider_ultimo_ano]
            else:
                tickers_escolhidos = list({lider_ultimo_ano, ticker_maior_participacao})

            valores_sel = [ultima_linha[t] for t in tickers_escolhidos if t in ultima_linha]
            total_sel = float(np.nansum(valores_sel)) if valores_sel else 0.0

            for t in tickers_escolhidos:
                peso = (ultima_linha[t] / total_sel) if total_sel > 0 else 0
                carteira_final.append(
                    {
                        "Segmento": segmento,
                        "Ticker": t,
                        "Modo": modo,
                        "Peso Proporcional": peso,
                    }
                )

        if segmentos_aprovados == 0:
            st.warning(
                "Nenhum segmento atingiu a margem mínima definida vs Tesouro Selic."
            )
            return

        df_final = pd.DataFrame(carteira_final)
        df_final = df_final.sort_values(by="Peso Proporcional", ascending=False)

        st.success(f"{segmentos_aprovados} segmentos aprovados.")
        st.dataframe(df_final, use_container_width=True)
