"""
page_advanced.py
~~~~~~~~~~~~~~~~
Módulo da página "Avançada" no Streamlit.
"""
from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import matplotlib.pyplot as plt

# Carregadores de dados
from db_loader import (
    load_setores_from_db,
    load_data_from_db,
    load_multiplos_from_db,
    load_multiplos_limitado_from_db,
    load_macro_summary,
)
# Funções utilitárias
from helpers import (
    obter_setor_da_empresa,
    determinar_lideres,
    formatar_real,
    get_logo_url,
)
# Scoring
from scoring import calcular_score_acumulado, penalizar_plato
# Pesos definidos por setor
from weights import PESOS_POR_SETOR as pesos_por_setor, INDICADORES_SCORE as indicadores_score
# Funções de yfinance encapsuladas
from yf_data import baixar_precos, coletar_dividendos
# Portfolio management
from portfolio import (
    gerir_carteira,
    gerir_carteira_todas_empresas,
    calcular_patrimonio_selic_macro,
)

# -------------------------------------------------------------
# Cálculo de momentum a partir de preços ajustados
# -------------------------------------------------------------
def calc_momentum_12m(precos: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna retorno acumulado dos últimos 12 meses (~252 pregões).
    """
    mom = precos / precos.shift(252) - 1
    mom = mom.dropna(how="all")
    mom.columns = [f"Momentum_{c}" for c in mom.columns]
    return mom

# -------------------------------------------------------------
# Renderização da página
# -------------------------------------------------------------
def render():
    pagina = st.session_state.get("pagina", "Avançada")
    if pagina != "Avançada":
        return

    # Cabeçalho
    st.markdown(
        """
        <h1 style='text-align:center; font-size:36px; color:#333;'>Análise Avançada de Ações</h1>
        """,
        unsafe_allow_html=True,
    )

    # Carregar setores
    setores = st.session_state.get("setores_df")
    if setores is None or setores.empty:
        st.error("Erro ao carregar setores do banco de dados.")
        return

    # Filtros: Setor, Subsetor, Segmento
    setor_sel = st.selectbox("Selecione o Setor:", sorted(setores['SETOR'].dropna().unique()))
    subset_sel = st.selectbox(
        "Selecione o Subsetor:",
        sorted(setores[setores['SETOR']==setor_sel]['SUBSETOR'].dropna().unique())
    )
    segmento_sel = st.selectbox(
        "Selecione o Segmento:",
        sorted(
            setores[
                (setores['SETOR']==setor_sel)&
                (setores['SUBSETOR']==subset_sel)
            ]['SEGMENTO'].dropna().unique()
        )
    )

    # Filtrar empresas
    df_emp = setores[
        (setores['SETOR']==setor_sel)&
        (setores['SUBSETOR']==subset_sel)&
        (setores['SEGMENTO']==segmento_sel)
    ]
    if df_emp.empty:
        st.warning("Não há empresas nesse segmento.")
        return

    # Quarto filtro: tipo de empresa
    tipo = st.selectbox("Tipo de Empresa:", ["Todas","Crescimento (<10 anos)","Estabelecida (>=10 anos)"])
    lista_empresas: list[dict] = []
    for _, row in df_emp.iterrows():
        tk = row['ticker']
        ticker_sa = f"{tk}.SA"
        df_dre = load_data_from_db(ticker_sa)
        if df_dre is None or df_dre.empty:
            continue
        anos_disp = pd.to_datetime(df_dre['Data'], errors='coerce').dt.year.nunique()
        if (
            tipo == "Todas" or
            (tipo.startswith("Crescimento") and anos_disp < 10) or
            (tipo.startswith("Estabelecida") and anos_disp >= 10)
        ):
            lista_empresas.append({
                'ticker': tk,
                'multiplos': load_multiplos_from_db(ticker_sa),
                'df_dre': df_dre,
            })
    if not lista_empresas:
        st.warning("Nenhuma empresa atende aos critérios do filtro.")
        return

    # Preparar dados macro e pesos
    dados_macro = load_macro_summary()
    setor_empresa = obter_setor_da_empresa(lista_empresas[0]['ticker'], df_emp)
    pesos_util = pesos_por_setor.get(setor_empresa, indicadores_score)

    # Baixar preços e calcular momentum
    tickers_sa = [f"{e['ticker']}.SA" for e in lista_empresas]
    precos = baixar_precos(tickers_sa)
    precos_mensal = precos.resample('M').last()
    momentum12m_df = calc_momentum_12m(precos)

    # Calcular Scores
    df_scores = calcular_score_acumulado(
        lista_empresas,
        {e['ticker']: setor_sel for e in lista_empresas},
        pesos_util,
        dados_macro,
        momentum12m_df,
        anos_minimos=4
    )
    df_scores = penalizar_plato(df_scores, precos_mensal, meses=18, penal=0.25)

    # Determinar líderes anuais
    lideres_por_ano = determinar_lideres(df_scores)

    # Coletar dividendos
    tickers = df_scores['ticker'].unique().tolist()
    dividendos_dict = coletar_dividendos(tickers)

    # Gerenciar carteiras
    patr_estrat, datas_aportes = gerir_carteira(precos, df_scores, lideres_por_ano, dividendos_dict)
    patr_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aportes)
    patr_empresas = gerir_carteira_todas_empresas(precos, tickers, datas_aportes, dividendos_dict)

    # Combinar e plotar evolução do patrimônio
    df_patr_final = pd.concat([patr_estrat, patr_empresas, patr_selic], axis=1)
    st.markdown("### Evolução do Patrimônio")
    fig, ax = plt.subplots(figsize=(12, 6))
    for col in df_patr_final.columns:
        df_patr_final[col].plot(ax=ax, label=col)
    ax.legend(); ax.set_ylabel("Patrimônio (R$)")
    st.pyplot(fig)

    # Exibir Score e líderes
    st.markdown("### Scores Ajustados")
    st.dataframe(df_scores)
    st.markdown("### Líderes por Ano")
    st.table(lideres_por_ano)
