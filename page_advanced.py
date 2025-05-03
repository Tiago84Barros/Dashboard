"""page_advanced.py
~~~~~~~~~~~~~~~~
Módulo da página Avançada: seleção de setor, filtro, cálculo de métricas, scoring, carteira e gráficos.
"""

from __future__ import annotations
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import matplotlib.pyplot as plt

from db_loader import (
    load_setores_from_db,
    load_data_from_db,
    load_multiplos_from_db,
    load_multiplos_limitado_from_db,
    load_macro_summary,
)
from helpers import (
    obter_setor_da_empresa,
    determinar_lideres,
    formatar_real,
)
from scoring import calcular_score_acumulado, penalizar_plato

# -------------------------------------------------------------
# Core da página Avançada
# -------------------------------------------------------------
def render():
    pagina = st.session_state.get("pagina", "Avançada")
    if pagina != "Avançada":
        return

    # Cabeçalho
    st.markdown(
        """
        <h1 style='text-align: center; font-size: 36px; color: #333;'>Análise Avançada de Ações</h1>
        """,
        unsafe_allow_html=True,
    )

    # Carrega setores
    setores = st.session_state.get("setores_df")
    if setores is None or setores.empty:
        st.error("Nenhum setor disponível.")
        return

    # Filtros de setor, subsetor, segmento, tipo
    setor_sel = st.selectbox("Selecione o Setor:", sorted(setores['SETOR'].dropna().unique()))
    if not setor_sel:
        return
    subset_sel = st.selectbox(
        "Selecione o Subsetor:",
        sorted(setores[setores['SETOR'] == setor_sel]['SUBSETOR'].dropna().unique())
    )
    if not subset_sel:
        return
    segmento_sel = st.selectbox(
        "Selecione o Segmento:",
        sorted(
            setores[
                (setores['SETOR']==setor_sel)&
                (setores['SUBSETOR']==subset_sel)
            ]['SEGMENTO'].dropna().unique()
        )
    )
    if not segmento_sel:
        return

    # Empresas após filtro
    emp_filtradas = setores[
        (setores['SETOR']==setor_sel)&
        (setores['SUBSETOR']==subset_sel)&
        (setores['SEGMENTO']==segmento_sel)
    ]
    if emp_filtradas.empty:
        st.warning("Não há empresas nesse segmento.")
        return

    tipo = st.selectbox("Tipo de Empresa:", ["Todas","Crescimento (<10 anos)","Estabelecida (>=10 anos)"])
    lista_empresas = []
    for _,row in emp_filtradas.iterrows():
        t = row['ticker']+".SA"
        df_dre = load_data_from_db(t)
        if df_dre is None or df_dre.empty:
            continue
        anos = pd.to_datetime(df_dre['Data']).dt.year.nunique()
        if tipo=="Todas" or (tipo.startswith("Crescimento") and anos<10) or (tipo.startswith("Estabelecida") and anos>=10):
            lista_empresas.append({
                'ticker':row['ticker'],
                'multiplos': load_multiplos_from_db(t),
                'df_dre': df_dre,
            })
    if not lista_empresas:
        st.warning("Nenhuma empresa atende ao filtro.")
        return

    # Preparação de dados para scoring
    setores_emp = {e['ticker']: segmento_sel for e in lista_empresas}
    dados_macro = load_macro_summary()
    pesos_utilizados = {}  # carregue seus pesos por setor aqui

    # Preços e momentum
    tickers_sa = [e['ticker']+".SA" for e in lista_empresas]
    precos = load_multiplos_from_db  # ou seu método de download de preços aqui
    # substitua pela função real baixar_precos
    # aqui apenas ilustrativo:
    # precos = baixar_precos(tickers_sa)
    # precos_mensal = precos.resample('M').last()
    # momentum12m_df = calc_momentum_12m(precos)

    # Score e penalização
    df_scores = calcular_score_acumulado(
        lista_empresas,
        setores_emp,
        pesos_utilizados,
        dados_macro,
        None,
        anos_minimos=4
    )
    df_scores = penalizar_plato(df_scores, precos_mensal, meses=18, penal=0.25)

    # Determine líderes e continue com a lógica de gráficos e carteiras...
    lideres = determinar_lideres(df_scores)
    # … restante do render() …
