"""
page_advanced.py
~~~~~~~~~~~~~~~~
Módulo da página “Avançada” no Streamlit.
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
# Helpers
from helpers import (
    obter_setor_da_empresa,
    determinar_lideres,
    formatar_real,
    get_logo_url,
)
# Scoring
from scoring import calcular_score_acumulado, penalizar_plato
# Pesos definidos por setor e genéricos
from weights import PESOS_POR_SETOR as pesos_por_setor, INDICADORES_SCORE as indicadores_score
# Funções de dados de mercado
from yf_data import baixar_precos, coletar_dividendos
# Portfolio management
from portfolio import (
    gerir_carteira,
    gerir_carteira_todas_empresas,
    calcular_patrimonio_selic_macro,
)

# -------------------------------------------------------------
# Função de momentum (12 meses)
# -------------------------------------------------------------
def calc_momentum_12m(precos: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna retorno acumulado de 12 meses (~252 pregões) para cada ticker.
    """
    mom = precos / precos.shift(252) - 1
    mom = mom.dropna(how="all")
    mom.columns = [f"Momentum_{c}" for c in mom.columns]
    return mom

# -------------------------------------------------------------
# Render da página Avançada
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

    # Carrega setores
    setores = st.session_state.get("setores_df")
    if setores is None or setores.empty:
        st.error("Erro ao carregar setores do banco de dados.")
        return

    # Seleção de Setor, Subsetor e Segmento
    setor = st.selectbox("Selecione o Setor:", sorted(setores['SETOR'].dropna().unique()))
    subsetor = st.selectbox(
        "Selecione o Subsetor:",
        sorted(setores[setores['SETOR']==setor]['SUBSETOR'].dropna().unique())
    )
    segmento = st.selectbox(
        "Selecione o Segmento:",
        sorted(
            setores[
                (setores['SETOR']==setor)&
                (setores['SUBSETOR']==subsetor)
            ]['SEGMENTO'].dropna().unique()
        )
    )

    # Filtra empresas
    df_emp = setores[
        (setores['SETOR']==setor)&
        (setores['SUBSETOR']==subsetor)&
        (setores['SEGMENTO']==segmento)
    ]
    if df_emp.empty:
        st.warning("Não há empresas nesse segmento.")
        return

    # Filtro de tipo de empresa
    tipo = st.selectbox("Tipo de Empresa:", ["Todas","Crescimento (<10 anos)","Estabelecida (>=10 anos)"])
    lista_empresas: list[dict] = []
    for _, row in df_emp.iterrows():
        tk = row['ticker']
        ticker_sa = f"{tk}.SA"
        df_dre = load_data_from_db(ticker_sa)
        if df_dre is None or df_dre.empty:
            continue
        anos_dispo = pd.to_datetime(df_dre['Data'], errors='coerce').dt.year.nunique()
        if (
            tipo == "Todas"
            or (tipo.startswith("Crescimento") and anos_dispo < 10)
            or (tipo.startswith("Estabelecida") and anos_dispo >= 10)
        ):
            lista_empresas.append({
                'ticker': tk,
                'multiplos': load_multiplos_from_db(ticker_sa),
                'df_dre': df_dre,
            })
    if not lista_empresas:
        st.warning("Nenhuma empresa atende aos critérios do filtro.")
        return

    # Macro e pesos
    dados_macro = load_macro_summary()
    setor_empresa = obter_setor_da_empresa(lista_empresas[0]['ticker'], df_emp)
    pesos_util = pesos_por_setor.get(setor_empresa, indicadores_score)

    # Baixa preços e calcula momentum
    tickers_sa = [f"{e['ticker']}.SA" for e in lista_empresas]
    precos = baixar_precos(tickers_sa)
    precos_mensal = precos.resample('M').last()
    momentum12m_df = calc_momentum_12m(precos)

    # Calcula scores e penaliza platô
    df_scores = calcular_score_acumulado(
        lista_empresas,
        {e['ticker']: setor for e in lista_empresas},
        pesos_util,
        dados_macro,
        momentum12m_df,
        anos_minimos=4
    )
    df_scores = penalizar_plato(df_scores, precos_mensal, meses=18, penal=0.25)

    # Determina líderes anuais
    lideres_por_ano = determinar_lideres(df_scores)

    # Gráfico de evolução de patrimônio
    dividendos = coletar_dividendos([e['ticker'] for e in lista_empresas])
    patr_estrat, datas_aportes = gerir_carteira(precos, df_scores, lideres_por_ano, dividendos)
    patr_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aportes)
    patr_emp = gerir_carteira_todas_empresas(precos, [e['ticker'] for e in lista_empresas], datas_aportes, dividendos)

    patrimonio_final = pd.concat([patr_estrat, patr_emp, patr_selic], axis=1)
    st.markdown("### Evolução do Patrimônio Acumulado")
    fig, ax = plt.subplots(figsize=(12,6))
    for col in patrimonio_final.columns:
        patrimonio_final[col].plot(ax=ax, label=col)
    ax.set_xlabel("Data"); ax.set_ylabel("Patrimônio (R$)"); ax.legend()
    st.pyplot(fig)

    # Blocos de patrimônio final
    df_pf = pd.concat([
        patr_estrat.iloc[-1:].melt(value_name='Valor', var_name='Ticker'),
        patr_emp.iloc[-1:].melt(value_name='Valor', var_name='Ticker'),
        patr_selic.iloc[-1:].melt(value_name='Valor', var_name='Ticker'),
    ], ignore_index=True)
    df_pf = df_pf.groupby('Ticker')['Valor'].last().reset_index().sort_values('Valor', ascending=False)

    cols = st.columns(3)
    cont_lider = lideres_por_ano['ticker'].value_counts().to_dict()
    for i, row in df_pf.iterrows():
        ticker = row['Ticker']
        val = row['Valor']
        icone = (
            "https://cdn-icons-png.flaticon.com/512/1019/1019709.png"
            if ticker==patr_estrat.columns[-1]
            else ("https://cdn-icons-png.flaticon.com/512/2331/2331949.png" if ticker=='Tesouro Selic' else get_logo_url(ticker))
        )
        border = (
            "#DAA520" if ticker==patr_estrat.columns[-1]
            else ("#007bff" if ticker=='Tesouro Selic' else "#d3d3d3")
        )
        lider_text = f"🏆 {cont_lider.get(ticker,0)}x Líder" if cont_lider.get(ticker,0)>0 else ""
        col = cols[i%3]
        with col:
            st.markdown(f"""
                <div style='background:#fff;border:3px solid {border};border-radius:10px;padding:15px;text-align:center;'>
                    <img src='{icone}' width='50'><h3>{ticker}</h3>
                    <p style='font-weight:bold;color:#2ecc71;'>{formatar_real(val)}</p>
                    <p style='color:#FFA500;'>{lider_text}</p>
                </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # Gráfico comparativo de múltiplos
    st.markdown("### Comparação de Múltiplos por Empresa")
    nomes_map = {col:col.replace('_',' ').title() for col in df_emp.index if False}
    df_hist = []
    indicadores_disp = ['Margem_Liquida','Margem_Operacional','ROE','ROIC','DY','P/VP','P/L','Endividamento_Total','Alavancagem_Financeira','Liquidez_Corrente']
    default = ['Margem_Liquida','ROE']
    sel_ind_disp = st.multiselect("Indicadores:", indicadores_disp, default=default)
    if sel_ind_disp:
        for _,r in df_emp.iterrows():
            tk = r['ticker']
            dfm = load_multiplos_from_db(f"{tk}.SA")
            if dfm is not None:
                dfm['Ano']=pd.to_datetime(dfm['Data']).dt.year
                for ind in sel_ind_disp:
                    if ind in dfm:
                        tmp = dfm[['Ano',ind]].copy(); tmp['Empresa']=r['nome_empresa']
                        df_hist.append(tmp.rename(columns={ind:'Valor'}))
        if df_hist:
            df_plot = pd.concat(df_hist)
            fig2 = px.bar(df_plot, x='Ano', y='Valor', color='Empresa', barmode='group', title='Histórico de Múltiplos')
            st.plotly_chart(fig2, use_container_width=True)

    # Gráfico comparativo de demonstrações
    st.markdown("### Comparação de Demonstrações Financeiras")
    dre_all=[]
    for _,r in df_emp.iterrows():
        tk=r['ticker']; dfdr=load_data_from_db(f"{tk}.SA")
        if dfdr is not None:
            dfdr['Ano']=pd.to_datetime(dfdr['Data']).dt.year; dfdr['Empresa']=r['nome_empresa']
            dre_all.append(dfdr)
    if dre_all:
        df_drc = pd.concat(dre_all)
        cols_dre = ['Receita_Liquida','EBIT','Lucro_Liquido','Patrimonio_Liquido','Divida_Liquida','Caixa_Liquido']
        sel_dre = st.selectbox("Indicador DRE:",[c for c in cols_dre if c in df_drc.columns])
        tmp = df_drc[['Ano',sel_dre,'Empresa']].rename(columns={sel_dre:'Valor'})
        fig3 = px.bar(tmp, x='Ano', y='Valor', color='Empresa', barmode='group', title=f"Comparação de {sel_dre}")
        st.plotly_chart(fig3, use_container_width=True)
