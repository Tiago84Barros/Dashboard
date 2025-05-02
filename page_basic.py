"""page_basic.py
~~~~~~~~~~~~~~~~~
Página “Básica” isolada em um módulo.

Uso:
-----
import page_basic as pb
pb.render()
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import yfinance as yf

from db_loader import (
    load_data_from_db,
    load_multiplos_from_db,
    load_multiplos_limitado_from_db,
)
from yf_data import get_company_info
from helpers import get_logo_url  # import corrigido para helpers

# ---------------------------------------------------------------------------
# Helper interno para mapear colunas → nomes amigáveis ----------------------
# ---------------------------------------------------------------------------

def _create_map(
    df: pd.DataFrame,
    exclude: list[str],
    custom: dict[str, str],
) -> tuple[dict[str, str], dict[str, str], list[str]]:
    """
    Gera:
      - col_map: {col: nome amigável}
      - rev_map: {nome amigável: col}
      - disp: lista de nomes amigáveis
    """
    col_map: dict[str, str] = {}
    for col in df.columns:
        if col in exclude:
            continue
        friendly = custom.get(col, col.replace('_', ' ').title())
        col_map[col] = friendly
    rev_map = {v: k for k, v in col_map.items()}
    disp = list(col_map.values())
    return col_map, rev_map, disp

# ---------------------------------------------------------------------------
# Função principal -----------------------------------------------------------
# ---------------------------------------------------------------------------

def render():
    """Renderiza a aba “Básica” no Streamlit."""
    # Só desenha se for a página correta
    if st.session_state.get('pagina') != 'Básica':
        return

    # Header
    st.markdown(
        """
        <h1 style='text-align:center;font-size:36px;color:#333;'>
          Análise Básica de Ações
        </h1>
        """,
        unsafe_allow_html=True,
    )

    # Botão Atualizar (canto superior direito)
    st.markdown(
        """
        <style>
        .button-container {
          display:flex;justify-content:flex-end;
          position:absolute;top:10px;right:10px;z-index:1;
        }
        .button-container button{
          background:#4CAF50;color:#fff;
          padding:10px 20px;border:none;border-radius:4px;cursor:pointer;
        }
        .button-container button:hover{background:#45a049;}
        </style>
        <div class="button-container">
          <form action="#"><button type="submit">Atualizar dados</button></form>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button('Atualizar dados'):
        st.cache_data.clear()
        st.experimental_rerun()

    # Estilo dos cards de setores
    st.markdown(
        """
        <style>
        .sector-box{border:1px solid #ddd;padding:15px;border-radius:10px;margin-bottom:10px;display:flex;justify-content:space-between;align-items:center;height:140px;cursor:pointer;transition:background .3s;}
        .sector-box:hover{background:#f0f0f0;}
        .sector-info{font-size:14px;color:#333;flex:1;overflow:hidden;text-overflow:ellipsis;}
        .sector-info strong{font-size:16px;color:#000;}
        .sector-logo{width:50px;margin-left:15px;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Input de ticker
    setores = st.session_state.get('setores_df')
    col1, _ = st.columns([4, 1])
    with col1:
        if 'ticker' in st.session_state:
            default = st.session_state.ticker.split('.SA')[0]
            ticker_in = st.text_input('DIGITE O TICKER:', value=default, key='ticker_input')
        else:
            ticker_in = st.text_input('Digite o ticker:', key='ticker_input')
        ticker_in = ticker_in.upper().strip()
        if not ticker_in:
            st.session_state.pop('ticker', None)
            ticker = None
        else:
            ticker = f"{ticker_in}.SA"
            st.session_state.ticker = ticker

    # Se não tem ticker, mostra lista de setores e retorna
    if not ticker:
        st.markdown('### Selecione um Ticker')
        if setores is not None and not setores.empty:
            for setor, dados in setores.groupby('SETOR'):
                st.markdown(f'#### {setor}')
                cols = st.columns(3)
                for i, row in dados.iterrows():
                    logo = get_logo_url(row['ticker'])
                    with cols[i % 3]:
                        if st.button(row['nome_empresa'], key=row['ticker']):
                            st.session_state.ticker = row['ticker']
                        st.markdown(
                            f"""
                            <div class='sector-box'>
                              <div class='sector-info'>
                                <strong>{row['nome_empresa']}</strong><br>
                                Ticker: {row['ticker']}<br>
                                Subsetor: {row['SUBSETOR']}<br>
                                Segmento: {row['SEGMENTO']}
                              </div>
                              <img src='{logo}' class='sector-logo'>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )
        else:
            st.warning('Nenhuma informação de setores encontrada.')
        return

    # Carrega indicadores financeiros
    indicadores = load_data_from_db(ticker)
    if indicadores is None or indicadores.empty:
        st.error('Dados financeiros não encontrados para o ticker.')
        return
    indicadores = indicadores.drop(columns=['Ticker'], errors='ignore')

    # Função interna de crescimento via regressão log
    def calculate_growth_rate(df: pd.DataFrame, column: str) -> float:
        try:
            df2 = df.copy()
            df2['Data'] = pd.to_datetime(df2['Data'], errors='coerce')
            df2 = df2.sort_values('Data')
            mask = df2[column].notnull() & (df2[column] > 0)
            dfv = df2.loc[mask]
            if dfv.shape[0] < 2:
                return np.nan
            X = (dfv['Data'] - dfv['Data'].iloc[0]).dt.days / 365.25
            y_log = np.log(dfv[column])
            slope, _ = np.polyfit(X, y_log, deg=1)
            return np.exp(slope) - 1
        except Exception:
            return np.nan

    # Calcula taxas de crescimento
    growth_rates = {col: calculate_growth_rate(indicadores, col)
                    for col in indicadores.columns if col != 'Data'}

    # Info da empresa e preço
    name, site = get_company_info(ticker)
    price_df = yf.Ticker(ticker).history(period='1d')
    current_price = price_df['Close'].iloc[0] if not price_df.empty else np.nan

    if name:
        cA, cB = st.columns([4,1])
        with cA:
            st.subheader(f"{name} — Preço Atual: R$ {current_price:,.2f}")
        with cB:
            st.image(get_logo_url(ticker), width=80)
    else:
        st.error('Empresa não encontrada.')

    # Growth boxes
    st.markdown(
        """
        <style>
        .growth-box{border:2px solid #ddd;padding:20px;border-radius:10px;margin-bottom:10px;display:flex;justify-content:center;align-items:center;height:100px;font-size:20px;font-weight:bold;background:#f9f9f9;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    def fmt(x): return f"{x:.2%}" if pd.notna(x) else '-'
    st.markdown('### Taxa de Crescimento Médio Anual')
    b1, b2, b3 = st.columns(3)
    with b1:
        st.markdown(f"<div class='growth-box'>Receita Líquida: {fmt(growth_rates.get('Receita_Liquida', np.nan))}</div>", unsafe_allow_html=True)
    with b2:
        st.markdown(f"<div class='growth-box'>Lucro Líquido: {fmt(growth_rates.get('Lucro_Liquido', np.nan))}</div>", unsafe_allow_html=True)
    with b3:
        st.markdown(f"<div class='growth-box'>Patrimônio Líquido: {fmt(growth_rates.get('Patrimonio_Liquido', np.nan))}</div>", unsafe_allow_html=True)

    st.divider()

    # Gráfico de DFPs
    col_map = {c: c.replace('_',' ').title() for c in indicadores.columns if c != 'Data'}
    corrections = {'Receita Liquida':'Receita Líquida','Lucro Liquido':'Lucro Líquido','Patrimonio Liquido':'Patrimônio Líquido','Caixa Liquido':'Caixa Líquido','Divida Liquida':'Dívida Líquida'}
    col_map = {k: corrections.get(v,v) for k,v in col_map.items()}
    rev_map = {v:k for k,v in col_map.items()}
    defaults = ['Receita Líquida','Lucro Líquido','Dívida Líquida']
    sel = st.multiselect('Escolha os Indicadores:', list(col_map.values()), default=[d for d in defaults if d in col_map.values()])
    if sel:
        cols = [rev_map[name] for name in sel]
        dfm = indicadores.melt(id_vars=['Data'],value_vars=cols,var_name='Indicador',value_name='Valor')
        dfm['Indicador'] = dfm['Indicador'].map(col_map)
        fig = px.bar(dfm,x='Data',y='Valor',color='Indicador',barmode='group',title='Evolução dos Balanços Selecionados')
        st.plotly_chart(fig,use_container_width=True)

    st.divider()

    # Múltiplos atuais
    m_atual = load_multiplos_limitado_from_db(ticker)
    if m_atual is not None and not m_atual.empty:
        m0 = m_atual.iloc[0]
        st.markdown("""
            <style>.metric-box{background:#fff;padding:20px;margin:10px;border-radius:10px;box-shadow:2px 2px 5px rgba(0,0,0,.1);text-align:center;}</style>
        """,unsafe_allow_html=True)
        c1,c2,c3,c4 = st.columns(4)
        with c1:
            st.markdown(f"<div class='metric-box'>{m0['Margem_Liquida']:.2f}%<br>Margem Líquida</div>",unsafe_allow_html=True)
        with c2:
            st.markdown(f"<div class='metric-box'>{m0['Margem_Operacional']:.2f}%<br>Margem Operacional</div>",unsafe_allow_html=True)
        with c3:
            st.markdown(f"<div class='metric-box'>{m0['ROE']:.2f}%<br>ROE</div>",unsafe_allow_html=True)
        with c4:
            st.markdown(f"<div class='metric-box'>{m0['ROIC']:.2f}%<br>ROIC</div>",unsafe_allow_html=True)

    # Histórico dos múltiplos
    m_hist = load_multiplos_from_db(ticker)
    if m_hist is not None and not m_hist.empty:
        m_hist['Data'] = pd.to_datetime(m_hist['Data'],errors='coerce')
        st.markdown('### Evolução Histórica dos Múltiplos')
        exclude = ['Data','Ticker','N Acoes']
        custom = {'DY':'Dividend Yield','P_L':'P/L','P_VP':'P/VP'}
        col_map_h, rev_map_h, disp_h = _create_map(m_hist, exclude, custom)
        default_h = [col_map_h[c] for c in ['Margem_Liquida','Margem_Operacional'] if c in col_map_h]
        sel_h = st.multiselect('Indicadores:', disp_h, default=default_h)
        if sel_h:
            cols_h = [rev_map_h[name] for name in sel_h]
            dfh = m_hist.melt(id_vars=['Data'],value_vars=cols_h,var_name='Indicador',value_name='Valor')
            dfh['Indicador'] = dfh['Indicador'].map(col_map_h)
            figh = px.bar(dfh,x='Data',y='Valor',color='Indicador',barmode='group',title='Histórico de Múltiplos')
            st.plotly_chart(figh,use_container_width=True)

    st.divider()

# Fim de page_basic.py
