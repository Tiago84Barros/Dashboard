"""page_basic.py
~~~~~~~~~~~~~~~~
Módulo da página Básica com exibição completa de múltiplos.
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
from helpers import get_company_info
from helpers import get_logo_url  # importado de helpers


def render():
    pagina = st.session_state.get("pagina", "Básica")
    if pagina != "Básica":
        return

    # Header e estilo
    st.markdown(
        """
        <h1 style='text-align: center; font-size: 36px; color: #333;'>Análise Básica de Ações</h1>
        """,
        unsafe_allow_html=True,
    )

    # Botão Atualizar
    st.markdown(
        """
        <style>
        .button-container {display:flex;justify-content:flex-end;position:absolute;top:10px;right:10px;z-index:1;}
        .button-container button{background:#4CAF50;color:#fff;padding:10px 20px;border:none;border-radius:4px;cursor:pointer;}
        .button-container button:hover{background:#45a049;}
        </style>
        <div class="button-container"><form action="#"><button type="submit">Atualizar dados</button></form></div>
        """,
        unsafe_allow_html=True,
    )
    if st.button("Atualizar dados"):
        st.cache_data.clear()
        st.experimental_rerun()

    # Seções iniciais: ticker ou setores
    setores = st.session_state.get("setores_df")
    col1, _ = st.columns([4, 1])
    with col1:
        if "ticker" in st.session_state:
            ticker_input = st.text_input(
                "DIGITE O TICKER:",
                value=st.session_state.ticker.split(".SA")[0],
                key="ticker_input",
            ).upper()
        else:
            ticker_input = st.text_input("Digite o ticker:", key="ticker_input").upper()
        if ticker_input == "":
            st.session_state.pop("ticker", None)
            ticker = None
        else:
            ticker = ticker_input + ".SA"
            st.session_state.ticker = ticker

    if not ticker:
        st.markdown("### Selecione um Ticker")
        if setores is not None and not setores.empty:
            for setor, dados in setores.groupby("SETOR"):
                st.markdown(f"#### {setor}")
                cols = st.columns(3)
                for i, row in dados.iterrows():
                    logo_url = get_logo_url(row["ticker"])
                    with cols[i % 3]:
                        if st.button(row["nome_empresa"], key=row["ticker"]):
                            st.session_state.ticker = row["ticker"]
                        st.markdown(
                            f"""
                            <div class='sector-box'>
                                <div class='sector-info'>
                                    <strong>{row['nome_empresa']}</strong><br>
                                    Ticker: {row['ticker']}<br>
                                    Subsetor: {row['SUBSETOR']}<br>
                                    Segmento: {row['SEGMENTO']}
                                </div>
                                <img src='{logo_url}' class='sector-logo'>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )
        else:
            st.warning("Nenhuma informação de setores encontrada.")
        return

    # Carregar dados
    indicadores = load_data_from_db(ticker)
    if indicadores is None or indicadores.empty:
        st.error("Dados financeiros não encontrados para o ticker.")
        return
    indicadores = indicadores.drop(columns=["Ticker"], errors='ignore')

    # Crescimento por regressão log
    def calculate_growth_rate(df: pd.DataFrame, col: str):
        try:
            df = df.copy()
            df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
            df = df.sort_values('Data')
            mask = df[col].notna() & (df[col] > 0)
            df_valid = df.loc[mask]
            if df_valid.shape[0] < 2:
                return np.nan
            X = (df_valid['Data'] - df_valid['Data'].iloc[0]).dt.days / 365.25
            y_log = np.log(df_valid[col])
            slope, _ = np.polyfit(X, y_log, 1)
            return np.exp(slope) - 1
        except:
            return np.nan

    growth_rates = {c: calculate_growth_rate(indicadores, c) if c!='Data' else np.nan for c in indicadores.columns}

    # Info da empresa
    name, site = get_company_info(ticker)
    price_df = yf.Ticker(ticker).history(period="1d")
    current_price = price_df['Close'].iloc[0] if not price_df.empty else np.nan
    if name:
        cA, cB = st.columns([4,1])
        with cA: st.subheader(f"{name} — Preço Atual: R$ {current_price:,.2f}")
        with cB: st.image(get_logo_url(ticker), width=80)
    else:
        st.error("Empresa não encontrada.")

    # Crescimento visual
    st.markdown("""
        <style>.growth-box{border:2px solid #ddd;padding:20px;border-radius:10px;margin-bottom:10px;display:flex;justify-content:center;align-items:center;height:100px;font-size:20px;font-weight:bold;background:#f9f9f9;}</style>
    """, unsafe_allow_html=True)
    def fmt(x): return f"{x:.2%}" if pd.notna(x) else "-"
    st.markdown("### Taxa de Crescimento Médio Anual")
    c1,c2,c3 = st.columns(3)
    with c1: st.markdown(f"<div class='growth-box'>Receita Líquida: {fmt(growth_rates['Receita_Liquida'])}</div>", unsafe_allow_html=True)
    with c2: st.markdown(f"<div class='growth-box'>Lucro Líquido: {fmt(growth_rates['Lucro_Liquido'])}</div>", unsafe_allow_html=True)
    with c3: st.markdown(f"<div class='growth-box'>Patrimônio Líquido: {fmt(growth_rates['Patrimonio_Liquido'])}</div>", unsafe_allow_html=True)

    st.divider()

    # Múltiplos atuais completos
    m_atual = load_multiplos_limitado_from_db(ticker)
    if m_atual is not None and not m_atual.empty:
        m0 = m_atual.iloc[0]
        st.markdown("""
            <style>.metric-box{background:#fff;padding:20px;margin:10px;border-radius:10px;box-shadow:2px 2px 5px rgba(0,0,0,.1);text-align:center;}</style>
        """,unsafe_allow_html=True)
        cols = st.columns(4)
        # linha 1
        with cols[0]: st.markdown(f"<div class='metric-box'>{m0['Margem_Liquida']:.2f}%<br>Margem Líquida</div>",unsafe_allow_html=True)
        with cols[1]: st.markdown(f"<div class='metric-box'>{m0['Margem_Operacional']:.2f}%<br>Margem Operacional</div>",unsafe_allow_html=True)
        with cols[2]: st.markdown(f"<div class='metric-box'>{m0['ROE']:.2f}%<br>ROE</div>",unsafe_allow_html=True)
        with cols[3]: st.markdown(f"<div class='metric-box'>{m0['ROIC']:.2f}%<br>ROIC</div>",unsafe_allow_html=True)
        # linha 2
        cols2 = st.columns(4)
        dy = m0.get('DY',0)
        dy_pct = '-' if pd.isna(dy) or current_price==0 else f"{100*(dy/current_price):.2f}%"
        with cols2[0]: st.markdown(f"<div class='metric-box'>{dy_pct}<br>Dividend Yield</div>",unsafe_allow_html=True)
        pvp = m0.get('P/VP', np.nan)
        pvp_fmt = '-' if pd.isna(pvp) or pvp==0 else f"{current_price/pvp:.2f}"
        with cols2[1]: st.markdown(f"<div class='metric-box'>{pvp_fmt}<br>P/VP</div>",unsafe_allow_html=True)
        payout = m0.get('Payout', np.nan)
        payout_fmt = '-' if pd.isna(payout) else f"{payout*100:.2f}%"
        with cols2[2]: st.markdown(f"<div class='metric-box'>{payout_fmt}<br>Payout</div>",unsafe_allow_html=True)
        pl = m0.get('P/L', np.nan)
        pl_fmt = '-' if pd.isna(pl) or pl==0 else f"{current_price/pl:.2f}"
        with cols2[3]: st.markdown(f"<div class='metric-box'>{pl_fmt}<br>P/L</div>",unsafe_allow_html=True)
        # linha 3
        cols3 = st.columns(4)
        with cols3[0]: st.markdown(f"<div class='metric-box'>{m0['Endividamento_Total']:.2f}<br>Endividamento Total</div>",unsafe_allow_html=True)
        with cols3[1]: st.markdown(f"<div class='metric-box'>{m0['Alavancagem_Financeira']:.2f}<br>Alavancagem Financeira</div>",unsafe_allow_html=True)
        with cols3[2]: st.markdown(f"<div class='metric-box'>{m0['Liquidez_Corrente']:.2f}<br>Liquidez Corrente</div>",unsafe_allow_html=True)
        # se tiver mais colunas, adicionar aqui
    st.divider()

    # Gráfico histórico de múltiplos
    mult_hist = load_multiplos_from_db(ticker)
    if mult_hist is not None and not mult_hist.empty:
        mult_hist['Data'] = pd.to_datetime(mult_hist['Data'], errors='coerce')
        st.markdown("### Evolução Histórica dos Múltiplos")
        exclude = ['Data','Ticker','N Acoes']
        custom = {'DY':'Dividend Yield','P/L':'P/L','P/VP':'P/VP'}
        # helper inline para mapeamento
            def create_map(df, exclude, custom_map):
        """
        Cria mapeamento de nomes de colunas para exibição amigável e inverso.
        """
        cm, dm = {}, {}
        for c in df.columns:
            if c in exclude:
                continue
            friendly = custom_map.get(c, c.replace('_', ' ').title())
            cm[c] = friendly
            dm[friendly] = c
        display_names = list(cm.values())
        return cm, dm, display_names
