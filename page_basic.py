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
from helpers import get_company_info, get_logo_url


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

    # Seleção de ticker ou setores ______________________________________________________________________________________________________________________________________________-
    setores = st.session_state.get("setores_df")
    col1, _ = st.columns([4, 1])
    with col1:
        if "ticker" in st.session_state:
            default = st.session_state.ticker.split(".SA")[0]
            ticker_input = st.text_input("DIGITE O TICKER:", value=default, key="ticker_input").upper()
        else:
            ticker_input = st.text_input("Digite o ticker:", key="ticker_input").upper()
        if ticker_input:
            ticker = ticker_input + ".SA"
            st.session_state.ticker = ticker
        else:
            ticker = None
            st.session_state.pop("ticker", None)

    if not ticker:
        st.markdown("### Selecione um Ticker")
        if setores is not None and not setores.empty:
            for setor, dados in setores.groupby("SETOR"):
                st.markdown(f"#### {setor}")
                cols = st.columns(3)
                for i, row in dados.iterrows():
                    logo = get_logo_url(row["ticker"])
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
                                <img src='{logo}' class='sector-logo'>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )
        else:
            st.warning("Nenhuma informação de setores encontrada.")
        return

    # Carrega dados financeiros ____________________________________________________________________________________________________________________________________________________
    indicadores = load_data_from_db(ticker)
    if indicadores is None or indicadores.empty:
        st.error("Dados financeiros não encontrados para o ticker.")
        return
    indicadores = indicadores.drop(columns=["Ticker"], errors='ignore')

    # Cálculo de taxas de crescimento _______________________________________________________________________________________________________________________________________________
    def calculate_growth_rate(df: pd.DataFrame, col: str) -> float:
        try:
            df2 = df.copy()
            df2['Data'] = pd.to_datetime(df2['Data'], errors='coerce')
            df2 = df2.sort_values('Data')
            mask = df2[col].notna() & (df2[col] > 0)
            dfv = df2.loc[mask]
            if dfv.shape[0] < 2:
                return np.nan
            X = (dfv['Data'] - dfv['Data'].iloc[0]).dt.days / 365.25
            y = np.log(dfv[col])
            slope, _ = np.polyfit(X, y, 1)
            return float(np.exp(slope) - 1)
        except:
            return np.nan

    growth_rates = {c: calculate_growth_rate(indicadores, c) if c != 'Data' else np.nan for c in indicadores.columns}

    # Inserindo espaçamento entre os elementos
    placeholder = st.empty()
    placeholder.markdown("<div style='height: 46px;'></div>", unsafe_allow_html=True)

    # Informações da empresa ___________________________________________________________________________________________________________________________________________________________
    name, site = get_company_info(ticker)
    hist = yf.Ticker(ticker).history(period="1d")
    price = hist['Close'].iloc[0] if not hist.empty else np.nan
    if name:
        ca, cb = st.columns([4,1])
        with ca:
            st.subheader(f"{name} — Preço Atual: R$ {price:,.2f}")
        with cb:
            st.image(get_logo_url(ticker), width=80)
    else:
        st.error("Empresa não encontrada.")

    # Inserindo espaçamento entre os elementos
    placeholder = st.empty()
    placeholder.markdown("<div style='height: 46px;'></div>", unsafe_allow_html=True)

    # Exibe taxas de crescimento ___________________________________________________________________________________________________________________________________________________________
    st.markdown(
        """
        <style>
        .growth-box{border:2px solid #ddd;padding:20px;border-radius:10px;margin-bottom:10px;display:flex;justify-content:center;align-items:center;height:100px;font-size:20px;font-weight:bold;background:#f9f9f9;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    def fmt(x): return f"{x:.2%}" if pd.notna(x) else "-"
    st.markdown("### Taxa de Crescimento Médio Anual")
    c1, c2, c3 = st.columns(3)
    with c1: st.markdown(f"<div class='growth-box'>Receita Líquida: {fmt(growth_rates.get('Receita_Liquida'))}</div>", unsafe_allow_html=True)
    with c2: st.markdown(f"<div class='growth-box'>Lucro Líquido: {fmt(growth_rates.get('Lucro_Liquido'))}</div>", unsafe_allow_html=True)
    with c3: st.markdown(f"<div class='growth-box'>Patrimônio Líquido: {fmt(growth_rates.get('Patrimonio_Liquido'))}</div>", unsafe_allow_html=True)

    # Inserindo espaçamento entre os elementos
    placeholder = st.empty()
    placeholder.markdown("<div style='height: 46px;'></div>", unsafe_allow_html=True)
    # ---------------------------------------------------------------------
    # Gráfico de Demonstrações Financeiras selecionáveis _____________________________________________________________________________________________________________________________________
    # ---------------------------------------------------------------------
    col_map = {c: c.replace('_', ' ').title() for c in indicadores.columns if c != 'Data'}
    correcoes = {
        'Receita Liquida': 'Receita Líquida',
        'Lucro Liquido': 'Lucro Líquido',
        'Divida Liquida': 'Dívida Líquida',
        'Patrimonio Liquido': 'Patrimônio Líquido',
        'Caixa Liquido': 'Caixa Líquido'
    }
    col_map = {k: correcoes.get(v, v) for k, v in col_map.items()}
    display_to_col = {v: k for k, v in col_map.items()}

    st.markdown("### Selecione os Balanços para Visualizar no Gráfico")
    default_cols = ['Receita Líquida', 'Lucro Líquido', 'Dívida Líquida']
    default = [d for d in default_cols if d in col_map.values()]
    selec = st.multiselect(
        "Escolha os Indicadores:",
        list(col_map.values()),
        default=default
    )
    if selec:
        sel_cols = [display_to_col[d] for d in selec]
        df_melt = indicadores.melt(
            id_vars=['Data'], value_vars=sel_cols,
            var_name='Indicador', value_name='Valor'
        )
        df_melt['Indicador'] = df_melt['Indicador'].map(col_map)
        fig = px.bar(
            df_melt,
            x='Data', y='Valor',
            color='Indicador',
            barmode='group',
            title='Evolução dos Balanços Selecionados'
        )
        st.plotly_chart(fig, use_container_width=True)


    st.divider()

    # -------------------------------------------------------------
    # Estilo global para os blocos de múltiplos
    # -------------------------------------------------------------
    st.markdown("""
    <style>
    .metric-box {
        background-color: #ffffff;
        padding: 20px;
        margin: 10px;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
        text-align: center;
    }
    .metric-value {
        font-size: 24px;
        font-weight: bold;
    }
    .metric-label {
        font-size: 14px;
        color: #FFA500;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

    # -------------------------------------------------------------
    # Múltiplos atuais completos
    # -------------------------------------------------------------

    # Exibir múltiplos em "quadrados"
    st.markdown("### Indicadores Financeiros")
    
    mdf = load_multiplos_limitado_from_db(ticker)
    if mdf is not None and not mdf.empty:
        m0 = mdf.iloc[0]

        # Primeira linha
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-value">{m0['Margem_Liquida']:.2f}%</div>
                <div class="metric-label" title="Eficiência em converter receita em lucro.">
                    Margem Líquida
                </div>
            </div>
            """, unsafe_allow_html=True)
        with c2:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-value">{m0['Margem_Operacional']:.2f}%</div>
                <div class="metric-label" title="Eficiência operacional (EBIT/Receita Líquida).">
                    Margem Operacional
                </div>
            </div>
            """, unsafe_allow_html=True)
        with c3:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-value">{m0['ROE']:.2f}%</div>
                <div class="metric-label" title="Retorno sobre o Patrimônio (Lucro/Patrimônio).">
                    ROE
                </div>
            </div>
            """, unsafe_allow_html=True)
        with c4:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-value">{m0['ROIC']:.2f}%</div>
                <div class="metric-label" title="Retorno sobre Capital Investido (EBIT/(Ativo-Passivo)).">
                    ROIC
                </div>
            </div>
            """, unsafe_allow_html=True)

        # Segunda linha
        c5, c6, c7, c8 = st.columns(4)
        dy = m0.get('DY', 0)
        dy_pct = '-' if pd.isna(dy) or price == 0 else f"{100*(dy/price):.2f}%"
        with c5:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-value">{dy_pct}</div>
                <div class="metric-label" title="Dividendos pagos por ação / preço da ação.">
                    Dividend Yield
                </div>
            </div>
            """, unsafe_allow_html=True)
        with c6:
            pvp = m0.get('P/VP', np.nan)
            pvp_fmt = '-' if pd.isna(pvp) or pvp == 0 else f"{price/pvp:.2f}"
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-value">{pvp_fmt}</div>
                <div class="metric-label" title="Preço / Valor Patrimonial.">
                    P/VP
                </div>
            </div>
            """, unsafe_allow_html=True)
        with c7:
            payout = m0.get('Payout', np.nan)
            payout_fmt = '-' if pd.isna(payout) else f"{payout*100:.2f}%"
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-value">{payout_fmt}</div>
                <div class="metric-label" title="Percentual do lucro distribuído em dividendos.">
                    Payout
                </div>
            </div>
            """, unsafe_allow_html=True)
        with c8:
            pl = m0.get('P/L', np.nan)
            pl_fmt = '-' if pd.isna(pl) or pl == 0 else f"{price/pl:.2f}"
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-value">{pl_fmt}</div>
                <div class="metric-label" title="Preço / Lucro (anos para retorno).">
                    P/L
                </div>
            </div>
            """, unsafe_allow_html=True)

        # Terceira linha (exemplo com 3 métricas; adicione mais se quiser)
        c9, c10, c11, c12 = st.columns(4)
        with c9:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-value">{m0['Endividamento_Total']:.2f}</div>
                <div class="metric-label" title="Passivo Total / Ativo Total.">
                    Endividamento Total
                </div>
            </div>
            """, unsafe_allow_html=True)
        with c10:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-value">{m0['Alavancagem_Financeira']:.2f}</div>
                <div class="metric-label" title="Dívida Líquida / Patrimônio Líquido.">
                    Alavancagem Financeira
                </div>
            </div>
            """, unsafe_allow_html=True)
        with c11:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-value">{m0['Liquidez_Corrente']:.2f}</div>
                <div class="metric-label" title="Ativo Circulante / Passivo Circulante.">
                    Liquidez Corrente
                </div>
            </div>
            """, unsafe_allow_html=True)
        # c12 pode ficar vazio ou para outro múltiplo


    st.divider()

    # Gráfico histórico de múltiplos _________________________________________________________________________________________________________________________________________________-
    hist_df = load_multiplos_from_db(ticker)
    if hist_df is not None and not hist_df.empty:
        hist_df['Data'] = pd.to_datetime(hist_df['Data'], errors='coerce')
        st.markdown("### Evolução Histórica dos Múltiplos")
        exclude = ['Data', 'Ticker', 'N Acoes']
        custom = {'DY': 'Dividend Yield', 'P/L': 'P/L', 'P/VP': 'P/VP'}

        def create_map(df, exclude, custom_map):
            """Cria mapeamento de nomes de colunas para exibição amigável."""
            cm, dm = {}, {}
            for c in df.columns:
                if c in exclude:
                    continue
                friendly = custom_map.get(c, c.replace('_', ' ').title())
                cm[c] = friendly
                dm[friendly] = c
            return cm, dm, list(cm.values())

        cm, dm, names = create_map(hist_df, exclude, custom)
        sel = st.multiselect("Indicadores:", names, default=names[:2], key='hist_mult')
        if sel:
            cols_sel = [dm[n] for n in sel]
            dfm = hist_df.melt(id_vars=['Data'], value_vars=cols_sel, var_name='Indicador', value_name='Valor')
            dfm['Indicador'] = dfm['Indicador'].map(cm)
            fig = px.bar(dfm, x='Data', y='Valor', color='Indicador', barmode='group', title='Histórico de Múltiplos')
            st.plotly_chart(fig, use_container_width=True)
