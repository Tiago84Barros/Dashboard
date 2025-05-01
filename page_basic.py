"""page_basic.py
~~~~~~~~~~~~~~~~
Página “Básica” isolada em um módulo.

Uso:
-----
import page_basic as pb
pb.render()

Pré‑requisitos (importados no seu app principal ou no `__init__.py`):
- streamlit as st
- pandas as pd
- numpy as np
- plotly.express as px
- yfinance as yf
- Funções utilitárias (`get_logo_url`, `get_company_info`,
  `load_data_from_db`, `load_multiplos_from_db`,
  `load_multiplos_limitado_from_db`, `calculate_growth_rate`, ...)

Essas dependências são chamadas dentro da função; garanta que estejam no
PYTHONPATH.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import yfinance as yf

# ---------------------------------------------------------------------------
# Dependências externas (devem existir no escopo principal) ------------------
# ---------------------------------------------------------------------------
from db_loader import (
    load_data_from_db,
    load_multiplos_from_db,
    load_multiplos_limitado_from_db,
)
from yf_data import get_company_info
from utils import get_logo_url  # Ajuste o import de acordo com a sua árvore

# ---------------------------------------------------------------------------
# Core da página -------------------------------------------------------------
# ---------------------------------------------------------------------------

def render():
    """Renderiza a aba “Básica” inteira dentro do Streamlit."""

    pagina = st.session_state.get("pagina", "Básica")

    if pagina != "Básica":  # early‑return para não poluir outras páginas
        return

    # ---------------------------------------------------------------------
    # Header
    # ---------------------------------------------------------------------
    st.markdown(
        """
        <h1 style='text-align: center; font-size: 36px; color: #333;'>Análise Básica de Ações</h1>
        """,
        unsafe_allow_html=True,
    )

    # ---------------------------------------------------------------------
    # Botão Atualizar dados (canto superior direito)
    # ---------------------------------------------------------------------
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

    # ---------------------------------------------------------------------
    # Estilo para os cards dos setores
    # ---------------------------------------------------------------------
    st.markdown(
        """
        <style>
        .sector-box{border:1px solid #ddd;padding:15px;border-radius:10px;margin-bottom:10px;display:flex;justify-content:space-between;align-items:center;height:140px;cursor:pointer;transition:background-color .3s ease;}
        .sector-box:hover{background:#f0f0f0;}
        .sector-info{font-size:14px;color:#333;text-align:left;flex:1;overflow:hidden;text-overflow:ellipsis;}
        .sector-info strong{font-size:16px;color:#000;}
        .sector-logo{width:50px;height:auto;margin-left:15px;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ---------------------------------------------------------------------
    # Campo de ticker
    # ---------------------------------------------------------------------
    setores = st.session_state.get("setores_df")  # carregue antes no app

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

    # ---------------------------------------------------------------------
    # Lista de setores (fallback se ticker vazio)
    # ---------------------------------------------------------------------
    if not ticker:
        st.markdown("### Selecione um Ticker")
        if setores is not None and not setores.empty:
            for setor, dados in setores.groupby("SETOR"):
                st.markdown(f"#### {setor}")
                col1, col2, col3 = st.columns(3)
                for i, row in dados.iterrows():
                    logo_url = get_logo_url(row["ticker"])
                    with [col1, col2, col3][i % 3]:
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
        return  # Não segue para análise de ticker

    # ---------------------------------------------------------------------
    # Carregar dados do ticker
    # ---------------------------------------------------------------------
    indicadores = load_data_from_db(ticker)
    if indicadores is None or indicadores.empty:
        st.error("Dados financeiros não encontrados para o ticker.")
        return

    indicadores = indicadores.drop(columns=["Ticker"])

    # ---------------------------------------------------------------------
    # Taxas de crescimento (regressão log)
    # ---------------------------------------------------------------------
    def calculate_growth_rate(df: pd.DataFrame, column: str):
        try:
            df = df.copy()
            df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
            df = df.sort_values("Data")
            mask = df[column].notnull() & (df[column] > 0)
            df_valid = df.loc[mask]
            if df_valid.shape[0] < 2:
                return np.nan
            X = (df_valid["Data"] - df_valid["Data"].iloc[0]).dt.days / 365.25
            y_log = np.log(df_valid[column])
            slope, _ = np.polyfit(X, y_log, deg=1)
            return np.exp(slope) - 1
        except Exception:
            return np.nan

    growth_rates = {
        col: calculate_growth_rate(indicadores, col) if col != "Data" else np.nan
        for col in indicadores.columns
    }

    # ---------------------------------------------------------------------
    # Info da empresa + logo + preço atual
    # ---------------------------------------------------------------------
    name, site = get_company_info(ticker)
    price_df = yf.Ticker(ticker).history(period="1d")
    current_price = price_df["Close"].iloc[0] if not price_df.empty else np.nan

    if name:
        colA, colB = st.columns([4, 1])
        with colA:
            st.subheader(f"{name} — Preço Atual: R$ {current_price:,.2f}")
        with colB:
            st.image(get_logo_url(ticker), width=80)
    else:
        st.error("Empresa não encontrada.")

    # ---------------------------------------------------------------------
    # Taxas de crescimento — visual
    # ---------------------------------------------------------------------
    st.markdown(
        """
        <style>
        .growth-box{border:2px solid #ddd;padding:20px;border-radius:10px;margin-bottom:10px;display:flex;justify-content:center;align-items:center;height:100px;font-size:20px;font-weight:bold;background:#f9f9f9;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    def fmt(x):
        return f"{x:.2%}" if pd.notna(x) else "-"

    st.markdown("### Taxa de Crescimento Médio Anual")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f"<div class='growth-box'>Receita Líquida: {fmt(growth_rates['Receita_Liquida'])}</div>", unsafe_allow_html=True)
    with c2:
        st.markdown(f"<div class='growth-box'>Lucro Líquido: {fmt(growth_rates['Lucro_Liquido'])}</div>", unsafe_allow_html=True)
    with c3:
        st.markdown(f"<div class='growth-box'>Patrimônio Líquido: {fmt(growth_rates['Patrimonio_Liquido'])}</div>", unsafe_allow_html=True)

    st.divider()

    # ---------------------------------------------------------------------
    # Gráfico de DFPs selecionáveis
    # ---------------------------------------------------------------------
    col_map = {c: c.replace("_", " ").title() for c in indicadores.columns if c != "Data"}
    correcoes = {
        'Receita Liquida': 'Receita Líquida',
        'Lucro Liquido': 'Lucro Líquido',
        'Patrimonio Liquido': 'Patrimônio Líquido',
        'Caixa Liquido': 'Caixa Líquido',
        'Divida Liquida': 'Dívida Líquida',
    }
    col_map = {k: correcoes.get(v, v) for k, v in col_map.items()}
    disp_to_col = {v: k for k, v in col_map.items()}

    default = ['Receita Líquida', 'Lucro Líquido', 'Dívida Líquida']

    sel_disp = st.multiselect(
        "Escolha os Indicadores:", list(col_map.values()), default=[d for d in default if d in col_map.values()]
    )

    if sel_disp:
        sel_cols = [disp_to_col[d] for d in sel_disp]
        df_melt = indicadores.melt(id_vars=['Data'], value_vars=sel_cols, var_name='Indicador', value_name='Valor')
        df_melt['Indicador'] = df_melt['Indicador'].map(col_map)
        fig = px.bar(df_melt, x='Data', y='Valor', color='Indicador', barmode='group', title='Evolução dos Balanços Selecionados')
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ---------------------------------------------------------------------
    # Múltiplos atuais em cards + gráfico histórico
    # ---------------------------------------------------------------------
    mult_atual = load_multiplos_limitado_from_db(ticker)
    if mult_atual is not None and not mult_atual.empty:
        mult_atual = mult_atual.iloc[0]
        st.markdown(
            """
            <style>.metric-box{background:#fff;padding:20px;margin:10px;border-radius:10px;box-shadow:2px 2px 5px rgba(0,0,0,.1);text-align:center;}</style>
            """,
            unsafe_allow_html=True,
        )
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"<div class='metric-box'><div class='metric-value'>{mult_atual['Margem_Liquida']:.2f}%</div><div class='metric-label'>Margem Líquida</div></div>", unsafe_allow_html=True)
        with c2:
            st.markdown(f"<div class='metric-box'><div class='metric-value'>{mult_atual['Margem_Operacional']:.2f}%</div><div class='metric-label'>Margem Operacional</div></div>", unsafe_allow_html=True)
        with c3:
            st.markdown(f"<div class='metric-box'><div class='metric-value'>{mult_atual['ROE']:.2f}%</div><div class='metric-label'>ROE</div></div>", unsafe_allow_html=True)
        with c4:
            st.markdown(f"<div class='metric-box'><div class='metric-value'>{mult_atual['ROIC']:.2f}%</div><div class='metric-label'>ROIC</div></div>", unsafe_allow_html=True)

    # Gráfico histórico dos múltiplos
    mult_hist = load_multiplos_from_db(ticker)
    if mult_hist is not None and not mult_hist.empty:
        mult_hist['Data'] = pd.to_datetime(mult_hist['Data'])
        st.markdown("### Evolução Histórica dos Múltiplos")

        exclude = ['Data', 'Ticker', 'N Acoes']
        custom = {'DY': 'Dividend Yield', 'P_L': 'P/L', 'P_VP': 'P/VP'}
        col_map_hist, disp_to_col_hist, disp_names_hist = _create_map(mult_hist, exclude, custom)  # helper inline

        sel_disp_hist = st.multiselect(
            "Indicadores:", disp_names
