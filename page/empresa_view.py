from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st
from core.helpers import get_company_info, get_logo_url
from core.db_loader import load_data_from_db, load_multiplos_from_db, load_multiplos_tri_from_db
from core.yf_data import get_price, get_fundamentals_yf
import plotly.express as px

# Regressão Linear para determinar a taxa de crescimento das variáveis ------------------------------------------------------------------------------------
def calculate_growth_rate(df, column):
    try:
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        df = df.sort_values(by='Data')
        df_valid = df[df[column].notnull() & (df[column] > 0)]
        if df_valid.shape[0] < 2:
            return np.nan
        X = (df_valid['Data'] - df_valid['Data'].iloc[0]).dt.days / 365.25
        y_log = np.log(df_valid[column].values)
        slope, _ = np.polyfit(X, y_log, deg=1)
        return np.exp(slope) - 1
    except Exception:
        return np.nan

# Transforma a taxa de crescimento em porcentagem -----------------------------------------------------------------------------------
def format_growth_rate(value): 
    if isinstance(value, (int, float)) and not pd.isna(value) and not np.isinf(value):
        return f"{value:.2%}"
    return "-"

# Início da página de visualização individual das empresas -----------------------------------------------------------------------------
def render_empresa_view(ticker: str):
    indicadores = load_data_from_db(ticker)
    if indicadores is None or indicadores.empty:
        st.error("Indicadores financeiros não encontrados.")
        return

    indicadores = indicadores.drop(columns=["Ticker"], errors="ignore")
    indicadores["Data"] = pd.to_datetime(indicadores["Data"], errors="coerce")
    indicadores = indicadores.sort_values("Data")

    company_name, company_website = get_company_info(ticker)
    current_price = get_price(ticker)
    logo_url = get_logo_url(ticker)

    st.write(f"Ticker inserido: {ticker}")
    col1, col2 = st.columns([4, 1])
    with col1:
        st.subheader(f"{company_name} - Preço Atual: R$ {current_price:.2f}")
        st.caption(f"Informações financeiras de {company_name}")
    with col2:
        st.image(logo_url, width=80)

    growth_rates = {
        col: calculate_growth_rate(indicadores, col)
        for col in indicadores.columns if col != "Data"
    }

    # Visualização das taxas de crescimento da receita, lucro e patrimônio da Empresa ------------------------------------------------
    st.markdown("## Visão Geral (Taxa de Crescimento Médio Anual)")
    st.markdown("""
        <style>
        .growth-box {
            border: 2px solid #ddd;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 10px;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100px;
            width: 100%;
            text-align: center;
            font-size: 20px;
            font-weight: bold;
            color: #333;
            background-color: #f9f9f9;
        }
        .metric-box {
            background-color: #f9f9f9;
            border-radius: 10px;
            padding: 10px;
            text-align: center;
            margin-bottom: 15px;
            border: 1px solid #e0e0e0;
        }
        .metric-value {
            font-size: 24px;
            font-weight: bold;
            color: #222;
        }
        .metric-label {
            font-size: 14px;
            color: #ff6600;
            font-weight: bold;
        }
        </style>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(
            f"<div class='growth-box'>Receita Líquida: {format_growth_rate(growth_rates.get('Receita_Liquida'))}</div>",
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            f"<div class='growth-box'>Lucro Líquido: {format_growth_rate(growth_rates.get('Lucro_Liquido'))}</div>",
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            f"<div class='growth-box'>Patrimônio Líquido: {format_growth_rate(growth_rates.get('Patrimonio_Liquido'))}</div>",
            unsafe_allow_html=True,
        )

    # Gráfico das demonstrações financeiras -------------------------------------------------------------------------------------------------
    st.markdown("---")
    st.markdown("### Demonstrações Financeiras")
    friendly = {
        "Receita_Liquida": "Receita Líquida",
        "Lucro_Liquido": "Lucro Líquido",
        "EBIT": "EBIT",
        "LPA": "LPA",
        "Divida_Liquida": "Dívida Líquida",
    }
    opcoes_grafico = [friendly.get(c, c.replace('_', ' ')) for c in indicadores.columns if c != "Data"]
    default_grafico = [x for x in ["Receita Líquida", "Lucro Líquido", "Dívida Líquida"] if x in opcoes_grafico]
    sel = st.multiselect("Escolha os Indicadores:", opcoes_grafico, default=default_grafico)
    if sel:
        rev = {v: k for k, v in friendly.items()}
        cols_sel = [rev.get(x, x.replace(" ", "_")) for x in sel]
        dfm = indicadores.melt(
            id_vars=["Data"], value_vars=cols_sel,
            var_name="Indicador", value_name="Valor")
        dfm["Indicador"] = dfm["Indicador"].map(lambda x: friendly.get(x, x.replace('_', ' ')))
        st.markdown("### Evolução dos Balanços Selecionados")
        st.plotly_chart(
            px.bar(dfm, x="Data", y="Valor", color="Indicador", barmode="group"),
            use_container_width=True,
        )

    # Gráfico dos blocos ------------------------------------------------------------------------------------------------------------------
    multiplos_db = load_multiplos_tri_from_db(ticker)
    multiplos_yf = get_fundamentals_yf(ticker)

    # ───────────── Faz fusão com fallback para dados do banco ─────────────
    if multiplos_db is None or multiplos_db.empty:
        multiplos = multiplos_yf
    else:
        for col in multiplos_yf.columns:
            if col in multiplos_db.columns:
                yf_val = multiplos_yf.at[0, col]
                db_val = multiplos_db.at[0, col]
                if pd.isna(yf_val) or yf_val == 0:
                    multiplos_yf.at[0, col] = db_val
        multiplos = multiplos_yf
          
    st.markdown("---")
    st.markdown("### Indicadores Financeiros")
    
    # Dicionário de descrições para tooltip
    descricoes = {
        "Margem Líquida": "Lucro Líquido ÷ Receita Líquida — mostra quanto sobra do faturamento como lucro final.",
        "Margem Operacional": "EBIT ÷ Receita Líquida — mostra a eficiência operacional antes dos impostos e juros.",
        "ROE": "Lucro Líquido ÷ Patrimônio Líquido — mede a rentabilidade para o acionista.",
        "ROIC": "NOPAT ÷ Capital Investido — indica a eficiência do capital operacional.",
        "Dividend Yield": "Dividendos por ação ÷ Preço da ação — mostra a rentabilidade via dividendos.",
        "P/VP": "Preço da ação ÷ Valor Patrimonial por ação — avalia o quanto se paga pelo patrimônio.",
        "Payout": "Dividendos ÷ Lucro Líquido — mostra a parte do lucro distribuída.",
        "P/L": "Preço da ação ÷ Lucro por ação — indica quantos anos o lucro 'paga' o preço da ação.",
        "Endividamento Total": "Dívida Total ÷ Patrimônio Líquido — mostra o grau de alavancagem financeira.",
        "Alavancagem Financeira": "Indicador de endividamento baseado no fluxo de caixa ou dívida líquida.",
        "Liquidez Corrente": "Ativo Circulante ÷ Passivo Circulante — indica a capacidade de pagar obrigações de curto prazo.",
    }
    
    # Indicadores a exibir
    valores = [
        ("Margem_Liquida", "Margem Líquida"),
        ("Margem_Operacional", "Margem Operacional"),
        ("ROE", "ROE"),
        ("ROIC", "ROIC"),
        ("DY", "Dividend Yield"),
        ("P/VP", "P/VP"),
        ("Payout", "Payout"),
        ("P/L", "P/L"),
        ("Endividamento_Total", "Endividamento Total"),
        ("Alavancagem_Financeira", "Alavancagem Financeira"),
        ("Liquidez_Corrente", "Liquidez Corrente"),
    ]
    
    # Layout em blocos com 4 por linha
    rows = (len(valores) + 3) // 4
    for i in range(rows):
        cols = st.columns(4)
        for j, (col, label) in enumerate(valores[i * 4:(i + 1) * 4]):
            with cols[j]:
                valor = multiplos[col].values[0] if col in multiplos else None
    
                if pd.isna(valor) or valor == 0:
                    val_formatado = "-"
                elif "Margem" in label or label in ["ROE", "ROIC", "Payout", "Dividend Yield", "Endividamento Total"]:
                    val_formatado = f"{valor:.2f}%"
                else:
                    val_formatado = f"{valor:.2f}"
    
                tooltip = descricoes.get(label, "")
                st.markdown(f"""
                <div class='metric-box' title="{tooltip}">
                    <div class='metric-value'>{val_formatado}</div>
                    <div class='metric-label'><strong>{label}</strong></div>
                </div>
                """, unsafe_allow_html=True)

                    
    # Gráfico dos múltiplos -----------------------------------------------------------------------------------------------------------------------------------
    multiplos = load_multiplos_from_db(ticker)
    st.markdown("---")
    st.markdown("### Gráfico de Múltiplos")
    multiplos['Data'] = pd.to_datetime(multiplos['Data'], errors='coerce')
    exclude_columns = ['Data', 'Ticker', 'N Acoes']
    col_name_mapping = {col: col.replace('_', ' ').title() for col in multiplos.columns if col not in exclude_columns}
    display_name_to_col = {v: k for k, v in col_name_mapping.items()}
    display_names = list(col_name_mapping.values())
    default_display = [n for n in ["Margem Líquida", "Margem Operacional"] if n in display_names]
    variaveis_selecionadas_display = st.multiselect("Escolha os Indicadores:", display_names, default=default_display)
    if variaveis_selecionadas_display:
        variaveis_selecionadas = [display_name_to_col[nome] for nome in variaveis_selecionadas_display]
        dfm_mult = multiplos.melt(
            id_vars=['Data'],
            value_vars=variaveis_selecionadas,
            var_name='Indicador',
            value_name='Valor'
        )
        dfm_mult['Indicador'] = dfm_mult['Indicador'].map(col_name_mapping)
        st.plotly_chart(
            px.bar(dfm_mult, x='Data', y='Valor', color='Indicador', barmode='group'),
            use_container_width=True
        )


