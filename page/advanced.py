from __future__ import annotations

import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import matplotlib.pyplot as plt

from core.helpers import (
    get_logo_url,
    obter_setor_da_empresa,
    determinar_lideres,
    formatar_real,
)
from core.db_loader import (
    load_setores_from_db,
    load_data_from_db,
    load_multiplos_from_db,
    load_macro_summary,
)
from core.yf_data import baixar_precos, coletar_dividendos

from core.scoring import (
    calcular_score_acumulado,
    penalizar_plato,
)
from core.portfolio import (
    gerir_carteira,
    gerir_carteira_todas_empresas,
    calcular_patrimonio_selic_macro,
)

from core.weights import get_pesos


def render() -> None:
    """Renderiza a aba Avançada."""

    st.markdown("<h1 style='text-align:center'>Análise Avançada de Ações</h1>", unsafe_allow_html=True)

    setores = st.session_state.get("setores_df")
    if setores is None or setores.empty:
        setores = load_setores_from_db()
        if setores is None or setores.empty:
            st.error("Tabela de setores não encontrada no banco de dados.")
            return
        st.session_state["setores_df"] = setores

    dados_macro = load_macro_summary()

    # ──────────────────────────────────────────────────────────────────────────
    # Sidebar
    # ──────────────────────────────────────────────────────────────────────────
    with st.sidebar:
        setor = st.selectbox("Setor:", sorted(setores["SETOR"].dropna().unique()))
        subsetores = setores.loc[setores["SETOR"] == setor, "SUBSETOR"].dropna().unique()
        subsetor = st.selectbox("Subsetor:", sorted(subsetores))
        segmentos = setores.loc[
            (setores["SETOR"] == setor) & (setores["SUBSETOR"] == subsetor),
            "SEGMENTO",
        ].dropna().unique()
        segmento = st.selectbox("Segmento:", sorted(segmentos))
        tipo = st.radio("Perfil de empresa:", ["Crescimento (<10 anos)", "Estabelecida (≥10 anos)", "Todas"], index=2)

        # Modo smartphone (para não depender de detecção automática)
        st.session_state["is_mobile"] = st.toggle("Modo smartphone (layout)", value=True)

    is_mobile = bool(st.session_state.get("is_mobile", False))

    # ──────────────────────────────────────────────────────────────────────────
    # Carrega empresas do filtro
    # ──────────────────────────────────────────────────────────────────────────
    empresas = []
    for _, row in setores.iterrows():
        if row["SETOR"] != setor or row["SUBSETOR"] != subsetor or row["SEGMENTO"] != segmento:
            continue

        tk = row["ticker"]
        dre = load_data_from_db(f"{tk}.SA")
        if dre is None or dre.empty:
            continue

        anos_hist = pd.to_datetime(dre["Data"]).dt.year.nunique()
        if (
            (tipo == "Crescimento (<10 anos)" and anos_hist < 10)
            or (tipo == "Estabelecida (≥10 anos)" and anos_hist >= 10)
            or (tipo == "Todas")
        ):
            empresas.append(row)

    if not empresas:
        st.warning("Nenhuma empresa atende aos filtros escolhidos.")
        return

    empresas = pd.DataFrame(empresas)

    # ──────────────────────────────────────────────────────────────────────────
    # Blocos: empresas selecionadas (RESPONSIVO)
    # ──────────────────────────────────────────────────────────────────────────
    st.subheader("Empresas Selecionadas")

    ncols_empresas = 1 if is_mobile else 3
    colunas_layout = st.columns(ncols_empresas)

    for idx, row in enumerate(empresas.itertuples()):
        col = colunas_layout[idx % len(colunas_layout)]
        with col:
            logo_url = get_logo_url(row.ticker)

            # Mantive seu visual original (claro), apenas responsivo.
            st.markdown(
                f"""
                <div style="
                    border: 2px solid #ddd;
                    border-radius: 10px;
                    padding: 15px;
                    margin: 10px;
                    background-color: #f9f9f9;
                    box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
                    text-align: center;
                ">
                    <img src="{logo_url}" style="width: 50px; height: 50px; margin-bottom: 10px;">
                    <h4 style="color: #333; margin: 0;">{row.nome_empresa} ({row.ticker})</h4>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # ──────────────────────────────────────────────────────────────────────────
    # Carrega variáveis das empresas selecionadas
    # ──────────────────────────────────────────────────────────────────────────
    lista_empresas = []
    for _, r in empresas.iterrows():
        tk_full = f"{r['ticker']}.SA"
        mult = load_multiplos_from_db(tk_full)
        dre = load_data_from_db(tk_full)

        if mult is None or mult.empty or dre is None or dre.empty:
            continue

        mult["Ano"] = pd.to_datetime(mult["Data"], errors="coerce").dt.year
        dre["Ano"] = pd.to_datetime(dre["Data"], errors="coerce").dt.year

        lista_empresas.append({"ticker": r["ticker"], "nome": r["nome_empresa"], "multiplos": mult, "dre": dre})

    if not lista_empresas:
        st.error("Não foi possível carregar dados financeiros para as empresas.")
        return

    # Setores das empresas
    setores_empresa = {e["ticker"]: obter_setor_da_empresa(e["ticker"], setores) for e in lista_empresas}

    # Pesos por setor
    pesos_utilizados = get_pesos(setor)

    # SCORE das empresas
    score = calcular_score_acumulado(lista_empresas, setores_empresa, pesos_utilizados, dados_macro, anos_minimos=4)

    # Preços
    precos = baixar_precos([e["ticker"] + ".SA" for e in lista_empresas])
    if precos is None or precos.empty:
        st.error("Não foi possível carregar preços (yfinance).")
        return

    precos_mensal = precos.resample("M").last()

    # Penalização platô
    score = penalizar_plato(score, precos_mensal, meses=12, penal=0.30)

    # Determina líderes (por ano, pela métrica padrão do helpers)
    lideres = determinar_lideres(score)

    # Lista de tickers filtrados
    tickers_filtrados = score["ticker"].unique()

    # Dividendos
    dividendos = coletar_dividendos(tickers_filtrados)

    # Carteira estratégia
    patrimonio_estrategia, datas_aportes = gerir_carteira(precos, score, lideres, dividendos)
    patrimonio_estrategia = patrimonio_estrategia[["Patrimônio"]]

    # Tesouro Selic (macro)
    patrimonio_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aportes)

    # Carteira para todas as empresas
    patrimonio_empresas = gerir_carteira_todas_empresas(precos, tickers_filtrados, datas_aportes, dividendos)

    # Combina p/ gráfico
    patrimonio_final = pd.concat([patrimonio_estrategia, patrimonio_empresas, patrimonio_selic], axis=1)

    # Checagem do líder mais recente (mantive)
    if score.empty:
        st.error("⚠️ Não há dados suficientes para determinar a empresa líder.")
        lider = None
    else:
        lider = score.sort_values("Ano", ascending=False).iloc[0]

    st.markdown("---")
    st.markdown("<div style='margin: 30px;'></div>", unsafe_allow_html=True)

    # ──────────────────────────────────────────────────────────────────────────
    # Gráfico de evolução patrimonial
    # ──────────────────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(12, 6))

    df_patrimonio_evolucao = patrimonio_final.copy()
    df_patrimonio_evolucao.index = pd.to_datetime(df_patrimonio_evolucao.index, errors="coerce")
    df_patrimonio_evolucao = df_patrimonio_evolucao.sort_index()

    if df_patrimonio_evolucao.empty:
        st.warning("⚠️ Dados insuficientes para plotar a evolução do patrimônio.")
    else:
        for ticker in df_patrimonio_evolucao.columns:
            if ticker == "Patrimônio":
                df_patrimonio_evolucao[ticker].plot(ax=ax, linewidth=2, color="red", label="Estratégia de Aporte")
            elif ticker == "Tesouro Selic":
                df_patrimonio_evolucao[ticker].plot(ax=ax, linewidth=2, linestyle="-.", color="blue", label="Tesouro Selic")
            else:
                df_patrimonio_evolucao[ticker].plot(ax=ax, linewidth=1, linestyle="--", alpha=0.6, color="gray", label=ticker)

        ax.set_title("Evolução do Patrimônio Acumulado")
        ax.set_xlabel("Data")
        ax.set_ylabel("Patrimônio (R$)")
        ax.legend()
        st.pyplot(fig)

    st.markdown("---")
    st.markdown("<div style='margin: 30px;'></div>", unsafe_allow_html=True)

    # ──────────────────────────────────────────────────────────────────────────
    # Cards: Patrimônio final por ativo (CORRIGIDO)
    # ──────────────────────────────────────────────────────────────────────────
    st.subheader("📊 Patrimônio Final para R$1.000/Mês Investidos desde a Data Inicial")

    # Em vez de melt/reset_index (que estava causando duplicações e “vazamento”),
    # consolidamos com uma série final robusta.
    try:
        s_final = pd.concat(
            [
                patrimonio_estrategia.iloc[-1],
                patrimonio_empresas.iloc[-1],
                patrimonio_selic.iloc[-1],
            ]
        )
    except Exception:
        st.warning("⚠️ Dados insuficientes para exibir o patrimônio final.")
        st.stop()

    df_patrimonio_final = (
        s_final.rename("Valor Final")
        .reset_index()
        .rename(columns={"index": "Ticker"})
        .dropna(subset=["Valor Final"])
        .sort_values("Valor Final", ascending=False)
        .reset_index(drop=True)
    )

    if df_patrimonio_final.empty:
        st.warning("⚠️ Dados insuficientes para exibir o patrimônio final.")
        st.stop()

    # Sanitização defensiva para evitar aparecer HTML como texto
    df_patrimonio_final["Ticker"] = df_patrimonio_final["Ticker"].astype(str).str.strip()
    df_patrimonio_final["Ticker"] = (
        df_patrimonio_final["Ticker"]
        .str.replace("<", "", regex=False)
        .str.replace(">", "", regex=False)
    )

    # Colunas responsivas
    num_columns = 1 if is_mobile else 3
    columns = st.columns(num_columns)

    # Contagem de líderes (se lideres vier vazio, não força tudo como líder)
    contagem_lideres = {}
    if lideres is not None and not lideres.empty and "ticker" in lideres.columns:
        contagem_lideres = lideres["ticker"].value_counts().to_dict()

    for i, row in df_patrimonio_final.iterrows():
        ticker = row["Ticker"]
        patrimonio = row["Valor Final"]

        if ticker == "Patrimônio":
            icone_url = "https://cdn-icons-png.flaticon.com/512/1019/1019709.png"
            border_color = "#DAA520"
            nome_exibicao = "Estratégia de Aporte"
        elif ticker == "Tesouro Selic":
            icone_url = "https://cdn-icons-png.flaticon.com/512/2331/2331949.png"
            border_color = "#007bff"
            nome_exibicao = "Tesouro Selic"
        else:
            icone_url = get_logo_url(ticker)
            border_color = "#d3d3d3"
            nome_exibicao = ticker

        vezes_lider = int(contagem_lideres.get(ticker, 0))
        lider_texto = f"🏆 {vezes_lider}x Líder" if vezes_lider > 0 else ""

        patrimonio_formatado = "Valor indisponível" if pd.isna(patrimonio) else formatar_real(patrimonio)

        col = columns[i % num_columns]
        with col:
            # Mantive seu card, mas com ajustes para NÃO quebrar no mobile
            st.markdown(
                f"""
                <div style="
                    background-color: #ffffff;
                    border: 3px solid {border_color};
                    border-radius: 10px;
                    padding: 15px;
                    margin: 10px;
                    text-align: center;
                    box-shadow: 2px 2px 5px rgba(0, 0, 0, 0.1);
                ">
                    <img src="{icone_url}" alt="{nome_exibicao}" style="width: 50px; height: auto; margin-bottom: 5px;">
                    <h3 style="margin: 0; color: #4a4a4a;">{nome_exibicao}</h3>

                    <p style="
                        font-size: 18px;
                        margin: 5px 0;
                        font-weight: bold;
                        color: #2ecc71;
                        white-space: nowrap;
                        font-variant-numeric: tabular-nums;
                    ">
                        {patrimonio_formatado}
                    </p>

                    <p style="font-size: 14px; color: #FFA500; margin: 0;">{lider_texto}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("---")
    st.markdown("<div style='margin: 30px;'></div>", unsafe_allow_html=True)

    # ──────────────────────────────────────────────────────────────────────────
    # Gráfico dos múltiplos
    # ──────────────────────────────────────────────────────────────────────────
    st.markdown("### Comparação de Indicadores (Múltiplos) entre Empresas do Segmento")

    indicadores_disponiveis = [
        "Margem Líquida",
        "Margem Operacional",
        "ROE",
        "ROIC",
        "P/L",
        "Liquidez Corrente",
        "Alavancagem Financeira",
        "Endividamento Total",
    ]

    nomes_to_col = {
        "Margem Líquida": "Margem_Liquida",
        "Margem Operacional": "Margem_Operacional",
        "ROE": "ROE",
        "ROIC": "ROIC",
        "P/L": "P/L",
        "Liquidez Corrente": "Liquidez_Corrente",
        "Alavancagem Financeira": "Alavancagem_Financeira",
        "Endividamento Total": "Endividamento_Total",
    }

    lista_empresas_ = [e["nome"] for e in lista_empresas]
    empresas_selecionadas = st.multiselect(
        "Selecione as empresas a serem exibidas no gráfico:",
        lista_empresas_,
        default=lista_empresas_,
    )

    indicador_selecionado = st.selectbox("Selecione o Indicador para Comparar:", indicadores_disponiveis, index=0)
    col_indicador = nomes_to_col[indicador_selecionado]

    normalizar = st.checkbox("Normalizar os Indicadores (Escala de 0 a 1)", value=False)

    df_historico = []
    for row in lista_empresas:
        nome_emp = row["nome"]
        if nome_emp in empresas_selecionadas:
            ticker = row["ticker"]
            multiplos_data = load_multiplos_from_db(ticker + ".SA")
            if multiplos_data is not None and not multiplos_data.empty and col_indicador in multiplos_data.columns:
                df_emp = multiplos_data[["Data", col_indicador]].copy()
                df_emp["Ano"] = pd.to_datetime(df_emp["Data"], errors="coerce").dt.year
                df_emp["Empresa"] = nome_emp
                df_historico.append(df_emp)
            else:
                st.info(f"Empresa {nome_emp} não possui dados para o indicador {indicador_selecionado}.")

    if len(df_historico) == 0:
        st.warning("Não há dados históricos disponíveis para as empresas selecionadas ou para o indicador escolhido.")
    else:
        df_historico = pd.concat(df_historico, ignore_index=True)
        df_historico = df_historico.dropna(subset=["Ano"])

        if normalizar:
            max_valor = df_historico[col_indicador].max()
            min_valor = df_historico[col_indicador].min()
            if max_valor != min_valor:
                df_historico[col_indicador] = (df_historico[col_indicador] - min_valor) / (max_valor - min_valor)

        anos_disponiveis = sorted(df_historico["Ano"].unique())
        df_historico["Ano"] = df_historico["Ano"].astype(str)

        fig = px.bar(
            df_historico,
            x="Ano",
            y=col_indicador,
            color="Empresa",
            barmode="group",
            title=f"Evolução Histórica de {indicador_selecionado} por Empresa",
        )

        fig.update_layout(
            xaxis_title="Ano",
            yaxis_title=f"{indicador_selecionado} {'(Normalizado)' if normalizar else ''}",
            xaxis=dict(type="category", categoryorder="category ascending", tickvals=anos_disponiveis),
            legend_title="Empresa",
        )

        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.markdown("<div style='margin: 30px;'></div>", unsafe_allow_html=True)

    # ──────────────────────────────────────────────────────────────────────────
    # Gráfico comparativo de demonstrações financeiras
    # ──────────────────────────────────────────────────────────────────────────
    st.markdown("### Comparação de Demonstrações Financeiras entre Empresas")

    empresas_completas_df = pd.DataFrame(lista_empresas)

    nomes_empresas_disponiveis = empresas_completas_df["nome"].tolist()
    empresas_selecionadas = st.multiselect(
        "Selecione as empresas para exibir:",
        nomes_empresas_disponiveis,
        default=nomes_empresas_disponiveis,
    )

    indicadores_dre = {
        "Receita Líquida": "Receita_Liquida",
        "EBIT": "EBIT",
        "Lucro Líquido": "Lucro_Liquido",
        "Patrimônio Líquido": "Patrimonio_Liquido",
        "Dívida Líquida": "Divida_Liquida",
        "Caixa Líquido": "Caixa_Liquido",
    }

    indicador_display = st.selectbox("Escolha o Indicador:", list(indicadores_dre.keys()))
    coluna_indicador = indicadores_dre[indicador_display]

    def load_dre_comparativo(empresas_df: pd.DataFrame) -> pd.DataFrame:
        dfs = []
        for _, row in empresas_df.iterrows():
            nome = row["nome"]
            ticker = row["ticker"]
            df = load_data_from_db(ticker + ".SA")
            if df is not None and not df.empty:
                df["Empresa"] = nome
                df["Ano"] = pd.to_datetime(df["Data"], errors="coerce").dt.year
                dfs.append(df)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    empresas_filtradas_df = empresas_completas_df[empresas_completas_df["nome"].isin(empresas_selecionadas)]
    dre_df = load_dre_comparativo(empresas_filtradas_df)

    if not dre_df.empty and coluna_indicador in dre_df.columns:
        df_plot = dre_df[["Ano", coluna_indicador, "Empresa"]].dropna()
        df_plot = df_plot.rename(columns={coluna_indicador: "Valor"})
        df_plot["Ano"] = df_plot["Ano"].astype(str)

        fig = px.bar(
            df_plot,
            x="Ano",
            y="Valor",
            color="Empresa",
            barmode="group",
            title=f"Comparação de {indicador_display} entre Empresas",
        )
        fig.update_layout(
            xaxis_title="Ano",
            yaxis_title=indicador_display,
            legend_title="Empresa",
            xaxis=dict(type="category"),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Não há dados suficientes para o indicador selecionado entre as empresas escolhidas.")
