"""page_advanced.py
~~~~~~~~~~~~~~~~~~~
Aba “Avançada” completamente modularizada.

Uso:
-----
import page_advanced as pa
pa.render()
"""

from __future__ import annotations

# ────────────────────────────────────────────────────────────────────────────────
# Dependências externas que o DASHBOARD já instala
# ────────────────────────────────────────────────────────────────────────────────
import streamlit as st
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import numpy  as np

# ────────────────────────────────────────────────────────────────────────────────
# MÓDULOS PRÓPRIOS (todos em arquivos .py do seu repo)
# ────────────────────────────────────────────────────────────────────────────────
from helpers   import (
    get_logo_url,
    obter_setor_da_empresa,
    determinar_lideres,
    formatar_real,
)
from db_loader import (
    load_setores_from_db,
    load_data_from_db,
    load_multiplos_from_db,
    load_multiplos_limitado_from_db,
    load_macro_summary,
)
from weights   import PESOS_POR_SETOR, INDICADORES_SCORE_GENERICO
from tech_ind  import (
    baixar_precos,
    calc_momentum_12m,
    coletar_dividendos,
)
from scoring   import (
    calcular_metricas_historicas_simplificadas,
    calcular_score_acumulado,
    penalizar_plato,
)
from portfolio import (
    gerir_carteira,
    gerir_carteira_todas_empresas,
    calcular_patrimonio_selic_macro,
)

# ☝️  se você usou nomes de ficheiro diferentes, ajuste apenas estes imports


# ════════════════════════════════════════════════════════════════════════════════
# Página propriamente dita
# ════════════════════════════════════════════════════════════════════════════════
def render() -> None:
    """Renderiza a aba “Avançada” inteira no Streamlit."""
    if st.session_state.get("pagina") != "Avançada":
        return  # ← estamos em outra página, não faz nada

    # --------------------------------------------------------------------- HEADER
    st.markdown(
        "<h1 style='text-align:center;font-size:36px;color:#333'>Análise Avançada de Ações</h1>",
        unsafe_allow_html=True,
    )

    # ------------------------------------------------------------------- DADOS DB
    setores = st.session_state.get("setores_df") or load_setores_from_db()
    if setores is None or setores.empty:
        st.error("Tabela de setores não encontrada no banco de dados.")
        return

    dados_macro = load_macro_summary()

    # --------------------------------------------------------- FILTROS HIERÁRQUICOS
    setor   = st.selectbox("Setor:",      sorted(setores["SETOR"].dropna().unique()))
    if not setor:
        return
    subsetor = st.selectbox("Subsetor:",  sorted(setores.query("SETOR==@setor")["SUBSETOR"].dropna().unique()))
    if not subsetor:
        return
    segmento = st.selectbox("Segmento:",  sorted(setores.query("SETOR==@setor & SUBSETOR==@subsetor")["SEGMENTO"].dropna().unique()))
    if not segmento:
        return

    empresas_raw = setores.query(
        "SETOR==@setor & SUBSETOR==@subsetor & SEGMENTO==@segmento"
    ).reset_index(drop=True)
    if empresas_raw.empty:
        st.warning("Não há empresas nesse segmento.")
        return

    # ------------------------------------ Filtro crescimento x estabelecida
    tipo = st.selectbox("Tipo de Empresa:",
                        ["Todas", "Crescimento (<10 anos)", "Estabelecida (≥10 anos)"])

    empresas = []
    for _, row in empresas_raw.iterrows():
        dre = load_data_from_db(row["ticker"] + ".SA")
        if dre is None or dre.empty:
            continue
        anos = pd.to_datetime(dre["Data"], errors="coerce").dt.year.nunique()
        if (
            (tipo == "Crescimento (<10 anos)" and anos < 10) or
            (tipo == "Estabelecida (≥10 anos)" and anos >= 10) or
            (tipo == "Todas")
        ):
            empresas.append(row)

    if not empresas:
        st.warning("Nenhuma empresa atende aos filtros escolhidos.")
        return
    empresas = pd.DataFrame(empresas)

    # --------------------------------------------- Mostrar logos das escolhidas
    st.markdown("### Empresas Selecionadas")
    cols = st.columns(3)
    for i, r in empresas.iterrows():
        with cols[i % 3]:
            st.image(get_logo_url(r["ticker"]), width=60)
            st.caption(f"{r['nome_empresa']} ({r['ticker']})")

    # ------------------------------------------------------------------- DOWNLOAD
    lista_empresas   = []
    tickers_pt_br    = []
    for _, r in empresas.iterrows():
        tk = r["ticker"]
        m  = load_multiplos_from_db(tk + ".SA")
        dre = load_data_from_db(tk + ".SA")
        if m is None or dre is None or m.empty or dre.empty:
            continue
        m["Ano"]   = pd.to_datetime(m["Data"], errors="coerce").dt.year
        dre["Ano"] = pd.to_datetime(dre["Data"], errors="coerce").dt.year
        lista_empresas.append({"ticker": tk, "multiplos": m, "df_dre": dre})
        tickers_pt_br.append(tk + ".SA")          # para yfinance

    if not lista_empresas:
        st.error("Nenhuma empresa com dados válidos.")
        return

    # ------------------------------------------------------------------ PREÇOS
    precos      = baixar_precos(tickers_pt_br)
    precos_mens = precos.resample("M").last()
    momentum    = calc_momentum_12m(precos)

    # ---------------------------------------------------------- SCORE / LÍDERES
    setores_map = dict(zip(empresas["ticker"], empresas["SETOR"]))
    pesos = PESOS_POR_SETOR.get(setor, INDICADORES_SCORE_GENERICO)

    df_scores = calcular_score_acumulado(
        lista_empresas,
        setores_map,
        pesos,
        dados_macro,
        momentum,
        anos_minimos=4,
    )
    df_scores = penalizar_plato(df_scores, precos_mens, meses=18, penal=0.25)
    lideres   = determinar_lideres(df_scores)

    # ----------------------------------------------------------------- CARTEIRAS
    dividendos = coletar_dividendos(empresas["ticker"].tolist())

    patrimonio, datas_ap = gerir_carteira(
        precos, df_scores, lideres, dividendos
    )
    patrimonio_selic = calcular_patrimonio_selic_macro(dados_macro, datas_ap)
    patrimonio_all   = gerir_carteira_todas_empresas(
        precos, empresas["ticker"], datas_ap, dividendos
    )

    patrimonio_total = pd.concat(
        [patrimonio, patrimonio_all, patrimonio_selic], axis=1
    )

    # ------------------------------------------------------------------- GRÁFICO
    # ---------------------------------------------------------------------
    # 📌 PLOTAGEM DO GRÁFICO DE EVOLUÇÃO DO PATRIMÔNIO
    # ---------------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Garantir que o índice está em datetime e ordenado
    df_patrimonio_evolucao = patrimonio_final.copy()
    df_patrimonio_evolucao.index = pd.to_datetime(df_patrimonio_evolucao.index,
                                                  errors="coerce")
    df_patrimonio_evolucao = df_patrimonio_evolucao.sort_index()
    
    if df_patrimonio_evolucao.empty:
        st.warning("⚠️ Dados insuficientes para plotar a evolução do patrimônio.")
    else:
        for col in df_patrimonio_evolucao.columns:
            if col == "Patrimônio":               # estratégia principal
                df_patrimonio_evolucao[col].plot(
                    ax=ax, linewidth=2, color="red", label="Estratégia de Aporte"
                )
            elif col == "Tesouro Selic":          # benchmark
                df_patrimonio_evolucao[col].plot(
                    ax=ax, linewidth=2, linestyle="-.", color="blue",
                    label="Tesouro Selic"
                )
            else:                                 # restantes
                df_patrimonio_evolucao[col].plot(
                    ax=ax, linewidth=1, linestyle="--", alpha=0.6,
                    color="gray", label=col
                )
    
        ax.set_title("Evolução do Patrimônio Acumulado")
        ax.set_xlabel("Data")
        ax.set_ylabel("Patrimônio (R$)")
        ax.legend()
    
        st.pyplot(fig)
    
    st.markdown("---")
    st.markdown("<div style='margin: 30px;'></div>", unsafe_allow_html=True)


    # ------------------------------------------------------ QUADROS DE RESULTADO
    st.markdown("---")
    st.subheader("📊 Patrimônio final (R$ 1 000/mês)")

    final = (
        patrimonio_total.iloc[-1]
        .rename_axis("Ticker")
        .reset_index(name="Valor")
        .sort_values("Valor", ascending=False)
    )
    líderes_contagem = lideres["ticker"].value_counts().to_dict()

    cols = st.columns(3)
    for i, (tk, val) in enumerate(zip(final["Ticker"], final["Valor"])):
        with cols[i % 3]:
            if tk == "Patrimônio":
                icon = "https://cdn-icons-png.flaticon.com/512/1019/1019709.png"
                borda = "#DAA520"; nome = "Estratégia"
            elif tk == "Tesouro Selic":
                icon = "https://cdn-icons-png.flaticon.com/512/2331/2331949.png"
                borda = "#007bff"; nome = "Tesouro Selic"
            else:
                icon = get_logo_url(tk)
                borda = "#d3d3d3"; nome = tk
            vezes = líderes_contagem.get(tk, 0)
            lider_txt = f"🏆 {vezes}× líder" if vezes else ""
            st.markdown(
                f"""
                <div style="background:#fff;border:3px solid {borda};border-radius:10px;
                            text-align:center;padding:15px;margin:8px">
                    <img src="{icon}" width="55"><br>
                    <b>{nome}</b><br>
                    <span style="color:#2ecc71;font-size:18px">{formatar_real(val)}</span><br>
                    <span style="color:#FFA500;font-size:14px">{lider_txt}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # ---------------------------------------------------- GRÁFICOS COMPARATIVOS
    st.markdown("---")
    st.subheader("Comparação de Múltiplos")

    indicadores_disp = {
        "Margem Líquida":          "Margem_Liquida",
        "Margem Operacional":      "Margem_Operacional",
        "ROE":                     "ROE",
        "ROIC":                    "ROIC",
        "P/L":                     "P/L",
        "Liquidez Corrente":       "Liquidez_Corrente",
        "Alavancagem Financeira":  "Alavancagem_Financeira",
        "Endividamento Total":     "Endividamento_Total",
    }

    empresas_plot = st.multiselect(
        "Empresas a exibir:",
        empresas["nome_empresa"].tolist(),
        default=empresas["nome_empresa"].tolist(),
    )
    ind_nome = st.selectbox("Indicador:", list(indicadores_disp.keys()))
    ind_col  = indicadores_disp[ind_nome]
    normal   = st.checkbox("Normalizar (0‑1)", value=False)

    df_mult_hist = []
    for _, r in empresas.iterrows():
        if r["nome_empresa"] not in empresas_plot:
            continue
        m = load_multiplos_from_db(r["ticker"] + ".SA")
        if m is None or m.empty or ind_col not in m:
            continue
        tmp = m[["Data", ind_col]].copy()
        tmp["Ano"]    = pd.to_datetime(tmp["Data"]).dt.year
        tmp["Empresa"] = r["nome_empresa"]
        df_mult_hist.append(tmp)
    if df_mult_hist:
        df_hist = pd.concat(df_mult_hist)
        if normal:
            vmin, vmax = df_hist[ind_col].min(), df_hist[ind_col].max()
            df_hist[ind_col] = (df_hist[ind_col] - vmin) / (vmax - vmin)
        fig = px.bar(
            df_hist,
            x="Ano", y=ind_col, color="Empresa",
            barmode="group",
            title=f"Evolução histórica – {ind_nome}"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Dados de múltiplos indisponíveis para os filtros escolhidos.")

    # -------------------------------------------------- DEMONSTRAÇÕES FINANCEIRAS
    st.markdown("---")
    st.subheader("Comparação de Demonstrações Financeiras")

    map_dre = {
        "Receita Líquida":     "Receita_Liquida",
        "EBIT":                "EBIT",
        "Lucro Líquido":       "Lucro_Liquido",
        "Patrimônio Líquido":  "Patrimonio_Liquido",
        "Dívida Líquida":      "Divida_Liquida",
        "Caixa Líquido":       "Caixa_Liquido",
    }
    ind_dre_nome = st.selectbox("Indicador DRE:", list(map_dre.keys()))
    ind_dre_col  = map_dre[ind_dre_nome]

    dre_hist = []
    for _, r in empresas.iterrows():
        if r["nome_empresa"] not in empresas_plot:
            continue
        dre = load_data_from_db(r["ticker"] + ".SA")
        if dre is None or dre.empty or ind_dre_col not in dre:
            continue
        tmp = dre[["Data", ind_dre_col]].copy()
        tmp["Ano"] = pd.to_datetime(tmp["Data"]).dt.year
        tmp["Empresa"] = r["nome_empresa"]
        dre_hist.append(tmp)
    if dre_hist:
        df_dre_hist = pd.concat(dre_hist)
        fig = px.bar(
            df_dre_hist, x="Ano", y=ind_dre_col, color="Empresa",
            barmode="group", title=f"Evolução – {ind_dre_nome}"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Indicador não disponível para as empresas escolhidas.")
