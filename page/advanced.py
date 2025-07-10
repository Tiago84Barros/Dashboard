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

    # Cria o sidebar com as opções de filtro a esquerda ---------------------------------------------------------------------------------------
    with st.sidebar:
        setor = st.selectbox("Setor:", sorted(setores["SETOR"].dropna().unique()))
        subsetores = setores.loc[setores["SETOR"] == setor, "SUBSETOR"].dropna().unique()
        subsetor = st.selectbox("Subsetor:", sorted(subsetores))
        segmentos = setores.loc[(setores["SETOR"] == setor) & (setores["SUBSETOR"] == subsetor), "SEGMENTO"].dropna().unique()
        segmento = st.selectbox("Segmento:", sorted(segmentos))
        tipo = st.radio("Perfil de empresa:", ["Crescimento (<10 anos)", "Estabelecida (≥10 anos)", "Todas"], index=2)

    # Carrega as empresas do filtro -----------------------------------------------------------------------------------------------------------
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

    # Carrega as empresas selecionadas em blocos ------------------------------------------------------------------------------------------------------------
    st.subheader("Empresas Selecionadas") 

    colunas_layout = st.columns(3)  # Ajuste o número de colunas conforme necessário
                    
    for idx, row in enumerate(empresas.itertuples()):
        col = colunas_layout[idx % len(colunas_layout)]  # Distribui os blocos entre as colunas
        with col:
            # Obter URL do logo da empresa (você pode modificar essa função conforme sua necessidade)
            logo_url = get_logo_url(row.ticker)  # Certifique-se de que essa função existe e retorna a URL correta

            # Criar bloco estilizado para cada empresa
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
                    <h4 style="color: #333;">{row.nome_empresa} ({row.ticker})</h4>
                 
                </div>
                """,
                unsafe_allow_html=True
            )
    
    # Carrega as variáveis das empresas selecionadas -------------------------------------------------------------------------------------------------------
    lista_empresas = []
    for _, r in empresas.iterrows():
        tk_full = f"{r['ticker']}.SA"
        mult = load_multiplos_from_db(tk_full)
        dre = load_data_from_db(tk_full)
        
        if mult is None or mult.empty or dre is None or dre.empty:
            continue
        mult['Ano'] = pd.to_datetime(mult['Data'], errors='coerce').dt.year
        dre['Ano'] = pd.to_datetime(dre['Data'], errors='coerce').dt.year
        lista_empresas.append({"ticker": r["ticker"], "nome": r["nome_empresa"], "multiplos": mult, "dre": dre})

    if not lista_empresas:
        st.error("Não foi possível carregar dados financeiros para as empresas.")
        return

    # Setores das empresas -----------------------------------------------------------------------------------------------
    setores_empresa = {e["ticker"]: obter_setor_da_empresa(e["ticker"], setores) for e in lista_empresas}
   
    # Pesos por setor ----------------------------------------------------------------------------------------------------
    pesos_utilizados = get_pesos(setor)

    # SCORE das empresas -------------------------------------------------------------------------------------------------
    score = calcular_score_acumulado(lista_empresas, setores_empresa, pesos_utilizados, dados_macro, anos_minimos=4)

    #precos = baixar_precos([e['ticker'] for e in lista_empresas])
    precos = baixar_precos([e['ticker'] + ".SA" for e in lista_empresas])

    #precos.index = pd.to_datetime(precos.index)
    precos_mensal = precos.resample('M').last()     # ⇢ último pregão do mês

    # Penalização do platô de preços -----------------------------------------------------------------------------------------
    score = penalizar_plato(score,  precos_mensal,  meses= 12, penal=0.5)        # 18 meses e –25 % no score quando perde da mediana)

    # Determina as líderes dependendo do score encontrado ----------------------------------------------------------------------
    lideres = determinar_lideres(score)
  
    # 🔹 Lista de tickers das empresas que estamos analisando ------------------------------------------------------------------
    tickers_filtrados = score['ticker'].unique()
    
    # 🔹 Baixar todos os dividendos de uma vez só ------------------------------------------------------------------------------
    dividendos = coletar_dividendos(tickers_filtrados)  

    # Gerenciamento da carteira ------------------------------------------------------------------------------------------------
    patrimonio_estrategia, datas_aportes = gerir_carteira(precos, score, lideres, dividendos)
    patrimonio_estrategia = patrimonio_estrategia[["Patrimônio"]]

    # Comparação com Tesouro Selic a partir da mesma data ----------------------------------------------------------------------
    patrimonio_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aportes)
                             
    # Gerir carteira para todas as empresas usando a mesma data de início ------------------------------------------------------
    patrimonio_empresas = gerir_carteira_todas_empresas(precos, tickers_filtrados, datas_aportes, dividendos)              
    
    # Combinar os resultados para exibição no gráfico --------------------------------------------------------------------------
    patrimonio_final = pd.concat([patrimonio_estrategia, patrimonio_empresas, patrimonio_selic], axis=1)

    # 📌 Verificar se df_scores não está vazio antes de tentar acessar a empresa líder
    if score.empty:
        st.error("⚠️ Não há dados suficientes para determinar a empresa líder.")
        lider = None
    else:
        # Determinar a empresa líder mais recente
        lider = score.sort_values("Ano", ascending=False).iloc[0]
        

    # Inserindo espaçamento entre os elementos
    st.markdown("---") # Espaçamento entre diferentes tipos de análise
    st.markdown("<div style='margin: 30px;'></div>", unsafe_allow_html=True)

    # Mostrar resultado final =========================================== GRÁFICO COMPARATIVO ESTRATÉGIA LIDER VS CONCORRENTES VS TESOURO SELIC ===================================               
    # 📌 PLOTAGEM DO GRÁFICO DE EVOLUÇÃO DO PATRIMÔNIO =======================================================================================================
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Garantir que os dados estão ordenados corretamente
    df_patrimonio_evolucao = patrimonio_final.copy()
  
    df_patrimonio_evolucao.index = pd.to_datetime(df_patrimonio_evolucao.index, errors='coerce')
    df_patrimonio_evolucao = df_patrimonio_evolucao.sort_index()
                     
    # Se não houver dados, exibir aviso
    if df_patrimonio_evolucao.empty:
        st.warning("⚠️ Dados insuficientes para plotar a evolução do patrimônio.")
    else:
        for ticker in df_patrimonio_evolucao.columns:
            if ticker == "Patrimônio":  # Destacando a estratégia principal
                df_patrimonio_evolucao[ticker].plot(ax=ax, linewidth=2, color="red", label="Estratégia de Aporte")
            elif ticker == "Tesouro Selic":
                df_patrimonio_evolucao[ticker].plot(ax=ax, linewidth=2, linestyle="-.", color="blue", label="Tesouro Selic")
            else:
                df_patrimonio_evolucao[ticker].plot(ax=ax, linewidth=1, linestyle="--", alpha=0.6, color="gray", label=ticker)
    
        # Melhorias no gráfico
        ax.set_title("Evolução do Patrimônio Acumulado")
        ax.set_xlabel("Data")
        ax.set_ylabel("Patrimônio (R$)")
        ax.legend()
    
        # Exibir gráfico no Streamlit
        st.pyplot(fig)

    # Inserindo espaçamento entre os elementos
    st.markdown("---") # Espaçamento entre diferentes tipos de análise
    st.markdown("<div style='margin: 30px;'></div>", unsafe_allow_html=True)
    

    # 📌 EXIBIÇÃO DOS QUADRADOS (BLOCOS COM OS RESULTADOS) ====================================================================================================================
    st.subheader("📊 Patrimônio Final para R$1.000/Mês Investidos desde a Data Inicial")
    
    # 🔹 Criar um DataFrame consolidado com os resultados finais das empresas, estratégia e Tesouro Selic
    df_patrimonio_final = pd.concat([
        patrimonio_estrategia.iloc[-1:].rename_axis("Data").reset_index().melt(id_vars="Data", var_name="index", value_name="Patrimônio Final"),
        patrimonio_empresas.iloc[-1:].rename_axis("Data").reset_index().melt(id_vars="Data", var_name="index", value_name="Patrimônio Final"),
        patrimonio_selic.iloc[-1:].rename_axis("Data").reset_index().melt(id_vars="Data", var_name="index", value_name="Patrimônio Final")
    ], ignore_index=True)
    
    # 📌 Verificação do formato
    if df_patrimonio_final.empty:
        st.warning("⚠️ Dados insuficientes para exibir o patrimônio final.")
        st.stop()  # Interrompe a execução para evitar erro
    
    # 🔹 Garantir que "Tesouro Selic" esteja presente no DataFrame
    if "Tesouro Selic" not in df_patrimonio_final["index"].values:
        patrimonio_selic_final = patrimonio_selic.iloc[-1]["Tesouro Selic"]  # Último valor acumulado do Tesouro Selic
        df_patrimonio_final = pd.concat([
            df_patrimonio_final,
            pd.DataFrame([{"index": "Tesouro Selic", "Patrimônio Final": patrimonio_selic_final}])
        ], ignore_index=True)
                     
    # 🔹 Garantir que o índice esteja resetado corretamente
    if df_patrimonio_final.index.name is not None:
        df_patrimonio_final = df_patrimonio_final.reset_index()
                                      
    # 🔹 Ajustar nomes de colunas, se necessário
    if "index" in df_patrimonio_final.columns and "Patrimônio Final" in df_patrimonio_final.columns:
        df_patrimonio_final.rename(columns={"index": "Ticker", "Patrimônio Final": "Valor Final"}, inplace=True)
    
    # 🔹 Ordenar os valores acumulados em ordem decrescente
    if "Valor Final" in df_patrimonio_final.columns:
        df_patrimonio_final = df_patrimonio_final.sort_values(by="Valor Final", ascending=False)
    else:
        st.error("Coluna 'Valor Final' não encontrada!")
    
    # 🔹 Criar colunas para exibição no Streamlit
    num_columns = 3  # Número de colunas no layout
    columns = st.columns(num_columns)
    
    # 🔹 Contar quantas vezes cada empresa foi líder no score
    #contagem_lideres = df_scores['ticker'].value_counts().to_dict()
    contagem_lideres = lideres['ticker'].value_counts().to_dict()
    
    # 🔹 Iterar sobre os valores do DataFrame ordenado
    for i, (index, row) in enumerate(df_patrimonio_final.iterrows()):
        ticker = row["Ticker"]
        patrimonio = row["Valor Final"]
    
        # 🔹 Definir borda dourada apenas para a estratégia de aporte
        if ticker == "Patrimônio":
            icone_url = "https://cdn-icons-png.flaticon.com/512/1019/1019709.png"
            border_color = "#DAA520"  # Dourado para a estratégia
            nome_exibicao = "Estratégia de Aporte"
        elif ticker == "Tesouro Selic":
            icone_url = "https://cdn-icons-png.flaticon.com/512/2331/2331949.png"
            border_color = "#007bff"  # Azul para Tesouro Selic
            nome_exibicao = "Tesouro Selic"
        else:
            icone_url = get_logo_url(ticker)
            border_color = "#d3d3d3"  # Cinza para empresas comuns
            nome_exibicao = ticker  # Nome normal para empresas comuns
    
        # 🔹 Contagem de quantas vezes uma empresa foi líder
        vezes_lider = contagem_lideres.get(ticker, 0)
        lider_texto = f"🏆 {vezes_lider}x Líder" if vezes_lider > 0 else ""
    
        # 🔹 Formatar patrimônio
        patrimonio_formatado = "Valor indisponível" if pd.isna(patrimonio) else formatar_real(patrimonio)
    
        # 🔹 Organizar os blocos corretamente
        col = columns[i % num_columns]
        with col:
            st.markdown(f"""
                <div style="
                    background-color: #ffffff;
                    border: 3px solid {border_color};
                    border-radius: 10px;
                    padding: 15px;
                    margin: 10px;
                    text-align: center;
                    box-shadow: 2px 2px 5px rgba(0, 0, 0, 0.1);
                    flex: 1;
                ">
                    <img src="{icone_url}" alt="{nome_exibicao}" style="width: 50px; height: auto; margin-bottom: 5px;">
                    <h3 style="margin: 0; color: #4a4a4a;">{nome_exibicao}</h3>
                    <p style="font-size: 18px; margin: 5px 0; font-weight: bold; color: #2ecc71;">
                        {patrimonio_formatado}
                    </p>
                    <p style="font-size: 14px; color: #FFA500;">{lider_texto}</p>
                </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---") # Espaçamento entre diferentes tipos de análise
    st.markdown("<div style='margin: 30px;'></div>", unsafe_allow_html=True)
            
    st.markdown("### Comparação de Indicadores (Múltiplos) entre Empresas do Segmento") #______GRÁFICO DOS MÚLTIPLOS_____________________________________________________________________________________________
    
    # Lista de indicadores disponíveis
    indicadores_disponiveis = ["Margem Líquida", "Margem Operacional", "ROE", "ROIC", "P/L", "Liquidez Corrente", "Alavancagem Financeira", "Endividamento Total"]
    
    # Mapeamento de nomes amigáveis para nomes de colunas no banco
    nomes_to_col = {
        "Margem Líquida": "Margem_Liquida",
        "Margem Operacional": "Margem_Operacional",
        "ROE": "ROE",
        "ROIC": "ROIC",
        "P/L": "P/L",
        "Liquidez Corrente": "Liquidez_Corrente",
        "Alavancagem Financeira": "Alavancagem_Financeira",
        "Endividamento Total": "Endividamento_Total"
        
    }

     # Selecionar as empresas a exibir
    lista_empresas_ = [e['nome'] for e in lista_empresas]
    empresas_selecionadas = st.multiselect("Selecione as empresas a serem exibidas no gráfico:", lista_empresas_, default=lista_empresas_)
    
    # Selecionar o indicador a ser exibido
    indicador_selecionado = st.selectbox("Selecione o Indicador para Comparar:", indicadores_disponiveis, index=0)
    col_indicador = nomes_to_col[indicador_selecionado]
    
                      
    # Opção para normalizar os dados
    normalizar = st.checkbox("Normalizar os Indicadores (Escala de 0 a 1)", value=False)
    
    # Construir o DataFrame com o histórico completo de cada empresa selecionada
    df_historico = []
    for i, row in enumerate(lista_empresas):
        nome_emp = row['nome']
        if nome_emp in empresas_selecionadas:
            ticker = row['ticker']
            multiplos_data = load_multiplos_from_db(ticker + ".SA")
            if multiplos_data is not None and not multiplos_data.empty and col_indicador in multiplos_data.columns:
                # Processar os dados da empresa
                df_emp = multiplos_data[['Data', col_indicador]].copy()
                df_emp['Ano'] = pd.to_datetime(df_emp['Data'], errors='coerce').dt.year  # Extrair apenas o ano
                df_emp['Empresa'] = nome_emp
                df_historico.append(df_emp)
            else:
                st.info(f"Empresa {nome_emp} não possui dados para o indicador {indicador_selecionado}.")
    
    if len(df_historico) == 0:
        st.warning("Não há dados históricos disponíveis para as empresas selecionadas ou para o indicador escolhido.")
    else:
        # Concatenar os DataFrames em um único DataFrame
        df_historico = pd.concat(df_historico, ignore_index=True)
    
        # Remover entradas com anos nulos
        df_historico = df_historico.dropna(subset=['Ano'])
    
        # Normalizar os dados se a opção estiver marcada
        if normalizar:
            max_valor = df_historico[col_indicador].max()
            min_valor = df_historico[col_indicador].min()
            df_historico[col_indicador] = (df_historico[col_indicador] - min_valor) / (max_valor - min_valor)
    
        # Garantir que todos os anos presentes no conjunto de dados sejam exibidos no gráfico
        anos_disponiveis = sorted(df_historico['Ano'].unique())
        df_historico['Ano'] = df_historico['Ano'].astype(str)  # Converter para string para lidar com gaps no eixo
    
        # Criar o gráfico de barras
        fig = px.bar(
            df_historico,
            x='Ano',
            y=col_indicador,
            color='Empresa',
            barmode='group',
            title=f"Evolução Histórica de {indicador_selecionado} por Empresa"
        )
    
        # Ajustar layout do gráfico
        fig.update_layout(
            xaxis_title="Ano",
            yaxis_title=f"{indicador_selecionado} {'(Normalizado)' if normalizar else ''}",
            xaxis=dict(type='category', categoryorder='category ascending', tickvals=anos_disponiveis),
            legend_title="Empresa"
        )
    
        # Exibir o gráfico no Streamlit
        st.plotly_chart(fig, use_container_width=True)
    
        st.markdown("---") # Espaçamento entre diferentes tipos de análise
        st.markdown("<div style='margin: 30px;'></div>", unsafe_allow_html=True)
    
    # Seção: Gráfico Comparativo de Demonstrações Financeiras ________________GRÁFICO DAS DEMONSTRAÇÕES FINANCEIRAS__________________________________________________________________
    st.markdown("### Comparação de Demonstrações Financeiras entre Empresas")
    
    # 🔹 Converter lista de empresas para DataFrame completo
    empresas_completas_df = pd.DataFrame(lista_empresas)
    
    # 🔹 Selecionar empresas para exibir
    nomes_empresas_disponiveis = empresas_completas_df['nome'].tolist()
    empresas_selecionadas = st.multiselect(
        "Selecione as empresas para exibir:",
        nomes_empresas_disponiveis,
        default=nomes_empresas_disponiveis
    )
    
    # 🔹 Indicadores financeiros disponíveis
    indicadores_dre = {
        "Receita Líquida": "Receita_Liquida",
        "EBIT": "EBIT",
        "Lucro Líquido": "Lucro_Liquido",
        "Patrimônio Líquido": "Patrimonio_Liquido",
        "Dívida Líquida": "Divida_Liquida",
        "Caixa Líquido": "Caixa_Liquido"
    }
    
    # 🔹 Escolher o indicador
    indicador_display = st.selectbox("Escolha o Indicador:", list(indicadores_dre.keys()))
    coluna_indicador = indicadores_dre[indicador_display]
    
    # 🔹 Carregar os dados DRE comparativos
    def load_dre_comparativo(empresas_df, colunas_desejadas):
        dfs = []
        for _, row in empresas_df.iterrows():
            nome = row['nome']
            ticker = row['ticker']
            df = load_data_from_db(ticker + ".SA")
            if df is not None and not df.empty:
                df['Empresa'] = nome
                df['Ano'] = pd.to_datetime(df['Data'], errors='coerce').dt.year
                dfs.append(df)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    # 🔹 Filtrar empresas selecionadas
    empresas_filtradas_df = empresas_completas_df[empresas_completas_df['nome'].isin(empresas_selecionadas)]
    dre_df = load_dre_comparativo(empresas_filtradas_df, list(indicadores_dre.values()))
    
    if not dre_df.empty and coluna_indicador in dre_df.columns:
        df_plot = dre_df[['Ano', coluna_indicador, 'Empresa']].dropna()
        df_plot = df_plot.rename(columns={coluna_indicador: "Valor"})
        df_plot['Ano'] = df_plot['Ano'].astype(str)
    
        fig = px.bar(
            df_plot,
            x='Ano',
            y='Valor',
            color='Empresa',
            barmode='group',
            title=f"Comparação de {indicador_display} entre Empresas"
        )
        fig.update_layout(
            xaxis_title="Ano",
            yaxis_title=indicador_display,
            legend_title="Empresa",
            xaxis=dict(type='category')
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Não há dados suficientes para o indicador selecionado entre as empresas escolhidas.")
