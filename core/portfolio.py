from __future__ import annotations

import pandas as pd
import numpy as np
import streamlit as st

# ──────────────────────────────────────────────────────

# Função para encontrar a próxima data disponível para aporte sem cair em datas onde o mercado está fechado ____________________________________________________________________________________ 
def encontrar_proxima_data_valida(data_aporte, precos):
    """
    Encontra a próxima data disponível para aporte no DataFrame de preços.
    Se a data não existir, pega o próximo dia disponível.
    """
    while data_aporte not in precos.index:
        data_aporte += pd.Timedelta(days=1)  # Avança um dia
        if data_aporte > precos.index.max():  # Evita sair do intervalo dos dados
            return None
    return data_aporte

# ────────────────────────────────────────────────────
# Carteira simulada com aporte fixo mensal igual para todas as empresas.
# ────────────────────────────────────────────────────
def gerir_carteira_simples(precos, tickers, datas_aportes, dividendos_dict=None, aporte_mensal=1000):
    """
    Realiza aportes mensais simples em todas as empresas fornecidas (tickers),
    somando dividendos distribuídos ao valor do aporte quando existirem.
    O valor da carteira é calculado com base apenas nas ações adquiridas até a data.
    """
    carteira = {ticker: 0 for ticker in tickers}
    patrimonio_aporte = pd.Series(index=precos.index, dtype=float)
    carteira_hist = {}  # snapshots da carteira após cada aporte

    for data_aporte in datas_aportes:
        if data_aporte not in precos.index:
            data_proxima = precos.index[precos.index >= data_aporte]
            if not data_proxima.empty:
                data_aporte = data_proxima[0]
            else:
                continue

        for ticker in tickers:
            if ticker in precos.columns and pd.notna(precos.loc[data_aporte, ticker]) and precos.loc[data_aporte, ticker] > 0:
                preco_acao = precos.loc[data_aporte, ticker]
                aporte_total = aporte_mensal / len(tickers)

                # Adiciona dividendos ao aporte
                if dividendos_dict and ticker in dividendos_dict:
                    dividendos_df = dividendos_dict[ticker]
                    dividendos_df.index = pd.to_datetime(dividendos_df.index)
                    dividendos_mes = dividendos_df[
                        (dividendos_df.index.year == data_aporte.year) &
                        (dividendos_df.index.month == data_aporte.month)
                    ].sum().values[0] if not dividendos_df.empty else 0

                    dividendos_recebidos = dividendos_mes * carteira[ticker]
                    aporte_total += dividendos_recebidos

                carteira[ticker] += aporte_total / preco_acao

        # snapshot da carteira após esse aporte
        carteira_hist[data_aporte] = carteira.copy()

    # Calcular o valor diário da carteira usando apenas os aportes até o dia
    for data in precos.index:
        carteira_dia = None
        for d in reversed(datas_aportes):
            if d <= data:
                carteira_dia = carteira_hist.get(d)
                break

        if carteira_dia:
            patrimonio_aporte.loc[data] = sum(
                carteira_dia[ticker] * precos.loc[data, ticker]
                for ticker in tickers
                if ticker in precos.columns and pd.notna(precos.loc[data, ticker])
            )
            
    return patrimonio_aporte.ffill()

 # Função para gerir o aporte mensal de todas as empresas do segmento sem estratégia 
def gerir_carteira_todas_empresas(precos, tickers, datas_aportes, dividendos_dict, aporte_mensal=1000):
    """
    Realiza aportes mensais em todas as empresas filtradas e reinveste dividendos pagos no respectivo mês.
    
    - `precos`: DataFrame com os preços históricos das empresas.
    - `tickers`: Lista dos tickers das empresas no portfólio.
    - `datas_aportes`: Lista de datas válidas para os aportes mensais.
    - `dividendos_dict`: Dicionário contendo o histórico de dividendos de cada empresa.
    - `aporte_mensal`: Valor investido em cada empresa a cada mês.

    Retorna:
    - `df_patrimonio_empresas`: DataFrame com a evolução do patrimônio de cada empresa ao longo do tempo.
    """
    patrimonio = {ticker: {} for ticker in tickers}
    carteira = {ticker: 0 for ticker in tickers}

    # Converter índice de preços para datetime (se ainda não estiver)
    precos.index = pd.to_datetime(precos.index)

    for data_aporte in datas_aportes:
        # Encontrar a data mais próxima disponível no DataFrame de preços
        if data_aporte not in precos.index:
            data_proxima = precos.index[precos.index >= data_aporte]
            if not data_proxima.empty:
                data_aporte = data_proxima[0]
            else:
                continue  # Se não houver preços disponíveis, pula o mês

        for ticker in tickers:
            if ticker not in precos.columns:
                continue  # Se o ticker não existir nos preços, ignora

            preco_atual = precos.loc[data_aporte, ticker]
            if pd.isna(preco_atual) or preco_atual == 0:
                continue  # Se o preço estiver vazio ou for zero, pula

            # Verificar dividendos pagos no mês e somar ao aporte mensal
            dividendos_mes = 0
            if ticker in dividendos_dict:
                dividendos_df = dividendos_dict[ticker]
                dividendos_df.index = pd.to_datetime(dividendos_df.index)  # Garantir formato datetime
                dividendos_ano_mes = dividendos_df[
                    (dividendos_df.index.year == data_aporte.year) &
                    (dividendos_df.index.month == data_aporte.month)
                ].sum()

                # Calcular dividendos recebidos com base na quantidade de ações na carteira
                dividendos_mes = dividendos_ano_mes * carteira.get(ticker, 0)

            # Somar dividendos ao aporte mensal
            aporte_total = aporte_mensal + dividendos_mes

            # Comprar fração de ações com o total disponível
            carteira[ticker] += aporte_total / preco_atual

            # Atualizar o valor do patrimônio da empresa
            patrimonio[ticker][data_aporte] = carteira[ticker] * preco_atual

    # Converter o dicionário em DataFrame para facilitar análise e plotagem
    df_patrimonio_empresas = pd.DataFrame.from_dict(patrimonio, orient='columns')

    # Ordenar por data
    df_patrimonio_empresas.sort_index(inplace=True)

    return df_patrimonio_empresas


# 📌 Função para calcular o patrimônio acumulado no Tesouro Selic ________________________________________________________________________________________________________________________
def calcular_patrimonio_selic_macro(dados_macro, datas_aportes, aporte_mensal=1000):
    """
    Corrige o cálculo da evolução do patrimônio investido no Tesouro Selic.
    """
    # Garantir que 'Data' seja uma coluna e que o índice não tenha absorvido
    if 'Data' not in dados_macro.columns and dados_macro.index.name == 'Data':
        dados_macro = dados_macro.reset_index()
    # Garantir que a coluna "Data" seja datetime e definir como índice
    dados_macro["Data"] = pd.to_datetime(dados_macro["Data"], errors='coerce')
    dados_macro.set_index("Data", inplace=True)

         
    # Criar DataFrame para armazenar os valores acumulados
    df_patrimonio = pd.DataFrame(index=datas_aportes, columns=["Tesouro Selic"])
    
    # Armazena o saldo total acumulado
    saldo = 0  

    for data in datas_aportes:
        ano_ref = pd.to_datetime(data).year  # Obter o ano do aporte
        
        # Obter taxa Selic anual
        taxa_anual = dados_macro.loc[dados_macro.index.year == ano_ref, "Selic"]
        if taxa_anual.empty:
            continue
        
        taxa_anual = taxa_anual.iloc[0] / 100  # Converter para decimal
        taxa_mensal = (1 + taxa_anual) ** (1/12) - 1  # Transformar em taxa mensal
        st.write("Taxa mensal Selic", taxa_mensal)
        
        
        
        # Aplicação do aporte
        saldo = (saldo + aporte_mensal) * (1 + taxa_mensal)  # Crescimento correto
        
        # Armazenar o saldo acumulado
        df_patrimonio.loc[data] = saldo

    # Ordenar o DataFrame corretamente
    df_patrimonio.sort_index(inplace=True)
    st.markdown("Patrimônio Selic")
    st.dataframe(df_patrimonio)

    return df_patrimonio


# Função responsável por criar a estratégia de comprar empresas Líderes do segmento e vender empresas com deterioração de fundamentos _____________________________________________________________ 
def gerir_carteira(
    precos,
    df_scores,
    lideres_por_ano,
    dividendos_dict,
    aporte_mensal=1000,
    deterioracao_limite=0.0,
    registrar_eventos=False  # novo argumento opcional
):
    from collections import defaultdict

    carteira = defaultdict(float)
    aporte_acumulado = 0.0
    registros = []
    eventos = [] if registrar_eventos else None
    lideres_atuais = []

    anos = sorted(df_scores['Ano'].unique())
    if not anos:
        return pd.DataFrame(), []

    # 🔹 Datas válidas de aporte baseadas na primeira ação
    datas_aportes = []
    for ano in anos:
        for mes in range(1, 13):
            data_nominal = pd.Timestamp(f"{ano + 1}-{mes:02d}-01")
            data_valida = precos.index[precos.index >= data_nominal]
            if not data_valida.empty:
                datas_aportes.append(data_valida[0])
    datas_aportes = sorted(set(datas_aportes))

    for ano in anos:
        empresa_lider = lideres_por_ano.query("Ano == @ano")['ticker'].iloc[0]
        if empresa_lider not in lideres_atuais:
            lideres_atuais.append(empresa_lider)
            if registrar_eventos:
                eventos.append({'data': f"{ano}", 'tipo': 'entrada', 'ticker': empresa_lider})

    for data_sinal in datas_aportes:
        # Reinvestimento de dividendos
        for empresa in list(carteira):
            if empresa in dividendos_dict:
                df_div = dividendos_dict[empresa]
                if not df_div.empty:
                    df_div.index = pd.to_datetime(df_div.index)
                    dividendos_mes = df_div[
                        (df_div.index.year == data_sinal.year) &
                        (df_div.index.month == data_sinal.month)
                    ].sum()
                    preco_empresa = precos.loc[data_sinal, empresa] if data_sinal in precos.index else None
                    if preco_empresa and preco_empresa > 0:
                        valor_reinvestido = dividendos_mes * carteira[empresa]
                        carteira[empresa] += valor_reinvestido / preco_empresa

        total_a_aportar = aporte_mensal + aporte_acumulado
        aporte_acumulado = 0.0

        if lideres_atuais:
            aporte_por_empresa = total_a_aportar / len(lideres_atuais)
            for lider in lideres_atuais:
                preco_lider = precos.loc[data_sinal, lider] if lider in precos.columns and data_sinal in precos.index else None
                if preco_lider is None or pd.isna(preco_lider) or preco_lider <= 0:
                    aporte_acumulado += aporte_por_empresa
                    continue
                carteira[lider] += aporte_por_empresa / preco_lider

        for antiga in list(carteira):
            if antiga in lideres_atuais:
                continue
            score_ini = df_scores.query("Ano == @anos[0] and ticker == @antiga")['Score_Ajustado']
            score_atual = df_scores.query("Ano == @data_sinal.year - 1 and ticker == @antiga")['Score_Ajustado']
            if score_ini.empty or score_atual.empty:
                continue
            if score_atual.values[0] / score_ini.values[0] < deterioracao_limite:
                preco_venda = precos.loc[data_sinal, antiga] if antiga in precos.columns and data_sinal in precos.index else None
                preco_lider = precos.loc[data_sinal, empresa_lider] if empresa_lider in precos.columns and data_sinal in precos.index else None
                if preco_venda and preco_lider and preco_lider > 0:
                    carteira[empresa_lider] += (carteira.pop(antiga) * preco_venda / preco_lider)
                    if registrar_eventos:
                        eventos.append({'data': data_sinal.strftime("%Y-%m"), 'tipo': 'saida', 'ticker': antiga})

        registro = {'date': data_sinal}
        total = 0.0
        for tk, qtd in carteira.items():
            if tk in precos.columns and data_sinal in precos.index:
                val = qtd * precos.loc[data_sinal, tk]
                registro[tk] = val
                total += val
        registro['Patrimônio'] = total
        registros.append(registro)

    if not registros:
        return pd.DataFrame(), []

    df_patrimonio = (
        pd.DataFrame(registros)
        .set_index('date')
        .sort_index()
        .fillna(method='ffill')
    )
    df_patrimonio = df_patrimonio[(df_patrimonio != 0).any(axis=1)]

    if registrar_eventos:
        return df_patrimonio, datas_aportes, eventos

    return df_patrimonio, datas_aportes
