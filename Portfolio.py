"""Portfolio.py
~~~~~~~~~~~~~~~~
Rotinas de gestão de carteira e benchmarks ligadas ao *Score* fundamentalista.

Funções públicas
----------------
- gerir_carteira                 → Estratégia "Líderes + deterioração".
- gerir_carteira_todas_empresas  → Aportes mensais em todas as empresas filtradas.
- calcular_patrimonio_selic_macro→ Benchmark Tesouro Selic.
- calcular_patrimonio_ibov       → Benchmark aportando R$ 1.000 no Ibovespa mensalmente.

Dependências: pandas, numpy.

Nota
----
A função `encontrar_proxima_data_valida` é referenciada por `gerir_carteira` e
não faz parte deste módulo original; incluímos um *stub* para evitar erros de
import. Substitua pelo seu utilitário real ou faça `from utils import ...`.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Dict, List, Sequence, Tuple

# ----------------------------------------------------------------------------
# Funções não utilizadas -----------------------------------------------------
# ----------------------------------------------------------------------------

'''
   # Calcula a Média Móvel Exponencial (EMA) para uma série de preços ____________________________________________________________________________________________________________________________-
    def calcular_ema(series, period=50):
        """
        Calcula a Média Móvel Exponencial (EMA) para uma série de preços.
        period: número de períodos usados no cálculo (padrão: 50)
        """
        return series.ewm(span=period, adjust=False).mean()

    # Função para Calcula o RSI (Relative Strength Index) com base na série de preço __________________________________________________________________________________________________________
    def calcular_rsi(series_precios, janela=14):
        """
        Calcula o RSI (Relative Strength Index) com base na série de preços.
        
        Parâmetros:
        - series_precios: Série de preços históricos da ação.
        - janela: Período para cálculo do RSI (padrão 14).
    
        Retorna:
        - Série com os valores do RSI calculados.
        """
        delta = series_precios.diff()
        ganho = (delta.where(delta > 0, 0)).rolling(window=janela, min_periods=1).mean()
        perda = (-delta.where(delta < 0, 0)).rolling(window=janela, min_periods=1).mean()
    
        rs = ganho / perda
        rsi = 100 - (100 / (1 + rs))
    
        return rsi
        
    # Função que utiliza análise técnica de médias móveis para determinar o melhor momento de compra da empresa Líder _______________________________________________________________________________    
    def validar_tendencia_entrada(ticker, precos, data_aporte_original, janela_rsi= 14, limite_rsi=40, ema_period=20):
        """
        Para o mês correspondente à data_aporte_original, essa função testa diariamente se os indicadores 
        técnicos indicam oportunidade de compra. Se encontrar um sinal favorável em algum dia do mês, retorna 
        esse dia (e seu preço). Se nenhum dia satisfizer os critérios, retorna um fallback, que será, por exemplo, 
        o primeiro dia de negociação válido do mês.
    
        Parâmetros:
          - ticker: Nome do ativo.
          - precos: DataFrame contendo os preços históricos (deve conter coluna com o ticker).
          - data_aporte_original: Data proposta para o aporte.
          - janela_rsi: Número de períodos para o cálculo do RSI (padrão: 14).
          - limite_rsi: Limite inferior para o RSI que indica sinal de compra (ex.: 30).
          - ema_period: Período para cálculo da EMA.
    
        Retorna:
          - (data_sinal, preco_sinal): onde data_sinal é a primeira data do mês em que os critérios são atendidos;
          - Se não houver sinal, retorna (fallback, preço_fallback), onde fallback é o primeiro dia válido do mês.
        """
        # Ajusta a data_aporte_original para uma data válida de negociação
        data_aporte_valid = encontrar_proxima_data_valida(data_aporte_original, precos)
        if data_aporte_valid is None or ticker not in precos.columns:
            return None, None
    
        # Define o mês a ser avaliado com base na data ajustada
        ano = data_aporte_valid.year
        mes = data_aporte_valid.month
        mes_inicio = pd.Timestamp(year=ano, month=mes, day=1)
        mes_fim = mes_inicio + pd.offsets.MonthEnd(0)
        
        # Seleciona os preços do ticker para todo o mês
        dados_mes = precos.loc[mes_inicio:mes_fim, ticker].dropna()
               
        # Se os dados do mês forem insuficientes, use o primeiro dia como fallback
        if len(dados_mes) < janela_rsi:
            fallback = dados_mes.index[0] if not dados_mes.empty else None
            return fallback, precos.loc[fallback, ticker] if fallback is not None else (None, None)
        
        # Percorre cada dia do mês e testa se os critérios técnicos são atendidos naquele dia
        for d in dados_mes.index:
            # Define uma janela que abrange os dados do mês até o dia 'd'
            window = dados_mes.loc[:d]
                       
            if len(window) < janela_rsi:
                continue  # Não há dados suficientes para calcular os indicadores
            rsi_val = calcular_rsi(window, janela=janela_rsi).iloc[-1]
            ema_val = calcular_ema(window, period=ema_period).iloc[-1]
            preco_val = window.iloc[-1]
                       
            # Testa os critérios: RSI <= limite e preço >= EMA
            if rsi_val <= limite_rsi and preco_val >= ema_val:
                st.markdown(f"O preço encontrado para compra é {preco_val} e a data é {d}")
                # Se encontrar, retorna essa data e o preço
                return d, preco_val
    
        # Se nenhum dia do mês satisfizer os critérios, use como fallback o primeiro dia de negociação válido do mês
        fallback = dados_mes.index[0]
        st.markdown(f"Nenhum dia do mês satisfez os critérios. Esse é o fallback é {fallback}")
        return fallback, None

    # Função responsável por determinar o melhor momento de venda da empresa que apresentou deterioração em seus fundamentos _____________________________________________________________________
    def validar_tendencia_saida(ticker, precos, data_aporte_original, janela_rsi=14, limite_rsi=60, ema_period=20):
        """
        Para o mês correspondente à data_aporte_original, essa função percorre dia a dia os
        dados de negociação e avalia se os indicadores técnicos indicam um bom momento para venda.
        
        Critério de venda (sinal favorável):
          - RSI >= limite_rsi (indicando sobrecompra)
          OU
          - Preço < EMA (sugerindo reversão de tendência)
        
        Parâmetros:
          - ticker: Nome do ativo.
          - precos: DataFrame contendo os preços históricos com uma coluna para o ticker.
          - data_aporte_original: Data proposta para iniciar a avaliação da saída.
          - janela_rsi: Número de períodos para o cálculo do RSI (padrão: 14).
          - limite_rsi: Limite superior do RSI que indica sinal de venda (por exemplo, 70).
          - ema_period: Período para cálculo da EMA.
        
        Retorna:
          - (data_sinal, preco_sinal): A primeira data do mês em que os critérios de venda são atendidos.
          - (None, None) se nenhum dia do mês apresentar sinal favorável.
        """
        # Ajusta a data de venda para um dia válido de negociação
        data_sell_valid = encontrar_proxima_data_valida(data_aporte_original, precos)
        if data_sell_valid is None or ticker not in precos.columns:
            return None, None
    
        # Define o período do mês: desde o primeiro dia até o último dia do mês
        ano = data_sell_valid.year
        mes = data_sell_valid.mont
        mes_inicio = pd.Timestamp(year=ano, month=mes, day=1)
        mes_fim = mes_inicio + pd.offsets.MonthEnd(0)
        
        # Seleciona os preços do ticker para todo o mês
        dados_mes = precos.loc[mes_inicio:mes_fim, ticker].dropna()
                
        if len(dados_mes) < janela_rsi:
            return None, None  # Dados insuficientes para cálculo do RSI
        
        # Percorre cada dia do mês para verificar a condição de venda
        for d in dados_mes.index:
            # Define a janela de dados: do início do mês até o dia 'd'
            janela = dados_mes.loc[:d]
            if len(janela) < janela_rsi:
                continue  # Dados insuficientes para calcular o RSI
            rsi_val = calcular_rsi(janela, janela=janela_rsi).iloc[-1]
            ema_val = calcular_ema(janela, period=ema_period).iloc[-1]
            preco_val = janela.iloc[-1]
                           
            # Se o RSI estiver acima do limite ou o preço cair abaixo da EMA, sinaliza venda
            if rsi_val >= limite_rsi or preco_val < ema_val:
                return d, preco_val
    
        # Se nenhum dia do mês apresenta sinal de venda, retorna (None, None)
        return None, None
'''

# ---------------------------------------------------------------------------
# Place‑holder utilitário ----------------------------------------------------
# ---------------------------------------------------------------------------

# Função para encontrar a próxima data disponível para aporte sem cair em datas onde o mercado está fechado ____________________________________________________________________________________ 
def encontrar_proxima_data_valida(data_aporte: pd.Timestamp, precos: pd.DataFrame) -> pd.Timestamp | None:
     """
        Encontra a próxima data disponível para aporte no DataFrame de preços.
        Se a data não existir, pega o próximo dia disponível.
        """
        while data_aporte not in precos.index:
            data_aporte += pd.Timedelta(days=1)  # Avança um dia
            if data_aporte > precos.index.max():  # Evita sair do intervalo dos dados
                return None
        return data_aporte

# ---------------------------------------------------------------------------
# 1. Estratégia líderes + deterioração --------------------------------------
# ---------------------------------------------------------------------------

def gerir_carteira(
    precos: pd.DataFrame,
    df_scores: pd.DataFrame,
    lideres_por_ano: pd.DataFrame,
    dividendos_dict: Dict[str, pd.Series | pd.DataFrame],
    aporte_mensal: float = 1000.0,
    deterioracao_limite: float = 0.0,
) -> Tuple[pd.DataFrame, List[pd.Timestamp]]:
    """Constrói carteira comprando líderes e vendendo empresas com deterioração.

    Retorna
    -------
    df_patrimonio : DataFrame (índice = datas, coluna "Patrimônio")
    datas_aportes : Lista de datas efetivas de aporte
    """
    from collections import defaultdict

    carteira: Dict[str, float] = defaultdict(float)
    aporte_acumulado = 0.0
    registros: List[Dict[str, float]] = []
    lideres_atuais: List[str] = []

    anos = sorted(df_scores['Ano'].unique())

    for ano in anos:
        empresa_lider = lideres_por_ano.query('Ano == @ano')['ticker'].iloc[0]
        if empresa_lider not in lideres_atuais:
            lideres_atuais.append(empresa_lider)

        for mes in range(1, 13):
            data_nominal = pd.Timestamp(f"{ano+1}-{mes:02d}-01")
            data_sinal = encontrar_proxima_data_valida(data_nominal, precos)
            if data_sinal is None:
                continue

            # --- Reinvestimento de dividendos ---------------------------------
            for empresa in list(carteira):
                if empresa not in dividendos_dict:
                    continue
                df_div = dividendos_dict[empresa]
                if df_div.empty:
                    continue
                df_div.index = pd.to_datetime(df_div.index)
                dividendos_mes = df_div[(df_div.index.year == data_sinal.year) &
                                         (df_div.index.month == data_sinal.month)].sum()
                preco_emp = precos.loc[data_sinal, empresa] if data_sinal in precos.index else np.nan
                if np.isfinite(preco_emp) and preco_emp > 0:
                    valor_reinvest = dividendos_mes * carteira[empresa]
                    carteira[empresa] += valor_reinvest / preco_emp

            # --- Aporte mensal dividido entre líderes atuais ------------------
            total_aporte = aporte_mensal + aporte_acumulado
            aporte_acumulado = 0.0
            if lideres_atuais:
                aporte_por_emp = total_aporte / len(lideres_atuais)
                for lider in lideres_atuais:
                    preco_lider = precos.loc[data_sinal, lider] if lider in precos.columns and data_sinal in precos.index else np.nan
                    if not np.isfinite(preco_lider) or preco_lider <= 0:
                        aporte_acumulado += aporte_por_emp
                        continue
                    carteira[lider] += aporte_por_emp / preco_lider

            # --- Deterioração de fundamentos ----------------------------------
            for antiga in list(carteira):
                if antiga in lideres_atuais:
                    continue
                score_ini = df_scores.query('Ano == @anos[0] and ticker == @antiga')['Score_Ajustado']
                score_atual = df_scores.query('Ano == @ano     and ticker == @antiga')['Score_Ajustado']
                if score_ini.empty or score_atual.empty:
                    continue
                if score_atual.values[0] / score_ini.values[0] < deterioracao_limite:
                    preco_venda = precos.loc[data_sinal, antiga] if antiga in precos.columns and data_sinal in precos.index else np.nan
                    preco_lider = precos.loc[data_sinal, empresa_lider] if empresa_lider in precos.columns and data_sinal in precos.index else np.nan
                    if np.isfinite(preco_venda) and preco_venda > 0 and np.isfinite(preco_lider) and preco_lider > 0:
                        carteira[empresa_lider] += (carteira.pop(antiga) * preco_venda / preco_lider)

            # --- Registro do patrimônio --------------------------------------
            total = 0.0
            for tk, qtd in carteira.items():
                if tk in precos.columns and data_sinal in precos.index:
                    total += qtd * precos.loc[data_sinal, tk]
            registros.append({'date': data_sinal, 'Patrimônio': total})

    df_patrimonio = pd.DataFrame(registros).set_index('date').sort_index()
    df_patrimonio = df_patrimonio[df_patrimonio['Patrimônio'] != 0].fillna(method='ffill')

    datas_aportes = df_patrimonio.index.unique().tolist()
    return df_patrimonio, datas_aportes

# ---------------------------------------------------------------------------
# 2. Aportes em todas as empresas do segmento --------------------------------
# ---------------------------------------------------------------------------

def gerir_carteira_todas_empresas(
    precos: pd.DataFrame,
    tickers: Sequence[str],
    datas_aportes: Sequence[pd.Timestamp],
    dividendos_dict: Dict[str, pd.Series | pd.DataFrame],
    aporte_mensal: float = 1000.0,
) -> pd.DataFrame:
    """Aportar mensalmente em todas as empresas selecionadas, reinvestindo dividendos."""
    patrimonio = {tk: {} for tk in tickers}
    carteira = {tk: 0.0 for tk in tickers}
    precos.index = pd.to_datetime(precos.index)

    for data in datas_aportes:
        if data not in precos.index:
            prox = precos.index[precos.index >= data]
            if prox.empty:
                continue
            data = prox[0]

        for tk in tickers:
            if tk not in precos.columns:
                continue
            preco_atual = precos.loc[data, tk]
            if not np.isfinite(preco_atual) or preco_atual <= 0:
                continue

            dividendos_mes = 0.0
            if tk in dividendos_dict and not dividendos_dict[tk].empty:
                df_div = dividendos_dict[tk]
                df_div.index = pd.to_datetime(df_div.index)
                dividendos_mes = df_div[(df_div.index.year == data.year) & (df_div.index.month == data.month)].sum()
                dividendos_mes *= carteira[tk]

            aporte_total = aporte_mensal + dividendos_mes
            carteira[tk] += aporte_total / preco_atual
            patrimonio[tk][data] = carteira[tk] * preco_atual

    df_patrimonio = pd.DataFrame.from_dict(patrimonio, orient='columns').sort_index()
    return df_patrimonio

# ---------------------------------------------------------------------------
# 3. Benchmark Tesouro Selic --------------------------------------------------
# ---------------------------------------------------------------------------

def calcular_patrimonio_selic_macro(
    dados_macro: pd.DataFrame,
    datas_aportes: Sequence[pd.Timestamp],
    aporte_mensal: float = 1000.0,
) -> pd.DataFrame:
    """Evolução do patrimônio investido no Tesouro Selic usando taxa anual."""
    dados_macro = dados_macro.copy()
    dados_macro['Data'] = pd.to_datetime(dados_macro['Data'], errors='coerce')
    dados_macro.set_index('Data', inplace=True)

    df_patrimonio = pd.DataFrame(index=datas_aportes, columns=['Tesouro Selic'])
    saldo = 0.0
    for data in datas_aportes:
        ano = data.year
        taxa_row = dados_macro.loc[dados_macro.index.year == ano, 'Selic']
        if taxa_row.empty:
            continue
        taxa_ano = taxa_row.iloc[0] / 100.0
        taxa_mensal = (1 + taxa_ano) ** (1 / 12) - 1
        saldo = (saldo + aporte_mensal) * (1 + taxa_mensal)
        df_patrimonio.loc[data] = saldo
    return df_patrimonio.sort_index()

# ---------------------------------------------------------------------------
# 4. Benchmark Ibovespa (aportes mensais) ------------------------------------
# ---------------------------------------------------------------------------
'''
def calcular_patrimonio_ibov(
    precos_ibov: pd.Series | pd.DataFrame,
    datas_aportes: Sequence[pd.Timestamp],
    aporte_mensal: float = 1000.0,
) -> pd.DataFrame:
    """Evolução do patrimônio ao investir R$1.000 por mês no Ibovespa.

    *`precos_ibov`* pode ser uma Series (fechamento) ou DataFrame com coluna
    'IBOV'. O índice deve ser datetime.
    """
    if isinstance(precos_ibov, pd.DataFrame):
        preco_series = precos_ibov.squeeze()
    else:
        preco_series = precos_ibov.copy()
    preco_series.index = pd.to_datetime(preco_series.index)
    preco_series.name = 'Preço_IBOV'

    unidades = 0.0
    registros: List[Dict[str, float]] = []

    for data in datas_aportes:
        if data not in preco_series.index:
            prox = preco_series.index[preco_series.index >= data]
            if prox.empty:
                continue
            data = prox[0]

        preco_atual = preco_series.loc[data]
        if not np.isfinite(preco_atual) or preco_atual <= 0:
            continue

        unidades += aporte_mensal / preco_atual
        patrimonio = unidades * preco_atual
        registros.append({'date': data, 'IBOV_Patrimônio': patrimonio})

    df_patrimonio = pd.DataFrame
    '''
