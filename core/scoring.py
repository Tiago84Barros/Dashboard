from __future__ import annotations

import collections
from typing import Dict, List

import numpy as np
import pandas as pd
from sklearn.linear_model import TheilSenRegressor


# Função para normalizar os dados __________________________________________________________________________________________________________________________________________________________
def z_score_normalize(series: pd.Series, melhor_alto: bool) -> pd.Series:
    mean = series.mean()
    std = series.std()
    if std == 0 or np.isnan(std):
        return pd.Series(0, index=series.index)
    z = (series - mean) / std
    return z if melhor_alto else -z
    

# Função responsável por realizar a regressão TheilSen para determinar a taxa de crescimento das variáveis das demonstrações financeiras ____________________________________________________
def slope_regressao_log(df, col):
        """
        Faz regressão linear robusta de ln(col) vs Ano utilizando o TheilSenRegressor,
        retornando o slope (beta). Filtra valores <= 0, pois ln(<=0) não é definido.
        Retorna 0.0 se não houver dados suficientes.
        """
        # Filtra dados válidos: não-nulos para 'Ano' e a coluna, e valores positivos para a coluna
        df_valid = df.dropna(subset=['Ano', col]).copy()
        df_valid = df_valid[df_valid[col] > 0]
        if len(df_valid) < 2:
            return 0.0
    
        # Calcula o logaritmo natural da coluna
        df_valid['ln_col'] = np.log(df_valid[col])
    
        # Cria a variável preditora X (Ano) e a variável alvo y (ln da coluna)
        X = df_valid[['Ano']].values
        y = df_valid['ln_col'].values
    
        # Ajusta o modelo robusto de regressão Theil-Sen
        model = TheilSenRegressor(random_state=42)
        model.fit(X, y)
        slope = model.coef_[0]
        
        return slope


# Função que transforma o valor absoluto do valor encontrado na regressão de Theil-Sen para porcentagem ____________________________________________________________________________________
def slope_to_growth_percent(slope): 
    """
    Converte slope da regressão log em taxa de crescimento aproximada (%).
    Ex.: se slope=0.07, growth ~ e^0.07 - 1 ~ 7.25%
    """
    return np.exp(slope) - 1


 # Função auxiliar para calcular média e desvio-padrão das variáveis dos múltiplos __________________________________________________________________________________________________________
def calcular_media_e_std(df, col):
    """
    Retorna a média e o desvio padrão da coluna `col` do DataFrame `df`.
    Remove valores nulos e infinitos antes do cálculo e exibe informações
    de depuração via Streamlit.
    """   
    
    # 1️⃣ Verificando se a coluna existe
    if col not in df.columns:
        st.error(f"⚠️ A coluna `{col}` não existe no DataFrame!")
        return (0.0, 0.0)

    # 5️⃣ Removendo valores NaN
    df_valid = df.dropna(subset=[col])
  
    # 6️⃣ Convertendo a coluna para numérico, tratando erros
    df_valid[col] = pd.to_numeric(df_valid[col], errors='coerce')

    # 7️⃣ Verificando quantos valores se tornaram NaN após conversão
    nan_count = df_valid[col].isna().sum()

    # 8️⃣ Removendo valores NaN novamente
    df_valid = df_valid.dropna(subset=[col])

    # 9️⃣ Removendo valores infinitos
    df_valid = df_valid[np.isfinite(df_valid[col])]

    # 🔟 Caso o DataFrame fique vazio após os tratamentos
    if df_valid.empty:
        return (0.0, 0.0)

    # 🔥 11️⃣ Calcular e exibir estatísticas finais
    media = df_valid[col].mean()
    std = df_valid[col].std()
      
    return (media, std)


# Calcular Métricas Históricas _______________________________________________________________________________________________________________________________________________________________    
def calcular_metricas_historicas_simplificadas(df_mult, df_dre): 
    """
    Calcula métricas essenciais para um conjunto pequeno de variáveis.
    - Múltiplos: Margem_Liquida, Margem_Operacional, ROE, ROIC, P/VP, Endividamento_Total, Alavancagem_Financeira, Liquidez_Corrente
    - DRE: Receita Líquida, Lucro Líquido, Patrimônio Líquido, Dívida Líquida, Caixa Líquido (com slope log)
    
    Retorna um dicionário que representa a 'linha' de métricas da empresa.
    """
    # Converter Data -> Ano
    df_mult['Ano'] = pd.to_datetime(df_mult['Data'], errors='coerce').dt.year
    df_dre['Ano']  = pd.to_datetime(df_dre['Data'], errors='coerce').dt.year
            
    # Ordenar por Ano
    df_mult.sort_values('Ano', inplace=True)
    df_dre.sort_values('Ano', inplace=True)
    
    # Dicionário final
    metrics = {}
    # PASSO 4
    # =============== MÚLTIPLOS ===============
    for col in ['Margem_Liquida', 'Margem_Operacional', 'ROE', 'ROA', 'ROIC', 'P/VP', 'Endividamento_Total', 'Alavancagem_Financeira', 'Liquidez_Corrente', 'DY']:
        mean, std = calcular_media_e_std(df_mult, col)
        metrics[f'{col}_mean'] = mean
        metrics[f'{col}_std'] = std
    
    # =============== DEMONSTRAÇÕES ===============
    for col in ['Receita_Liquida', 'Lucro_Liquido', 'Patrimonio_Liquido', 'Divida_Liquida', 'Caixa_Liquido']:
        slope = slope_regressao_log(df_dre, col)
        metrics[f'{col}_slope_log'] = slope
        metrics[f'{col}_growth_approx'] = slope_to_growth_percent(slope)
    
    return metrics


# Função que realiza penalização por baixo desvio-padrão ____________________________________________________________________________________________________________________________________
def calc_crowding_penalty(df_setor: pd.DataFrame,
                          coluna='P/VP',
                          floor=0.85, ceil=1.50) -> float:
    """
    Retorna um fator ∈[floor, ceil] que diminui quando o desvio-padrão
    dos múltiplos do setor é baixo (crowding alto).
    """
    if df_setor.empty or coluna not in df_setor:
        return 1.0                                  # neutro

    dispersion = df_setor[coluna].std()
    media      = df_setor[coluna].mean()
    if not np.isfinite(dispersion) or media == 0:
        return 1.0

    crowd_score = 1 - np.tanh(dispersion / media)   # 0-1
    return floor + (ceil - floor) * crowd_score     # linear entre limites
                              

# Função responsável por Reduzir o Score_Ajustado quando a ação está abaixo da mediana setorial ______________________________________________________________________________________________________
def penalizar_plato(df_scores, precos_mensal, meses=18, penal=0.25):
    """
    Reduz Score_Ajustado quando a ação está abaixo da mediana setorial
    no retorno acumulado dos ÚLTIMOS `meses` MESES — sem olhar o futuro.

    Parâmetros
    ----------
    df_scores : DataFrame  # colunas ['Ano', 'ticker', 'Score_Ajustado']
    precos_mensal : DataFrame  # preços ajustados, index = último dia útil de cada mês
    meses : int (default 18)
    penal : float (default 0.25)  # % de penalização (ex.: 0.25 → -25 %)
    """
    # 1️⃣ Retorno retrospectivo de 18 meses
    ret_18m = precos_mensal.pct_change(periods=meses)

    # 2️⃣ Itera ano a ano, SEM olhar além de 31/12/Y
    for ano in df_scores['Ano'].unique():
        data_fim = precos_mensal.index[precos_mensal.index.year == ano].max()
        if pd.isna(data_fim):               # não há preços esse ano
            continue
        # mediana do setor em 31/12/Y
        ret_setor = ret_18m.loc[data_fim].median(skipna=True)

        # percorre as ações daquele ano
        mask_ano = df_scores['Ano'] == ano
        for idx, row in df_scores[mask_ano].iterrows():
            tk = row['ticker']
            if tk not in ret_18m.columns or pd.isna(ret_18m.loc[data_fim, tk]):
                continue

            # 3️⃣ compara retorno vs. mediana
            if ret_18m.loc[data_fim, tk] < ret_setor:
                df_scores.at[idx, 'Score_Ajustado'] *= (1 - penal)

    return df_scores
    
 # Ajuste do score baseado nos pesos ajustados ______________________________________________________________________________________________________________________________________________
def calcular_score_ajustado(df, pesos_utilizados):
    """
    Calcula o Score_Ajustado com tratamento completo:
    - Winsorize
    - Penalização por volatilidade
    - Bônus histórico
    - Normalização z-score
    - Soma ponderada com pesos ajustados
    """
    for col, cfg in pesos_utilizados.items():
        if col in df.columns:
            #df[col] = winsorize(df[col])
            vol_col = col.replace("_mean", "_volatility_penalty")
            if vol_col in df.columns:
                df[col] *= (1 - df[vol_col])
            if 'historico_bonus' in df.columns:
                df[col] *= (df['historico_bonus'] ** 10)
    
    df['Score_Ajustado'] = 0.0

    for col, cfg in pesos_utilizados.items():
        if col in df.columns:
            df[col + '_norm'] = z_score_normalize(df[col], cfg['melhor_alto'])
            df['Score_Ajustado'] += df[col + '_norm'] * cfg['peso']

    return df


  # Calcula o Score para cada empresa de acordo com o segmento que ela está inserido _________________________________________________________________________________________________________
def calcular_score_acumulado(lista_empresas, setores_empresa, pesos_utilizados, dados_macro, anos_minimos=4):
    """
    Calcula o Score Acumulado ao longo dos anos, considerando ajustes macroeconômicos e pesos específicos por segmento ou setor.

    Parâmetros:
    - lista_empresas: Lista contendo dados financeiros de cada empresa.
    - setores_df: DataFrame com colunas ['ticker', 'SETOR', 'SEGMENTO'].
    - pesos_por_segmento: Dicionário com pesos ajustados por segmento.
    - pesos_por_setor: Dicionário com pesos ajustados por setor.
    - indicadores_score_ajustados: Dicionário de fallback com pesos genéricos.
    - dados_macro: DataFrame com os indicadores macroeconômicos ao longo dos anos.
    - anos_minimos: Número mínimo de anos para iniciar o cálculo do score.

    Retorna:
    - DataFrame com Score ajustado ao longo dos anos.
    """

    anos_disponiveis = sorted(set(ano for emp in lista_empresas for ano in emp['multiplos']['Ano'].unique()))
    df_resultados = []

       
    ##### 2) estado que persiste de um ano para outro ##########################
    anos_lider = collections.defaultdict(int)   # fora do loop anual
    
    ##### 3) LOOP ANUAL (trecho que substitui o seu agrupamento existente) #####
    for idx in range(anos_minimos, len(anos_disponiveis)):
        ano = anos_disponiveis[idx]
        dados_ano = []
    
        # === reúno todos os multiplos de cada empresa para este ano ==========
        for emp in lista_empresas:
            ticker = emp['ticker']
            df_mult = emp['multiplos'][emp['multiplos']['Ano'] <= ano].copy()
            df_dre  = emp['dre'   ][emp['dre'   ]['Ano'] <= ano].copy()
            if df_mult.empty or df_dre.empty:
                continue
    
            # setor da empresa
            setor = setores_empresa.get(ticker, "OUTROS")
    
            # ---------------- Crowd penalty (precisa de dados do setor) ------
            df_setor_ano = pd.concat(
                [e['multiplos'][e['multiplos']['Ano'] == ano][['Ticker', 'P/VP']]
                 for e in lista_empresas
                 if setores_empresa.get(e['ticker']) == setor],
                ignore_index=True
            )
            crowd_pen  = calc_crowding_penalty(df_setor_ano, 'P/VP')
    
            # ---------------- Métricas “clássicas” que você já calculava ----
            metricas = calcular_metricas_historicas_simplificadas(
                           df_mult, df_dre)
    
            row = {'ticker': ticker,
                   'Ano'   : ano,
                   **metricas,
                   'Penalty_Crowd': crowd_pen}
    
            dados_ano.append(row)
    
        # ---------- DataFrame anual com crowd-penalty incluído --------------
        df_ano = pd.DataFrame(dados_ano)
        if df_ano.empty:
            continue
    
        # ------------ normaliza + soma ponderada (sua rotina) ---------------
        df_ano = calcular_score_ajustado(df_ano, pesos_utilizados)
    
        # --------------------------------------------------------------------
        # 4) determina líder e aplica Penalty_Decay
        # --------------------------------------------------------------------
        df_ano = df_ano.sort_values('Score_Ajustado', ascending=False)
    
        lider_ano = df_ano.iloc[0]['ticker']        # 1º da lista
        for ix, row in df_ano.iterrows():
            tk = row['ticker']
            # acumulo anos consecutivos de liderança
            if tk == lider_ano:
                anos_lider[tk] += 1
            else:
                anos_lider[tk] = 0
    
            # fator de decaimento (trunca em -25 %)
            decay_factor = 1 - min(0.03 * max(anos_lider[tk]-1, 0), 0.25)
            df_ano.at[ix, 'Penalty_Decay'] = decay_factor
    
            # aplica ambas as penalizações ao score já normalizado
            df_ano.at[ix, 'Score_Ajustado'] *= \
                df_ano.at[ix, 'Penalty_Crowd'] * decay_factor
    
        # agora sim, mantenha apenas as colunas que precisa
        df_resultados.append(
            df_ano[['Ano', 'ticker', 'Score_Ajustado']]
        )

    if df_resultados:
        df_scores = pd.concat(df_resultados, ignore_index=True)
    else:
        df_scores = pd.DataFrame(columns=['Ano', 'ticker', 'Score_Ajustado'])
                
    return df_scores

__all__ = ["calcular_score_acumulado"]
