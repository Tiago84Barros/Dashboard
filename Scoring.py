"""scoring.py
~~~~~~~~~~~~~
Módulo de cálculo de pontuação fundamentalista.

Funções públicas
----------------
penalizar_plato            → Penaliza Score_Ajustado se retorno 18 m < mediana setorial.
calcular_score_ajustado    → Aplica normalização, pesos e bônus para gerar Score_Ajustado.
calcular_score_acumulado   → Calcula Score_Ajustado ano‑a‑ano, incluindo crowd‑penalty e decay.

Dependências: pandas, numpy, collections.
Algumas rotinas utilitárias (z_score_normalize, calcular_metricas_historicas_simplificadas)
foram incluídas como *placeholders* — substitua pela sua implementação.
"""

from __future__ import annotations

import collections
from typing import Any, Dict, List, Sequence

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import TheilSenRegressor

# ---------------------------------------------------------------------------
# Funções auxiliares (PLACEHOLDERS)
# ---------------------------------------------------------------------------

#  Trunca os valores muito fora da média ____________________________________________________________________________________________________________________________________________
def winsorize(series, lower_quantile=0.05, upper_quantile=0.95):
    """
    Aplica winsorização à série: substitui valores abaixo do quantil inferior e acima do quantil superior
    pelos próprios limites.
    """
    s = series.dropna()
    if s.empty:
        return series
    lower_bound = s.quantile(lower_quantile)
    upper_bound = s.quantile(upper_quantile)
    return series.clip(lower=lower_bound, upper=upper_bound)

# Função que realiza a normalização dos dados (comparabilidade dos múltiplos, reduzindo distorções causadas por concentração de valores em um extremo)_______________________________
def z_score_normalize(series, melhor_alto=True):
    series = series.replace([np.inf, -np.inf], np.nan)
    valid = series.dropna()
    if valid.empty:
        return pd.Series([0.0] * len(series), index=series.index)
    mean_val = valid.mean()
    std_val = valid.std()
    if std_val == 0:
        return pd.Series([0.0] * len(series), index=series.index)
    normalized = (series - mean_val) / std_val
    return normalized.fillna(0.0) if melhor_alto else -normalized.fillna(0.0)
    
# Calcula a média e o desvio padrão das variáveis dos múltiplos  __________________________________________________________________________________________________________________
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

# Regressão Linear utilizando o TheilSenRegressor para determinar o coeficiente de crescimento das variáveis das demonstrações finaneiras _____________________________________________________
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

# transforma o valor absoluto do valor encontrado na regressão para porcentagem ______________________________________________________________________________________________________________
def slope_to_growth_percent(slope): 
        """
        Converte slope da regressão log em taxa de crescimento aproximada (%).
        Ex.: se slope=0.07, growth ~ e^0.07 - 1 ~ 7.25%
        """
        return np.exp(slope) - 1
    

# Função responsável por criar as métricas das informações financeiras e dos múltiplos das empresas___________________________________________________________________________________________
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
    
    # Penalização por alta volatilidade (desvio padrão relativo à média) # PASSO 5
    #for col in ['Margem_Liquida', 'ROE', 'ROA', 'ROIC', 'Endividamento_Total', 'Liquidez_Corrente']:
     #   if metrics[f'{col}_mean'] != 0:
      #      coef_var = metrics[f'{col}_std'] / abs(metrics[f'{col}_mean'])
       #     metrics[f'{col}_volatility_penalty'] = min(1.0, coef_var)  # Penalização limitada a 100% 
        #else:
          #  metrics[f'{col}_volatility_penalty'] = 1.0  # Penalização máxima se a média for zero
    
     # 📌 NOVA Penalização por Histórico Longo → Agora mais severa # PASSO 5
    # num_anos = df_dre['Ano'].nunique()
    
    #def calcular_historico_bonus(anos):
        #return anos / ((10 + anos) ** 10)  # Penalização bem mais severa para novatas

    # Aplicando penalização aprimorada
    # metrics['historico_bonus'] = calcular_historico_bonus(num_anos)
    
    return metrics

# ---------------------------------------------------------------------------
# 1. Penalização por *plato* (retorno abaixo da mediana setorial) -------------
# ---------------------------------------------------------------------------

def penalizar_plato(df_scores: pd.DataFrame,
                    precos_mensal: pd.DataFrame,
                    meses: int = 18,
                    penal: float = 0.25) -> pd.DataFrame:
    """Reduz *Score_Ajustado* quando o retorno 18 m está abaixo da mediana setorial.

    Parameters
    ----------
    df_scores : DataFrame
        Colunas obrigatórias: ['Ano', 'ticker', 'Score_Ajustado']
    precos_mensal : DataFrame
        Preços ajustados mensais (index = último dia útil de cada mês).
    meses : int, default 18
        Janela de retorno retrospectivo.
    penal : float, default 0.25
        Percentual de penalização (0.25 → –25 %).
    """
    # Retorno retrospectivo
    ret_18m = precos_mensal.pct_change(periods=meses)

    # Itera ano a ano
    for ano in df_scores['Ano'].unique():
        data_fim = precos_mensal.index[precos_mensal.index.year == ano].max()
        if pd.isna(data_fim):
            continue  # sem preços neste ano

        ret_setor = ret_18m.loc[data_fim].median(skipna=True)
        mask_ano = df_scores['Ano'] == ano

        for idx, row in df_scores[mask_ano].iterrows():
            tk = row['ticker']
            if tk not in ret_18m.columns or pd.isna(ret_18m.loc[data_fim, tk]):
                continue
            if ret_18m.loc[data_fim, tk] < ret_setor:
                df_scores.at[idx, 'Score_Ajustado'] *= (1 - penal)

    return df_scores

# ---------------------------------------------------------------------------
# 2. Cálculo do Score_Ajustado (normalização + pesos) -------------------------
# ---------------------------------------------------------------------------
def calcular_score_ajustado(df: pd.DataFrame,
                            pesos_utilizados: Dict[str, Dict[str, Any]],
                            bonus_power: int = 10) -> pd.DataFrame:
    """Calcula *Score_Ajustado* usando pesos, volatilidade e bônus histórico.

    pesos_utilizados → {'coluna': {'peso': float, 'melhor_alto': bool}}
    """
    df = df.copy()

    # Ajustes por coluna
    for col, cfg in pesos_utilizados.items():
        if col not in df.columns:
            continue
        vol_col = col.replace("_mean", "_volatility_penalty")
        if vol_col in df.columns:
            df[col] *= (1 - df[vol_col])
        if 'historico_bonus' in df.columns:
            df[col] *= df['historico_bonus'] ** bonus_power

    # Normaliza e pondera
    df['Score_Ajustado'] = 0.0
    for col, cfg in pesos_utilizados.items():
        if col not in df.columns:
            continue
        df[f'{col}_norm'] = z_score_normalize(df[col], cfg.get('melhor_alto', True))
        df['Score_Ajustado'] += df[f'{col}_norm'] * cfg['peso']

    return df


# Função responsável por penalizar Lider que não apresenta crescimento de suas ações a médio prazo
def _calc_crowding_penalty(df_setor: pd.DataFrame,
                           coluna: str = 'P/VP',
                           floor: float = 0.85,
                           ceil: float = 1.50) -> float:
    """Fator que diminui quando o desvio‑padrão dos múltiplos é baixo."""
    if df_setor.empty or coluna not in df_setor:
        return 1.0
    dispersion = df_setor[coluna].std()
    media = df_setor[coluna].mean()
    if not np.isfinite(dispersion) or media == 0:
        return 1.0
    crowd_score = 1 - np.tanh(dispersion / media)  # 0‑1
    return floor + (ceil - floor) * crowd_score

# ---------------------------------------------------------------------------
# 3. Score acumulado ano‑a‑ano ----------------------------------------------
# ---------------------------------------------------------------------------
def calcular_score_acumulado(lista_empresas: Sequence[Dict[str, Any]],
                             setores_empresa: Dict[str, str],
                             pesos_utilizados: Dict[str, Dict[str, Any]],
                             dados_macro: pd.DataFrame | None,
                             momentum12m_df: pd.DataFrame | None,
                             anos_minimos: int = 4) -> pd.DataFrame:
    """Devolve DataFrame com ['Ano', 'ticker', 'Score_Ajustado'] pronto para uso."""
    anos_disponiveis = sorted({
        ano for emp in lista_empresas for ano in emp['multiplos']['Ano'].unique()
    })
    df_resultados: List[pd.DataFrame] = []
    anos_lider = collections.defaultdict(int)

    for idx in range(anos_minimos, len(anos_disponiveis)):
        ano = anos_disponiveis[idx]
        registro_ano: List[Dict[str, Any]] = []

        for emp in lista_empresas:
            tk = emp['ticker']
            df_mult = emp['multiplos'][emp['multiplos']['Ano'] <= ano].copy()
            df_dre = emp['df_dre'][emp['df_dre']['Ano'] <= ano].copy()
            if df_mult.empty or df_dre.empty:
                continue
            setor = setores_empresa.get(tk, 'OUTROS')
            df_setor = pd.concat([
                e['multiplos'][e['multiplos']['Ano'] == ano][['Ticker', 'P/VP']]
                for e in lista_empresas if setores_empresa.get(e['ticker']) == setor
            ], ignore_index=True)
            crowd_pen = _calc_crowding_penalty(df_setor)
            métricas = calcular_metricas_historicas_simplificadas(df_mult, df_dre)
            registro_ano.append({'ticker': tk, 'Ano': ano, **métricas, 'Penalty_Crowd': crowd_pen})

        df_ano = pd.DataFrame(registro_ano)
        if df_ano.empty:
            continue
        df_ano = _calcular_score_ajustado(df_ano, pesos_utilizados).sort_values('Score_Ajustado', ascending=False)

        lider = df_ano.iloc[0]['ticker']
        for ix, row in df_ano.iterrows():
            tk = row['ticker']
            anos_lider[tk] = anos_lider[tk] + 1 if tk == lider else 0
            decay = 1 - min(0.03 * max(anos_lider[tk]-1, 0), 0.25)
            df_ano.at[ix, 'Score_Ajustado'] *= row['Penalty_Crowd'] * decay
        df_resultados.append(df_ano[['Ano', 'ticker', 'Score_Ajustado']])

    return pd.concat(df_resultados, ignore_index=True) if df_resultados else pd.DataFrame(columns=['Ano', 'ticker', 'Score_Ajustado'])

# ---------------------------------------------------------------------------
__all__ = ['calcular_score_acumulado']
