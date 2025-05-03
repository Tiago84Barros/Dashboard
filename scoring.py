"""
scoring.py
===========
Módulo com funções de scoring:
- penalizar_plato
- calcular_score_ajustado
- calcular_score_acumulado
"""

import pandas as pd
import numpy as np
import collections


def z_score_normalize(series: pd.Series, melhor_alto: bool) -> pd.Series:
    """
    Normaliza em z-score; inverte se melhor_alto=False.
    """
    mean = series.mean()
    std = series.std()
    if std == 0 or np.isnan(std):
        return pd.Series(0, index=series.index)
    z = (series - mean) / std
    return z if melhor_alto else -z


def penalizar_plato(df_scores: pd.DataFrame,
                    precos_mensal: pd.DataFrame,
                    meses: int = 18,
                    penal: float = 0.25) -> pd.DataFrame:
    """
    Reduz Score_Ajustado quando abaixo da mediana setorial no retorno acumulado.
    """
    # retorno retrospectivo
    ret = precos_mensal.pct_change(periods=meses)
    for ano in df_scores['Ano'].unique():
        data_fim = precos_mensal.index[precos_mensal.index.year == ano].max()
        if pd.isna(data_fim):
            continue
        med = ret.loc[data_fim].median(skipna=True)
        mask = df_scores['Ano'] == ano
        for idx, row in df_scores[mask].iterrows():
            tk = row['ticker']
            if tk not in ret.columns or pd.isna(ret.at[data_fim, tk]):
                continue
            if ret.at[data_fim, tk] < med:
                df_scores.at[idx, 'Score_Ajustado'] *= (1 - penal)
    return df_scores


def calcular_score_ajustado(df: pd.DataFrame,
                             pesos_utilizados: dict) -> pd.DataFrame:
    """
    Ajuste de scores: winsorize, penalidade, bonus histórico, z-score e soma ponderada.
    """
    # Parcial: aplicar penalidade por volatilidade e histórico
    for col, cfg in pesos_utilizados.items():
        if col in df.columns:
            vol_col = col.replace('_mean', '_volatility_penalty')
            if vol_col in df.columns:
                df[col] *= (1 - df[vol_col])
            if 'historico_bonus' in df.columns:
                df[col] *= df['historico_bonus'] ** 10
    # Z-score e soma ponderada
    df['Score_Ajustado'] = 0.0
    for col, cfg in pesos_utilizados.items():
        if col in df.columns:
            df[col + '_norm'] = z_score_normalize(df[col], cfg['melhor_alto'])
            df['Score_Ajustado'] += df[col + '_norm'] * cfg['peso']
    return df


def calcular_score_acumulado(lista_empresas: list,
                              setores_empresa: dict,
                              pesos_utilizados: dict,
                              dados_macro: pd.DataFrame,
                              momentum12m_df: pd.DataFrame,
                              anos_minimos: int = 4) -> pd.DataFrame:
    """
    Loop anual: coleta múltiplos, aplica penalidades, normaliza e soma pesos.
    Retorna DataFrame com ['Ano','ticker','Score_Ajustado'].
    """
    # utilitário de penalidade de crowding
    def calc_crowding_penalty(df_setor: pd.DataFrame,
                              coluna: str = 'P/VP',
                              floor: float = 0.85,
                              ceil: float = 1.50) -> float:
        if df_setor.empty or coluna not in df_setor:
            return 1.0
        disp = df_setor[coluna].std()
        media = df_setor[coluna].mean()
        if not np.isfinite(disp) or media == 0:
            return 1.0
        score = 1 - np.tanh(disp / media)
        return floor + (ceil - floor) * score

    anos = sorted({
        ano for emp in lista_empresas
        for ano in emp['multiplos']['Ano'].unique()
    })
    results = []
    anos_lider = collections.defaultdict(int)
    for idx in range(anos_minimos, len(anos)):
        ano = anos[idx]
        rows = []
        for emp in lista_empresas:
            tk = emp['ticker']
            df_mult = emp['multiplos'][emp['multiplos']['Ano'] <= ano]
            df_dre = emp['df_dre'][emp['df_dre']['Ano'] <= ano]
            if df_mult.empty or df_dre.empty:
                continue
            setor = setores_empresa.get(tk, 'OUTROS')
            # crowd penalty
            df_set = pd.concat(
                [e['multiplos'][e['multiplos']['Ano']==ano][['Ticker','P/VP']]
                 for e in lista_empresas
                 if setores_empresa.get(e['ticker']) == setor],
                ignore_index=True
            )
            pen_crowd = calc_crowding_penalty(df_set, 'P/VP')
            # métricas históricas (usuário deve implementar essa função)
            try:
                from metrics import calcular_metricas_historicas_simplificadas
                metricas = calcular_metricas_historicas_simplificadas(df_mult, df_dre)
            except ImportError:
                metricas = {}
            row = {
                'ticker': tk,
                'Ano': ano,
                **metricas,
                'Penalty_Crowd': pen_crowd
            }
            rows.append(row)
        df_ano = pd.DataFrame(rows)
        if df_ano.empty:
            continue
        df_ano = calcular_score_ajustado(df_ano, pesos_utilizados)
        df_ano = df_ano.sort_values('Score_Ajustado', ascending=False)
        # decay penalty
        lider = df_ano.iloc[0]['ticker']
        for i, r in df_ano.iterrows():
            tk = r['ticker']
            if tk == lider:
                anos_lider[tk] += 1
            else:
                anos_lider[tk] = 0
            decay = 1 - min(0.03 * max(anos_lider[tk]-1, 0), 0.25)
            df_ano.at[i,'Penalty_Decay'] = decay
            df_ano.at[i,'Score_Ajustado'] *= decay * df_ano.at[i,'Penalty_Crowd']
        results.append(df_ano[['Ano','ticker','Score_Ajustado']])
    if results:
        return pd.concat(results, ignore_index=True)
    return pd.DataFrame(columns=['Ano','ticker','Score_Ajustado'])
