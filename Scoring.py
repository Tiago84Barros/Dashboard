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

# ---------------------------------------------------------------------------
# Funções auxiliares (PLACEHOLDERS) — ajuste conforme sua base de código
# ---------------------------------------------------------------------------

def z_score_normalize(series: pd.Series, melhor_alto: bool = True) -> pd.Series:
    """Normaliza a série por z‑score. Se *melhor_alto* = False, inverte o sinal."""
    if not melhor_alto:
        series = -series
    return (series - series.mean()) / series.std(ddof=0)


def calcular_metricas_historicas_simplificadas(df_mult: pd.DataFrame,
                                               df_dre: pd.DataFrame) -> Dict[str, Any]:
    """Extrai métricas históricas (stub). Implemente de acordo com sua lógica."""
    raise NotImplementedError(
        "Implemente 'calcular_metricas_historicas_simplificadas' antes de usar o módulo.")

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

# ---------------------------------------------------------------------------
# 3. Score acumulado ano‑a‑ano -------------------------------------------------
# ---------------------------------------------------------------------------

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


def calcular_score_acumulado(lista_empresas: Sequence[Dict[str, Any]],
                             setores_empresa: Dict[str, str],
                             pesos_utilizados: Dict[str, Dict[str, Any]],
                             dados_macro: pd.DataFrame | None,
                             momentum12m_df: pd.DataFrame | None,
                             anos_minimos: int = 4) -> pd.DataFrame:
    """Calcula Score_Ajustado ao longo dos anos, com crowd‑penalty e decay."""
    anos_disponiveis = sorted({
        ano for emp in lista_empresas for ano in emp['multiplos']['Ano'].unique()
    })
    df_resultados: List[pd.DataFrame] = []
    anos_lider = collections.defaultdict(int)  # estado acumulado

    for idx in range(anos_minimos, len(anos_disponiveis)):
        ano = anos_disponiveis[idx]
        dados_ano: List[Dict[str, Any]] = []

        for emp in lista_empresas:
            tk = emp['ticker']
            df_mult = emp['multiplos'][emp['multiplos']['Ano'] <= ano].copy()
            df_dre = emp['df_dre'][emp['df_dre']['Ano'] <= ano].copy()
            if df_mult.empty or df_dre.empty:
                continue

            setor = setores_empresa.get(tk, 'OUTROS')

            df_setor_ano = pd.concat([
                e['multiplos'][e['multiplos']['Ano'] == ano][['Ticker', 'P/VP']]
                for e in lista_empresas if setores_empresa.get(e['ticker']) == setor
            ], ignore_index=True)
            crowd_pen = _calc_crowding_penalty(df_setor_ano)

            metricas = calcular_metricas_historicas_simplificadas(df_mult, df_dre)
            dados_ano.append({'ticker': tk, 'Ano': ano, **metricas, 'Penalty_Crowd': crowd_pen})

        df_ano = pd.DataFrame(dados_ano)
        if df_ano.empty:
            continue
        df_ano = calcular_score_ajustado(df_ano, pesos_utilizados)
        df_ano = df_ano.sort_values('Score_Ajustado', ascending=False)

        lider = df_ano.iloc[0]['ticker']
        for ix, row in df_ano.iterrows():
            tk = row['ticker']
            anos_lider[tk] = anos_lider[tk] + 1 if tk == lider else 0
            decay = 1 - min(0.03 * max(anos_lider[tk] - 1, 0), 0.25)
            df_ano.at[ix, 'Penalty_Decay'] = decay
            df_ano.at[ix, 'Score_Ajustado'] *= row['Penalty_Crowd'] * decay

        df_resultados.append(df_ano[['Ano', 'ticker', 'Score_Ajustado']])

    if df_resultados:
        return pd.concat(df_resultados, ignore_index=True)
    return pd.DataFrame(columns=['Ano', 'ticker', 'Score_Ajustado'])

# ---------------------------------------------------------------------------
__all__ = [
    'penalizar_plato',
    'calcular_score_ajustado',
    'calcular_score_acumulado',
]
