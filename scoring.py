"""scoring.py
~~~~~~~~~~~~
Módulo para cálculo de Score Ajustado das empresas.

Funções públicas
----------------
- calcular_score_acumulado(lista_empresas, setores_empresa, pesos_utilizados, dados_macro, momentum_df, anos_minimos)
- _penalizar_plato(df_scores, precos_mensal, meses=18, penal=0.25)
"""

import pandas as pd
import numpy as np
import collections


def _penalizar_plato(df_scores, precos_mensal, meses=18, penal=0.25):
    """
    Reduz Score_Ajustado quando a ação fica abaixo da mediana setorial
    no retorno acumulado dos últimos `meses` meses.

    Parâmetros:
    - df_scores: DataFrame com colunas ['Ano', 'ticker', 'Score_Ajustado']
    - precos_mensal: DataFrame de preços ajustados, index mensal
    - meses: número de meses para cálculo de retorno (padrão 18)
    - penal: percentual de penalização (0.25 = 25%)

    Retorna o DataFrame modificado.
    """
    # 1) Retorno acumulado dos últimos `meses` meses
    ret = precos_mensal.pct_change(periods=meses)

    # 2) Iterar por ano
    for ano in df_scores['Ano'].unique():
        # encontra última data do ano
        datas_ano = precos_mensal.index[precos_mensal.index.year == ano]
        if datas_ano.empty:
            continue
        data_fim = datas_ano.max()

        # 3) mediana setorial
        med_setor = ret.loc[data_fim].median(skipna=True)

        # 4) aplicar penalização
        mask = df_scores['Ano'] == ano
        for idx, row in df_scores[mask].iterrows():
            tk = row['ticker']
            if tk not in ret.columns or pd.isna(ret.at[data_fim, tk]):
                continue
            if ret.at[data_fim, tk] < med_setor:
                df_scores.at[idx, 'Score_Ajustado'] *= (1 - penal)

    return df_scores


def calcular_score_acumulado(
    lista_empresas,
    setores_empresa,
    pesos_utilizados,
    dados_macro,
    momentum_df,
    anos_minimos=4,
):
    """
    Calcula o Score Ajustado acumulado ao longo dos anos, com:
    - crowd penalty
    - decay de liderança

    Parâmetros:
    - lista_empresas: list de dicts {'ticker', 'multiplos':DF, 'df_dre':DF}
    - setores_empresa: dict {ticker: setor}
    - pesos_utilizados: dict de indicadores com {'peso', 'melhor_alto'}
    - dados_macro: DataFrame com indicadores macro (index anual)
    - momentum_df: DataFrame com momentum (pode ser None)
    - anos_minimos: mínimo de anos para iniciar cálculo (default 4)

    Retorna DataFrame com colunas ['Ano', 'ticker', 'Score_Ajustado']
    """
    # helper: penalidade por crowding
    def calc_crowd(df_setor, col='P/VP', floor=0.85, ceil=1.5):
        if df_setor.empty or col not in df_setor:
            return 1.0
        disp = df_setor[col].std()
        media = df_setor[col].mean()
        if not np.isfinite(disp) or media == 0:
            return 1.0
        crowd_score = 1 - np.tanh(disp / media)
        return floor + (ceil - floor) * crowd_score

    # lista de anos disponíveis
    anos = sorted({ano for emp in lista_empresas for ano in emp['multiplos']['Ano'].unique()})
    resultados = []
    anos_consec = collections.defaultdict(int)

    # loop anual
    for i in range(anos_minimos, len(anos)):
        ano = anos[i]
        rows = []

        for emp in lista_empresas:
            tk = emp['ticker']
            mult = emp['multiplos']
            dre = emp['df_dre']
            df_mult = mult[mult['Ano'] == ano]
            df_dre  = dre[dre['Ano'] == ano]
            if df_mult.empty or df_dre.empty:
                continue

            setor = setores_empresa.get(tk, 'OUTROS')
            # crowd
            df_setor_ano = pd.concat([
                e['multiplos'][e['multiplos']['Ano'] == ano][['Ticker', 'P/VP']]
                for e in lista_empresas if setores_empresa.get(e['ticker']) == setor
            ], ignore_index=True)
            crowd = calc_crowd(df_setor_ano)

            # montar row
            data = {'ticker': tk, 'Ano': ano, 'Penalty_Crowd': crowd}
            # merge métricas de múltiplos
            data.update(df_mult.iloc[0].to_dict())
            rows.append(data)

        if not rows:
            continue
        df_ano = pd.DataFrame(rows)

        # normalização z-score e soma ponderada
        df_ano['Score_Ajustado'] = 0.0
        for col, cfg in pesos_utilizados.items():
            if col in df_ano:
                mean = df_ano[col].mean()
                std  = df_ano[col].std()
                if std and not np.isnan(std):
                    norm = (df_ano[col] - mean) / std
                else:
                    norm = 0
                df_ano['Score_Ajustado'] += norm * cfg.get('peso', 0)

        # aplica penalização por plateau
        df_ano = df_ano.sort_values('Score_Ajustado', ascending=False)
        lider = df_ano.iloc[0]['ticker']
        for idx, r in df_ano.iterrows():
            tk = r['ticker']
            # conta anos de liderança
            if tk == lider:
                anos_consec[tk] += 1
            else:
                anos_consec[tk] = 0
            decay = 1 - min(0.03 * max(anos_consec[tk] - 1, 0), 0.25)
            df_ano.at[idx, 'Score_Ajustado'] *= decay * r['Penalty_Crowd']

        resultados.append(df_ano[['Ano', 'ticker', 'Score_Ajustado']])

    if resultados:
        return pd.concat(resultados, ignore_index=True)
    return pd.DataFrame(columns=['Ano', 'ticker', 'Score_Ajustado'])
