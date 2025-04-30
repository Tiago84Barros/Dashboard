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

# ---------------------------------------------------------------------------
# Place‑holder utilitário ----------------------------------------------------
# ---------------------------------------------------------------------------

def encontrar_proxima_data_valida(data: pd.Timestamp, precos: pd.DataFrame) -> pd.Timestamp | None:
    """Stub — substitua pela lógica que encontra o próximo dia útil
    presente no índice de *precos*. Retorna `None` se não houver data válida."""
    idx = precos.index[precos.index >= data]
    return idx[0] if not idx.empty else None

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
