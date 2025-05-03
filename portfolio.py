"""portfolio.py
~~~~~~~~~~~~~
Funções para simular estratégias de aporte e comparar carteiras.

Funções públicas
----------------
- gerir_carteira(precos, df_scores, lideres_por_ano, dividendos_dict, aporte_mensal=1000, deterioracao_limite=0.0)
- gerir_carteira_todas_empresas(precos, tickers, datas_aportes, dividendos_dict, aporte_mensal=1000)
- calcular_patrimonio_selic_macro(dados_macro, datas_aportes, aporte_mensal=1000)
"""

import pandas as pd
import numpy as np

def gerir_carteira(
    precos: pd.DataFrame,
    df_scores: pd.DataFrame,
    lideres_por_ano: pd.DataFrame,
    dividendos_dict: dict,
    aporte_mensal: float = 1000,
    deterioracao_limite: float = 0.0,
):
    """
    Simula aportes mensais seguindo líderes e vendendo deterioradas.
    Retorna DataFrame de patrimônio e lista de datas de aporte.
    """
    from collections import defaultdict

    carteira = defaultdict(float)
    aporte_acum = 0.0
    registros = []
    lideres_atuais = []

    anos = sorted(df_scores['Ano'].unique())
    for ano in anos:
        empresa_lider = lideres_por_ano.query("Ano == @ano")['ticker'].iloc[0]
        if empresa_lider not in lideres_atuais:
            lideres_atuais.append(empresa_lider)

        for mes in range(1, 13):
            data_nominal = pd.Timestamp(f"{ano+1}-{mes:02d}-01")
            # encontrar_proxima_data_valida equivalente: usar index >= data_nominal
            poss = precos.index[precos.index >= data_nominal]
            if poss.empty:
                continue
            data_sinal = poss[0]

            # reinvestir dividendos
            for emp in list(carteira):
                if emp in dividendos_dict:
                    df_div = dividendos_dict[emp]
                    if not df_div.empty:
                        df_div.index = pd.to_datetime(df_div.index)
                        div_mes = df_div[(df_div.index.year == data_sinal.year) & (df_div.index.month == data_sinal.month)].sum()
                        preco_emp = precos.at[data_sinal, emp] if data_sinal in precos.index else None
                        if preco_emp and preco_emp > 0:
                            carteira[emp] += (div_mes * carteira[emp]) / preco_emp

            total = aporte_mensal + aporte_acum
            aporte_acum = 0.0
            if lideres_atuais:
                ap_por = total / len(lideres_atuais)
                for lider in lideres_atuais:
                    if lider not in precos.columns or data_sinal not in precos.index:
                        aporte_acum += ap_por
                        continue
                    p = precos.at[data_sinal, lider]
                    if p <= 0 or np.isnan(p):
                        aporte_acum += ap_por
                    else:
                        carteira[lider] += ap_por / p

            # vende deterioradas
            for antiga in list(carteira):
                if antiga in lideres_atuais:
                    continue
                score_ini = df_scores.query("Ano == @anos[0] and ticker == @antiga")['Score_Ajustado']
                score_at = df_scores.query("Ano == @ano and ticker == @antiga")['Score_Ajustado']
                if score_ini.empty or score_at.empty:
                    continue
                if score_at.values[0] / score_ini.values[0] < deterioracao_limite:
                    if data_sinal in precos.index and antiga in precos.columns:
                        preco_venda = precos.at[data_sinal, antiga]
                        p_lider = precos.at[data_sinal, empresa_lider]
                        if preco_venda > 0 and p_lider > 0:
                            carteira[empresa_lider] += (carteira.pop(antiga) * preco_venda) / p_lider

            # registro
            total_val = sum(qtd * precos.at[data_sinal, tk] for tk, qtd in carteira.items() if data_sinal in precos.index and tk in precos.columns)
            registros.append({'date': data_sinal, 'Patrimônio': total_val})

    df_p = pd.DataFrame(registros).set_index('date').sort_index().fillna(method='ffill')
    return df_p, df_p.index.unique().tolist()


def gerir_carteira_todas_empresas(
    precos: pd.DataFrame,
    tickers: list,
    datas_aportes: list,
    dividendos_dict: dict,
    aporte_mensal: float = 1000,
):
    """
    Aporta igualmente em todas as `tickers`, reinveste dividendos.
    Retorna DataFrame de patrimônios por empresa.
    """
    carteira = {t: 0.0 for t in tickers}
    patrimonio = {t: {} for t in tickers}
    precos.index = pd.to_datetime(precos.index)

    for data in datas_aportes:
        if data not in precos.index:
            nxt = precos.index[precos.index >= data]
            if nxt.empty:
                continue
            data = nxt[0]
        for t in tickers:
            if t not in precos.columns:
                continue
            p = precos.at[data, t]
            if p <= 0 or np.isnan(p):
                continue
            divs = 0
            if t in dividendos_dict:
                df_div = dividendos_dict[t]
                df_div.index = pd.to_datetime(df_div.index)
                divs = df_div[(df_div.index.year == data.year) & (df_div.index.month == data.month)].sum() * carteira[t]
            total = aporte_mensal + divs
            carteira[t] += total / p
            patrimonio[t][data] = carteira[t] * p

    df = pd.DataFrame.from_dict(patrimonio).sort_index()
    return df


def calcular_patrimonio_selic_macro(
    dados_macro: pd.DataFrame,
    datas_aportes: list,
    aporte_mensal: float = 1000,
):
    """
    Simula investimento contínuo no Tesouro Selic.
    """
    df_macro = dados_macro.copy()
    df_macro['Data'] = pd.to_datetime(df_macro['Data'], errors='coerce')
    df_macro.set_index('Data', inplace=True, drop=True)

    saldo = 0.0
    reg = []
    for data in datas_aportes:
        ano = pd.to_datetime(data).year
        taxa = df_macro.loc[df_macro.index.year == ano, 'Selic']
        if taxa.empty:
            continue
        tx = taxa.iloc[0] / 100
        tx_m = (1 + tx) ** (1/12) - 1
        saldo = (saldo + aporte_mensal) * (1 + tx_m)
        reg.append({'date': data, 'Tesouro Selic': saldo})
    df = pd.DataFrame(reg).set_index('date').sort_index()
    return df
