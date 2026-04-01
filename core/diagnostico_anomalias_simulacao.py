import numpy as np
import pandas as pd
from typing import Dict, Sequence, Union


# =========================================================
# Utilidades (espelhadas da sua base)
# =========================================================

def _ensure_dt_index(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.index, pd.DatetimeIndex):
        df = df.copy()
        df.index = pd.to_datetime(df.index)
    return df.sort_index()


def encontrar_proxima_data_valida(data: pd.Timestamp, df: pd.DataFrame):
    datas_validas = df.index[df.index >= data]
    if len(datas_validas) == 0:
        return None
    return datas_validas[0]


def _as_div_series(div):
    if div is None:
        return pd.Series(dtype="float64")
    if isinstance(div, pd.DataFrame):
        # tenta inferir a coluna de valor
        for col in ["valor", "dividendo", "amount", "provento"]:
            if col in div.columns:
                s = div[col]
                break
        else:
            s = div.iloc[:, 0]
    else:
        s = div
    s = s.copy()
    s.index = pd.to_datetime(s.index)
    return s.sort_index()


def _get_price(precos: pd.DataFrame, data: pd.Timestamp, ticker: str):
    try:
        px = float(precos.loc[data, ticker])
        if px <= 0 or np.isnan(px):
            return None
        return px
    except Exception:
        return None


def _cost_factor(fee_bps: float, slippage_bps: float) -> float:
    return 1.0 - (fee_bps + slippage_bps) / 10000.0


# =========================================================
# Diagnóstico de Anomalias
# =========================================================

def diagnosticar_anomalias_simulacao(
    precos: pd.DataFrame,
    tickers: Sequence[str],
    datas_aportes: Sequence[pd.Timestamp],
    dividendos_dict: Dict[str, Union[pd.Series, pd.DataFrame]],
    aporte_mensal: float = 1000.0,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
    min_preco_aceitavel: float = 0.10,
    max_div_por_acao_mes: float = 50.0,
    max_multiplicador_patrimonio: float = 300.0,
) -> pd.DataFrame:
    """
    Executa uma simulação idêntica à sua (aporte por ação),
    mas registra SOMENTE eventos suspeitos:
      - preço fora de escala
      - dividendo fora de unidade
      - multiplicador absurdo do patrimônio

    Retorna DataFrame com os eventos anômalos.
    """

    precos = _ensure_dt_index(precos)
    tickers = [t for t in tickers if t in precos.columns]
    divs = {t: _as_div_series(dividendos_dict.get(t)) for t in tickers}
    cf = _cost_factor(fee_bps, slippage_bps)

    carteira = {t: 0.0 for t in tickers}
    meses = 0
    logs = []

    for data0 in datas_aportes:
        data_aporte = encontrar_proxima_data_valida(pd.Timestamp(data0), precos)
        if data_aporte is None:
            continue

        meses += 1

        for t in tickers:
            px = _get_price(precos, data_aporte, t)
            if px is None:
                continue

            s = divs.get(t, pd.Series(dtype="float64"))
            if not s.empty:
                div_mes = float(
                    s[(s.index.year == data_aporte.year) &
                      (s.index.month == data_aporte.month)].sum()
                )
            else:
                div_mes = 0.0

            reinvest = div_mes * carteira[t]
            aporte_total = (float(aporte_mensal) * cf) + reinvest
            qtd_comprada = aporte_total / px
            carteira[t] += qtd_comprada

            patrimonio = carteira[t] * px
            aportado_teorico = meses * float(aporte_mensal)
            multiplicador = (
                patrimonio / aportado_teorico
                if aportado_teorico > 0 else np.nan
            )

            flag_preco = px < min_preco_aceitavel
            flag_div = div_mes > max_div_por_acao_mes
            flag_mult = multiplicador > max_multiplicador_patrimonio

            if flag_preco or flag_div or flag_mult:
                logs.append({
                    "data": data_aporte,
                    "ticker": t,
                    "preco": px,
                    "div_mes": div_mes,
                    "reinvest": reinvest,
                    "aporte_total": aporte_total,
                    "qtd_total": carteira[t],
                    "patrimonio": patrimonio,
                    "aportado_teorico": aportado_teorico,
                    "multiplicador": multiplicador,
                    "flag_preco": flag_preco,
                    "flag_div": flag_div,
                    "flag_mult": flag_mult,
                })

    if not logs:
        return pd.DataFrame(
            columns=[
                "data", "ticker", "preco", "div_mes", "reinvest",
                "aporte_total", "qtd_total", "patrimonio",
                "aportado_teorico", "multiplicador",
                "flag_preco", "flag_div", "flag_mult"
            ]
        )

    return (
        pd.DataFrame(logs)
        .sort_values(["ticker", "data"])
        .reset_index(drop=True)
    )
