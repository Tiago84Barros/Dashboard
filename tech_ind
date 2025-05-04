"""tech_ind.py
~~~~~~~~~~~~~~
Indicadores técnicos simples usados em *page_advanced*.

Funções públicas
----------------
- ema(series, period=20)
- rsi(series, period=14)
- momentum_12m(df_preco)
"""

from __future__ import annotations
import pandas as pd
import numpy as np

# ------------------------------------------------------------------ #
# Média Móvel Exponencial (EMA)                                      #
# ------------------------------------------------------------------ #
def ema(series: pd.Series, period: int = 20) -> pd.Series:
    """Retorna a EMA da *series* com span = *period*."""
    return series.ewm(span=period, adjust=False).mean()


# ------------------------------------------------------------------ #
# Relative Strength Index (RSI)                                      #
# ------------------------------------------------------------------ #
def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Calcula o RSI clássico (Wilder)."""
    delta = series.diff()

    gain = delta.clip(lower=0).rolling(window=period, min_periods=1).mean()
    loss = (-delta.clip(upper=0)).rolling(window=period, min_periods=1).mean()

    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


# ------------------------------------------------------------------ #
# Momentum 12 meses (Total‑Return se os preços incluírem dividendos) #
# ------------------------------------------------------------------ #
def momentum_12m(prices: pd.DataFrame, lookback_days: int = 252) -> pd.DataFrame:
    """
    Calcula o retorno acumulado de ~12 meses para cada coluna
    (prices / prices.shift(lookback_days)  −  1).
    """
    mom = prices / prices.shift(lookback_days) - 1
    mom = mom.dropna(how="all")
    mom.columns = [f"Momentum_{c}" for c in mom.columns]
    return mom


__all__ = ["ema", "rsi", "momentum_12m"]
