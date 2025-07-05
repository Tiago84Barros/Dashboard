from __future__ import annotations

from functools import lru_cache
from typing import Dict, List, Sequence, Tuple

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import streamlit as st

# ────────────────────────── Util ────────────────────────────
def _norm(ticker: str) -> str:
    t = ticker.upper()
    return t if t.endswith(".SA") else t + ".SA"

# ────────────────────────── Cache abstrato ──────────────────
try:
    import streamlit as st

    def _cache(func):
        return st.cache_data(func)

except ModuleNotFoundError:
    def _cache(func):
        return lru_cache(maxsize=128)(func)

# ────────────────────────── get_company_info ────────────────
@_cache
def get_company_info(ticker: str) -> Tuple[str | None, str | None]:
    try:
        info = yf.Ticker(_norm(ticker)).info
        return info.get("longName") or info.get("shortName"), info.get("website")
    except Exception:
        return None, None

# ────────────────────────── _download_prices ────────────────
def _download_prices(
    tickers: Sequence[str],
    start: str | pd.Timestamp = "2010-01-01",
    end: str | pd.Timestamp | None = None,
    auto_adjust: bool = True,
) -> pd.DataFrame:
    joined = " ".join(tickers)
    raw = yf.download(
        joined,
        start=start,
        end=end,
        progress=False,
        auto_adjust=auto_adjust,
        group_by="ticker",
        threads=True,
    )

    if raw.empty:
        return pd.DataFrame()

    frames: dict[str, pd.Series] = {}
    for t in tickers:
        try:
            if raw.columns.nlevels == 1:
                col = "Adj Close" if "Adj Close" in raw else "Close"
                frames[t] = raw[col]
                break
            else:
                frames[t] = raw[t]["Adj Close"]
        except (KeyError, TypeError):
            continue

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, axis=1)
    return df.sort_index()

# ────────────────────────── baixar_precos ───────────────────

def baixar_precos(tickers, start="2010-01-01"):
    """
    Baixa os preços das ações a partir de uma data fixa.
    
    tickers: lista de tickers das empresas.
    start: data inicial padrão (exemplo: 2010-01-01).
    
    Retorna: DataFrame com preços ajustados.
    """
    try:
        precos = yf.download(tickers, start=start, end="2025-12-31", auto_adjust=True, progress=False)["Close"]
        precos.columns = precos.columns.str.replace(".SA", "", regex=False)  # Ajustar tickers
        # Remover linhas onde todos os preços são NaN (empresas sem dados nesse período)
        precos = precos.dropna(how="all")
        return precos

    except Exception as e:
        st.error(f"Erro ao baixar preços: {e}")
        return None

# Usado no módulo criar_portfolio --------------------------------------------------------------------

def baixar_precos_ano_corrente(tickers):
    ano_corrente = datetime.now().year
    start = f"{ano_corrente}-01-01"
    end = f"{ano_corrente}-12-31"

    try:
        precos = yf.download(tickers, start=start, end=end, auto_adjust=True, progress=False)["Close"]
        if isinstance(precos, pd.Series):
            precos = precos.to_frame()
        precos.columns = precos.columns.str.replace(".SA", "", regex=False)
        precos = precos.dropna(how="all")
        return precos

    except Exception as e:
        st.error(f"Erro ao baixar preços do ano atual: {e}")
        return pd.DataFrame()

# ────────────────────────── coletar_dividendos ──────────────
@_cache
def coletar_dividendos(tickers: Sequence[str]) -> Dict[str, pd.Series]:
    result = {}
    for t in tickers:
        tk = _norm(t)
        try:
            div = yf.Ticker(tk).dividends
            div.index = pd.to_datetime(div.index)
            result[tk] = div
        except Exception:
            result[tk] = pd.Series(dtype="float64")
    return result

# ────────────────────────── get_price ─────────────────────────
@_cache
def get_price(ticker: str) -> float | None:
    try:
        stock = yf.Ticker(_norm(ticker))
        stock_info = stock.history(period="1d")
        if not stock_info.empty:
            return stock_info["Close"].iloc[-1]
    except Exception:
        pass
    return None

# ────────────────────────── indicadores ───────────────────────
@_cache
def get_fundamentals_yf(ticker: str) -> pd.DataFrame:
    """
    Extrai indicadores fundamentalistas diretamente do yfinance.info,
    retorna como DataFrame para ser usado na seção de blocos.
    """
    try:
        info = yf.Ticker(_norm(ticker)).info
    except Exception:
        info = {}

    def percent(val):
        try:
            return round(float(val) * 100, 2)
        except (TypeError, ValueError):
            return None

    data = {
        "Margem_Liquida": percent(info.get("profitMargins")),
        "Margem_Operacional": percent(info.get("operatingMargins")),
        "ROE": percent(info.get("returnOnEquity")),
        "ROIC": percent(info.get("returnOnCapitalEmployed")),
        "DY": round(info.get("dividendYield"), 2) if isinstance(info.get("dividendYield"), (int, float)) else None,
        "P/VP": info.get("priceToBook"),
        "Payout": percent(info.get("payoutRatio")),
        "P/L": info.get("trailingPE"),
        "Endividamento_Total": None,
        "Alavancagem_Financeira": info.get("leveredFreeCashFlow"),
        "Liquidez_Corrente": info.get("currentRatio"),
    }

    df = pd.DataFrame([data])
    df["Ticker"] = ticker
    df["Data"] = pd.Timestamp.today().normalize()

    return df

# ────────────────────────── NOVA FUNÇÃO get_precos_ajustados ─────────────

def get_precos_ajustados(
    ticker: str,
    start: str = "2010-01-01",
    freq: str = "M"
) -> pd.Series:
    """
    Wrapper que retorna uma série de preços ajustados para um único ticker,
    reamostrada na frequência desejada ("M" para mensal, "D" para diário).
    """
    # Baixa o DataFrame de preços ajustados
    df = baixar_precos([_norm(ticker)], start=start)
    if df is None or df.empty:
        return pd.Series(dtype="float64")

    # Seleciona a coluna sem sufixo .SA
    col = ticker if ticker in df.columns else ticker.replace(".SA", "")
    s = df[col]

    # Reamostra e pega último valor em cada período
    return s.resample(freq).last()

# ────────────────────────── __all__ ─────────────────────────
__all__: List[str] = [
    "get_company_info",
    "baixar_precos",
    "baixar_precos_ano_corrente",
    "coletar_dividendos",
    "get_price",
    "get_fundamentals_yf",
    "get_precos_ajustados",
]
