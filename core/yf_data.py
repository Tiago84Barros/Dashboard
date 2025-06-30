from __future__ import annotations

from functools import lru_cache
from typing import Dict, List, Sequence, Tuple

import pandas as pd
import yfinance as yf
from datetime import datetime
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

    frames = {}
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
def baixar_precos_full_calendar(tickers, start="2010-01-01", end=None, fill_missing=True):
    """
    Download adjusted daily close prices and reindex to every calendar day.
    """
    # 1) Fetch data once
    df = yf.download(
        tickers,
        start=start,
        end=end,
        auto_adjust=True,
        progress=False
    )["Close"]

    # 2) Clean up
    df.columns = df.columns.str.replace(".SA", "", regex=False)
    df.dropna(how="all", inplace=True)
    df.index = pd.to_datetime(df.index)

    # 3) Create full daily calendar
    full_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq='D')
    df_full = df.reindex(full_range)

    # 4) Forward-fill if desired
    if fill_missing:
        df_full.ffill(inplace=True)

    df_full.index.name = 'Date'
    return df_full



# Usado no módulo criar_portfolio --------------------------------------------------------------------
def baixar_precos_ano_corrente(tickers):
    from datetime import datetime
    import yfinance as yf
    import pandas as pd
    import streamlit as st

    ano_corrente = datetime.now().year
    start = f"{ano_corrente}-01-01"

    try:
        # Baixa preços ajustados diretamente
        precos = yf.download(
            tickers,
            start=start,
            auto_adjust=True,
            progress=False
        )['Close']

        # Se for Series (1 ativo), transforma em DataFrame
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

# _____________________ indicadores ____________________________________
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

# ────────────────────────── __all__ ─────────────────────────
__all__: List[str] = [
    "get_company_info", 
    "baixar_precos", 
    "baixar_precos_ano_corrente",
    "coletar_dividendos",
    "get_price",
    "get_fundamentals_yf",
]
