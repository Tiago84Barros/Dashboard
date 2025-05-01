"""yf_data.py
~~~~~~~~~~~~
Utilitários finos para coletar cotações, dividendos e informações de empresas
via *yfinance*.

Funções públicas
----------------
- get_company_info(ticker)
- baixar_precos(tickers, start="2010-01-01", end=None, auto_adjust=True)
- coletar_dividendos(tickers)

Dependências: yfinance, pandas
"""

from __future__ import annotations

from typing import Dict, List, Tuple, Sequence
import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------------
# Helpers --------------------------------------------------------------------
# ---------------------------------------------------------------------------

def _normalize_ticker(ticker: str, suffix: str = ".SA") -> str:
    """Garante *ticker* no formato aceito pelo Yahoo (B3 = ".SA")."""
    return ticker if ticker.endswith(suffix) else f"{ticker}{suffix}"

# ---------------------------------------------------------------------------
# 1. Informações da empresa ---------------------------------------------------
# ---------------------------------------------------------------------------

def get_company_info(ticker: str) -> Tuple[str | None, str | None]:
    """Retorna (nome completo, website) da empresa. Se falhar ➜ (None, None)."""
    try:
        tk_norm = _normalize_ticker(ticker)
        info = yf.Ticker(tk_norm).info  # type: ignore[attr-defined]
        return info.get("longName"), info.get("website")
    except Exception:
        return None, None

# ---------------------------------------------------------------------------
# 2. Preços ajustados ---------------------------------------------------------
# ---------------------------------------------------------------------------

def baixar_precos(
    tickers: Sequence[str],
    start: str | pd.Timestamp = "2010-01-01",
    end: str | pd.Timestamp | None = None,
    auto_adjust: bool = True,
) -> pd.DataFrame | None:
    """Baixa preços *Close* ajustados de vários tickers.

    - *tickers* pode ser lista ou string com espaço.
    - Remove sufixo ".SA" das colunas de retorno.

    Retorna DataFrame indexado por data ou **None** em caso de erro.
    """
    if isinstance(tickers, str):
        tickers = [tickers]
    tickers_norm = [_normalize_ticker(t) for t in tickers]
    try:
        df = yf.download(tickers_norm, start=start, end=end, auto_adjust=auto_adjust)["Close"]
        if df.empty:
            return None
        df.columns = [col.replace(".SA", "") for col in df.columns]
        df.dropna(how="all", inplace=True)
        return df
    except Exception as exc:
        print(f"Erro ao baixar preços: {exc}")
        return None

# ---------------------------------------------------------------------------
# 3. Dividendos históricos ----------------------------------------------------
# ---------------------------------------------------------------------------

def coletar_dividendos(tickers: Sequence[str]) -> Dict[str, pd.Series]:
    """Retorna dict {ticker: Series de dividendos mensais}."""
    dividendos_dict: Dict[str, pd.Series] = {}
    for tk in tickers:
        try:
            tk_norm = _normalize_ticker(tk)
            div = yf.Ticker(tk_norm).dividends  # type: ignore[attr-defined]
            if div.empty:
                dividendos_dict[tk] = pd.Series(dtype="float64")
                continue
            div.index = pd.to_datetime(div.index)
            div_mensal = div.resample("M").sum()
            dividendos_dict[tk] = div_mensal
        except Exception as exc:
            print(f"Erro ao coletar dividendos de {tk}: {exc}")
            dividendos_dict[tk] = pd.Series(dtype="float64")
    return dividendos_dict

# ---------------------------------------------------------------------------
__all__ = [
    "get_company_info",
    "baixar_precos",
    "coletar_dividendos",
]
