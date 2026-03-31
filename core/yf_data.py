from __future__ import annotations

"""
Camada de mercado via yfinance (B3).

Objetivos desta versão:
- Não depender de Streamlit (mas usar st.cache_data se disponível).
- Remover duplicidades e padronizar retornos.
- Evitar "end" fixo que compromete reprodutibilidade e alinhamento temporal.
- Robustez para 1 ticker vs múltiplos tickers.
"""

from functools import lru_cache
from typing import Dict, List, Sequence, Tuple, Optional, Union
import logging
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import yfinance as yf

logger = logging.getLogger(__name__)

from core.ticker_utils import normalize_ticker, add_sa_suffix  # noqa: E402


# ────────────────────────── Util ────────────────────────────
def _norm(ticker: str) -> str:
    """Normaliza ticker para padrão B3 no Yahoo Finance (.SA)."""
    return add_sa_suffix(ticker)


def _strip_sa(col: str) -> str:
    """Remove sufixo .SA do nome da coluna."""
    return col.replace(".SA", "")


# ────────────────────────── Cache abstrato ──────────────────
try:
    import streamlit as st  # type: ignore

    def _cache(func):
        return st.cache_data(func)  # pragma: no cover

except Exception:  # sem streamlit
    def _cache(func):
        return lru_cache(maxsize=128)(func)


# ────────────────────────── get_company_info ────────────────
@_cache
def get_company_info(ticker: str) -> Tuple[Optional[str], Optional[str]]:
    """Retorna (nome, website) quando disponíveis."""
    try:
        info = yf.Ticker(_norm(ticker)).info
        nome = info.get("longName") or info.get("shortName")
        website = info.get("website")
        return nome, website
    except Exception as e:
        logger.debug("get_company_info falhou para %s: %s", ticker, e)
        return None, None


# ────────────────────────── Core downloader ─────────────────
def _download_prices(
    tickers: Sequence[str],
    start: Union[str, pd.Timestamp] = "2010-01-01",
    end: Optional[Union[str, pd.Timestamp]] = None,
    auto_adjust: bool = True,
    price_field: str = "Close",
) -> pd.DataFrame:
    """Baixa preços via yfinance, retornando DataFrame (index: datas; cols: tickers sem .SA)."""
    tks = [t for t in (tickers or []) if (t or "").strip()]
    if not tks:
        return pd.DataFrame()

    # Normaliza para o Yahoo
    tks_yf = [_norm(t) for t in tks]

    raw = yf.download(
        tickers=" ".join(tks_yf),
        start=start,
        end=end,
        progress=False,
        auto_adjust=auto_adjust,
        group_by="ticker",
        threads=True,
    )

    if raw is None or raw.empty:
        return pd.DataFrame()

    # Caso comum: MultiIndex quando múltiplos tickers
    # Ex.: raw.columns = (('Close','ABEV3.SA'), ('Close','VALE3.SA'), ...)
    # ou (('ABEV3.SA','Close'), ...)
    # O yfinance varia; tratamos os dois.
    df_out: Optional[pd.DataFrame] = None

    if isinstance(raw.columns, pd.MultiIndex):
        # tenta padrão: primeiro nível = atributo (Close/Adj Close/...)
        if raw.columns.nlevels == 2:
            lvl0 = raw.columns.get_level_values(0)
            lvl1 = raw.columns.get_level_values(1)

            if price_field in set(lvl0):
                # colunas como (Close, TICKER)
                df_out = raw[price_field].copy()
            elif price_field in set(lvl1):
                # colunas como (TICKER, Close)
                df_out = raw.xs(price_field, axis=1, level=1).copy()
            else:
                # fallback: tenta "Adj Close" ou "Close"
                for alt in ("Adj Close", "Close"):
                    if alt in set(lvl0):
                        df_out = raw[alt].copy()
                        break
                    if alt in set(lvl1):
                        df_out = raw.xs(alt, axis=1, level=1).copy()
                        break

    else:
        # Caso de 1 ticker: colunas simples (Open/High/Low/Close/...)
        if price_field in raw.columns:
            df_out = raw[[price_field]].copy()
            # nome da coluna vira o ticker original (sem .SA) para consistência
            df_out.columns = [_strip_sa(tks_yf[0])]
        else:
            # fallback
            col = "Adj Close" if "Adj Close" in raw.columns else ("Close" if "Close" in raw.columns else None)
            if col is None:
                return pd.DataFrame()
            df_out = raw[[col]].copy()
            df_out.columns = [_strip_sa(tks_yf[0])]

    if df_out is None or df_out.empty:
        return pd.DataFrame()

    # Normaliza nomes de colunas removendo .SA
    df_out = df_out.copy()
    df_out.columns = [_strip_sa(str(c)) for c in df_out.columns]

    # Limpeza: remove linhas onde todos os preços são NaN
    df_out = df_out.dropna(how="all")

    # Ordena índice
    df_out = df_out.sort_index()

    return df_out


# ────────────────────────── baixar_precos ───────────────────
def baixar_precos(
    tickers: Union[str, Sequence[str]],
    start: str = "2010-01-01",
) -> pd.DataFrame:
    """
    Baixa preços ajustados (auto_adjust=True) a partir de `start` até hoje (padrão).

    Retorna:
      DataFrame com colunas sem ".SA".
      Se não houver dados, retorna DataFrame vazio.
    """
    if isinstance(tickers, str):
        tickers_list = [tickers]
    else:
        tickers_list = list(tickers)

    # Para evitar inconsistência temporal: por padrão, end = "amanhã" (inclui pregão de hoje se disponível),
    # sem travar em um ano futuro fixo.
    end = (pd.Timestamp.today().normalize() + pd.Timedelta(days=1)).date().isoformat()

    try:
        df = _download_prices(
            tickers_list,
            start=start,
            end=end,
            auto_adjust=True,
            price_field="Close",
        )
        return df
    except Exception as e:
        logger.exception("Erro ao baixar preços: %s", e)
        return pd.DataFrame()


# ────────────────────────── baixar_precos_ano_corrente ──────
def baixar_precos_ano_corrente(tickers: Union[str, Sequence[str]]) -> pd.DataFrame:
    """
    Baixa preços ajustados (auto_adjust=True) do ano corrente.

    Retorna:
      DataFrame com colunas sem ".SA" (ou vazio).
    """
    if isinstance(tickers, str):
        tickers_list = [tickers]
    else:
        tickers_list = list(tickers)

    ano = datetime.now().year
    start = f"{ano}-01-01"
    end = f"{ano + 1}-01-01"  # exclusivo (melhor do que 12-31, evita timezone/fechamentos)

    try:
        df = _download_prices(
            tickers_list,
            start=start,
            end=end,
            auto_adjust=True,
            price_field="Close",
        )
        return df
    except Exception as e:
        logger.exception("Erro ao baixar preços do ano atual: %s", e)
        return pd.DataFrame()


# ────────────────────────── coletar_dividendos ──────────────
@_cache
def coletar_dividendos(tickers: Sequence[str]) -> Dict[str, pd.Series]:
    """
    Retorna dict {TICKER_SEM_SA: Series(dividendos)}, com índice datetime.
    """
    result: Dict[str, pd.Series] = {}
    for t in tickers:
        tk_yf = _norm(t)
        tk = _strip_sa(tk_yf)
        try:
            div = yf.Ticker(tk_yf).dividends
            if div is None or len(div) == 0:
                result[tk] = pd.Series(dtype="float64")
                continue
            div = div.copy()
            div.index = pd.to_datetime(div.index, errors="coerce")
            div = div.dropna()
            result[tk] = div.astype(float)
        except Exception as e:
            logger.debug("coletar_dividendos falhou para %s: %s", tk, e)
            result[tk] = pd.Series(dtype="float64")
    return result


# ────────────────────────── get_price ───────────────────────
@_cache
def get_price(ticker: str) -> Optional[float]:
    """
    Retorna último preço disponível (Close) para o ticker.
    """
    try:
        stock = yf.Ticker(_norm(ticker))
        hist = stock.history(period="5d", auto_adjust=True)
        if hist is None or hist.empty or "Close" not in hist.columns:
            return None
        val = float(hist["Close"].dropna().iloc[-1])
        return val if np.isfinite(val) else None
    except Exception as e:
        logger.debug("get_price falhou para %s: %s", ticker, e)
        return None


# ────────────────────────── indicadores via info ────────────
@_cache
def get_fundamentals_yf(ticker: str) -> pd.DataFrame:
    """
    Extrai indicadores do yfinance.info e retorna como DataFrame (1 linha).
    Observação: esses dados variam por ativo e podem vir incompletos.
    """
    try:
        info = yf.Ticker(_norm(ticker)).info
    except Exception:
        info = {}

    def percent(val):
        try:
            return round(float(val) * 100.0, 2)
        except (TypeError, ValueError):
            return None

    def as_float(val):
        try:
            v = float(val)
            return v if np.isfinite(v) else None
        except (TypeError, ValueError):
            return None

    data = {
        "Margem_Liquida": percent(info.get("profitMargins")),
        "Margem_Operacional": percent(info.get("operatingMargins")),
        "ROE": percent(info.get("returnOnEquity")),
        "ROIC": percent(info.get("returnOnCapitalEmployed")),
        "DY": as_float(info.get("dividendYield")),
        "P/VP": as_float(info.get("priceToBook")),
        "Payout": percent(info.get("payoutRatio")),
        "P/L": as_float(info.get("trailingPE")),
        # Campos que o Yahoo pode não ter no mesmo conceito do seu DB:
        "Endividamento_Total": None,
        "Alavancagem_Financeira": as_float(info.get("leveredFreeCashFlow")),
        "Liquidez_Corrente": as_float(info.get("currentRatio")),
    }

    df = pd.DataFrame([data])
    df["Ticker"] = str(ticker).strip().upper()
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
