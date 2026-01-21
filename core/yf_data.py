from __future__ import annotations

"""
Camada de mercado via yfinance (B3).

Objetivos desta versão:
- Não depender de Streamlit (mas usar st.cache_data se disponível).
- Remover duplicidades e padronizar retornos.
- Evitar "end" fixo que compromete reprodutibilidade e alinhamento temporal.
- Robustez para 1 ticker vs múltiplos tickers.
- Robustez contra Rate Limit do Yahoo (YFRateLimitError).
"""

from functools import lru_cache
from typing import Dict, List, Sequence, Tuple, Optional, Union
import logging
from datetime import datetime
import time

import pandas as pd
import numpy as np
import yfinance as yf

logger = logging.getLogger(__name__)

# yfinance rate-limit exception (nem sempre disponível dependendo da versão)
try:
    from yfinance.exceptions import YFRateLimitError  # type: ignore
except Exception:  # pragma: no cover
    YFRateLimitError = Exception  # fallback conservador


# ────────────────────────── Util ────────────────────────────
def _norm(ticker: str) -> str:
    """Normaliza ticker para padrão B3 no Yahoo Finance (.SA)."""
    t = (ticker or "").strip().upper()
    if not t:
        return t
    return t if t.endswith(".SA") else f"{t}.SA"


def _strip_sa(col: str) -> str:
    """Remove sufixo .SA do nome da coluna."""
    return col.replace(".SA", "")


# ────────────────────────── Rate limit guard ─────────────────
# Cooldown global quando detecta YFRateLimitError.
# Quando Streamlit estiver disponível, também persistimos isso em st.session_state.
_RATE_LIMIT_UNTIL_TS: float = 0.0
_COOLDOWN_SECONDS_DEFAULT = 30 * 60  # 30 minutos


def _now_ts() -> float:
    return time.time()


def _is_rate_limited() -> bool:
    global _RATE_LIMIT_UNTIL_TS
    return _now_ts() < float(_RATE_LIMIT_UNTIL_TS)


def _set_rate_limit_cooldown(seconds: int = _COOLDOWN_SECONDS_DEFAULT) -> None:
    global _RATE_LIMIT_UNTIL_TS
    _RATE_LIMIT_UNTIL_TS = _now_ts() + int(seconds)


# ────────────────────────── Cache abstrato ──────────────────
try:
    import streamlit as st  # type: ignore

    _ST_AVAILABLE = True
    _SS_KEY = "yf_rate_limit_until_ts"

    def _sync_rate_limit_from_session() -> None:
        """
        Sincroniza o cooldown do módulo com o session_state (por sessão),
        para não martelar o Yahoo durante o rate limit.
        """
        global _RATE_LIMIT_UNTIL_TS
        try:
            until = float(st.session_state.get(_SS_KEY, 0.0))
            if until > _RATE_LIMIT_UNTIL_TS:
                _RATE_LIMIT_UNTIL_TS = until
        except Exception:
            pass

    def _sync_rate_limit_to_session() -> None:
        global _RATE_LIMIT_UNTIL_TS
        try:
            st.session_state[_SS_KEY] = float(_RATE_LIMIT_UNTIL_TS)
        except Exception:
            pass

    def _cache(ttl_seconds: int = 6 * 60 * 60):
        """
        Cache com TTL para não congelar respostas vazias em caso de falha/ratelimit.
        """
        def deco(func):
            return st.cache_data(ttl=ttl_seconds, show_spinner=False)(func)  # pragma: no cover
        return deco

except Exception:  # sem streamlit
    _ST_AVAILABLE = False

    def _cache(ttl_seconds: int = 0):
        """
        Fallback sem Streamlit: LRU cache simples.
        TTL é ignorado nesse modo.
        """
        def deco(func):
            return lru_cache(maxsize=128)(func)
        return deco


# ────────────────────────── get_company_info ────────────────
@_cache(ttl_seconds=24 * 60 * 60)  # 24h
def get_company_info(ticker: str) -> Tuple[Optional[str], Optional[str]]:
    """Retorna (nome, website) quando disponíveis."""
    if _ST_AVAILABLE:
        _sync_rate_limit_from_session()
    if _is_rate_limited():
        return None, None

    try:
        info = yf.Ticker(_norm(ticker)).info
        nome = info.get("longName") or info.get("shortName")
        website = info.get("website")
        return nome, website

    except YFRateLimitError as e:
        logger.warning("Rate limit (get_company_info) para %s: %s", ticker, e)
        _set_rate_limit_cooldown()
        if _ST_AVAILABLE:
            _sync_rate_limit_to_session()
        return None, None

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
    if _ST_AVAILABLE:
        _sync_rate_limit_from_session()
    if _is_rate_limited():
        return pd.DataFrame()

    tks = [t for t in (tickers or []) if (t or "").strip()]
    if not tks:
        return pd.DataFrame()

    tks_yf = [_norm(t) for t in tks]

    try:
        raw = yf.download(
            tickers=" ".join(tks_yf),
            start=start,
            end=end,
            progress=False,
            auto_adjust=auto_adjust,
            group_by="ticker",
            threads=True,
        )
    except YFRateLimitError as e:
        logger.warning("Rate limit (_download_prices) para %s: %s", tks_yf, e)
        _set_rate_limit_cooldown()
        if _ST_AVAILABLE:
            _sync_rate_limit_to_session()
        return pd.DataFrame()

    if raw is None or raw.empty:
        return pd.DataFrame()

    df_out: Optional[pd.DataFrame] = None

    if isinstance(raw.columns, pd.MultiIndex):
        if raw.columns.nlevels == 2:
            lvl0 = raw.columns.get_level_values(0)
            lvl1 = raw.columns.get_level_values(1)

            if price_field in set(lvl0):
                df_out = raw[price_field].copy()
            elif price_field in set(lvl1):
                df_out = raw.xs(price_field, axis=1, level=1).copy()
            else:
                for alt in ("Adj Close", "Close"):
                    if alt in set(lvl0):
                        df_out = raw[alt].copy()
                        break
                    if alt in set(lvl1):
                        df_out = raw.xs(alt, axis=1, level=1).copy()
                        break

    else:
        if price_field in raw.columns:
            df_out = raw[[price_field]].copy()
            df_out.columns = [_strip_sa(tks_yf[0])]
        else:
            col = "Adj Close" if "Adj Close" in raw.columns else ("Close" if "Close" in raw.columns else None)
            if col is None:
                return pd.DataFrame()
            df_out = raw[[col]].copy()
            df_out.columns = [_strip_sa(tks_yf[0])]

    if df_out is None or df_out.empty:
        return pd.DataFrame()

    df_out = df_out.copy()
    df_out.columns = [_strip_sa(str(c)) for c in df_out.columns]
    df_out = df_out.dropna(how="all").sort_index()
    return df_out


# ────────────────────────── baixar_precos ───────────────────
def baixar_precos(
    tickers: Union[str, Sequence[str]],
    start: str = "2010-01-01",
) -> pd.DataFrame:
    """
    Baixa preços ajustados (auto_adjust=True) a partir de `start` até hoje (padrão).
    """
    if isinstance(tickers, str):
        tickers_list = [tickers]
    else:
        tickers_list = list(tickers)

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
    """
    if isinstance(tickers, str):
        tickers_list = [tickers]
    else:
        tickers_list = list(tickers)

    ano = datetime.now().year
    start = f"{ano}-01-01"
    end = f"{ano + 1}-01-01"  # exclusivo

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
@_cache(ttl_seconds=12 * 60 * 60)  # 12h
def coletar_dividendos(tickers: Sequence[str]) -> Dict[str, pd.Series]:
    """
    Retorna dict {TICKER_SEM_SA: Series(dividendos)}, com índice datetime.
    """
    if _ST_AVAILABLE:
        _sync_rate_limit_from_session()
    if _is_rate_limited():
        return {_strip_sa(_norm(t)): pd.Series(dtype="float64") for t in (tickers or [])}

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

        except YFRateLimitError as e:
            logger.warning("Rate limit (coletar_dividendos) para %s: %s", tk, e)
            _set_rate_limit_cooldown()
            if _ST_AVAILABLE:
                _sync_rate_limit_to_session()
            result[tk] = pd.Series(dtype="float64")

        except Exception as e:
            logger.debug("coletar_dividendos falhou para %s: %s", tk, e)
            result[tk] = pd.Series(dtype="float64")
    return result


# ────────────────────────── get_price ───────────────────────
@_cache(ttl_seconds=60 * 60)  # 1h (preço muda; não cachear demais)
def get_price(ticker: str) -> Optional[float]:
    """
    Retorna último preço disponível (Close) para o ticker.
    Usa history() (mais estável que info()).
    """
    if _ST_AVAILABLE:
        _sync_rate_limit_from_session()
    if _is_rate_limited():
        return None

    try:
        stock = yf.Ticker(_norm(ticker))
        hist = stock.history(period="5d", auto_adjust=True)
        if hist is None or hist.empty or "Close" not in hist.columns:
            return None
        val = float(hist["Close"].dropna().iloc[-1])
        return val if np.isfinite(val) else None

    except YFRateLimitError as e:
        logger.warning("Rate limit (get_price) para %s: %s", ticker, e)
        _set_rate_limit_cooldown()
        if _ST_AVAILABLE:
            _sync_rate_limit_to_session()
        return None

    except Exception as e:
        logger.debug("get_price falhou para %s: %s", ticker, e)
        return None


# ────────────────────────── indicadores via info ────────────
@_cache(ttl_seconds=12 * 60 * 60)  # 12h
def get_fundamentals_yf(ticker: str) -> pd.DataFrame:
    """
    Extrai indicadores do yfinance.info e retorna como DataFrame (1 linha).

    IMPORTANTE:
    - .info é o endpoint mais sujeito a bloqueio (rate limit).
    - Quando houver rate limit, retornamos DF com Nones para o UI usar fallback do DB.
    """
    if _ST_AVAILABLE:
        _sync_rate_limit_from_session()
    if _is_rate_limited():
        df = pd.DataFrame([{
            "Margem_Liquida": None,
            "Margem_Operacional": None,
            "ROE": None,
            "ROIC": None,
            "DY": None,
            "P/VP": None,
            "Payout": None,
            "P/L": None,
            "Endividamento_Total": None,
            "Alavancagem_Financeira": None,
            "Liquidez_Corrente": None,
            "Ticker": str(ticker).strip().upper(),
            "Data": pd.Timestamp.today().normalize(),
        }])
        return df

    try:
        info = yf.Ticker(_norm(ticker)).info

    except YFRateLimitError as e:
        logger.warning("Rate limit (get_fundamentals_yf) para %s: %s", ticker, e)
        _set_rate_limit_cooldown()
        if _ST_AVAILABLE:
            _sync_rate_limit_to_session()
        info = {}

    except Exception as e:
        logger.debug("get_fundamentals_yf falhou para %s: %s", ticker, e)
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

    # DY no Yahoo costuma vir em fração (0.05=5%). Seu UI trata DY como %.
    # Aqui padronizamos para porcentagem.
    def dy_to_pct(val):
        try:
            if val is None:
                return None
            v = float(val)
            if not np.isfinite(v):
                return None
            return round(v * 100.0, 2) if v <= 1.5 else round(v, 2)
        except Exception:
            return None

    data = {
        "Margem_Liquida": percent(info.get("profitMargins")),
        "Margem_Operacional": percent(info.get("operatingMargins")),
        "ROE": percent(info.get("returnOnEquity")),
        "ROIC": percent(info.get("returnOnCapitalEmployed")),
        "DY": dy_to_pct(info.get("dividendYield")),
        "P/VP": as_float(info.get("priceToBook")),
        "Payout": percent(info.get("payoutRatio")),
        "P/L": as_float(info.get("trailingPE")),
        # Campos que o Yahoo pode não ter no mesmo conceito do seu DB:
        "Endividamento_Total": None,
        # OBS: esse campo no seu arquivo original estava mapeado para leveredFreeCashFlow,
        # o que NÃO é "alavancagem financeira" no sentido clássico. Mantive para não quebrar,
        # mas recomendo você obter isso do DB.
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
