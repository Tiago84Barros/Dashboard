from __future__ import annotations

"""
Camada de mercado via yfinance (B3).

PATCH (2026-01):
- Cache TTL manual (não congela vazio quando falha).
- Chunking + pausa entre chunks para reduzir 429.
- threads=False no yf.download (reduz agressividade).
- Cache de dividendos por ticker (TTL) sem cachear falha como definitivo.
- Mantém assinaturas públicas para minimizar mudanças no app.
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

try:
    from yfinance.exceptions import YFRateLimitError  # type: ignore
except Exception:  # pragma: no cover
    YFRateLimitError = Exception


# ────────────────────────── Util ────────────────────────────
def _norm(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    if not t:
        return t
    return t if t.endswith(".SA") else f"{t}.SA"


def _strip_sa(col: str) -> str:
    return col.replace(".SA", "")


def _looks_like_rate_limit(exc: Exception) -> bool:
    msg = str(exc).lower()
    return ("429" in msg) or ("too many requests" in msg) or ("rate limit" in msg) or ("ratelimit" in msg)


# ────────────────────────── Rate limit guard ─────────────────
_RATE_LIMIT_UNTIL_TS: float = 0.0
_COOLDOWN_SECONDS_DEFAULT = 30 * 60  # 30 minutos


def _now_ts() -> float:
    return time.time()


def _is_rate_limited() -> bool:
    return _now_ts() < float(_RATE_LIMIT_UNTIL_TS)


def _set_rate_limit_cooldown(seconds: int = _COOLDOWN_SECONDS_DEFAULT) -> None:
    global _RATE_LIMIT_UNTIL_TS
    _RATE_LIMIT_UNTIL_TS = _now_ts() + int(seconds)


# ────────────────────────── Streamlit sync (opcional) ───────
try:
    import streamlit as st  # type: ignore

    _ST_AVAILABLE = True
    _SS_KEY = "yf_rate_limit_until_ts"

    def _sync_rate_limit_from_session() -> None:
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

except Exception:
    _ST_AVAILABLE = False

    def _sync_rate_limit_from_session() -> None:
        return

    def _sync_rate_limit_to_session() -> None:
        return


# ────────────────────────── Cache TTL manual ─────────────────
# IMPORTANTE: este cache só grava SUCESSO (df não vazio / series não vazia)
# evitando "congelar vazio" em caso de falha/ratelimit.
_PRICE_CACHE: Dict[str, Dict[str, object]] = {}
_DIV_CACHE: Dict[str, Dict[str, object]] = {}

def _price_key(tickers: Sequence[str], start: str, end: str, auto_adjust: bool, price_field: str) -> str:
    t = sorted({_strip_sa(_norm(x)) for x in tickers if (x or "").strip()})
    return f"PX|{start}|{end}|{int(auto_adjust)}|{price_field}|" + ",".join(t)

def _cache_get_price(key: str, ttl_seconds: int) -> Optional[pd.DataFrame]:
    hit = _PRICE_CACHE.get(key)
    if not hit:
        return None
    ts = float(hit.get("ts", 0.0))
    if (_now_ts() - ts) > ttl_seconds:
        return None
    df = hit.get("df")
    if isinstance(df, pd.DataFrame) and not df.empty:
        return df
    return None

def _cache_set_price(key: str, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    _PRICE_CACHE[key] = {"ts": _now_ts(), "df": df}

def _cache_get_div(ticker_clean: str, ttl_seconds: int) -> Optional[pd.Series]:
    hit = _DIV_CACHE.get(ticker_clean)
    if not hit:
        return None
    ts = float(hit.get("ts", 0.0))
    if (_now_ts() - ts) > ttl_seconds:
        return None
    s = hit.get("s")
    if isinstance(s, pd.Series) and not s.empty:
        return s
    return None

def _cache_set_div(ticker_clean: str, s: pd.Series) -> None:
    if s is None or s.empty:
        return
    _DIV_CACHE[ticker_clean] = {"ts": _now_ts(), "s": s}


# ────────────────────────── get_company_info ────────────────
@lru_cache(maxsize=256)
def get_company_info(ticker: str) -> Tuple[Optional[str], Optional[str]]:
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
        if _looks_like_rate_limit(e):
            logger.warning("Possível rate limit (get_company_info) para %s: %s", ticker, e)
            _set_rate_limit_cooldown()
            if _ST_AVAILABLE:
                _sync_rate_limit_to_session()
            return None, None
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
    """Baixa preços via yfinance, retornando DF (index datas; cols tickers sem .SA)."""
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
            threads=False,  # PATCH: menos agressivo -> menos 429
        )
    except YFRateLimitError as e:
        logger.warning("Rate limit (_download_prices) para %s: %s", tks_yf, e)
        _set_rate_limit_cooldown()
        if _ST_AVAILABLE:
            _sync_rate_limit_to_session()
        return pd.DataFrame()
    except Exception as e:
        if _looks_like_rate_limit(e):
            logger.warning("Possível rate limit (_download_prices) para %s: %s", tks_yf, e)
            _set_rate_limit_cooldown()
            if _ST_AVAILABLE:
                _sync_rate_limit_to_session()
            return pd.DataFrame()
        logger.debug("_download_prices falhou para %s: %s", tks_yf, e)
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


def _download_prices_batched(
    tickers: Sequence[str],
    start: str,
    end: str,
    auto_adjust: bool = True,
    price_field: str = "Close",
    chunk_size: int = 80,
    pause_seconds: float = 1.25,
    max_retries: int = 2,
) -> pd.DataFrame:
    """PATCH: baixa em chunks para reduzir 429 e consolida colunas."""
    tks = [t for t in (tickers or []) if (t or "").strip()]
    if not tks:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []

    for i in range(0, len(tks), chunk_size):
        chunk = tks[i:i + chunk_size]

        last_exc: Optional[Exception] = None
        for attempt in range(max_retries + 1):
            try:
                df = _download_prices(chunk, start=start, end=end, auto_adjust=auto_adjust, price_field=price_field)
                if df is not None and not df.empty:
                    frames.append(df)
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                msg = str(e)
                if ("Too Many Requests" in msg) or ("YFRateLimitError" in msg) or _looks_like_rate_limit(e):
                    time.sleep(5 + attempt * 5)
                else:
                    time.sleep(1 + attempt * 1)

        if last_exc is not None:
            logger.debug("Chunk falhou (%s-%s): %s", i, i + chunk_size, last_exc)

        time.sleep(pause_seconds)

    if not frames:
        return pd.DataFrame()

    df_all = pd.concat(frames, axis=1)
    df_all = df_all.loc[:, ~df_all.columns.duplicated()].copy()
    df_all = df_all.sort_index()
    return df_all


# ────────────────────────── baixar_precos ───────────────────
def baixar_precos(
    tickers: Union[str, Sequence[str]],
    start: str = "2010-01-01",
) -> pd.DataFrame:
    """
    Baixa preços ajustados (auto_adjust=True) a partir de `start` até hoje.
    PATCH: cache TTL manual + chunking.
    """
    if isinstance(tickers, str):
        tickers_list = [tickers]
    else:
        tickers_list = list(tickers)

    if _ST_AVAILABLE:
        _sync_rate_limit_from_session()
    if _is_rate_limited():
        return pd.DataFrame()

    end = (pd.Timestamp.today().normalize() + pd.Timedelta(days=1)).date().isoformat()
    key = _price_key(tickers_list, start, end, True, "Close")

    cached = _cache_get_price(key, ttl_seconds=6 * 60 * 60)  # 6h
    if cached is not None:
        return cached

    try:
        df = _download_prices_batched(
            tickers_list,
            start=start,
            end=end,
            auto_adjust=True,
            price_field="Close",
            chunk_size=80,
            pause_seconds=1.25,
            max_retries=2,
        )
        if df is not None and not df.empty:
            _cache_set_price(key, df)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        logger.exception("Erro ao baixar preços: %s", e)
        return pd.DataFrame()


# ────────────────────────── baixar_precos_ano_corrente ──────
def baixar_precos_ano_corrente(tickers: Union[str, Sequence[str]]) -> pd.DataFrame:
    """
    Mantida por compatibilidade.
    PATCH: cache TTL manual (1h) + chunking.
    """
    if isinstance(tickers, str):
        tickers_list = [tickers]
    else:
        tickers_list = list(tickers)

    if _ST_AVAILABLE:
        _sync_rate_limit_from_session()
    if _is_rate_limited():
        return pd.DataFrame()

    ano = datetime.now().year
    start = f"{ano}-01-01"
    end = f"{ano + 1}-01-01"

    key = _price_key(tickers_list, start, end, True, "Close")
    cached = _cache_get_price(key, ttl_seconds=60 * 60)  # 1h
    if cached is not None:
        return cached

    try:
        df = _download_prices_batched(
            tickers_list,
            start=start,
            end=end,
            auto_adjust=True,
            price_field="Close",
            chunk_size=80,
            pause_seconds=1.25,
            max_retries=2,
        )
        if df is not None and not df.empty:
            _cache_set_price(key, df)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        logger.exception("Erro ao baixar preços do ano atual: %s", e)
        return pd.DataFrame()


# ────────────────────────── coletar_dividendos ──────────────
def coletar_dividendos(tickers: Sequence[str]) -> Dict[str, pd.Series]:
    """
    Retorna dict {TICKER_SEM_SA: Series(dividendos)} com índice datetime.
    PATCH: cache por ticker (12h) e não cacheia vazio/erro como definitivo.
    """
    if _ST_AVAILABLE:
        _sync_rate_limit_from_session()
    if _is_rate_limited():
        return {_strip_sa(_norm(t)): pd.Series(dtype="float64") for t in (tickers or [])}

    ttl_seconds = 12 * 60 * 60
    result: Dict[str, pd.Series] = {}

    for t in (tickers or []):
        tk_yf = _norm(t)
        tk = _strip_sa(tk_yf)

        hit = _cache_get_div(tk, ttl_seconds=ttl_seconds)
        if hit is not None:
            result[tk] = hit
            continue

        try:
            div = yf.Ticker(tk_yf).dividends
            if div is None or len(div) == 0:
                # IMPORTANTE: não cacheia vazio
                result[tk] = pd.Series(dtype="float64")
                continue

            div = div.copy()
            div.index = pd.to_datetime(div.index, errors="coerce")
            div = div.dropna()
            div = div.astype(float)

            if not div.empty:
                _cache_set_div(tk, div)

            result[tk] = div if not div.empty else pd.Series(dtype="float64")

        except YFRateLimitError as e:
            logger.warning("Rate limit (coletar_dividendos) para %s: %s", tk, e)
            _set_rate_limit_cooldown()
            if _ST_AVAILABLE:
                _sync_rate_limit_to_session()
            result[tk] = pd.Series(dtype="float64")

        except Exception as e:
            if _looks_like_rate_limit(e):
                logger.warning("Possível rate limit (coletar_dividendos) para %s: %s", tk, e)
                _set_rate_limit_cooldown()
                if _ST_AVAILABLE:
                    _sync_rate_limit_to_session()
                result[tk] = pd.Series(dtype="float64")
                continue

            logger.debug("coletar_dividendos falhou para %s: %s", tk, e)
            result[tk] = pd.Series(dtype="float64")

    return result


# ────────────────────────── get_price ───────────────────────
@lru_cache(maxsize=512)
def get_price(ticker: str) -> Optional[float]:
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
        if _looks_like_rate_limit(e):
            logger.warning("Possível rate limit (get_price) para %s: %s", ticker, e)
            _set_rate_limit_cooldown()
            if _ST_AVAILABLE:
                _sync_rate_limit_to_session()
        logger.debug("get_price falhou para %s: %s", ticker, e)
        return None


# ────────────────────────── indicadores via info ────────────
@lru_cache(maxsize=512)
def get_fundamentals_yf(ticker: str) -> pd.DataFrame:
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
        if _looks_like_rate_limit(e):
            logger.warning("Possível rate limit (get_fundamentals_yf) para %s: %s", ticker, e)
            _set_rate_limit_cooldown()
            if _ST_AVAILABLE:
                _sync_rate_limit_to_session()
            info = {}
        else:
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
        "Endividamento_Total": None,
        "Alavancagem_Financeira": as_float(info.get("leveredFreeCashFlow")),
        "Liquidez_Corrente": as_float(info.get("currentRatio")),
    }

    df = pd.DataFrame([data])
    df["Ticker"] = str(ticker).strip().upper()
    df["Data"] = pd.Timestamp.today().normalize()
    return df
    
def get_yahoo_status() -> dict:
    """
    Retorna status simples para debug/telemetria no UI.
    """
    global _RATE_LIMIT_UNTIL_TS
    now = _now_ts()
    until = float(_RATE_LIMIT_UNTIL_TS)
    remaining = max(0.0, until - now)
    return {
        "rate_limited": bool(remaining > 0),
        "cooldown_remaining_seconds": int(remaining),
        "cooldown_until_ts": until,
    }


__all__: List[str] = [
    "get_company_info",
    "baixar_precos",
    "baixar_precos_ano_corrente",
    "coletar_dividendos",
    "get_price",
    "get_fundamentals_yf",
    "get_yahoo_status",
]
