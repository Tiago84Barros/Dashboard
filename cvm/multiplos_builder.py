from __future__ import annotations

import datetime as dt
import time
from dataclasses import dataclass

import numpy as np
import pandas as pd
import yfinance as yf
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class MultiplosConfig:
    dfp_table: str = "cvm.demonstracoes_financeiras_dfp"
    out_table: str = "cvm.multiplos"
    price_lookback_years: int = 20
    sleep_s: float = 0.2  # reduz rate limit


def _norm_yf_ticker(ticker: str) -> str:
    t = ticker.strip().upper()
    if not t.endswith(".SA"):
        t += ".SA"
    return t


def fetch_prices_daily(ticker_sa: str, years: int) -> pd.DataFrame:
    end = dt.date.today()
    start = dt.date(end.year - years, 1, 1)
    df = yf.download(ticker_sa, start=str(start), end=str(end), auto_adjust=False, progress=False)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index().rename(columns={"Date": "date", "Close": "close"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df[["date", "close"]].dropna()


def year_end_price(prices: pd.DataFrame) -> pd.DataFrame:
    """Último fechamento disponível de cada ano."""
    if prices.empty:
        return pd.DataFrame(columns=["ano", "ref_date", "price_close"])
    prices = prices.copy()
    prices["ano"] = prices["date"].dt.year
    last = prices.sort_values("date").groupby("ano", as_index=False).tail(1)
    return last.rename(columns={"date": "ref_date", "close": "price_close"})[["ano", "ref_date", "price_close"]]


def load_dfp(engine: Engine, table: str, ticker_no_sa: str) -> pd.DataFrame:
    sql = f"""
        SELECT ticker, ano, receita_liquida, ebit, lucro_liquido, lpa, patrimonio_liquido, dividendos
        FROM {table}
        WHERE ticker = :ticker
        ORDER BY ano
    """
    df = pd.read_sql(text(sql), con=engine, params={"ticker": ticker_no_sa})
    return df if df is not None else pd.DataFrame()


def build_multiplos_dfp(engine: Engine, ticker: str, cfg: MultiplosConfig = MultiplosConfig()) -> pd.DataFrame:
    t_no_sa = ticker.strip().upper().replace(".SA", "")
    t_sa = _norm_yf_ticker(t_no_sa)

    dfp = load_dfp(engine, cfg.dfp_table, t_no_sa)
    if dfp.empty:
        return pd.DataFrame()

    prices = fetch_prices_daily(t_sa, cfg.price_lookback_years)
    time.sleep(cfg.sleep_s)

    px_year = year_end_price(prices)
    if px_year.empty:
        return pd.DataFrame()

    df = dfp.merge(px_year, on="ano", how="left")

    # numéricos
    for c in ["lucro_liquido", "lpa", "patrimonio_liquido", "dividendos", "price_close"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # shares estimado via LPA
    df["shares_est"] = np.where(df["lpa"].abs() > 1e-12, df["lucro_liquido"] / df["lpa"], np.nan)

    # VPA e DPS (se possível)
    df["vpa_est"] = np.where(df["shares_est"].abs() > 1e-12, df["patrimonio_liquido"] / df["shares_est"], np.nan)
    df["dps_est"] = np.where(df["shares_est"].abs() > 1e-12, df["dividendos"] / df["shares_est"], np.nan)

    # múltiplos
    df["pl"] = np.where(df["lpa"].abs() > 1e-12, df["price_close"] / df["lpa"], np.nan)
    df["pvp"] = np.where(df["vpa_est"].abs() > 1e-12, df["price_close"] / df["vpa_est"], np.nan)
    df["dy"] = np.where(df["price_close"].abs() > 1e-12, df["dps_est"] / df["price_close"], np.nan)

    out = df[["ticker", "ano", "ref_date", "price_close", "pl", "pvp", "dy", "shares_est"]].copy()
    out["ticker"] = t_no_sa
    return out


def upsert_multiplos(engine: Engine, out: pd.DataFrame, table: str) -> None:
    """
    Para simplicidade: apaga e reinsere por ticker.
    (Depois podemos trocar para UPSERT com PK(ticker,ano)).
    """
    if out.empty:
        return

    t = out["ticker"].iloc[0]
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table} WHERE ticker = :ticker"), {"ticker": t})
        out.to_sql(table.split(".")[-1], con=conn, schema=table.split(".")[0], if_exists="append", index=False)
