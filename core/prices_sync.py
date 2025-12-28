from __future__ import annotations

import datetime as dt
import time
from dataclasses import dataclass

import pandas as pd
import yfinance as yf
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class PricesSyncConfig:
    out_table: str = "cvm.prices_b3"
    lookback_years: int = 20
    sleep_s: float = 0.25  # ajuda a evitar rate-limit


def _norm_no_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    if t.endswith(".SA"):
        t = t[:-3]
    return t


def _to_yf(ticker: str) -> str:
    t = _norm_no_sa(ticker)
    return f"{t}.SA"


def fetch_prices_daily(ticker_sa: str, lookback_years: int) -> pd.DataFrame:
    end = dt.date.today()
    start = dt.date(end.year - lookback_years, 1, 1)

    df = yf.download(
        ticker_sa,
        start=str(start),
        end=str(end),
        auto_adjust=False,
        progress=False,
        actions=False,
        threads=False,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.reset_index()
    # yfinance normalmente retorna colunas: Date, Open, High, Low, Close, Adj Close, Volume
    df = df.rename(columns={"Date": "date", "Close": "close"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close"])
    return df[["date", "close"]]


def mark_month_year_end(df_daily: pd.DataFrame) -> pd.DataFrame:
    if df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["year"] = df["date"].dt.year.astype("int64")
    df["month"] = df["date"].dt.month.astype("int64")

    df = df.sort_values("date")

    # último pregão do mês
    month_end = df.groupby(["year", "month"], as_index=False).tail(1)[["date"]].copy()
    month_end["is_month_end"] = True

    # último pregão do ano
    year_end = df.groupby(["year"], as_index=False).tail(1)[["date"]].copy()
    year_end["is_year_end"] = True

    df["is_month_end"] = False
    df["is_year_end"] = False

    df = df.merge(month_end, on="date", how="left", suffixes=("", "_m"))
    df["is_month_end"] = df["is_month_end"] | df["is_month_end_m"].fillna(False)
    df = df.drop(columns=["is_month_end_m"])

    df = df.merge(year_end, on="date", how="left", suffixes=("", "_y"))
    df["is_year_end"] = df["is_year_end"] | df["is_year_end_y"].fillna(False)
    df = df.drop(columns=["is_year_end_y"])

    df["date"] = df["date"].dt.date
    df["fetched_at"] = pd.Timestamp.utcnow()

    # manter apenas colunas do schema
    return df[["date", "close", "year", "month", "is_month_end", "is_year_end", "fetched_at"]]


def upsert_prices(engine: Engine, ticker_no_sa: str, df_prices: pd.DataFrame, out_table: str) -> None:
    if df_prices.empty:
        return

    schema, table = out_table.split(".", 1)

    df_out = df_prices.copy()
    df_out["ticker"] = ticker_no_sa

    # upsert por PK (ticker, date)
    sql = f"""
    insert into {out_table} (ticker, date, close, year, month, is_month_end, is_year_end, fetched_at)
    values (:ticker, :date, :close, :year, :month, :is_month_end, :is_year_end, :fetched_at)
    on conflict (ticker, date)
    do update set
      close = excluded.close,
      year = excluded.year,
      month = excluded.month,
      is_month_end = excluded.is_month_end,
      is_year_end = excluded.is_year_end,
      fetched_at = excluded.fetched_at;
    """

    rows = df_out.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(text(sql), rows)


def sync_prices_for_ticker(engine: Engine, ticker: str, cfg: PricesSyncConfig = PricesSyncConfig()) -> bool:
    t = _norm_no_sa(ticker)
    t_sa = _to_yf(t)

    df_daily = fetch_prices_daily(t_sa, cfg.lookback_years)
    time.sleep(cfg.sleep_s)

    if df_daily.empty:
        return False

    df_marked = mark_month_year_end(df_daily)
    upsert_prices(engine, t, df_marked, cfg.out_table)
    return True
