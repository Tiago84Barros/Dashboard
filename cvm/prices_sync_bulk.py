# cvm/prices_sync_bulk.py
from __future__ import annotations

import time
import datetime as dt
from dataclasses import dataclass

import pandas as pd
import yfinance as yf
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class PricesBulkConfig:
    start_date: dt.date = dt.date(2010, 1, 1)
    batch_size: int = 40
    pause_s: float = 0.5
    out_table: str = "cvm.prices_b3"


def _normalize_ticker(t: str) -> str:
    return t.strip().upper().replace(".SA", "")


def _to_yf(t: str) -> str:
    return f"{_normalize_ticker(t)}.SA"


def _mark_month_year_end(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    df = df.sort_values(["ticker", "date"])

    df["is_month_end"] = False
    df["is_year_end"] = False

    month_end = (
        df.groupby(["ticker", "year", "month"], as_index=False)
        .tail(1)[["ticker", "date"]]
        .assign(is_month_end=True)
    )

    year_end = (
        df.groupby(["ticker", "year"], as_index=False)
        .tail(1)[["ticker", "date"]]
        .assign(is_year_end=True)
    )

    df = df.merge(month_end, on=["ticker", "date"], how="left")
    df = df.merge(year_end, on=["ticker", "date"], how="left")

    df["is_month_end"] = df["is_month_end"].fillna(False)
    df["is_year_end"] = df["is_year_end"].fillna(False)

    df["date"] = df["date"].dt.date
    df["fetched_at"] = pd.Timestamp.utcnow()

    return df[
        [
            "ticker",
            "date",
            "close",
            "year",
            "month",
            "is_month_end",
            "is_year_end",
            "fetched_at",
        ]
    ]


def _upsert_prices(engine: Engine, df: pd.DataFrame, table: str) -> None:
    if df.empty:
        return

    sql = f"""
    insert into {table} (
        ticker, date, close, year, month,
        is_month_end, is_year_end, fetched_at
    )
    values (
        :ticker, :date, :close, :year, :month,
        :is_month_end, :is_year_end, :fetched_at
    )
    on conflict (ticker, date)
    do update set
        close = excluded.close,
        year = excluded.year,
        month = excluded.month,
        is_month_end = excluded.is_month_end,
        is_year_end = excluded.is_year_end,
        fetched_at = excluded.fetched_at;
    """

    with engine.begin() as conn:
        conn.execute(text(sql), df.to_dict(orient="records"))


def sync_prices_universe(engine: Engine, tickers: list[str]) -> dict:
    cfg = PricesBulkConfig()
    tickers = sorted({_normalize_ticker(t) for t in tickers if t})

    end = dt.date.today()
    start = cfg.start_date

    stats = {"total": len(tickers), "ok": 0, "fail": 0}

    for i in range(0, len(tickers), cfg.batch_size):
        batch = tickers[i : i + cfg.batch_size]
        yf_list = " ".join(_to_yf(t) for t in batch)

        try:
            raw = yf.download(
                yf_list,
                start=str(start),
                end=str(end),
                auto_adjust=False,
                progress=False,
                actions=False,
                threads=True,
                group_by="ticker",
            )
        except Exception:
            stats["fail"] += len(batch)
            continue

        rows = []

        if isinstance(raw.columns, pd.MultiIndex):
            for t in batch:
                col = _to_yf(t)
                if col not in raw.columns.get_level_values(0):
                    stats["fail"] += 1
                    continue
                s = raw[(col, "Close")].dropna()
                if s.empty:
                    stats["fail"] += 1
                    continue
                tmp = s.reset_index().rename(
                    columns={"Date": "date", (col, "Close"): "close"}
                )
                tmp["ticker"] = t
                rows.append(tmp[["ticker", "date", "close"]])
                stats["ok"] += 1
        else:
            if "Close" in raw.columns and len(batch) == 1:
                s = raw["Close"].dropna()
                if not s.empty:
                    tmp = s.reset_index().rename(
                        columns={"Date": "date", "Close": "close"}
                    )
                    tmp["ticker"] = batch[0]
                    rows.append(tmp[["ticker", "date", "close"]])
                    stats["ok"] += 1

        if rows:
            df = pd.concat(rows, ignore_index=True)
            df = _mark_month_year_end(df)
            _upsert_prices(engine, df, cfg.out_table)

        time.sleep(cfg.pause_s)

    return stats
