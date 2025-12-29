from __future__ import annotations

import pandas as pd
import yfinance as yf
from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import List, Dict


# =============================================================================
# Helpers
# =============================================================================

def _norm_ticker(t: str) -> str:
    return t.upper().replace(".SA", "")


def _last_closed_month_end() -> pd.Timestamp:
    today = pd.Timestamp.today().normalize()
    return (today.replace(day=1) - pd.Timedelta(days=1)).normalize()


def _get_last_month(engine: Engine, ticker: str):
    df = pd.read_sql(
        text(
            "select max(month_end) as last_month "
            "from cvm.prices_b3_monthly where ticker=:t"
        ),
        engine,
        params={"t": ticker},
    )
    v = df.iloc[0]["last_month"]
    return None if pd.isna(v) else pd.to_datetime(v)


def _get_last_year(engine: Engine, ticker: str):
    df = pd.read_sql(
        text(
            "select max(ano) as last_ano "
            "from cvm.prices_b3_yearly where ticker=:t"
        ),
        engine,
        params={"t": ticker},
    )
    v = df.iloc[0]["last_ano"]
    return None if pd.isna(v) else int(v)


# =============================================================================
# Core
# =============================================================================

def sync_prices_monthly_yearly_universe(
    engine: Engine,
    tickers: List[str],
    *,
    start: str = "2010-01-01",
) -> Dict[str, int]:
    """
    Sincroniza preços:
    - último pregão de cada mês (prices_b3_monthly)
    - último pregão de cada ano  (prices_b3_yearly)

    INCREMENTAL:
    - pula ticker já atualizado até o último mês fechado
    - nunca rebaixa histórico desnecessariamente
    """

    last_closed_month = _last_closed_month_end()
    inserted_monthly = 0
    inserted_yearly = 0

    for t in tickers:
        t = _norm_ticker(t)
        yf_ticker = f"{t}.SA"

        last_m = _get_last_month(engine, t)
        last_y = _get_last_year(engine, t)

        if last_m is not None and last_m.normalize() >= last_closed_month:
            continue  # totalmente atualizado

        yf_start = start if last_m is None else (last_m + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        df = yf.download(
            yf_ticker,
            start=yf_start,
            progress=False,
            auto_adjust=True,
        )

        if df.empty:
            continue

        df = df.reset_index()
        df["Date"] = pd.to_datetime(df["Date"])

        # -------------------------
        # Monthly
        # -------------------------
        monthly = (
            df.groupby(df["Date"].dt.to_period("M"))
            .last()
            .reset_index(drop=True)
        )
        monthly["month_end"] = monthly["Date"].dt.to_period("M").dt.to_timestamp("M")
        monthly["ticker"] = t
        monthly["close"] = monthly["Close"]

        sql_m = """
        insert into cvm.prices_b3_monthly (ticker, month_end, close, fetched_at)
        values (:ticker, :month_end, :close, now())
        on conflict (ticker, month_end)
        do update set close=excluded.close, fetched_at=excluded.fetched_at
        """

        payload_m = monthly[["ticker", "month_end", "close"]].to_dict("records")
        if payload_m:
            with engine.begin() as conn:
                conn.execute(text(sql_m), payload_m)
            inserted_monthly += len(payload_m)

        # -------------------------
        # Yearly
        # -------------------------
        yearly = (
            df.groupby(df["Date"].dt.year)
            .last()
            .reset_index(drop=True)
        )
        yearly["ano"] = yearly["Date"].dt.year
        yearly["ticker"] = t
        yearly["close"] = yearly["Close"]

        if last_y is not None:
            yearly = yearly[yearly["ano"] > last_y]

        sql_y = """
        insert into cvm.prices_b3_yearly (ticker, ano, close, fetched_at)
        values (:ticker, :ano, :close, now())
        on conflict (ticker, ano)
        do update set close=excluded.close, fetched_at=excluded.fetched_at
        """

        payload_y = yearly[["ticker", "ano", "close"]].to_dict("records")
        if payload_y:
            with engine.begin() as conn:
                conn.execute(text(sql_y), payload_y)
            inserted_yearly += len(payload_y)

    return {
        "monthly": inserted_monthly,
        "yearly": inserted_yearly,
    }
