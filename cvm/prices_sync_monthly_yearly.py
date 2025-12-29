from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import yfinance as yf
from sqlalchemy import text
from sqlalchemy.engine import Engine


DEFAULT_START = "2010-01-01"


@dataclass
class SyncStats:
    total: int = 0
    ok: int = 0
    fail: int = 0
    empty: int = 0
    monthly_rows: int = 0
    yearly_rows: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "total": self.total,
            "ok": self.ok,
            "fail": self.fail,
            "empty": self.empty,
            "monthly_rows": self.monthly_rows,
            "yearly_rows": self.yearly_rows,
        }


def _norm_ticker(t: str) -> str:
    t = (t or "").strip().upper()
    return t.replace(".SA", "")


def _normalize_prices_df(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza retorno do yfinance para colunas: date (datetime), close (float)
    """
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "close"])

    df = raw.copy()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.columns = [str(c).strip().lower() for c in df.columns]
    df = df.reset_index()
    df.columns = [str(c).strip().lower() for c in df.columns]

    # coluna de data
    date_col = None
    for cand in ("date", "datetime", "index"):
        if cand in df.columns:
            date_col = cand
            break
    if date_col is None:
        date_col = df.columns[0]

    # coluna de close
    if "close" in df.columns:
        close_col = "close"
    elif "adj close" in df.columns:
        close_col = "adj close"
    elif "adj_close" in df.columns:
        close_col = "adj_close"
    else:
        raise ValueError("Retorno Yahoo sem coluna close/adj close.")

    out = df[[date_col, close_col]].rename(columns={date_col: "date", close_col: "close"})
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["date", "close"]).sort_values("date")
    return out.reset_index(drop=True)


def _download_daily(
    ticker_sa: str,
    start: str,
    end: Optional[str],
    retries: int = 3,
    pause_s: float = 0.7,
) -> pd.DataFrame:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            raw = yf.download(
                ticker_sa,
                start=start,
                end=end,
                progress=False,
                auto_adjust=False,
                threads=False,
                group_by="column",
            )
            return _normalize_prices_df(raw)
        except Exception as e:
            last_err = e
            time.sleep(pause_s * attempt)
    raise last_err  # type: ignore[misc]


def _get_last_month_end(engine: Engine, ticker: str) -> Optional[pd.Timestamp]:
    df = pd.read_sql(
        text("select max(month_end) as last_month_end from cvm.prices_b3_monthly where ticker=:t"),
        con=engine,
        params={"t": ticker},
    )
    v = df.iloc[0]["last_month_end"]
    if pd.isna(v):
        return None
    return pd.to_datetime(v)


def _get_last_year(engine: Engine, ticker: str) -> Optional[int]:
    df = pd.read_sql(
        text("select max(ano) as last_ano from cvm.prices_b3_yearly where ticker=:t"),
        con=engine,
        params={"t": ticker},
    )
    v = df.iloc[0]["last_ano"]
    if pd.isna(v):
        return None
    return int(v)


def _daily_to_monthly_yearly(df_daily: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Converte df diário -> último pregão do mês + último pregão do ano.
    """
    if df_daily.empty:
        return (
            pd.DataFrame(columns=["month_end", "close"]),
            pd.DataFrame(columns=["ano", "year_end", "close"]),
        )

    df = df_daily.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["ano"] = df["date"].dt.year.astype(int)
    df["ym"] = df["date"].dt.to_period("M")

    # último pregão do mês
    idx_m = df.groupby("ym")["date"].idxmax()
    monthly = df.loc[idx_m, ["date", "close"]].rename(columns={"date": "month_end"}).sort_values("month_end")
    monthly["month_end"] = monthly["month_end"].dt.date

    # último pregão do ano
    idx_y = df.groupby("ano")["date"].idxmax()
    yearly = df.loc[idx_y, ["ano", "date", "close"]].rename(columns={"date": "year_end"}).sort_values("ano")
    yearly["year_end"] = pd.to_datetime(yearly["year_end"]).dt.date

    return monthly.reset_index(drop=True), yearly.reset_index(drop=True)


def _upsert_monthly(engine: Engine, ticker: str, monthly: pd.DataFrame, chunk_size: int = 5000) -> int:
    if monthly.empty:
        return 0

    payload = [(ticker, r["month_end"], float(r["close"])) for r in monthly.to_dict("records")]

    sql = """
        insert into cvm.prices_b3_monthly (ticker, month_end, close, fetched_at)
        values %s
        on conflict (ticker, month_end)
        do update set close=excluded.close, fetched_at=excluded.fetched_at
    """

    raw = engine.raw_connection()
    inserted = 0
    try:
        from psycopg2.extras import execute_values

        with raw.cursor() as cur:
            for i in range(0, len(payload), chunk_size):
                batch = payload[i : i + chunk_size]
                execute_values(cur, sql, batch, template="(%s,%s,%s,now())", page_size=chunk_size)
                inserted += len(batch)
        raw.commit()
    finally:
        raw.close()

    return inserted


def _upsert_yearly(engine: Engine, ticker: str, yearly: pd.DataFrame, chunk_size: int = 5000) -> int:
    if yearly.empty:
        return 0

    payload = [(ticker, int(r["ano"]), r["year_end"], float(r["close"])) for r in yearly.to_dict("records")]

    sql = """
        insert into cvm.prices_b3_yearly (ticker, ano, year_end, close, fetched_at)
        values %s
        on conflict (ticker, ano)
        do update set year_end=excluded.year_end, close=excluded.close, fetched_at=excluded.fetched_at
    """

    raw = engine.raw_connection()
    inserted = 0
    try:
        from psycopg2.extras import execute_values

        with raw.cursor() as cur:
            for i in range(0, len(payload), chunk_size):
                batch = payload[i : i + chunk_size]
                execute_values(cur, sql, batch, template="(%s,%s,%s,%s,now())", page_size=chunk_size)
                inserted += len(batch)
        raw.commit()
    finally:
        raw.close()

    return inserted


def sync_prices_monthly_yearly_universe(
    engine: Engine,
    tickers: Iterable[str],
    *,
    start: str = DEFAULT_START,
    end: Optional[str] = None,
    retries: int = 3,
    pause_s: float = 0.7,
    per_ticker_sleep_s: float = 0.10,
) -> Dict[str, Any]:
    """
    Sincroniza preços:
    - monthly: último pregão de cada mês (cvm.prices_b3_monthly)
    - yearly : último pregão de cada ano (cvm.prices_b3_yearly)

    Incremental:
    - se já tem monthly, baixa desde o último month_end + 1 dia
    - senão, baixa desde 2010-01-01
    """
    tickers_list: List[str] = sorted(set(_norm_ticker(t) for t in tickers if str(t).strip()))
    stats = SyncStats(total=len(tickers_list))

    for t in tickers_list:
        ticker_sa = f"{t}.SA"
        try:
            last_m = _get_last_month_end(engine, t)

            # start incremental (preferível pelo mensal)
            if last_m is None:
                start_eff = start
            else:
                start_eff = (last_m + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

            df_daily = _download_daily(ticker_sa, start=start_eff, end=end, retries=retries, pause_s=pause_s)

            if df_daily.empty:
                stats.empty += 1
                continue

            monthly, yearly = _daily_to_monthly_yearly(df_daily)

            # upsert somente o que veio novo
            stats.monthly_rows += _upsert_monthly(engine, t, monthly)
            stats.yearly_rows += _upsert_yearly(engine, t, yearly)

            stats.ok += 1
        except Exception:
            stats.fail += 1
        finally:
            if per_ticker_sleep_s > 0:
                time.sleep(per_ticker_sleep_s)

    return stats.as_dict()
