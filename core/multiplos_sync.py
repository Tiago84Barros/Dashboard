from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class MultiplosConfig:
    dfp_table: str = "cvm.demonstracoes_financeiras_dfp"
    prices_table: str = "cvm.prices_b3"
    out_table: str = "cvm.multiplos"


def _norm_no_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    if t.endswith(".SA"):
        t = t[:-3]
    return t


def load_dfp(engine: Engine, ticker_no_sa: str, table: str) -> pd.DataFrame:
    sql = f"""
      select
        ticker,
        data,
        extract(year from data)::int as ano,
        lucro_liquido,
        lpa,
        patrimonio_liquido,
        dividendos
      from {table}
      where ticker = :ticker
      order by data;
    """
    df = pd.read_sql(text(sql), con=engine, params={"ticker": ticker_no_sa})
    return df if df is not None else pd.DataFrame()


def load_year_end_prices(engine: Engine, ticker_no_sa: str, table: str) -> pd.DataFrame:
    sql = f"""
      select
        ticker,
        year as ano,
        date as ref_date,
        close as price_close
      from {table}
      where ticker = :ticker
        and is_year_end = true
      order by year;
    """
    df = pd.read_sql(text(sql), con=engine, params={"ticker": ticker_no_sa})
    return df if df is not None else pd.DataFrame()


def build_multiplos(engine: Engine, ticker: str, cfg: MultiplosConfig = MultiplosConfig()) -> pd.DataFrame:
    t = _norm_no_sa(ticker)

    dfp = load_dfp(engine, t, cfg.dfp_table)
    px = load_year_end_prices(engine, t, cfg.prices_table)

    if dfp.empty or px.empty:
        return pd.DataFrame()

    df = dfp.merge(px, on=["ticker", "ano"], how="left")

    for c in ["lucro_liquido", "lpa", "patrimonio_liquido", "dividendos", "price_close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # shares estimado via LPA
    df["shares_est"] = np.where(df["lpa"].abs() > 1e-12, df["lucro_liquido"] / df["lpa"], np.nan)

    # VPA e DPS estimados
    df["vpa_est"] = np.where(df["shares_est"].abs() > 1e-12, df["patrimonio_liquido"] / df["shares_est"], np.nan)
    df["dps_est"] = np.where(df["shares_est"].abs() > 1e-12, df["dividendos"] / df["shares_est"], np.nan)

    # múltiplos
    df["pl"] = np.where(df["lpa"].abs() > 1e-12, df["price_close"] / df["lpa"], np.nan)
    df["pvp"] = np.where(df["vpa_est"].abs() > 1e-12, df["price_close"] / df["vpa_est"], np.nan)
    df["dy"] = np.where(df["price_close"].abs() > 1e-12, df["dps_est"] / df["price_close"], np.nan)

    out = df[["ticker", "ano", "ref_date", "price_close", "pl", "pvp", "dy", "shares_est"]].copy()
    out["ticker"] = t
    return out


def upsert_multiplos(engine: Engine, out: pd.DataFrame, out_table: str) -> None:
    if out.empty:
        return

    # Recomendação: PK (ticker, ano) no Supabase.
    # Se ainda não existir, funciona assim mesmo com "delete+insert" por ticker.
    ticker = out["ticker"].iloc[0]

    with engine.begin() as conn:
        conn.execute(text(f"delete from {out_table} where ticker = :ticker"), {"ticker": ticker})

        sql = f"""
          insert into {out_table} (ticker, ano, ref_date, price_close, pl, pvp, dy, shares_est)
          values (:ticker, :ano, :ref_date, :price_close, :pl, :pvp, :dy, :shares_est)
        """
        conn.execute(text(sql), out.to_dict(orient="records"))


def sync_multiplos_for_ticker(engine: Engine, ticker: str, cfg: MultiplosConfig = MultiplosConfig()) -> bool:
    out = build_multiplos(engine, ticker, cfg=cfg)
    if out.empty:
        return False
    upsert_multiplos(engine, out, cfg.out_table)
    return True
