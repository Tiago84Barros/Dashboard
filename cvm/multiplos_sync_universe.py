from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.multiplos_builder import compute_multiplos_full


DFP_TABLE = "cvm.demonstracoes_financeiras_dfp"
PRICES_TABLE = "cvm.prices_b3"
OUT_TABLE = "cvm.multiplos"


def load_dfp_universe(engine: Engine) -> pd.DataFrame:
    sql = f"""
      select
        ticker,
        data,
        extract(year from data)::int as ano,
        receita_liquida, ebit, lucro_liquido, lpa,
        ativo_total, ativo_circulante,
        passivo_circulante, passivo_total,
        patrimonio_liquido,
        dividendos,
        caixa_e_equivalentes,
        divida_total,
        divida_liquida
      from {DFP_TABLE}
      where ticker is not null
      order by ticker, data;
    """
    return pd.read_sql(text(sql), con=engine)


def load_year_end_prices_universe(engine: Engine) -> pd.DataFrame:
    sql = f"""
      select
        ticker,
        year as ano,
        date as ref_date,
        close as price_close
      from {PRICES_TABLE}
      where is_year_end = true
        and ticker is not null
      order by ticker, year;
    """
    return pd.read_sql(text(sql), con=engine)


def upsert_multiplos(engine: Engine, df: pd.DataFrame) -> None:
    if df.empty:
        return

    sql = f"""
    insert into {OUT_TABLE} (
      ticker, ano, ref_date, price_close,
      liquidez_corrente, endividamento_total, alavancagem_financeira,
      margem_operacional, margem_liquida,
      roe, roa, roic,
      dy, pl, pvp, payout,
      shares_est,
      fetched_at
    )
    values (
      :ticker, :ano, :ref_date, :price_close,
      :liquidez_corrente, :endividamento_total, :alavancagem_financeira,
      :margem_operacional, :margem_liquida,
      :roe, :roa, :roic,
      :dy, :pl, :pvp, :payout,
      :shares_est,
      now()
    )
    on conflict (ticker, ano)
    do update set
      ref_date = excluded.ref_date,
      price_close = excluded.price_close,
      liquidez_corrente = excluded.liquidez_corrente,
      endividamento_total = excluded.endividamento_total,
      alavancagem_financeira = excluded.alavancagem_financeira,
      margem_operacional = excluded.margem_operacional,
      margem_liquida = excluded.margem_liquida,
      roe = excluded.roe,
      roa = excluded.roa,
      roic = excluded.roic,
      dy = excluded.dy,
      pl = excluded.pl,
      pvp = excluded.pvp,
      payout = excluded.payout,
      shares_est = excluded.shares_est,
      fetched_at = now();
    """

    rows = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(text(sql), rows)


def rebuild_multiplos_universe(engine: Engine) -> dict:
    dfp = load_dfp_universe(engine)
    px = load_year_end_prices_universe(engine)

    if dfp.empty:
        return {"ok": False, "msg": "DFP vazio"}
    if px.empty:
        return {"ok": False, "msg": "prices_b3 (year_end) vazio"}

    out = compute_multiplos_full(dfp, px)

    # Remove linhas sem preço anual (não dá valuation)
    out = out.dropna(subset=["price_close", "ano", "ticker"])

    upsert_multiplos(engine, out)
    return {"ok": True, "rows": len(out)}
