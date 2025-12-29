from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import Dict


# =============================================================================
# Helpers
# =============================================================================

def _norm_ticker(t: str) -> str:
    return t.upper().replace(".SA", "")


def _safe_div(a, b):
    return a / b if b not in (0, None) else None


# =============================================================================
# Core
# =============================================================================

def rebuild_multiplos_universe(engine: Engine) -> Dict[str, int]:
    """
    Recalcula múltiplos APENAS para (ticker, ano) ainda inexistentes em cvm.multiplos.
    """

    dfp = pd.read_sql(
        """
        select
            ticker,
            extract(year from data)::int as ano,
            receita_liquida,
            ebit,
            lucro_liquido,
            lpa,
            patrimonio_liquido,
            divida_total,
            divida_liquida
        from cvm.demonstracoes_financeiras_dfp
        """,
        engine,
    )

    prices = pd.read_sql(
        "select ticker, ano, close from cvm.prices_b3_yearly",
        engine,
    )

    exist = pd.read_sql(
        "select ticker, ano from cvm.multiplos",
        engine,
    )

    for df in (dfp, prices, exist):
        df["ticker"] = df["ticker"].astype(str).map(_norm_ticker)
        df["ano"] = df["ano"].astype(int)

    df = (
        dfp.merge(prices, on=["ticker", "ano"], how="left")
           .merge(exist.assign(_exists=1), on=["ticker", "ano"], how="left")
    )

    df = df[df["_exists"].isna()].drop(columns=["_exists"])

    if df.empty:
        return {"inserted": 0}

    df["pl"] = df["close"] / df["lpa"]
    df["roe"] = df["lucro_liquido"] / df["patrimonio_liquido"]
    df["margem_liquida"] = df["lucro_liquido"] / df["receita_liquida"]
    df["margem_ebit"] = df["ebit"] / df["receita_liquida"]
    df["divida_liquida_ebit"] = df["divida_liquida"] / df["ebit"]
    df["divida_total_patrimonio"] = df["divida_total"] / df["patrimonio_liquido"]

    payload = df[
        [
            "ticker",
            "ano",
            "close",
            "pl",
            "roe",
            "margem_liquida",
            "margem_ebit",
            "divida_liquida_ebit",
            "divida_total_patrimonio",
        ]
    ].rename(columns={"close": "preco_fim_ano"}).to_dict("records")

    sql = """
    insert into cvm.multiplos (
        ticker, ano, preco_fim_ano,
        pl, roe,
        margem_liquida, margem_ebit,
        divida_liquida_ebit, divida_total_patrimonio,
        fetched_at
    )
    values (
        :ticker, :ano, :preco_fim_ano,
        :pl, :roe,
        :margem_liquida, :margem_ebit,
        :divida_liquida_ebit, :divida_total_patrimonio,
        now()
    )
    """

    with engine.begin() as conn:
        conn.execute(text(sql), payload)

    return {"inserted": len(payload)}
