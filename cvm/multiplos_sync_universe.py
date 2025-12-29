from __future__ import annotations

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import Dict


# =============================================================================
# Helpers
# =============================================================================

def _norm_ticker(t: str) -> str:
    return t.upper().replace(".SA", "")


def _safe_div(n, d):
    if n is None or d is None:
        return None
    if d == 0:
        return None
    return n / d


def _clean(x):
    if x is None:
        return None
    if isinstance(x, float) and (np.isnan(x) or np.isinf(x)):
        return None
    return float(x)


# =============================================================================
# Core
# =============================================================================

def rebuild_multiplos_universe(engine: Engine) -> Dict[str, int]:
    """
    Recalcula múltiplos APENAS para (ticker, ano) inexistentes.
    Robusto contra NaN, prejuízo, EBIT negativo e patrimônio negativo.
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

    df = df[df["_exists"].isna()].drop(columns="_exists")

    if df.empty:
        return {"inserted": 0}

    rows = []

    for _, r in df.iterrows():
        row = {
            "ticker": r["ticker"],
            "ano": int(r["ano"]),
            "preco_fim_ano": _clean(r["close"]),

            "pl": _clean(_safe_div(r["close"], r["lpa"])),
            "roe": _clean(_safe_div(r["lucro_liquido"], r["patrimonio_liquido"])),

            "margem_liquida": _clean(_safe_div(r["lucro_liquido"], r["receita_liquida"])),
            "margem_ebit": _clean(_safe_div(r["ebit"], r["receita_liquida"])),

            "divida_liquida_ebit": _clean(_safe_div(r["divida_liquida"], r["ebit"])),
            "divida_total_patrimonio": _clean(_safe_div(r["divida_total"], r["patrimonio_liquido"])),
        }
        rows.append(row)

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
        conn.execute(text(sql), rows)

    return {"inserted": len(rows)}
