from __future__ import annotations

from typing import Callable, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# Postgres: identificadores não-quoteados viram minúsculo.
SCHEMA = "cvm"
SRC_TABLE = "demonstracoes_financeiras"
DST_TABLE = "financial_metrics"

SRC_FULL = f"{SCHEMA}.{SRC_TABLE}"
DST_FULL = f"{SCHEMA}.{DST_TABLE}"


# ------------------------------------------------------------
# Infra
# ------------------------------------------------------------
def _ensure_table(engine: Engine) -> None:
    ddl = f"""
    create schema if not exists {SCHEMA};

    create table if not exists {DST_FULL} (
        ticker text not null,
        ano integer not null,

        receita_liquida double precision,
        ebit double precision,
        lucro_liquido double precision,

        margem_ebit double precision,
        margem_liquida double precision,

        roe double precision,
        roic double precision,

        cagr_receita double precision,
        cagr_lucro double precision,

        primary key (ticker, ano)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    den0 = den.replace({0: np.nan})
    return num / den0


def _cagr_group(series: pd.Series) -> float | None:
    """
    CAGR sobre uma série anual (já ordenada).
    Usa apenas primeiro e último valor não-nulo.
    """
    s = series.dropna()
    if len(s) < 2:
        return None
    first = float(s.iloc[0])
    last = float(s.iloc[-1])
    if first == 0:
        return None
    n = len(s) - 1
    try:
        return (last / first) ** (1.0 / n) - 1.0
    except Exception:
        return None


def _upsert(engine: Engine, df: pd.DataFrame, batch: int = 2000) -> None:
    if df.empty:
        return

    sql = f"""
    insert into {DST_FULL} (
        ticker, ano,
        receita_liquida, ebit, lucro_liquido,
        margem_ebit, margem_liquida,
        roe, roic,
        cagr_receita, cagr_lucro
    ) values (
        :ticker, :ano,
        :receita_liquida, :ebit, :lucro_liquido,
        :margem_ebit, :margem_liquida,
        :roe, :roic,
        :cagr_receita, :cagr_lucro
    )
    on conflict (ticker, ano) do update set
        receita_liquida = excluded.receita_liquida,
        ebit = excluded.ebit,
        lucro_liquido = excluded.lucro_liquido,
        margem_ebit = excluded.margem_ebit,
        margem_liquida = excluded.margem_liquida,
        roe = excluded.roe,
        roic = excluded.roic,
        cagr_receita = excluded.cagr_receita,
        cagr_lucro = excluded.cagr_lucro;
    """

    df2 = df.where(pd.notnull(df), None)
    rows = df2.to_dict(orient="records")

    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(sql), rows[i : i + batch])


# ------------------------------------------------------------
# Principal
# ------------------------------------------------------------
def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
    start_year: int | None = None,
    batch: int = 2000,
) -> pd.DataFrame:
    """
    Construção de Métricas Financeiras (OTIMIZADO + DEDUP ANUAL)

    Melhorias-chave:
    - DEDUP por (ticker, ano): DISTINCT ON ... ORDER BY data DESC
      (resolve contagens > 1 por ano e reduz carga)
    - Vetorizado (sem iterrows)
    - CAGR por grupo e broadcast
    - UPSERT em lotes
    - Compatível com progress_cb
    """

    _ensure_table(engine)

    if progress_cb:
        progress_cb("Métricas: carregando base anual (dedup por ticker/ano)…")

    where_year = ""
    params: dict = {}
    if start_year is not None:
        where_year = "and extract(year from data)::int >= :start_year"
        params["start_year"] = int(start_year)

    # 1 linha por ticker/ano (pega a mais recente do ano)
    sql = text(f"""
        with base as (
            select distinct on (ticker, extract(year from data)::int)
                ticker,
                extract(year from data)::int as ano,
                data,
                receita_liquida,
                ebit,
                lucro_liquido,
                patrimonio_liquido,
                ativo_total,
                divida_total
            from {SRC_FULL}
            where 1=1
              {where_year}
            order by
                ticker,
                extract(year from data)::int,
                data desc
        )
        select
            ticker,
            ano,
            receita_liquida,
            ebit,
            lucro_liquido,
            patrimonio_liquido,
            ativo_total,
            divida_total
        from base
        order by ticker, ano;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        if progress_cb:
            progress_cb("Métricas: base vazia, nada a calcular.")
        return df

    if progress_cb:
        progress_cb(f"Métricas: calculando indicadores (linhas={len(df)})…")

    # Tipos/saneamento
    df["ano"] = df["ano"].astype(int)
    df = df.sort_values(["ticker", "ano"]).reset_index(drop=True)

    # Margens
    df["margem_ebit"] = _safe_div(df["ebit"], df["receita_liquida"])
    df["margem_liquida"] = _safe_div(df["lucro_liquido"], df["receita_liquida"])

    # ROE / ROIC
    df["roe"] = _safe_div(df["lucro_liquido"], df["patrimonio_liquido"])
    invested_capital = (df["ativo_total"] - df["divida_total"]).replace({0: np.nan})
    df["roic"] = df["ebit"] / invested_capital

    if progress_cb:
        progress_cb("Métricas: calculando CAGR por empresa…")

    # CAGR por ticker (rápido)
    cagr_receita = (
        df.groupby("ticker", sort=False)["receita_liquida"]
        .apply(_cagr_group)
        .rename("cagr_receita")
    )
    cagr_lucro = (
        df.groupby("ticker", sort=False)["lucro_liquido"]
        .apply(_cagr_group)
        .rename("cagr_lucro")
    )

    df = df.join(cagr_receita, on="ticker")
    df = df.join(cagr_lucro, on="ticker")

    df_final = df[
        [
            "ticker",
            "ano",
            "receita_liquida",
            "ebit",
            "lucro_liquido",
            "margem_ebit",
            "margem_liquida",
            "roe",
            "roic",
            "cagr_receita",
            "cagr_lucro",
        ]
    ].copy()

    if progress_cb:
        progress_cb(f"Métricas: upsert em {DST_FULL} (linhas={len(df_final)})…")

    _upsert(engine, df_final, batch=batch)

    if progress_cb:
        progress_cb("Métricas: concluído.")

    return df_final
