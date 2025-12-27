from __future__ import annotations

from typing import Callable, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

try:
    from sklearn.linear_model import LinearRegression
except Exception as e:
    raise ImportError(
        "O módulo de métricas precisa do scikit-learn para regressão linear. "
        "Instale/adicione no requirements: scikit-learn"
    ) from e


SCHEMA = "cvm"
SRC_TABLE = "demonstracoes_financeiras"
DST_TABLE = "financial_metrics"

SRC_FULL = f"{SCHEMA}.{SRC_TABLE}"
DST_FULL = f"{SCHEMA}.{DST_TABLE}"


# ------------------------------------------------------------
# Infra
# ------------------------------------------------------------
def _ensure_table(engine: Engine) -> None:
    """
    Mantém compatibilidade: preserva cagr_* (pode ficar NULL),
    adiciona growth_* (novo padrão).
    """
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

        -- legado/compatibilidade
        cagr_receita double precision,
        cagr_lucro double precision,

        -- novo padrão (regressão)
        growth_receita double precision,
        growth_lucro double precision,

        primary key (ticker, ano)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

    # garante colunas novas em bases já existentes
    alter_sql = f"""
    alter table {DST_FULL}
      add column if not exists growth_receita double precision;
    alter table {DST_FULL}
      add column if not exists growth_lucro double precision;
    alter table {DST_FULL}
      add column if not exists cagr_receita double precision;
    alter table {DST_FULL}
      add column if not exists cagr_lucro double precision;
    """
    with engine.begin() as conn:
        conn.execute(text(alter_sql))


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    den0 = den.replace({0: np.nan})
    return num / den0


def _linear_growth(series: pd.Series) -> float | None:
    """
    Tendência (slope) por regressão linear:
      X = [0..n-1], y = valores.
    Aceita valores negativos e evita complex.
    """
    s = series.dropna()
    if len(s) < 2:
        return None

    X = np.arange(len(s), dtype=float).reshape(-1, 1)
    y = s.values.astype(float).reshape(-1, 1)

    try:
        model = LinearRegression()
        model.fit(X, y)
        coef = float(model.coef_[0][0])
        return coef if np.isfinite(coef) else None
    except Exception:
        return None


def _sanitize_for_db(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()

    # remove inf/-inf
    df2 = df2.replace([np.inf, -np.inf], np.nan)

    # qualquer complex -> None (defesa final)
    for col in df2.columns:
        df2[col] = df2[col].apply(lambda x: None if isinstance(x, complex) else x)

    # NaN -> None
    df2 = df2.where(pd.notnull(df2), None)
    return df2


def _upsert(engine: Engine, df: pd.DataFrame, batch: int = 2000) -> None:
    if df.empty:
        return

    sql = f"""
    insert into {DST_FULL} (
        ticker, ano,
        receita_liquida, ebit, lucro_liquido,
        margem_ebit, margem_liquida,
        roe, roic,
        cagr_receita, cagr_lucro,
        growth_receita, growth_lucro
    ) values (
        :ticker, :ano,
        :receita_liquida, :ebit, :lucro_liquido,
        :margem_ebit, :margem_liquida,
        :roe, :roic,
        :cagr_receita, :cagr_lucro,
        :growth_receita, :growth_lucro
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
        cagr_lucro = excluded.cagr_lucro,
        growth_receita = excluded.growth_receita,
        growth_lucro = excluded.growth_lucro;
    """

    df2 = _sanitize_for_db(df)
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
    Métricas Financeiras (OTIMIZADO + DEDUP + GROWTH POR REGRESSÃO)

    - Dedup por (ticker, ano): DISTINCT ON ... ORDER BY data DESC
    - Vetorizado para margens/ROE/ROIC
    - Growth por regressão linear (slope) para Receita e Lucro
    - Persiste em cvm.financial_metrics
    """

    _ensure_table(engine)

    if progress_cb:
        progress_cb("Métricas: carregando base anual (dedup por ticker/ano)…")

    where_year = ""
    params: dict = {}
    if start_year is not None:
        where_year = "and extract(year from data)::int >= :start_year"
        params["start_year"] = int(start_year)

    # 1 linha por ticker/ano (mais recente do ano)
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

    df["ano"] = df["ano"].astype(int)
    df = df.sort_values(["ticker", "ano"]).reset_index(drop=True)

    if progress_cb:
        progress_cb(f"Métricas: calculando indicadores (linhas={len(df)})…")

    # Margens
    df["margem_ebit"] = _safe_div(df["ebit"], df["receita_liquida"])
    df["margem_liquida"] = _safe_div(df["lucro_liquido"], df["receita_liquida"])

    # ROE
    df["roe"] = _safe_div(df["lucro_liquido"], df["patrimonio_liquido"])

    # ROIC (pode ser NaN se denominador <=0/0; será sanitizado)
    invested_capital = (df["ativo_total"] - df["divida_total"]).replace({0: np.nan})
    df["roic"] = df["ebit"] / invested_capital

    if progress_cb:
        progress_cb("Métricas: calculando crescimento por regressão (slope) por empresa…")

    growth_receita = (
        df.groupby("ticker", sort=False)["receita_liquida"]
        .apply(_linear_growth)
        .rename("growth_receita")
    )
    growth_lucro = (
        df.groupby("ticker", sort=False)["lucro_liquido"]
        .apply(_linear_growth)
        .rename("growth_lucro")
    )

    df = df.join(growth_receita, on="ticker")
    df = df.join(growth_lucro, on="ticker")

    # Mantém compatibilidade: cagr_* ficará NULL (não calculamos)
    df["cagr_receita"] = None
    df["cagr_lucro"] = None

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
            "growth_receita",
            "growth_lucro",
        ]
    ].copy()

    if progress_cb:
        progress_cb(f"Métricas: upsert em {DST_FULL} (linhas={len(df_final)})…")

    _upsert(engine, df_final, batch=batch)

    if progress_cb:
        progress_cb("Métricas: concluído.")

    return df_final
