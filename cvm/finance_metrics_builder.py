from __future__ import annotations

from typing import Callable, Optional
import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


SCHEMA = "cvm"
SRC_TABLE = "Demonstracoes_Financeiras"
DST_TABLE = "Financial_Metrics"

SRC_FULL = f"{SCHEMA}.{SRC_TABLE}"
DST_FULL = f"{SCHEMA}.{DST_TABLE}"


# ------------------------------------------------------------
# Infra
# ------------------------------------------------------------
def _ensure_table(engine: Engine) -> None:
    ddl = f"""
    create schema if not exists {SCHEMA};

    create table if not exists {DST_FULL} (
        Ticker text not null,
        Ano integer not null,

        Receita_Liquida double precision,
        EBIT double precision,
        Lucro_Liquido double precision,

        Margem_EBIT double precision,
        Margem_Liquida double precision,

        ROE double precision,
        ROIC double precision,

        CAGR_Receita double precision,
        CAGR_Lucro double precision,

        primary key (Ticker, Ano)
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
        Ticker, Ano,
        Receita_Liquida, EBIT, Lucro_Liquido,
        Margem_EBIT, Margem_Liquida,
        ROE, ROIC,
        CAGR_Receita, CAGR_Lucro
    ) values (
        :Ticker, :Ano,
        :Receita_Liquida, :EBIT, :Lucro_Liquido,
        :Margem_EBIT, :Margem_Liquida,
        :ROE, :ROIC,
        :CAGR_Receita, :CAGR_Lucro
    )
    on conflict (Ticker, Ano) do update set
        Receita_Liquida = excluded.Receita_Liquida,
        EBIT = excluded.EBIT,
        Lucro_Liquido = excluded.Lucro_Liquido,
        Margem_EBIT = excluded.Margem_EBIT,
        Margem_Liquida = excluded.Margem_Liquida,
        ROE = excluded.ROE,
        ROIC = excluded.ROIC,
        CAGR_Receita = excluded.CAGR_Receita,
        CAGR_Lucro = excluded.CAGR_Lucro;
    """

    # Postgres-friendly: NaN -> None
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
    Construção de Métricas Financeiras (OTIMIZADO)
    - Vetorizado (sem iterrows)
    - CAGR calculado por grupo (ticker) e broadcast
    - UPSERT em lotes
    - Compatível com progress_cb
    """

    _ensure_table(engine)

    if progress_cb:
        progress_cb("Métricas: carregando base anual (DFP)…")

    # Observação: se sua tabela tiver outra coluna de data, ajuste aqui.
    # Também deixei filtro por start_year opcional.
    where_year = ""
    params: dict = {}
    if start_year is not None:
        where_year = "where extract(year from Data)::int >= :start_year"
        params["start_year"] = int(start_year)

    # Leia só o que precisa (reduz tráfego e tempo)
    sql = text(f"""
        select
            Ticker,
            extract(year from Data)::int as Ano,
            Receita_Liquida,
            EBIT,
            Lucro_Liquido,
            Patrimonio_Liquido,
            Ativo_Total,
            Divida_Total
        from {SRC_FULL}
        {where_year}
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        if progress_cb:
            progress_cb("Métricas: base vazia, nada a calcular.")
        return df

    if progress_cb:
        progress_cb(f"Métricas: calculando métricas (linhas={len(df)})…")

    # Tipos e saneamento
    df["Ano"] = df["Ano"].astype(int)
    df = df.sort_values(["Ticker", "Ano"]).reset_index(drop=True)

    # Margens
    df["Margem_EBIT"] = _safe_div(df["EBIT"], df["Receita_Liquida"])
    df["Margem_Liquida"] = _safe_div(df["Lucro_Liquido"], df["Receita_Liquida"])

    # ROE / ROIC
    df["ROE"] = _safe_div(df["Lucro_Liquido"], df["Patrimonio_Liquido"])

    invested_capital = (df["Ativo_Total"] - df["Divida_Total"]).replace({0: np.nan})
    df["ROIC"] = df["EBIT"] / invested_capital

    if progress_cb:
        progress_cb("Métricas: calculando CAGR por empresa…")

    # CAGR por ticker (rápido e direto)
    cagr_receita = (
        df.groupby("Ticker", sort=False)["Receita_Liquida"]
        .apply(_cagr_group)
        .rename("CAGR_Receita")
    )
    cagr_lucro = (
        df.groupby("Ticker", sort=False)["Lucro_Liquido"]
        .apply(_cagr_group)
        .rename("CAGR_Lucro")
    )

    # Broadcast para todas as linhas do ticker
    df = df.join(cagr_receita, on="Ticker")
    df = df.join(cagr_lucro, on="Ticker")

    # Seleção final (colunas exatamente como destino)
    df_final = df[
        [
            "Ticker",
            "Ano",
            "Receita_Liquida",
            "EBIT",
            "Lucro_Liquido",
            "Margem_EBIT",
            "Margem_Liquida",
            "ROE",
            "ROIC",
            "CAGR_Receita",
            "CAGR_Lucro",
        ]
    ].copy()

    if progress_cb:
        progress_cb(f"Métricas: upsert em {DST_FULL} (linhas={len(df_final)})…")

    _upsert(engine, df_final, batch=batch)

    if progress_cb:
        progress_cb("Métricas: concluído.")

    return df_final
