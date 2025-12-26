from __future__ import annotations

from typing import Callable, Optional, Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from scipy.stats import zscore


SCHEMA = "cvm"
SRC_TABLE = "financial_metrics"     # entrada
DST_TABLE = "fundamental_score"     # saída

SRC_FULL = f"{SCHEMA}.{SRC_TABLE}"
DST_FULL = f"{SCHEMA}.{DST_TABLE}"


# ============================================================
# Infraestrutura Supabase
# ============================================================
def _ensure_table(engine: Engine) -> None:
    ddl = f"""
    create schema if not exists {SCHEMA};

    create table if not exists {DST_FULL} (
        ticker text not null,
        ano integer not null,

        score_qualidade double precision,
        score_crescimento double precision,
        score_rentabilidade double precision,
        score_total double precision,
        ranking integer,

        fetched_at timestamptz default now(),

        primary key (ticker, ano)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


# ============================================================
# Normalização de nomes (case-insensitive)
# ============================================================
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _require_cols(df: pd.DataFrame, cols: list[str], prefix_msg: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{prefix_msg}: colunas ausentes em {SRC_FULL}: {missing}")


# ============================================================
# Z-score vetorizado por grupo (ano)
# ============================================================
def _zscore_grouped(df: pd.DataFrame, group_col: str, value_col: str, invert: bool = False) -> pd.Series:
    """
    Calcula z-score por grupo (ex: por ano) de forma vetorizada.
    - Converte para float
    - Ignora NaN
    - Se o grupo tiver variância 0, zscore devolve NaN (ok)
    """
    s = df[value_col].astype(float)

    def _zs(x: pd.Series) -> np.ndarray:
        return zscore(x.to_numpy(dtype=float), nan_policy="omit")

    out = df.groupby(group_col, sort=True)[value_col].transform(lambda x: pd.Series(_zs(x.astype(float)), index=x.index))
    if invert:
        out = -out
    return out


# ============================================================
# Função principal
# ============================================================
def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
    **kwargs: Any,  # compatibilidade: ignora args extras do orquestrador
) -> pd.DataFrame:
    """
    Scoring Fundamentalista (otimizado e compatível)

    - Lê cvm.financial_metrics
    - Normaliza colunas para minúsculo
    - Z-score por ANO (vetorizado)
    - Scores agregados e ranking anual
    - UPSERT em cvm.fundamental_score
    """

    _ensure_table(engine)

    if progress_cb:
        progress_cb("FUNDAMENTAL SCORE: carregando métricas base…")

    df = pd.read_sql(f"select * from {SRC_FULL}", engine)

    if df.empty:
        if progress_cb:
            progress_cb("FUNDAMENTAL SCORE: tabela financial_metrics vazia; nada a fazer.")
        return df

    df = _normalize_columns(df)

    required = [
        "ticker",
        "ano",
        "margem_ebit",
        "margem_liquida",
        "roe",
        "roic",
        "cagr_receita",
        "cagr_lucro",
    ]
    _require_cols(df, required, "FUNDAMENTAL SCORE")

    # tipos mínimos
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["ticker", "ano"]).copy()
    df["ano"] = df["ano"].astype(int)

    if df.empty:
        if progress_cb:
            progress_cb("FUNDAMENTAL SCORE: sem linhas válidas após limpeza.")
        return df

    if progress_cb:
        progress_cb("FUNDAMENTAL SCORE: calculando z-scores por ano…")

    # métricas e flags
    metricas = {
        "margem_ebit": False,
        "margem_liquida": False,
        "roe": False,
        "roic": False,
        "cagr_receita": False,
        "cagr_lucro": False,
    }

    # z-scores vetorizados por ano
    for col, invert in metricas.items():
        df[f"z_{col}"] = _zscore_grouped(df, "ano", col, invert=invert)

    if progress_cb:
        progress_cb("FUNDAMENTAL SCORE: agregando scores…")

    # scores parciais
    df["score_qualidade"] = (df["z_margem_ebit"] * 0.5) + (df["z_margem_liquida"] * 0.5)
    df["score_rentabilidade"] = (df["z_roe"] * 0.5) + (df["z_roic"] * 0.5)
    df["score_crescimento"] = (df["z_cagr_receita"] * 0.5) + (df["z_cagr_lucro"] * 0.5)

    # score total
    df["score_total"] = (df["score_qualidade"] * 0.4) + (df["score_rentabilidade"] * 0.4) + (df["score_crescimento"] * 0.2)

    # ranking por ano (denso)
    df["ranking"] = (
        df.groupby("ano")["score_total"]
        .rank(ascending=False, method="dense")
        .astype("Int64")
    )

    df_final = df[
        [
            "ticker",
            "ano",
            "score_qualidade",
            "score_crescimento",
            "score_rentabilidade",
            "score_total",
            "ranking",
        ]
    ].copy()

    # Postgres-friendly
    df_final = df_final.replace([np.inf, -np.inf], np.nan)
    df_final = df_final.where(pd.notnull(df_final), None)

    if progress_cb:
        progress_cb(f"FUNDAMENTAL SCORE: gravando em {DST_FULL}…")

    upsert = f"""
    insert into {DST_FULL} (
        ticker, ano,
        score_qualidade, score_crescimento,
        score_rentabilidade, score_total, ranking,
        fetched_at
    )
    values (
        :ticker, :ano,
        :score_qualidade, :score_crescimento,
        :score_rentabilidade, :score_total, :ranking,
        now()
    )
    on conflict (ticker, ano) do update set
        score_qualidade = excluded.score_qualidade,
        score_crescimento = excluded.score_crescimento,
        score_rentabilidade = excluded.score_rentabilidade,
        score_total = excluded.score_total,
        ranking = excluded.ranking,
        fetched_at = now();
    """

    with engine.begin() as conn:
        conn.execute(text(upsert), df_final.to_dict(orient="records"))

    if progress_cb:
        progress_cb("FUNDAMENTAL SCORE: concluído.")

    return df_final
