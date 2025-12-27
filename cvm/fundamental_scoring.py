# fundamental_scoring.py
from __future__ import annotations

from typing import Callable, Optional

import numpy as np
import pandas as pd
from scipy.stats import zscore
from sqlalchemy import text
from sqlalchemy.engine import Engine

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
SCHEMA = "cvm"
SRC_TABLE = "financial_metrics"     # entrada (Postgres tende a guardar em minúsculo)
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

        primary key (ticker, ano)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


# ============================================================
# Utilitários de saneamento
# ============================================================
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _require_cols(df: pd.DataFrame, cols: list[str], prefix_msg: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{prefix_msg}: colunas ausentes em {SRC_FULL}: {missing}")


def _to_float(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Converte colunas numéricas para float, forçando inválidos para NaN.
    """
    df = df.copy()
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# ============================================================
# Z-score robusto (evita NaN/Inf por variância zero)
# ============================================================
def _z(df: pd.DataFrame, col: str, invert: bool = False) -> pd.Series:
    s = pd.to_numeric(df[col], errors="coerce").astype(float)

    # Se a série tem variância zero (todos iguais) ou não tem dados suficientes,
    # usamos score neutro (0.0) para evitar NaN do zscore.
    if s.nunique(dropna=True) <= 1:
        z = pd.Series(0.0, index=df.index)
    else:
        z = pd.Series(zscore(s, nan_policy="omit"), index=df.index)

    if invert:
        z = -z

    # Blindagem final: remove inf/-inf
    z = z.replace([np.inf, -np.inf], np.nan)

    return z


# ============================================================
# Função principal
# ============================================================
def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    """
    Algoritmo — Scoring Fundamentalista

    - Lê cvm.financial_metrics (normaliza colunas para minúsculo)
    - Calcula z-score por ANO (robusto: sem NaN/inf por variância zero)
    - Agrega scores e calcula ranking anual (nullable)
    - Persiste em cvm.fundamental_score
    """

    _ensure_table(engine)

    if progress_cb:
        progress_cb("FUNDAMENTAL SCORE: carregando métricas base…")

    df = pd.read_sql(f"select * from {SRC_FULL}", engine)

    if df.empty:
        if progress_cb:
            progress_cb("FUNDAMENTAL SCORE: tabela de métricas vazia; nada a fazer.")
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

    # Tipos
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")

    num_cols = [
        "margem_ebit",
        "margem_liquida",
        "roe",
        "roic",
        "cagr_receita",
        "cagr_lucro",
    ]
    df = _to_float(df, num_cols)

    # Remove linhas inválidas mínimas (sem ticker ou ano)
    df = df[df["ticker"].notna() & df["ano"].notna()].copy()
    if df.empty:
        if progress_cb:
            progress_cb("FUNDAMENTAL SCORE: não há linhas válidas (ticker/ano).")
        return df

    if progress_cb:
        progress_cb("FUNDAMENTAL SCORE: calculando scores por ano…")

    metricas = {
        "margem_ebit": {"invert": False},
        "margem_liquida": {"invert": False},
        "roe": {"invert": False},
        "roic": {"invert": False},
        "cagr_receita": {"invert": False},
        "cagr_lucro": {"invert": False},
    }

    resultados: list[pd.DataFrame] = []

    # groupby em Int64: converter para int nativo para estabilidade
    for ano, df_ano in df.groupby(df["ano"].astype(int), sort=True):
        df_ano = df_ano.copy()

        # z-scores por métrica
        for m, cfg in metricas.items():
            df_ano[f"z_{m}"] = _z(df_ano, m, cfg["invert"])

        # scores parciais
        df_ano["score_qualidade"] = (
            df_ano["z_margem_ebit"] * 0.5
            + df_ano["z_margem_liquida"] * 0.5
        )

        df_ano["score_rentabilidade"] = (
            df_ano["z_roe"] * 0.5
            + df_ano["z_roic"] * 0.5
        )

        df_ano["score_crescimento"] = (
            df_ano["z_cagr_receita"] * 0.5
            + df_ano["z_cagr_lucro"] * 0.5
        )

        df_ano["score_total"] = (
            df_ano["score_qualidade"] * 0.4
            + df_ano["score_rentabilidade"] * 0.4
            + df_ano["score_crescimento"] * 0.2
        )

        # Blindagem: remove inf/-inf antes do ranking
        df_ano["score_total"] = df_ano["score_total"].replace([np.inf, -np.inf], np.nan)

        # Ranking: somente onde score_total é finito
        rank = df_ano["score_total"].rank(ascending=False, method="dense")
        df_ano["ranking"] = (
            rank.where(np.isfinite(rank))      # NaN permanece NaN
            .astype("Int64")                   # inteiro nullable (sem crash)
        )

        resultados.append(
            df_ano[
                [
                    "ticker",
                    "ano",
                    "score_qualidade",
                    "score_crescimento",
                    "score_rentabilidade",
                    "score_total",
                    "ranking",
                ]
            ]
        )

    df_final = pd.concat(resultados, ignore_index=True)

    # Postgres-safe: NaN -> None
    df_final = df_final.replace({np.nan: None})

    if progress_cb:
        progress_cb(f"FUNDAMENTAL SCORE: gravando em {DST_FULL}…")

    upsert = f"""
    insert into {DST_FULL} (
        ticker, ano,
        score_qualidade, score_crescimento,
        score_rentabilidade, score_total, ranking
    )
    values (
        :ticker, :ano,
        :score_qualidade, :score_crescimento,
        :score_rentabilidade, :score_total, :ranking
    )
    on conflict (ticker, ano) do update set
        score_qualidade = excluded.score_qualidade,
        score_crescimento = excluded.score_crescimento,
        score_rentabilidade = excluded.score_rentabilidade,
        score_total = excluded.score_total,
        ranking = excluded.ranking;
    """

    with engine.begin() as conn:
        conn.execute(text(upsert), df_final.to_dict(orient="records"))

    if progress_cb:
        progress_cb("FUNDAMENTAL SCORE: concluído.")

    return df_final
