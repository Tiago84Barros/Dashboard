from __future__ import annotations

from typing import Callable, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from scipy.stats import zscore


SCHEMA = "cvm"
SRC_TABLE = "financial_metrics"     # tabela de entrada (o Postgres tende a guardar em minúsculo)
DST_TABLE = "fundamental_score"     # tabela de saída

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
# Funções auxiliares
# ============================================================
def _zscore(df: pd.DataFrame, col: str, invert: bool = False) -> pd.Series:
    # zscore precisa de float; nan_policy omit ignora NaN
    s = zscore(df[col].astype(float), nan_policy="omit")
    if invert:
        s = -s
    # zscore pode retornar ndarray; converte para Series alinhada
    return pd.Series(s, index=df.index)


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
    - Normaliza por z-score por ANO
    - Calcula scores agregados e ranking anual
    - Persiste em cvm.fundamental_score (minúsculo, Postgres-friendly)
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

    # garante tipos mínimos
    df["ano"] = df["ano"].astype(int)

    if progress_cb:
        progress_cb("FUNDAMENTAL SCORE: calculando scores por ano…")

    metricas = {
        "margem_ebit": {"peso": 0.2, "invert": False},
        "margem_liquida": {"peso": 0.2, "invert": False},
        "roe": {"peso": 0.2, "invert": False},
        "roic": {"peso": 0.2, "invert": False},
        "cagr_receita": {"peso": 0.1, "invert": False},
        "cagr_lucro": {"peso": 0.1, "invert": False},
    }

    resultados = []

    for ano, df_ano in df.groupby("ano", sort=True):
        df_ano = df_ano.copy()

        # z-scores
        for m, cfg in metricas.items():
            df_ano[f"z_{m}"] = _zscore(df_ano, m, cfg["invert"])

        # scores parciais (mesma lógica)
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

        df_ano["ranking"] = (
            df_ano["score_total"]
            .rank(ascending=False, method="dense")
            .astype(int)
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
    df_final = df_final.where(pd.notnull(df_final), None)

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
