from __future__ import annotations

from typing import Callable, Optional, Dict, Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ============================================================
# Infraestrutura Supabase
# ============================================================
def _ensure_table(engine: Engine) -> None:
    ddl = """
    create schema if not exists cvm;

    create table if not exists cvm.Fundamental_Score (
        Ticker text not null,
        Ano integer not null,

        Score_Qualidade double precision,
        Score_Crescimento double precision,
        Score_Rentabilidade double precision,
        Score_Total double precision,
        Ranking integer,

        primary key (Ticker, Ano)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


# ============================================================
# Funções auxiliares
# ============================================================
def _safe_zscore(s: pd.Series, invert: bool = False) -> pd.Series:
    """
    Z-score robusto:
    - ignora NaN
    - se std == 0 (ou tudo NaN), devolve 0 (neutro)
    - opcionalmente inverte o sinal
    """
    s = pd.to_numeric(s, errors="coerce")
    mean = s.mean(skipna=True)
    std = s.std(skipna=True, ddof=0)

    if pd.isna(std) or std == 0:
        z = pd.Series(0.0, index=s.index)
    else:
        z = (s - mean) / std

    if invert:
        z = -z

    # z pode ter NaN onde s era NaN; mantemos NaN (não atrapalha nas somas com fillna)
    return z


def _require_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"FUNDAMENTAL SCORE: colunas ausentes em cvm.Financial_Metrics: {missing}"
        )


# ============================================================
# Função principal
# ============================================================
def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    """
    Algoritmo 4 — Scoring Fundamentalista

    - Normalização por z-score (robusto, sem SciPy)
    - Scores agregados
    - Ranking anual
    - Persistência em cvm.Fundamental_Score

    Compatível com pipeline: aceita progress_cb.
    """

    _ensure_table(engine)

    if progress_cb:
        progress_cb("FUNDAMENTAL SCORE: lendo Financial_Metrics…")

    df = pd.read_sql(
        """
        select
            Ticker,
            Ano,
            Margem_EBIT,
            Margem_Liquida,
            ROE,
            ROIC,
            CAGR_Receita,
            CAGR_Lucro
        from cvm.Financial_Metrics
        """,
        engine,
    )

    if df.empty:
        if progress_cb:
            progress_cb("FUNDAMENTAL SCORE: Financial_Metrics vazio. Nada a fazer.")
        return df

    # Garante que as colunas usadas existem
    _require_columns(
        df,
        [
            "Ticker",
            "Ano",
            "Margem_EBIT",
            "Margem_Liquida",
            "ROE",
            "ROIC",
            "CAGR_Receita",
            "CAGR_Lucro",
        ],
    )

    # Normaliza tipos básicos
    df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce").astype("Int64")
    df["Ticker"] = df["Ticker"].astype(str)

    # Remove linhas sem Ano/Ticker (evita explosão de grupos)
    df = df.dropna(subset=["Ano", "Ticker"])
    if df.empty:
        if progress_cb:
            progress_cb("FUNDAMENTAL SCORE: sem linhas válidas após limpeza.")
        return df

    # --------------------------------------------------------
    # Definição de métricas e pesos (iguais ao notebook)
    # --------------------------------------------------------
    metricas: Dict[str, Dict[str, Any]] = {
        "Margem_EBIT": {"peso": 0.2, "invert": False},
        "Margem_Liquida": {"peso": 0.2, "invert": False},
        "ROE": {"peso": 0.2, "invert": False},
        "ROIC": {"peso": 0.2, "invert": False},
        "CAGR_Receita": {"peso": 0.1, "invert": False},
        "CAGR_Lucro": {"peso": 0.1, "invert": False},
    }

    if progress_cb:
        progress_cb("FUNDAMENTAL SCORE: calculando scores por ano…")

    resultados: list[pd.DataFrame] = []

    # --------------------------------------------------------
    # Cálculo dos scores por ano
    # --------------------------------------------------------
    for ano, df_ano in df.groupby("Ano"):
        df_ano = df_ano.copy()

        # z-scores por métrica
        for m, cfg in metricas.items():
            df_ano[f"z_{m}"] = _safe_zscore(df_ano[m], invert=bool(cfg["invert"]))

        # Scores parciais (mantendo a mesma lógica do seu script)
        # Usamos fillna(0) nas somas para não derrubar o score quando uma métrica não existe.
        df_ano["Score_Qualidade"] = (
            df_ano["z_Margem_EBIT"].fillna(0) * 0.5
            + df_ano["z_Margem_Liquida"].fillna(0) * 0.5
        )

        df_ano["Score_Rentabilidade"] = (
            df_ano["z_ROE"].fillna(0) * 0.5
            + df_ano["z_ROIC"].fillna(0) * 0.5
        )

        df_ano["Score_Crescimento"] = (
            df_ano["z_CAGR_Receita"].fillna(0) * 0.5
            + df_ano["z_CAGR_Lucro"].fillna(0) * 0.5
        )

        df_ano["Score_Total"] = (
            df_ano["Score_Qualidade"] * 0.4
            + df_ano["Score_Rentabilidade"] * 0.4
            + df_ano["Score_Crescimento"] * 0.2
        )

        # Ranking denso, maior score = melhor
        df_ano["Ranking"] = (
            df_ano["Score_Total"]
            .rank(ascending=False, method="dense")
            .astype(int)
        )

        resultados.append(
            df_ano[
                [
                    "Ticker",
                    "Ano",
                    "Score_Qualidade",
                    "Score_Crescimento",
                    "Score_Rentabilidade",
                    "Score_Total",
                    "Ranking",
                ]
            ]
        )

    df_final = pd.concat(resultados, ignore_index=True)

    # Limpa inf/-inf e converte NaN -> None
    df_final = df_final.replace([np.inf, -np.inf], np.nan)
    df_final = df_final.where(pd.notnull(df_final), None)

    if progress_cb:
        progress_cb(f"FUNDAMENTAL SCORE: upsert ({len(df_final)} linhas)…")

    # --------------------------------------------------------
    # Persistência (UPSERT)
    # --------------------------------------------------------
    upsert = """
    insert into cvm.Fundamental_Score (
        Ticker, Ano,
        Score_Qualidade, Score_Crescimento,
        Score_Rentabilidade, Score_Total, Ranking
    )
    values (
        :Ticker, :Ano,
        :Score_Qualidade, :Score_Crescimento,
        :Score_Rentabilidade, :Score_Total, :Ranking
    )
    on conflict (Ticker, Ano) do update set
        Score_Qualidade = excluded.Score_Qualidade,
        Score_Crescimento = excluded.Score_Crescimento,
        Score_Rentabilidade = excluded.Score_Rentabilidade,
        Score_Total = excluded.Score_Total,
        Ranking = excluded.Ranking;
    """

    with engine.begin() as conn:
        conn.execute(text(upsert), df_final.to_dict(orient="records"))

    if progress_cb:
        progress_cb("FUNDAMENTAL SCORE: concluído.")

    return df_final
