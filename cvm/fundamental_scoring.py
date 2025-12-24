# Antigo Algoritmo 4
from __future__ import annotations

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine
from scipy.stats import zscore


# ============================================================
# Infraestrutura Supabase
# ============================================================
def _ensure_table(engine: Engine):
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
# Funções auxiliares (mesma lógica do notebook)
# ============================================================
def _zscore(df: pd.DataFrame, col: str, invert: bool = False) -> pd.Series:
    s = zscore(df[col].astype(float), nan_policy="omit")
    if invert:
        s = -s
    return s


# ============================================================
# Função principal
# ============================================================
def run(engine: Engine) -> pd.DataFrame:
    """
    Algoritmo 4 — Scoring Fundamentalista

    Conversão fiel do notebook:
    - Normalização por z-score
    - Pesos fixos por métrica
    - Scores agregados
    - Ranking anual
    """

    _ensure_table(engine)

    # --------------------------------------------------------
    # 1) Leitura das métricas base
    # --------------------------------------------------------
    df = pd.read_sql(
        """
        select *
        from cvm.Financial_Metrics
        """,
        engine,
    )

    if df.empty:
        return df

    # --------------------------------------------------------
    # 2) Definição de métricas e pesos (iguais ao notebook)
    # --------------------------------------------------------
    metricas = {
        "Margem_EBIT": {"peso": 0.2, "invert": False},
        "Margem_Liquida": {"peso": 0.2, "invert": False},
        "ROE": {"peso": 0.2, "invert": False},
        "ROIC": {"peso": 0.2, "invert": False},
        "CAGR_Receita": {"peso": 0.1, "invert": False},
        "CAGR_Lucro": {"peso": 0.1, "invert": False},
    }

    # --------------------------------------------------------
    # 3) Cálculo dos scores por ano
    # --------------------------------------------------------
    resultados = []

    for ano, df_ano in df.groupby("Ano"):
        df_ano = df_ano.copy()

        for m, cfg in metricas.items():
            df_ano[f"z_{m}"] = _zscore(df_ano, m, cfg["invert"])

        # Scores parciais
        df_ano["Score_Qualidade"] = (
            df_ano["z_Margem_EBIT"] * 0.5
            + df_ano["z_Margem_Liquida"] * 0.5
        )

        df_ano["Score_Rentabilidade"] = (
            df_ano["z_ROE"] * 0.5
            + df_ano["z_ROIC"] * 0.5
        )

        df_ano["Score_Crescimento"] = (
            df_ano["z_CAGR_Receita"] * 0.5
            + df_ano["z_CAGR_Lucro"] * 0.5
        )

        df_ano["Score_Total"] = (
            df_ano["Score_Qualidade"] * 0.4
            + df_ano["Score_Rentabilidade"] * 0.4
            + df_ano["Score_Crescimento"] * 0.2
        )

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

    df_final = df_final.where(pd.notnull(df_final), None)

    # --------------------------------------------------------
    # 4) Persistência (UPSERT)
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

    return df_final
