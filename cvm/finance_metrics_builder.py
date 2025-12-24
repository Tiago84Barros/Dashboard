from __future__ import annotations

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sklearn.linear_model import LinearRegression


# ============================================================
# Infraestrutura
# ============================================================
def _ensure_table(engine: Engine):
    ddl = """
    create schema if not exists cvm;

    create table if not exists cvm.Financial_Metrics (
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


# ============================================================
# Métricas auxiliares (mesma lógica do notebook)
# ============================================================
def _calc_cagr(series: pd.Series) -> float | None:
    series = series.dropna()
    if len(series) < 2:
        return None
    n = len(series) - 1
    try:
        return (series.iloc[-1] / series.iloc[0]) ** (1 / n) - 1
    except Exception:
        return None


def _calc_slope(series: pd.Series) -> float | None:
    series = series.dropna()
    if len(series) < 2:
        return None

    X = np.arange(len(series)).reshape(-1, 1)
    y = series.values.reshape(-1, 1)

    try:
        model = LinearRegression()
        model.fit(X, y)
        return float(model.coef_[0][0])
    except Exception:
        return None


# ============================================================
# Função principal
# ============================================================
def run(engine: Engine) -> pd.DataFrame:
    """
    Algoritmo 2 — Construção de Métricas Financeiras
    Conversão fiel do notebook:
    - Consome dados anuais (DFP)
    - Calcula margens, ROE, ROIC
    - Calcula crescimento (CAGR)
    - Persiste em tabela curada no Supabase
    """

    _ensure_table(engine)

    # --------------------------------------------------------
    # 1) Leitura dos dados base
    # --------------------------------------------------------
    df = pd.read_sql(
        """
        select
            Ticker,
            extract(year from Data)::int as Ano,
            Receita_Liquida,
            EBIT,
            Lucro_Liquido,
            Patrimonio_Liquido,
            Ativo_Total,
            Divida_Total
        from cvm.Demonstracoes_Financeiras
        """,
        engine,
    )

    if df.empty:
        return df

    # --------------------------------------------------------
    # 2) Métricas básicas
    # --------------------------------------------------------
    df["Margem_EBIT"] = df["EBIT"] / df["Receita_Liquida"]
    df["Margem_Liquida"] = df["Lucro_Liquido"] / df["Receita_Liquida"]

    df["ROE"] = df["Lucro_Liquido"] / df["Patrimonio_Liquido"]
    df["ROIC"] = df["EBIT"] / (df["Ativo_Total"] - df["Divida_Total"])

    # --------------------------------------------------------
    # 3) Métricas de crescimento (por empresa)
    # --------------------------------------------------------
    resultados = []

    for ticker, df_emp in df.groupby("Ticker"):
        df_emp = df_emp.sort_values("Ano")

        cagr_receita = _calc_cagr(df_emp["Receita_Liquida"])
        cagr_lucro = _calc_cagr(df_emp["Lucro_Liquido"])

        for _, row in df_emp.iterrows():
            resultados.append(
                {
                    "Ticker": ticker,
                    "Ano": int(row["Ano"]),
                    "Receita_Liquida": row["Receita_Liquida"],
                    "EBIT": row["EBIT"],
                    "Lucro_Liquido": row["Lucro_Liquido"],
                    "Margem_EBIT": row["Margem_EBIT"],
                    "Margem_Liquida": row["Margem_Liquida"],
                    "ROE": row["ROE"],
                    "ROIC": row["ROIC"],
                    "CAGR_Receita": cagr_receita,
                    "CAGR_Lucro": cagr_lucro,
                }
            )

    df_final = pd.DataFrame(resultados)

    # --------------------------------------------------------
    # 4) Persistência (UPSERT)
    # --------------------------------------------------------
    upsert = """
    insert into cvm.Financial_Metrics (
        Ticker, Ano,
        Receita_Liquida, EBIT, Lucro_Liquido,
        Margem_EBIT, Margem_Liquida,
        ROE, ROIC,
        CAGR_Receita, CAGR_Lucro
    )
    values (
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

    df_final = df_final.where(pd.notnull(df_final), None)

    with engine.begin() as conn:
        conn.execute(text(upsert), df_final.to_dict(orient="records"))

    return df_final
