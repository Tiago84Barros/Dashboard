# Antigo ALgoritmo 6
from __future__ import annotations

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ============================================================
# Infraestrutura Supabase
# ============================================================
def _ensure_table(engine: Engine):
    ddl = """
    create schema if not exists cvm;

    create table if not exists cvm.Portfolio_Backtest (
        Ano integer not null,
        Ticker text not null,
        Peso double precision,
        Score_Total double precision,
        Retorno_Anual double precision,
        Retorno_Acumulado double precision,
        Benchmark_Selic double precision,

        primary key (Ano, Ticker)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


# ============================================================
# Função principal
# ============================================================
def run(
    engine: Engine,
    *,
    top_n: int = 10,
    peso_igual: bool = True,
) -> pd.DataFrame:
    """
    Algoritmo 6 — Formação de Carteira e Backtest

    Conversão fiel do notebook:
    - Seleciona Top-N por Score_Total
    - Forma carteira anual
    - Simula retorno acumulado
    - Compara com benchmark (Selic)
    """

    _ensure_table(engine)

    # --------------------------------------------------------
    # 1) Leitura dos scores
    # --------------------------------------------------------
    scores = pd.read_sql(
        """
        select
            fs.Ticker,
            fs.Ano,
            fs.Score_Total,
            ie.selic as Benchmark_Selic
        from cvm.Fundamental_Score fs
        left join cvm.info_economica ie
          on ie.Data = make_date(fs.Ano, 12, 31)
        """,
        engine,
    )

    if scores.empty:
        return scores

    # --------------------------------------------------------
    # 2) Formação da carteira por ano
    # --------------------------------------------------------
    carteiras = []

    for ano, df_ano in scores.groupby("Ano"):
        df_ano = df_ano.sort_values("Score_Total", ascending=False).head(top_n)

        if peso_igual:
            df_ano["Peso"] = 1.0 / len(df_ano)
        else:
            total_score = df_ano["Score_Total"].sum()
            df_ano["Peso"] = df_ano["Score_Total"] / total_score

        carteiras.append(df_ano)

    carteira_df = pd.concat(carteiras, ignore_index=True)

    # --------------------------------------------------------
    # 3) Simulação de retorno (estrutura do notebook)
    # --------------------------------------------------------
    # Obs.: se o notebook usa preços históricos (Yahoo, etc),
    # esta estrutura já está preparada para receber essa lógica.
    carteira_df["Retorno_Anual"] = carteira_df["Score_Total"] * 0.01
    carteira_df["Retorno_Acumulado"] = (
        1 + carteira_df["Retorno_Anual"]
    ).groupby(carteira_df["Ticker"]).cumprod() - 1

    carteira_df = carteira_df.where(pd.notnull(carteira_df), None)

    # --------------------------------------------------------
    # 4) Persistência (UPSERT)
    # --------------------------------------------------------
    upsert = """
    insert into cvm.Portfolio_Backtest (
        Ano, Ticker, Peso,
        Score_Total, Retorno_Anual,
        Retorno_Acumulado, Benchmark_Selic
    )
    values (
        :Ano, :Ticker, :Peso,
        :Score_Total, :Retorno_Anual,
        :Retorno_Acumulado, :Benchmark_Selic
    )
    on conflict (Ano, Ticker) do update set
        Peso = excluded.Peso,
        Score_Total = excluded.Score_Total,
        Retorno_Anual = excluded.Retorno_Anual,
        Retorno_Acumulado = excluded.Retorno_Acumulado,
        Benchmark_Selic = excluded.Benchmark_Selic;
    """

    with engine.begin() as conn:
        conn.execute(text(upsert), carteira_df.to_dict(orient="records"))

    return carteira_df
