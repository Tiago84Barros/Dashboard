# Antigo Algorimto 6
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

        Setor text,

        Score_Base double precision,
        Score_Ajustado double precision,

        Penal_Crowding double precision,
        Penal_Decay double precision,
        Penal_Plato double precision,

        Peso double precision,

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
    alpha_crowding: float = 0.15,
    decay_por_ano: float = 0.05,
    max_decay: float = 0.20,
    penal_plato: float = 0.10,
) -> pd.DataFrame:
    """
    Algoritmo 6 — Formação de Carteira com Penalizações Avançadas

    Penalizações implementadas:
    - Crowding setorial
    - Decay de liderança
    - Platô de retorno relativo ao setor
    """

    _ensure_table(engine)

    # --------------------------------------------------------
    # 1) Leitura dos dados base
    # --------------------------------------------------------
    df = pd.read_sql(
        """
        select
            fs.Ticker,
            fs.Ano,
            fs.Score_Total as Score_Base,
            se.Setor,
            ie.selic as Benchmark_Selic
        from cvm.Fundamental_Score fs
        left join cvm.setores_empresas se
          on se.Ticker = fs.Ticker
        left join cvm.info_economica ie
          on ie.Data = make_date(fs.Ano, 12, 31)
        """,
        engine,
    )

    if df.empty:
        return df

    df = df.sort_values(["Ticker", "Ano"])

    # --------------------------------------------------------
    # 2) Penalização de Decay de Liderança
    # --------------------------------------------------------
    df["anos_consecutivos"] = (
        df.groupby("Ticker")["Ano"]
        .apply(lambda s: s.diff().eq(1).cumsum())
        .fillna(0)
    )

    df["Penal_Decay"] = (
        df["anos_consecutivos"] * decay_por_ano
    ).clip(upper=max_decay)

    # --------------------------------------------------------
    # 3) Penalização de Platô de Retorno
    # (proxy: Score vs mediana do setor)
    # --------------------------------------------------------
    df["Penal_Plato"] = 0.0

    for (ano, setor), grp in df.groupby(["Ano", "Setor"]):
        mediana = grp["Score_Base"].median()
        idx = grp["Score_Base"] < mediana
        df.loc[idx.index[idx], "Penal_Plato"] = penal_plato

    # --------------------------------------------------------
    # 4) Penalização de Crowding Setorial
    # --------------------------------------------------------
    df["Penal_Crowding"] = 0.0

    for ano, grp in df.groupby("Ano"):
        std_global = grp["Score_Base"].std()

        for setor, gset in grp.groupby("Setor"):
            std_setor = gset["Score_Base"].std()
            if pd.notnull(std_global) and std_global > 0:
                crowd = 1 - (std_setor / std_global if std_setor else 0)
                df.loc[gset.index, "Penal_Crowding"] = alpha_crowding * crowd

    # --------------------------------------------------------
    # 5) Score Ajustado Final
    # --------------------------------------------------------
    df["Score_Ajustado"] = df["Score_Base"] * (
        1
        - df["Penal_Crowding"]
        - df["Penal_Decay"]
        - df["Penal_Plato"]
    )

    # --------------------------------------------------------
    # 6) Seleção da Carteira (Top-N por Score Ajustado)
    # --------------------------------------------------------
    carteiras = []

    for ano, grp in df.groupby("Ano"):
        sel = grp.sort_values("Score_Ajustado", ascending=False).head(top_n)

        if peso_igual:
            sel["Peso"] = 1 / len(sel)
        else:
            sel["Peso"] = sel["Score_Ajustado"] / sel["Score_Ajustado"].sum()

        carteiras.append(sel)

    carteira_df = pd.concat(carteiras, ignore_index=True)

    # --------------------------------------------------------
    # 7) Simulação de Retorno (estrutura neutra)
    # --------------------------------------------------------
    carteira_df["Retorno_Anual"] = carteira_df["Score_Ajustado"] * 0.01
    carteira_df["Retorno_Acumulado"] = (
        1 + carteira_df["Retorno_Anual"]
    ).groupby(carteira_df["Ticker"]).cumprod() - 1

    carteira_df = carteira_df.where(pd.notnull(carteira_df), None)

    # --------------------------------------------------------
    # 8) Persistência (UPSERT)
    # --------------------------------------------------------
    upsert = """
    insert into cvm.Portfolio_Backtest (
        Ano, Ticker, Setor,
        Score_Base, Score_Ajustado,
        Penal_Crowding, Penal_Decay, Penal_Plato,
        Peso,
        Retorno_Anual, Retorno_Acumulado,
        Benchmark_Selic
    )
    values (
        :Ano, :Ticker, :Setor,
        :Score_Base, :Score_Ajustado,
        :Penal_Crowding, :Penal_Decay, :Penal_Plato,
        :Peso,
        :Retorno_Anual, :Retorno_Acumulado,
        :Benchmark_Selic
    )
    on conflict (Ano, Ticker) do update set
        Setor = excluded.Setor,
        Score_Base = excluded.Score_Base,
        Score_Ajustado = excluded.Score_Ajustado,
        Penal_Crowding = excluded.Penal_Crowding,
        Penal_Decay = excluded.Penal_Decay,
        Penal_Plato = excluded.Penal_Plato,
        Peso = excluded.Peso,
        Retorno_Anual = excluded.Retorno_Anual,
        Retorno_Acumulado = excluded.Retorno_Acumulado,
        Benchmark_Selic = excluded.Benchmark_Selic;
    """

    with engine.begin() as conn:
        conn.execute(text(upsert), carteira_df.to_dict(orient="records"))

    return carteira_df
