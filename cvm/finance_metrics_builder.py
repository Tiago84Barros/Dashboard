from __future__ import annotations

from typing import Optional, Callable

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ============================================================
# Infraestrutura
# ============================================================
SCHEMA = "cvm"
TABLE = "Financial_Metrics"  # sem aspas -> Postgres cria como financial_metrics


def _ensure_table(engine: Engine) -> None:
    ddl = f"""
    create schema if not exists {SCHEMA};

    create table if not exists {SCHEMA}.{TABLE} (
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
# Métricas auxiliares
# ============================================================
def _safe_div(num, den):
    """Evita divisão por zero e propaga None."""
    if num is None or den is None:
        return None
    try:
        if den == 0:
            return None
        return num / den
    except Exception:
        return None


def _calc_cagr(series: pd.Series) -> float | None:
    series = series.dropna()
    if len(series) < 2:
        return None
    n = len(series) - 1
    try:
        first = series.iloc[0]
        last = series.iloc[-1]
        if first in (0, None) or pd.isna(first):
            return None
        return (last / first) ** (1 / n) - 1
    except Exception:
        return None


# ============================================================
# Função principal
# ============================================================
def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
    start_year: int = 2010,
    batch: int = 5000,
) -> pd.DataFrame:
    """
    Construção de Métricas Financeiras:
    - Consome dados anuais (DFP) da tabela cvm.Demonstracoes_Financeiras
    - Calcula margens, ROE, ROIC
    - Calcula CAGR por empresa
    - Persiste em cvm.Financial_Metrics (upsert)
    """

    if progress_cb:
        progress_cb("MÉTRICAS: garantindo tabela…")
    _ensure_table(engine)

    if progress_cb:
        progress_cb("MÉTRICAS: lendo base anual (DFP)…")

    df = pd.read_sql(
        text(
            f"""
            select
                Ticker,
                extract(year from Data)::int as Ano,
                Receita_Liquida,
                EBIT,
                Lucro_Liquido,
                Patrimonio_Liquido,
                Ativo_Total,
                Divida_Total
            from {SCHEMA}.Demonstracoes_Financeiras
            where extract(year from Data)::int >= :start_year
            """
        ),
        engine,
        params={"start_year": start_year},
    )

    if df.empty:
        if progress_cb:
            progress_cb("MÉTRICAS: base vazia. Nada a fazer.")
        return df

    # Normaliza tipos numéricos (evita strings e objetos)
    num_cols = [
        "Receita_Liquida", "EBIT", "Lucro_Liquido",
        "Patrimonio_Liquido", "Ativo_Total", "Divida_Total",
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    if progress_cb:
        progress_cb("MÉTRICAS: calculando margens e retornos…")

    # Métricas básicas com segurança
    df["Margem_EBIT"] = df.apply(lambda r: _safe_div(r["EBIT"], r["Receita_Liquida"]), axis=1)
    df["Margem_Liquida"] = df.apply(lambda r: _safe_div(r["Lucro_Liquido"], r["Receita_Liquida"]), axis=1)

    df["ROE"] = df.apply(lambda r: _safe_div(r["Lucro_Liquido"], r["Patrimonio_Liquido"]), axis=1)

    # ROIC = EBIT / (Ativo_Total - Divida_Total)  (blindando denominador zero)
    invested_capital = df["Ativo_Total"] - df["Divida_Total"]
    df["ROIC"] = [
        _safe_div(e, ic) for e, ic in zip(df["EBIT"].tolist(), invested_capital.tolist())
    ]

    if progress_cb:
        progress_cb("MÉTRICAS: calculando CAGR por ticker…")

    resultados: list[dict] = []

    for ticker, df_emp in df.groupby("Ticker", dropna=True):
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
    if df_final.empty:
        if progress_cb:
            progress_cb("MÉTRICAS: nada para persistir.")
        return df_final

    if progress_cb:
        progress_cb(f"MÉTRICAS: persistindo {len(df_final)} linhas (upsert)…")

    upsert = f"""
    insert into {SCHEMA}.{TABLE} (
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

    # NaN -> None para o driver
    df_final = df_final.replace({np.nan: None})
    rows = df_final.to_dict(orient="records")

    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(upsert), rows[i : i + batch])

    if progress_cb:
        progress_cb("MÉTRICAS: concluído.")
    return df_final
