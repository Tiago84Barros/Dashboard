from __future__ import annotations

from typing import Optional, Callable

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


SCHEMA = "cvm"
TABLE = "financial_metrics"  # sem aspas -> postgres usa lowercase


def _ensure_table(engine: Engine) -> None:
    ddl = f"""
    create schema if not exists {SCHEMA};

    create table if not exists {SCHEMA}.{TABLE} (
        ticker text not null,
        ano integer not null,

        receita_liquida double precision,
        ebit double precision,
        lucro_liquido double precision,

        margem_ebit double precision,
        margem_liquida double precision,

        roe double precision,
        roic double precision,

        cagr_receita double precision,
        cagr_lucro double precision,

        primary key (ticker, ano)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _safe_div(num, den):
    if num is None or den is None:
        return None
    try:
        if den == 0:
            return None
        return float(num) / float(den)
    except Exception:
        return None


def _calc_cagr(series: pd.Series) -> float | None:
    s = series.dropna()
    if len(s) < 2:
        return None
    n = len(s) - 1
    try:
        first = s.iloc[0]
        last = s.iloc[-1]
        if first in (0, None) or pd.isna(first):
            return None
        return (last / first) ** (1 / n) - 1
    except Exception:
        return None


def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
    start_year: int = 2010,
    batch: int = 5000,
) -> pd.DataFrame:
    _ensure_table(engine)

    if progress_cb:
        progress_cb("MÉTRICAS: lendo base anual (DFP)…")

    # IMPORTANTÍSSIMO: aliases padronizados em lowercase
    df = pd.read_sql(
        text(
            f"""
            select
                ticker as ticker,
                extract(year from data)::int as ano,
                receita_liquida as receita_liquida,
                ebit as ebit,
                lucro_liquido as lucro_liquido,
                patrimonio_liquido as patrimonio_liquido,
                ativo_total as ativo_total,
                divida_total as divida_total
            from {SCHEMA}.demonstracoes_financeiras
            where extract(year from data)::int >= :start_year
            """
        ),
        engine,
        params={"start_year": start_year},
    )

    if df.empty:
        if progress_cb:
            progress_cb("MÉTRICAS: base vazia. Nada a fazer.")
        return df

    # Normaliza numéricos
    num_cols = [
        "receita_liquida",
        "ebit",
        "lucro_liquido",
        "patrimonio_liquido",
        "ativo_total",
        "divida_total",
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    if progress_cb:
        progress_cb("MÉTRICAS: calculando margens e retornos…")

    df["margem_ebit"] = df.apply(lambda r: _safe_div(r["ebit"], r["receita_liquida"]), axis=1)
    df["margem_liquida"] = df.apply(lambda r: _safe_div(r["lucro_liquido"], r["receita_liquida"]), axis=1)
    df["roe"] = df.apply(lambda r: _safe_div(r["lucro_liquido"], r["patrimonio_liquido"]), axis=1)

    invested_capital = df["ativo_total"] - df["divida_total"]
    df["roic"] = [
        _safe_div(e, ic) for e, ic in zip(df["ebit"].tolist(), invested_capital.tolist())
    ]

    if progress_cb:
        progress_cb("MÉTRICAS: calculando CAGR por ticker…")

    resultados: list[dict] = []

    for ticker, df_emp in df.groupby("ticker", dropna=True):
        df_emp = df_emp.sort_values("ano")

        cagr_receita = _calc_cagr(df_emp["receita_liquida"])
        cagr_lucro = _calc_cagr(df_emp["lucro_liquido"])

        for _, row in df_emp.iterrows():
            resultados.append(
                {
                    "ticker": ticker,
                    "ano": int(row["ano"]),
                    "receita_liquida": row["receita_liquida"],
                    "ebit": row["ebit"],
                    "lucro_liquido": row["lucro_liquido"],
                    "margem_ebit": row["margem_ebit"],
                    "margem_liquida": row["margem_liquida"],
                    "roe": row["roe"],
                    "roic": row["roic"],
                    "cagr_receita": cagr_receita,
                    "cagr_lucro": cagr_lucro,
                }
            )

    df_final = pd.DataFrame(resultados).replace({np.nan: None})
    if df_final.empty:
        if progress_cb:
            progress_cb("MÉTRICAS: nada para persistir.")
        return df_final

    if progress_cb:
        progress_cb(f"MÉTRICAS: persistindo {len(df_final)} linhas (upsert)…")

    upsert = f"""
    insert into {SCHEMA}.{TABLE} (
        ticker, ano,
        receita_liquida, ebit, lucro_liquido,
        margem_ebit, margem_liquida,
        roe, roic,
        cagr_receita, cagr_lucro
    )
    values (
        :ticker, :ano,
        :receita_liquida, :ebit, :lucro_liquido,
        :margem_ebit, :margem_liquida,
        :roe, :roic,
        :cagr_receita, :cagr_lucro
    )
    on conflict (ticker, ano) do update set
        receita_liquida = excluded.receita_liquida,
        ebit = excluded.ebit,
        lucro_liquido = excluded.lucro_liquido,
        margem_ebit = excluded.margem_ebit,
        margem_liquida = excluded.margem_liquida,
        roe = excluded.roe,
        roic = excluded.roic,
        cagr_receita = excluded.cagr_receita,
        cagr_lucro = excluded.cagr_lucro;
    """

    rows = df_final.to_dict(orient="records")

    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(upsert), rows[i : i + batch])

    if progress_cb:
        progress_cb("MÉTRICAS: concluído.")
    return df_final
