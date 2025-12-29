from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ============================================================
# Helpers
# ============================================================

def _norm_ticker(t: str) -> str:
    t = (t or "").strip().upper()
    return t.replace(".SA", "")


def _ensure_multiplos_table(engine: Engine, table: str = "cvm.multiplos") -> None:
    """
    Cria a tabela cvm.multiplos se ela não existir.
    PK: (ticker, ano)
    """
    schema, _, name = table.partition(".")
    if not name:
        schema, name = "public", schema

    ddl = f"""
    create table if not exists {schema}.{name} (
        ticker text not null,
        ano int not null,

        -- preço referência (último pregão do ano)
        preco_fim_ano double precision,

        -- valuation / qualidade (determinísticos com dados disponíveis)
        pl double precision,
        roe double precision,

        margem_liquida double precision,
        margem_ebit double precision,

        divida_liquida_ebit double precision,
        divida_total_patrimonio double precision,

        fetched_at timestamptz not null default now(),

        primary key (ticker, ano)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    """
    Divisão segura: NaN quando b==0 ou NaN.
    """
    out = a / b
    out[(b == 0) | b.isna()] = pd.NA
    return out


def _load_dfp(engine: Engine) -> pd.DataFrame:
    """
    Carrega DFP anual. Assume:
      - ticker
      - data (data do balanço)
      - receita_liquida, ebit, lucro_liquido, lpa, patrimonio_liquido
      - divida_total, divida_liquida (se existirem)
    """
    df = pd.read_sql(
        text(
            """
            select
                ticker,
                data,
                receita_liquida,
                ebit,
                lucro_liquido,
                lpa,
                ativo_total,
                patrimonio_liquido,
                divida_total,
                divida_liquida
            from cvm.demonstracoes_financeiras_dfp
            """
        ),
        con=engine,
    )

    if df.empty:
        return df

    df["ticker"] = df["ticker"].astype(str).map(_norm_ticker)
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df = df.dropna(subset=["ticker", "data"])

    df["ano"] = df["data"].dt.year.astype(int)

    # numéricos
    for c in [
        "receita_liquida",
        "ebit",
        "lucro_liquido",
        "lpa",
        "ativo_total",
        "patrimonio_liquido",
        "divida_total",
        "divida_liquida",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def _load_year_end_prices(engine: Engine) -> pd.DataFrame:
    """
    Preço do último pregão do ano, já pré-calculado em cvm.prices_b3_yearly:
      - ticker
      - ano
      - close (preço)
    """
    df = pd.read_sql(
        text(
            """
            select
                ticker,
                ano,
                close as preco_fim_ano
            from cvm.prices_b3_yearly
            where close is not null
            """
        ),
        con=engine,
    )

    if df.empty:
        return df

    df["ticker"] = df["ticker"].astype(str).map(_norm_ticker)
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    df["preco_fim_ano"] = pd.to_numeric(df["preco_fim_ano"], errors="coerce")
    df = df.dropna(subset=["ticker", "ano"])
    df["ano"] = df["ano"].astype(int)

    return df[["ticker", "ano", "preco_fim_ano"]]


@dataclass
class RebuildResult:
    ok: bool
    rows: int = 0
    error: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return {"ok": self.ok, "rows": self.rows, "error": self.error}


# ============================================================
# Public API
# ============================================================

def rebuild_multiplos_universe(engine: Engine, *, table: str = "cvm.multiplos") -> Dict[str, Any]:
    """
    Recalcula e grava cvm.multiplos para TODO o universo:
    - junta DFP anual com preço do último pregão do ano (prices_b3_yearly)
    - calcula múltiplos e métricas derivadas disponíveis
    - upsert em (ticker, ano)

    Retorna: {"ok": bool, "rows": int, "error": str|None}
    """
    try:
        _ensure_multiplos_table(engine, table=table)

        dfp = _load_dfp(engine)
        if dfp.empty:
            return RebuildResult(ok=False, rows=0, error="DFP vazio: cvm.demonstracoes_financeiras_dfp sem dados.").as_dict()

        prices = _load_year_end_prices(engine)
        if prices.empty:
            return RebuildResult(ok=False, rows=0, error="Prices vazio: cvm.prices_b3_yearly sem dados. Rode o sync de preços anual.").as_dict()

        # join por ticker+ano
        df = dfp.merge(prices, on=["ticker", "ano"], how="left")

        # -------------------------
        # Métricas determinísticas
        # -------------------------

        # P/L = preço fim ano / LPA
        if "lpa" in df.columns:
            df["pl"] = _safe_div(df["preco_fim_ano"], df["lpa"])
        else:
            df["pl"] = pd.NA

        # ROE = lucro líquido / patrimônio líquido
        df["roe"] = _safe_div(df["lucro_liquido"], df["patrimonio_liquido"])

        # margens
        df["margem_liquida"] = _safe_div(df["lucro_liquido"], df["receita_liquida"])
        df["margem_ebit"] = _safe_div(df["ebit"], df["receita_liquida"])

        # alavancagem (quando existir)
        df["divida_liquida_ebit"] = _safe_div(df["divida_liquida"], df["ebit"])
        df["divida_total_patrimonio"] = _safe_div(df["divida_total"], df["patrimonio_liquido"])

        # -------------------------
        # Payload
        # -------------------------
        out = df[
            [
                "ticker",
                "ano",
                "preco_fim_ano",
                "pl",
                "roe",
                "margem_liquida",
                "margem_ebit",
                "divida_liquida_ebit",
                "divida_total_patrimonio",
            ]
        ].copy()

        out = out.dropna(subset=["ticker", "ano"])
        out["ticker"] = out["ticker"].astype(str).map(_norm_ticker)
        out["ano"] = out["ano"].astype(int)

        schema, _, name = table.partition(".")
        if not name:
            schema, name = "public", schema

        sql = text(
            f"""
            insert into {schema}.{name} (
                ticker, ano,
                preco_fim_ano,
                pl, roe,
                margem_liquida, margem_ebit,
                divida_liquida_ebit,
                divida_total_patrimonio,
                fetched_at
            )
            values (
                :ticker, :ano,
                :preco_fim_ano,
                :pl, :roe,
                :margem_liquida, :margem_ebit,
                :divida_liquida_ebit,
                :divida_total_patrimonio,
                now()
            )
            on conflict (ticker, ano)
            do update set
                preco_fim_ano = excluded.preco_fim_ano,
                pl = excluded.pl,
                roe = excluded.roe,
                margem_liquida = excluded.margem_liquida,
                margem_ebit = excluded.margem_ebit,
                divida_liquida_ebit = excluded.divida_liquida_ebit,
                divida_total_patrimonio = excluded.divida_total_patrimonio,
                fetched_at = excluded.fetched_at
            """
        )

        payload = out.to_dict("records")
        if not payload:
            return RebuildResult(ok=True, rows=0).as_dict()

        with engine.begin() as conn:
            conn.execute(sql, payload)

        return RebuildResult(ok=True, rows=len(payload)).as_dict()

    except Exception as e:
        return RebuildResult(ok=False, rows=0, error=str(e)).as_dict()
