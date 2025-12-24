from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def _table_exists(engine: Engine, schema: str, table: str) -> bool:
    sql = """
    select 1
    from information_schema.tables
    where table_schema = :schema
      and table_name = :table
    limit 1
    """
    with engine.begin() as conn:
        return conn.execute(text(sql), {"schema": schema, "table": table}).fetchone() is not None


def _get_columns(engine: Engine, schema: str, table: str) -> set[str]:
    sql = """
    select column_name
    from information_schema.columns
    where table_schema = :schema
      and table_name = :table
    """
    with engine.begin() as conn:
        rows = conn.execute(text(sql), {"schema": schema, "table": table}).fetchall()
    return {str(r[0]) for r in rows}


def _pick_col(cols: set[str], *candidates: str) -> str:
    for c in candidates:
        if c in cols:
            return c
    raise KeyError(f"Colunas esperadas não encontradas. Candidatos={candidates}. Existentes={sorted(cols)}")


def _q(col: str) -> str:
    """
    Quote apenas quando a coluna não for lower_snake (ex.: SETOR).
    """
    return f'"{col}"' if col.lower() != col else col


def load_setores(engine: Engine) -> pd.DataFrame:
    """
    Carrega setores do Supabase.

    Prioridade:
      1) cvm.setores (seu caso)
      2) public.setores (fallback, se existir)

    Também trata colunas em maiúsculo (ex.: "SETOR") ou minúsculo.
    """
    schema = "cvm" if _table_exists(engine, "cvm", "setores") else ("public" if _table_exists(engine, "public", "setores") else None)
    if schema is None:
        raise RuntimeError("Tabela 'setores' não encontrada em cvm.setores nem public.setores.")

    cols = _get_columns(engine, schema, "setores")

    col_ticker = _pick_col(cols, "ticker", "Ticker")
    col_setor = _pick_col(cols, "setor", "SETOR")
    col_subsetor = _pick_col(cols, "subsetor", "SUBSETOR")
    col_segmento = _pick_col(cols, "segmento", "SEGMENTO")
    col_nome = _pick_col(cols, "nome_empresa", "NOME_EMPRESA", "nome", "NOME")

    sql = f"""
    select
        {_q(col_ticker)} as ticker,
        {_q(col_setor)} as setor,
        {_q(col_subsetor)} as subsetor,
        {_q(col_segmento)} as segmento,
        {_q(col_nome)} as nome_empresa
    from {schema}.setores
    """

    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn)

    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    for c in ["setor", "subsetor", "segmento", "nome_empresa"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df
