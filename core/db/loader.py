from __future__ import annotations

from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
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


def _pick_schema(engine: Engine, table: str, preferred: str = "cvm") -> str:
    """
    Decide em qual schema está a tabela:
      - tenta preferred (default 'cvm')
      - senão tenta 'public'
    """
    if _table_exists(engine, preferred, table):
        return preferred
    if _table_exists(engine, "public", table):
        return "public"
    raise RuntimeError(f"Tabela '{table}' não encontrada em {preferred}.{table} nem public.{table}.")


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
    raise KeyError(f"Coluna não encontrada. Candidatos={candidates}. Existentes={sorted(cols)}")


def _q(col: str) -> str:
    """
    Quote apenas quando a coluna não for lower_snake (ex.: SETOR).
    """
    return f'"{col}"' if col.lower() != col else col


def _read_sql_df(engine: Engine, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


# ──────────────────────────────────────────────────────────────────────
# Loaders usados pelo app
# ──────────────────────────────────────────────────────────────────────
def load_setores(engine: Engine) -> pd.DataFrame:
    """
    Carrega setores do Supabase.
    Aceita colunas em maiúsculo (SETOR/SUBSETOR/SEGMENTO) ou minúsculo.
    """
    schema = _pick_schema(engine, "setores", preferred="cvm")
    cols = _get_columns(engine, schema, "setores")

    col_ticker = _pick_col(cols, "ticker", "Ticker")
    col_setor = _pick_col(cols, "setor", "SETOR")
    col_subsetor = _pick_col(cols, "subsetor", "SUBSETOR")
    col_segmento = _pick_col(cols, "segmento", "SEGMENTO")
    col_nome = _pick_col(cols, "nome_empresa", "NOME_EMPRESA", "nome", "NOME")

    sql = f"""
    select
        {_q(col_ticker)}   as ticker,
        {_q(col_setor)}    as setor,
        {_q(col_subsetor)} as subsetor,
        {_q(col_segmento)} as segmento,
        {_q(col_nome)}     as nome_empresa
    from {schema}.setores
    """

    df = _read_sql_df(engine, sql)

    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()

    for c in ["setor", "subsetor", "segmento", "nome_empresa"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df


def load_demonstracoes_financeiras(engine: Engine) -> pd.DataFrame:
    """
    DFP anual (sua tabela grande): cvm.demonstracoes_financeiras
    Retorna um DataFrame com todas as colunas existentes.
    """
    schema = _pick_schema(engine, "demonstracoes_financeiras", preferred="cvm")
    sql = f"select * from {schema}.demonstracoes_financeiras"
    df = _read_sql_df(engine, sql)

    # Normalizações mínimas (não assume todas colunas)
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")

    return df


def load_demonstracoes_financeiras_tri(engine: Engine) -> pd.DataFrame:
    """
    ITR trimestral: cvm.demonstracoes_financeiras_tri
    """
    schema = _pick_schema(engine, "demonstracoes_financeiras_tri", preferred="cvm")
    sql = f"select * from {schema}.demonstracoes_financeiras_tri"
    df = _read_sql_df(engine, sql)

    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")

    return df


# ──────────────────────────────────────────────────────────────────────
# Aliases de compatibilidade (evita quebrar imports antigos nas pages)
# ──────────────────────────────────────────────────────────────────────
# Algumas versões antigas do projeto podem importar nomes diferentes.
load_dfp = load_demonstracoes_financeiras
load_itr = load_demonstracoes_financeiras_tri
load_dfp_df = load_demonstracoes_financeiras
load_itr_df = load_demonstracoes_financeiras_tri
