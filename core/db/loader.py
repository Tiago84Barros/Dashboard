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
        r = conn.execute(text(sql), {"schema": schema, "table": table}).fetchone()
    return r is not None


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
    """
    Escolhe a primeira coluna existente dentre os candidatos.
    """
    for c in candidates:
        if c in cols:
            return c
    raise KeyError(f"Nenhuma das colunas esperadas existe. Candidatos={candidates}. Existentes={sorted(cols)}")


def load_setores(engine: Engine) -> pd.DataFrame:
    """
    Carrega a tabela de setores do Supabase.

    Prioridade:
      1) cvm.setores
      2) public.setores

    E lida com colunas podendo estar como:
      - setor/subsetor/segmento/nome_empresa
      - SETOR/SUBSETOR/SEGMENTO/nome_empresa
      - ou nomes levemente diferentes (fallback controlado)
    """
    schema = None
    if _table_exists(engine, "cvm", "setores"):
        schema = "cvm"
    elif _table_exists(engine, "public", "setores"):
        schema = "public"
    else:
        raise RuntimeError("Tabela 'setores' não encontrada nos schemas cvm ou public no Supabase.")

    cols = _get_columns(engine, schema, "setores")

    # ticker quase sempre é ticker mesmo
    col_ticker = _pick_col(cols, "ticker", "Ticker")

    # setor/subsetor/segmento podem existir em minúsculo ou maiúsculo (quoted)
    col_setor = _pick_col(cols, "setor", "SETOR")
    col_subsetor = _pick_col(cols, "subsetor", "SUBSETOR")
    col_segmento = _pick_col(cols, "segmento", "SEGMENTO")

    # nome_empresa pode variar
    col_nome = _pick_col(cols, "nome_empresa", "NOME_EMPRESA", "nome", "NOME")

    # Monta SQL com aspas apenas quando necessário (se vier maiúsculo)
    def q(c: str) -> str:
        # se tiver qualquer caractere fora do padrão lower_snake, quote
        # (principalmente colunas maiúsculas)
        if c.lower() != c:
            return f'"{c}"'
        return c

    sql = f"""
    select
        {q(col_ticker)} as ticker,
        {q(col_setor)} as setor,
        {q(col_subsetor)} as subsetor,
        {q(col_segmento)} as segmento,
        {q(col_nome)} as nome_empresa
    from {schema}.setores
    """

    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn)

    # Normalização leve
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    for c in ["setor", "subsetor", "segmento", "nome_empresa"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df
