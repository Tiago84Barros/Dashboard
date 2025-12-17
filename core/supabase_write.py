import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

def get_conn():
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("Defina a env var SUPABASE_DB_URL com a connection string do Supabase.")
    return psycopg2.connect(db_url)

def _infer_pg_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_integer_dtype(series):
        return "numeric"
    if pd.api.types.is_float_dtype(series):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "timestamptz"
    # datas como string yyyy-mm-dd podem ficar como date se você converter antes
    return "text"

def ensure_columns(conn, table: str, df: pd.DataFrame, schema: str = "public"):
    # Busca colunas atuais
    with conn.cursor() as cur:
        cur.execute("""
            select column_name
            from information_schema.columns
            where table_schema = %s and table_name = %s
        """, (schema, table))
        existing = {r[0] for r in cur.fetchall()}

        # Adiciona colunas ausentes
        for col in df.columns:
            if col in existing:
                continue
            pg_type = _infer_pg_type(df[col])
            # quote duplo para respeitar colunas com maiúsculas / caracteres especiais
            cur.execute(f'alter table "{schema}"."{table}" add column "{col}" {pg_type};')
        conn.commit()

def upsert_df(conn, table: str, df: pd.DataFrame, conflict_cols: list[str], schema: str = "public"):
    if df is None or df.empty:
        return 0

    # Normaliza NaN -> None
    df = df.copy()
    df = df.where(pd.notnull(df), None)

    ensure_columns(conn, table, df, schema=schema)

    cols = list(df.columns)
    values = [tuple(row) for row in df.to_numpy()]

    quoted_cols = ",".join([f'"{c}"' for c in cols])
    conflict = ",".join([f'"{c}"' for c in conflict_cols])

    # Atualiza todos os campos exceto os de conflito
    update_cols = [c for c in cols if c not in set(conflict_cols)]
    set_clause = ",".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols]) if update_cols else ""

    sql = f"""
        insert into "{schema}"."{table}" ({quoted_cols})
        values %s
        on conflict ({conflict})
        do update set {set_clause};
    """.strip()

    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=1000)
    conn.commit()
    return len(df)
