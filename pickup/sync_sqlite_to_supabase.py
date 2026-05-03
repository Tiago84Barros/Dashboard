"""
pickup/sync_sqlite_to_supabase.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sincroniza as tabelas Demonstracoes_Financeiras e multiplos do SQLite local
para o Supabase (PostgreSQL).

Motivo: o filtro de perfil de empresa em advanced.py consulta o Supabase para
contar anos de histórico de DRE. Tickers presentes no SQLite local mas ausentes
no Supabase ficam invisíveis mesmo com 15 anos de dados (ITUB3, BBAS3, BBDC3, etc.).

Execute:
    python pickup/sync_sqlite_to_supabase.py

Requer SUPABASE_DB_URL no ambiente.
"""
from __future__ import annotations

import os
import sqlite3
import math

import pandas as pd
from sqlalchemy import create_engine, text

SQLITE_PATH = os.getenv("SQLITE_METADADOS_PATH", "data/metadados.db")
CHUNK_SIZE = 500

# Mapeamento: (tabela_sqlite, tabela_supabase, chave_primaria)
TABLES = [
    {
        "sqlite_table": "Demonstracoes_Financeiras",
        "supabase_table": '"Demonstracoes_Financeiras"',
        "pk": ("Ticker", "Data"),
        "cols": [
            "Ticker", "Data", "Receita_Liquida", "EBIT", "Lucro_Liquido",
            "LPA", "Ativo_Total", "Ativo_Circulante", "Passivo_Circulante",
            "Passivo_Total", "Divida_Total", "Patrimonio_Liquido",
            "Dividendos", "Caixa_Liquido", "Divida_Liquida",
        ],
    },
    {
        "sqlite_table": "multiplos",
        "supabase_table": "multiplos",
        "pk": ("Ticker", "Data"),
        "cols": [
            "Ticker", "Data", "Liquidez_Corrente", "Endividamento_Total",
            "Alavancagem_Financeira", "Margem_Operacional", "Margem_Liquida",
            "ROE", "ROA", "ROIC", "DY", '"P/L"', '"P/VP"', "Payout",
        ],
    },
]


def _quote(col: str) -> str:
    """Garante que a coluna fique entre aspas duplas no SQL."""
    return col if col.startswith('"') else f'"{col}"'


def _load_sqlite(sqlite_path: str, table: str, cols: list[str]) -> pd.DataFrame:
    """Lê todas as colunas disponíveis da tabela SQLite."""
    with sqlite3.connect(sqlite_path) as conn:
        # Obtém colunas reais da tabela
        real_cols = pd.read_sql_query(f'PRAGMA table_info("{table}")', conn)["name"].tolist()
        # Filtra para colunas desejadas (sem aspas) que existem
        wanted = [c.strip('"') for c in cols]
        present = [c for c in wanted if c in real_cols]
        sel = ", ".join(f'"{c}"' for c in present)
        df = pd.read_sql_query(f'SELECT {sel} FROM "{table}"', conn)
    return df


def _build_upsert_sql(supabase_table: str, pk: tuple[str, str], df: pd.DataFrame) -> str:
    """Constrói um SQL de INSERT ... ON CONFLICT DO UPDATE para o Supabase."""
    col_names = [f'"{c}"' for c in df.columns]
    col_placeholders = ", ".join(f":{c}" for c in df.columns)
    pk_cols = ", ".join(f'"{k}"' for k in pk)
    updates = ", ".join(
        f'"{c}" = EXCLUDED."{c}"'
        for c in df.columns
        if c not in pk
    )
    return f"""
    INSERT INTO public.{supabase_table} ({", ".join(col_names)})
    VALUES ({col_placeholders})
    ON CONFLICT ({pk_cols}) DO UPDATE SET {updates}
    """


def _upsert_table(engine, cfg: dict) -> int:
    sqlite_table = cfg["sqlite_table"]
    supabase_table = cfg["supabase_table"]
    pk = cfg["pk"]
    cols = cfg["cols"]

    print(f"\n[{sqlite_table}] Lendo do SQLite...")
    df = _load_sqlite(SQLITE_PATH, sqlite_table, cols)
    print(f"[{sqlite_table}] {len(df)} linhas × {len(df.columns)} colunas")

    if df.empty:
        print(f"[{sqlite_table}] AVISO: tabela vazia, pulando.")
        return 0

    # Converte Data para string ISO para o Postgres
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["Data"])

    # Substitui NaN/inf por None (NULL no Postgres)
    df = df.where(pd.notnull(df), None)
    for col in df.select_dtypes(include="float").columns:
        df[col] = df[col].apply(lambda x: None if (x is not None and isinstance(x, float) and not math.isfinite(x)) else x)

    sql = _build_upsert_sql(supabase_table, pk, df)
    rows = df.to_dict(orient="records")
    total = 0

    print(f"[{sqlite_table}] Enviando {len(rows)} linhas em chunks de {CHUNK_SIZE}...")
    for i in range(0, len(rows), CHUNK_SIZE):
        batch = rows[i: i + CHUNK_SIZE]
        with engine.begin() as conn:
            conn.execute(text(sql), batch)
        total += len(batch)
        pct = round(total / len(rows) * 100)
        print(f"  [{sqlite_table}] {total}/{len(rows)} ({pct}%)")

    return total


def main():
    supabase_url = os.getenv("SUPABASE_DB_URL")
    if not supabase_url:
        print("[ERRO] SUPABASE_DB_URL não definida.")
        return

    if not os.path.exists(SQLITE_PATH):
        print(f"[ERRO] SQLite não encontrado: {SQLITE_PATH}")
        return

    engine = create_engine(supabase_url, pool_pre_ping=True)

    totals = {}
    for cfg in TABLES:
        try:
            n = _upsert_table(engine, cfg)
            totals[cfg["sqlite_table"]] = n
        except Exception as e:
            print(f"[ERRO] Falha ao sincronizar {cfg['sqlite_table']}: {e}")
            import traceback; traceback.print_exc()

    print("\n══ Resultado ══")
    for table, n in totals.items():
        print(f"  {table}: {n} linhas upserted no Supabase")
    print("Sincronização concluída.")


if __name__ == "__main__":
    main()
