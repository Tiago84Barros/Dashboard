from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# Caminho fixo do banco versionado no repositório
METADADOS_DB_PATH = Path("data/metadados.db")


def _ensure_table(engine: Engine) -> None:
    ddl = """
    create table if not exists public.setores (
        ticker text primary key,
        "SETOR" text,
        "SUBSETOR" text,
        "SEGMENTO" text,
        nome_empresa text,
        created_at timestamptz not null default now()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _load_setores_from_metadados() -> pd.DataFrame:
    if not METADADOS_DB_PATH.exists():
        raise FileNotFoundError(
            f"Banco não encontrado em {METADADOS_DB_PATH.resolve()}"
        )

    conn = sqlite3.connect(METADADOS_DB_PATH)
    try:
        df = pd.read_sql(
            """
            SELECT
                UPPER(TRIM(ticker))      AS ticker,
                SETOR,
                SUBSETOR,
                SEGMENTO,
                nome_empresa
            FROM setores
            WHERE ticker IS NOT NULL
            """,
            conn,
        )
    finally:
        conn.close()

    df = df.dropna(subset=["ticker"])
    df = df.drop_duplicates(subset=["ticker"])

    return df


def _upsert(engine: Engine, df: pd.DataFrame, batch: int = 5000) -> None:
    if df.empty:
        return

    sql = """
    insert into public.setores (ticker, "SETOR", "SUBSETOR", "SEGMENTO", nome_empresa)
    values (:ticker, :SETOR, :SUBSETOR, :SEGMENTO, :nome_empresa)
    on conflict (ticker) do update set
      "SETOR" = excluded."SETOR",
      "SUBSETOR" = excluded."SUBSETOR",
      "SEGMENTO" = excluded."SEGMENTO",
      nome_empresa = excluded.nome_empresa;
    """

    rows = df.to_dict("records")
    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(sql), rows[i : i + batch])


def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    _ensure_table(engine)

    if progress_cb:
        progress_cb("SETORES: carregando dados do metadados.db...")

    df = _load_setores_from_metadados()

    if df.empty:
        raise RuntimeError("Tabela setores no metadados.db está vazia.")

    if progress_cb:
        progress_cb(f"SETORES: upsert de {len(df):,} registros...".replace(",", "."))

    _upsert(engine, df)

    if progress_cb:
        progress_cb("SETORES: concluído.")
