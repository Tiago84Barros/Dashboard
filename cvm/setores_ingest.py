# cvm/setores_ingest.py
from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional

import pandas as pd
import sqlite3
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.config.settings import get_settings


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


def _load_from_metadados(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        raise FileNotFoundError(f"metadados.db não encontrado em {db_path}")

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql(
            """
            select
                upper(trim(ticker)) as ticker,
                setor as "SETOR",
                subsetor as "SUBSETOR",
                segmento as "SEGMENTO",
                nome_empresa
            from setores
            where ticker is not null
            """,
            conn,
        )

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

    settings = get_settings()
    metadados_path = Path(settings.metadados_db_path)

    if progress_cb:
        progress_cb("SETORES: carregando dados de metadados.db...")

    df = _load_from_metadados(metadados_path)

    if progress_cb:
        progress_cb(f"SETORES: {len(df):,} registros encontrados.".replace(",", "."))

    _upsert(engine, df)

    if progress_cb:
        progress_cb("SETORES: concluído.")
