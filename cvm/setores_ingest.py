from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ───────────────────────── Config ─────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parents[1]
METADADOS_DB_PATH = PROJECT_ROOT / "data" / "metadados.db"

DADOS_MERCADO_URL = "https://api.dadosdemercado.com.br/v1/companies"
DADOS_MERCADO_TOKEN = os.getenv("DADOS_MERCADO_TOKEN")  # opcional

SCHEMA = "cvm"
TABLE = "setores"
FULL_TABLE = f"{SCHEMA}.{TABLE}"


# ───────────────────────── DDL ─────────────────────────

def _ensure_schema_and_table(engine: Engine) -> None:
    ddl_schema = f"create schema if not exists {SCHEMA};"

    ddl_table = f"""
    create table if not exists {FULL_TABLE} (
        ticker text primary key,
        setor text,
        subsetor text,
        segmento text,
        nome_empresa text,
        source text,
        fetched_at timestamptz default now()
    );
    """

    with engine.begin() as conn:
        conn.execute(text(ddl_schema))
        conn.execute(text(ddl_table))


def _count_remote(engine: Engine) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text(f"select count(*) from {FULL_TABLE}")).scalar() or 0)


# ───────────────────────── Fonte 1: API ─────────────────────────

def _load_from_api() -> pd.DataFrame:
    headers = {}
    if DADOS_MERCADO_TOKEN:
        headers["Authorization"] = f"Bearer {DADOS_MERCADO_TOKEN}"

    resp = requests.get(DADOS_MERCADO_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    df = pd.DataFrame(data)

    if df.empty:
        return df

    # Normaliza nomes
    df = df.rename(
        columns={
            "ticker": "ticker",
            "b3_sector": "setor",
            "b3_subsector": "subsetor",
            "b3_segment": "segmento",
            "company_name": "nome_empresa",
        }
    )

    if "ticker" not in df.columns:
        return pd.DataFrame()

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["source"] = "dadosdemercado"

    cols = ["ticker", "setor", "subsetor", "segmento", "nome_empresa", "source"]
    for c in cols:
        if c not in df.columns:
            df[c] = None

    df = df[cols].dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"])
    return df


# ───────────────────────── Fonte 2: SQLite (fallback) ─────────────────────────

def _load_from_sqlite() -> pd.DataFrame:
    if not METADADOS_DB_PATH.exists():
        return pd.DataFrame()

    conn = sqlite3.connect(METADADOS_DB_PATH)
    try:
        df = pd.read_sql(
            """
            SELECT
                UPPER(TRIM(ticker)) AS ticker,
                SETOR  AS setor,
                SUBSETOR AS subsetor,
                SEGMENTO AS segmento,
                nome_empresa
            FROM setores
            WHERE ticker IS NOT NULL
            """,
            conn,
        )
    finally:
        conn.close()

    if df.empty:
        return df

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["source"] = "metadados_sqlite"
    df = df.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"])

    # garante colunas
    for c in ["setor", "subsetor", "segmento", "nome_empresa", "source"]:
        if c not in df.columns:
            df[c] = None

    return df[["ticker", "setor", "subsetor", "segmento", "nome_empresa", "source"]]


# ───────────────────────── Upsert ─────────────────────────

def _upsert(engine: Engine, df: pd.DataFrame, batch: int = 2000) -> None:
    if df.empty:
        return

    sql = f"""
    insert into {FULL_TABLE}
        (ticker, setor, subsetor, segmento, nome_empresa, source, fetched_at)
    values
        (:ticker, :setor, :subsetor, :segmento, :nome_empresa, :source, now())
    on conflict (ticker) do update set
        setor = excluded.setor,
        subsetor = excluded.subsetor,
        segmento = excluded.segmento,
        nome_empresa = excluded.nome_empresa,
        source = excluded.source,
        fetched_at = now();
    """

    rows = df.to_dict("records")
    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(sql), rows[i : i + batch])


# ───────────────────────── Entry point ─────────────────────────

def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    _ensure_schema_and_table(engine)

    before = _count_remote(engine)

    if progress_cb:
        progress_cb("SETORES: carregando via API Dados de Mercado...")

    df = pd.DataFrame()
    try:
        df = _load_from_api()
    except Exception as e:
        if progress_cb:
            progress_cb(f"SETORES: falha na API ({e}). Tentando fallback local...")

    if df.empty:
        if progress_cb:
            progress_cb("SETORES: carregando via metadados.db (fallback)...")
        df = _load_from_sqlite()

    if df.empty:
        raise RuntimeError("SETORES: nenhuma fonte retornou dados (API e SQLite vazios).")

    if progress_cb:
        progress_cb(f"SETORES: upsert de {len(df)} registros em {FULL_TABLE}...")

    _upsert(engine, df)

    after = _count_remote(engine)
    if after == 0:
        raise RuntimeError(
            f"SETORES: ingestão executada, mas {FULL_TABLE} permanece vazia. "
            "Verifique URL/engine do Supabase, permissões do schema cvm e RLS."
        )

    if progress_cb:
        progress_cb(f"SETORES: concluído — total {after} (delta +{after - before}).")
