from __future__ import annotations

import os
import sqlite3
import requests
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ============================================================
# CONFIGURAÇÕES
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
METADADOS_DB_PATH = PROJECT_ROOT / "data" / "metadados.db"

DADOS_MERCADO_URL = "https://api.dadosdemercado.com.br/v1/companies"
DADOS_MERCADO_TOKEN = os.getenv("DADOS_MERCADO_TOKEN")  # opcional


# ============================================================
# UTILIDADES
# ============================================================

def _ensure_table(engine: Engine) -> None:
    ddl = """
    create table if not exists public.setores (
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
        conn.execute(text(ddl))


def _count_remote(engine: Engine) -> int:
    with engine.connect() as conn:
        return int(
            conn.execute(text("select count(*) from public.setores")).scalar() or 0
        )


# ============================================================
# FONTE 1 — API DADOS DE MERCADO
# ============================================================

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

    df = df.rename(
        columns={
            "ticker": "ticker",
            "b3_sector": "setor",
            "b3_subsector": "subsetor",
            "b3_segment": "segmento",
            "company_name": "nome_empresa",
        }
    )

    df["ticker"] = df["ticker"].str.upper().str.strip()
    df["source"] = "dadosdemercado"

    return df[
        ["ticker", "setor", "subsetor", "segmento", "nome_empresa", "source"]
    ].dropna(subset=["ticker"])


# ============================================================
# FONTE 2 — SQLITE (FALLBACK)
# ============================================================

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

    df["source"] = "metadados_sqlite"
    return df.drop_duplicates(subset=["ticker"])


# ============================================================
# UPSERT
# ============================================================

def _upsert(engine: Engine, df: pd.DataFrame, batch: int = 2000) -> None:
    sql = """
    insert into public.setores
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


# ============================================================
# ENTRYPOINT
# ============================================================

def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    _ensure_table(engine)

    before = _count_remote(engine)

    if progress_cb:
        progress_cb("SETORES: carregando via API Dados de Mercado...")

    try:
        df = _load_from_api()
    except Exception as e:
        if progress_cb:
            progress_cb(f"SETORES: falha na API ({e}), usando fallback local...")
        df = pd.DataFrame()

    if df.empty:
        df = _load_from_sqlite()

    if df.empty:
        raise RuntimeError(
            "SETORES: nenhuma fonte retornou dados (API e SQLite vazios)."
        )

    if progress_cb:
        progress_cb(f"SETORES: upsert de {len(df)} registros...")

    _upsert(engine, df)

    after = _count_remote(engine)

    if after == 0:
        raise RuntimeError(
            "SETORES: ingestão executada, mas Supabase permanece vazio. "
            "Verifique a URL/engine do Supabase."
        )

    if progress_cb:
        progress_cb(
            f"SETORES: concluído — total {after} registros "
            f"(delta +{after - before})."
        )
