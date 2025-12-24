# core/db/loader.py
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.config.settings import get_settings


def _sqlite_conn() -> sqlite3.Connection:
    s = get_settings()
    p = Path(s.sqlite_path)
    if not p.exists():
        raise FileNotFoundError(f"SQLite local não encontrado: {p}")
    return sqlite3.connect(str(p))


# ------------------------------------------------------------
# Setores (Supabase: public.setores)
# ------------------------------------------------------------
def load_setores(engine: Engine | None = None) -> pd.DataFrame:
    if engine is not None:
        q = text('select ticker, "SETOR", "SUBSETOR", "SEGMENTO", nome_empresa from public.setores')
        return pd.read_sql(q, engine)

    with _sqlite_conn() as conn:
        return pd.read_sql_query("SELECT * FROM setores", conn)


def load_setores_from_db(engine: Engine | None = None) -> pd.DataFrame:
    return load_setores(engine=engine)


# ------------------------------------------------------------
# DFP (tabela alvo: cvm.demonstracoes_financeiras_dfp)
# ------------------------------------------------------------
def load_data_from_db(ticker: str, engine: Engine | None = None) -> pd.DataFrame:
    tk1 = (ticker or "").upper().strip()
    tk2 = tk1.replace(".SA", "")

    if engine is not None:
        q = text("""
            select *
            from cvm.demonstracoes_financeiras_dfp
            where ticker = :tk1 or ticker = :tk2
            order by data asc
        """)
        return pd.read_sql(q, engine, params={"tk1": tk1, "tk2": tk2})

    with _sqlite_conn() as conn:
        q = f"""
            SELECT * FROM Demonstracoes_Financeiras
            WHERE Ticker = '{tk1}' OR Ticker = '{tk2}'
            ORDER BY Data ASC
        """
        return pd.read_sql_query(q, conn)


# ------------------------------------------------------------
# TRI (ITR consolidado)
# ------------------------------------------------------------
def load_tri_from_db(ticker: str, engine: Engine | None = None) -> pd.DataFrame:
    tk1 = (ticker or "").upper().strip()
    tk2 = tk1.replace(".SA", "")

    if engine is not None:
        q = text("""
            select *
            from cvm.demonstracoes_financeiras_tri
            where ticker = :tk1 or ticker = :tk2
            order by data asc
        """)
        return pd.read_sql(q, engine, params={"tk1": tk1, "tk2": tk2})

    return pd.DataFrame()


# ------------------------------------------------------------
# Múltiplos: tenta cvm.multiplos; se não existir, tenta cvm.financial_metrics
# (seu app usa “multiplos” para os cards e históricos)
# ------------------------------------------------------------
def load_multiplos_from_db(ticker: str, engine: Engine | None = None) -> pd.DataFrame:
    tk1 = (ticker or "").upper().strip()
    tk2 = tk1.replace(".SA", "")

    if engine is None:
        with _sqlite_conn() as conn:
            q = f"""
                SELECT * FROM multiplos
                WHERE Ticker = '{tk1}' OR Ticker = '{tk2}'
                ORDER BY Data ASC
            """
            return pd.read_sql_query(q, conn)

    # Supabase
    for table in ["cvm.multiplos", "cvm.financial_metrics", 'cvm."Financial_Metrics"']:
        try:
            q = text(f"""
                select *
                from {table}
                where ticker = :tk1 or ticker = :tk2
                order by data asc
            """)
            return pd.read_sql(q, engine, params={"tk1": tk1, "tk2": tk2})
        except Exception:
            continue

    return pd.DataFrame()


def load_multiplos_limitado_from_db(ticker: str, limite: int = 12, engine: Engine | None = None) -> pd.DataFrame:
    df = load_multiplos_from_db(ticker, engine=engine)
    if df is None or df.empty:
        return pd.DataFrame()
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
        df = df.dropna(subset=["Data"]).sort_values("Data")
        return df.tail(int(limite))
    return df.tail(int(limite))


# ------------------------------------------------------------
# Macro (macro_bcb_ingest.py escreve em cvm.info_economica)
# ------------------------------------------------------------
def load_macro_summary(engine: Engine | None = None) -> pd.DataFrame:
    if engine is not None:
        q = text("select * from cvm.info_economica order by data asc")
        return pd.read_sql(q, engine)

    with _sqlite_conn() as conn:
        return pd.read_sql_query("SELECT * FROM info_economica ORDER BY Data ASC", conn)
