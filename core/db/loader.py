# core/db/loader.py
from __future__ import annotations

from pathlib import Path
import sqlite3
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.config.settings import get_settings


def _sqlite_conn() -> sqlite3.Connection:
    settings = get_settings()
    db_path = Path(settings.sqlite_path)
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite local não encontrado: {db_path}")
    return sqlite3.connect(str(db_path))


def load_setores(engine: Engine | None = None) -> pd.DataFrame:
    """
    Carrega tabela de setores/segmentos.
    Ajuste o SELECT conforme seu schema/tabela no Supabase.
    """
    if engine is not None:
        q = text("select * from cvm.setores")
        return pd.read_sql(q, engine)

    with _sqlite_conn() as conn:
        return pd.read_sql_query("SELECT * FROM setores", conn)


def load_demonstracoes_financeiras(ticker: str, engine: Engine | None = None) -> pd.DataFrame:
    tk1 = (ticker or "").upper().strip()
    tk2 = tk1.replace(".SA", "")

    if engine is not None:
        q = text("""
            select *
            from cvm.demonstracoes_financeiras
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


def load_multiplos(ticker: str, engine: Engine | None = None) -> pd.DataFrame:
    tk1 = (ticker or "").upper().strip()
    tk2 = tk1.replace(".SA", "")

    if engine is not None:
        q = text("""
            select *
            from cvm.multiplos
            where ticker = :tk1 or ticker = :tk2
            order by data asc
        """)
        return pd.read_sql(q, engine, params={"tk1": tk1, "tk2": tk2})

    with _sqlite_conn() as conn:
        q = f"""
            SELECT * FROM multiplos
            WHERE Ticker = '{tk1}' OR Ticker = '{tk2}'
            ORDER BY Data ASC
        """
        return pd.read_sql_query(q, conn)


def load_macro_summary(engine: Engine | None = None) -> pd.DataFrame:
    if engine is not None:
        q = text("select * from cvm.info_economica order by data asc")
        return pd.read_sql(q, engine)

    with _sqlite_conn() as conn:
        return pd.read_sql_query("SELECT * FROM info_economica ORDER BY Data ASC", conn)
