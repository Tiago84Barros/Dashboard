"""db_loader.py
~~~~~~~~~~~~~~~~
Funções utilitárias para baixar e acessar o banco SQLite hospedado no GitHub.

Principais funções públicas
---------------------------
- download_db_from_github(db_url, local_path="metadados.db")
- load_setores_from_db()
- load_data_from_db(ticker)
- load_multiplos_from_db(ticker)
- load_multiplos_limitado_from_db(ticker)
- load_macro_summary()

As funções usam `st.cache_data` quando Streamlit está disponível; caso
contrário, entram em cache com `functools.lru_cache`.
"""

from __future__ import annotations

import os
import sqlite3
from functools import lru_cache
from typing import Optional

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Cache decorator (Streamlit se houver, senão lru_cache) ---------------------
# ---------------------------------------------------------------------------
try:
    import streamlit as st  # type: ignore
    cache_decorator = st.cache_data  # pragma: no cover
except ModuleNotFoundError:
    def cache_decorator(func=None, *, ttl=None):  # type: ignore
        if func is None:
            return lambda f: lru_cache(maxsize=None)(f)
        return lru_cache(maxsize=None)(func)

# ---------------------------------------------------------------------------
# Configurações --------------------------------------------------------------
# ---------------------------------------------------------------------------
DB_URL: str = "https://raw.githubusercontent.com/Tiago84Barros/Dashboard/main/metadados.db"
LOCAL_DB_PATH: str = "metadados.db"

# ---------------------------------------------------------------------------
# 1. Download do banco -------------------------------------------------------
# ---------------------------------------------------------------------------

@cache_decorator(ttl=3600)
def download_db_from_github(db_url: str = DB_URL, local_path: str = LOCAL_DB_PATH) -> Optional[str]:
    try:
        resp = requests.get(db_url, allow_redirects=True, timeout=30)
        if resp.status_code == 200:
            with open(local_path, "wb") as fh:
                fh.write(resp.content)
            return local_path
        return None
    except requests.exceptions.RequestException as exc:
        print(f"Erro ao baixar DB: {exc}")
        return None

# ---------------------------------------------------------------------------
# 2. Tabela setores ----------------------------------------------------------
# ---------------------------------------------------------------------------

@cache_decorator
def load_setores_from_db() -> Optional[pd.DataFrame]:
    db_path = download_db_from_github()
    if db_path is None or not os.path.exists(db_path):
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            return pd.read_sql_query("SELECT * FROM setores", conn)
    except Exception as exc:
        print(f"Erro load_setores_from_db: {exc}")
        return None

# ---------------------------------------------------------------------------
# 3. Demonstrações financeiras ----------------------------------------------
# ---------------------------------------------------------------------------

@cache_decorator
def load_data_from_db(ticker: str) -> Optional[pd.DataFrame]:
    db_path = download_db_from_github()
    if db_path is None or not os.path.exists(db_path):
        return None
    tk_clean = ticker.replace(".SA", "")
    try:
        with sqlite3.connect(db_path) as conn:
            query = (
                "SELECT * FROM Demonstracoes_Financeiras "
                f"WHERE Ticker = '{ticker}' OR Ticker = '{tk_clean}'"
            )
            return pd.read_sql_query(query, conn)
    except Exception as exc:
        print(f"Erro load_data_from_db: {exc}")
        return None

# ---------------------------------------------------------------------------
# 4. Múltiplos completos -----------------------------------------------------
# ---------------------------------------------------------------------------

@cache_decorator
def load_multiplos_from_db(ticker: str) -> Optional[pd.DataFrame]:
    db_path = download_db_from_github()
    if db_path is None or not os.path.exists(db_path):
        return None
    tk_clean = ticker.replace(".SA", "")
    try:
        with sqlite3.connect(db_path) as conn:
            query = (
                "SELECT * FROM multiplos "
                f"WHERE Ticker = '{ticker}' OR Ticker = '{tk_clean}' "
                "ORDER BY Data ASC"
            )
            return pd.read_sql_query(query, conn)
    except Exception as exc:
        print(f"Erro load_multiplos_from_db: {exc}")
        return None

# ---------------------------------------------------------------------------
# 5. Múltiplos TRI (último registro) ----------------------------------------
# ---------------------------------------------------------------------------

@cache_decorator
def load_multiplos_limitado_from_db(ticker: str) -> Optional[pd.DataFrame]:
    """Retorna o registro mais recente da tabela *multiplos_TRI* para o ticker."""
    db_path = download_db_from_github()
    if db_path is None or not os.path.exists(db_path):
        return None
    tk_clean = ticker.replace(".SA", "")
    try:
        with sqlite3.connect(db_path) as conn:
            query = (
                "SELECT * FROM multiplos_TRI "
                f"WHERE Ticker = '{ticker}' OR Ticker = '{tk_clean}' "
                "ORDER BY Data DESC LIMIT 1"
            )
            return pd.read_sql_query(query, conn)
    except Exception as exc:
        print(f"Erro load_multiplos_limitado_from_db: {exc}")
        return None

# ---------------------------------------------------------------------------
# 6. Macro resumo ------------------------------------------------------------
# ---------------------------------------------------------------------------

@cache_decorator
def load_macro_summary() -> Optional[pd.DataFrame]:
    db_path = download_db_from_github()
    if db_path is None or not os.path.exists(db_path):
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            return pd.read_sql_query("SELECT * FROM info_economica ORDER BY Data ASC", conn)
    except Exception as exc:
        print(f"Erro load_macro_summary: {exc}")
        return None

# ---------------------------------------------------------------------------
__all__ = [
    "download_db_from_github",
    "load_setores_from_db",
    "load_data_from_db",
    "load_multiplos_from_db",
    "load_multiplos_limitado_from_db",
    "load_macro_summary",
]
