"""db_loader.py
~~~~~~~~~~~~~~~~
Funções utilitárias para baixar e acessar o banco SQLite hospedado no GitHub.

Principais funções públicas
---------------------------
- download_db_from_github(db_url, local_path="metadados.db")
- load_setores_from_db()
- load_data_from_db(ticker)
- load_multiplos_from_db(ticker)
- load_macro_summary()

Caso **Streamlit** esteja instalado, as funções são _cacheadas_ com
`st.cache_data`. Fora do Streamlit o módulo continua funcional (decorador
`noop_cache`).
"""

from __future__ import annotations

import os
import sqlite3
from functools import lru_cache
from typing import Optional

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Opção de cache: usa st.cache_data se Streamlit estiver disponível ----------
# ---------------------------------------------------------------------------
try:
    import streamlit as st  # type: ignore
    cache_decorator = st.cache_data  # pragma: no cover
except ModuleNotFoundError:  # fallback CLI ou notebook
    def cache_decorator(func=None, *, ttl=None):  # type: ignore
        if func is None:
            return lambda f: lru_cache(maxsize=None)(f)  # com parâmetros
        return lru_cache(maxsize=None)(func)

# ---------------------------------------------------------------------------
# Configurações gerais -------------------------------------------------------
# ---------------------------------------------------------------------------

DB_URL: str = (
    "https://raw.githubusercontent.com/Tiago84Barros/Dashboard/main/metadados.db"
)
LOCAL_DB_PATH: str = "metadados.db"

# ---------------------------------------------------------------------------
# 1. Download do banco de dados ---------------------------------------------
# ---------------------------------------------------------------------------

@cache_decorator(ttl=3600)
def download_db_from_github(
    db_url: str = DB_URL, local_path: str = LOCAL_DB_PATH
) -> Optional[str]:
    """Baixa o arquivo SQLite do GitHub e devolve o *path* local."""
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
# 2. Carrega tabela *setores* -------------------------------------------------
# ---------------------------------------------------------------------------

@cache_decorator
def load_setores_from_db() -> Optional[pd.DataFrame]:
    db_path = download_db_from_github()
    if db_path is None or not os.path.exists(db_path):
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            query = "SELECT * FROM setores"
            return pd.read_sql_query(query, conn)
    except Exception as exc:
        print(f"Erro load_setores_from_db: {exc}")
        return None

# ---------------------------------------------------------------------------
# 3. Carrega demonstrações financeiras ---------------------------------------
# ---------------------------------------------------------------------------

@cache_decorator
def load_data_from_db(ticker: str) -> Optional[pd.DataFrame]:
    db_path = download_db_from_github()
    if db_path is None or not os.path.exists(db_path):
        return None
    ticker_clean = ticker.replace(".SA", "")
    try:
        with sqlite3.connect(db_path) as conn:
            query = (
                "SELECT * FROM Demonstracoes_Financeiras "
                f"WHERE Ticker = '{ticker}' OR Ticker = '{ticker_clean}'"
            )
            return pd.read_sql_query(query, conn)
    except Exception as exc:
        print(f"Erro load_data_from_db: {exc}")
        return None

# ---------------------------------------------------------------------------
# 4. Carrega múltiplos --------------------------------------------------------
# ---------------------------------------------------------------------------

@cache_decorator
def load_multiplos_from_db(ticker: str) -> Optional[pd.DataFrame]:
    db_path = download_db_from_github()
    if db_path is None or not os.path.exists(db_path):
        return None
    ticker_clean = ticker.replace(".SA", "")
    try:
        with sqlite3.connect(db_path) as conn:
            query = (
                "SELECT * FROM multiplos "
                f"WHERE Ticker = '{ticker}' OR Ticker = '{ticker_clean}' "
                "ORDER BY Data ASC"
            )
            return pd.read_sql_query(query, conn)
    except Exception as exc:
        print(f"Erro load_multiplos_from_db: {exc}")
        return None

# ---------------------------------------------------------------------------
# 5. Dados macroeconômicos ----------------------------------------------------
# ---------------------------------------------------------------------------

@cache_decorator
def load_macro_summary() -> Optional[pd.DataFrame]:
    db_path = download_db_from_github()
    if db_path is None or not os.path.exists(db_path):
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            query = "SELECT * FROM info_economica ORDER BY Data ASC"
            return pd.read_sql_query(query, conn)
    except Exception as exc:
        print(f"Erro load_macro_summary: {exc}")
        return None

# ---------------------------------------------------------------------------
__all__ = [
    "download_db_from_github",
    "load_setores_from_db",
    "load_data_from_db",
    "load_multiplos_from_db",
    "load_macro_summary",
]
