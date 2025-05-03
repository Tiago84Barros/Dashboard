"""db_loader.py
~~~~~~~~~~~~~~
Funções para carregar dados do banco de dados SQLite local (metadados.db) incluso no repositório.

Dependências:
- pandas
- sqlite3
- streamlit
- os
"""
import os
import sqlite3
import pandas as pd
import streamlit as st

# Caminho para o arquivo local de banco de dados (incluso no repositório)
DB_LOCAL = os.path.join(os.path.dirname(__file__), 'metadados.db')


def _get_db_path() -> str | None:
    """
    Retorna o caminho para o banco de dados local se existir.
    Caso contrário, exibe erro e retorna None.
    """
    if os.path.exists(DB_LOCAL):
        return DB_LOCAL
    st.error(f"Arquivo de banco de dados não encontrado: {DB_LOCAL}")
    return None


@st.cache_data
def load_setores_from_db() -> pd.DataFrame | None:
    """
    Carrega a tabela 'setores' do banco de dados local.
    """
    path = _get_db_path()
    if path is None:
        return None
    try:
        conn = sqlite3.connect(path)
        df = pd.read_sql_query("SELECT * FROM setores", conn)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar tabela 'setores': {e}")
        return None
    finally:
        conn.close()


@st.cache_data
def load_data_from_db(ticker: str) -> pd.DataFrame | None:
    """
    Carrega a tabela 'Demonstracoes_Financeiras' para o ticker.
    """
    path = _get_db_path()
    if path is None:
        return None
    try:
        conn = sqlite3.connect(path)
        query = (
            f"SELECT * FROM Demonstracoes_Financeiras "
            f"WHERE Ticker = '{ticker}' OR Ticker = '{ticker.replace('.SA', '')}'"
        )
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar demonstrações financeiras: {e}")
        return None
    finally:
        conn.close()


@st.cache_data
def load_multiplos_from_db(ticker: str) -> pd.DataFrame | None:
    """
    Carrega todos os registros da tabela 'multiplos' para o ticker.
    """
    path = _get_db_path()
    if path is None:
        return None
    try:
        conn = sqlite3.connect(path)
        query = (
            f"SELECT * FROM multiplos "
            f"WHERE Ticker = '{ticker}' OR Ticker = '{ticker.replace('.SA', '')}' "
            f"ORDER BY Data ASC"
        )
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos históricos: {e}")
        return None
    finally:
        conn.close()


@st.cache_data
def load_multiplos_limitado_from_db(ticker: str) -> pd.DataFrame | None:
    """
    Carrega o registro mais recente da tabela 'multiplos_TRI' para o ticker.
    """
    path = _get_db_path()
    if path is None:
        return None
    try:
        conn = sqlite3.connect(path)
        query = (
            f"SELECT * FROM multiplos_TRI "
            f"WHERE Ticker = '{ticker}' OR Ticker = '{ticker.replace('.SA', '')}' "
            f"ORDER BY Data DESC LIMIT 1"
        )
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos TRI: {e}")
        return None
    finally:
        conn.close()


@st.cache_data
def load_macro_summary() -> pd.DataFrame | None:
    """
    Carrega a tabela 'info_economica' do banco de dados.
    """
    path = _get_db_path()
    if path is None:
        return None
    try:
        conn = sqlite3.connect(path)
        df = pd.read_sql_query("SELECT * FROM info_economica ORDER BY Data ASC", conn)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados macroeconômicos: {e}")
        return None
    finally:
        conn.close()
