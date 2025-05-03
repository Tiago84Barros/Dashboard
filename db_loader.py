"""db_loader.py
~~~~~~~~~~~~~~
Funções para baixar e carregar dados do banco de dados SQLite hospedado no GitHub.

Dependências:
- pandas
- requests
- sqlite3
- streamlit
- os
"""
import os
import requests
import sqlite3
import pandas as pd
import streamlit as st

# URL do banco de dados no GitHub
DB_URL = "https://raw.githubusercontent.com/Tiago84Barros/Dashboard/main/metadados.db"

@st.cache_data(ttl=3600)
def download_db_from_github(local_path: str = 'metadados.db') -> str | None:
    """
    Baixa o arquivo SQLite do GitHub e o salva localmente.
    Retorna o caminho para o arquivo local ou None em caso de falha.
    """
    try:
        response = requests.get(DB_URL, allow_redirects=True)
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(response.content)
            return local_path
        return None
    except requests.exceptions.RequestException:
        return None

@st.cache_data
def load_setores_from_db() -> pd.DataFrame | None:
    """
    Carrega a tabela 'setores' do banco de dados.
    Retorna DataFrame ou None se falhar.
    """
    db_path = download_db_from_github()
    if not db_path or not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM setores", conn)
        return df
    except Exception:
        return None
    finally:
        conn.close()

@st.cache_data
def load_data_from_db(ticker: str) -> pd.DataFrame | None:
    """
    Carrega a tabela 'Demonstracoes_Financeiras' para o ticker.
    """
    db_path = download_db_from_github()
    if not db_path or not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
        query = (
            f"SELECT * FROM Demonstracoes_Financeiras "
            f"WHERE Ticker = '{ticker}' OR Ticker = '{ticker.replace('.SA', '')}'"
        )
        df = pd.read_sql_query(query, conn)
        return df
    except Exception:
        return None
    finally:
        conn.close()

@st.cache_data
def load_multiplos_from_db(ticker: str) -> pd.DataFrame | None:
    """
    Carrega todos os registros da tabela 'multiplos' para o ticker.
    """
    db_path = download_db_from_github()
    if not db_path or not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
        query = (
            f"SELECT * FROM multiplos "
            f"WHERE Ticker = '{ticker}' OR Ticker = '{ticker.replace('.SA', '')}' "
            f"ORDER BY Data ASC"
        )
        df = pd.read_sql_query(query, conn)
        return df
    except Exception:
        return None
    finally:
        conn.close()

@st.cache_data
def load_multiplos_limitado_from_db(ticker: str) -> pd.DataFrame | None:
    """
    Carrega o registro mais recente da tabela 'multiplos_TRI' para o ticker.
    """
    db_path = download_db_from_github()
    if not db_path or not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
        query = (
            f"SELECT * FROM multiplos_TRI "
            f"WHERE Ticker = '{ticker}' OR Ticker = '{ticker.replace('.SA', '')}' "
            f"ORDER BY Data DESC LIMIT 1"
        )
        df = pd.read_sql_query(query, conn)
        return df
    except Exception:
        return None
    finally:
        conn.close()

@st.cache_data
def load_macro_summary() -> pd.DataFrame | None:
    """
    Carrega a tabela 'info_economica' do banco de dados.
    """
    db_path = download_db_from_github()
    if not db_path or not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM info_economica ORDER BY Data ASC", conn)
        return df
    except Exception:
        return None
    finally:
        conn.close()
