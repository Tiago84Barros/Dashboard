"""db_loader.py
~~~~~~~~~~~~~~
Funções para baixar e carregar dados do banco de dados SQLite hospedado no GitHub,
com mensagens de depuração para status de download.

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

# URL do banco de dados no GitHub (verifique se está correto e branch ativo)
DB_URL = "https://raw.githubusercontent.com/Tiago84Barros/Dashboard/main/metadados.db"

@st.cache_data(ttl=3600)
def download_db_from_github(local_path: str = 'metadados.db') -> str | None:
    """
    Baixa o arquivo SQLite do GitHub e o salva localmente.
    Retorna o caminho para o arquivo local ou None em caso de falha.
    Exibe mensagens de status para depuração.
    """
    try:
        response = requests.get(DB_URL, allow_redirects=True)
        if response.status_code != 200:
            st.error(f"Erro ao baixar banco de dados: status {response.status_code}\nURL: {DB_URL}")
            return None
        with open(local_path, 'wb') as f:
            f.write(response.content)
        st.success(f"Banco de dados baixado com sucesso: {local_path}")
        return local_path
    except Exception as e:
        st.error(f"Erro ao tentar conectar ao GitHub: {e}")
        return None

@st.cache_data
def load_setores_from_db() -> pd.DataFrame | None:
    """
    Carrega a tabela 'setores' do banco de dados.
    Retorna DataFrame ou None se falhar, mostrando erros de caminho e existência.
    """
    db_path = download_db_from_github()
    if not db_path:
        st.error(f"download_db_from_github retornou None. Verifique a URL: {DB_URL}")
        return None
    if not os.path.exists(db_path):
        st.error(f"Arquivo não encontrado: {db_path}")
        return None
    try:
        conn = sqlite3.connect(db_path)
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
    db_path = download_db_from_github()
    if not db_path or not os.path.exists(db_path):
        st.error("load_data_from_db: DB não disponível")
        return None
    try:
        conn = sqlite3.connect(db_path)
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
    db_path = download_db_from_github()
    if not db_path or not os.path.exists(db_path):
        st.error("load_multiplos_from_db: DB não disponível")
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
    db_path = download_db_from_github()
    if not db_path or not os.path.exists(db_path):
        st.error("load_multiplos_limitado_from_db: DB não disponível")
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
    db_path = download_db_from_github()
    if not db_path or not os.path.exists(db_path):
        st.error("load_macro_summary: DB não disponível")
        return None
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM info_economica ORDER BY Data ASC", conn)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados macroeconômicos: {e}")
        return None
    finally:
        conn.close()
