"""db_loader.py
~~~~~~~~~~~~~~
Funções de acesso ao banco SQLite `metadados.db`, agora incluído no
repositório na pasta `data/`.

Mantém as mesmas assinaturas usadas no código monolítico, mas ajusta o
caminho do arquivo para refletir a nova hierarquia de pastas.

Dependências:
- pandas
- sqlite3
- streamlit
- pathlib / os
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

# ────────────────────────────────────────────────────────────────────────────────
# Localização do banco
# ────────────────────────────────────────────────────────────────────────────────
#   <repo_root> /
#       data /
#           metadados.db
#       core /
#           db_loader.py   ← este arquivo
#
ROOT_DIR = Path(__file__).resolve().parent.parent          # <repo_root>
DB_LOCAL = ROOT_DIR / "data" / "metadados.db"


def _get_db_path() -> str | None:
    """
    Retorna o caminho absoluto até o banco de dados local,
    exibindo mensagem de erro no Streamlit caso não exista.
    """
    if DB_LOCAL.exists():
        return str(DB_LOCAL)
    st.error(f"Arquivo de banco de dados não encontrado: {DB_LOCAL}")
    return None


# ════════════════════════════════════════════════════════════════════════════════
# Funções de carregamento – todas em cache para evitar I/O repetido
# ════════════════════════════════════════════════════════════════════════════════
@st.cache_data
def load_setores_from_db() -> pd.DataFrame | None:
    """Carrega a tabela **setores**."""
    path = _get_db_path()
    if path is None:
        return None
    try:
        with sqlite3.connect(path) as conn:
            return pd.read_sql_query("SELECT * FROM setores", conn)
    except Exception as e:
        st.error(f"Erro ao carregar tabela 'setores': {e}")
        return None

# Carrega demonstrações financeiras -----------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_data_from_db(ticker: str) -> pd.DataFrame | None:
    """
    Carrega a tabela **Demonstracoes_Financeiras** para o ticker informado.
    Aceita `PETR4` ou `PETR4.SA` (faz tratamento interno).
    """
    path = _get_db_path()
    if path is None:
        return None

    tk1 = ticker.upper()
    tk2 = tk1.replace(".SA", "")

    query = (
        "SELECT * FROM Demonstracoes_Financeiras "
        f"WHERE Ticker = '{tk1}' OR Ticker = '{tk2}' "
        "ORDER BY Data ASC"
    )

    try:
        with sqlite3.connect(path) as conn:
            return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Erro ao carregar DRE para {ticker}: {e}")
        return None

# Carrega múltiplos anuais -----------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_multiplos_from_db(ticker: str) -> pd.DataFrame | None:
    """Carrega a tabela **multiplos** completa para o ticker."""
    path = _get_db_path()
    if path is None:
        return None

    tk1 = ticker.upper()
    tk2 = tk1.replace(".SA", "")

    query = (
        "SELECT * FROM multiplos "
        f"WHERE Ticker = '{tk1}' OR Ticker = '{tk2}' "
        "ORDER BY Data ASC"
    )

    try:
        with sqlite3.connect(path) as conn:
            return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos para {ticker}: {e}")
        return None

# carrega múltiplos anuais, mas entregando somente informações até o limite desejado -------------------------------------------------------------
@st.cache_data
def load_multiplos_limitado_from_db(ticker: str, limite: int = 250) -> pd.DataFrame | None:
    """
    Carrega os últimos `limite` registros da tabela **multiplos**
    para exibir gráficos leves.
    """
    path = _get_db_path()
    if path is None:
        return None

    tk1 = ticker.upper()
    tk2 = tk1.replace(".SA", "")

    query = (
        "SELECT * FROM multiplos "
        f"WHERE Ticker = '{tk1}' OR Ticker = '{tk2}' "
        f"ORDER BY Data DESC LIMIT {limite}"
    )

    try:
        with sqlite3.connect(path) as conn:
            return pd.read_sql_query(query, conn).sort_values("Data")
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos limitados para {ticker}: {e}")
        return None

# Carrega múltiplos trimestrais (Cálculo para os últimos 12 meses) ---------------------------------------------------------------------------
@st.cache_data
def load_multiplos_tri_from_db(ticker: str) -> pd.DataFrame | None:
    """Carrega o registro mais recente de **multiplos_TRI** (dados trimestrais)."""
    path = _get_db_path()
    if path is None:
        return None

    tk1 = ticker.upper()
    tk2 = tk1.replace(".SA", "")

    query = (
        "SELECT * FROM multiplos_TRI "
        f"WHERE Ticker = '{tk1}' OR Ticker = '{tk2}' "
        "ORDER BY Data DESC LIMIT 1"
    )

    try:
        with sqlite3.connect(path) as conn:
            return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos TRI para {ticker}: {e}")
        return None

# Cálculo de dados marco para informações macroeconômicas ----------------------------------------------------------------------
@st.cache_data
def load_macro_summary() -> pd.DataFrame | None:
    """Carrega a tabela **info_economica** (dados macro)."""
    path = _get_db_path()
    if path is None:
        return None
    try:
        with sqlite3.connect(path) as conn:
            return pd.read_sql_query("SELECT * FROM info_economica ORDER BY Data ASC", conn)
    except Exception as e:
        st.error(f"Erro ao carregar dados macroeconômicos: {e}")
        return None
