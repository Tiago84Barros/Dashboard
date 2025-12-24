from __future__ import annotations

from typing import Any, Dict

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# =========================================================
# SETORES
# =========================================================
def load_setores(engine: Engine) -> pd.DataFrame:
    """
    Carrega tabela cvm.setores.
    """
    sql = """
        SELECT
            ticker,
            "SETOR",
            "SUBSETOR",
            "SEGMENTO",
            nome_empresa
        FROM cvm.setores
        ORDER BY "SETOR", ticker
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


# =========================================================
# DFP – DEMONSTRAÇÕES ANUAIS
# =========================================================
def load_demonstracoes_financeiras(engine: Engine, ticker: str) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM cvm.demonstracoes_financeiras
        WHERE ticker = :ticker
        ORDER BY data
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"ticker": ticker})


# =========================================================
# ITR – DEMONSTRAÇÕES TRIMESTRAIS
# =========================================================
def load_demonstracoes_financeiras_tri(engine: Engine, ticker: str) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM cvm.demonstracoes_financeiras_tri
        WHERE ticker = :ticker
        ORDER BY data
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"ticker": ticker})


# =========================================================
# MÚLTIPLOS ANUAIS
# =========================================================
def load_multiplos(engine: Engine, ticker: str) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM cvm.multiplos
        WHERE ticker = :ticker
        ORDER BY data
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"ticker": ticker})


# =========================================================
# MÚLTIPLOS TRIMESTRAIS
# =========================================================
def load_multiplos_tri(engine: Engine, ticker: str) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM cvm.multiplos_tri
        WHERE ticker = :ticker
        ORDER BY data
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"ticker": ticker})


# =========================================================
# FINANCIAL METRICS (DERIVADOS)
# =========================================================
def load_financial_metrics(engine: Engine, ticker: str) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM cvm.financial_metrics
        WHERE ticker = :ticker
        ORDER BY data
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"ticker": ticker})


# =========================================================
# FUNDAMENTAL SCORE
# =========================================================
def load_fundamental_score(engine: Engine, ticker: str) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM cvm.fundamental_score
        WHERE ticker = :ticker
        ORDER BY data
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"ticker": ticker})


# =========================================================
# INFO ECONÔMICA (MACRO) – TABELA
# =========================================================
def load_info_economica(engine: Engine) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM cvm.info_economica
        ORDER BY data
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


# =========================================================
# MACRO SUMMARY (USADO NO AVANÇADO)
# =========================================================
def load_macro_summary(engine: Engine) -> Dict[str, Any]:
    """
    Retorna um resumo (dict) do último registro de cvm.info_economica.
    Essa função existe porque a página Avançada importa ela.

    - Se a tabela não existir ou estiver vazia, retorna dict vazio.
    - Não assume colunas específicas: devolve todas as colunas do último registro.
    """
    sql = """
        SELECT *
        FROM cvm.info_economica
        ORDER BY data DESC
        LIMIT 1
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        if df.empty:
            return {}
        # Converte a linha final em dict simples
        return df.iloc[0].to_dict()
    except Exception:
        return {}


# =========================================================
# SYNC STATUS (CONFIGURAÇÕES)
# =========================================================
def load_sync_log(engine: Engine) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM cvm.sync_log
        ORDER BY run_at DESC
        LIMIT 1
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)
