from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# =========================================================
# SETORES
# =========================================================
def load_setores(engine: Engine) -> pd.DataFrame:
    """
    Carrega tabela cvm.setores.
    Conexão é aberta e fechada corretamente.
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
# INFO ECONÔMICA (MACRO)
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
