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

    Compatibilidade de schema:
    - Preferência: colunas novas (setor/subsetor/segmento)
    - Fallback: colunas legadas com aspas ("SETOR"/"SUBSETOR"/"SEGMENTO")

    Para manter compatibilidade com as páginas atuais, o DataFrame retornado
    SEMPRE expõe as colunas em CAIXA ALTA: SETOR/SUBSETOR/SEGMENTO.
    """

    sql_new = """
        SELECT
            ticker,
            setor    AS "SETOR",
            subsetor AS "SUBSETOR",
            segmento AS "SEGMENTO",
            nome_empresa
        FROM cvm.setores
        ORDER BY setor NULLS LAST, ticker
    """

    sql_old = """
        SELECT
            ticker,
            "SETOR",
            "SUBSETOR",
            "SEGMENTO",
            nome_empresa
        FROM cvm.setores
        ORDER BY "SETOR" NULLS LAST, ticker
    """

    with engine.connect() as conn:
        try:
            df = pd.read_sql(text(sql_new), conn)
        except Exception:
            df = pd.read_sql(text(sql_old), conn)

    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()

    return df


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
# INFO ECONÔMICA (ANUAL) – TABELA
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
# INFO ECONÔMICA (MENSAL) - TABELA
# =========================================================

def load_macro_mensal(engine: Engine) -> pd.DataFrame:
    sql = """
        select *
        from cvm.info_economica_mensal
        order by data
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)



# =========================================================
# MACRO SUMMARY (USADO NO AVANÇADO)
# =========================================================
def load_macro_summary(engine: Engine) -> pd.DataFrame:
    """
    Retorna o histórico completo de cvm.info_economica como DataFrame.
    (O Advanced precisa de série temporal para o benchmark Selic.)
    """
    sql = """
        SELECT *
        FROM cvm.info_economica
        ORDER BY data
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn)
    except Exception:
        return pd.DataFrame()

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
