from __future__ import annotations

import os
import logging
from functools import lru_cache
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Engine / Conexão
# ─────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def get_engine():
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL não configurada.")
    return create_engine(db_url, pool_pre_ping=True)


def _read_sql_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params)


# ─────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────

def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.astype(str).str.strip().str.replace("\ufeff", "", regex=False)
    return out


def _coerce_sort_by_data(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "Data" not in df.columns:
        return df
    df = df.copy()
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df = df.dropna(subset=["Data"]).sort_values("Data").reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────
# Loaders básicos (já existentes, mantidos)
# ─────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_setores_from_db() -> pd.DataFrame:
    sql = """
        SELECT
            ticker,
            nome_empresa,
            "SETOR",
            "SUBSETOR",
            "SEGMENTO",
            "LISTAGEM"
        FROM public.setores
        WHERE ticker IS NOT NULL
    """
    return _clean_columns(_read_sql_df(sql))


@st.cache_data(show_spinner=False)
def load_data_from_db(ticker: str) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM public."Demonstracoes_Financeiras"
        WHERE ticker = :ticker
        ORDER BY "Data"
    """
    return _coerce_sort_by_data(_clean_columns(_read_sql_df(sql, {"ticker": ticker})))


@st.cache_data(show_spinner=False)
def load_multiplos_from_db(ticker: str) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM public.multiplos
        WHERE ticker = :ticker
        ORDER BY "Data"
    """
    return _coerce_sort_by_data(_clean_columns(_read_sql_df(sql, {"ticker": ticker})))


@st.cache_data(show_spinner=False)
def load_macro_summary() -> pd.DataFrame:
    sql = """
        SELECT *
        FROM public.info_economica_mensal
        ORDER BY "Data"
    """
    return _coerce_sort_by_data(_clean_columns(_read_sql_df(sql)))


# ─────────────────────────────────────────────────────────────
# 🔥 NOVOS LOADERS DE ALTO NÍVEL (PATCH)
# ─────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_empresa_completa(ticker: str) -> Dict[str, pd.DataFrame]:
    """
    Retorna pacote completo da empresa:
    - dre
    - multiplos
    - anos_hist (int)
    """
    dre = load_data_from_db(ticker)
    multiplos = load_multiplos_from_db(ticker)

    anos_hist = 0
    if dre is not None and not dre.empty and "Data" in dre.columns:
        anos_hist = pd.to_datetime(dre["Data"], errors="coerce").dt.year.nunique()

    return {
        "dre": dre,
        "multiplos": multiplos,
        "anos_hist": int(anos_hist),
    }


@st.cache_data(show_spinner=False)
def load_empresas_segmento(setor: str, subsetor: str, segmento: str) -> pd.DataFrame:
    sql = """
        SELECT
            ticker,
            nome_empresa
        FROM public.setores
        WHERE "SETOR" = :setor
          AND "SUBSETOR" = :subsetor
          AND "SEGMENTO" = :segmento
    """
    return _clean_columns(
        _read_sql_df(
            sql,
            {
                "setor": setor,
                "subsetor": subsetor,
                "segmento": segmento,
            },
        )
    )


@st.cache_data(show_spinner=False)
def load_macro_clean() -> pd.DataFrame:
    """
    Retorna macroeconômico mensal já limpo e ordenado.
    """
    return load_macro_summary()
