from __future__ import annotations

import os
from typing import Optional

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ────────────────────────────────────────────────────────────────────────────────
# Supabase / PostgreSQL
# ────────────────────────────────────────────────────────────────────────────────
def _get_supabase_url() -> str:
    # padrão do seu projeto (Streamlit Secrets -> env)
    db_url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Defina SUPABASE_DB_URL (ou DATABASE_URL) nas secrets/env vars.")
    return db_url



@st.cache_resource(show_spinner=False)
def get_supabase_engine() -> Engine:
    """
    Engine singleton em cache (recomendado para Streamlit).
    """
    return create_engine(_get_supabase_url(), pool_pre_ping=True)


def _normalize_ticker(ticker: str) -> tuple[str, str]:
    """
    Aceita PETR4 ou PETR4.SA e retorna (tk1, tk2):
      tk1 = upper original
      tk2 = sem sufixo .SA
    """
    tk1 = (ticker or "").strip().upper()
    tk2 = tk1.replace(".SA", "")
    return tk1, tk2


def _read_sql_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    """
    Executa SQL no Supabase e retorna DataFrame.
    """
    engine = get_supabase_engine()
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


# ════════════════════════════════════════════════════════════════════════════════
# Loaders (Supabase) – em cache para evitar I/O repetido
# ════════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner=False)
def load_setores_from_db() -> Optional[pd.DataFrame]:
    """
    Compatibilidade: mantém o nome antigo, mas agora lê do Supabase.
    Retorna as colunas esperadas pelo basic.py: ticker, SETOR, SUBSETOR, SEGMENTO.
    """
    try:
        df = _read_sql_df(
            """
            SELECT
                ticker,
                setor     AS "SETOR",
                subsetor  AS "SUBSETOR",
                segmento  AS "SEGMENTO"
            FROM public.setores
            WHERE ticker IS NOT NULL
            """
        )
        # Normalização defensiva
        df["ticker"] = df["ticker"].astype(str).str.replace(".SA", "", regex=False).str.upper()
        return df
    except Exception as e:
        st.error(f"Erro ao carregar tabela 'setores' do Supabase: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_setores_from_supabase() -> Optional[pd.DataFrame]:
    """
    Nome explícito (recomendado para novas páginas).
    """
    return load_setores_from_db()


@st.cache_data(show_spinner=False)
def load_data_from_db(ticker: str) -> Optional[pd.DataFrame]:
    """
    Carrega a tabela Demonstracoes_Financeiras (DFP anual) do Supabase para o ticker informado.
    Mantém assinatura legado.
    """
    tk1, tk2 = _normalize_ticker(ticker)
    try:
        return _read_sql_df(
            """
            SELECT *
            FROM public."Demonstracoes_Financeiras"
            WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
            ORDER BY "Data" ASC
            """,
            {"tk1": tk1, "tk2": tk2},
        )
    except Exception as e:
        st.error(f"Erro ao carregar DRE (DFP) para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_multiplos_from_db(ticker: str) -> Optional[pd.DataFrame]:
    """
    Carrega a tabela multiplos (anuais) do Supabase para o ticker.
    """
    tk1, tk2 = _normalize_ticker(ticker)
    try:
        return _read_sql_df(
            """
            SELECT *
            FROM public.multiplos
            WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
            ORDER BY "Data" ASC
            """,
            {"tk1": tk1, "tk2": tk2},
        )
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos (anuais) para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_multiplos_limitado_from_db(ticker: str, limite: int = 250) -> Optional[pd.DataFrame]:
    """
    Carrega os últimos `limite` registros da tabela multiplos (anuais) para gráficos leves.
    """
    tk1, tk2 = _normalize_ticker(ticker)
    try:
        df = _read_sql_df(
            """
            SELECT *
            FROM public.multiplos
            WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
            ORDER BY "Data" DESC
            LIMIT :limite
            """,
            {"tk1": tk1, "tk2": tk2, "limite": int(limite)},
        )
        # volta a ordem crescente por Data
        if "Data" in df.columns:
            df = df.sort_values("Data")
        return df
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos limitados para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_multiplos_tri_from_db(ticker: str) -> Optional[pd.DataFrame]:
    """
    Carrega o registro mais recente de multiplos_TRI (trimestrais, LTM) no Supabase.
    """
    tk1, tk2 = _normalize_ticker(ticker)
    try:
        return _read_sql_df(
            """
            SELECT *
            FROM public.multiplos_TRI
            WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
            ORDER BY "Data" DESC
            LIMIT 1
            """,
            {"tk1": tk1, "tk2": tk2},
        )
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos TRI para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_macro_summary() -> Optional[pd.DataFrame]:
    """
    Carrega a tabela info_economica (macro anual) do Supabase.
    """
    try:
        return _read_sql_df(
            """
            SELECT *
            FROM public.info_economica
            ORDER BY "Data" ASC
            """
        )
    except Exception as e:
        st.error(f"Erro ao carregar dados macroeconômicos: {e}")
        return None
