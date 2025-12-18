# core/db_loader_supabase.py
from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text

from core.db_supabase import get_engine

@st.cache_data
def load_setores_from_db() -> pd.DataFrame | None:
    try:
        engine = get_engine()
        return pd.read_sql("select * from cvm.setores", engine)
    except Exception as e:
        st.error(f"Erro ao carregar setores (Supabase): {e}")
        return None

@st.cache_data
def load_data_from_db(ticker: str) -> pd.DataFrame | None:
    try:
        engine = get_engine()
        tk1 = (ticker or "").upper()
        tk2 = tk1.replace(".SA", "")
        q = text("""
            select *
            from cvm.demonstracoes_financeiras
            where "Ticker" = :tk1 or "Ticker" = :tk2
            order by "Data" asc
        """)
        return pd.read_sql(q, engine, params={"tk1": tk1, "tk2": tk2})
    except Exception as e:
        st.error(f"Erro ao carregar DRE para {ticker} (Supabase): {e}")
        return None

@st.cache_data
def load_multiplos_from_db(ticker: str) -> pd.DataFrame | None:
    try:
        engine = get_engine()
        tk1 = (ticker or "").upper()
        tk2 = tk1.replace(".SA", "")
        q = text("""
            select *
            from cvm.multiplos
            where "Ticker" = :tk1 or "Ticker" = :tk2
            order by "Data" asc
        """)
        return pd.read_sql(q, engine, params={"tk1": tk1, "tk2": tk2})
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos para {ticker} (Supabase): {e}")
        return None

@st.cache_data
def load_multiplos_limitado_from_db(ticker: str, limite: int = 250) -> pd.DataFrame | None:
    try:
        engine = get_engine()
        tk1 = (ticker or "").upper()
        tk2 = tk1.replace(".SA", "")
        q = text(f"""
            select *
            from cvm.multiplos
            where "Ticker" = :tk1 or "Ticker" = :tk2
            order by "Data" desc
            limit {int(limite)}
        """)
        df = pd.read_sql(q, engine, params={"tk1": tk1, "tk2": tk2})
        return df.sort_values("Data")
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos limitados para {ticker} (Supabase): {e}")
        return None

@st.cache_data
def load_multiplos_tri_from_db(ticker: str) -> pd.DataFrame | None:
    try:
        engine = get_engine()
        tk1 = (ticker or "").upper()
        tk2 = tk1.replace(".SA", "")
        q = text("""
            select *
            from cvm.multiplos_tri
            where "Ticker" = :tk1 or "Ticker" = :tk2
            order by "Data" desc
            limit 1
        """)
        return pd.read_sql(q, engine, params={"tk1": tk1, "tk2": tk2})
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos TRI para {ticker} (Supabase): {e}")
        return None

@st.cache_data
def load_macro_summary() -> pd.DataFrame | None:
    try:
        engine = get_engine()
        return pd.read_sql("select * from cvm.info_economica order by \"Data\" asc", engine)
    except Exception as e:
        st.error(f"Erro ao carregar macro (Supabase): {e}")
        return None
