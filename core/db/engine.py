# core/db/engine.py
from __future__ import annotations

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import streamlit as st

from core.db_supabase import get_engine  # reexport

@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    """
    Engine único por sessão Streamlit.
    Pool mínimo para não estourar conexões no Supabase Free.
    """

    db_url = st.secrets.get("SUPABASE_DB_URL") or os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL não configurada.")

    engine = create_engine(
        db_url,
        pool_size=1,          # 🔴 crítico
        max_overflow=0,       # 🔴 crítico
        pool_pre_ping=True,
        pool_recycle=1800,
        future=True,
    )
    return engine
