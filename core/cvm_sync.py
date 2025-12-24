from __future__ import annotations

import os
from urllib.parse import quote_plus

import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def _build_db_url() -> str:
    """
    Monta a URL a partir de variáveis de ambiente (recomendado no Streamlit Cloud).
    Você pode definir no Secrets:
      SUPABASE_DB_USER
      SUPABASE_DB_PASSWORD
      SUPABASE_DB_HOST
      SUPABASE_DB_PORT
      SUPABASE_DB_NAME
    """
    user = os.getenv("SUPABASE_DB_USER", "")
    password = os.getenv("SUPABASE_DB_PASSWORD", "")
    host = os.getenv("SUPABASE_DB_HOST", "")
    port = os.getenv("SUPABASE_DB_PORT", "5432")
    dbname = os.getenv("SUPABASE_DB_NAME", "postgres")

    if not user or not password or not host:
        raise RuntimeError(
            "Variáveis do Supabase não configuradas. Defina SUPABASE_DB_USER, "
            "SUPABASE_DB_PASSWORD, SUPABASE_DB_HOST, SUPABASE_DB_PORT, SUPABASE_DB_NAME."
        )

    return f"postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:{port}/{dbname}"


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    """
    Engine única por processo (cache_resource).
    Pool pequeno para evitar estourar 'max client connections' no pooler do Supabase.
    """
    url = _build_db_url()

    engine = create_engine(
        url,
        pool_size=2,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        future=True,
    )
    return engine
