from __future__ import annotations

import os
from urllib.parse import quote_plus

import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def _get_secret_or_env(key: str, default: str = "") -> str:
    """Read from Streamlit secrets first, then environment variables."""
    try:
        val = st.secrets.get(key, default)  # type: ignore[attr-defined]
    except Exception:
        val = default
    return val or os.getenv(key, default)


def _build_db_url() -> str:
    """
    Resolve a URL do banco para o Supabase.

    Prioridade:
    1) SUPABASE_DB_URL (secrets ou env)  -> formato postgresql+psycopg2://...
    2) Componentes (USER/PASSWORD/HOST/PORT/NAME) (secrets ou env)

    Isso evita quebra no Streamlit Cloud quando você configura apenas o SUPABASE_DB_URL.
    """
    db_url = _get_secret_or_env("SUPABASE_DB_URL", "")
    if db_url:
        return db_url

    user = _get_secret_or_env("SUPABASE_DB_USER", "")
    password = _get_secret_or_env("SUPABASE_DB_PASSWORD", "")
    host = _get_secret_or_env("SUPABASE_DB_HOST", "")
    port = _get_secret_or_env("SUPABASE_DB_PORT", "5432")
    dbname = _get_secret_or_env("SUPABASE_DB_NAME", "postgres")

    if not user or not password or not host:
        raise RuntimeError(
            "Supabase não configurado. Defina SUPABASE_DB_URL (recomendado) "
            "ou então SUPABASE_DB_USER, SUPABASE_DB_PASSWORD, SUPABASE_DB_HOST, "
            "SUPABASE_DB_PORT, SUPABASE_DB_NAME em Secrets/variáveis de ambiente."
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
