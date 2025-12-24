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
def get_engine():
    db_url = os.getenv("SUPABASE_DB_URL")

    # ✅ PRIORIDADE TOTAL PARA URL ÚNICA
    if db_url:
        return create_engine(db_url, pool_pre_ping=True)

    # fallback legado
    user = os.getenv("SUPABASE_USER")
    password = os.getenv("SUPABASE_PASSWORD")
    host = os.getenv("SUPABASE_HOST")
    port = os.getenv("SUPABASE_PORT", "5432")
    name = os.getenv("SUPABASE_DB_NAME", "postgres")

    if not all([user, password, host]):
        raise RuntimeError(
            "Config Supabase incompleta. "
            "Defina SUPABASE_DB_URL ou USER/PASSWORD/HOST."
        )

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return create_engine(url, pool_pre_ping=True)

