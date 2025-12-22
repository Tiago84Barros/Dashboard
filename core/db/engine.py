# core/db/engine.py
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from core.config.settings import get_settings


def get_engine() -> Engine:
    """
    Engine do Supabase (Postgres) via SQLAlchemy.
    Requer SUPABASE_DB_URL em st.secrets ou ENV.
    """
    settings = get_settings()
    if not settings.supabase_db_url:
        raise RuntimeError(
            "SUPABASE_DB_URL não configurada. Defina em st.secrets (Streamlit Cloud) ou ENV."
        )

    return create_engine(
        settings.supabase_db_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
        pool_recycle=1800,
    )
