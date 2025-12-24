# core/db/engine.py
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from core.config.settings import get_settings


def get_engine() -> Engine:
    """
    Engine SQLAlchemy para Supabase Postgres.
    Usa pool_pre_ping para reduzir erros de conexão.
    """
    s = get_settings()

    # OBS: mantém credenciais fora do código (st.secrets / env)
    url = (
        f"postgresql+psycopg2://{s.supabase_user}:{s.supabase_password}"
        f"@{s.supabase_host}:{s.supabase_port}/{s.supabase_dbname}"
    )

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )
