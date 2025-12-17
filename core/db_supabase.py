import os
from sqlalchemy import create_engine

def get_database_url() -> str:
    """
    Espera DATABASE_URL no formato SQLAlchemy, ex:
    postgresql+psycopg2://user:pass@host:port/postgres
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL não definida. Configure no ambiente/Secrets.")
    return url

def get_engine():
    """
    Engine SQLAlchemy para Supabase/Postgres.
    pool_pre_ping evita conexões quebradas.
    """
    url = get_database_url()
    return create_engine(url, pool_pre_ping=True)
