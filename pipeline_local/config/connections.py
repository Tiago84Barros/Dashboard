from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Connection

from pipeline_local.config.settings import load_settings


def get_local_engine() -> Engine:
    settings = load_settings()
    return create_engine(settings.local_db_url, pool_pre_ping=True, future=True)


def get_supabase_engine() -> Engine:
    settings = load_settings()
    if not settings.supabase_db_url:
        raise RuntimeError("SUPABASE_DB_URL ou DATABASE_URL não encontrado para publicação remota.")
    return create_engine(settings.supabase_db_url, pool_pre_ping=True, future=True)


@contextmanager
def local_connection() -> Iterator[Connection]:
    engine = get_local_engine()
    with engine.begin() as conn:
        yield conn


@contextmanager
def supabase_connection() -> Iterator[Connection]:
    engine = get_supabase_engine()
    with engine.begin() as conn:
        yield conn
