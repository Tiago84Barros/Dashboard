# core/sync_state.py
from __future__ import annotations

from typing import Optional, Dict

from sqlalchemy import text
from sqlalchemy.engine import Engine


def ensure_sync_table(engine: Engine) -> None:
    ddl = """
    create schema if not exists cvm;

    create table if not exists cvm.sync_state (
        key text primary key,
        value text,
        updated_at timestamptz not null default now()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def get_state(engine: Engine, key: str) -> Optional[Dict[str, str]]:
    ensure_sync_table(engine)
    q = text("select key, value, updated_at from cvm.sync_state where key = :k")
    with engine.begin() as conn:
        r = conn.execute(q, {"k": key}).mappings().first()
    return dict(r) if r else None


def set_state(engine: Engine, key: str, value: str) -> None:
    ensure_sync_table(engine)
    q = text(
        """
        insert into cvm.sync_state(key, value, updated_at)
        values (:k, :v, now())
        on conflict (key) do update set
            value = excluded.value,
            updated_at = now();
        """
    )
    with engine.begin() as conn:
        conn.execute(q, {"k": key, "v": value})
