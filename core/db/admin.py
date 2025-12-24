# core/db/admin.py
from __future__ import annotations

from dataclasses import dataclass
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class DbHealth:
    ok: bool
    message: str
    server_version: str | None = None


def healthcheck(engine: Engine) -> DbHealth:
    try:
        with engine.connect() as conn:
            version = conn.execute(text("select version()")).scalar()
        return DbHealth(ok=True, message="Conexão OK", server_version=str(version))
    except Exception as e:
        return DbHealth(ok=False, message=f"Falha ao conectar: {e}")


def row_count(engine: Engine, full_table: str) -> int:
    """
    full_table exemplo: 'cvm.demonstracoes_financeiras'
    """
    with engine.connect() as conn:
        return int(conn.execute(text(f"select count(*) from {full_table}")).scalar() or 0)


def last_update_date(engine: Engine, full_table: str, date_col: str = "data") -> str | None:
    with engine.connect() as conn:
        val = conn.execute(text(f"select max({date_col}) from {full_table}")).scalar()
    return str(val) if val is not None else None
