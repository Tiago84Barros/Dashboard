from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class LocalPipelineSettings:
    local_db_url: str
    supabase_db_url: Optional[str]
    local_schema: str
    log_level: str
    batch_size: int
    chunk_size: int
    start_year: int
    end_year: Optional[int]


def _safe_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return int(str(raw).strip())
    except Exception as exc:
        raise ValueError(f"Variável {name} inválida: {raw}") from exc


def _optional_int(name: str) -> Optional[int]:
    raw = os.getenv(name)
    if raw in (None, "", "0"):
        return None
    try:
        return int(str(raw).strip())
    except Exception as exc:
        raise ValueError(f"Variável {name} inválida: {raw}") from exc


_DEFAULT_DUCKDB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),  # pipeline_local/
    "..",                                                          # raiz do projeto
    "data",
    "local_pipeline.duckdb",   # nome diferente do schema para evitar conflito no DuckDB
)
_DEFAULT_LOCAL_DB_URL = f"duckdb:///{os.path.normpath(_DEFAULT_DUCKDB_PATH)}"


def load_settings() -> LocalPipelineSettings:
    local_db_url = os.getenv("LOCAL_DB_URL", "").strip()
    if not local_db_url:
        local_db_url = _DEFAULT_LOCAL_DB_URL

    supabase_db_url = (os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or "").strip() or None

    return LocalPipelineSettings(
        local_db_url=local_db_url,
        supabase_db_url=supabase_db_url,
        local_schema=os.getenv("PIPELINE_LOCAL_SCHEMA", "pipeline_local").strip() or "pipeline_local",
        log_level=os.getenv("PIPELINE_LOG_LEVEL", "INFO").strip().upper() or "INFO",
        batch_size=_safe_int("PIPELINE_BATCH_SIZE", 5000),
        chunk_size=_safe_int("PIPELINE_CHUNK_SIZE", 10000),
        start_year=_safe_int("PIPELINE_START_YEAR", 2010),
        end_year=_optional_int("PIPELINE_END_YEAR"),
    )
