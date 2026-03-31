# auditoria_dados/ingestion_log.py
#
# Log de auditoria para pipelines de ingestão.
#
# Uso nos scripts de pickup:
#
#   from auditoria_dados.ingestion_log import IngestionLog
#
#   with IngestionLog("dfp") as log:
#       log.set_params({"years": [2022, 2023], "tickers": ["PETR4"]})
#       # ... processamento ...
#       log.add_rows(inserted=150, updated=30, skipped=5)
#       log.add_error("Ticker XPTO não encontrado no CVM")
#
# A tabela public.ingestion_log deve existir (ver migration abaixo).
#
# Criação da tabela (rodar uma vez no Supabase):
#
#   CREATE TABLE IF NOT EXISTS public.ingestion_log (
#       id            BIGSERIAL PRIMARY KEY,
#       pipeline      TEXT NOT NULL,
#       started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
#       finished_at   TIMESTAMPTZ,
#       status        TEXT NOT NULL DEFAULT 'running',
#       rows_inserted INT DEFAULT 0,
#       rows_updated  INT DEFAULT 0,
#       rows_skipped  INT DEFAULT 0,
#       errors_count  INT DEFAULT 0,
#       params        JSONB,
#       error_detail  TEXT
#   );
#
from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def _get_engine() -> Optional[Engine]:
    url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not url:
        return None
    return create_engine(url, pool_pre_ping=True)


class IngestionLog:
    """
    Context manager que registra início/fim e estatísticas de uma execução.

    Se o banco não estiver acessível, opera silenciosamente (soft failure)
    para não bloquear o pipeline de ingestão.
    """

    def __init__(self, pipeline: str):
        self.pipeline = pipeline
        self._engine: Optional[Engine] = None
        self._log_id: Optional[int] = None
        self._params: Dict[str, Any] = {}
        self._rows_inserted = 0
        self._rows_updated = 0
        self._rows_skipped = 0
        self._errors: List[str] = []
        self._started_at = datetime.now(timezone.utc)

    # ── public API ────────────────────────────────────────────────────────

    def set_params(self, params: Dict[str, Any]) -> None:
        self._params = params

    def add_rows(
        self,
        inserted: int = 0,
        updated: int = 0,
        skipped: int = 0,
    ) -> None:
        self._rows_inserted += inserted
        self._rows_updated += updated
        self._rows_skipped += skipped

    def add_error(self, message: str) -> None:
        self._errors.append(message)

    # ── context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "IngestionLog":
        self._engine = _get_engine()
        if self._engine:
            try:
                self._log_id = self._insert_start()
            except Exception:
                pass  # soft failure
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is not None:
            self._errors.append(
                "".join(traceback.format_exception(exc_type, exc_val, exc_tb))[-500:]
            )
        status = "failed" if exc_type else ("partial" if self._errors else "success")
        if self._engine and self._log_id:
            try:
                self._update_finish(status)
            except Exception:
                pass  # soft failure
        return False  # don't suppress exceptions

    # ── private ───────────────────────────────────────────────────────────

    def _insert_start(self) -> Optional[int]:
        sql = text("""
            INSERT INTO public.ingestion_log
                (pipeline, started_at, status, params)
            VALUES
                (:pipeline, :started_at, 'running', :params)
            RETURNING id
        """)
        with self._engine.begin() as conn:
            row = conn.execute(
                sql,
                {
                    "pipeline": self.pipeline,
                    "started_at": self._started_at,
                    "params": json.dumps(self._params),
                },
            ).fetchone()
            return row[0] if row else None

    def _update_finish(self, status: str) -> None:
        error_detail = "\n---\n".join(self._errors) if self._errors else None
        sql = text("""
            UPDATE public.ingestion_log SET
                finished_at    = :finished_at,
                status         = :status,
                rows_inserted  = :rows_inserted,
                rows_updated   = :rows_updated,
                rows_skipped   = :rows_skipped,
                errors_count   = :errors_count,
                error_detail   = :error_detail
            WHERE id = :id
        """)
        with self._engine.begin() as conn:
            conn.execute(
                sql,
                {
                    "finished_at": datetime.now(timezone.utc),
                    "status": status,
                    "rows_inserted": self._rows_inserted,
                    "rows_updated": self._rows_updated,
                    "rows_skipped": self._rows_skipped,
                    "errors_count": len(self._errors),
                    "error_detail": error_detail,
                    "id": self._log_id,
                },
            )
