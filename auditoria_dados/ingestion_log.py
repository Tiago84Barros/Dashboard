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
from typing import Any, Dict, Iterable, List, Optional, Sequence
from uuid import uuid4

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
        self.run_id = uuid4().hex
        self._engine: Optional[Engine] = None
        self._log_id: Optional[int] = None
        self._db_columns: set[str] = set()
        self._params: Dict[str, Any] = {}
        self._rows_inserted = 0
        self._rows_updated = 0
        self._rows_skipped = 0
        self._errors: List[str] = []
        self._warnings: List[str] = []
        self._metrics: Dict[str, Any] = {}
        self._events: List[Dict[str, Any]] = []
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
        self.log("ERROR", "error", message=message)

    def add_warning(self, message: str) -> None:
        self._warnings.append(message)
        self.log("WARN", "warning", message=message)

    def set_metric(self, key: str, value: Any) -> None:
        self._metrics[key] = value

    def increment_metric(self, key: str, amount: int = 1) -> None:
        current = self._metrics.get(key, 0)
        if not isinstance(current, (int, float)):
            current = 0
        self._metrics[key] = current + amount

    def add_event(self, event: str, level: str = "INFO", **fields: Any) -> Dict[str, Any]:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "pipeline": self.pipeline,
            "run_id": self.run_id,
            "level": str(level).upper(),
            "event": event,
        }
        payload.update(fields)
        self._events.append(payload)
        print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)
        return payload

    def add_source_metrics(
        self,
        *,
        source: str,
        ticker: str,
        documents_read: int = 0,
        documents_inserted: int = 0,
        duplicates: int = 0,
        chunks_generated: int = 0,
        stubs: int = 0,
        failures: int = 0,
    ) -> Dict[str, Any]:
        source_key = (source or "unknown").strip() or "unknown"
        ticker_key = (ticker or "").strip().upper()
        sources = self._metrics.setdefault("source_metrics", {})
        bucket_key = f"{source_key}:{ticker_key}"
        bucket = sources.setdefault(
            bucket_key,
            {
                "source": source_key,
                "ticker": ticker_key,
                "documents_read": 0,
                "documents_inserted": 0,
                "duplicates": 0,
                "chunks_generated": 0,
                "stubs": 0,
                "failures": 0,
            },
        )

        bucket["documents_read"] += int(documents_read or 0)
        bucket["documents_inserted"] += int(documents_inserted or 0)
        bucket["duplicates"] += int(duplicates or 0)
        bucket["chunks_generated"] += int(chunks_generated or 0)
        bucket["stubs"] += int(stubs or 0)
        bucket["failures"] += int(failures or 0)

        self.add_event("source_metrics", **bucket)
        return dict(bucket)

    def log(self, level: str, event: str, **fields: Any) -> None:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "pipeline": self.pipeline,
            "run_id": self.run_id,
            "level": str(level).upper(),
            "event": event,
        }
        payload.update(fields)
        print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)

    def log_step(self, event: str, **fields: Any) -> None:
        self.log("INFO", event, **fields)

    def summary(self, status: Optional[str] = None) -> Dict[str, Any]:
        finished_at = datetime.now(timezone.utc)
        duration_s = round((finished_at - self._started_at).total_seconds(), 3)
        return {
            "pipeline": self.pipeline,
            "run_id": self.run_id,
            "status": status or ("failed" if self._errors else "success"),
            "started_at": self._started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_s": duration_s,
            "rows_inserted": self._rows_inserted,
            "rows_updated": self._rows_updated,
            "rows_skipped": self._rows_skipped,
            "warnings_count": len(self._warnings),
            "errors_count": len(self._errors),
            "events_count": len(self._events),
            "metrics": self._metrics,
            "params": self._params,
        }

    def emit_summary(self, status: Optional[str] = None) -> Dict[str, Any]:
        summary = self.summary(status=status)
        self.log("INFO", "summary", **summary)
        return summary

    # ── context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "IngestionLog":
        self.log("INFO", "start", params=self._params)
        self._engine = _get_engine()
        if self._engine:
            try:
                self._db_columns = self._fetch_ingestion_log_columns()
                self._log_id = self._insert_start()
            except Exception:
                self.log("WARN", "ingestion_log_db_unavailable")
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
                self.log("WARN", "ingestion_log_db_finish_unavailable", status=status)
        self.emit_summary(status=status)
        return False  # don't suppress exceptions

    # ── private ───────────────────────────────────────────────────────────

    def _insert_start(self) -> Optional[int]:
        cols = ["pipeline", "started_at", "status", "params"]
        values = [":pipeline", ":started_at", "'running'", ":params"]
        params = {
            "pipeline": self.pipeline,
            "started_at": self._started_at,
            "params": json.dumps(self._params),
        }
        if "run_id" in self._db_columns:
            cols.append("run_id")
            values.append(":run_id")
            params["run_id"] = self.run_id

        sql = text(
            f"""
            INSERT INTO public.ingestion_log
                ({", ".join(cols)})
            VALUES
                ({", ".join(values)})
            RETURNING id
            """
        )
        with self._engine.begin() as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def _update_finish(self, status: str) -> None:
        error_detail = "\n---\n".join(self._errors) if self._errors else None
        assignments = [
            "finished_at = :finished_at",
            "status = :status",
            "rows_inserted = :rows_inserted",
            "rows_updated = :rows_updated",
            "rows_skipped = :rows_skipped",
            "errors_count = :errors_count",
            "error_detail = :error_detail",
        ]
        params = {
            "finished_at": datetime.now(timezone.utc),
            "status": status,
            "rows_inserted": self._rows_inserted,
            "rows_updated": self._rows_updated,
            "rows_skipped": self._rows_skipped,
            "errors_count": len(self._errors),
            "error_detail": error_detail,
            "id": self._log_id,
        }
        if "warnings_count" in self._db_columns:
            assignments.append("warnings_count = :warnings_count")
            params["warnings_count"] = len(self._warnings)
        if "metrics" in self._db_columns:
            assignments.append("metrics = :metrics")
            params["metrics"] = json.dumps(self._metrics)
        if "events" in self._db_columns:
            assignments.append("events = :events")
            params["events"] = json.dumps(self._events[-500:])
        if "run_id" in self._db_columns:
            assignments.append("run_id = :run_id")
            params["run_id"] = self.run_id

        sql = text(
            f"""
            UPDATE public.ingestion_log SET
                {", ".join(assignments)}
            WHERE id = :id
            """
        )
        with self._engine.begin() as conn:
            conn.execute(sql, params)

    def _fetch_ingestion_log_columns(self) -> set[str]:
        sql = text(
            """
            select column_name
            from information_schema.columns
            where table_schema = 'public'
              and table_name = 'ingestion_log'
            """
        )
        with self._engine.begin() as conn:
            rows = conn.execute(sql).fetchall()
        return {str(r[0]).lower() for r in rows}


def validate_required_columns(
    df,
    required_columns: Iterable[str],
    *,
    context: str,
    logger: Optional[IngestionLog] = None,
) -> List[str]:
    if df is None:
        missing = list(required_columns)
    else:
        missing = [col for col in required_columns if col not in df.columns]

    if missing:
        message = f"{context}: colunas obrigatórias ausentes: {missing}"
        if logger:
            logger.add_error(message)
        raise ValueError(message)

    if logger:
        logger.log(
            "INFO",
            "validated_required_columns",
            context=context,
            columns=list(required_columns),
        )
    return list(required_columns)


def validate_schema_columns(
    available_columns: Iterable[str],
    required_columns: Iterable[str],
    *,
    context: str,
    logger: Optional[IngestionLog] = None,
    optional_columns: Optional[Iterable[str]] = None,
    allow_extra: bool = True,
) -> List[str]:
    available = [str(col) for col in (available_columns or [])]
    available_set = set(available)
    required = [str(col) for col in required_columns]
    optional = [str(col) for col in (optional_columns or [])]

    missing = [col for col in required if col not in available_set]
    extras = sorted(available_set - set(required) - set(optional))

    if missing or (extras and not allow_extra):
        parts: List[str] = []
        if missing:
            parts.append(f"ausentes={missing}")
        if extras and not allow_extra:
            parts.append(f"extras={extras}")
        message = f"{context}: contrato de colunas inválido ({', '.join(parts)})"
        if logger:
            logger.add_error(message)
        raise ValueError(message)

    if logger:
        logger.log(
            "INFO",
            "validated_schema_columns",
            context=context,
            required=required,
            optional=optional,
            columns=available,
        )
    return required + optional


def validate_non_null_columns(
    df,
    columns: Sequence[str],
    *,
    context: str,
    logger: Optional[IngestionLog] = None,
    allow_empty_strings: bool = False,
) -> List[str]:
    validate_required_columns(df, columns, context=context, logger=logger)

    if df is None:
        raise ValueError(f"{context}: DataFrame ausente para validação de nulidade.")

    problems: List[str] = []
    for col in columns:
        series = df[col]
        null_count = int(series.isna().sum())
        if null_count > 0:
            problems.append(f"{col}: {null_count} nulo(s)")

        if not allow_empty_strings:
            non_null = series[~series.isna()]
            if not non_null.empty:
                empty_count = int(non_null.astype(str).str.strip().eq("").sum())
                if empty_count > 0:
                    problems.append(f"{col}: {empty_count} vazio(s)")

    if problems:
        message = f"{context}: colunas inválidas: {problems}"
        if logger:
            logger.add_error(message)
        raise ValueError(message)

    if logger:
        logger.log(
            "INFO",
            "validated_non_null_columns",
            context=context,
            columns=list(columns),
            rows=int(len(df)),
        )
    return list(columns)


def validate_key_columns(
    df,
    key_columns: Sequence[str],
    *,
    context: str,
    logger: Optional[IngestionLog] = None,
    allow_empty_strings: bool = False,
    check_duplicates: bool = False,
) -> List[str]:
    validate_non_null_columns(
        df,
        key_columns,
        context=context,
        logger=logger,
        allow_empty_strings=allow_empty_strings,
    )

    if check_duplicates:
        validate_unique_rows(df, key_columns, context=context, logger=logger)
    elif logger:
        logger.log(
            "INFO",
            "validated_key_columns",
            context=context,
            columns=list(key_columns),
        )

    return list(key_columns)


def validate_column_types(
    df,
    expected_types: Dict[str, Sequence[str] | str],
    *,
    context: str,
    logger: Optional[IngestionLog] = None,
) -> Dict[str, Sequence[str] | str]:
    validate_required_columns(df, expected_types.keys(), context=context, logger=logger)

    if df is None:
        raise ValueError(f"{context}: DataFrame ausente para validação de tipos.")

    import pandas.api.types as ptypes

    def _matches(series, expected: str) -> bool:
        kind = expected.lower().strip()
        if kind in {"datetime", "datetime64", "date"}:
            return bool(ptypes.is_datetime64_any_dtype(series))
        if kind in {"numeric", "number", "float", "int", "integer"}:
            return bool(ptypes.is_numeric_dtype(series))
        if kind in {"string", "str", "object"}:
            return bool(ptypes.is_string_dtype(series) or ptypes.is_object_dtype(series))
        if kind in {"bool", "boolean"}:
            return bool(ptypes.is_bool_dtype(series))
        raise ValueError(f"{context}: tipo esperado não suportado: {expected}")

    problems: List[str] = []
    for col, expected in expected_types.items():
        accepted = [expected] if isinstance(expected, str) else list(expected)
        if not any(_matches(df[col], item) for item in accepted):
            problems.append(f"{col}: dtype={df[col].dtype} esperado={accepted}")

    if problems:
        message = f"{context}: tipos inválidos: {problems}"
        if logger:
            logger.add_error(message)
        raise TypeError(message)

    if logger:
        logger.log(
            "INFO",
            "validated_column_types",
            context=context,
            columns={col: str(df[col].dtype) for col in expected_types},
        )
    return expected_types


def validate_unique_rows(
    df,
    unique_columns: Sequence[str],
    *,
    context: str,
    logger: Optional[IngestionLog] = None,
) -> List[str]:
    validate_required_columns(df, unique_columns, context=context, logger=logger)

    if df is None:
        raise ValueError(f"{context}: DataFrame ausente para validação de unicidade.")

    dup_mask = df.duplicated(subset=list(unique_columns), keep=False)
    if bool(dup_mask.any()):
        dup_df = df.loc[dup_mask, list(unique_columns)].head(10)
        preview = dup_df.to_dict(orient="records")
        message = (
            f"{context}: chaves duplicadas para {list(unique_columns)} "
            f"(amostra={preview})"
        )
        if logger:
            logger.add_error(message)
        raise ValueError(message)

    if logger:
        logger.log(
            "INFO",
            "validated_unique_rows",
            context=context,
            columns=list(unique_columns),
            rows=int(len(df)),
        )
    return list(unique_columns)
