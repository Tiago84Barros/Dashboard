"""
pipeline_local/utils/logger.py
Logging estruturado JSON para todos os estágios do pipeline local.

Uso:
    from pipeline_local.utils.logger import get_logger
    log = get_logger("extract_cvm_dfp")
    log.info("Iniciando extração", ano=2023, doc_type="DFP")
    log.error("Falha ao inserir batch", erro="...", batch_size=1000)
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any


_LOG_LEVEL = os.getenv("PIPELINE_LOG_LEVEL", "INFO").upper()


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Campos extras passados via extra={}
        for key, val in record.__dict__.items():
            if key.startswith("_pl_"):
                payload[key[4:]] = val
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def get_logger(name: str) -> "PipelineLogger":
    base = logging.getLogger(f"pipeline_local.{name}")
    if not base.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        base.addHandler(handler)
        base.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))
        base.propagate = False
    return PipelineLogger(base)


class PipelineLogger:
    """Wrapper fino que injeta campos extras no JSON sem poluir a assinatura do logger."""

    def __init__(self, base: logging.Logger) -> None:
        self._base = base

    def _emit(self, level: str, message: str, **fields: Any) -> None:
        extra = {f"_pl_{k}": v for k, v in fields.items()}
        getattr(self._base, level)(message, extra=extra)

    def info(self, message: str, **fields: Any) -> None:
        self._emit("info", message, **fields)

    def warning(self, message: str, **fields: Any) -> None:
        self._emit("warning", message, **fields)

    def error(self, message: str, **fields: Any) -> None:
        self._emit("error", message, **fields)

    def debug(self, message: str, **fields: Any) -> None:
        self._emit("debug", message, **fields)

    def summary(self, pipeline: str, status: str, **metrics: Any) -> None:
        """Emite evento de summary padronizado — idêntico ao formato de pickup/."""
        payload = {
            "event": "summary",
            "pipeline": pipeline,
            "status": status,
            "finished_at": datetime.now(tz=timezone.utc).isoformat(),
            **metrics,
        }
        self._emit("info", json.dumps(payload, ensure_ascii=False, default=str), event="summary")
