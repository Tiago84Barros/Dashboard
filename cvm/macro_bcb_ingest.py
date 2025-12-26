# core/macro_bcb_ingest.py
from __future__ import annotations

from typing import Callable, Optional

from sqlalchemy.engine import Engine

from core.macro_bcb_raw_ingest import ingest_macro_bcb_raw
from core.macro_bcb_analytics import build_info_economica_mensal


def run(engine: Engine, *, progress_cb: Optional[Callable[[str], None]] = None) -> None:
    """
    Orquestrador Macro (BCB):
      1) Ingest RAW -> cvm.macro_bcb
      2) Build mensal -> cvm.info_economica_mensal
    """
    if progress_cb:
        progress_cb("MACRO: iniciando pipeline (RAW -> ANALYTICS).")

    ingest_macro_bcb_raw(engine, progress_cb=progress_cb)

    if progress_cb:
        progress_cb("MACRO: RAW concluído. Gerando tabelas analíticas mensais...")

    build_info_economica_mensal(engine, progress_cb=progress_cb)

    if progress_cb:
        progress_cb("MACRO: pipeline concluído com sucesso.")
