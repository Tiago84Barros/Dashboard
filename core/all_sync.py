# core/sync/all_sync.py
from __future__ import annotations

import datetime as dt
import traceback
from dataclasses import dataclass
from typing import Callable, Optional, Any, Dict

from sqlalchemy import text
from sqlalchemy.engine import Engine


SCHEMA = "cvm"
TARGET_DFP_TABLE = f"{SCHEMA}.demonstracoes_financeiras"  # coerente com cvm_dfp_ingest.py
SYNC_LOG_TABLE = f"{SCHEMA}.sync_log"


@dataclass(frozen=True)
class SyncConfig:
    # DFP incremental (seu cvm_dfp_ingest é “1 ano por execução” por padrão)
    start_year: int = 2010
    end_year: int = dt.datetime.now().year
    years_per_run: int = 1

    # Flags (você pode desligar etapas)
    run_dfp: bool = True
    run_itr: bool = True
    run_setores: bool = True
    run_macro: bool = True
    run_metrics_builder: bool = True
    run_scoring: bool = True


def _ensure_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("create schema if not exists cvm;"))


def _synclog_insert(
    engine: Engine,
    status: str,
    last_year: Optional[int],
    remote_latest_year: Optional[int],
    message: str,
) -> None:
    """
    Assume que a tabela cvm.sync_log já existe.
    Se sua tabela tiver nomes de colunas diferentes, ajuste aqui.
    """
    q = text(
        f"""
        insert into {SYNC_LOG_TABLE} (run_at, status, last_year, remote_latest_year, message)
        values (now(), :status, :last_year, :remote_latest_year, :message)
        """
    )
    with engine.begin() as conn:
        conn.execute(
            q,
            {
                "status": status,
                "last_year": last_year,
                "remote_latest_year": remote_latest_year,
                "message": (message or "")[:4000],
            },
        )


def get_last_year_in_db(engine: Engine) -> Optional[int]:
    q = text(f"select max(extract(year from data))::int as last_year from {TARGET_DFP_TABLE}")
    with engine.begin() as conn:
        return conn.execute(q).scalar()


def _call_progress(progress_cb: Optional[Callable[[float, str], None]], pct: float, msg: str) -> None:
    if progress_cb:
        progress_cb(float(pct), msg)


def _safe_import(module_path: str):
    import importlib
    return importlib.import_module(module_path)


def apply_full_update(
    engine: Engine,
    cfg: SyncConfig,
    *,
    progress_cb: Optional[Callable[[float, str], None]] = None,
) -> Dict[str, Any]:
    """
    Roda TODAS as rotinas necessárias para o app.
    Registra no sync_log.
    Retorna um dict com resumo.
    """
    _ensure_schema(engine)

    last_year_before = get_last_year_in_db(engine)
    remote_latest_year = None  # opcional (se você criar detector no futuro)

    try:
        _synclog_insert(engine, "running", last_year_before, remote_latest_year, "Iniciando sincronização completa")
        _call_progress(progress_cb, 2, "Preparando ambiente…")

        # 1) DFP
        if cfg.run_dfp:
            _call_progress(progress_cb, 10, "DFP: iniciando (CVM → Supabase)…")
            dfp_mod = _safe_import("cvm.cvm_dfp_ingest")
            dfp_run = getattr(dfp_mod, "run", None)
            if not callable(dfp_run):
                raise ImportError("cvm_dfp_ingest.run() não encontrado")

            # Seu DFP usa progress_cb(msg: str). Vamos adaptar para pct.
            steps_total = max(1, int(cfg.years_per_run))
            steps_done = {"i": 0}

            def dfp_progress(msg: str) -> None:
                steps_done["i"] += 1
                pct = 10 + (25 * min(steps_done["i"], steps_total) / steps_total)  # 10 → 35
                _call_progress(progress_cb, pct, msg)

            dfp_run(
                engine,
                progress_cb=dfp_progress,
                start_year=int(cfg.start_year),
                end_year=int(cfg.end_year),
                years_per_run=int(cfg.years_per_run),
            )
            _call_progress(progress_cb, 35, "DFP: concluído.")

        # 2) ITR
        if cfg.run_itr:
            _call_progress(progress_cb, 40, "ITR: iniciando (CVM → Supabase)…")
            itr_mod = _safe_import("cvm.cvm_itr_ingest")
            itr_run = getattr(itr_mod, "run", None)
            if not callable(itr_run):
                raise ImportError("cvm_itr_ingest.run() não encontrado")
            # Mantém flexível: se seu run aceitar progress_cb, ótimo; se não, roda direto.
            try:
                itr_run(engine=engine, progress_cb=lambda m: _call_progress(progress_cb, 48, str(m)))
            except TypeError:
                try:
                    itr_run(engine)
                except TypeError:
                    itr_run(engine=engine)
            _call_progress(progress_cb, 50, "ITR: concluído.")

        # 3) Setores
        if cfg.run_setores:
            _call_progress(progress_cb, 55, "Setores: atualizando (B3 + cvm_to_ticker → Supabase)…")
            set_mod = _safe_import("cvm.setores_ingest")
            set_run = getattr(set_mod, "run", None)
            if not callable(set_run):
                raise ImportError("setores_ingest.run() não encontrado")
            try:
                set_run(engine=engine)
            except TypeError:
                set_run(engine)
            _call_progress(progress_cb, 65, "Setores: concluído.")

        # 4) Macro (BCB)
        if cfg.run_macro:
            _call_progress(progress_cb, 70, "Macro (BCB): atualizando…")
            macro_mod = _safe_import("cvm.macro_bcb_ingest")
            macro_run = getattr(macro_mod, "run", None)
            if not callable(macro_run):
                raise ImportError("macro_bcb_ingest.run() não encontrado")
            try:
                macro_run(engine=engine)
            except TypeError:
                macro_run(engine)
            _call_progress(progress_cb, 78, "Macro: concluído.")

        # 5) Builder de métricas
        if cfg.run_metrics_builder:
            _call_progress(progress_cb, 82, "Métricas: recalculando…")
            met_mod = _safe_import("cvm.finance_metrics_builder")
            met_run = getattr(met_mod, "run", None)
            if not callable(met_run):
                raise ImportError("finance_metrics_builder.run() não encontrado")
            try:
                met_run(engine=engine)
            except TypeError:
                met_run(engine)
            _call_progress(progress_cb, 90, "Métricas: concluído.")

        # 6) Score / tabelas finais do app
        if cfg.run_scoring:
            _call_progress(progress_cb, 93, "Score: recalculando tabelas finais…")
            sc_mod = _safe_import("cvm.fundamental_scoring")
            sc_run = getattr(sc_mod, "run", None)
            if not callable(sc_run):
                raise ImportError("fundamental_scoring.run() não encontrado")
            try:
                sc_run(engine=engine)
            except TypeError:
                sc_run(engine)
            _call_progress(progress_cb, 98, "Score: concluído.")

        last_year_after = get_last_year_in_db(engine)
        _call_progress(progress_cb, 100, "Sincronização completa concluída.")

        _synclog_insert(engine, "success", last_year_after, remote_latest_year, "Atualização completa OK")

        return {
            "ok": True,
            "last_year_before": last_year_before,
            "last_year_after": last_year_after,
        }

    except Exception as e:
        tb = traceback.format_exc()
        _synclog_insert(engine, "error", last_year_before, remote_latest_year, f"{repr(e)}\n{tb}")
        raise
