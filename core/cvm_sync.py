# core/cvm_sync.py
from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional, Callable, List, Tuple

import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.db.engine import get_engine

# Ingestores
import cvm_dfp_ingest
import cvm_tri_ingest
import macro_bcb_ingest
from cvm.setores_ingest import run as setores_run

# Derivados (se existirem no seu projeto com run(engine,...))
try:
    import finance_metrics_builder
except Exception:
    finance_metrics_builder = None

try:
    import fundamental_scoring
except Exception:
    fundamental_scoring = None


def _ensure_sync_log(engine: Engine) -> None:
    """
    Você disse que já existe public.sync_log.
    Mesmo assim, garantimos a existência com um schema simples e compatível.
    """
    ddl = """
    create table if not exists public.sync_log (
        id bigserial primary key,
        job text not null,
        status text not null,
        started_at timestamptz not null default now(),
        finished_at timestamptz,
        details text
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _log_start(engine: Engine, job: str) -> int:
    _ensure_sync_log(engine)
    q = text("insert into public.sync_log(job, status, started_at) values (:j, 'running', now()) returning id;")
    with engine.begin() as conn:
        r = conn.execute(q, {"j": job}).scalar_one()
    return int(r)


def _log_finish(engine: Engine, row_id: int, status: str, details: str = "") -> None:
    q = text("""
        update public.sync_log
        set status = :s, finished_at = now(), details = :d
        where id = :id
    """)
    with engine.begin() as conn:
        conn.execute(q, {"s": status, "d": details[:5000], "id": row_id})


def _max_year_dfp(engine: Engine) -> Optional[int]:
    try:
        q = text("select max(extract(year from data)) as y from cvm.demonstracoes_financeiras_dfp")
        with engine.begin() as conn:
            y = conn.execute(q).scalar()
        return int(y) if y is not None else None
    except Exception:
        return None


def _remote_latest_year_cvm_dfp(timeout_sec: int = 20) -> Optional[int]:
    """
    Descobre o último ano disponível na CVM para DFP tentando HEAD de trás para frente.
    """
    base = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{year}.zip"
    now = dt.datetime.now().year

    for y in range(now, 2009, -1):
        try:
            r = requests.head(base.format(year=y), timeout=timeout_sec)
            if r.status_code == 200:
                return y
        except Exception:
            continue
    return None


def get_sync_status(engine: Optional[Engine] = None) -> Dict[str, Any]:
    """
    Retorna informações para a tela de Configurações.
    """
    engine = engine or get_engine()
    _ensure_sync_log(engine)

    last_year = _max_year_dfp(engine)
    remote_latest = _remote_latest_year_cvm_dfp()

    last_run_at = None
    try:
        q = text("select finished_at from public.sync_log where status in ('success','failed') order by finished_at desc nulls last limit 1;")
        with engine.begin() as conn:
            last_run_at = conn.execute(q).scalar()
        if last_run_at is not None:
            last_run_at = last_run_at.isoformat()
    except Exception:
        last_run_at = None

    has_updates = None
    if last_year is not None and remote_latest is not None:
        has_updates = bool(remote_latest > last_year)

    return {
        "last_year": last_year,
        "last_run_at": last_run_at,
        "remote_latest_year": remote_latest,
        "has_updates": has_updates,
        "notes": "",
    }


def apply_update(
    engine: Optional[Engine] = None,
    *,
    progress_cb: Optional[Callable[[float, str], None]] = None,
    dfp_years_per_run: int = 1,
    itr_quarters_per_run: int = 1,
    start_year: int = 2010,
    end_year: int = 2025,
) -> None:
    """
    Atualiza TODAS as tabelas necessárias do Supabase.
    A estratégia é incremental (DFP por ano, ITR por trimestre) para evitar travar o Streamlit.
    """
    engine = engine or get_engine()

    def _p(pct: float, msg: str) -> None:
        if progress_cb:
            progress_cb(float(pct), msg)

    row_id = _log_start(engine, job="full_sync")

    try:
        _p(2, "Iniciando sincronização completa...")

        # 1) Setores (B3)
        _p(6, "Atualizando Setores (B3 → Supabase)...")
        setores_run(engine, progress_cb=lambda m: _p(10, m))

        # 2) Macro (BCB)
        _p(18, "Atualizando Macro (BCB/SGS → cvm.info_economica)...")
        macro_bcb_ingest.run(engine)  # já faz upsert
        _p(28, "Macro: concluído.")

        # 3) DFP (CVM) – incremental
        _p(32, "Atualizando DFP (CVM → cvm.demonstracoes_financeiras_dfp)...")
        cvm_dfp_ingest.run(
            engine,
            progress_cb=lambda m: _p(55, m),
            start_year=start_year,
            end_year=end_year,
            years_per_run=int(dfp_years_per_run),
        )
        _p(60, "DFP: concluído (nesta execução).")

        # 4) ITR/TRI (CVM) – incremental
        _p(64, "Atualizando ITR/TRI (CVM → cvm.demonstracoes_financeiras_tri)...")
        cvm_tri_ingest.run(
            engine,
            progress_cb=lambda m: _p(78, m),
            start_year=start_year,
            end_year=end_year,
            quarters_per_run=int(itr_quarters_per_run),
        )
        _p(82, "ITR/TRI: concluído (nesta execução).")

        # 5) Derivados: métricas / score (se módulos existirem)
        if finance_metrics_builder is not None and hasattr(finance_metrics_builder, "run"):
            _p(86, "Recalculando métricas financeiras (derivados)...")
            finance_metrics_builder.run(engine)
            _p(92, "Métricas: concluído.")

        if fundamental_scoring is not None and hasattr(fundamental_scoring, "run"):
            _p(94, "Recalculando score fundamentalista (derivados)...")
            fundamental_scoring.run(engine)
            _p(98, "Score: concluído.")

        _p(100, "Sincronização completa finalizada com sucesso.")
        _log_finish(engine, row_id, "success", details="full_sync ok")
    except Exception as e:
        _log_finish(engine, row_id, "failed", details=str(e))
        raise
