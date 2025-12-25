from __future__ import annotations

import datetime as dt
from typing import Any, Callable, Dict, Optional

from sqlalchemy import text

from core.db_supabase import get_engine


# -----------------------------
# Tabelas de controle
# -----------------------------
def _ensure_sync_log(engine) -> None:
    ddl = """
    create schema if not exists cvm;

    create table if not exists cvm.sync_log (
        id bigserial primary key,
        run_at timestamptz not null default now(),
        status text not null,
        last_year integer,
        remote_latest_year integer,
        message text
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _insert_sync_log(
    engine,
    *,
    status: str,
    last_year: Optional[int],
    remote_latest_year: Optional[int],
    message: str,
) -> None:
    sql = """
    insert into cvm.sync_log (run_at, status, last_year, remote_latest_year, message)
    values (now(), :status, :last_year, :remote_latest_year, :message)
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "status": status,
                "last_year": last_year,
                "remote_latest_year": remote_latest_year,
                "message": (message or "")[:4000],
            },
        )


def get_sync_status() -> Dict[str, Any]:
    engine = get_engine()
    _ensure_sync_log(engine)

    sql = """
    select run_at, status, last_year, remote_latest_year, message
    from cvm.sync_log
    order by run_at desc
    limit 1
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql)).mappings().first()

    if not row:
        return {
            "last_year": None,
            "last_run_at": None,
            "remote_latest_year": None,
            "has_updates": None,
            "notes": "Sem histórico em cvm.sync_log.",
            "status": None,
        }

    last_year = row.get("last_year")
    remote_latest_year = row.get("remote_latest_year")

    has_updates: Optional[bool] = None
    if isinstance(last_year, int) and isinstance(remote_latest_year, int):
        has_updates = remote_latest_year > last_year

    return {
        "last_year": last_year,
        "last_run_at": row.get("run_at"),
        "remote_latest_year": remote_latest_year,
        "has_updates": has_updates,
        "notes": row.get("message"),
        "status": row.get("status"),
    }


# -----------------------------
# Orquestrador: ATUALIZA TUDO
# -----------------------------
def apply_update(
    *,
    start_year: int = 2010,
    end_year: Optional[int] = None,
    years_per_run: int = 1,
    quarters_per_run: int = 1,
    progress_cb: Optional[Callable[..., None]] = None,  # aceita 1 ou 2 args
) -> None:
    if end_year is None:
        end_year = dt.datetime.now().year

    def _emit(pct: float, msg: str) -> None:
        """Compatível com callbacks antigos e novos:
        - progress_cb(msg)
        - progress_cb(pct, msg)
        """
        if not progress_cb:
            return
        try:
            progress_cb(float(pct), str(msg))
        except TypeError:
            progress_cb(str(msg))

    engine = get_engine()
    _ensure_sync_log(engine)

    logs: list[str] = []
    last_year: Optional[int] = None
    remote_latest_year: Optional[int] = int(end_year)

    def _stage_done(label: str) -> None:
        _emit(0, f"{label}: concluído.")
        logs.append(f"{label}:done")

    try:
        _emit(2, "Iniciando sincronização…")
        logs.append("start")

        # -------- DFP --------
        _emit(10, "DFP (anual): executando…")
        logs.append("dfp:start")
        try:
            import cvm.cvm_dfp_ingest as cvm_dfp_ingest

            cvm_dfp_ingest.run(
                engine,
                progress_cb=lambda s: logs.append(f"DFP:{s}"),
                start_year=int(start_year),
                end_year=int(end_year),
                years_per_run=int(years_per_run),
            )
            logs.append("dfp:ok")
            _stage_done("DFP (anual)")
        except Exception as e:
            logs.append(f"dfp:error:{e}")
            raise

        # -------- ITR --------
        _emit(30, "ITR (trimestral): executando…")
        logs.append("itr:start")
        try:
            import cvm.cvm_tri_ingest as cvm_tri_ingest

            cvm_tri_ingest.run(
                engine,
                progress_cb=lambda s: logs.append(f"ITR:{s}"),
                start_year=int(start_year),
                end_year=int(end_year),
                quarters_per_run=int(quarters_per_run),
            )
            logs.append("itr:ok")
            _stage_done("ITR (trimestral)")
        except Exception as e:
            logs.append(f"itr:error:{e}")
            raise

        # -------- Setores --------
        _emit(50, "Setores: executando…")
        logs.append("setores:start")
        try:
            import cvm.setores_ingest as setores_ingest

            setores_ingest.run(engine, progress_cb=lambda s: logs.append(f"SETORES:{s}"))
            logs.append("setores:ok")
            _stage_done("Setores")
        except ModuleNotFoundError:
            logs.append("setores:skip (módulo cvm.setores_ingest não encontrado)")
            _emit(50, "Setores: ignorado (módulo não encontrado).")
        except Exception as e:
            logs.append(f"setores:error:{e}")
            raise

        # -------- Macro --------
        _emit(65, "Macro (BCB): executando…")
        logs.append("macro:start")
        try:
            import cvm.macro_bcb_ingest as macro_bcb_ingest

            macro_bcb_ingest.run(engine, progress_cb=lambda s: logs.append(f"MACRO:{s}"))
            logs.append("macro:ok")
            _stage_done("Macro (BCB)")
        except Exception as e:
            logs.append(f"macro:error:{e}")
            raise

        # -------- Métricas --------
        _emit(80, "Métricas: executando…")
        logs.append("metrics:start")
        try:
            import cvm.finance_metrics_builder as finance_metrics_builder

            finance_metrics_builder.run(engine, progress_cb=lambda s: logs.append(f"METRICS:{s}"))
            logs.append("metrics:ok")
            _stage_done("Métricas")
        except Exception as e:
            logs.append(f"metrics:error:{e}")
            raise

        # -------- Score --------
        _emit(92, "Fundamental score: executando…")
        logs.append("score:start")
        try:
            import cvm.fundamental_scoring as fundamental_scoring

            fundamental_scoring.run(engine, progress_cb=lambda s: logs.append(f"SCORE:{s}"))
            logs.append("score:ok")
            _stage_done("Fundamental score")
        except Exception as e:
            logs.append(f"score:error:{e}")
            raise

        # -------- last_year (melhor esforço) --------
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text("select max(extract(year from data))::int as y from cvm.demonstracoes_financeiras_dfp")
                ).mappings().first()
            if row and row.get("y"):
                last_year = int(row["y"])
        except Exception:
            pass

        _emit(100, "Concluído.")
        _insert_sync_log(
            engine,
            status="success",
            last_year=last_year,
            remote_latest_year=remote_latest_year,
            message=" | ".join(logs[-80:]),
        )

    except Exception as e:
        _insert_sync_log(
            engine,
            status="error",
            last_year=last_year,
            remote_latest_year=remote_latest_year,
            message=f"{e} | logs: " + " | ".join(logs[-80:]),
        )
        raise
