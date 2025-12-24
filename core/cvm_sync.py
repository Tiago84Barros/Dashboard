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
    """
    Retorna o último status de sincronização para a tela de Configurações.
    """
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
    progress_cb: Optional[Callable[[float, str], None]] = None,
) -> None:
    """
    Atualiza TODAS as tabelas necessárias do app.

    - DFP (anual)    -> cvm_dfp_ingest.run
    - ITR (tri)      -> cvm_itr_ingest.run
    - Setores        -> setores_ingest.run
    - Macro (BCB)    -> macro_bcb_ingest.run
    - Metrics        -> finance_metrics_builder.run
    - Score          -> fundamental_scoring.run

    Importante: imports são lazy (dentro da função) para não quebrar a página Configurações.
    """
    if end_year is None:
        end_year = dt.datetime.now().year

    def _p(pct: float, msg: str) -> None:
        if progress_cb:
            progress_cb(float(pct), str(msg))

    engine = get_engine()
    _ensure_sync_log(engine)

    logs: list[str] = []
    last_year: Optional[int] = None
    remote_latest_year: Optional[int] = int(end_year)

    try:
        _p(2, "Iniciando sincronização…")
        logs.append("start")

        # -------- DFP --------
        _p(10, "DFP (anual): executando…")
        logs.append("dfp:start")
        try:
            import cvm_dfp_ingest  # noqa
            cvm_dfp_ingest.run(
                engine,
                progress_cb=lambda s: logs.append(f"DFP:{s}"),
                start_year=int(start_year),
                end_year=int(end_year),
                years_per_run=int(years_per_run),
            )
            logs.append("dfp:ok")
        except Exception as e:
            logs.append(f"dfp:error:{e}")
            raise

        # -------- ITR --------
        _p(30, "ITR (trimestral): executando…")
        logs.append("itr:start")
        try:
            import cvm_itr_ingest  # noqa
            cvm_itr_ingest.run(
                engine,
                progress_cb=lambda s: logs.append(f"ITR:{s}"),
                start_year=int(start_year),
                end_year=int(end_year),
                quarters_per_run=int(quarters_per_run),
            )
            logs.append("itr:ok")
        except Exception as e:
            logs.append(f"itr:error:{e}")
            raise

        # -------- Setores --------
        _p(50, "Setores: atualizando…")
        logs.append("setores:start")
        try:
            import setores_ingest  # noqa
            setores_ingest.run(engine, progress_cb=lambda s: logs.append(f"SETORES:{s}"))
            logs.append("setores:ok")
        except Exception as e:
            logs.append(f"setores:error:{e}")
            raise

        # -------- Macro --------
        _p(65, "Macro (BCB): atualizando…")
        logs.append("macro:start")
        try:
            import macro_bcb_ingest  # noqa
            macro_bcb_ingest.run(engine, progress_cb=lambda s: logs.append(f"MACRO:{s}"))
            logs.append("macro:ok")
        except Exception as e:
            logs.append(f"macro:error:{e}")
            raise

        # -------- Métricas --------
        _p(80, "Métricas: recalculando…")
        logs.append("metrics:start")
        try:
            import finance_metrics_builder  # noqa
            finance_metrics_builder.run(engine, progress_cb=lambda s: logs.append(f"METRICS:{s}"))
            logs.append("metrics:ok")
        except Exception as e:
            logs.append(f"metrics:error:{e}")
            raise

        # -------- Score --------
        _p(92, "Fundamental score: recalculando…")
        logs.append("score:start")
        try:
            import fundamental_scoring  # noqa
            fundamental_scoring.run(engine, progress_cb=lambda s: logs.append(f"SCORE:{s}"))
            logs.append("score:ok")
        except Exception as e:
            logs.append(f"score:error:{e}")
            raise

        # -------- last_year (melhor esforço) --------
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text("select max(extract(year from data))::int as y from cvm.demonstracoes_financeiras")
                ).mappings().first()
            if row and row.get("y"):
                last_year = int(row["y"])
        except Exception:
            # não derruba o sync por isso
            pass

        _p(100, "Concluído.")
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
