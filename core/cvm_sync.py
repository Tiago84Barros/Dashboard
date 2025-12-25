from __future__ import annotations

import datetime as dt
import traceback
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
                # Limita para evitar erro de payload/banco
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

    - DFP (anual)    -> cvm.cvm_dfp_ingest.run
    - ITR (tri)      -> cvm.cvm_tri_ingest.run
    - (Opcional) Setores -> cvm.setores_ingest.run
    - Macro (BCB)    -> cvm.macro_bcb_ingest.run
    - Metrics        -> cvm.finance_metrics_builder.run
    - Score          -> cvm.fundamental_scoring.run

    Importante: imports são lazy (dentro da função) para não quebrar a página Configurações.
    """
    if end_year is None:
        end_year = dt.datetime.now().year

    def _p(pct: float, msg: str) -> None:
        if progress_cb:
            progress_cb(float(pct), str(msg))

    # Mantém logs em memória para gravar em cvm.sync_log
    logs: list[str] = []
    last_year: Optional[int] = None
    remote_latest_year: Optional[int] = int(end_year)

    def _log(msg: str) -> None:
        # garante string
        logs.append(str(msg))

    def _tail(n: int = 120) -> str:
        # junta e devolve só o final
        return " | ".join(logs[-n:])

    def _run_step(
        step_name: str,
        pct_start: float,
        pct_end: float,
        fn: Callable[[], None],
    ) -> None:
        """
        Executa um passo com logging e captura de traceback completo.
        O traceback completo vai para logs (e portanto para o sync_log no banco).
        Na UI, mostramos a mensagem curta e uma instrução para olhar o log.
        """
        _p(pct_start, f"{step_name}: executando…")
        _log(f"{step_name}:start")

        try:
            fn()
            _log(f"{step_name}:ok")
            _p(pct_end, f"{step_name}: concluído.")
        except Exception as e:
            tb = traceback.format_exc()
            _log(f"{step_name}:error:{repr(e)}")
            _log(f"{step_name}:traceback:{tb}")

            # Mensagem curta para UI (evita texto gigante na tela)
            _p(pct_end, f"{step_name}: ERRO -> {e}. Veja detalhes no log (cvm.sync_log).")

            # Re-raise para manter o comportamento atual (tela mostra “Falha ao atualizar: …”)
            raise

    engine = get_engine()
    _ensure_sync_log(engine)

    try:
        _p(2, "Iniciando sincronização…")
        _log("start")

        # -------- DFP --------
        def _dfp():
            import cvm.cvm_dfp_ingest as cvm_dfp_ingest

            cvm_dfp_ingest.run(
                engine,
                progress_cb=lambda s: _log(f"DFP:{s}"),
                start_year=int(start_year),
                end_year=int(end_year),
                years_per_run=int(years_per_run),
            )

        _run_step("DFP (anual)", 10, 28, _dfp)

        # -------- ITR --------
        def _itr():
            import cvm.cvm_tri_ingest as cvm_tri_ingest

            cvm_tri_ingest.run(
                engine,
                progress_cb=lambda s: _log(f"ITR:{s}"),
                start_year=int(start_year),
                end_year=int(end_year),
                quarters_per_run=int(quarters_per_run),
            )

        _run_step("ITR (trimestral)", 30, 48, _itr)

        # -------- Setores --------
        def _setores():
            import cvm.setores_ingest as setores_ingest

            setores_ingest.run(engine, progress_cb=lambda s: _log(f"SETORES:{s}"))

        # Setores é opcional: se o módulo não existir, apenas pula
        try:
            _run_step("Setores", 50, 62, _setores)
        except ModuleNotFoundError:
            _log("Setores:skip (módulo cvm.setores_ingest não encontrado)")
            _p(62, "Setores: pulado (módulo não encontrado).")

        # -------- Macro --------
        def _macro():
            import cvm.macro_bcb_ingest as macro_bcb_ingest

            macro_bcb_ingest.run(engine, progress_cb=lambda s: _log(f"MACRO:{s}"))

        _run_step("Macro (BCB)", 65, 76, _macro)

        # -------- Métricas --------
        def _metrics():
            import cvm.finance_metrics_builder as finance_metrics_builder

            finance_metrics_builder.run(engine, progress_cb=lambda s: _log(f"METRICS:{s}"))

        _run_step("Métricas", 80, 90, _metrics)

        # -------- Score --------
        def _score():
            import cvm.fundamental_scoring as fundamental_scoring

            fundamental_scoring.run(engine, progress_cb=lambda s: _log(f"SCORE:{s}"))

        _run_step("Fundamental score", 92, 98, _score)

        # -------- last_year (melhor esforço) --------
        # OBS: seu código consultava cvm.demonstracoes_financeiras (não existe).
        # Aqui tentamos DFP e TRI.
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        """
                        select
                          greatest(
                            coalesce((select max(extract(year from data))::int from cvm.demonstracoes_financeiras_dfp), 0),
                            coalesce((select max(extract(year from data))::int from cvm.demonstracoes_financeiras_tri), 0)
                          ) as y
                        """
                    )
                ).mappings().first()
            if row and row.get("y"):
                y = int(row["y"])
                if y > 0:
                    last_year = y
        except Exception as e:
            _log(f"last_year:warn:{e}")

        _p(100, "Concluído.")
        _insert_sync_log(
            engine,
            status="success",
            last_year=last_year,
            remote_latest_year=remote_latest_year,
            message=_tail(120),
        )

    except Exception as e:
        # grava erro com cauda + traceback (se já capturado nos passos)
        _insert_sync_log(
            engine,
            status="error",
            last_year=last_year,
            remote_latest_year=remote_latest_year,
            message=f"{e} | logs: " + _tail(120),
        )
        raise
