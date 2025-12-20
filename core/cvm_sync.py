from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Optional

from core.sync_state import ensure_sync_table, get_state, set_state
from core.pipeline import run_all

KEY_LAST_RUN = "cvm:last_run_utc"

# Chaves de progresso (persistidas no banco)
KEY_RUN_ID = "cvm:run_id"
KEY_STARTED_AT = "cvm:started_at_utc"
KEY_STATUS = "cvm:status"              # idle | running | success | failed
KEY_STEP = "cvm:step"                  # texto da etapa atual
KEY_PROGRESS = "cvm:progress"          # 0..100 (int)
KEY_ERROR = "cvm:error"                # mensagem resumida (se falhar)

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_sync_status(engine):
    """
    Retorna o status de sincronização + progresso (para a UI fazer polling).
    """
    ensure_sync_table(engine)

    last = get_state(engine, KEY_LAST_RUN)
    status = get_state(engine, KEY_STATUS)
    step = get_state(engine, KEY_STEP)
    prog = get_state(engine, KEY_PROGRESS)
    started = get_state(engine, KEY_STARTED_AT)
    run_id = get_state(engine, KEY_RUN_ID)
    err = get_state(engine, KEY_ERROR)

    return {
        "last_run": None if not last else last["updated_at"],
        "last_value": None if not last else last["value"],

        "status": None if not status else status["value"],
        "step": None if not step else step["value"],
        "progress": None if not prog else prog["value"],
        "started_at": None if not started else started["value"],
        "run_id": None if not run_id else run_id["value"],
        "error": None if not err else err["value"],
    }

def apply_update(engine, progress_cb: Optional[Callable[[str], None]] = None):
    """
    Executa o pipeline de atualização (algoritmos 1,5,2,3,4,6),
    persistindo progresso no banco para permitir UI em "tempo real".
    """
    ensure_sync_table(engine)

    run_id = _utc_now_iso()
    set_state(engine, KEY_RUN_ID, run_id)
    set_state(engine, KEY_STARTED_AT, run_id)
    set_state(engine, KEY_STATUS, "running")
    set_state(engine, KEY_ERROR, "")
    set_state(engine, KEY_PROGRESS, 1)
    set_state(engine, KEY_STEP, "Iniciando atualização CVM...")

    # Ajuste simples: percentuais por etapa (melhor que travar em 8%)
    # Você pode refinar depois se o pipeline fornecer eventos mais ricos.
    step_map = {
        "cvm.cvm_dfp_ingest": 15,
        "cvm.cvm_itr_ingest": 55,
        "cvm.macro_bcb_ingest": 75,
        "finance_metrics_builder": 85,
        "fundamental_scoring": 92,
        "portfolio_backtest": 98,
    }

    def _persist(msg: str):
        # 1) callback para UI (se estiver usando)
        if progress_cb:
            progress_cb(msg)

        # 2) persistência no banco (para polling)
        # Normaliza msg e tenta detectar etapa
        msg_norm = (msg or "").strip()

        # Etapa: se vier "Executando X..." extrai X
        step_detected = None
        if "Executando" in msg_norm:
            # exemplos: "Executando cvm.cvm_dfp_ingest..."
            parts = msg_norm.replace("...", "").split()
            # tenta achar algo como cvm.cvm_dfp_ingest
            for p in parts:
                if "." in p and ("cvm_" in p or "cvm." in p or "builder" in p or "scoring" in p or "backtest" in p):
                    step_detected = p.strip()
                    break

        # Atualiza step
        if step_detected:
            set_state(engine, KEY_STEP, step_detected)
            # Atualiza % (estimado)
            for k, v in step_map.items():
                if k in step_detected:
                    set_state(engine, KEY_PROGRESS, v)
                    break
        else:
            # mantém uma mensagem humana, sem derrubar a etapa técnica
            set_state(engine, KEY_STEP, msg_norm[:200])

    try:
        _persist("Iniciando atualização CVM...")
        # IMPORTANTÍSSIMO: o run_all deve chamar progress_cb várias vezes.
        run_all(engine, progress_cb=_persist)

        # Finaliza
        set_state(engine, KEY_PROGRESS, 100)
        set_state(engine, KEY_STATUS, "success")
        set_state(engine, KEY_STEP, "Atualização concluída.")
        set_state(engine, KEY_LAST_RUN, _utc_now_iso())

        if progress_cb:
            progress_cb("Atualização concluída.")

    except Exception as e:
        set_state(engine, KEY_STATUS, "failed")
        set_state(engine, KEY_ERROR, str(e)[:500])
        set_state(engine, KEY_STEP, "Falha na atualização (ver erro).")
        # não força 100, deixa como estava
        if progress_cb:
            progress_cb(f"Falha na atualização: {e}")
        raise
