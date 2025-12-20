from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Optional

from core.sync_state import ensure_sync_table, get_state, set_state
from core.pipeline import run_all

# Chaves persistidas no banco (cvm.sync_state)
KEY_LAST_RUN = "cvm:last_run_utc"
KEY_LAST_SUCCESS = "cvm:last_success_utc"
KEY_LAST_ERROR = "cvm:last_error"
KEY_STARTED_AT = "cvm:started_at_utc"
KEY_PROGRESS_PCT = "cvm:progress_pct"
KEY_STAGE = "cvm:stage"
KEY_MESSAGE = "cvm:message"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_sync_status(engine) -> dict:
    """
    Retorna um snapshot do estado de sincronização persistido no banco.
    """
    ensure_sync_table(engine)

    def _val(key: str):
        row = get_state(engine, key)
        return None if not row else row.get("value")

    return {
        "started_at": _val(KEY_STARTED_AT),
        "progress_pct": _val(KEY_PROGRESS_PCT),
        "stage": _val(KEY_STAGE),
        "message": _val(KEY_MESSAGE),
        "last_run": _val(KEY_LAST_RUN),
        "last_success": _val(KEY_LAST_SUCCESS),
        "last_error": _val(KEY_LAST_ERROR),
    }


def apply_update(engine, progress_cb: Optional[Callable[[str], None]] = None) -> None:
    """
    Executa o pipeline e grava progresso no banco (cvm.sync_state) para o dashboard
    conseguir mostrar atualização "em tempo real".
    """
    ensure_sync_table(engine)

    # inicializa estado
    set_state(engine, KEY_STARTED_AT, _utc_now_iso())
    set_state(engine, KEY_PROGRESS_PCT, "0")
    set_state(engine, KEY_STAGE, "Inicializando")
    set_state(engine, KEY_MESSAGE, "Iniciando atualização CVM...")
    set_state(engine, KEY_LAST_ERROR, "")  # limpa erro anterior
    set_state(engine, KEY_LAST_RUN, _utc_now_iso())

    def _emit(msg: str) -> None:
        # callback opcional (UI local)
        if progress_cb:
            progress_cb(msg)

        # Persiste mensagens e tenta inferir progresso por etapa (STEP i/n)
        set_state(engine, KEY_MESSAGE, msg)

        if msg.startswith("STEP "):
            # formato: STEP i/n :: module.name
            try:
                head, rest = msg.split("::", 1)
                frac = head.replace("STEP", "").strip()  # "i/n"
                i_s, n_s = frac.split("/", 1)
                i = int(i_s.strip())
                n = int(n_s.strip())
                pct = int(round((i / max(n, 1)) * 100))
                set_state(engine, KEY_PROGRESS_PCT, str(pct))
                set_state(engine, KEY_STAGE, rest.strip())
            except Exception:
                # se der ruim no parse, pelo menos atualiza stage
                set_state(engine, KEY_STAGE, msg)

        elif msg == "DONE":
            set_state(engine, KEY_PROGRESS_PCT, "100")
            set_state(engine, KEY_STAGE, "Concluído")

    try:
        _emit("Iniciando atualização CVM...")
        run_all(engine, progress_cb=_emit)

        now = _utc_now_iso()
        set_state(engine, KEY_LAST_RUN, now)
        set_state(engine, KEY_LAST_SUCCESS, now)
        set_state(engine, KEY_STAGE, "Concluído")
        set_state(engine, KEY_PROGRESS_PCT, "100")
        set_state(engine, KEY_MESSAGE, "Atualização concluída.")
        _emit("DONE")

    except Exception as e:
        # registra erro para o dashboard exibir
        set_state(engine, KEY_LAST_ERROR, f"{type(e).__name__}: {e}")
        set_state(engine, KEY_STAGE, "Erro")
        set_state(engine, KEY_MESSAGE, "Falha na atualização.")
        raise
