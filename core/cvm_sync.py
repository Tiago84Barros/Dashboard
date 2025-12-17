from datetime import datetime, timezone
from core.sync_state import ensure_sync_table, get_state, set_state
from core.pipeline import run_all

KEY_LAST_RUN = "cvm:last_run_utc"

def get_sync_status(engine):
    ensure_sync_table(engine)
    last = get_state(engine, KEY_LAST_RUN)
    return {
        "last_run": None if not last else last["updated_at"],
        "last_value": None if not last else last["value"],
    }

def apply_update(engine, progress_cb=None):
    """
    Executa o pipeline de atualização (algoritmos 1,5,2,3,4,6).
    """
    ensure_sync_table(engine)

    def _p(msg):
        if progress_cb:
            progress_cb(msg)

    _p("Iniciando atualização CVM...")
    run_all(engine, progress_cb=_p)

    now = datetime.now(timezone.utc).isoformat()
    set_state(engine, KEY_LAST_RUN, now)
    _p("Atualização concluída.")
