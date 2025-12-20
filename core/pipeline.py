import importlib
from typing import Callable, Optional

# Ajuste aqui se sua pasta de ingest estiver em outro namespace.
# Ex.: se seus ingests estão em "cvm/" então "cvm.cvm_dfp_ingest" está correto.
PRIORITY = [
    "cvm.cvm_dfp_ingest",
    "cvm.cvm_itr_ingest",
    "core.macro_bcb_ingest",
    "core.fundamental_scoring",
    "core.portfolio_backtest",
]


def _call_module(module_name: str, engine):
    """
    Convenção:
      - se existir run(engine) -> chama
      - senão se existir main(engine) -> chama
      - senão se existir main() -> chama
    """
    mod = importlib.import_module(module_name)

    if hasattr(mod, "run"):
        return mod.run(engine)

    if hasattr(mod, "main"):
        try:
            return mod.main(engine)  # type: ignore[misc]
        except TypeError:
            return mod.main()  # type: ignore[misc]

    raise AttributeError(f"Módulo {module_name} não possui run(engine) nem main().")


def run_all(engine, progress_cb: Optional[Callable[[str], None]] = None):
    total = len(PRIORITY)

    for i, name in enumerate(PRIORITY, start=1):
        if progress_cb:
            progress_cb(f"STEP {i}/{total} :: {name}")
        _call_module(name, engine)

    if progress_cb:
        progress_cb("DONE")
