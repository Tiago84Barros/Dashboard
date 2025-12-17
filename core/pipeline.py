import importlib

PRIORITY = ["cvm.cvm_dfp_ingest", "cvm.cvm_itr_ingest", "finance_metrics_builder", "macro_bcb_ingest", "fundamental_scoring", "algoritmo6"]

def _call_module(module_name: str, engine):
    """
    Convenção:
    - Se existir run(engine) -> chama
    - senão se existir main(engine) -> chama
    - senão se existir main() -> chama
    """
    mod = importlib.import_module(module_name)

    if hasattr(mod, "run"):
        return mod.run(engine)
    if hasattr(mod, "main"):
        try:
            return mod.main(engine)
        except TypeError:
            return mod.main()

    raise AttributeError(f"Módulo {module_name} não possui run(engine) nem main().")

def run_all(engine, progress_cb=None):
    """
    Executa os algoritmos na prioridade definida.
    progress_cb: função opcional progress_cb(msg: str)
    """
    for name in PRIORITY:
        if progress_cb:
            progress_cb(f"Executando {name}...")
        _call_module(name, engine)
    if progress_cb:
        progress_cb("Pipeline finalizado.")
