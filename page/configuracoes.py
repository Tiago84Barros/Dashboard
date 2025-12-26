# page/configuracoes.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, List, Tuple

import streamlit as st
from sqlalchemy import text


@dataclass
class StepResult:
    name: str
    ok: bool
    message: str


def _get_engine_safe():
    try:
        from core.db_supabase import get_engine
        return get_engine()
    except Exception as e:
        st.error(f"Falha ao importar/get_engine() de core.db_supabase: {e}")
        return None


def _call_step(step_name: str, fn: Callable[[], None], log: Callable[[str], None]) -> StepResult:
    try:
        log(f"▶ {step_name}: iniciando")
        fn()
        log(f"✅ {step_name}: concluído")
        return StepResult(step_name, True, "OK")
    except Exception as e:
        log(f"❌ {step_name}: ERRO -> {repr(e)}")
        return StepResult(step_name, False, repr(e))


def _resolve_runner(module_candidates: List[str]):
    """
    Tenta importar um módulo em múltiplos caminhos e retorna a função run(engine, progress_cb=?).
    Isso resolve o seu erro: "No module named core.macro_bcb_raw_ingest".
    """
    import importlib

    last_err = None
    for mod_path in module_candidates:
        try:
            mod = importlib.import_module(mod_path)
            if hasattr(mod, "run") and callable(getattr(mod, "run")):
                return mod_path, getattr(mod, "run")
            raise AttributeError(f"Módulo '{mod_path}' importou, mas não tem função run().")
        except Exception as e:
            last_err = e

    raise ModuleNotFoundError(
        f"Não consegui importar nenhum dos módulos: {module_candidates}. Último erro: {last_err}"
    )


def _sql_count_macro_raw(engine) -> str:
    # Mostra o que realmente foi gravado em cvm.macro_bcb
    q = """
    select series_name,
           count(*) as n_linhas,
           count(valor) as n_valor,
           min(data) as data_min,
           max(data) as data_max
    from cvm.macro_bcb
    group by series_name
    order by n_linhas desc;
    """
    with engine.begin() as conn:
        rows = conn.execute(text(q)).fetchall()
    if not rows:
        return "cvm.macro_bcb: sem dados."
    lines = ["cvm.macro_bcb (por série):"]
    for r in rows:
        lines.append(
            f"- {r[0]} | linhas={r[1]} | valor={r[2]} | {r[3]} -> {r[4]}"
        )
    return "\n".join(lines)


def _sql_count_info_economica(engine, table_full: str) -> str:
    # Auditoria das tabelas analíticas
    q = f"""
    select
      count(*) as linhas,
      count(selic) as selic_ok,
      count(cambio) as cambio_ok,
      count(ipca) as ipca_ok,
      count(icc) as icc_ok,
      count(pib) as pib_ok,
      count(balanca_comercial) as balanca_ok,
      min(data) as data_min,
      max(data) as data_max
    from {table_full};
    """
    with engine.begin() as conn:
        r = conn.execute(text(q)).fetchone()
    if not r:
        return f"{table_full}: sem dados."
    return (
        f"{table_full}: linhas={r[0]} | selic={r[1]} | cambio={r[2]} | ipca={r[3]} | "
        f"icc={r[4]} | pib={r[5]} | balanca={r[6]} | {r[7]} -> {r[8]}"
    )


def render():
    st.title("Configurações — Auditoria Macro (BCB)")

    st.caption(
        "Esta tela audita exclusivamente o pipeline de Macro: "
        "1) RAW (SGS -> cvm.macro_bcb)  2) Analítico (-> info_economica / info_economica_mensal)."
    )

    engine = _get_engine_safe()
    if engine is None:
        st.stop()

    # Logs
    if "cfg_logs" not in st.session_state:
        st.session_state.cfg_logs = []

    def log(msg: str):
        st.session_state.cfg_logs.append(msg)

    # Botões
    c1, c2 = st.columns([3, 1])
    with c1:
        clicked = st.button("Atualizar Macro (BCB) — Auditoria Completa", use_container_width=True)
    with c2:
        if st.button("Limpar logs", use_container_width=True):
            st.session_state.cfg_logs = []

    # Área de auditoria
    st.subheader("Auditoria (passo a passo)")
    st.code("\n".join(st.session_state.cfg_logs[-400:]) if st.session_state.cfg_logs else "Aguardando...")

    if not clicked:
        return

    st.session_state.cfg_logs = []
    log("Macro: início da auditoria completa.")

    # Resolve módulos (tenta raiz e core/)
    raw_candidates = [
        "macro_bcb_raw_ingest",
        "core.macro_bcb_raw_ingest",
    ]
    analitico_candidates = [
        "macro_bcb_ingest",
        "core.macro_bcb_ingest",
    ]

    def step_imports():
        mod_path, _ = _resolve_runner(raw_candidates)
        log(f"Import OK (RAW): {mod_path}")
        mod_path2, _ = _resolve_runner(analitico_candidates)
        log(f"Import OK (ANALÍTICO): {mod_path2}")

    def step_raw():
        mod_path, runner = _resolve_runner(raw_candidates)
        log(f"Executando RAW via: {mod_path}.run(engine, progress_cb=log)")
        runner(engine, progress_cb=log)
        log(_sql_count_macro_raw(engine))

    def step_analitico():
        mod_path, runner = _resolve_runner(analitico_candidates)
        log(f"Executando ANALÍTICO via: {mod_path}.run(engine, progress_cb=log)")
        runner(engine, progress_cb=log)

        # Auditoria das duas tabelas (se existirem)
        try:
            log(_sql_count_info_economica(engine, "cvm.info_economica"))
        except Exception as e:
            log(f"⚠ Não consegui auditar cvm.info_economica: {repr(e)}")
        try:
            log(_sql_count_info_economica(engine, "cvm.info_economica_mensal"))
        except Exception as e:
            log(f"⚠ Não consegui auditar cvm.info_economica_mensal: {repr(e)}")

    steps: List[Tuple[str, Callable[[], None]]] = [
        ("Validar imports Macro", step_imports),
        ("Ingest RAW (SGS -> cvm.macro_bcb)", step_raw),
        ("Gerar Analítico (-> info_economica / info_economica_mensal)", step_analitico),
    ]

    results: List[StepResult] = []
    with st.spinner("Executando Macro com auditoria..."):
        for name, fn in steps:
            results.append(_call_step(name, fn, log))

    st.divider()
    st.subheader("Resumo (Macro)")
    ok_all = all(r.ok for r in results)
    for r in results:
        if r.ok:
            st.success(f"{r.name}: OK")
        else:
            st.error(f"{r.name}: FALHOU — {r.message}")

    if ok_all:
        st.success("Macro finalizado com sucesso.")
    else:
        st.error("Macro finalizado com falhas. Veja a auditoria acima.")


# Aliases de compatibilidade com loaders antigos
def run(*args, **kwargs):
    return render()


def main(*args, **kwargs):
    return render()


def show(*args, **kwargs):
    return render()
