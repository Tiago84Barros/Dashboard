# page/configuracoes.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, List, Tuple

import streamlit as st

# ============================================================
# Compatibilidade máxima: o dashboard pode chamar render(), run(),
# main() ou show(). Vamos expor todos.
# ============================================================


@dataclass
class StepResult:
    name: str
    ok: bool
    message: str


def _get_engine_safe():
    """
    Tenta obter o engine do Supabase do mesmo lugar que o resto do projeto.
    Ajuste aqui se o seu caminho for diferente.
    """
    try:
        from core.db_supabase import get_engine  # padrão do seu projeto
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
        log(f"❌ {step_name}: ERRO -> {e}")
        return StepResult(step_name, False, str(e))


def _resolve_runner(module_path: str):
    """
    Importa dinamicamente um módulo e tenta achar uma função executável.
    Prioridade: run(engine, progress_cb=...)
    """
    import importlib

    mod = importlib.import_module(module_path)

    if hasattr(mod, "run") and callable(getattr(mod, "run")):
        return getattr(mod, "run")

    raise AttributeError(f"Módulo '{module_path}' não expõe função run(engine, progress_cb=...).")


def _run_all_updates(
    engine,
    *,
    ano_inicial: int,
    ano_final: int,
    dfp_por_clique_anos: int,
    itr_por_clique_trimestres: int,
    log: Callable[[str], None],
):
    """
    Orquestrador único: CVM + Macro.
    Ajuste os módulos abaixo conforme o nome real no seu repo.
    """

    steps: List[Tuple[str, Callable[[], None]]] = []

    # -----------------------------
    # CVM (DFP/ITR) - usa core.cvm_sync.apply_update ou módulos dedicados
    # -----------------------------
    def step_cvm():
        # Se você tiver um orquestrador pronto:
        try:
            from core.cvm_sync import apply_update  # já apareceu no seu dashboard.py
        except Exception as e:
            raise RuntimeError(f"Não consegui importar core.cvm_sync.apply_update: {e}")

        # apply_update(engine, ano_inicial, ano_final, ...) => depende da sua assinatura real.
        # Para não quebrar, tentamos chamar de forma flexível.
        try:
            # caso mais comum
            apply_update(
                engine,
                ano_inicial=ano_inicial,
                ano_final=ano_final,
                dfp_por_clique_anos=dfp_por_clique_anos,
                itr_por_clique_trimestres=itr_por_clique_trimestres,
                progress_cb=log,
            )
        except TypeError:
            # fallback: assinatura diferente
            apply_update(engine)  # e deixe o módulo lidar com defaults

    steps.append(("Atualizar CVM (DFP/ITR)", step_cvm))

    # -----------------------------
    # Macro RAW (BCB -> cvm.macro_bcb)
    # -----------------------------
    def step_macro_raw():
        runner = _resolve_runner("core.macro_bcb_raw_ingest")  # ajuste se estiver em outro path
        runner(engine, progress_cb=log)

    steps.append(("Atualizar Macro RAW (BCB -> cvm.macro_bcb)", step_macro_raw))

    # -----------------------------
    # Macro Analítico (gera info_economica / info_economica_mensal)
    # -----------------------------
    def step_macro_analitico():
        runner = _resolve_runner("core.macro_bcb_ingest")  # ajuste se estiver em outro path
        runner(engine, progress_cb=log)

    steps.append(("Gerar tabelas analíticas (info_economica / info_economica_mensal)", step_macro_analitico))

    # -----------------------------
    # Executa as etapas com auditoria
    # -----------------------------
    results: List[StepResult] = []
    for name, fn in steps:
        results.append(_call_step(name, fn, log))

    return results


def render():
    st.title("Configurações")

    st.caption(
        "Esta página é o orquestrador único de sincronização do Supabase. "
        "Ela executa CVM e Macro em sequência e exibe auditoria de cada etapa."
    )

    engine = _get_engine_safe()
    if engine is None:
        st.stop()

    # -----------------------------
    # Parâmetros (mantive o que você já usava)
    # -----------------------------
    c1, c2 = st.columns(2)
    with c1:
        ano_inicial = st.number_input("Ano inicial", min_value=1990, max_value=2100, value=2010, step=1)
        dfp_por_clique_anos = st.number_input("DFP por clique (anos)", min_value=1, max_value=10, value=1, step=1)

    with c2:
        ano_final = st.number_input("Ano final", min_value=1990, max_value=2100, value=2025, step=1)
        itr_por_clique_trimestres = st.number_input(
            "ITR por clique (trimestres)", min_value=1, max_value=20, value=1, step=1
        )

    st.divider()

    # -----------------------------
    # Auditoria: log em tela
    # -----------------------------
    if "cfg_logs" not in st.session_state:
        st.session_state.cfg_logs = []

    def log(msg: str):
        st.session_state.cfg_logs.append(msg)

    # -----------------------------
    # Botão ÚNICO (como você pediu)
    # -----------------------------
    col_btn, col_clear = st.columns([3, 1])
    with col_btn:
        clicked = st.button("Atualizar banco (CVM + Macro)", use_container_width=True)

    with col_clear:
        if st.button("Limpar logs", use_container_width=True):
            st.session_state.cfg_logs = []

    # Área de log sempre visível
    st.subheader("Auditoria (passo a passo)")
    st.code("\n".join(st.session_state.cfg_logs[-300:]) if st.session_state.cfg_logs else "Aguardando...")

    if clicked:
        st.session_state.cfg_logs = []  # reinicia auditoria a cada execução
        log("Iniciando atualização completa (CVM + Macro)...")

        with st.spinner("Executando etapas..."):
            results = _run_all_updates(
                engine,
                ano_inicial=int(ano_inicial),
                ano_final=int(ano_final),
                dfp_por_clique_anos=int(dfp_por_clique_anos),
                itr_por_clique_trimestres=int(itr_por_clique_trimestres),
                log=log,
            )

        ok_all = all(r.ok for r in results)
        st.divider()
        st.subheader("Resumo")

        for r in results:
            if r.ok:
                st.success(f"{r.name}: OK")
            else:
                st.error(f"{r.name}: FALHOU — {r.message}")

        if ok_all:
            st.success("Atualização completa finalizada com sucesso.")
        else:
            st.error("Atualização completa finalizada com falhas. Veja a auditoria acima.")


# ============================================================
# Aliases de compatibilidade (para qualquer loader antigo)
# ============================================================
def run(*args, **kwargs):
    return render()


def main(*args, **kwargs):
    return render()


def show(*args, **kwargs):
    return render()
