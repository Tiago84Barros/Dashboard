from __future__ import annotations

import importlib
import importlib.util
import io
import os
import pathlib
import runpy
import sys
import traceback
from contextlib import redirect_stdout, redirect_stderr

import streamlit as st


def _project_root() -> pathlib.Path:
    # .../Dashboard-Modulos/page/configuracoes.py -> .../Dashboard-Modulos -> (pai) = raiz do repo
    dashboard_dir = pathlib.Path(__file__).resolve().parents[1]
    return dashboard_dir.parent


def _ensure_project_root_on_path() -> None:
    root = _project_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def _resolve_module_py_path(module_path: str) -> pathlib.Path:
    root = _project_root()
    rel_parts = module_path.split(".")
    return root.joinpath(*rel_parts).with_suffix(".py")


def _run_module_main(module_path: str = "pickup.dados_cvm_dfp") -> dict:
    """
    Executa o módulo como se fosse `python -m pickup.dados_cvm_dfp`,
    mas dentro do mesmo processo, para podermos capturar logs e exceções.
    """
    _ensure_project_root_on_path()

    # Preferir run_module (equivalente a python -m)
    return runpy.run_module(module_path, run_name="__main__")


def render() -> None:
    st.header("Configurações")
    st.caption(
        "Use esta seção para executar rotinas de atualização das tabelas no Supabase. "
        "Nesta primeira etapa, validaremos apenas a carga de demonstrações anuais (DFP)."
    )

    st.subheader("Atualização de Base")
    st.write(
        "**Ordem recomendada (parcial):**\n"
        "1) Demonstrações completas (DFP/anual)\n\n"
        "Após validar a carga, adicionaremos os próximos botões e a lógica de ordem."
    )

    # =========================
    # PRÉ-CHECKS VISÍVEIS
    # =========================
    st.markdown("### Diagnóstico rápido do ambiente")

    _ensure_project_root_on_path()

    module_path = "pickup.dados_cvm_dfp"
    module_file = _resolve_module_py_path(module_path)

    supabase_db_url_ok = bool(os.getenv("SUPABASE_DB_URL"))
    ticker_path = os.getenv("TICKER_PATH")  # opcional
    default_ticker_path = _project_root() / "pickup" / "cvm_to_ticker.csv"
    # (se você usa outro caminho, o seu script pode esperar outro; isto é só diagnóstico)
    ticker_file_exists = default_ticker_path.exists()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.write("SUPABASE_DB_URL:", "OK" if supabase_db_url_ok else "NÃO DEFINIDA")
    with c2:
        st.write("Arquivo do módulo:", "OK" if module_file.exists() else f"NÃO ENCONTRADO ({module_file})")
    with c3:
        st.write("cvm_to_ticker.csv (default):", "OK" if ticker_file_exists else f"NÃO ENCONTRADO ({default_ticker_path})")

    if ticker_path:
        st.write("TICKER_PATH (env):", ticker_path)

    st.divider()

    # =========================
    # BOTÃO
    # =========================
    st.subheader("Executar carga")

    if "job_dfp_running" not in st.session_state:
        st.session_state["job_dfp_running"] = False

    run_dfp = st.button(
        "Atualizar Demonstrações Completas (DFP)",
        use_container_width=True,
        disabled=st.session_state["job_dfp_running"],
    )

    # Área dedicada a logs
    st.markdown("### Logs da execução")
    log_placeholder = st.empty()

    # “Reset” para destravar
    with st.expander("Ações de manutenção", expanded=False):
        if st.button("Resetar trava do botão (job_dfp_running)"):
            st.session_state["job_dfp_running"] = False
            st.success("Trava resetada.")
            st.rerun()

    if run_dfp:
        st.session_state["job_dfp_running"] = True

        # validações antes de executar (mostrando claramente)
        if not supabase_db_url_ok:
            st.error("SUPABASE_DB_URL não está definida no ambiente do Streamlit. Configure em Secrets/Env Vars.")
            st.session_state["job_dfp_running"] = False
            st.stop()

        if not module_file.exists():
            st.error(f"Não encontrei o módulo {module_path}. Esperado em: {module_file}")
            st.session_state["job_dfp_running"] = False
            st.stop()

        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()

        try:
            with st.status("Executando carga DFP (isso pode demorar)...", expanded=True) as status:
                status.write("Iniciando execução do módulo…")
                status.write(f"Módulo: `{module_path}`")
                status.write("Capturando stdout/stderr do processo…")

                with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
                    _run_module_main(module_path)

                status.update(label="Execução finalizada.", state="complete")

            # Mostra logs capturados
            out = stdout_buf.getvalue().strip()
            err = stderr_buf.getvalue().strip()

            if out:
                log_placeholder.code(out, language="text")
            else:
                log_placeholder.info("Nenhum log foi produzido em stdout.")

            if err:
                st.warning("Houve saída em stderr (avisos/erros):")
                st.code(err, language="text")

            st.success("Carga DFP concluída (sem exceção Python).")

            # Opcional: limpar cache, se você usa cache_data em leituras do banco
            try:
                st.cache_data.clear()
            except Exception:
                pass

        except Exception as e:
            st.error("Falha ao executar a carga DFP. Traceback completo abaixo:")
            st.code("".join(traceback.format_exception(type(e), e, e.__traceback__)), language="text")

            # também exibe o que foi capturado até o momento do erro
            out = stdout_buf.getvalue().strip()
            err = stderr_buf.getvalue().strip()
            if out:
                st.markdown("#### stdout (até o erro)")
                st.code(out, language="text")
            if err:
                st.markdown("#### stderr (até o erro)")
                st.code(err, language="text")

        finally:
            st.session_state["job_dfp_running"] = False


# Compatibilidade caso seu loader chame `configuracoes()`
def configuracoes():
    render()
