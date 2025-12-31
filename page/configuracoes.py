from __future__ import annotations

import importlib
import importlib.util
import pathlib
import sys
import traceback

import streamlit as st


def _ensure_project_root_on_path() -> None:
    """
    Garante que o diretório raiz do projeto esteja no sys.path.

    Motivo: os scripts de carga (ex.: pickup/dados_cvm_dfp.py) normalmente ficam
    fora do diretório do dashboard.
    """
    dashboard_dir = pathlib.Path(__file__).resolve().parents[1]  # .../Dashboard-Modulos
    project_root = dashboard_dir.parent
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))


def _run_job(module_path: str, fn_name: str = "main") -> None:
    """
    Importa e executa uma função 'main' de um módulo.

    Primeiro tenta import normal (pickup como package). Se não existir, tenta
    carregar direto do arquivo pickup/dados_cvm_dfp.py.
    """
    _ensure_project_root_on_path()

    try:
        mod = importlib.import_module(module_path)
    except ModuleNotFoundError:
        dashboard_dir = pathlib.Path(__file__).resolve().parents[1]
        project_root = dashboard_dir.parent

        rel_parts = module_path.split(".")
        py_path = project_root.joinpath(*rel_parts).with_suffix(".py")

        if not py_path.exists():
            raise

        spec = importlib.util.spec_from_file_location(module_path, str(py_path))
        if spec is None or spec.loader is None:
            raise ImportError(f"Não foi possível carregar o módulo pelo caminho: {py_path}")

        mod = importlib.util.module_from_spec(spec)
        sys.modules[module_path] = mod
        spec.loader.exec_module(mod)

    fn = getattr(mod, fn_name, None)
    if not callable(fn):
        raise AttributeError(f"Função {fn_name}() não encontrada em {module_path}")

    fn()


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

    if "job_dfp_running" not in st.session_state:
        st.session_state["job_dfp_running"] = False

    col1, col2 = st.columns([1, 2], gap="large")
    with col1:
        run_dfp = st.button(
            "Atualizar Demonstrações Completas (DFP)",
            use_container_width=True,
            disabled=st.session_state["job_dfp_running"],
        )

    with col2:
        st.info(
            "Este botão executa o script **pickup/dados_cvm_dfp.py** para baixar os DFP da CVM, "
            "consolidar e gravar em **public.Demonstracoes_Financeiras** no Supabase.\n\n"
            "Requisitos: a variável de ambiente **SUPABASE_DB_URL** deve estar configurada no ambiente "
            "do Streamlit, e o arquivo **cvm_to_ticker.csv** deve existir conforme o seu pipeline."
        )

    if run_dfp:
        st.session_state["job_dfp_running"] = True
        try:
            with st.spinner("Executando carga DFP (pode demorar alguns minutos)..."):
                _run_job("pickup.dados_cvm_dfp", "main")

            st.success("Carga DFP concluída. Atualizando cache do dashboard...")

            try:
                st.cache_data.clear()
            except Exception:
                pass

        except Exception as e:
            st.error("Falha ao executar a carga DFP.")
            st.code("".join(traceback.format_exception(type(e), e, e.__traceback__)))
        finally:
            st.session_state["job_dfp_running"] = False

        st.rerun()
