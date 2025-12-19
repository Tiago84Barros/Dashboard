"""
dashboard.py
~~~~~~~~~~~~
Script principal da aplicação Streamlit.

Execute com:
    streamlit run dashboard.py
"""

from __future__ import annotations

import importlib
import logging
import pathlib
import sys
from typing import Callable

import streamlit as st

from core.db_supabase import get_engine
from core.cvm_sync import get_sync_status, apply_update

engine = get_engine()
logger = logging.getLogger(__name__)


# ───────────────────────── Ajuste de path ──────────────────────────
ROOT_DIR = pathlib.Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))


# ───────────────────────── Imports com fallback ─────────────────────
def _import_first(*module_paths: str):
    """
    Tenta importar o primeiro módulo disponível na lista.
    Retorna o módulo importado ou levanta ImportError com detalhes.
    """
    errors = []
    for p in module_paths:
        try:
            return importlib.import_module(p)
        except Exception as e:
            errors.append((p, e))
    msg = "Falha ao importar módulos. Tentativas:\n" + "\n".join([f"- {p}: {repr(e)}" for p, e in errors])
    raise ImportError(msg)


def _get_layout_funcs() -> tuple[Callable[[], None], Callable[[], None]]:
    """
    Busca configurar_pagina() e aplicar_estilos_css() em:
    - design.layout (estrutura modular)
    - layout (arquivo solto)
    Se não existir, usa fallback minimalista.
    """
    try:
        mod = _import_first("design.layout", "layout")
        configurar_pagina = getattr(mod, "configurar_pagina", None)
        aplicar_estilos_css = getattr(mod, "aplicar_estilos_css", None)
        if callable(configurar_pagina) and callable(aplicar_estilos_css):
            return configurar_pagina, aplicar_estilos_css
    except Exception:
        pass

    # fallback seguro
    def _fallback_config():
        try:
            st.set_page_config(
                page_title="Dashboard Fundamentalista",
                layout="wide",
                initial_sidebar_state="expanded",
            )
        except Exception:
            # set_page_config só pode ser chamado uma vez; ignora se já foi chamado.
            pass

    def _fallback_css():
        st.markdown(
            """
            <style>
              .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
              [data-testid="stSidebar"] { padding-top: 1rem; }
            </style>
            """,
            unsafe_allow_html=True,
        )

    return _fallback_config, _fallback_css


def _get_db_loader():
    """
    Obtém load_setores_from_db em:
    - core.db_loader
    - db_loader
    """
    mod = _import_first("core.db_loader", "db_loader")
    fn = getattr(mod, "load_setores_from_db", None)
    if not callable(fn):
        raise ImportError("load_setores_from_db não encontrado em core.db_loader/db_loader.")
    return fn


def _load_page_renderer(page_key: str) -> Callable[[], None]:
    """
    Carrega a função render() da página escolhida, com fallback de caminhos:
    - page.basic / basic
    - page.advanced / advanced
    - page.criacao_portfolio / criacao_portfolio
    """
    mapping = {
        "Básica": ("page.basic", "basic"),
        "Avançada": ("page.advanced", "advanced"),
        "Criação de Portfólio": ("page.criacao_portfolio", "criacao_portfolio"),
    }
    paths = mapping.get(page_key)
    if not paths:
        raise ValueError(f"Página desconhecida: {page_key}")

    mod = _import_first(*paths)
    render = getattr(mod, "render", None)
    if not callable(render):
        raise ImportError(f"Função render() não encontrada no módulo da página: {paths}")
    return render


# ───────────────────────── Layout Global ───────────────────────────
configurar_pagina, aplicar_estilos_css = _get_layout_funcs()
configurar_pagina()
aplicar_estilos_css()


# ───────────────────────── Cache inicial ───────────────────────────
def _ensure_setores_df() -> None:
    if "setores_df" in st.session_state and st.session_state["setores_df"] is not None:
        return
    load_setores_from_db = _get_db_loader()
    setores_df = load_setores_from_db()
    st.session_state["setores_df"] = setores_df


# ───────────────────────── Sidebar navegação ───────────────────────
with st.sidebar:
    st.markdown("## Análises")
    pagina_escolhida = st.radio(
        "Escolha a seção:",
        ["Básica", "Avançada", "Criação de Portfólio"],
        index=0,
    )

    st.markdown("---")

    # ───────────────────── Atualização CVM (Supabase) ─────────────────────
    st.markdown("## Atualização CVM")

    try:
        status = get_sync_status(engine)
        last_run = status.get("last_run")

        if last_run:
            st.success(f"Última atualização (UTC): {last_run}")
        else:
            st.warning("Nenhuma atualização executada ainda.")

        if st.button("Atualizar agora", use_container_width=True):
            placeholder = st.empty()

            def _progress(msg: str):
                placeholder.info(msg)

            try:
                apply_update(engine, progress_cb=_progress)

                # Garante que o app reflita os dados novos
                st.session_state.pop("setores_df", None)

                st.success("Atualização finalizada com sucesso.")
                st.rerun()
            except Exception as e:
                st.error(f"Falha ao atualizar CVM: {e}")
                logger.exception("Falha ao executar apply_update()", exc_info=e)

    except Exception as e:
        st.error(f"Não foi possível consultar status da CVM: {e}")
        logger.exception("Falha ao consultar get_sync_status()", exc_info=e)

    st.markdown("---")

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Recarregar cache", use_container_width=True):
            # limpa apenas o que é crítico aqui
            st.session_state.pop("setores_df", None)
            st.rerun()
    with col_b:
        if st.button("Diagnóstico", use_container_width=True):
            st.session_state["__show_diag__"] = True


# ───────────────────────── Diagnóstico leve ─────────────────────────
if st.session_state.get("__show_diag__"):
    st.session_state["__show_diag__"] = False
    with st.expander("Diagnóstico do App", expanded=True):
        st.write("Root dir:", str(ROOT_DIR))
        st.write("Python path contém root:", str(ROOT_DIR) in sys.path)
        try:
            _ensure_setores_df()
            s = st.session_state.get("setores_df")
            st.write("setores_df carregado:", (s is not None) and (getattr(s, "empty", True) is False))
            if s is not None and not getattr(s, "empty", True):
                st.write("Linhas/Colunas:", s.shape)
                st.write("Colunas:", list(s.columns))
        except Exception as e:
            st.error(f"Falha ao carregar setores_df: {e}")


# ───────────────────────── Execução / Roteamento ────────────────────
try:
    _ensure_setores_df()
except Exception as e:
    st.error(f"Falha ao inicializar dados base (setores_df): {e}")
    st.stop()

try:
    render_page = _load_page_renderer(pagina_escolhida)
    render_page()
except Exception as e:
    st.error("Falha ao carregar a página selecionada.")
    st.exception(e)

