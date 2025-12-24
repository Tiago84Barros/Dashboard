# dashboard.py
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

from core.db.engine import get_engine

logger = logging.getLogger(__name__)

ROOT_DIR = pathlib.Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))


@st.cache_resource
def _engine():
    return get_engine()


def _import_first(*module_paths: str):
    errors = []
    for p in module_paths:
        try:
            return importlib.import_module(p)
        except Exception as e:
            errors.append((p, e))
    msg = "Falha ao importar módulos. Tentativas:\n" + "\n".join([f"- {p}: {repr(e)}" for p, e in errors])
    raise ImportError(msg)


def _get_layout_funcs() -> tuple[Callable[[], None], Callable[[], None]]:
    try:
        mod = _import_first("design.layout", "layout")
        configurar_pagina = getattr(mod, "configurar_pagina", None)
        aplicar_estilos_css = getattr(mod, "aplicar_estilos_css", None)
        if callable(configurar_pagina) and callable(aplicar_estilos_css):
            return configurar_pagina, aplicar_estilos_css
    except Exception:
        pass

    def _fallback_config():
        try:
            st.set_page_config(
                page_title="Dashboard Fundamentalista",
                layout="wide",
                initial_sidebar_state="expanded",
            )
        except Exception:
            pass

    def _fallback_css():
        st.markdown(
            """
            <style>
              .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
              [data-testid="stSidebar"] { padding-top: 0.75rem; }
            </style>
            """,
            unsafe_allow_html=True,
        )

    return _fallback_config, _fallback_css


def _get_loader():
    mod = _import_first("core.db.loader", "loader")
    fn = getattr(mod, "load_setores", None)
    if not callable(fn):
        raise ImportError("load_setores não encontrado em core.db.loader/loader.")
    return fn


def _load_page_renderer(page_key: str) -> Callable[[], None]:
    mapping = {
        "Básica": ("page.basic", "basic"),
        "Avançada": ("page.advanced", "advanced"),
        "Criação de Portfólio": ("page.criacao_portfolio", "criacao_portfolio"),
        "Configurações": ("page.configuracoes", "configuracoes"),
    }
    paths = mapping.get(page_key)
    if not paths:
        raise ValueError(f"Página desconhecida: {page_key}")

    mod = _import_first(*paths)
    render = getattr(mod, "render", None)
    if not callable(render):
        raise ImportError(f"Função render() não encontrada no módulo da página: {paths}")
    return render


configurar_pagina, aplicar_estilos_css = _get_layout_funcs()
configurar_pagina()
aplicar_estilos_css()


def _ensure_setores_df() -> None:
    s = st.session_state.get("setores_df")
    if s is not None and getattr(s, "empty", False) is False:
        return
    load_setores = _get_loader()
    st.session_state["setores_df"] = load_setores(engine=_engine())


def _sidebar() -> str:
    st.sidebar.markdown(
        """
        <style>
          /* Remove scrollbar “sobrando” do sidebar */
          [data-testid="stSidebar"] > div:first-child { overflow: hidden; }
          [data-testid="stSidebar"] .stVerticalBlock { gap: .65rem; }

          /* Layout flex: topo + empurra configurações para baixo */
          .sb-wrap { height: calc(100vh - 1rem); display:flex; flex-direction:column; }
          .sb-top { }
          .sb-bottom { margin-top:auto; padding-top:.5rem; border-top:1px solid rgba(0,0,0,.08); }

          .sb-title { font-size: 1.1rem; font-weight: 800; margin: .25rem 0 .25rem; }
          .sb-sub { color:#6b7280; font-size:.85rem; margin-bottom:.25rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.sidebar.markdown("<div class='sb-wrap'>", unsafe_allow_html=True)

    st.sidebar.markdown("<div class='sb-top'>", unsafe_allow_html=True)
    st.sidebar.markdown("<div class='sb-title'>Análises</div>", unsafe_allow_html=True)
    st.sidebar.markdown("<div class='sb-sub'>Selecione uma seção.</div>", unsafe_allow_html=True)

    pagina = st.sidebar.radio(
        "Escolha a seção:",
        ["Básica", "Avançada", "Criação de Portfólio"],
        index=0,
        label_visibility="collapsed",
    )
    st.sidebar.markdown("</div>", unsafe_allow_html=True)

    st.sidebar.markdown("<div class='sb-bottom'>", unsafe_allow_html=True)
    cfg = st.sidebar.button("Configurações", use_container_width=True)
    st.sidebar.markdown("</div>", unsafe_allow_html=True)

    st.sidebar.markdown("</div>", unsafe_allow_html=True)

    if cfg:
        return "Configurações"
    return pagina


# --- Execução
try:
    _ensure_setores_df()
except Exception as e:
    st.error(f"Falha ao inicializar dados base (setores_df): {e}")
    st.stop()

pagina_escolhida = _sidebar()

try:
    render_page = _load_page_renderer(pagina_escolhida)
    render_page()
except Exception as e:
    st.error("Falha ao carregar a página selecionada.")
    st.exception(e)
