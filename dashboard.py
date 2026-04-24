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
from typing import Callable, Optional
import os
import streamlit as st

logger = logging.getLogger(__name__)

# ───────────────────────── Ajuste de path ──────────────────────────
# Deve rodar ANTES de qualquer import local (core, page, design).
# insert(0) garante prioridade sobre site-packages.
# invalidate_caches() limpa o sys.path_importer_cache do processo em execução,
# permitindo que arquivos criados/adicionados depois do início do processo
# (ex: __init__.py) sejam encontrados sem reiniciar o servidor.
ROOT_DIR = pathlib.Path(__file__).resolve().parent
_root_str = str(ROOT_DIR)
if _root_str not in sys.path:
    sys.path.insert(0, _root_str)
importlib.invalidate_caches()

logger.debug("dashboard bootstrap | __file__=%s", __file__)
logger.debug("dashboard bootstrap | ROOT_DIR=%s", ROOT_DIR)
logger.debug("dashboard bootstrap | sys.path[:4]=%s", sys.path[:4])
logger.debug("dashboard bootstrap | core/__init__.py exists=%s",
             (ROOT_DIR / "core" / "__init__.py").exists())

# ___________________ carrega a API da OpenAI _______________________
def _load_env_from_secrets():
    for k in ("OPENAI_API_KEY", "AI_PROVIDER", "AI_MODEL"):
        if k in st.secrets and not os.getenv(k):
            os.environ[k] = str(st.secrets[k])

_load_env_from_secrets()


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
    Carrega a função render() da página escolhida, com fallback de caminhos.
    """
    mapping = {
        "Básica": ("page.basic", "basic"),
        "Avançada": ("page.advanced", "advanced"),
        "Criação de Portfólio": ("page.criacao_portfolio", "criacao_portfolio"),
        "Análises de Portfólio": ("page.analises_portfolio", "analises_portfolio"),
        #"Análises de Portfólio V2": ("page.analises_portfolio_v2", "analises_portfolio_v2"),
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
        [
            "Básica",
            "Avançada",
            "Criação de Portfólio",
            "Análises de Portfólio",
            #"Análises de Portfólio V2",
            "Configurações",
        ],
        index=0,
    )

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
