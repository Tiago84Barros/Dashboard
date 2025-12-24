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

# ───────────────────────── Ajuste de path ──────────────────────────
ROOT_DIR = pathlib.Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))


# ───────────────────────── Engine (Supabase) ───────────────────────
@st.cache_resource
def _engine():
    return get_engine()


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
              .block-container { padding-top: 1.1rem; padding-bottom: 2rem; }
              [data-testid="stSidebar"] { padding-top: 0.75rem; }
            </style>
            """,
            unsafe_allow_html=True,
        )

    return _fallback_config, _fallback_css


def _get_loader():
    """
    Obtém load_setores em:
    - core.db.loader (padrão novo)
    - loader (fallback legado, se existir)
    """
    mod = _import_first("core.db.loader", "loader")
    fn = getattr(mod, "load_setores", None)
    if not callable(fn):
        raise ImportError("load_setores não encontrado em core.db.loader/loader.")
    return fn


def _load_page_renderer(page_key: str) -> Callable[[], None]:
    """
    Carrega a função render() da página escolhida, com fallback de caminhos.
    """
    mapping = {
        "Básica": ("page.basic", "basic"),
        "Avançada": ("page.advanced", "advanced"),
        "Criação de Portfólio": ("page.criacao_portfolio", "criacao_portfolio"),
        "Configurações": ("page.configuracoes", "configuracoes"),
    }
    paths = mapping.get(page_key)
    if not paths:
        raise ValueError(f"Página desconhecida: {page_key}")

    # Se for Configurações e não existir, fallback inline (não quebra o app)
    if page_key == "Configurações":
        for p in paths:
            try:
                mod = importlib.import_module(p)
                render = getattr(mod, "render", None)
                if callable(render):
                    return render
            except Exception:
                continue

        def _fallback_config_page():
            st.markdown("## Configurações")
            st.info(
                "A página de configurações não foi encontrada como módulo importável.\n\n"
                "Recomendação: renomeie para **page/configuracoes.py** (sem acento e pasta em minúsculo) "
                "com uma função **render()**."
            )

        return _fallback_config_page

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
    """
    Garante setores_df no session_state, carregando do Supabase (engine).
    """
    s = st.session_state.get("setores_df")
    if s is not None and getattr(s, "empty", False) is False:
        return

    load_setores = _get_loader()
    setores_df = load_setores(engine=_engine())
    st.session_state["setores_df"] = setores_df


# ───────────────────────── Sidebar (seguro) ─────────────────────────
def _sidebar() -> str:
    """
    Sidebar com:
      - Navegação no topo (radio)
      - Busca de ações (botão)
      - Botão Configurações isolado no rodapé (sticky)

    Ajuste necessário:
      - Persistir a página atual no session_state para não voltar ao default após rerun.
    """
    st.sidebar.markdown(
        """
        <style>
          /* Ajustes seguros (não mexe em overflow do root) */
          [data-testid="stSidebar"] { padding-top: 0.75rem; }
          [data-testid="stSidebar"] .stVerticalBlock { gap: .65rem; }

          .sb-title { font-size: 1.15rem; font-weight: 800; margin: .25rem 0 .25rem; }
          .sb-sub { color:#9ca3af; font-size:.85rem; margin-bottom:.35rem; }

          .sb-card {
            border: 1px solid rgba(255,255,255,.08);
            border-radius: 14px;
            padding: 12px;
            background: rgba(255,255,255,.03);
          }

          .sb-bottom {
            position: sticky;
            bottom: 0;
            padding-top: .75rem;
            padding-bottom: .75rem;
            margin-top: 1rem;
            border-top: 1px solid rgba(255,255,255,.08);
            background: inherit;
            z-index: 10;
          }

          [data-testid="stSidebar"] .stButton > button {
            border-radius: 12px;
            font-weight: 700;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.sidebar.markdown("<div class='sb-title'>Análises</div>", unsafe_allow_html=True)
    st.sidebar.markdown("<div class='sb-sub'>Selecione uma seção.</div>", unsafe_allow_html=True)

    # ── Estado persistente da página (FIX do rerun) ──
    if "pagina_atual" not in st.session_state:
        st.session_state["pagina_atual"] = "Básica"

    opcoes_radio = ["Básica", "Avançada", "Criação de Portfólio"]
    # Se a página atual for "Configurações", o radio não tem essa opção;
    # então usamos "Básica" como fallback apenas para o índice visual do radio,
    # mantendo a página real via session_state.
    pagina_state = st.session_state.get("pagina_atual", "Básica")
    radio_index = opcoes_radio.index(pagina_state) if pagina_state in opcoes_radio else 0

    pagina_radio = st.sidebar.radio(
        "Escolha a seção:",
        opcoes_radio,
        index=radio_index,
        label_visibility="visible",
        key="radio_secao",
    )

    # Se estivermos em Configurações, NÃO deixe o radio sobrescrever a página atual
    if pagina_state != "Configurações":
        if pagina_radio != pagina_state and pagina_radio in opcoes_radio:
            st.session_state["pagina_atual"] = pagina_radio
            pagina_state = pagina_radio

    # ───────────── Busca de ações (recolocada) ─────────────
    with st.sidebar.container():
        st.markdown("<div class='sb-card'>", unsafe_allow_html=True)
        st.markdown("**Busca de ações**")
        ticker = st.text_input(
            "Ticker",
            value=st.session_state.get("ticker_busca", ""),
            placeholder="Ex: PETR4",
        )
        col_a, col_b = st.columns([1, 1])
        with col_a:
            buscar = st.button("Buscar", use_container_width=True)
        with col_b:
            limpar = st.button("Limpar", use_container_width=True)

        if limpar:
            st.session_state["ticker_busca"] = ""
            st.session_state.pop("ticker_selecionado", None)
            st.session_state.pop("ticker_filtrado", None)
            st.rerun()

        if buscar:
            t = (ticker or "").strip().upper()
            st.session_state["ticker_busca"] = t
            # Chaves genéricas que você pode usar nas páginas
            st.session_state["ticker_selecionado"] = t
            st.session_state["ticker_filtrado"] = t
            st.success(f"Ticker definido: {t if t else '—'}")

        st.markdown("</div>", unsafe_allow_html=True)

    # ───────────── Botão Configurações fixo no rodapé ─────────────
    st.sidebar.markdown("<div class='sb-bottom'>", unsafe_allow_html=True)
    cfg = st.sidebar.button("Configurações", use_container_width=True)
    st.sidebar.markdown("</div>", unsafe_allow_html=True)

    if cfg:
        # FIX: persistir a escolha para sobreviver ao rerun
        st.session_state["pagina_atual"] = "Configurações"
        return "Configurações"

    # Se não clicou em Configurações, devolve a página persistida (radio)
    return st.session_state["pagina_atual"]


# ───────────────────────── Roteamento / Execução ────────────────────
pagina_escolhida = _sidebar()

# Só carrega setores_df se não for Configurações
if pagina_escolhida != "Configurações":
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

