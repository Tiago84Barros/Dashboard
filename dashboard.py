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
import threading
import time
from typing import Callable

import streamlit as st
from sqlalchemy import text

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
              [data-testid="stSidebar"] { padding-top: 1rem; }
            </style>
            """,
            unsafe_allow_html=True,
        )

    return _fallback_config, _fallback_css


def _get_db_loader():
    mod = _import_first("core.db_loader", "db_loader")
    fn = getattr(mod, "load_setores_from_db", None)
    if not callable(fn):
        raise ImportError("load_setores_from_db não encontrado em core.db_loader/db_loader.")
    return fn


def _load_page_renderer(page_key: str) -> Callable[[], None]:
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


# ───────────────────────── UI Helpers ──────────────────────────
def _fmt_mmss(seconds: int) -> str:
    seconds = max(0, int(seconds))
    m = seconds // 60
    s = seconds % 60
    return f"{m:02d}:{s:02d}"


def _db_scalar(engine, sql: str):
    with engine.begin() as conn:
        return conn.execute(text(sql)).scalar()


def _db_row(engine, sql: str):
    with engine.begin() as conn:
        return conn.execute(text(sql)).mappings().first()


def _bank_summary(engine) -> dict:
    out = {"dfp_tickers": None, "dfp_years": None, "itr_last_date": None}

    try:
        out["dfp_tickers"] = _db_scalar(engine, "select count(distinct ticker) from cvm.demonstracoes_financeiras;")
        r = _db_row(engine, "select min(data) as mn, max(data) as mx from cvm.demonstracoes_financeiras;")
        if r and r["mn"] and r["mx"]:
            out["dfp_years"] = f"{r['mn'].year} → {r['mx'].year}"
    except Exception:
        pass

    try:
        r = _db_row(engine, "select max(data) as mx from cvm.demonstracoes_financeiras_tri;")
        if r and r["mx"]:
            out["itr_last_date"] = str(r["mx"])
    except Exception:
        pass

    return out


# ───────────────────────── UI: Configurações ──────────────────────────
def _render_configuracoes(engine):
    st.title("Configurações")
    st.caption("Atualização do banco CVM e status da sincronização.")

    status0 = get_sync_status(engine)
    last_error0 = (status0.get("last_error") or "").strip()

    if last_error0:
        st.error(f"Última execução com erro: {last_error0}")
    elif status0.get("last_success"):
        st.success(f"Última atualização concluída: {status0.get('last_success')}")
    else:
        st.info("Nenhuma atualização concluída ainda.")

    col_a, col_b = st.columns([1, 2])
    with col_a:
        start = st.button("Atualizar agora", type="primary", use_container_width=True)
    with col_b:
        st.write("")

    if "sync_thread" not in st.session_state:
        st.session_state.sync_thread = None

    if start and (st.session_state.sync_thread is None or not st.session_state.sync_thread.is_alive()):

        def _job():
            apply_update(engine)

        st.session_state.sync_thread = threading.Thread(target=_job, daemon=True)
        st.session_state.sync_thread.start()

    panel_slot = st.empty()
    t0 = time.time()
    timeout_s = 15 * 60

    while True:
        status = get_sync_status(engine)

        pct_raw = status.get("progress_pct") or "0"
        try:
            pct = max(0, min(100, int(float(pct_raw))))
        except Exception:
            pct = 0

        stage = status.get("stage") or "-"
        msg = status.get("message") or ""
        last_error = (status.get("last_error") or "").strip()

        elapsed = int(time.time() - t0)

        if pct > 0:
            est_total = int(elapsed * (100 / pct))
            eta = max(0, est_total - elapsed)
            eta_txt = _fmt_mmss(eta)
        else:
            eta_txt = "—"

        with panel_slot.container():
            panel_slot.empty()

            st.subheader("Progresso da atualização")
            st.progress(pct)

            c1, c2, c3 = st.columns(3, gap="medium")
            c1.metric("Progresso", f"{pct}%")
            c2.metric("Tempo decorrido", _fmt_mmss(elapsed))
            c3.metric("Tempo restante (est.)", eta_txt)

            st.markdown(f"**Etapa:** {stage}")
            if msg:
                st.caption(msg)
            if last_error:
                st.error(last_error)

        th = st.session_state.sync_thread
        alive = (th is not None and th.is_alive())

        if not alive:
            break

        if elapsed > timeout_s:
            with panel_slot.container():
                st.warning("A atualização ultrapassou o tempo esperado. Verifique logs/timeout no Supabase.")
            break

        time.sleep(1)

    st.subheader("Resumo do banco (CVM)")
    s = _bank_summary(engine)
    c1, c2, c3 = st.columns(3)
    c1.metric("DFP (tickers)", str(s.get("dfp_tickers") or "—"))
    c2.metric("Período DFP", str(s.get("dfp_years") or "—"))
    c3.metric("Última data ITR", str(s.get("itr_last_date") or "—"))


# ───────────────────────── Sidebar (layout robusto) ───────────────────────
st.markdown(
    """
    <style>
      [data-testid="stSidebarContent"]{
        display: flex;
        flex-direction: column;
        height: 100%;
      }
      .sb-spacer{
        flex: 1 1 auto;
      }
      .sb-footer{
        padding-top: 12px;
        padding-bottom: 8px;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# ───────────────────────── Sidebar navegação ───────────────────────
with st.sidebar:
    # Título
    st.markdown("## Análises")
    st.divider()

    # Escolha da seção
    pagina_escolhida = st.radio(
        "Escolha a seção:",
        ["Básica", "Avançada", "Criação de Portfólio"],
        index=0,
        key="pagina_escolhida",
    )

    st.divider()

    # Busca
    st.text_input("Buscar ticker (ex.: PETR4)", key="buscar_ticker")

    # Spacer NATIVO (não quebra cliques)
    spacer = st.empty()
    spacer.write("")  # ocupa espaço sem bloquear interação

    st.divider()

    # Rodapé – Configurações
    if st.button("⚙️ Configurações", use_container_width=True, key="btn_config"):
        st.session_state["page"] = "Configurações"
        st.rerun()

    # Roteamento padrão
    if st.session_state.get("page") != "Configurações":
        st.session_state["page"] = pagina_escolhida

page = st.session_state.get("page", "Básica")

if page == "Configurações":
    _render_configuracoes(engine)
    return

renderer = _load_page_renderer(page)
renderer()


if _name_ == "_main_":
main()


