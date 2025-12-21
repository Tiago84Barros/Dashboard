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

logger = logging.getLogger(__name__)
engine = get_engine()

# ───────────────────────── Path / Imports helpers ──────────────────────────
ROOT_DIR = pathlib.Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))


def _import_first(*module_names: str):
    last_err = None
    for name in module_names:
        try:
            return importlib.import_module(name)
        except Exception as e:
            last_err = e
    raise ImportError(f"Falha ao importar módulos {module_names}. Último erro: {last_err}")


# ───────────────────────── Page loaders ──────────────────────────
def _load_page_renderer(page_key: str) -> Callable[[], None]:
    mapping = {
        "Básica": ("page.basic", "basic"),
        "Avançada": ("page.advanced", "advanced"),
        "Criação de Portfólio": ("page.criacao_portfolio", "criacao_portfolio"),
    }
    mods = mapping.get(page_key)
    if not mods:
        raise ValueError(f"Página inválida: {page_key}")

    mod = _import_first(*mods)
    fn = getattr(mod, "render", None)
    if not callable(fn):
        raise ImportError(f"render() não encontrado em {mods}.")
    return fn


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
        out["dfp_tickers"] = _db_scalar(
            engine,
            "select count(distinct ticker) from cvm.demonstracoes_financeiras;"
        )
        r = _db_row(
            engine,
            "select min(data) as mn, max(data) as mx from cvm.demonstracoes_financeiras;"
        )
        if r and r["mn"] and r["mx"]:
            out["dfp_years"] = f"{r['mn'].year} → {r['mx'].year}"
    except Exception:
        pass

    try:
        r = _db_row(
            engine,
            "select max(data) as mx from cvm.demonstracoes_financeiras_tri;"
        )
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

    if start and (
        st.session_state.sync_thread is None
        or not st.session_state.sync_thread.is_alive()
    ):
        def _job():
            apply_update(engine)

        st.session_state.sync_thread = threading.Thread(
            target=_job, daemon=True
        )
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

            c1, c2, c3 = st.columns(3)
            c1.metric("Progresso", f"{pct}%")
            c2.metric("Tempo decorrido", _fmt_mmss(elapsed))
            c3.metric("Tempo restante (est.)", eta_txt)

            st.markdown(f"**Etapa:** {stage}")
            if msg:
                st.caption(msg)
            if last_error:
                st.error(last_error)

        th = st.session_state.sync_thread
        if th is None or not th.is_alive():
            break

        if elapsed > timeout_s:
            st.warning(
                "A atualização ultrapassou o tempo esperado. "
                "Verifique logs/timeout no Supabase."
            )
            break

        time.sleep(1)

    st.subheader("Resumo do banco (CVM)")
    s = _bank_summary(engine)
    c1, c2, c3 = st.columns(3)
    c1.metric("DFP (tickers)", str(s.get("dfp_tickers") or "—"))
    c2.metric("Período DFP", str(s.get("dfp_years") or "—"))
    c3.metric("Última data ITR", str(s.get("itr_last_date") or "—"))


# ───────────────────────── Sidebar layout (robusto e responsivo) ───────────────────────
st.markdown(
    """
    <style>
      [data-testid="stSidebarContent"]{
        display: flex;
        flex-direction: column;
        height: 100%;
      }
      .sb-footer{
        margin-top: auto;
        padding-top: 12px;
        padding-bottom: 8px;
      }
    </style>
    """,
    unsafe_allow_html=True,
)


# ───────────────────────── Sidebar navegação ───────────────────────
with st.sidebar:
    st.markdown("## Análises")
    st.divider()

    pagina_escolhida = st.radio(
        "Escolha a seção:",
        ["Básica", "Avançada", "Criação de Portfólio"],
        index=0,
        key="pagina_escolhida",
    )

    st.divider()
    st.text_input("Buscar ticker", key="buscar_ticker")
    st.divider()
    st.markdown('<div class="sb-footer">', unsafe_allow_html=True)
    if st.button("⚙️ Configurações", use_container_width=True, key="btn_config"):
        st.session_state["page"] = "Configurações"
        st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)


if page == "Configurações":
    _render_configuracoes(engine)
else:
    renderer = _load_page_renderer(page)
    renderer()




