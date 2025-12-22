"""
dashboard.py
~~~~~~~~~~~~
Script principal Streamlit.

Execute:
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

logger = logging.getLogger(__name__)


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


def _get_engine():
    mod = _import_first("core.db_supabase", "db_supabase")
    if hasattr(mod, "get_engine"):
        return mod.get_engine()
    if hasattr(mod, "engine"):
        return mod.engine
    raise ImportError("Não encontrei get_engine() em core.db_supabase/db_supabase.")


# ───────────────────────── CVM sync API ──────────────────────────
_sync_mod = _import_first("core.cvm_sync", "cvm_sync")
get_sync_status = getattr(_sync_mod, "get_sync_status")
apply_update = getattr(_sync_mod, "apply_update")


# ───────────────────────── Page loaders ──────────────────────────
def _load_page_renderer(page_key: str) -> Callable[[], None]:
    """
    Mapeia a página para um módulo e retorna render() daquela página.

    Importante: tentamos primeiro em "page.<x>" e depois "<x>" para manter compatibilidade.
    """
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


# ───────────────────────── Helpers DB / UI ──────────────────────────
def _db_scalar(engine, sql: str):
    with engine.begin() as conn:
        return conn.execute(text(sql)).scalar()


def _db_row(engine, sql: str):
    with engine.begin() as conn:
        return conn.execute(text(sql)).mappings().first()


def _fmt_mmss(seconds: int) -> str:
    m = seconds // 60
    s = seconds % 60
    return f"{m:02d}:{s:02d}"


def _bank_summary(engine) -> dict:
    """
    Resumo leve (sem número de linhas) para evitar custo.
    """
    out = {
        "dfp_tickers": None,
        "dfp_years": None,
        "itr_last_quarter": None,
        "macro_rows": None,
    }

    # DFP
    try:
        out["dfp_tickers"] = _db_scalar(engine, "select count(distinct ticker) from cvm.demonstracoes_financeiras;")
        r = _db_row(engine, "select min(data) as mn, max(data) as mx from cvm.demonstracoes_financeiras;")
        if r and r.get("mn") and r.get("mx"):
            out["dfp_years"] = f"{r['mn'].year} → {r['mx'].year}"
    except Exception:
        pass

    # ITR (último trimestre pelo max(data))
    try:
        r = _db_row(engine, "select max(data) as mx from cvm.demonstracoes_financeiras_tri;")
        if r and r.get("mx"):
            out["itr_last_quarter"] = str(r["mx"])
    except Exception:
        pass

    # Macro (se existir tabela)
    try:
        out["macro_rows"] = _db_scalar(engine, "select count(*) from cvm.info_economica;")
    except Exception:
        out["macro_rows"] = None

    return out


# ───────────────────────── UI: Configurações ──────────────────────────
def _render_configuracoes(engine):
    st.title("Configurações")
    st.caption("Atualização do banco CVM e status de sincronização.")

    # Estado do job (thread local)
    if "sync_thread" not in st.session_state:
        st.session_state.sync_thread = None
    if "sync_started_at" not in st.session_state:
        st.session_state.sync_started_at = None

    # Banner de status
    status = get_sync_status(engine) or {}
    last_error = (status.get("last_error") or "").strip()
    last_success = (status.get("last_success") or "").strip()

    if last_error:
        st.error(f"Última execução com erro: {last_error}")
    elif last_success:
        st.success(f"Última atualização concluída: {last_success}")
    else:
        st.info("Nenhuma atualização concluída ainda.")

    # Ação: iniciar atualização (só aqui, NÃO no sidebar)
    col_a, col_b = st.columns([1, 3])
    with col_a:
        start = st.button("Atualizar agora", type="primary", use_container_width=True)
    with col_b:
        st.write("")

    th = st.session_state.sync_thread
    running = (th is not None and th.is_alive())

    if start and not running:
        def _job():
            # roda pipeline; o progresso é gravado no banco via cvm_sync/pipeline
            apply_update(engine)

        st.session_state.sync_thread = threading.Thread(target=_job, daemon=True)
        st.session_state.sync_started_at = time.time()
        st.session_state.sync_thread.start()
        running = True

    # Progresso (ÚNICO bloco)
    st.subheader("Progresso da atualização")

    # placeholders fixos (não cria “3 progressos”)
    bar = st.progress(0)
    k1, k2, k3, k4 = st.columns(4)
    stage_box = st.empty()

    # lê status do banco (progress_pct/stage/message etc)
    status = get_sync_status(engine) or {}
    pct_raw = status.get("progress_pct") or 0
    try:
        pct = int(float(pct_raw))
    except Exception:
        pct = 0
    pct = max(0, min(100, pct))

    stage = status.get("stage") or "—"
    msg = status.get("message") or ""

    # tempo decorrido (mm:ss)
    if st.session_state.sync_started_at:
        elapsed = int(time.time() - float(st.session_state.sync_started_at))
    else:
        elapsed = 0

    # ETA simples (se progresso > 0)
    if pct > 0 and elapsed > 0 and pct < 100:
        est_total = int(elapsed * (100 / pct))
        eta = max(0, est_total - elapsed)
        eta_txt = _fmt_mmss(eta)
    else:
        eta_txt = "—"

    bar.progress(pct)
    k1.metric("Progresso", f"{pct}%")
    k2.metric("Tempo decorrido", _fmt_mmss(elapsed))
    k3.metric("Tempo restante (est.)", eta_txt)
    k4.metric("Etapa", stage)

    if msg:
        stage_box.info(msg)
    else:
        stage_box.write("")

    # Auto-atualização visual enquanto estiver rodando
    # (streamlit não tem “background UI”, então fazemos um refresh leve)
    th = st.session_state.sync_thread
    running = (th is not None and th.is_alive())

    if running:
        time.sleep(1)
        st.experimental_rerun()

    # Resumo do banco (enxuto)
    st.subheader("Resumo do banco (CVM)")
    s = _bank_summary(engine)
    c1, c2, c3 = st.columns(3)
    c1.metric("DFP (tickers)", str(s.get("dfp_tickers") or "—"))
    c2.metric("Período DFP", str(s.get("dfp_years") or "—"))
    c3.metric("Última data ITR", str(s.get("itr_last_quarter") or "—"))


# ───────────────────────── Main app ──────────────────────────
def main():
    st.set_page_config(page_title="Dashboard Financeiro", layout="wide")

    engine = _get_engine()

    # Estado inicial (página)
    if "page" not in st.session_state:
        st.session_state["page"] = "Básica"

    # Sidebar: manter BUSCA na posição atual e Configurações no rodapé
    with st.sidebar:
        st.header("Análises")

        # BUSCA (mantida no topo, como você pediu)
        ticker_input = st.text_input("Buscar ticker (ex.: PETR4)", key="ticker_box")
        if ticker_input.strip():
            t = ticker_input.upper().strip()
            if not t.endswith(".SA"):
                t += ".SA"
            st.session_state["ticker"] = t
        else:
            # se usuário limpou a busca, remove ticker para voltar à lista
            st.session_state.pop("ticker", None)

        st.write("")  # espaçamento

        pagina_analises = st.radio(
            "Escolha a seção:",
            ["Básica", "Avançada", "Criação de Portfólio"],
            index=["Básica", "Avançada", "Criação de Portfólio"].index(
                st.session_state.get("page", "Básica") if st.session_state.get("page") in ["Básica", "Avançada", "Criação de Portfólio"] else "Básica"
            ),
            key="pagina_analises",
        )

        # Se mudou a rádio, atualiza a page (exceto se estiver em Configurações)
        if st.session_state.get("page") != "Configurações":
            st.session_state["page"] = pagina_analises

        # Empurra o botão para o rodapé
        st.markdown(
            """
            <style>
              [data-testid="stSidebar"] > div:first-child {
                display: flex;
                flex-direction: column;
                height: 100vh;
              }
              .sidebar-bottom {
                margin-top: auto;
                padding-top: 12px;
              }
            </style>
            """,
            unsafe_allow_html=True,
        )

        st.markdown('<div class="sidebar-bottom">', unsafe_allow_html=True)
        # ÚNICO botão no “rodapé” do sidebar
        if st.button("⚙  Configurações", use_container_width=True):
            st.session_state["page"] = "Configurações"
        st.markdown("</div>", unsafe_allow_html=True)

    # Roteamento
    page = st.session_state.get("page", "Básica")

    if page == "Configurações":
        _render_configuracoes(engine)
        return

    renderer = _load_page_renderer(page)
    renderer()


if __name__ == "__main__":
    main()
