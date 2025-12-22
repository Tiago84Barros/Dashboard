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


# ───────────────────────── DB helpers ──────────────────────────
def _db_scalar(engine, sql: str):
    with engine.begin() as conn:
        return conn.execute(text(sql)).scalar()


def _db_row(engine, sql: str):
    with engine.begin() as conn:
        return conn.execute(text(sql)).mappings().first()


def _bank_summary(engine) -> dict:
    out = {"dfp_tickers": None, "dfp_years": None, "itr_last": None}
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
            # exibe como trimestre aproximado se quiser, aqui fica data mesmo
            out["itr_last"] = str(r["mx"])
    except Exception:
        pass

    return out


def _fmt_mmss(seconds: int) -> str:
    m = seconds // 60
    s = seconds % 60
    return f"{m}m {s:02d}s"


# ───────────────────────── UI: Configurações (sem poluir visual) ──────────────────────────
def _render_configuracoes(engine):
    st.title("Configurações")
    st.caption("Atualização do banco CVM e status de sincronização.")

    status = get_sync_status(engine) or {}
    last_error = (status.get("last_error") or "").strip()
    last_success = status.get("last_success") or None

    if last_error:
        st.error(f"Última execução com erro: {last_error}")
    elif last_success:
        st.success(f"Última atualização concluída: {last_success}")
    else:
        st.info("Nenhuma atualização concluída ainda.")

    st.divider()

    # Controle de execução
    if "sync_thread" not in st.session_state:
        st.session_state.sync_thread = None

    col1, col2 = st.columns([1, 3])
    with col1:
        start = st.button("Atualizar agora", type="primary", use_container_width=True)
    with col2:
        st.write("")

    if start and (st.session_state.sync_thread is None or not st.session_state.sync_thread.is_alive()):
        def _job():
            # apply_update deve atualizar sync_state via pipeline progress_cb internamente
            apply_update(engine)

        st.session_state.sync_thread = threading.Thread(target=_job, daemon=True)
        st.session_state.sync_thread.start()

    # Progresso único (não duplicar)
    st.subheader("Progresso")
    bar = st.progress(0)
    c1, c2, c3, c4 = st.columns(4)
    stage_box = st.empty()

    t0 = time.time()
    timeout_s = 30 * 60  # 30 min

    # Polling leve enquanto thread viva
    while True:
        status = get_sync_status(engine) or {}

        pct_raw = status.get("progress_pct") or 0
        try:
            pct = max(0, min(100, int(float(pct_raw))))
        except Exception:
            pct = 0

        stage = status.get("stage") or "-"
        msg = status.get("message") or ""

        elapsed = int(time.time() - t0)
        if pct > 0:
            est_total = int(elapsed * (100 / pct))
            eta = max(0, est_total - elapsed)
            eta_txt = _fmt_mmss(eta)
        else:
            eta_txt = "—"

        bar.progress(pct)
        c1.metric("Progresso", f"{pct}%")
        c2.metric("Tempo decorrido", _fmt_mmss(elapsed))
        c3.metric("Tempo restante (est.)", eta_txt)
        c4.metric("Etapa", stage)

        if msg:
            stage_box.caption(msg)
        else:
            stage_box.caption("")

        th = st.session_state.sync_thread
        alive = (th is not None and th.is_alive())

        if not alive:
            break

        if elapsed > timeout_s:
            st.warning("A atualização ultrapassou o tempo esperado. Verifique timeout/logs no Supabase.")
            break

        time.sleep(1)

    st.divider()

    st.subheader("Resumo do banco (CVM)")
    s = _bank_summary(engine)
    r1, r2, r3 = st.columns(3)
    r1.metric("DFP (tickers)", str(s.get("dfp_tickers") or "—"))
    r2.metric("Período DFP", str(s.get("dfp_years") or "—"))
    r3.metric("Última data ITR", str(s.get("itr_last") or "—"))


# ───────────────────────── Main app ──────────────────────────
def main():
    st.set_page_config(page_title="Dashboard Financeiro", layout="wide")

    engine = _get_engine()

    # CSS: fixa o botão de Configurações no rodapé do sidebar
    st.markdown(
        """
        <style>
        div[data-testid="stSidebar"] { position: relative; }
        div[data-testid="stSidebar"] .config-bottom {
            position: fixed;
            bottom: 18px;
            left: 18px;
            width: 300px; /* ajuste fino */
            z-index: 9999;
        }
        @media (max-width: 1100px) {
            div[data-testid="stSidebar"] .config-bottom { width: 260px; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Sidebar único e consistente
    with st.sidebar:
        st.header("Análises")

        # Busca por ticker (mantém no topo, como você pediu)
        ticker_input = st.text_input("Buscar ticker (ex.: PETR4)", key="ticker_box")
        if ticker_input.strip():
            t = ticker_input.strip().upper()
            if not t.endswith(".SA"):
                t += ".SA"
            st.session_state["ticker"] = t
        else:
            # IMPORTANTÍSSIMO: se está vazio, remove do session_state,
            # senão a Avançada pode “sumir” filtros por achar que há ticker ativo.
            st.session_state.pop("ticker", None)

        st.markdown("")

        pagina_analises = st.radio(
            "Escolha a seção:",
            ["Básica", "Avançada", "Criação de Portfólio"],
            index=0,
            key="pagina_analises",
        )

        # define page default quando não estiver em configurações
        if st.session_state.get("page") != "Configurações":
            st.session_state["page"] = pagina_analises

        # espaço (para não colar no botão fixo)
        st.markdown("<div style='height:80px'></div>", unsafe_allow_html=True)

        # botão fixo no rodapé
        st.markdown("<div class='config-bottom'>", unsafe_allow_html=True)
        if st.button("⚙️  Configurações", key="btn_config", use_container_width=True):
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
