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
import time
from typing import Callable

import streamlit as st
from sqlalchemy import text

from core.db_supabase import get_engine
from core.cvm_sync import apply_update, get_sync_status

logger = logging.getLogger(__name__)
engine = get_engine()

# ───────────────────────── Ajuste de path ──────────────────────────
ROOT_DIR = pathlib.Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))


# ───────────────────────── Imports com fallback ─────────────────────
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
    """
    Busca configurar_pagina() e aplicar_estilos_css() em:
    - design.layout
    - layout
    Se não existir, usa fallback.
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
    """
    Facade preferencial:
    - core.data_access / data_access
    fallback:
    - core.db_loader / db_loader
    """
    mod = _import_first("core.data_access", "data_access", "core.db_loader", "db_loader")
    fn = getattr(mod, "load_setores_from_db", None)
    if not callable(fn):
        raise ImportError("load_setores_from_db não encontrado em core.data_access/data_access/core.db_loader/db_loader.")
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
    st.session_state["setores_df"] = load_setores_from_db()


# ───────────────────────── Helpers DB snapshot ─────────────────────
def _safe_scalar(sql: str):
    try:
        with engine.begin() as conn:
            return conn.execute(text(sql)).scalar()
    except Exception:
        return None


def _get_snapshot_light() -> dict:
    """
    Snapshot "limpo": sem contagem de linhas (ruído). Só o que importa para UX.
    """
    snap = {}

    snap["dfp_tickers"] = _safe_scalar("select count(distinct ticker) from cvm.demonstracoes_financeiras;")
    snap["dfp_min"] = _safe_scalar("select min(data) from cvm.demonstracoes_financeiras;")
    snap["dfp_max"] = _safe_scalar("select max(data) from cvm.demonstracoes_financeiras;")

    snap["itr_tickers"] = _safe_scalar("select count(distinct ticker) from cvm.demonstracoes_financeiras_tri;")
    snap["itr_max"] = _safe_scalar("select max(data) from cvm.demonstracoes_financeiras_tri;")

    snap["macro_max"] = _safe_scalar("select max(data) from cvm.info_economica;")

    return snap


def _fmt_quarter(d) -> str:
    if not d:
        return "-"
    try:
        q = (d.month - 1) // 3 + 1
        return f"{d.year}T{q}"
    except Exception:
        return "-"


def _fmt_years(min_d, max_d) -> str:
    if not min_d or not max_d:
        return "-"
    try:
        return f"{min_d.year} → {max_d.year}"
    except Exception:
        return "-"


# ───────────────────────── Página Configurações ────────────────────
def render_configuracoes() -> None:
    st.title("Configurações")

    # Estado UI
    st.session_state.setdefault("cfg_show_diag", False)

    # Estado update
    st.session_state.setdefault("update_running", False)
    st.session_state.setdefault("update_started_at", None)
    st.session_state.setdefault("update_step", (0, 0))          # (idx, total)
    st.session_state.setdefault("update_msg", "")
    st.session_state.setdefault("update_pct_hint", 0.0)

    # ── Top bar: ações
    c1, c2, c3 = st.columns([1, 1, 2], gap="medium")

    with c1:
        if st.button("Recarregar cache", use_container_width=True):
            st.session_state.pop("setores_df", None)
            try:
                st.cache_data.clear()
            except Exception:
                pass
            st.success("Cache recarregado.")
            st.rerun()

    with c2:
        if st.button("Diagnóstico", use_container_width=True):
            st.session_state["cfg_show_diag"] = not st.session_state["cfg_show_diag"]

    with c3:
        iniciar = st.button(
            "Atualizar tabelas (CVM)",
            use_container_width=True,
            disabled=st.session_state["update_running"],
        )

    # ── Diagnóstico (opcional)
    if st.session_state["cfg_show_diag"]:
        with st.expander("Diagnóstico", expanded=True):
            try:
                status = get_sync_status(engine)
                st.write("sync_status:", status)
            except Exception as e:
                st.error(f"Falha ao obter status: {e}")

            try:
                _ensure_setores_df()
                df = st.session_state.get("setores_df")
                st.write("setores_df carregado:", df is not None and not getattr(df, "empty", True))
            except Exception as e:
                st.error(f"Falha ao carregar setores_df: {e}")

    st.divider()

    # ── Placeholders únicos (evita duplicação / “3 carregamentos”)
    status_slot = st.empty()
    progress_slot = st.empty()

    def _render_status_header():
        with status_slot.container():
            status_slot.empty()
            try:
                status = get_sync_status(engine)
                last_run = status.get("last_run")
                if last_run:
                    st.info(f"Última atualização registrada (UTC): {last_run}")
                else:
                    st.warning("Nenhuma atualização executada ainda.")
            except Exception as e:
                st.warning(f"Não foi possível consultar status da CVM agora: {e}")

    def _render_progress_panel():
        """
        Painel limpo e “profissional”, sem ruído de contagem de linhas.
        """
        idx, total = st.session_state.get("update_step", (0, 0))

        if total and total > 0:
            pct = min(max(idx / total, 0.0), 1.0)
        else:
            # progresso “sensação” quando não há STEP i/t
            pct = min(max(st.session_state.get("update_pct_hint", 0.0), 0.0), 0.95)

        started = st.session_state.get("update_started_at")
        elapsed_s = int(time.time() - started) if started else 0

        remaining_s = None
        if started and total and total > 0 and idx > 0:
            per_step = (time.time() - started) / idx
            remaining_s = int(per_step * (total - idx))

        snap = _get_snapshot_light()

        with progress_slot.container():
            progress_slot.empty()

            st.subheader("Progresso da atualização")

            st.progress(pct)

            # Cards de métricas (mais clean)
            m1, m2, m3, m4 = st.columns(4, gap="medium")
            m1.metric("Progresso", f"{pct*100:.0f}%")
            m2.metric("Tempo decorrido", f"{elapsed_s}s")
            if remaining_s is not None:
                m3.metric("Tempo restante (est.)", f"{remaining_s}s")
            else:
                m3.metric("Tempo restante (est.)", "-")
            m4.metric("Etapa", f"{idx}/{total}" if total and total > 0 else "-")

            st.caption(st.session_state.get("update_msg", "").strip() or "Aguardando…")

            st.markdown("#### Resumo do banco (CVM)")

            r1, r2, r3 = st.columns(3, gap="medium")
            r1.metric("DFP (tickers)", f"{snap.get('dfp_tickers') or 0}")
            r2.metric("Período DFP", _fmt_years(snap.get("dfp_min"), snap.get("dfp_max")))
            r3.metric("Último trimestre ITR", _fmt_quarter(snap.get("itr_max")))

            # Macro opcional (sem “0 linhas”)
            macro_max = snap.get("macro_max")
            if macro_max:
                st.caption(f"Macro (info_economica) atualizada até: {macro_max}")

    # Render inicial (parado)
    _render_status_header()
    _render_progress_panel()

    # ── Execução do update (callback)
    if iniciar and not st.session_state["update_running"]:
        st.session_state["update_running"] = True
        st.session_state["update_started_at"] = time.time()
        st.session_state["update_msg"] = "Iniciando atualização…"
        st.session_state["update_step"] = (0, 0)
        st.session_state["update_pct_hint"] = 0.02

        _render_status_header()
        _render_progress_panel()

        def progress_cb(msg: str):
            st.session_state["update_msg"] = msg

            # STEP i/t :: módulo (se pipeline emitir)
            if isinstance(msg, str) and msg.startswith("STEP "):
                try:
                    header = msg.split("::", 1)[0].strip()     # "STEP i/t"
                    frac = header.replace("STEP", "").strip()  # "i/t"
                    i_s, t_s = frac.split("/", 1)
                    st.session_state["update_step"] = (int(i_s), int(t_s))
                except Exception:
                    pass
            else:
                # fallback: avança devagar
                st.session_state["update_pct_hint"] = min(st.session_state["update_pct_hint"] + 0.03, 0.95)

            _render_progress_panel()

        try:
            apply_update(engine, progress_cb=progress_cb)

            # recarrega setores após update
            st.session_state.pop("setores_df", None)

            st.session_state["update_msg"] = "Concluído."
            st.session_state["update_step"] = (1, 1)
            st.session_state["update_pct_hint"] = 1.0

            _render_status_header()
            _render_progress_panel()
            st.success("Atualização finalizada com sucesso.")

        except Exception as e:
            st.session_state["update_msg"] = f"Falha: {e}"
            _render_progress_panel()
            st.error(f"Falha ao atualizar CVM: {e}")
            logger.exception("Falha ao executar apply_update()", exc_info=e)

        finally:
            st.session_state["update_running"] = False


# ───────────────────────── Sidebar navegação ───────────────────────
with st.sidebar:
    st.markdown("## Análises")
    pagina_escolhida = st.radio(
        "Escolha a seção:",
        ["Básica", "Avançada", "Criação de Portfólio", "Configurações"],
        index=0,
    )


# ───────────────────────── Roteamento ──────────────────────────────
if pagina_escolhida == "Configurações":
    render_configuracoes()
    st.stop()

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
