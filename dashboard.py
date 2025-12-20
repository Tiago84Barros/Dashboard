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
from typing import Callable, Optional, Tuple

import streamlit as st
from sqlalchemy import text

from core.db_supabase import get_engine
from core.cvm_sync import apply_update, get_sync_status

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
    Obtém load_setores_from_db preferencialmente via facade (Opção A):
    - core.data_access (decide SQLite/Supabase via DATA_SOURCE)
    fallback:
    - data_access
    - core.db_loader / db_loader (legado)
    """
    mod = _import_first("core.data_access", "data_access", "core.db_loader", "db_loader")
    fn = getattr(mod, "load_setores_from_db", None)
    if not callable(fn):
        raise ImportError("load_setores_from_db não encontrado em core.data_access/data_access/core.db_loader/db_loader.")
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


# ───────────────────────── Helpers (DB Snapshot) ───────────────────
def _safe_scalar(sql: str):
    try:
        with engine.begin() as conn:
            return conn.execute(text(sql)).scalar()
    except Exception:
        return None


def _get_db_snapshot() -> dict:
    """
    Coleta métricas rápidas para exibir no painel de atualização.
    Deve ser leve e tolerante a tabelas ainda inexistentes.
    """
    snap = {}

    # DFP (anual)
    snap["dfp_rows"] = _safe_scalar("select count(*) from cvm.demonstracoes_financeiras;")
    snap["dfp_tickers"] = _safe_scalar("select count(distinct ticker) from cvm.demonstracoes_financeiras;")
    snap["dfp_min_date"] = _safe_scalar("select min(data) from cvm.demonstracoes_financeiras;")
    snap["dfp_max_date"] = _safe_scalar("select max(data) from cvm.demonstracoes_financeiras;")

    # ITR (trimestral)
    snap["itr_rows"] = _safe_scalar("select count(*) from cvm.demonstracoes_financeiras_tri;")
    snap["itr_tickers"] = _safe_scalar("select count(distinct ticker) from cvm.demonstracoes_financeiras_tri;")
    snap["itr_min_date"] = _safe_scalar("select min(data) from cvm.demonstracoes_financeiras_tri;")
    snap["itr_max_date"] = _safe_scalar("select max(data) from cvm.demonstracoes_financeiras_tri;")

    # Macro
    snap["macro_rows"] = _safe_scalar("select count(*) from cvm.info_economica;")
    snap["macro_min_date"] = _safe_scalar("select min(data) from cvm.info_economica;")
    snap["macro_max_date"] = _safe_scalar("select max(data) from cvm.info_economica;")

    return snap


def _format_quarter(d) -> str:
    if not d:
        return "-"
    try:
        month = d.month
        year = d.year
    except Exception:
        return "-"
    q = (month - 1) // 3 + 1
    return f"{year}T{q}"


def _format_year_range(min_d, max_d) -> str:
    if not min_d or not max_d:
        return "-"
    try:
        return f"{min_d.year} → {max_d.year}"
    except Exception:
        return "-"


# ───────────────────────── Página: Configurações ───────────────────
def render_configuracoes() -> None:
    st.title("Configurações")

    # Estado da UI
    if "cfg_show_diag" not in st.session_state:
        st.session_state["cfg_show_diag"] = False

    # Estado do update/progresso
    if "update_running" not in st.session_state:
        st.session_state["update_running"] = False
    if "update_started_at" not in st.session_state:
        st.session_state["update_started_at"] = None
    if "update_step" not in st.session_state:
        st.session_state["update_step"] = (0, 0)  # idx, total (quando disponível)
    if "update_msg" not in st.session_state:
        st.session_state["update_msg"] = ""
    if "update_pct_hint" not in st.session_state:
        st.session_state["update_pct_hint"] = 0.0  # fallback quando não há STEP i/t

    # Header de status do sync
    st.subheader("Status de atualização (CVM)")
    try:
        status = get_sync_status(engine)
        last_run = status.get("last_run")
        if last_run:
            st.success(f"Última atualização (UTC): {last_run}")
        else:
            st.warning("Nenhuma atualização executada ainda.")
    except Exception as e:
        st.error(f"Não foi possível consultar status da CVM: {e}")

    st.divider()

    # Ações
    c1, c2, c3 = st.columns([1, 1, 2], gap="medium")

    with c1:
        if st.button("Recarregar cache", use_container_width=True):
            st.session_state.pop("setores_df", None)
            # cache_data.clear() existe nas versões recentes; protege em try
            try:
                st.cache_data.clear()
            except Exception:
                pass
            st.success("Cache recarregado.")
            st.rerun()

    with c2:
        if st.button("Diagnóstico", use_container_width=True):
            st.session_state["cfg_show_diag"] = not st.session_state.get("cfg_show_diag", False)

    with c3:
        iniciar = st.button("Atualizar tabelas (CVM)", use_container_width=True, disabled=st.session_state["update_running"])

    # Diagnóstico (no corpo da página, não mais no sidebar)
    if st.session_state.get("cfg_show_diag"):
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

    st.divider()

    # Área de progresso (aparece aqui, na região principal)
    progress_container = st.container()

    def _render_progress_panel() -> None:
        with progress_container:
            progress_container.empty()
            st.subheader("Progresso da atualização")

            idx, total = st.session_state.get("update_step", (0, 0))
            if total and total > 0:
                pct = min(max(idx / total, 0.0), 1.0)
            else:
                # fallback “sensação de progresso” quando não há step definido
                pct = min(max(st.session_state.get("update_pct_hint", 0.0), 0.0), 0.97)

            st.progress(pct)
            st.write(f"**Carregado:** {pct * 100:.1f}%")
            if total and total > 0:
                st.write(f"**Etapa:** {idx}/{total}")
            st.write(f"**Status:** {st.session_state.get('update_msg', '')}")

            started = st.session_state.get("update_started_at")
            if started:
                elapsed = time.time() - started
                st.write(f"**Tempo decorrido:** {int(elapsed)}s")

                if total and total > 0 and idx > 0:
                    per_step = elapsed / idx
                    remaining = per_step * (total - idx)
                    st.write(f"**Tempo restante (estimado):** {int(remaining)}s")

            snap = _get_db_snapshot()

            st.markdown("### Dados já no banco")

            st.write(
                f"**DFP (anual)**: {snap.get('dfp_rows') or 0} linhas | {snap.get('dfp_tickers') or 0} tickers"
            )
            if snap.get("dfp_min_date") and snap.get("dfp_max_date"):
                st.write(
                    f"**Anos inseridos (DFP):** {_format_year_range(snap['dfp_min_date'], snap['dfp_max_date'])} "
                    f"(até {snap['dfp_max_date']})"
                )

            st.write(
                f"**ITR (trimestral)**: {snap.get('itr_rows') or 0} linhas | {snap.get('itr_tickers') or 0} tickers"
            )
            if snap.get("itr_max_date"):
                st.write(
                    f"**Último trimestre (ITR):** {_format_quarter(snap['itr_max_date'])} "
                    f"(data máx: {snap['itr_max_date']})"
                )

            st.write(f"**Macro (info_economica)**: {snap.get('macro_rows') or 0} linhas")
            if snap.get("macro_max_date"):
                st.write(f"**Macro até:** {snap['macro_max_date']}")

            # Última atualização
            try:
                status = get_sync_status(engine)
                last_run = status.get("last_run")
                if last_run:
                    st.markdown("### Última atualização registrada")
                    st.write(f"**Data (UTC):** {last_run}")
            except Exception:
                pass

    # Execução do update com callback (mostra no corpo do app)
    if iniciar and not st.session_state["update_running"]:
        st.session_state["update_running"] = True
        st.session_state["update_started_at"] = time.time()
        st.session_state["update_msg"] = "Iniciando..."
        st.session_state["update_step"] = (0, 0)
        st.session_state["update_pct_hint"] = 0.02
        _render_progress_panel()

        def progress_cb(msg: str):
            # Atualiza mensagem
            st.session_state["update_msg"] = msg

            # Progresso por etapas (se o pipeline emitir STEP i/t)
            if isinstance(msg, str) and msg.startswith("STEP "):
                # Ex.: "STEP 2/6 :: cvm.cvm_dfp_ingest"
                try:
                    header = msg.split("::", 1)[0].strip()  # "STEP 2/6"
                    frac = header.replace("STEP", "").strip()  # "2/6"
                    i_s, t_s = frac.split("/", 1)
                    st.session_state["update_step"] = (int(i_s), int(t_s))
                except Exception:
                    pass
            else:
                # fallback: avança lentamente para “sensação de execução”
                st.session_state["update_pct_hint"] = min(st.session_state.get("update_pct_hint", 0.0) + 0.03, 0.95)

            _render_progress_panel()

        try:
            apply_update(engine, progress_cb=progress_cb)

            # força recarregar base e páginas após update
            st.session_state.pop("setores_df", None)

            st.session_state["update_msg"] = "Concluído com sucesso."
            st.session_state["update_step"] = (1, 1)
            st.session_state["update_pct_hint"] = 1.0
            _render_progress_panel()

        except Exception as e:
            st.session_state["update_msg"] = f"Falha: {e}"
            _render_progress_panel()
            st.error(f"Falha ao atualizar CVM: {e}")
            logger.exception("Falha ao executar apply_update()", exc_info=e)

        finally:
            st.session_state["update_running"] = False

    else:
        # Render “parado” (mostra snapshot e última atualização sem rodar nada)
        _render_progress_panel()


# ───────────────────────── Sidebar navegação ───────────────────────
with st.sidebar:
    st.markdown("## Análises")
    pagina_escolhida = st.radio(
        "Escolha a seção:",
        ["Básica", "Avançada", "Criação de Portfólio", "Configurações"],
        index=0,
    )


# ───────────────────────── Execução / Roteamento ────────────────────
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
