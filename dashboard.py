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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Any

import pandas as pd
import streamlit as st
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.db.engine import get_engine

logger = logging.getLogger(__name__)

# ───────────────────────── Ajuste de path ──────────────────────────
ROOT_DIR = pathlib.Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

# ───────────────────────── Engine (Supabase) ───────────────────────
@st.cache_resource
def _engine() -> Engine:
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
              .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
              [data-testid="stSidebar"] { padding-top: 1rem; }
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

# ───────────────────────── Constantes do Sync ──────────────────────
TARGET_TABLE = "cvm.demonstracoes_financeiras_dfp"  # tabela alvo definida por você

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

# ───────────────────────── Helpers DB (status) ─────────────────────
@dataclass
class SyncStatus:
    last_year: Optional[int]
    last_updated_at: Optional[datetime]
    updates_likely_available: Optional[bool]
    notes: str = ""

def _col_exists(engine: Engine, schema: str, table: str, col: str) -> bool:
    sql = text(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name   = :table
          AND column_name  = :col
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        r = conn.execute(sql, {"schema": schema, "table": table, "col": col}).fetchone()
        return r is not None

def _guess_year_column(engine: Engine, schema: str, table: str) -> Optional[str]:
    """
    Tenta descobrir qual coluna representa o ano na tabela DFP.
    Ajuste aqui se sua coluna de ano tiver nome específico.
    """
    candidates = ["ano", "ano_referencia", "exercicio", "ano_exercicio", "dt_referencia", "data_referencia"]
    for c in candidates:
        if _col_exists(engine, schema, table, c):
            return c
    return None

def _get_sync_status(engine: Engine) -> SyncStatus:
    schema, table = TARGET_TABLE.split(".", 1)
    year_col = _guess_year_column(engine, schema, table)

    last_year = None
    last_updated_at = None
    notes = []

    with engine.connect() as conn:
        # last year
        if year_col:
            if year_col in ("dt_referencia", "data_referencia"):
                q = text(f"SELECT MAX(EXTRACT(YEAR FROM {year_col})::int) AS last_year FROM {TARGET_TABLE}")
            else:
                q = text(f"SELECT MAX({year_col}::int) AS last_year FROM {TARGET_TABLE}")
            last_year = conn.execute(q).scalar()
        else:
            notes.append("Coluna de ano não encontrada automaticamente. Ajuste _guess_year_column().")

        # last updated at (se existir)
        if _col_exists(engine, schema, table, "created_at"):
            q2 = text(f"SELECT MAX(created_at) AS last_updated_at FROM {TARGET_TABLE}")
            last_updated_at = conn.execute(q2).scalar()
        else:
            notes.append("Coluna created_at não existe na tabela alvo; última atualização ficará indisponível.")

    # Heurística simples (sem consulta web):
    # Se last_year < ano atual, provável que exista algo novo para buscar
    now_year = datetime.now(timezone.utc).year
    updates_likely = None
    if last_year is not None:
        updates_likely = last_year < now_year

    return SyncStatus(
        last_year=last_year,
        last_updated_at=last_updated_at,
        updates_likely_available=updates_likely,
        notes=" ".join(notes).strip(),
    )

# ───────────────────────── Invocador do ingest DFP ─────────────────
def _invoke_dfp_ingest(
    engine: Engine,
    start_year: Optional[int],
    end_year: Optional[int],
    progress_cb: Optional[Callable[[float, str], None]] = None,
) -> None:
    """
    Tenta executar o ingest do DFP.
    Procura por funções comuns em cvm.cvm_dfp_ingest ou cvm_dfp_ingest.

    Você pode padronizar seu ingest para aceitar:
      - engine= (SQLAlchemy Engine)
      - start_year / end_year
      - progress_cb(percent_float_0_1, message)

    Se o seu módulo tiver função com outro nome, ajuste o bloco "CALL ORDER".
    """
    mod = _import_first("cvm.cvm_dfp_ingest", "cvm_dfp_ingest")

    # CALL ORDER (tentativas)
    candidates = [
        "run",
        "main",
        "ingest",
        "ingest_dfp",
        "update",
        "sync",
    ]

    fn = None
    for name in candidates:
        f = getattr(mod, name, None)
        if callable(f):
            fn = f
            break

    if fn is None:
        raise ImportError(
            "Não encontrei função executável em cvm_dfp_ingest. "
            "Crie uma função (ex: run(engine, start_year, end_year, progress_cb)) "
            "ou ajuste _invoke_dfp_ingest() para o nome correto."
        )

    # Chamada resiliente: tenta várias assinaturas
    try:
        fn(engine=engine, start_year=start_year, end_year=end_year, progress_cb=progress_cb)
        return
    except TypeError:
        pass

    try:
        fn(engine, start_year, end_year, progress_cb)
        return
    except TypeError:
        pass

    try:
        fn(engine=engine)
        return
    except TypeError:
        pass

    # Último fallback: chama sem args
    fn()

# ───────────────────────── UI: Página Configurações ────────────────
def _render_config_page(engine: Engine) -> None:
    st.markdown("## Configurações")
    st.caption("Atualização e status do banco de dados (Supabase / CVM)")

    status = _get_sync_status(engine)

    # Cards (layout moderno)
    c1, c2, c3 = st.columns(3)

    with c1:
        st.metric(
            "Último ano inserido",
            value=str(status.last_year) if status.last_year is not None else "—",
        )

    with c2:
        if status.last_updated_at:
            try:
                # exibe em horário local do navegador via formatação simples
                st.metric("Última atualização", value=str(status.last_updated_at))
            except Exception:
                st.metric("Última atualização", value="(indisponível)")
        else:
            st.metric("Última atualização", value="—")

    with c3:
        if status.updates_likely_available is None:
            st.metric("Novas atualizações", value="—")
        else:
            st.metric("Novas atualizações", value="Provável" if status.updates_likely_available else "Em dia")

    if status.notes:
        st.info(status.notes)

    st.markdown("---")

    # Controles de atualização
    st.subheader("Atualizar base CVM (DFP)")
    st.caption(f"Tabela alvo: `{TARGET_TABLE}`")

    now_year = datetime.now(timezone.utc).year
    default_start = (status.last_year + 1) if status.last_year else (now_year - 1)
    default_end = now_year

    colA, colB, colC = st.columns([1, 1, 2])
    with colA:
        start_year = st.number_input("Ano inicial", min_value=1990, max_value=now_year, value=int(default_start))
    with colB:
        end_year = st.number_input("Ano final", min_value=1990, max_value=now_year, value=int(default_end))

    if start_year > end_year:
        st.error("Ano inicial não pode ser maior que o ano final.")
        return

    st.markdown("")

    run = st.button("Atualizar agora", type="primary", use_container_width=True)

    if run:
        prog = st.progress(0, text="Iniciando atualização...")
        log_box = st.empty()

        logs: list[str] = []

        def _progress_cb(p: float, msg: str):
            p = max(0.0, min(1.0, float(p)))
            prog.progress(p, text=msg)
            logs.append(f"{int(p*100):3d}% — {msg}")
            log_box.code("\n".join(logs[-30:]), language="text")

        try:
            _progress_cb(0.02, "Preparando ingest...")
            _invoke_dfp_ingest(
                engine=engine,
                start_year=int(start_year),
                end_year=int(end_year),
                progress_cb=_progress_cb,
            )
            _progress_cb(0.98, "Finalizando e validando status...")
            # Recarrega status após ingest
            new_status = _get_sync_status(engine)
            _progress_cb(1.0, "Concluído.")

            st.success("Atualização concluída com sucesso.")
            st.markdown("### Status após atualização")
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("Último ano inserido", str(new_status.last_year) if new_status.last_year else "—")
            with c2:
                st.metric("Última atualização", str(new_status.last_updated_at) if new_status.last_updated_at else "—")
            with c3:
                if new_status.updates_likely_available is None:
                    st.metric("Novas atualizações", "—")
                else:
                    st.metric("Novas atualizações", "Provável" if new_status.updates_likely_available else "Em dia")

        except Exception as e:
            prog.empty()
            st.error("Falha ao atualizar a base.")
            st.exception(e)

# ───────────────────────── Sidebar (Topo + Rodapé) ──────────────────
with st.sidebar:
    # Sidebar sem scroll + layout flex para topo/rodapé
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] { overflow: hidden !important; }
        [data-testid="stSidebarContent"] {
            height: 100vh !important;
            overflow: hidden !important;
            display: flex;
            flex-direction: column;
        }
        .sidebar-top {
            flex: 1 1 auto;
            overflow: hidden;
        }
        .sidebar-bottom {
            flex-shrink: 0;
            padding-top: 0.75rem;
            border-top: 1px solid rgba(255,255,255,0.08);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Topo
    st.markdown('<div class="sidebar-top">', unsafe_allow_html=True)
    st.markdown("## Análises")

    # Mantém a navegação normal no topo
    page_key = st.session_state.get("page_key", "Básica")
    pagina_escolhida = st.radio(
        "Escolha a seção:",
        ["Básica", "Avançada", "Criação de Portfólio"],
        index=["Básica", "Avançada", "Criação de Portfólio"].index(page_key)
        if page_key in ["Básica", "Avançada", "Criação de Portfólio"]
        else 0,
    )
    st.session_state["page_key"] = pagina_escolhida
    st.markdown("</div>", unsafe_allow_html=True)

    # Rodapé: só Configurações
    st.markdown('<div class="sidebar-bottom">', unsafe_allow_html=True)
    if st.button("⚙️ Configurações", use_container_width=True, type="secondary"):
        st.session_state["page_key"] = "Configurações"
    st.markdown("</div>", unsafe_allow_html=True)

# ───────────────────────── Execução / Roteamento ────────────────────
try:
    _ensure_setores_df()
except Exception as e:
    st.error(f"Falha ao inicializar dados base (setores_df): {e}")
    st.stop()

# Decide qual página renderizar
current = st.session_state.get("page_key", "Básica")

if current == "Configurações":
    _render_config_page(engine=_engine())
else:
    try:
        render_page = _load_page_renderer(current)
        render_page()
    except Exception as e:
        st.error("Falha ao carregar a página selecionada.")
        st.exception(e)
