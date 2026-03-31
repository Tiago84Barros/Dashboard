# core/ui_bridge.py
# Streamlit UI bridge — caching and error surface between page/ and core.db.
#
# Responsibilities:
#   - @st.cache_resource / @st.cache_data decorators
#   - Convert exceptions from core.db into st.error() messages with safe fallbacks
#   - Single place to tune cache TTLs across the app
#
# Rules:
#   - May import streamlit freely
#   - Must only call core.db for data — never raw SQLAlchemy
#   - core/db.py must not be modified for UI concerns
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import streamlit as st

import core.db as _db
from core.db import make_doc_hash  # pure — re-exported for convenience
from core.ticker_utils import normalize_ticker


# ────────────────────────────────────────────────────────────────────────────────
# Engine
# ────────────────────────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def get_supabase_engine():
    """Engine singleton, cached as a Streamlit resource."""
    return _db.get_engine()


# ────────────────────────────────────────────────────────────────────────────────
# Setores
# ────────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_setores_from_db() -> pd.DataFrame | None:
    try:
        return _db.load_setores_from_db()
    except Exception as e:
        st.error(f"Erro ao carregar tabela 'setores' do Supabase: {e}")
        return None


# Legacy alias kept for backward-compat
@st.cache_data(show_spinner=False)
def load_setores_from_supabase() -> pd.DataFrame | None:
    return load_setores_from_db()


# ────────────────────────────────────────────────────────────────────────────────
# Demonstrações financeiras
# ────────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_data_from_db(ticker: str) -> pd.DataFrame | None:
    try:
        return _db.load_data_from_db(ticker)
    except Exception as e:
        st.error(f"Erro ao carregar DRE (DFP) para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_data_tri_from_db(ticker: str) -> pd.DataFrame | None:
    try:
        return _db.load_data_tri_from_db(ticker)
    except Exception as e:
        st.error(f"Erro ao carregar TRI para {ticker}: {e}")
        return None


# ────────────────────────────────────────────────────────────────────────────────
# Múltiplos
# ────────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_multiplos_from_db(ticker: str) -> pd.DataFrame | None:
    try:
        return _db.load_multiplos_from_db(ticker)
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos (anuais) para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_multiplos_limitado_from_db(ticker: str, limite: int = 250) -> pd.DataFrame | None:
    try:
        return _db.load_multiplos_limitado_from_db(ticker, limite)
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos limitados para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_multiplos_tri_from_db(ticker: str) -> pd.DataFrame | None:
    try:
        return _db.load_multiplos_tri_from_db(ticker)
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos TRI para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_multiplos_tri_hist_from_db(ticker: str, limite: int = 250) -> pd.DataFrame | None:
    try:
        return _db.load_multiplos_tri_hist_from_db(ticker, limite)
    except Exception as e:
        st.error(f"Erro ao carregar histórico múltiplos TRI para {ticker}: {e}")
        return None


# ────────────────────────────────────────────────────────────────────────────────
# Macro
# ────────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_macro_summary() -> pd.DataFrame | None:
    try:
        return _db.load_macro_summary()
    except Exception as e:
        st.error(f"Erro ao carregar macro (info_economica): {e}")
        return None


@st.cache_data(show_spinner=False)
def load_macro_mensal() -> pd.DataFrame | None:
    try:
        return _db.load_macro_mensal()
    except Exception as e:
        st.error(f"Erro ao carregar macro mensal (info_economica_mensal): {e}")
        return None


# ────────────────────────────────────────────────────────────────────────────────
# Documentos corporativos (Patch 6 / RAG)
# ────────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=60 * 30)
def load_docs_corporativos_by_ticker(
    tickers: List[str],
    limit_per_ticker: int = 8,
    days_back: int = 365,
) -> Dict[str, List[Dict[str, Any]]]:
    try:
        return _db.load_docs_corporativos_by_ticker(tickers, limit_per_ticker, days_back)
    except Exception as e:
        st.error(f"Erro ao carregar docs_corporativos: {e}")
        tks = [normalize_ticker(t) for t in (tickers or []) if str(t or "").strip()]
        return {tk: [] for tk in tks if tk}


@st.cache_data(show_spinner=False, ttl=6 * 60 * 60)
def load_docs_corporativos_from_db(
    ticker: str,
    *,
    limit: int = 20,
    tipos: list[str] | None = None,
    fontes: list[str] | None = None,
) -> pd.DataFrame | None:
    try:
        return _db.load_docs_corporativos_from_db(ticker, limit=limit, tipos=tipos, fontes=fontes)
    except Exception as e:
        st.error(f"Erro ao carregar docs corporativos para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False, ttl=6 * 60 * 60)
def load_docs_corporativos_chunks_from_db(
    ticker: str,
    *,
    limit_docs: int = 12,
    limit_chunks_per_doc: int = 6,
) -> pd.DataFrame | None:
    try:
        return _db.load_docs_corporativos_chunks_from_db(
            ticker, limit_docs=limit_docs, limit_chunks_per_doc=limit_chunks_per_doc
        )
    except Exception as e:
        st.error(f"Erro ao carregar chunks para {ticker}: {e}")
        return None


# ────────────────────────────────────────────────────────────────────────────────
# Public interface
# ────────────────────────────────────────────────────────────────────────────────

__all__ = [
    "get_supabase_engine",
    "make_doc_hash",
    "load_setores_from_db",
    "load_setores_from_supabase",
    "load_data_from_db",
    "load_data_tri_from_db",
    "load_multiplos_from_db",
    "load_multiplos_limitado_from_db",
    "load_multiplos_tri_from_db",
    "load_multiplos_tri_hist_from_db",
    "load_macro_summary",
    "load_macro_mensal",
    "load_docs_corporativos_by_ticker",
    "load_docs_corporativos_from_db",
    "load_docs_corporativos_chunks_from_db",
]
