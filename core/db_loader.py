# core/db_loader.py
# Compatibility shim — do NOT add logic here.
# All Streamlit caching and error handling lives in core/ui_bridge.py.
# All pure data access lives in core/db.py.
#
# Existing callers (page/, pickup/, core/) that import from here continue
# to work unchanged. New code should import from core.ui_bridge directly.
from __future__ import annotations

from core.ui_bridge import (  # noqa: F401
    get_supabase_engine,
    make_doc_hash,
    load_setores_from_db,
    load_setores_from_supabase,
    load_data_from_db,
    load_data_tri_from_db,
    load_multiplos_from_db,
    load_multiplos_limitado_from_db,
    load_multiplos_tri_from_db,
    load_multiplos_tri_hist_from_db,
    load_macro_summary,
    load_macro_mensal,
    load_docs_corporativos_by_ticker,
    load_docs_corporativos_from_db,
    load_docs_corporativos_chunks_from_db,
)

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
