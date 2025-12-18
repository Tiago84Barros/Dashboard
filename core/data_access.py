# core/data_access.py
from __future__ import annotations

import os
import importlib
from typing import Any

def get_data_source() -> str:
    ds = (os.getenv("DATA_SOURCE") or "sqlite").strip().lower()
    if ds not in ("sqlite", "supabase"):
        raise RuntimeError("DATA_SOURCE inválido. Use 'sqlite' ou 'supabase'.")
    return ds

def _load_backend():
    ds = get_data_source()
    if ds == "supabase":
        return importlib.import_module("core.db_loader_supabase")
    return importlib.import_module("core.db_loader")

_backend = _load_backend()

# Reexporta as funções padronizadas (mesma assinatura do SQLite)
load_setores_from_db = getattr(_backend, "load_setores_from_db")
load_data_from_db = getattr(_backend, "load_data_from_db")
load_multiplos_from_db = getattr(_backend, "load_multiplos_from_db")
load_multiplos_limitado_from_db = getattr(_backend, "load_multiplos_limitado_from_db")
load_multiplos_tri_from_db = getattr(_backend, "load_multiplos_tri_from_db")
load_macro_summary = getattr(_backend, "load_macro_summary")
