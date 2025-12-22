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


# ───────────────────────── UI Helpers ──────────────────────────
def _fmt_mmss(seconds: int) -> str:
    seconds = max(0, int(seconds))
    m = seconds // 60
    s = seconds %
