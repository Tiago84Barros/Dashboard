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
from typing import Callable, Optional, Tuple

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
    # tenta pegar o engine supabase do core
    mod = _import_first("core.db_supabase", "db_supabase")
    if hasattr(mod, "get_engine"):
        return mod.get_engine()
    if hasattr(mod, "engine"):
        return mod.engine
    raise ImportError("Não encontrei get_engine() em core.db_supabase/db_supabase.")


# ───────────────────────── CVM sync API ──────────────────────────
_sync_mod = _import_first("core.cvm_sync", "cvm_sync
