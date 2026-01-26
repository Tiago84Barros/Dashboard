# =========================
# ARQUIVO 1/3
# core/session_store.py
# =========================
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import hashlib
import json

import pandas as pd
import streamlit as st


@dataclass(frozen=True)
class RunStoreConfig:
    """
    Namespace por página (ex.: 'portfolio', 'advanced').
    """
    namespace: str


def _schema_signature(df: Optional[pd.DataFrame]) -> str:
    """
    Assinatura leve do schema (colunas + dtype).
    Não depende do conteúdo (rápido e estável).
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return "empty"

    payload = [(str(c), str(df[c].dtype)) for c in df.columns]
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return hashlib.md5(raw).hexdigest()


def make_run_key(
    cfg: RunStoreConfig,
    *,
    params: Dict[str, Any],
    setores_df: Optional[pd.DataFrame] = None,
    macro_df: Optional[pd.DataFrame] = None,
) -> str:
    """
    Gera uma chave determinística para a execução.
    Inclui params (ordenado) + assinatura de schema de setores/macro para evitar reuso indevido.
    """
    params_norm = json.dumps(params, ensure_ascii=False, sort_keys=True, default=str)
    setores_sig = _schema_signature(setores_df)
    macro_sig = _schema_signature(macro_df)

    raw = f"{cfg.namespace}|params={params_norm}|setores={setores_sig}|macro={macro_sig}".encode("utf-8")
    return hashlib.md5(raw).hexdigest()


def _runs_key(cfg: RunStoreConfig) -> str:
    return f"{cfg.namespace}_runs"


def _last_key(cfg: RunStoreConfig) -> str:
    return f"{cfg.namespace}_last_run_key"


def list_runs(cfg: RunStoreConfig) -> Dict[str, Dict[str, Any]]:
    """
    Retorna dict {run_key: payload}.
    """
    return st.session_state.get(_runs_key(cfg), {})


def save_run(cfg: RunStoreConfig, run_key: str, payload: Dict[str, Any]) -> None:
    """
    Salva payload no session_state e marca como último.
    """
    runs = st.session_state.setdefault(_runs_key(cfg), {})
    payload = dict(payload)
    payload.setdefault("created_at", datetime.now().isoformat(timespec="seconds"))

    runs[run_key] = payload
    st.session_state[_last_key(cfg)] = run_key


def load_run(cfg: RunStoreConfig, run_key: Optional[str]) -> Optional[Dict[str, Any]]:
    if not run_key:
        return None
    return list_runs(cfg).get(run_key)


def last_run_key(cfg: RunStoreConfig) -> Optional[str]:
    return st.session_state.get(_last_key(cfg))


def clear_runs(cfg: RunStoreConfig) -> None:
    st.session_state.pop(_runs_key(cfg), None)
    st.session_state.pop(_last_key(cfg), None)


def set_force_render_saved(cfg: RunStoreConfig, value: bool = True) -> None:
    st.session_state[f"{cfg.namespace}_force_render_saved"] = bool(value)


def consume_force_render_saved(cfg: RunStoreConfig) -> bool:
    k = f"{cfg.namespace}_force_render_saved"
    v = bool(st.session_state.get(k, False))
    st.session_state[k] = False
    return v
