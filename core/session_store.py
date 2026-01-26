# core/session_store.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from datetime import datetime

import streamlit as st

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


@dataclass(frozen=True)
class RunStoreConfig:
    namespace: str = "default"
    max_runs: int = 10


def _store_key(cfg: RunStoreConfig) -> str:
    return f"_run_store__{cfg.namespace}"


def _meta_key(cfg: RunStoreConfig) -> str:
    return f"_run_store_meta__{cfg.namespace}"


def _force_key(cfg: RunStoreConfig) -> str:
    return f"_run_force_render_saved__{cfg.namespace}"


def _last_key(cfg: RunStoreConfig) -> str:
    return f"_run_last__{cfg.namespace}"


def _now_sp() -> datetime:
    """
    Padroniza timestamp no fuso de São Paulo.
    """
    if ZoneInfo is None:
        return datetime.now()
    try:
        return datetime.now(ZoneInfo("America/Sao_Paulo"))
    except Exception:
        return datetime.now()


def set_force_render_saved(cfg: RunStoreConfig, v: bool) -> None:
    st.session_state[_force_key(cfg)] = bool(v)


def consume_force_render_saved(cfg: RunStoreConfig) -> bool:
    k = _force_key(cfg)
    v = bool(st.session_state.get(k, False))
    st.session_state[k] = False
    return v


def clear_runs(cfg: RunStoreConfig) -> None:
    st.session_state[_store_key(cfg)] = {}
    st.session_state[_meta_key(cfg)] = {}
    st.session_state[_last_key(cfg)] = None


def save_run(cfg: RunStoreConfig, run_key: str, payload: Dict[str, Any]) -> None:
    """
    Salva um "run" na sessão com metadados amigáveis.

    Mantém:
      - store[run_key] = payload
      - meta[run_key] = {created_at_iso, created_at, label, ...}
      - last_run_key por namespace
    """
    store: Dict[str, Any] = st.session_state.setdefault(_store_key(cfg), {})
    meta: Dict[str, Dict[str, Any]] = st.session_state.setdefault(_meta_key(cfg), {})

    dt = _now_sp()
    created_at_iso = dt.isoformat(timespec="seconds")
    created_at_human = dt.strftime("%Y-%m-%d %H:%M:%S")

    margem = payload.get("margem_superior", None)
    v2 = payload.get("use_score_v2", None)
    n = len(payload.get("empresas_lideres_finais", []) or [])

    label = f"{created_at_human} | margem={margem} | v2={v2} | líderes={n}"

    store[run_key] = payload
    meta[run_key] = {
        "created_at_iso": created_at_iso,
        "created_at": created_at_human,
        "label": label,
        "margem_superior": margem,
        "use_score_v2": v2,
        "n_lideres": n,
    }

    # mantém a última execução
    st.session_state[_last_key(cfg)] = run_key

    # trim para não crescer indefinidamente
    keys = list(meta.keys())
    if len(keys) > cfg.max_runs:
        def _iso(k: str) -> str:
            return str((meta.get(k, {}) or {}).get("created_at_iso", ""))

        # mais antigos primeiro
        keys_sorted = sorted(keys, key=_iso)
        for k in keys_sorted[:-cfg.max_runs]:
            store.pop(k, None)
            meta.pop(k, None)


def load_run(cfg: RunStoreConfig, run_key: Optional[str]) -> Optional[Dict[str, Any]]:
    if not run_key:
        return None
    store = st.session_state.get(_store_key(cfg), {})
    return store.get(run_key)


def last_run_key(cfg: RunStoreConfig) -> Optional[str]:
    return st.session_state.get(_last_key(cfg))


def last_run_label(cfg: RunStoreConfig) -> Optional[str]:
    rk = last_run_key(cfg)
    if not rk:
        return None
    meta = st.session_state.get(_meta_key(cfg), {})
    return (meta.get(rk) or {}).get("label")


def list_runs(cfg: RunStoreConfig) -> List[Dict[str, str]]:
    """
    Retorna lista (mais recente primeiro):
      [{"key": "...", "label": "...", "created_at_iso": "...", "created_at": "..."}, ...]
    """
    meta = st.session_state.get(_meta_key(cfg), {}) or {}
    out: List[Dict[str, str]] = []

    for k, m in meta.items():
        m = m or {}
        out.append(
            {
                "key": str(k),
                "label": str(m.get("label", k)),
                "created_at_iso": str(m.get("created_at_iso", "")),
                "created_at": str(m.get("created_at", "")),
            }
        )

    out = sorted(out, key=lambda item: item.get("created_at_iso", ""), reverse=True)
    return out


# Se você já tem make_run_key, mantenha; não é obrigatório mudar.
def make_run_key(cfg: RunStoreConfig, **kwargs) -> str:
    import hashlib
    import json

    raw = json.dumps(kwargs, default=str, sort_keys=True).encode("utf-8")
    return hashlib.md5(raw).hexdigest()
