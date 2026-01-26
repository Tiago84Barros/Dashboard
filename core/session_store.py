# core/session_store.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from datetime import datetime
from zoneinfo import ZoneInfo

import streamlit as st


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


def _now_sp() -> datetime:
    return datetime.now(ZoneInfo("America/Sao_Paulo"))


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
    st.session_state[f"_run_last__{cfg.namespace}"] = None


def save_run(cfg: RunStoreConfig, run_key: str, payload: Dict[str, Any]) -> None:
    store = st.session_state.setdefault(_store_key(cfg), {})
    meta = st.session_state.setdefault(_meta_key(cfg), {})

    # cria label amigável
    dt = _now_sp()
    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")

    margem = payload.get("margem_superior", None)
    v2 = payload.get("use_score_v2", None)
    n = len(payload.get("empresas_lideres_finais", []) or [])

    label = f"{dt_str} | margem={margem} | v2={v2} | líderes={n}"

    store[run_key] = payload
    meta[run_key] = {"created_at": dt_str, "label": label}

    # mantém a última execução
    st.session_state[f"_run_last__{cfg.namespace}"] = run_key

    # trim para não crescer indefinidamente
    keys = list(meta.keys())
    if len(keys) > cfg.max_runs:
        # remove os mais antigos pelo created_at
        def _dt(k: str) -> str:
            return str(meta.get(k, {}).get("created_at", ""))

        keys_sorted = sorted(keys, key=_dt)
        for k in keys_sorted[:-cfg.max_runs]:
            store.pop(k, None)
            meta.pop(k, None)


def load_run(cfg: RunStoreConfig, run_key: Optional[str]) -> Optional[Dict[str, Any]]:
    if not run_key:
        return None
    store = st.session_state.get(_store_key(cfg), {})
    return store.get(run_key)


def last_run_key(cfg: RunStoreConfig) -> Optional[str]:
    return st.session_state.get(f"_run_last__{cfg.namespace}")


def last_run_label(cfg: RunStoreConfig) -> Optional[str]:
    rk = last_run_key(cfg)
    if not rk:
        return None
    meta = st.session_state.get(_meta_key(cfg), {})
    return (meta.get(rk) or {}).get("label")


def list_runs(cfg: RunStoreConfig) -> List[Dict[str, str]]:
    meta = st.session_state.get(_meta_key(cfg), {})
    # retorna lista já amigável para UI
    out: List[Dict[str, str]] = []
    for k, m in meta.items():
        out.append({"key": k, "label": str(m.get("label", k))})
    # ordena mais recente primeiro
    def _created(item: Dict[str, str]) -> str:
        return item["label"].split("|")[0].strip()

    out = sorted(out, key=_created, reverse=True)
    return out


# se você já tem make_run_key, mantenha; não é obrigatório mudar.
def make_run_key(cfg: RunStoreConfig, **kwargs) -> str:
    # se você já tinha hash, pode manter; não precisa exibir isso na UI
    import hashlib, json
    raw = json.dumps(kwargs, default=str, sort_keys=True).encode("utf-8")
    return hashlib.md5(raw).hexdigest()
