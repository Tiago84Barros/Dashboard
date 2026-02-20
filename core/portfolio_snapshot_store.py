from __future__ import annotations

import json
import hashlib
import os
from typing import Any, Dict, List, Optional, Tuple

def _get_env(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) and v.strip() else None

def _get_supabase_client():
    try:
        from supabase import create_client  # type: ignore
    except Exception as e:
        raise ImportError("supabase-py não está instalado/ativo no ambiente.") from e

    url = _get_env("SUPABASE_URL") or _get_env("SUPABASE_PROJECT_URL")
    key = _get_env("SUPABASE_SERVICE_ROLE_KEY") or _get_env("SUPABASE_ANON_KEY")
    if not url or not key:
        try:
            import streamlit as st  # type: ignore
            url = url or st.secrets.get("SUPABASE_URL")
            key = key or st.secrets.get("SUPABASE_SERVICE_ROLE_KEY") or st.secrets.get("SUPABASE_ANON_KEY")
        except Exception:
            pass
    if not url or not key:
        raise ValueError("Credenciais do Supabase ausentes (SUPABASE_URL e SUPABASE_*_KEY).")
    return create_client(url, key)

SNAP_TABLE = "portfolio_snapshots"
ITEMS_TABLE = "portfolio_snapshot_items"

def _stable_hash(payload: Dict[str, Any]) -> str:
    s = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def save_snapshot(
    *,
    items: List[Dict[str, Any]],
    selic_ref: Optional[float],
    margem_superior: Optional[float],
    tipo_empresa: Optional[str],
    filters_json: Dict[str, Any],
    notes: Optional[str] = None,
) -> Tuple[Optional[str], str]:
    norm_items = []
    for it in items or []:
        tk = str(it.get("ticker", "")).strip().upper().replace(".SA", "")
        if not tk:
            continue
        peso = float(it.get("peso", 0.0) or 0.0)
        norm_items.append({"ticker": tk, "peso": peso})

    plan_payload = {
        "items": norm_items,
        "selic_ref": selic_ref,
        "margem_superior": margem_superior,
        "tipo_empresa": tipo_empresa,
        "filters_json": filters_json or {},
    }
    plan_hash = _stable_hash(plan_payload)

    sb = _get_supabase_client()

    header = {
        "selic_ref": selic_ref,
        "margem_superior": margem_superior,
        "tipo_empresa": tipo_empresa,
        "filters_json": filters_json or {},
        "plan_hash": plan_hash,
        "status": "active",
        "notes": notes,
    }

    res = sb.table(SNAP_TABLE).upsert(header, on_conflict="plan_hash").execute()
    data = getattr(res, "data", None) or []
    snapshot_id = None
    if data and isinstance(data, list):
        snapshot_id = data[0].get("id")
    if not snapshot_id:
        q = sb.table(SNAP_TABLE).select("id").eq("plan_hash", plan_hash).limit(1).execute()
        qd = getattr(q, "data", None) or []
        snapshot_id = qd[0].get("id") if qd else None

    if not snapshot_id:
        return None, plan_hash

    sb.table(ITEMS_TABLE).delete().eq("snapshot_id", snapshot_id).execute()
    if norm_items:
        rows = [{"snapshot_id": snapshot_id, **it} for it in norm_items]
        sb.table(ITEMS_TABLE).insert(rows).execute()

    return str(snapshot_id), plan_hash

def get_latest_snapshot() -> Optional[Dict[str, Any]]:
    sb = _get_supabase_client()
    res = sb.table(SNAP_TABLE).select("*").order("created_at", desc=True).limit(1).execute()
    data = getattr(res, "data", None) or []
    return data[0] if data else None

def get_snapshot(snapshot_id: str) -> Optional[Dict[str, Any]]:
    sb = _get_supabase_client()
    res = sb.table(SNAP_TABLE).select("*").eq("id", snapshot_id).limit(1).execute()
    data = getattr(res, "data", None) or []
    return data[0] if data else None

def list_snapshots(limit: int = 20) -> List[Dict[str, Any]]:
    sb = _get_supabase_client()
    res = sb.table(SNAP_TABLE).select("*").order("created_at", desc=True).limit(limit).execute()
    return getattr(res, "data", None) or []

def get_snapshot_items(snapshot_id: str) -> List[Dict[str, Any]]:
    sb = _get_supabase_client()
    res = sb.table(ITEMS_TABLE).select("*").eq("snapshot_id", snapshot_id).execute()
    return getattr(res, "data", None) or []
