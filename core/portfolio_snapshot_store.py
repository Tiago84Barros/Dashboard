# -*- coding: utf-8 -*-
"""
portfolio_snapshot_store.py

Compat layer for snapshot persistence in Supabase.

- Works with supabase-py *if installed*.
- Falls back to Supabase PostgREST (requests) if supabase-py is not available.

Schema (user-provided):
  public.portfolio_snapshots:
    id uuid PK
    created_at timestamptz default now()
    selic_ref numeric null
    margem_superior numeric null
    tipo_empresa text null
    filters_json jsonb not null default '{}'
    plan_hash text not null UNIQUE
    status text not null default 'active'
    notes text null

Expected items table:
  public.portfolio_snapshot_items:
    snapshot_id uuid FK -> portfolio_snapshots(id)
    ticker text
    peso numeric
    (optional) setor/subsetor/segmento/ano_compra/motivos...

This module provides:
  - get_latest_snapshot()
  - list_snapshots()
  - get_snapshot(snapshot_id)
  - save_snapshot(...)
"""

from __future__ import annotations

import json
import os
import hashlib
from typing import Any, Dict, List, Optional, Tuple

SNAP_TABLE = os.getenv("SNAPSHOT_TABLE", "portfolio_snapshots")
ITEMS_TABLE = os.getenv("SNAPSHOT_ITEMS_TABLE", "portfolio_snapshot_items")

# -----------------------------
# Hash helpers (deterministic)
# -----------------------------
def _json_deterministico(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def compute_plan_hash(payload: Dict[str, Any]) -> str:
    """MD5 over deterministic JSON. Should NOT include created_at/timestamps."""
    s = _json_deterministico(payload)
    return hashlib.md5(s.encode("utf-8")).hexdigest()

# -----------------------------
# Client selection
# -----------------------------
def _try_get_supabase_py():
    try:
        from supabase import create_client  # type: ignore
        return create_client
    except Exception:
        return None

def _get_env() -> Tuple[str, str]:
    url = os.getenv("SUPABASE_URL") or os.getenv("SUPABASE_PROJECT_URL") or ""
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_KEY") or ""
    if not url or not key:
        raise RuntimeError(
            "Supabase não configurado. Defina SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY (ou SUPABASE_ANON_KEY)."
        )
    return url.rstrip("/"), key

def _rest_headers(key: str) -> Dict[str, str]:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "return=representation",
    }

def _rest_base(url: str) -> str:
    return f"{url}/rest/v1"

def _http():
    try:
        import requests  # type: ignore
        return requests
    except Exception as e:
        raise ImportError("requests não está disponível no ambiente para fallback REST do Supabase.") from e

def _mode():
    create_client = _try_get_supabase_py()
    if create_client is not None:
        try:
            url, key = _get_env()
            sb = create_client(url, key)
            return ("supabase", sb)
        except Exception:
            # If env isn't ready, still allow REST error message later
            pass
    return ("rest", None)

# -----------------------------
# REST helpers
# -----------------------------
def _rest_get(table: str, params: Dict[str, str]) -> List[Dict[str, Any]]:
    url, key = _get_env()
    requests = _http()
    endpoint = f"{_rest_base(url)}/{table}"
    r = requests.get(endpoint, headers=_rest_headers(key), params=params, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Supabase REST GET falhou ({r.status_code}): {r.text}")
    return r.json() if r.text else []

def _rest_post(table: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    url, key = _get_env()
    requests = _http()
    endpoint = f"{_rest_base(url)}/{table}"
    r = requests.post(endpoint, headers=_rest_headers(key), data=json.dumps(rows), timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Supabase REST POST falhou ({r.status_code}): {r.text}")
    return r.json() if r.text else []

def _rest_patch(table: str, params: Dict[str, str], patch_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    url, key = _get_env()
    requests = _http()
    endpoint = f"{_rest_base(url)}/{table}"
    # PATCH with filters in query string
    hdrs = _rest_headers(key).copy()
    hdrs["Prefer"] = "return=representation"
    r = requests.patch(endpoint, headers=hdrs, params=params, data=json.dumps(patch_obj), timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Supabase REST PATCH falhou ({r.status_code}): {r.text}")
    return r.json() if r.text else []

def _rest_delete(table: str, params: Dict[str, str]) -> None:
    url, key = _get_env()
    requests = _http()
    endpoint = f"{_rest_base(url)}/{table}"
    r = requests.delete(endpoint, headers=_rest_headers(key), params=params, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Supabase REST DELETE falhou ({r.status_code}): {r.text}")

# -----------------------------
# Public API expected by Patch 6
# -----------------------------
def list_snapshots(limit: int = 25, status: str = "active"):
    """
    Returns a pandas.DataFrame with snapshot headers.
    Patch 6 expects columns at least: id, created_at.
    """
    try:
        import pandas as pd  # type: ignore
    except Exception:
        pd = None  # type: ignore

    mode, sb = _mode()

    fields = "id,created_at,selic_ref,margem_superior,tipo_empresa,plan_hash,status"
    if mode == "supabase":
        try:
            q = sb.table(SNAP_TABLE).select(fields).order("created_at", desc=True).limit(int(limit))
            if status:
                q = q.eq("status", status)
            resp = q.execute()
            data = getattr(resp, "data", None) or []
        except Exception as e:
            raise RuntimeError(f"Falha ao listar snapshots via supabase-py: {type(e).__name__}: {e}") from e
    else:
        params = {
            "select": fields,
            "order": "created_at.desc",
            "limit": str(int(limit)),
        }
        if status:
            params["status"] = f"eq.{status}"
        data = _rest_get(SNAP_TABLE, params)

    if pd is None:
        return data  # fallback: list[dict]
    return pd.DataFrame(data)

def get_snapshot(snapshot_id: str) -> Optional[Dict[str, Any]]:
    """Returns snapshot dict with 'items' included."""
    if not snapshot_id:
        return None
    mode, sb = _mode()

    fields = "id,created_at,selic_ref,margem_superior,tipo_empresa,filters_json,plan_hash,status,notes"
    if mode == "supabase":
        try:
            resp = sb.table(SNAP_TABLE).select(fields).eq("id", snapshot_id).limit(1).execute()
            rows = getattr(resp, "data", None) or []
            header = rows[0] if rows else None
        except Exception as e:
            raise RuntimeError(f"Falha ao buscar snapshot via supabase-py: {type(e).__name__}: {e}") from e
        if not header:
            return None
        try:
            resp2 = sb.table(ITEMS_TABLE).select("*").eq("snapshot_id", snapshot_id).execute()
            items = getattr(resp2, "data", None) or []
        except Exception:
            items = []
    else:
        header_rows = _rest_get(SNAP_TABLE, {"select": fields, "id": f"eq.{snapshot_id}", "limit": "1"})
        header = header_rows[0] if header_rows else None
        if not header:
            return None
        items = _rest_get(ITEMS_TABLE, {"select": "*", "snapshot_id": f"eq.{snapshot_id}"})

    header["items"] = items
    return header

def get_latest_snapshot(status: str = "active") -> Optional[Dict[str, Any]]:
    """Convenience: latest snapshot header+items."""
    try:
        df = list_snapshots(limit=1, status=status)
        if df is None:
            return None
        # df can be list[dict] if pandas missing
        if isinstance(df, list):
            if not df:
                return None
            return get_snapshot(df[0].get("id"))
        if df.empty:
            return None
        snap_id = str(df.iloc[0]["id"])
        return get_snapshot(snap_id)
    except Exception:
        return None

def save_snapshot(
    *,
    selic_ref: Optional[float],
    margem_superior: Optional[float],
    tipo_empresa: Optional[str],
    filters_json: Optional[Dict[str, Any]],
    plan_hash: str,
    items: List[Dict[str, Any]],
    status: str = "active",
    notes: Optional[str] = None,
) -> str:
    """
    Upsert snapshot header by plan_hash, then replace items.
    Returns snapshot_id.
    """
    filters_json = filters_json or {}
    mode, sb = _mode()

    header_row = {
        "selic_ref": selic_ref,
        "margem_superior": margem_superior,
        "tipo_empresa": tipo_empresa,
        "filters_json": filters_json,
        "plan_hash": plan_hash,
        "status": status,
        "notes": notes,
    }

    if mode == "supabase":
        try:
            # upsert by plan_hash
            resp = sb.table(SNAP_TABLE).upsert(header_row, on_conflict="plan_hash").execute()
            rows = getattr(resp, "data", None) or []
            if not rows:
                # fetch by plan_hash
                resp2 = sb.table(SNAP_TABLE).select("id").eq("plan_hash", plan_hash).limit(1).execute()
                rows2 = getattr(resp2, "data", None) or []
                snapshot_id = rows2[0]["id"] if rows2 else None
            else:
                snapshot_id = rows[0]["id"]
            if not snapshot_id:
                raise RuntimeError("Upsert não retornou snapshot_id.")
            # replace items
            try:
                sb.table(ITEMS_TABLE).delete().eq("snapshot_id", snapshot_id).execute()
            except Exception:
                pass
            if items:
                rows_items = []
                for it in items:
                    r = dict(it)
                    r["snapshot_id"] = snapshot_id
                    rows_items.append(r)
                sb.table(ITEMS_TABLE).insert(rows_items).execute()
            return str(snapshot_id)
        except Exception as e:
            raise RuntimeError(f"Falha ao salvar snapshot via supabase-py: {type(e).__name__}: {e}") from e

    # REST path
    # 1) upsert em REST: PostgREST não tem upsert simples sem 'on_conflict' via query param.
    # We implement: try PATCH by plan_hash; if no row updated, POST.
    existing = _rest_get(SNAP_TABLE, {"select": "id", "plan_hash": f"eq.{plan_hash}", "limit": "1"})
    if existing:
        snapshot_id = existing[0]["id"]
        _rest_patch(SNAP_TABLE, {"plan_hash": f"eq.{plan_hash}"}, header_row)
    else:
        created = _rest_post(SNAP_TABLE, [header_row])
        if not created:
            # fetch again
            created = _rest_get(SNAP_TABLE, {"select": "id", "plan_hash": f"eq.{plan_hash}", "limit": "1"})
        snapshot_id = created[0]["id"] if created else None
    if not snapshot_id:
        raise RuntimeError("Não foi possível obter snapshot_id após upsert/insert (REST).")

    # 2) replace items
    try:
        _rest_delete(ITEMS_TABLE, {"snapshot_id": f"eq.{snapshot_id}"})
    except Exception:
        pass
    if items:
        rows_items = []
        for it in items:
            r = dict(it)
            r["snapshot_id"] = snapshot_id
            rows_items.append(r)
        _rest_post(ITEMS_TABLE, rows_items)

    return str(snapshot_id)
