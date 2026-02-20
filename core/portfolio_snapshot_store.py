from __future__ import annotations

import json
import os
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

# Supabase client (supabase-py)
def _get_client():
    try:
        from supabase import create_client  # type: ignore
    except Exception:
        return None

    url = os.environ.get("SUPABASE_URL") or os.environ.get("SUPABASE_PROJECT_URL")
    key = os.environ.get("SUPABASE_ANON_KEY") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
    if not url or not key:
        return None
    try:
        return create_client(url, key)
    except Exception:
        return None


# Compatível com o schema informado pelo usuário (Supabase / Postgres)
# Header: public.portfolio_snapshots (plan_hash único)
# Items:  public.portfolio_snapshot_items (FK snapshot_id)
SNAP_TABLE = os.environ.get("SNAPSHOT_TABLE", "portfolio_snapshots")
ITEMS_TABLE = os.environ.get("SNAPSHOT_ITEMS_TABLE", "portfolio_snapshot_items")


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def _det_hash(payload: Dict[str, Any]) -> str:
    try:
        s = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.md5(s.encode("utf-8")).hexdigest()
    except Exception:
        return ""


def save_snapshot(
    tickers: List[Dict[str, Any]],
    selic_ref: float,
    margem_superior: float,
    tipo_empresa: str,
    hash_value: str = "",
    filters_json: Optional[Dict[str, Any]] = None,
    notes: Optional[str] = None,
) -> Optional[str]:
    """Salva snapshot (header + items). Retorna snapshot_id (uuid/str) ou None."""
    sb = _get_client()
    if sb is None:
        return None

    tickers = tickers or []
    # normaliza itens
    items = []
    for it in tickers:
        tk = str(it.get("ticker") or "").strip().upper().replace(".SA", "")
        if not tk:
            continue
        items.append({"ticker": tk, "peso": float(it.get("peso") or 0.0)})

    # Hash determinístico do plano (idealmente NÃO depende do timestamp)
    plan_payload = {
        "tickers": items,
        "selic_ref": float(selic_ref),
        "margem_superior": float(margem_superior),
        "tipo_empresa": str(tipo_empresa),
        "filters_json": filters_json or {},
    }
    hv = str(hash_value or _det_hash(plan_payload))

    header = {
        "created_at": _now_iso(),
        "selic_ref": float(selic_ref),
        "margem_superior": float(margem_superior),
        "tipo_empresa": str(tipo_empresa),
        "filters_json": filters_json or {},
        "plan_hash": hv,
        "status": "active",
    }
    if notes:
        header["notes"] = str(notes)

    # upsert header por plan_hash (índice único)
    res = sb.table(SNAP_TABLE).upsert(header, on_conflict="plan_hash").execute()
    data = getattr(res, "data", None) or (res.get("data") if isinstance(res, dict) else None)
    if not data:
        return None
    snap_id = str(data[0].get("id") or "")

    if not snap_id:
        return None

    # sincroniza items (remove antigos e insere novos)
    if items:
        try:
            sb.table(ITEMS_TABLE).delete().eq("snapshot_id", snap_id).execute()
        except Exception:
            pass
        rows = [{"snapshot_id": snap_id, "ticker": it["ticker"], "peso": it["peso"]} for it in items]
        sb.table(ITEMS_TABLE).insert(rows).execute()

    return snap_id


def list_snapshots(limit: int = 25, status: str = "active") -> Optional[pd.DataFrame]:
    sb = _get_client()
    if sb is None:
        return None
    q = sb.table(SNAP_TABLE).select("*").order("created_at", desc=True).limit(int(limit))
    if status:
        q = q.eq("status", status)
    res = q.execute()
    data = getattr(res, "data", None) or (res.get("data") if isinstance(res, dict) else None)
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)


def get_snapshot(snapshot_id: str) -> Optional[Dict[str, Any]]:
    sb = _get_client()
    if sb is None:
        return None

    sid = str(snapshot_id or "").strip()
    if not sid:
        return None

    res = sb.table(SNAP_TABLE).select("*").eq("id", sid).limit(1).execute()
    data = getattr(res, "data", None) or (res.get("data") if isinstance(res, dict) else None)
    if not data:
        return None
    header = dict(data[0])

    res2 = sb.table(ITEMS_TABLE).select("*").eq("snapshot_id", sid).execute()
    items = getattr(res2, "data", None) or (res2.get("data") if isinstance(res2, dict) else None) or []
    header["items"] = items
    return header


def get_latest_snapshot() -> Optional[Dict[str, Any]]:
    df = list_snapshots(limit=1, status="active")
    if df is None or df.empty:
        return None
    sid = str(df.iloc[0].get("id") or "")
    return get_snapshot(sid) if sid else None
