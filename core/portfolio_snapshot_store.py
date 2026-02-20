# -*- coding: utf-8 -*-
"""
portfolio_snapshot_store.py (ENGINE MODE)

Snapshot persistence using SQLAlchemy engine (get_supabase_engine),
same pattern used by patch6_runs_store.py.

Requires tables:
- public.portfolio_snapshots (id, created_at, selic_ref, margem_superior, tipo_empresa, filters_json, plan_hash, status, notes)
- public.portfolio_snapshot_items (snapshot_id, ticker, peso, ...)

Provides API expected by Patch 6:
- compute_plan_hash(payload)
- list_snapshots(limit=25, status="active") -> pd.DataFrame
- get_snapshot(snapshot_id) -> dict|None  (includes "items")
- get_latest_snapshot(status="active") -> dict|None
- save_snapshot(...) -> snapshot_id (str)  (upsert header by plan_hash + replace items)
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import text

from core.db_loader import get_supabase_engine

SNAP_TABLE = "public.portfolio_snapshots"
ITEMS_TABLE = "public.portfolio_snapshot_items"


# -----------------------------
# Hash helpers (deterministic)
# -----------------------------
def _json_deterministico(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def compute_plan_hash(payload: Dict[str, Any]) -> str:
    """MD5 over deterministic JSON. Do NOT include timestamps."""
    s = _json_deterministico(payload)
    return hashlib.md5(s.encode("utf-8")).hexdigest()


# -----------------------------
# Public API expected by Patch 6
# -----------------------------
def list_snapshots(limit: int = 25, status: str = "active") -> pd.DataFrame:
    engine = get_supabase_engine()
    sql = f"""
        select
            id,
            created_at,
            selic_ref,
            margem_superior,
            tipo_empresa,
            plan_hash,
            status
        from {SNAP_TABLE}
        where (:status is null or status = :status)
        order by created_at desc
        limit :lim
    """
    with engine.connect() as conn:
        return pd.read_sql_query(
            text(sql),
            conn,
            params={"lim": int(limit), "status": status},
        )


def get_snapshot(snapshot_id: str) -> Optional[Dict[str, Any]]:
    if not snapshot_id:
        return None

    engine = get_supabase_engine()

    sql_header = f"""
        select
            id,
            created_at,
            selic_ref,
            margem_superior,
            tipo_empresa,
            filters_json,
            plan_hash,
            status,
            notes
        from {SNAP_TABLE}
        where id = :sid
        limit 1
    """
    sql_items = f"""
        select *
        from {ITEMS_TABLE}
        where snapshot_id = :sid
        order by ticker asc
    """

    with engine.connect() as conn:
        header_df = pd.read_sql_query(text(sql_header), conn, params={"sid": snapshot_id})
        if header_df.empty:
            return None

        header = header_df.iloc[0].to_dict()

        items_df = pd.read_sql_query(text(sql_items), conn, params={"sid": snapshot_id})
        items = items_df.to_dict(orient="records") if not items_df.empty else []

    header["items"] = items
    return header


def get_latest_snapshot(status: str = "active") -> Optional[Dict[str, Any]]:
    df = list_snapshots(limit=1, status=status)
    if df is None or df.empty:
        return None
    sid = str(df.iloc[0]["id"])
    return get_snapshot(sid)


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
    Upsert header by plan_hash, then replace items.
    Returns snapshot_id.
    """
    if not plan_hash:
        raise ValueError("plan_hash é obrigatório.")

    filters_json = filters_json or {}

    engine = get_supabase_engine()

    # 1) UPSERT header by plan_hash (unique)
    sql_upsert = f"""
        insert into {SNAP_TABLE}
            (selic_ref, margem_superior, tipo_empresa, filters_json, plan_hash, status, notes)
        values
            (:selic_ref, :margem_superior, :tipo_empresa, :filters_json::jsonb, :plan_hash, :status, :notes)
        on conflict (plan_hash) do update
        set
            selic_ref = excluded.selic_ref,
            margem_superior = excluded.margem_superior,
            tipo_empresa = excluded.tipo_empresa,
            filters_json = excluded.filters_json,
            status = excluded.status,
            notes = excluded.notes
        returning id
    """

    # 2) Replace items
    sql_delete_items = f"delete from {ITEMS_TABLE} where snapshot_id = :sid"

    # Insert items (batch)
    # We keep only snapshot_id, ticker, peso. Extra keys in dict are ignored.
    sql_insert_item = f"""
        insert into {ITEMS_TABLE} (snapshot_id, ticker, peso)
        values (:snapshot_id, :ticker, :peso)
    """

    with engine.begin() as conn:
        res = conn.execute(
            text(sql_upsert),
            {
                "selic_ref": selic_ref,
                "margem_superior": margem_superior,
                "tipo_empresa": tipo_empresa,
                "filters_json": json.dumps(filters_json, ensure_ascii=False),
                "plan_hash": plan_hash,
                "status": status,
                "notes": notes,
            },
        )
        row = res.fetchone()
        snapshot_id = str(row[0]) if row else None
        if not snapshot_id:
            # fallback read
            res2 = conn.execute(
                text(f"select id from {SNAP_TABLE} where plan_hash = :ph limit 1"),
                {"ph": plan_hash},
            ).fetchone()
            snapshot_id = str(res2[0]) if res2 else None

        if not snapshot_id:
            raise RuntimeError("Não foi possível obter snapshot_id após upsert.")

        # Replace items
        conn.execute(text(sql_delete_items), {"sid": snapshot_id})

        rows_items = []
        for it in items or []:
            tk = (it.get("ticker") or "").strip().upper()
            if not tk:
                continue
            peso = it.get("peso")
            rows_items.append({"snapshot_id": snapshot_id, "ticker": tk, "peso": float(peso) if peso is not None else 0.0})

        if rows_items:
            conn.execute(text(sql_insert_item), rows_items)

    return snapshot_id
