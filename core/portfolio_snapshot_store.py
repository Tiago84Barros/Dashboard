# -*- coding: utf-8 -*-
"""
dashboard/core/portfolio_snapshot_store.py

Persistência de snapshots do portfólio via SQLAlchemy engine (get_supabase_engine),
no mesmo padrão já usado em patch6_runs_store.py.

✅ Não depende de SUPABASE_URL / SERVICE_ROLE_KEY / supabase-py.
✅ Compatível com Patch 6 (imports):
   - get_latest_snapshot
   - list_snapshots
   - get_snapshot
   - save_snapshot
   - compute_plan_hash

Tabelas esperadas (schema do usuário):
- public.portfolio_snapshots
  (id uuid PK, created_at timestamptz, selic_ref numeric, margem_superior numeric,
   tipo_empresa text, filters_json jsonb, plan_hash text unique, status text, notes text)

- public.portfolio_snapshot_items
  (snapshot_id uuid FK, ticker text, peso numeric, created_at timestamptz default now(),
   primary key (snapshot_id, ticker))
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
    """MD5 sobre JSON determinístico. NÃO inclua timestamps no payload."""
    s = _json_deterministico(payload)
    return hashlib.md5(s.encode("utf-8")).hexdigest()


# -----------------------------
# Queries
# -----------------------------
def list_snapshots(limit: int = 25, status: str = "active") -> pd.DataFrame:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        return pd.read_sql_query(
            text(f"""
                select
                    id,
                    created_at,
                    selic_ref,
                    margem_superior,
                    tipo_empresa,
                    plan_hash,
                    status,
                    notes
                from {SNAP_TABLE}
                where (:status is null or status = :status)
                order by created_at desc
                limit :lim
            """),
            conn,
            params={"lim": int(limit), "status": status},
        )


def get_snapshot(snapshot_id: str) -> Optional[Dict[str, Any]]:
    if not snapshot_id:
        return None

    engine = get_supabase_engine()

    with engine.connect() as conn:
        header_df = pd.read_sql_query(
            text(f"""
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
            """),
            conn,
            params={"sid": snapshot_id},
        )
        if header_df.empty:
            return None

        items_df = pd.read_sql_query(
            text(f"""
                select snapshot_id, ticker, peso
                from {ITEMS_TABLE}
                where snapshot_id = :sid
                order by ticker asc
            """),
            conn,
            params={"sid": snapshot_id},
        )

    header = header_df.iloc[0].to_dict()
    header["items"] = items_df.to_dict(orient="records") if not items_df.empty else []
    return header


def get_latest_snapshot(status: str = "active") -> Optional[Dict[str, Any]]:
    df = list_snapshots(limit=1, status=status)
    if df is None or df.empty:
        return None
    sid = str(df.iloc[0]["id"])
    return get_snapshot(sid)


def save_snapshot(
    *,
    items: List[Dict[str, Any]],
    selic_ref: Optional[float],
    margem_superior: Optional[float],
    tipo_empresa: Optional[str],
    filters_json: Optional[Dict[str, Any]],
    plan_hash: str,
    status: str = "active",
    notes: Optional[str] = None,
) -> str:
    """
    Upsert do cabeçalho por plan_hash e replace total dos itens.
    Retorna snapshot_id (uuid como string).
    """
    if not plan_hash:
        raise ValueError("plan_hash é obrigatório.")

    filters_json = filters_json or {}

    sql_upsert = f"""
        insert into {SNAP_TABLE}
            (selic_ref, margem_superior, tipo_empresa, filters_json, plan_hash, status, notes)
        values
            (:selic_ref, :margem_superior, :tipo_empresa, cast(:filters_json as jsonb), :plan_hash, :status, :notes)
        on conflict (plan_hash) do update
        set
            selic_ref = excluded.selic_ref,
            margem_superior = excluded.margem_superior,
            tipo_empresa = excluded.tipo_empresa,
            filters_json = excluded.filters_json,
            status = excluded.status,
            notes = excluded.notes,
            created_at = now()
        returning id
    """

    sql_delete_items = f"delete from {ITEMS_TABLE} where snapshot_id = :sid"

    sql_insert_item = f"""
        insert into {ITEMS_TABLE} (snapshot_id, ticker, peso)
        values (:snapshot_id, :ticker, :peso)
        on conflict (snapshot_id, ticker) do update
        set peso = excluded.peso
    """

    engine = get_supabase_engine()
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
            raise RuntimeError("Não foi possível obter snapshot_id após upsert.")

        # Replace total (mantém determinismo do snapshot)
        conn.execute(text(sql_delete_items), {"sid": snapshot_id})

        batch = []
        for it in (items or []):
            tk = (it.get("ticker") or "").strip().upper()
            if not tk:
                continue
            peso = it.get("peso")
            batch.append(
                {"snapshot_id": snapshot_id, "ticker": tk, "peso": float(peso) if peso is not None else 0.0}
            )

        if batch:
            conn.execute(text(sql_insert_item), batch)

    return snapshot_id
