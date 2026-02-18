
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import text

from core.db_loader import get_supabase_engine


def _hash_plan(payload: Dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def save_snapshot(header: Dict[str, Any], items: List[Dict[str, Any]]) -> str:
    """
    Salva um snapshot (cabeçalho + itens) e retorna snapshot_id.
    - Idempotente por plan_hash (determinístico).
    - Não quebra o dashboard se rodar repetidas vezes com o mesmo plano.
    """
    engine = get_supabase_engine()

    payload_for_hash = {
        "header": header,
        "items": sorted(items, key=lambda x: (x.get("ticker") or "")),
    }
    plan_hash = _hash_plan(payload_for_hash)

    with engine.begin() as conn:
        res = conn.execute(
            text("""
            insert into public.portfolio_snapshots
                (selic_ref, margem_superior, tipo_empresa, filters_json, plan_hash, status)
            values
                (:selic_ref, :margem_superior, :tipo_empresa, :filters_json, :plan_hash, 'active')
            on conflict (plan_hash) do update
            set status = 'active'
            returning id
            """),
            {
                "selic_ref": header.get("selic_ref"),
                "margem_superior": header.get("margem_superior"),
                "tipo_empresa": header.get("tipo_empresa"),
                "filters_json": json.dumps(header.get("filters_json", {}), ensure_ascii=False),
                "plan_hash": plan_hash,
            },
        )
        snapshot_id = str(res.fetchone()[0])

        # itens (upsert)
        for item in items:
            conn.execute(
                text("""
                insert into public.portfolio_snapshot_items
                    (snapshot_id, ticker, segmento, peso, meta_json)
                values
                    (:snapshot_id, :ticker, :segmento, :peso, :meta_json)
                on conflict (snapshot_id, ticker) do update
                set segmento = excluded.segmento,
                    peso = excluded.peso,
                    meta_json = excluded.meta_json
                """),
                {
                    "snapshot_id": snapshot_id,
                    "ticker": (item.get("ticker") or "").strip().upper(),
                    "segmento": item.get("segmento"),
                    "peso": item.get("peso"),
                    "meta_json": json.dumps(item.get("meta_json", {}), ensure_ascii=False),
                },
            )

    return snapshot_id


def list_snapshots(limit: int = 30, status: str = "active") -> pd.DataFrame:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        return pd.read_sql_query(
            text("""
            select id, created_at, selic_ref, margem_superior, tipo_empresa, status, plan_hash
            from public.portfolio_snapshots
            where (:status is null) or (status = :status)
            order by created_at desc
            limit :lim
            """),
            conn,
            params={"lim": int(limit), "status": status},
        )


def get_snapshot(snapshot_id: str) -> Optional[Dict[str, Any]]:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("""
            select id, created_at, selic_ref, margem_superior, tipo_empresa, filters_json, plan_hash, status
            from public.portfolio_snapshots
            where id = :sid
            """),
            {"sid": snapshot_id},
        ).fetchone()

        if not row:
            return None

        items = conn.execute(
            text("""
            select ticker, segmento, peso, meta_json
            from public.portfolio_snapshot_items
            where snapshot_id = :sid
            order by ticker asc
            """),
            {"sid": snapshot_id},
        ).fetchall()

    return {
        "id": str(row[0]),
        "created_at": str(row[1]),
        "selic_ref": row[2],
        "margem_superior": row[3],
        "tipo_empresa": row[4],
        "filters_json": row[5],
        "plan_hash": row[6],
        "status": row[7],
        "items": [
            {
                "ticker": r[0],
                "segmento": r[1],
                "peso": float(r[2]) if r[2] is not None else None,
                "meta_json": r[3],
            }
            for r in items
        ],
    }


def get_latest_snapshot(status: str = "active") -> Optional[Dict[str, Any]]:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("""
            select id
            from public.portfolio_snapshots
            where status = :st
            order by created_at desc
            limit 1
            """),
            {"st": status},
        ).fetchone()
    if not row:
        return None
    return get_snapshot(str(row[0]))
