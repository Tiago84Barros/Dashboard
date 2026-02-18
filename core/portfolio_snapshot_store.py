
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from core.db_loader import get_supabase_engine


def _hash_plan(payload: Dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def save_snapshot(header: Dict[str, Any], items: List[Dict[str, Any]]) -> str:
    engine = get_supabase_engine()

    payload_for_hash = {
        "header": header,
        "items": sorted(items, key=lambda x: x.get("ticker", "")),
    }
    plan_hash = _hash_plan(payload_for_hash)

    with engine.begin() as conn:
        res = conn.execute(
            text("""
            insert into public.portfolio_snapshots
                (selic_ref, margem_superior, tipo_empresa, filters_json, plan_hash)
            values
                (:selic_ref, :margem_superior, :tipo_empresa, :filters_json, :plan_hash)
            on conflict (plan_hash) do update
            set plan_hash = excluded.plan_hash
            returning id
            """),
            {
                "selic_ref": header.get("selic_ref"),
                "margem_superior": header.get("margem_superior"),
                "tipo_empresa": header.get("tipo_empresa"),
                "filters_json": json.dumps(header.get("filters_json", {})),
                "plan_hash": plan_hash,
            },
        )
        snapshot_id = str(res.fetchone()[0])

        for item in items:
            conn.execute(
                text("""
                insert into public.portfolio_snapshot_items
                    (snapshot_id, ticker, segmento, peso, meta_json)
                values
                    (:snapshot_id, :ticker, :segmento, :peso, :meta_json)
                on conflict (snapshot_id, ticker) do update
                set peso = excluded.peso,
                    meta_json = excluded.meta_json
                """),
                {
                    "snapshot_id": snapshot_id,
                    "ticker": item.get("ticker"),
                    "segmento": item.get("segmento"),
                    "peso": item.get("peso"),
                    "meta_json": json.dumps(item.get("meta_json", {})),
                },
            )

    return snapshot_id


def get_latest_snapshot() -> Optional[Dict[str, Any]]:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("""
            select id, created_at, selic_ref, margem_superior, tipo_empresa
            from public.portfolio_snapshots
            where status = 'active'
            order by created_at desc
            limit 1
            """)
        ).fetchone()

        if not row:
            return None

        snapshot_id = str(row[0])

        items = conn.execute(
            text("""
            select ticker, segmento, peso, meta_json
            from public.portfolio_snapshot_items
            where snapshot_id = :sid
            """),
            {"sid": snapshot_id},
        ).fetchall()

    return {
        "id": snapshot_id,
        "created_at": str(row[1]),
        "selic_ref": row[2],
        "margem_superior": row[3],
        "tipo_empresa": row[4],
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
