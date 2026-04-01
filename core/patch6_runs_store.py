
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import text

from core.db import get_engine as get_supabase_engine


def save_patch6_run(snapshot_id: str, ticker: str, period_ref: str, result: Dict[str, Any]) -> None:
    """
    Salva o resultado do Patch 6 (LLM) para histórico trimestral.
    """
    engine = get_supabase_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
            insert into public.patch6_runs
                (snapshot_id, ticker, period_ref, perspectiva_compra, resumo, result_json, schema_version)
            values
                (:snapshot_id, :ticker, :period_ref, :perspectiva, :resumo, :result_json, :schema_version)
            on conflict (snapshot_id, ticker, period_ref) do update
            set perspectiva_compra = excluded.perspectiva_compra,
                resumo = excluded.resumo,
                result_json = excluded.result_json,
                schema_version = excluded.schema_version,
                created_at = now()
            """),
            {
                "snapshot_id": snapshot_id,
                "ticker": (ticker or "").strip().upper(),
                "period_ref": period_ref,
                "perspectiva": (result or {}).get("perspectiva_compra"),
                "resumo": (result or {}).get("resumo"),
                "result_json": json.dumps(result or {}, ensure_ascii=False),
                "schema_version": "v3",
            },
        )


def list_patch6_history(ticker: str, limit: int = 8) -> pd.DataFrame:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        return pd.read_sql_query(
            text("""
            select period_ref, created_at, perspectiva_compra, resumo
            from public.patch6_runs
            where ticker = :tk
            order by created_at desc
            limit :lim
            """),
            conn,
            params={"tk": (ticker or "").strip().upper(), "lim": int(limit)},
        )
