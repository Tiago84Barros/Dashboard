
# core/patch6_runs_store.py
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from sqlalchemy import text

from core.db_loader import get_supabase_engine


def save_patch6_run(snapshot_id: str, ticker: str, period_ref: str, result: Dict[str, Any]) -> None:
    """Upsert do resultado Patch6 por (snapshot_id, ticker, period_ref)."""
    engine = get_supabase_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
            insert into public.patch6_runs
                (snapshot_id, ticker, period_ref, perspectiva_compra, resumo, result_json)
            values
                (:snapshot_id, :ticker, :period_ref, :perspectiva, :resumo, :result_json)
            on conflict (snapshot_id, ticker, period_ref) do update
            set perspectiva_compra = excluded.perspectiva_compra,
                resumo = excluded.resumo,
                result_json = excluded.result_json,
                created_at = now()
            """
            ),
            {
                "snapshot_id": snapshot_id,
                "ticker": (ticker or "").strip().upper(),
                "period_ref": period_ref,
                "perspectiva": (result or {}).get("perspectiva_compra"),
                "resumo": (result or {}).get("resumo"),
                "result_json": json.dumps(result or {}, ensure_ascii=False),
            },
        )


def load_patch6_runs(
    snapshot_id: str,
    tickers: Sequence[str],
    period_ref: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Carrega runs do Patch6 para o snapshot e tickers.

    Se period_ref for None, retorna a última execução por ticker (mais recente).
    """
    engine = get_supabase_engine()
    tk = [str(x).strip().upper() for x in (tickers or []) if str(x).strip()]
    if not tk:
        return []

    if period_ref:
        sql = """
        select ticker, period_ref, created_at, perspectiva_compra, resumo, result_json
        from public.patch6_runs
        where snapshot_id = :sid
          and ticker = any(:tickers)
          and period_ref = :pref
        order by created_at desc
        """
        params = {"sid": snapshot_id, "tickers": tk, "pref": period_ref}
    else:
        # latest per ticker
        sql = """
        select distinct on (ticker)
            ticker, period_ref, created_at, perspectiva_compra, resumo, result_json
        from public.patch6_runs
        where snapshot_id = :sid
          and ticker = any(:tickers)
        order by ticker, created_at desc
        """
        params = {"sid": snapshot_id, "tickers": tk}

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        try:
            d["result"] = json.loads(d.get("result_json") or "{}")
        except Exception:
            d["result"] = {}
        out.append(d)
    return out


def list_patch6_history(ticker: str, limit: int = 8) -> pd.DataFrame:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        return pd.read_sql_query(
            text(
                """
            select period_ref, created_at, perspectiva_compra, resumo
            from public.patch6_runs
            where ticker = :tk
            order by created_at desc
            limit :lim
            """
            ),
            conn,
            params={"tk": (ticker or "").strip().upper(), "lim": int(limit)},
        )
