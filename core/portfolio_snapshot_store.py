
# core/portfolio_snapshot_store.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class PortfolioSnapshot:
    id: str
    created_at: Optional[str]
    filters: Dict[str, Any]
    tickers: List[str]
    selic_ref: Optional[float] = None
    margem_superior: Optional[float] = None
    plan_hash: Optional[str] = None
    status: Optional[str] = None
    notes: Optional[str] = None
    tipo_empresa: Optional[str] = None


def _extract_tickers_from_filters(filters: Dict[str, Any]) -> List[str]:
    # Try common keys
    for k in ("tickers", "ativos", "acoes", "selected_tickers", "selected"):
        v = filters.get(k)
        if isinstance(v, list) and v:
            return [str(x).strip().upper() for x in v if str(x).strip()]
    # Sometimes tickers are inside an items dict
    v = filters.get("items")
    if isinstance(v, list) and v:
        out=[]
        for it in v:
            if isinstance(it, dict):
                t = it.get("ticker") or it.get("symbol")
                if t:
                    out.append(str(t).strip().upper())
        if out:
            return out
    return []


def get_latest_snapshot(engine: Engine) -> Optional[PortfolioSnapshot]:
    """Return latest snapshot from public.portfolio_snapshots.

    IMPORTANT: Your Supabase table uses filters_json (jsonb) and not snapshot_json.
    We therefore treat filters_json as the canonical payload.
    """
    sql = """
    select
        id::text as id,
        created_at::text as created_at,
        coalesce(filters_json, '{}'::jsonb) as filters_json,
        selic_ref,
        margem_superior,
        plan_hash,
        status,
        notes,
        tipo_empresa
    from public.portfolio_snapshots
    order by created_at desc
    limit 1
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql)).mappings().first()
    if not row:
        return None

    filters = dict(row.get("filters_json") or {})
    tickers: List[str] = []

    # Try items table if exists (best source)
    try:
        sql_items = """
        select distinct upper(ticker) as ticker
        from public.portfolio_snapshot_items
        where snapshot_id = :sid
        order by 1
        """
        with engine.connect() as conn:
            items = conn.execute(text(sql_items), {"sid": row["id"]}).mappings().all()
        tickers = [r["ticker"] for r in items if r.get("ticker")]
    except Exception:
        tickers = []

    if not tickers:
        tickers = _extract_tickers_from_filters(filters)

    return PortfolioSnapshot(
        id=row["id"],
        created_at=row.get("created_at"),
        filters=filters,
        tickers=tickers,
        selic_ref=row.get("selic_ref"),
        margem_superior=row.get("margem_superior"),
        plan_hash=row.get("plan_hash"),
        status=row.get("status"),
        notes=row.get("notes"),
        tipo_empresa=row.get("tipo_empresa"),
    )
