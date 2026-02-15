from __future__ import annotations

"""
pickup/docs_rag.py
------------------
Helpers para o Patch 6 (RAG) ler documentos no Supabase.

Exports (compat):
- get_docs_by_tickers(tickers, limit_per_ticker=50) -> List[dict]
- count_docs_by_tickers(tickers) -> Dict[str,int]

Tabelas esperadas:
- public.docs_corporativos
"""

from typing import Any, Dict, List, Sequence
from sqlalchemy import text
from core.db_loader import get_supabase_engine


def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()


def count_docs_by_tickers(tickers: Sequence[str]) -> Dict[str, int]:
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {}
    engine = get_supabase_engine()
    sql = text(
        """
        select upper(ticker) as ticker, count(*) as qtd
        from public.docs_corporativos
        where upper(ticker) = any(:tks)
        group by upper(ticker)
        """
    )
    out: Dict[str, int] = {t: 0 for t in tks}
    with engine.begin() as conn:
        rows = conn.execute(sql, {"tks": tks}).fetchall()
    for r in rows:
        out[str(r[0]).upper()] = int(r[1])
    return out


def get_docs_by_tickers(tickers: Sequence[str], *, limit_per_ticker: int = 50) -> List[Dict[str, Any]]:
    """
    Retorna docs recentes (por data desc, depois id desc) para uso como contexto RAG.
    """
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return []
    engine = get_supabase_engine()

    # Busca por ticker em batches via LATERAL (limit por ticker)
    sql = text(
        """
        with tks as (
          select unnest(:tks::text[]) as tk
        )
        select d.*
        from tks
        join lateral (
          select *
          from public.docs_corporativos d
          where upper(d.ticker) = upper(tks.tk)
          order by d.data desc nulls last, d.id desc
          limit :lim
        ) d on true
        order by d.ticker, d.data desc nulls last, d.id desc
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql, {"tks": tks, "lim": int(limit_per_ticker)}).mappings().all()
    return [dict(r) for r in rows]
