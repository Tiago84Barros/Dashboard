from __future__ import annotations

"""
pickup/docs_rag.py
------------------
Leitura de documentos do Supabase para uso no Patch 6 (RAG).

Tabelas:
- public.docs_corporativos
- public.docs_corporativos_chunks
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
        SELECT ticker, COUNT(*)::int AS qtd
        FROM public.docs_corporativos
        WHERE ticker = ANY(:tks)
        GROUP BY ticker
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"tks": tks}).fetchall()

    out = {tk: 0 for tk in tks}
    for r in rows:
        out[str(r[0]).upper()] = int(r[1])
    return out


def get_docs_by_ticker(ticker: str, *, limit: int = 30) -> List[Dict[str, Any]]:
    tk = _norm_ticker(ticker)
    if not tk:
        return []

    engine = get_supabase_engine()
    sql = text(
        """
        SELECT id, ticker, data, fonte, tipo, titulo, url, raw_text, doc_hash, created_at
        FROM public.docs_corporativos
        WHERE ticker = :tk
        ORDER BY data DESC NULLS LAST, created_at DESC
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"tk": tk, "lim": int(limit)}).mappings().all()
    return [dict(r) for r in rows]


def get_docs_by_tickers(tickers: Sequence[str], *, limit_per_ticker: int = 20) -> Dict[str, List[Dict[str, Any]]]:
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    out: Dict[str, List[Dict[str, Any]]] = {tk: [] for tk in tks}
    if not tks:
        return out

    for tk in tks:
        out[tk] = get_docs_by_ticker(tk, limit=int(limit_per_ticker))
    return out
