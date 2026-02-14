from __future__ import annotations

"""
pickup/docs_rag.py
------------------
Leitura de documentos e chunks do Supabase para uso no Patch 6 (RAG).

Tabelas esperadas:
- public.docs_corporativos
- public.docs_corporativos_chunks
"""

from typing import Any, Dict, List, Sequence, Optional, Tuple
import re

import pandas as pd
from sqlalchemy import text

from core.db_loader import get_supabase_engine


def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()


def _read_df(sql: str, params: Dict[str, Any]) -> pd.DataFrame:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


def count_docs_by_tickers(tickers: Sequence[str]) -> Dict[str, int]:
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys([t for t in tks if t]))
    if not tks:
        return {}

    df = _read_df(
        """
        SELECT ticker, COUNT(*)::int as qtd
        FROM public.docs_corporativos
        WHERE ticker = ANY(:tks)
        GROUP BY ticker
        """,
        {"tks": tks},
    )
    out = {tk: 0 for tk in tks}
    for _, row in df.iterrows():
        out[str(row["ticker"]).upper()] = int(row["qtd"])
    return out


def get_docs_by_ticker(
    ticker: str,
    *,
    limit: int = 50,
    tipo: Optional[str] = None,
    fonte: Optional[str] = None,
) -> List[Dict[str, Any]]:
    tk = _norm_ticker(ticker)
    if not tk:
        return []

    where = ["ticker = :tk"]
    params: Dict[str, Any] = {"tk": tk, "limit": int(limit)}

    if tipo:
        where.append("tipo = :tipo")
        params["tipo"] = str(tipo)

    if fonte:
        where.append("fonte = :fonte")
        params["fonte"] = str(fonte)

    sql = f"""
        SELECT id, ticker, data, fonte, tipo, titulo, url, raw_text, created_at
        FROM public.docs_corporativos
        WHERE {" AND ".join(where)}
        ORDER BY COALESCE(data, created_at::date) DESC, id DESC
        LIMIT :limit
    """

    df = _read_df(sql, params)
    return df.to_dict(orient="records") if df is not None and not df.empty else []


def get_chunks_by_ticker(
    ticker: str,
    *,
    limit: int = 120,
) -> List[Dict[str, Any]]:
    tk = _norm_ticker(ticker)
    if not tk:
        return []

    df = _read_df(
        """
        SELECT c.id, c.doc_id, c.ticker, c.chunk_index, c.chunk_text, c.created_at
        FROM public.docs_corporativos_chunks c
        WHERE c.ticker = :tk
        ORDER BY c.id DESC
        LIMIT :limit
        """,
        {"tk": tk, "limit": int(limit)},
    )
    return df.to_dict(orient="records") if df is not None and not df.empty else []


def get_rag_context_for_ticker(
    ticker: str,
    *,
    max_chunks: int = 80,
) -> List[Dict[str, Any]]:
    """
    Retorna uma lista de mensagens/sumários prontos para passar como context no LLM.
    """
    chunks = get_chunks_by_ticker(ticker, limit=int(max_chunks))
    if not chunks:
        return []

    # reduz ruído e garante tamanho razoável por chunk
    out = []
    for ch in chunks:
        txt = str(ch.get("chunk_text") or "").strip()
        txt = re.sub(r"\s+", " ", txt)
        if not txt:
            continue
        out.append({"ticker": ticker, "chunk_index": ch.get("chunk_index"), "text": txt})
    return out
