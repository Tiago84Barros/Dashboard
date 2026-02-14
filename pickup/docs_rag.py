# pickup/docs_rag.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd

from sqlalchemy import text

# tenta pegar engine do seu projeto (core.db_loader ou db_loader)
def _get_engine():
    try:
        from core.db_loader import get_supabase_engine  # type: ignore
        return get_supabase_engine()
    except Exception:
        from db_loader import get_supabase_engine  # type: ignore
        return get_supabase_engine()

def _read_sql_df(sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    engine = _get_engine()
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})

def _norm_tk(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()

def build_docs_by_ticker_from_db(
    tickers: List[str],
    *,
    limit_docs: int = 12,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Retorna docs_by_ticker no formato esperado pelo Patch 6:

      docs_by_ticker[ticker] = [
        {"source": "...", "date": "YYYY-MM-DD", "text": "..."},
        ...
      ]

    Usa a tabela public.docs_corporativos.
    """
    out: Dict[str, List[Dict[str, Any]]] = {}

    for tk_raw in tickers:
        tk = _norm_tk(tk_raw)
        if not tk:
            continue

        try:
            df = _read_sql_df(
                """
                SELECT
                  id,
                  ticker,
                  data,
                  fonte,
                  tipo,
                  titulo,
                  url,
                  raw_text,
                  created_at
                FROM public.docs_corporativos
                WHERE ticker = :tk
                ORDER BY COALESCE(data, created_at::date) DESC, id DESC
                LIMIT :lim
                """,
                {"tk": tk, "lim": int(limit_docs)},
            )
        except Exception:
            df = pd.DataFrame()

        if df is None or df.empty:
            out[tk] = []
            continue

        rows: List[Dict[str, Any]] = []
        for _, r in df.iterrows():
            rows.append(
                {
                    "source": str(r.get("fonte") or "db").strip(),
                    "date": str(r.get("data") or r.get("created_at") or "NA")[:10],
                    "text": str(r.get("raw_text") or "").strip(),
                }
            )
        out[tk] = rows

    return out

def build_chunks_by_ticker_from_db(
    tickers: List[str],
    *,
    limit_docs: int = 12,
    limit_chunks_per_doc: int = 6,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Retorna chunks_by_ticker (útil para Patch 7 / RAG) no formato:

      chunks_by_ticker[ticker] = [
        {"doc_id": 123, "chunk_index": 0, "text": "..."},
        ...
      ]

    Usa a tabela public.docs_corporativos_chunks.
    """
    out: Dict[str, List[Dict[str, Any]]] = {}

    for tk_raw in tickers:
        tk = _norm_tk(tk_raw)
        if not tk:
            continue

        try:
            df = _read_sql_df(
                """
                WITH docs AS (
                  SELECT id
                  FROM public.docs_corporativos
                  WHERE ticker = :tk
                  ORDER BY COALESCE(data, created_at::date) DESC, id DESC
                  LIMIT :lim_docs
                )
                SELECT
                  c.doc_id,
                  c.ticker,
                  c.chunk_index,
                  c.chunk_text,
                  c.created_at
                FROM public.docs_corporativos_chunks c
                JOIN docs d ON d.id = c.doc_id
                WHERE c.ticker = :tk
                ORDER BY c.doc_id DESC, c.chunk_index ASC
                """,
                {"tk": tk, "lim_docs": int(limit_docs)},
            )
        except Exception:
            df = pd.DataFrame()

        if df is None or df.empty:
            out[tk] = []
            continue

        # limita chunks por doc
        rows: List[Dict[str, Any]] = []
        counts: Dict[int, int] = {}

        for _, r in df.iterrows():
            doc_id = int(r.get("doc_id") or 0)
            if doc_id <= 0:
                continue
            counts.setdefault(doc_id, 0)
            if counts[doc_id] >= int(limit_chunks_per_doc):
                continue
            counts[doc_id] += 1

            rows.append(
                {
                    "doc_id": doc_id,
                    "chunk_index": int(r.get("chunk_index") or 0),
                    "text": str(r.get("chunk_text") or "").strip(),
                }
            )
        out[tk] = rows

    return out


# ------------------------------------------------------------------
# ALIASES para compatibilidade com o que a página está importando
# ------------------------------------------------------------------

def get_docs_by_ticker(
    tickers: List[str],
    *,
    limit_docs: int = 12,
) -> Dict[str, List[Dict[str, Any]]]:
    return build_docs_by_ticker_from_db(tickers, limit_docs=limit_docs)

def get_chunks_by_ticker(
    tickers: List[str],
    *,
    limit_docs: int = 12,
    limit_chunks_per_doc: int = 6,
) -> Dict[str, List[Dict[str, Any]]]:
    return build_chunks_by_ticker_from_db(
        tickers,
        limit_docs=limit_docs,
        limit_chunks_per_doc=limit_chunks_per_doc,
    )

__all__ = [
    "build_docs_by_ticker_from_db",
    "build_chunks_by_ticker_from_db",
    "get_docs_by_ticker",
    "get_chunks_by_ticker",
]
