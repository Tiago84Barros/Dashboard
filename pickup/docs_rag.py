# core/docs_rag.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from core.db_loader import (
    load_docs_corporativos_from_db,
    load_docs_corporativos_chunks_from_db,
)


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

    Observação: aqui usamos raw_text do documento (não chunks).
    Para RAG com chunks, use build_chunks_by_ticker_from_db().
    """
    out: Dict[str, List[Dict[str, Any]]] = {}
    for tk in tickers:
        df = load_docs_corporativos_from_db(tk, limit=limit_docs)
        if df is None or df.empty:
            out[tk] = []
            continue
        rows: List[Dict[str, Any]] = []
        for _, r in df.iterrows():
            rows.append(
                {
                    "source": str(r.get("fonte", "db")).strip(),
                    "date": str(r.get("data") or r.get("created_at") or "NA")[:10],
                    "text": str(r.get("raw_text", "")).strip(),
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
    Retorna chunks_by_ticker (útil para Patch 7 / RAG), no formato:

      chunks_by_ticker[ticker] = [
        {"doc_id": 123, "chunk_index": 0, "text": "..."},
        ...
      ]
    """
    out: Dict[str, List[Dict[str, Any]]] = {}
    for tk in tickers:
        df = load_docs_corporativos_chunks_from_db(
            tk,
            limit_docs=limit_docs,
            limit_chunks_per_doc=limit_chunks_per_doc,
        )
        if df is None or df.empty:
            out[tk] = []
            continue
        rows: List[Dict[str, Any]] = []
        for _, r in df.iterrows():
            rows.append(
                {
                    "doc_id": int(r.get("doc_id") or 0),
                    "chunk_index": int(r.get("chunk_index") or 0),
                    "text": str(r.get("chunk_text", "")).strip(),
                }
            )
        out[tk] = rows
    return out
