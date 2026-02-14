# pickup/docs_rag.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import re

import pandas as pd
import streamlit as st
from sqlalchemy import text

# Usa seu engine do core.db_loader (já existe no seu projeto)
from core.db_loader import get_supabase_engine


def _norm_ticker(t: str) -> str:
    return (t or "").strip().upper().replace(".SA", "").strip()


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _limit_text(s: str, max_chars: int) -> str:
    s = (s or "").strip()
    if max_chars and max_chars > 0 and len(s) > max_chars:
        return s[:max_chars] + "…"
    return s


def _build_source(fonte: str, tipo: str, titulo: str, url: str) -> str:
    parts = []
    fonte = (fonte or "").strip()
    tipo = (tipo or "").strip()
    titulo = (titulo or "").strip()
    url = (url or "").strip()

    if fonte:
        parts.append(fonte)
    if tipo:
        parts.append(tipo)
    if titulo:
        parts.append(titulo)
    if url:
        parts.append(url)

    return " | ".join(parts) if parts else "docs_corporativos"


@st.cache_data(show_spinner=False, ttl=60 * 10)
def count_docs_by_ticker(ticker: str) -> int:
    tk = _norm_ticker(ticker)
    if not tk:
        return 0
    engine = get_supabase_engine()
    with engine.connect() as conn:
        r = conn.execute(
            text("SELECT COUNT(*) AS c FROM public.docs_corporativos WHERE ticker = :t"),
            {"t": tk},
        ).fetchone()
    return _safe_int(r[0] if r else 0, 0)


@st.cache_data(show_spinner=False, ttl=60 * 10)
def count_docs_by_tickers(tickers: List[str]) -> Dict[str, int]:
    tks = [_norm_ticker(x) for x in (tickers or []) if _norm_ticker(x)]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {}

    engine = get_supabase_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT ticker, COUNT(*)::int AS c
                FROM public.docs_corporativos
                WHERE ticker = ANY(:tks)
                GROUP BY ticker
                """
            ),
            {"tks": tks},
        ).fetchall()

    out = {tk: 0 for tk in tks}
    for r in rows or []:
        out[_norm_ticker(str(r[0]))] = _safe_int(r[1], 0)
    return out


@st.cache_data(show_spinner=False, ttl=60 * 10)
def get_docs_by_ticker(
    ticker: str,
    *,
    limit_docs: int = 12,
    max_chars_per_doc: int = 6000,
    prefer_chunks: bool = True,
    limit_chunks: int = 18,
) -> List[Dict[str, Any]]:
    """
    Retorna lista no formato esperado pelo Patch6:
      [{"source": "...", "date": "YYYY-MM-DD", "text": "..."}]

    - Se prefer_chunks=True e houver chunks, retorna chunks (melhor para RAG).
    - Caso contrário retorna raw_text de docs_corporativos.
    """
    tk = _norm_ticker(ticker)
    if not tk:
        return []

    engine = get_supabase_engine()

    # 1) tenta chunks (RAG)
    if prefer_chunks:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        d.fonte,
                        d.tipo,
                        COALESCE(d.titulo,'') AS titulo,
                        COALESCE(d.url,'') AS url,
                        COALESCE(d.data::text,'NA') AS data_txt,
                        c.chunk_text
                    FROM public.docs_corporativos_chunks c
                    JOIN public.docs_corporativos d ON d.id = c.doc_id
                    WHERE c.ticker = :t
                    ORDER BY d.data DESC NULLS LAST, d.id DESC, c.chunk_index ASC
                    LIMIT :lim
                    """
                ),
                {"t": tk, "lim": int(limit_chunks)},
            ).fetchall()

        if rows:
            out: List[Dict[str, Any]] = []
            for r in rows:
                fonte, tipo, titulo, url, data_txt, chunk_text = r
                out.append(
                    {
                        "source": _build_source(str(fonte), str(tipo), str(titulo), str(url)),
                        "date": str(data_txt),
                        "text": _limit_text(str(chunk_text or ""), int(max_chars_per_doc)),
                    }
                )
            return out[: int(limit_docs)]

    # 2) fallback: docs (raw_text)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    fonte,
                    tipo,
                    COALESCE(titulo,'') AS titulo,
                    COALESCE(url,'') AS url,
                    COALESCE(data::text,'NA') AS data_txt,
                    raw_text
                FROM public.docs_corporativos
                WHERE ticker = :t
                ORDER BY data DESC NULLS LAST, id DESC
                LIMIT :lim
                """
            ),
            {"t": tk, "lim": int(limit_docs)},
        ).fetchall()

    out2: List[Dict[str, Any]] = []
    for r in rows or []:
        fonte, tipo, titulo, url, data_txt, raw_text = r
        out2.append(
            {
                "source": _build_source(str(fonte), str(tipo), str(titulo), str(url)),
                "date": str(data_txt),
                "text": _limit_text(str(raw_text or ""), int(max_chars_per_doc)),
            }
        )
    return out2


def get_docs_by_tickers(
    tickers: List[str],
    *,
    limit_docs: int = 12,
    max_chars_per_doc: int = 6000,
    prefer_chunks: bool = True,
    limit_chunks: int = 18,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Conveniência: retorna dict ticker -> docs list.
    """
    tks = [_norm_ticker(x) for x in (tickers or []) if _norm_ticker(x)]
    tks = list(dict.fromkeys(tks))
    out: Dict[str, List[Dict[str, Any]]] = {}
    for tk in tks:
        out[tk] = get_docs_by_ticker(
            tk,
            limit_docs=limit_docs,
            max_chars_per_doc=max_chars_per_doc,
            prefer_chunks=prefer_chunks,
            limit_chunks=limit_chunks,
        )
    return out


__all__ = [
    "get_docs_by_ticker",
    "get_docs_by_tickers",
    "count_docs_by_ticker",
    "count_docs_by_tickers",
]
