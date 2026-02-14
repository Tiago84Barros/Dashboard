from __future__ import annotations

"""
pickup/docs_rag.py
------------------
Leitura de documentos corporativos (RAG) no Supabase para o Patch 6.

Tabelas esperadas:
- public.docs_corporativos
- public.docs_corporativos_chunks

Funções públicas:
- get_docs_by_ticker
- get_docs_by_tickers
- count_docs_by_tickers
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
import hashlib

import pandas as pd
import streamlit as st
from sqlalchemy import text

from core.db_loader import get_supabase_engine


# ─────────────────────────────────────────────────────────────
# Utils
# ─────────────────────────────────────────────────────────────

def _norm_tk(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()

def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)

def _hash_key(*parts: str) -> str:
    raw = "|".join([p or "" for p in parts])
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────────────────────
# Queries (docs + chunks)
# ─────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=60 * 10)
def count_docs_by_tickers(
    tickers: Sequence[str],
) -> Dict[str, int]:
    """
    Retorna contagem de docs por ticker em public.docs_corporativos.
    Ex: {"PETR4": 12, "VALE3": 5}
    """
    tks = [_norm_tk(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys([t for t in tks if t]))
    if not tks:
        return {}

    engine = get_supabase_engine()

    # Usa ANY(:tickers) com array
    sql = text(
        """
        SELECT ticker, COUNT(*) AS n
        FROM public.docs_corporativos
        WHERE ticker = ANY(:tickers)
        GROUP BY ticker
        """
    )

    with engine.connect() as conn:
        df = pd.read_sql_query(sql, conn, params={"tickers": tks})

    if df is None or df.empty:
        return {tk: 0 for tk in tks}

    out = {str(r["ticker"]): _safe_int(r["n"], 0) for _, r in df.iterrows()}
    # garante que todos existam no dict
    for tk in tks:
        out.setdefault(tk, 0)
    return out


@st.cache_data(show_spinner=False, ttl=60 * 10)
def get_docs_by_ticker(
    ticker: str,
    *,
    limit_docs: int = 12,
    limit_chunks_per_doc: int = 6,
    prefer_chunks: bool = True,
) -> List[Dict[str, Any]]:
    """
    Retorna uma lista de itens no formato esperado pelo Patch6:
      [{source, date, text, title?, url?, tipo?, fonte?}, ...]

    Estratégia:
    - Busca docs_corporativos do ticker, ordenado por data desc / created_at desc
    - Se prefer_chunks=True, tenta puxar chunks por doc (mais granular)
    - Se não houver chunks, usa raw_text do doc (cortado)
    """
    tk = _norm_tk(ticker)
    if not tk:
        return []

    limit_docs = max(1, int(limit_docs))
    limit_chunks_per_doc = max(1, int(limit_chunks_per_doc))

    engine = get_supabase_engine()

    sql_docs = text(
        """
        SELECT id, ticker, data, fonte, tipo, titulo, url, raw_text, created_at
        FROM public.docs_corporativos
        WHERE ticker = :tk
        ORDER BY
          COALESCE(data, created_at::date) DESC,
          created_at DESC
        LIMIT :lim
        """
    )

    with engine.connect() as conn:
        docs = pd.read_sql_query(sql_docs, conn, params={"tk": tk, "lim": limit_docs})

    if docs is None or docs.empty:
        return []

    items: List[Dict[str, Any]] = []

    # carrega chunks em lote (mais rápido que um select por doc)
    chunks_map: Dict[int, List[str]] = {}
    if prefer_chunks:
        doc_ids = docs["id"].dropna().astype(int).tolist()
        if doc_ids:
            sql_chunks = text(
                """
                SELECT doc_id, chunk_index, chunk_text
                FROM public.docs_corporativos_chunks
                WHERE doc_id = ANY(:doc_ids)
                ORDER BY doc_id ASC, chunk_index ASC
                """
            )
            with engine.connect() as conn:
                ch = pd.read_sql_query(sql_chunks, conn, params={"doc_ids": doc_ids})

            if ch is not None and not ch.empty:
                ch["doc_id"] = ch["doc_id"].astype(int)
                ch["chunk_index"] = pd.to_numeric(ch["chunk_index"], errors="coerce").fillna(0).astype(int)
                ch = ch.sort_values(["doc_id", "chunk_index"])
                for did, grp in ch.groupby("doc_id"):
                    texts = [str(x) for x in grp["chunk_text"].astype(str).tolist() if str(x).strip()]
                    if texts:
                        chunks_map[int(did)] = texts[:limit_chunks_per_doc]

    # monta itens finais
    for _, r in docs.iterrows():
        doc_id = int(r["id"])
        data = None
        try:
            if pd.notna(r.get("data")):
                data = pd.to_datetime(r.get("data"), errors="coerce")
        except Exception:
            data = None

        date_str = data.date().isoformat() if (data is not None and pd.notna(data)) else "NA"

        fonte = str(r.get("fonte") or "NA").strip()
        tipo = str(r.get("tipo") or "NA").strip()
        titulo = str(r.get("titulo") or "").strip()
        url = str(r.get("url") or "").strip()

        # se tem chunks, vira vários itens (cada chunk como “text”)
        if doc_id in chunks_map:
            for i, chunk_text in enumerate(chunks_map[doc_id]):
                text_clean = str(chunk_text).strip()
                if not text_clean:
                    continue
                items.append(
                    {
                        "source": f"{fonte}:{tipo}",
                        "date": date_str,
                        "text": text_clean[:4000],
                        "title": titulo,
                        "url": url,
                        "fonte": fonte,
                        "tipo": tipo,
                        "doc_id": doc_id,
                        "chunk_index": i,
                    }
                )
        else:
            raw_text = str(r.get("raw_text") or "").strip()
            if not raw_text:
                continue
            items.append(
                {
                    "source": f"{fonte}:{tipo}",
                    "date": date_str,
                    "text": raw_text[:4000],
                    "title": titulo,
                    "url": url,
                    "fonte": fonte,
                    "tipo": tipo,
                    "doc_id": doc_id,
                }
            )

    return items


@st.cache_data(show_spinner=False, ttl=60 * 10)
def get_docs_by_tickers(
    tickers: Sequence[str],
    *,
    limit_docs_per_ticker: int = 10,
    limit_chunks_per_doc: int = 6,
    prefer_chunks: bool = True,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Retorna dict:
      { "PETR4": [ {source,date,text...}, ... ], "VALE3": [...] }

    Implementação: chama get_docs_by_ticker por ticker (cacheado).
    """
    tks = [_norm_tk(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys([t for t in tks if t]))
    out: Dict[str, List[Dict[str, Any]]] = {}
    for tk in tks:
        out[tk] = get_docs_by_ticker(
            tk,
            limit_docs=int(limit_docs_per_ticker),
            limit_chunks_per_doc=int(limit_chunks_per_doc),
            prefer_chunks=bool(prefer_chunks),
        )
    return out


__all__ = [
    "get_docs_by_ticker",
    "get_docs_by_tickers",
    "count_docs_by_tickers",
]
