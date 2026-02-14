# pickup/docs_rag.py
# ──────────────────────────────────────────────────────────────────────────────
# Leitura de documentos (RAG) no Supabase para o Patch 6 / Patch6_teste
#
# Tabelas esperadas:
#   public.docs_corporativos
#   public.docs_corporativos_chunks
#
# Funções esperadas (pelos imports do seu patch6_teste):
#   - get_docs_by_ticker(ticker, limit_docs=..., limit_chars=...)
#   - get_chunks_by_ticker(ticker, limit_chunks=..., limit_chars=...)
#   - count_docs_by_tickers(tickers)   ✅ (corrige seu erro atual)
#
# Depende de:
#   - SUPABASE_DB_URL ou DATABASE_URL
#   - sqlalchemy, pandas, streamlit
# ──────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ──────────────────────────────────────────────────────────────────────────────
# Engine / SQL helpers
# ──────────────────────────────────────────────────────────────────────────────

def _get_supabase_url() -> str:
    db_url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Defina SUPABASE_DB_URL (ou DATABASE_URL) nas secrets/env vars.")
    return db_url


@st.cache_resource(show_spinner=False)
def get_supabase_engine() -> Engine:
    return create_engine(_get_supabase_url(), pool_pre_ping=True)


def _read_sql_df(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    eng = get_supabase_engine()
    with eng.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


def _norm_ticker(t: str) -> str:
    return (t or "").strip().upper().replace(".SA", "")


# ──────────────────────────────────────────────────────────────────────────────
# Public API (Patch 6)
# ──────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def count_docs_by_tickers(tickers: List[str]) -> Dict[str, int]:
    """
    Retorna contagem de docs por ticker na tabela public.docs_corporativos.
    Ex.: {"ROMI3": 12, "BBAS3": 3}
    """
    tickers_n = sorted({_norm_ticker(t) for t in (tickers or []) if _norm_ticker(t)})
    if not tickers_n:
        return {}

    # Evita IN () vazio e mantém performance
    df = _read_sql_df(
        """
        SELECT ticker, COUNT(*)::int AS qtd
        FROM public.docs_corporativos
        WHERE ticker = ANY(:tickers)
        GROUP BY ticker
        """,
        {"tickers": tickers_n},
    )

    out = {t: 0 for t in tickers_n}
    if df is None or df.empty:
        return out

    for _, r in df.iterrows():
        tk = _norm_ticker(str(r.get("ticker", "")))
        q = int(r.get("qtd", 0) or 0)
        if tk:
            out[tk] = q
    return out


@st.cache_data(show_spinner=False)
def get_docs_by_ticker(
    ticker: str,
    *,
    limit_docs: int = 8,
    limit_chars: int = 120_000,
) -> List[Dict[str, Any]]:
    """
    Retorna lista de docs (metadados + raw_text) do ticker.
    Útil quando você quer dar para a IA um "pacote" de contexto por ticker.
    """
    tk = _norm_ticker(ticker)
    if not tk:
        return []

    limit_docs = int(max(1, limit_docs))
    limit_chars = int(max(1_000, limit_chars))

    df = _read_sql_df(
        """
        SELECT
          id, ticker, data, fonte, tipo, titulo, url, raw_text, lang, doc_hash, created_at
        FROM public.docs_corporativos
        WHERE ticker = :tk
        ORDER BY data DESC NULLS LAST, created_at DESC
        LIMIT :lim
        """,
        {"tk": tk, "lim": limit_docs},
    )

    if df is None or df.empty:
        return []

    docs: List[Dict[str, Any]] = []
    total_chars = 0

    for _, r in df.iterrows():
        raw = str(r.get("raw_text") or "")
        if not raw.strip():
            continue

        # controla tamanho total
        remaining = limit_chars - total_chars
        if remaining <= 0:
            break
        if len(raw) > remaining:
            raw = raw[:remaining]

        docs.append(
            {
                "id": int(r.get("id")),
                "ticker": _norm_ticker(str(r.get("ticker") or "")),
                "data": str(r.get("data") or ""),
                "fonte": str(r.get("fonte") or ""),
                "tipo": str(r.get("tipo") or ""),
                "titulo": str(r.get("titulo") or ""),
                "url": str(r.get("url") or ""),
                "lang": str(r.get("lang") or "pt"),
                "doc_hash": str(r.get("doc_hash") or ""),
                "raw_text": raw,
            }
        )
        total_chars += len(raw)

    return docs


@st.cache_data(show_spinner=False)
def get_chunks_by_ticker(
    ticker: str,
    *,
    limit_chunks: int = 50,
    limit_chars: int = 120_000,
) -> List[Dict[str, Any]]:
    """
    Retorna chunks por ticker (mais eficiente do que raw_text gigante).
    Ideal para RAG simples: mandar só os trechos.
    """
    tk = _norm_ticker(ticker)
    if not tk:
        return []

    limit_chunks = int(max(1, limit_chunks))
    limit_chars = int(max(1_000, limit_chars))

    df = _read_sql_df(
        """
        SELECT
          c.id,
          c.doc_id,
          c.ticker,
          c.chunk_index,
          c.chunk_text,
          c.chunk_hash,
          d.data,
          d.fonte,
          d.tipo,
          d.titulo,
          d.url
        FROM public.docs_corporativos_chunks c
        JOIN public.docs_corporativos d ON d.id = c.doc_id
        WHERE c.ticker = :tk
        ORDER BY d.data DESC NULLS LAST, d.created_at DESC, c.chunk_index ASC
        LIMIT :lim
        """,
        {"tk": tk, "lim": limit_chunks},
    )

    if df is None or df.empty:
        return []

    out: List[Dict[str, Any]] = []
    total = 0

    for _, r in df.iterrows():
        ch = str(r.get("chunk_text") or "")
        if not ch.strip():
            continue

        remaining = limit_chars - total
        if remaining <= 0:
            break
        if len(ch) > remaining:
            ch = ch[:remaining]

        out.append(
            {
                "id": int(r.get("id")),
                "doc_id": int(r.get("doc_id")),
                "ticker": _norm_ticker(str(r.get("ticker") or "")),
                "chunk_index": int(r.get("chunk_index") or 0),
                "chunk_hash": str(r.get("chunk_hash") or ""),
                "data": str(r.get("data") or ""),
                "fonte": str(r.get("fonte") or ""),
                "tipo": str(r.get("tipo") or ""),
                "titulo": str(r.get("titulo") or ""),
                "url": str(r.get("url") or ""),
                "chunk_text": ch,
            }
        )
        total += len(ch)

    return out


def format_docs_for_llm(
    docs_or_chunks: List[Dict[str, Any]],
    *,
    mode: str = "chunks",
    max_chars: int = 120_000,
) -> str:
    """
    Utilitário opcional: transforma docs/chunks em um único texto para prompt.
    mode:
      - "chunks": usa chunk_text
      - "docs": usa raw_text
    """
    mode = (mode or "chunks").lower().strip()
    max_chars = int(max(1_000, max_chars))

    parts: List[str] = []
    total = 0

    for item in docs_or_chunks or []:
        if mode == "docs":
            body = str(item.get("raw_text") or "")
        else:
            body = str(item.get("chunk_text") or "")

        header = (
            f"[TICKER:{item.get('ticker','')}] "
            f"[DATA:{item.get('data','')}] "
            f"[FONTE:{item.get('fonte','')}] "
            f"[TIPO:{item.get('tipo','')}] "
            f"[TITULO:{item.get('titulo','')}] "
            f"[URL:{item.get('url','')}]"
        ).strip()

        block = f"{header}\n{body}\n"
        remaining = max_chars - total
        if remaining <= 0:
            break
        if len(block) > remaining:
            block = block[:remaining]

        parts.append(block)
        total += len(block)

    return "\n---\n".join(parts).strip()


__all__ = [
    "get_supabase_engine",
    "count_docs_by_tickers",
    "get_docs_by_ticker",
    "get_chunks_by_ticker",
    "format_docs_for_llm",
]
