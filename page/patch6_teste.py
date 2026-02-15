
# dashboard/page/patch6_teste.py
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st
from sqlalchemy import text

from core.db_loader import get_supabase_engine


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _parse_tickers(raw: str) -> List[str]:
    if not raw:
        return []
    out = []
    for p in raw.replace(";", ",").split(","):
        t = p.strip().upper()
        if t:
            out.append(t)
    return list(dict.fromkeys(out))


# ------------------------------------------------------------
# Supabase via SQLAlchemy
# ------------------------------------------------------------
def count_docs_by_tickers(tickers: List[str]) -> Tuple[int, Dict[str, int]]:
    engine = get_supabase_engine()
    total = 0
    by = {}

    sql = text("""
        SELECT ticker, COUNT(*) as cnt
        FROM public.docs_corporativos
        WHERE ticker = ANY(:tickers)
        GROUP BY ticker
    """)

    with engine.connect() as conn:
        result = conn.execute(sql, {"tickers": tickers}).fetchall()

    for row in result:
        by[row.ticker] = int(row.cnt)
        total += int(row.cnt)

    for tk in tickers:
        if tk not in by:
            by[tk] = 0

    return total, by


def get_recent_docs(ticker: str, limit: int = 20) -> List[Dict[str, Any]]:
    engine = get_supabase_engine()
    sql = text("""
        SELECT id, ticker, titulo, fonte, tipo, created_at
        FROM public.docs_corporativos
        WHERE ticker = :ticker
        ORDER BY id DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"ticker": ticker, "limit": limit}).fetchall()

    return [dict(r._mapping) for r in rows]


def get_chunks_for_rag(ticker: str, top_k: int = 25) -> List[str]:
    engine = get_supabase_engine()
    sql = text("""
        SELECT chunk_text
        FROM public.docs_corporativos_chunks
        WHERE ticker = :ticker
        ORDER BY id DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"ticker": ticker, "limit": top_k}).fetchall()

    return [r.chunk_text for r in rows]


# ------------------------------------------------------------
# LLM Stub (simplificado para teste)
# ------------------------------------------------------------
def run_llm_simples(ticker: str, chunks: List[str], manual_text: str) -> Dict[str, Any]:
    if not chunks and not manual_text.strip():
        return {"ok": False, "error": "Sem contexto disponível."}

    contexto = "\n\n".join(chunks[:5])[:2000]

    return {
        "ok": True,
        "result": {
            "ticker": ticker,
            "perspectiva_compra": "moderada",
            "resumo": "Teste LLM executado com contexto disponível.",
            "context_preview": contexto[:500],
        },
    }


# ------------------------------------------------------------
# UI
# ------------------------------------------------------------
def render():
    st.title("🧪 Patch6 — Teste SQLAlchemy")

    tickers_raw = st.text_input("Tickers", value="BBAS3")
    tickers = _parse_tickers(tickers_raw)

    if not tickers:
        st.warning("Informe ao menos 1 ticker.")
        return

    if st.button("Contar docs"):
        total, by = count_docs_by_tickers(tickers)
        st.success(f"Total docs: {total}")
        st.json(by)

    st.divider()

    ticker_sel = st.selectbox("Ticker", tickers)

    if st.button("Ver docs recentes"):
        docs = get_recent_docs(ticker_sel)
        st.json(docs)

    st.divider()

    manual_text = st.text_area("Texto manual opcional")

    if st.button("Rodar LLM"):
        chunks = get_chunks_for_rag(ticker_sel)
        out = run_llm_simples(ticker_sel, chunks, manual_text)
        st.json(out)


if __name__ == "__main__":
    render()
