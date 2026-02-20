# docs_corporativos_store.py
from __future__ import annotations

import hashlib
from typing import List, Dict, Any

import pandas as pd
from sqlalchemy import text

from core.db_loader import get_supabase_engine


# ============================================================
# Contagem
# ============================================================

def count_docs(ticker: str) -> int:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        r = conn.execute(
            text("select count(*) from public.docs_corporativos where ticker = :tk"),
            {"tk": ticker.upper()},
        )
        return int(r.scalar() or 0)


def count_chunks(ticker: str) -> int:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        r = conn.execute(
            text("select count(*) from public.docs_corporativos_chunks where ticker = :tk"),
            {"tk": ticker.upper()},
        )
        return int(r.scalar() or 0)


# ============================================================
# Chunking
# ============================================================

def _split_text(texto: str, max_chars: int = 1500) -> List[str]:
    partes = []
    atual = ""
    for linha in texto.split("\n"):
        if len(atual) + len(linha) < max_chars:
            atual += linha + "\n"
        else:
            partes.append(atual.strip())
            atual = linha + "\n"
    if atual.strip():
        partes.append(atual.strip())
    return partes


def process_missing_chunks_for_ticker(ticker: str, limit_docs: int = 50):
    engine = get_supabase_engine()

    with engine.begin() as conn:
        docs = pd.read_sql_query(
            text("""
                select id, coalesce(raw_text, texto) as texto
                from public.docs_corporativos
                where ticker = :tk
                order by data desc
                limit :lim
            """),
            conn,
            params={"tk": ticker.upper(), "lim": limit_docs},
        )

        for _, row in docs.iterrows():
            doc_id = row["id"]
            texto = row["texto"] or ""
            if not texto.strip():
                continue

            partes = _split_text(texto)

            for idx, chunk in enumerate(partes):
                h = hashlib.md5(chunk.encode()).hexdigest()

                conn.execute(
                    text("""
                        insert into public.docs_corporativos_chunks
                        (doc_id, ticker, chunk_index, chunk_text, chunk_hash)
                        values (:doc_id, :tk, :idx, :txt, :h)
                        on conflict (chunk_hash) do nothing
                    """),
                    {
                        "doc_id": doc_id,
                        "tk": ticker.upper(),
                        "idx": idx,
                        "txt": chunk,
                        "h": h,
                    },
                )


# ============================================================
# Top-K para RAG
# ============================================================

def fetch_topk_chunks(ticker: str, k: int = 6) -> List[str]:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        df = pd.read_sql_query(
            text("""
                select chunk_text
                from public.docs_corporativos_chunks
                where ticker = :tk
                order by created_at desc
                limit :k
            """),
            conn,
            params={"tk": ticker.upper(), "k": k},
        )
    return df["chunk_text"].tolist()
