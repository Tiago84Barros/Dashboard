
from __future__ import annotations

import hashlib
from typing import Dict, List

from sqlalchemy import text

from core.db_loader import get_supabase_engine
from core.ai_models.llm_client.factory import get_llm_client

CHUNK_SIZE = 1200
CHUNK_OVERLAP = 200
MIN_TEXT_CHARS = 80


def _norm_ticker(t: str) -> str:
    return (t or "").strip().upper().replace(".SA", "").strip()


def split_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    if not text:
        return []
    out: List[str] = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + int(chunk_size), n)
        chunk = text[start:end].strip()
        if chunk:
            out.append(chunk)
        if end >= n:
            break
        start = max(0, end - int(overlap))
    return out


def hash_chunk(text_chunk: str) -> str:
    return hashlib.sha256((text_chunk or "").encode("utf-8")).hexdigest()


def process_document_chunks(doc_id: int, *, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP, min_text_chars: int = MIN_TEXT_CHARS) -> int:
    engine = get_supabase_engine()
    inserted = 0

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                select
                    id,
                    ticker,
                    coalesce(raw_text, texto, '') as doc_text,
                    data as document_date,
                    tipo as categoria
                from public.docs_corporativos
                where id = :id
            """),
            {"id": int(doc_id)},
        ).fetchone()

        if not row:
            return 0

        _, ticker, doc_text, document_date, categoria = row
        ticker = _norm_ticker(str(ticker))
        doc_text = (doc_text or "").strip()
        categoria = (categoria or "").strip() if categoria else ""

        if len(doc_text) < int(min_text_chars):
            return 0

        chunks = split_text(doc_text, chunk_size=int(chunk_size), overlap=int(overlap))
        if not chunks:
            return 0

        llm = get_llm_client()

        for idx, chunk_text in enumerate(chunks):
            chunk_text = (chunk_text or "").strip()
            if not chunk_text:
                continue

            chunk_hash = hash_chunk(chunk_text)

            exists = conn.execute(
                text("""
                    select 1
                    from public.docs_corporativos_chunks
                    where chunk_hash = :h
                    limit 1
                """),
                {"h": chunk_hash},
            ).fetchone()
            if exists:
                continue

            emb = llm.embed([chunk_text])[0]

            conn.execute(
                text("""
                    insert into public.docs_corporativos_chunks
                    (
                        doc_id,
                        ticker,
                        chunk_index,
                        chunk_text,
                        embedding,
                        chunk_hash,
                        document_date,
                        categoria
                    )
                    values
                    (
                        :doc_id,
                        :ticker,
                        :chunk_index,
                        :chunk_text,
                        :embedding,
                        :chunk_hash,
                        :document_date,
                        :categoria
                    )
                    on conflict (chunk_hash) do nothing
                """),
                {
                    "doc_id": int(doc_id),
                    "ticker": ticker,
                    "chunk_index": int(idx),
                    "chunk_text": chunk_text,
                    "embedding": emb,
                    "chunk_hash": chunk_hash,
                    "document_date": document_date,
                    "categoria": categoria,
                },
            )
            inserted += 1

    return inserted


def process_missing_chunks_for_ticker(ticker: str, *, limit_docs: int = 500, chunk_size: int = CHUNK_SIZE) -> Dict[str, int]:
    tk = _norm_ticker(ticker)
    engine = get_supabase_engine()

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                select d.id
                from public.docs_corporativos d
                where d.ticker = :tk
                  and not exists (
                      select 1 from public.docs_corporativos_chunks c
                      where c.doc_id = d.id
                  )
                order by d.data desc nulls last
                limit :lim
            """),
            {"tk": tk, "lim": int(limit_docs)},
        ).fetchall()

    total_chunks = 0
    docs_processed = 0

    for r in rows:
        doc_id = int(r[0])
        n = process_document_chunks(doc_id, chunk_size=chunk_size)
        if n > 0:
            docs_processed += 1
            total_chunks += n

    return {
        "ticker": tk,
        "docs_processed": docs_processed,
        "chunks_inserted": total_chunks,
    }
