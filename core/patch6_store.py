from __future__ import annotations

import hashlib
from typing import Dict, List

from sqlalchemy import text
from core.db_loader import get_supabase_engine
from core.ai_models.llm_client.factory import get_llm_client

CHUNK_SIZE = 1200
CHUNK_OVERLAP = 200


def split_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    if not text:
        return []
    chunks: List[str] = []
    start = 0
    text_len = len(text)
    while start < text_len:
        end = min(start + chunk_size, text_len)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start = max(0, end - overlap)
        if end == text_len:
            break
    return chunks


def hash_chunk(text_chunk: str) -> str:
    return hashlib.sha256(text_chunk.encode("utf-8")).hexdigest()


def process_document_chunks(doc_id: int) -> int:
    engine = get_supabase_engine()
    inserted = 0

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT id, ticker, texto FROM public.docs_corporativos WHERE id = :id"),
            {"id": doc_id},
        ).fetchone()

        if not row:
            return 0

        _, ticker, texto = row
        texto = (texto or "").strip()
        if not texto:
            return 0

        chunks = split_text(texto)

        # instancia cliente UMA VEZ por documento
        llm = get_llm_client()

        for idx, chunk_text in enumerate(chunks):
            chunk_hash = hash_chunk(chunk_text)

            exists = conn.execute(
                text("""
                    SELECT 1 FROM public.docs_corporativos_chunks
                    WHERE chunk_hash = :chunk_hash
                    LIMIT 1
                """),
                {"chunk_hash": chunk_hash},
            ).fetchone()

            if exists:
                continue

            embedding = llm.embed([chunk_text])[0]

            conn.execute(
                text("""
                    INSERT INTO public.docs_corporativos_chunks
                    (doc_id, ticker, chunk_index, chunk_text, embedding, chunk_hash)
                    VALUES (:doc_id, :ticker, :chunk_index, :chunk_text, :embedding, :chunk_hash)
                    ON CONFLICT (chunk_hash) DO NOTHING
                """),
                {
                    "doc_id": int(doc_id),
                    "ticker": str(ticker),
                    "chunk_index": int(idx),
                    "chunk_text": chunk_text,
                    "embedding": embedding,
                    "chunk_hash": chunk_hash,
                },
            )
            inserted += 1

    return inserted


def process_missing_chunks_for_ticker(
    ticker: str,
    *,
    limit_docs: int = 50,
    only_with_text: bool = True,
) -> Dict[str, int]:
    tk = (ticker or "").strip().upper().replace(".SA", "")
    if not tk:
        return {"docs": 0, "docs_processed": 0, "chunks_inserted": 0}

    engine = get_supabase_engine()
    with engine.begin() as conn:
        sql = """
        SELECT d.id
        FROM public.docs_corporativos d
        WHERE d.ticker = :tk
          AND NOT EXISTS (
            SELECT 1 FROM public.docs_corporativos_chunks c
            WHERE c.doc_id = d.id
          )
        """
        if only_with_text:
            sql += " AND COALESCE(d.texto,'') <> '' "
        sql += " ORDER BY d.id DESC LIMIT :lim"

        rows = conn.execute(text(sql), {"tk": tk, "lim": int(limit_docs)}).fetchall()
        doc_ids = [int(r[0]) for r in rows]

    chunks_total = 0
    docs_processed = 0
    for did in doc_ids:
        n = process_document_chunks(did)
        if n > 0:
            docs_processed += 1
            chunks_total += n

    return {"docs": len(doc_ids), "docs_processed": docs_processed, "chunks_inserted": chunks_total}
