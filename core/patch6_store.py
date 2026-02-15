
from __future__ import annotations

import hashlib
from typing import List

from sqlalchemy import text
from core.db_loader import get_engine

# Ajuste o import abaixo se seu projeto usar outro caminho para embeddings
from core.ai_models.llm_client import get_embedding


CHUNK_SIZE = 1200
CHUNK_OVERLAP = 200


def split_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    if not text:
        return []

    chunks = []
    start = 0
    text_len = len(text)

    while start < text_len:
        end = min(start + chunk_size, text_len)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start = end - overlap

    return chunks


def hash_chunk(text_chunk: str) -> str:
    return hashlib.sha256(text_chunk.encode("utf-8")).hexdigest()


def process_document_chunks(doc_id: int) -> int:
    engine = get_engine()
    inserted = 0

    with engine.begin() as conn:

        # Buscar documento principal
        result = conn.execute(
            text("SELECT id, ticker, texto FROM public.docs_corporativos WHERE id = :id"),
            {"id": doc_id},
        ).fetchone()

        if not result:
            return 0

        _, ticker, texto = result

        chunks = split_text(texto)

        for idx, chunk_text in enumerate(chunks):
            chunk_hash = hash_chunk(chunk_text)

            # Evitar duplicação
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

            embedding = get_embedding(chunk_text)

            conn.execute(
                text("""
                    INSERT INTO public.docs_corporativos_chunks
                    (doc_id, ticker, chunk_index, chunk_text, embedding, chunk_hash)
                    VALUES (:doc_id, :ticker, :chunk_index, :chunk_text, :embedding, :chunk_hash)
                """),
                {
                    "doc_id": doc_id,
                    "ticker": ticker,
                    "chunk_index": idx,
                    "chunk_text": chunk_text,
                    "embedding": embedding,
                    "chunk_hash": chunk_hash,
                },
            )

            inserted += 1

    return inserted
