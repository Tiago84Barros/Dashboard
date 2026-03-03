from __future__ import annotations

import hashlib
from typing import Dict, List, Optional

from sqlalchemy import text

from core.db_loader import get_supabase_engine
from core.ai_models.llm_client.factory import get_llm_client

# ---------------------------------------------------------------------
# Chunking defaults
# ---------------------------------------------------------------------
CHUNK_SIZE = 1200
CHUNK_OVERLAP = 200
MIN_TEXT_CHARS = 80  # evita chunkar lixo muito curto


# ---------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------
def _norm_ticker(t: str) -> str:
    return (t or "").strip().upper().replace(".SA", "").strip()


def split_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    """Split simples com overlap; determinístico e barato."""
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


# ---------------------------------------------------------------------
# Core routines
# ---------------------------------------------------------------------
def process_document_chunks(
    doc_id: int,
    *,
    chunk_size: int = CHUNK_SIZE,
    overlap: int = CHUNK_OVERLAP,
    min_text_chars: int = MIN_TEXT_CHARS,
) -> int:
    """
    Chunk + embed + insert para 1 doc_id.
    Retorna quantidade de chunks inseridos (int).
    """
    engine = get_supabase_engine()
    inserted = 0

    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                select
                    id,
                    ticker,
                    coalesce(raw_text, texto, '') as doc_text
                from public.docs_corporativos
                where id = :id
                """
            ),
            {"id": int(doc_id)},
        ).fetchone()

        if not row:
            return 0

        _, ticker, doc_text = row
        ticker = _norm_ticker(str(ticker))
        doc_text = (doc_text or "").strip()

        if len(doc_text) < int(min_text_chars):
            return 0

        chunks = split_text(doc_text, chunk_size=int(chunk_size), overlap=int(overlap))
        if not chunks:
            return 0

        llm = get_llm_client()

        for idx, chunk_text in enumerate(chunks):
            # defesa extra
            chunk_text = (chunk_text or "").strip()
            if not chunk_text:
                continue

            chunk_hash = hash_chunk(chunk_text)

            # dedupe
            exists = conn.execute(
                text(
                    """
                    select 1
                    from public.docs_corporativos_chunks
                    where chunk_hash = :h
                    limit 1
                    """
                ),
                {"h": chunk_hash},
            ).fetchone()
            if exists:
                continue

            emb = llm.embed([chunk_text])[0]

            conn.execute(
                text(
                    """
                    insert into public.docs_corporativos_chunks
                        (doc_id, ticker, chunk_index, chunk_text, embedding, chunk_hash)
                    values
                        (:doc_id, :ticker, :chunk_index, :chunk_text, :embedding, :chunk_hash)
                    on conflict (chunk_hash) do nothing
                    """
                ),
                {
                    "doc_id": int(doc_id),
                    "ticker": ticker,
                    "chunk_index": int(idx),
                    "chunk_text": chunk_text,
                    "embedding": emb,
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
    chunk_size: int = CHUNK_SIZE,
    overlap: int = CHUNK_OVERLAP,
    min_text_chars: int = MIN_TEXT_CHARS,
) -> Dict[str, object]:
    """
    Processa docs ainda sem chunks para um ticker.
    Retorna dict com auditoria (B + C).
    """
    tk = _norm_ticker(ticker)
    if not tk:
        return {"ticker": tk, "docs": 0, "docs_processed": 0, "chunks_inserted": 0, "reasons": {"invalid_ticker": 1}}

    engine = get_supabase_engine()

    # Auditoria / razões
    reasons = {
        "selected": 0,
        "no_text": 0,
        "too_short": 0,
        "chunked": 0,
        "already_had_chunks": 0,
        "errors": 0,
    }

    with engine.begin() as conn:
        sql = """
        select d.id,
               length(coalesce(d.raw_text, d.texto, ''))::int as text_len
        from public.docs_corporativos d
        where d.ticker = :tk
          and not exists (
              select 1 from public.docs_corporativos_chunks c
              where c.doc_id = d.id
          )
        """
        if only_with_text:
            sql += " and coalesce(d.raw_text, d.texto, '') <> '' "

        sql += " order by d.id desc limit :lim"

        rows = conn.execute(text(sql), {"tk": tk, "lim": int(limit_docs)}).fetchall()

    doc_ids: List[int] = []
    text_lens: Dict[int, int] = {}
    for r in rows:
        did = int(r[0])
        tl = int(r[1] or 0)
        doc_ids.append(did)
        text_lens[did] = tl

    reasons["selected"] = len(doc_ids)

    docs_processed = 0
    chunks_total = 0

    for did in doc_ids:
        tl = text_lens.get(did, 0)
        if tl <= 0:
            reasons["no_text"] += 1
            continue
        if tl < int(min_text_chars):
            reasons["too_short"] += 1
            continue

        try:
            n = process_document_chunks(
                did,
                chunk_size=int(chunk_size),
                overlap=int(overlap),
                min_text_chars=int(min_text_chars),
            )
            if n > 0:
                docs_processed += 1
                chunks_total += int(n)
                reasons["chunked"] += 1
        except Exception:
            reasons["errors"] += 1

    return {
        "ticker": tk,
        "docs": len(doc_ids),
        "docs_processed": docs_processed,
        "chunks_inserted": chunks_total,
        "reasons": reasons,
        "params": {
            "limit_docs": int(limit_docs),
            "only_with_text": bool(only_with_text),
            "chunk_size": int(chunk_size),
            "overlap": int(overlap),
            "min_text_chars": int(min_text_chars),
        },
    }
