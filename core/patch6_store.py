from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

from core.db_loader import get_supabase_engine
from core.ai_models.llm_client.factory import get_llm_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------
CHUNK_SIZE = 1200
CHUNK_OVERLAP = 200
MIN_TEXT_CHARS = 80
DEFAULT_LIMIT_DOCS = 60
DEFAULT_MAX_RUNTIME_S = 60.0

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _norm_ticker(t: str) -> str:
    return (t or "").strip().upper().replace(".SA", "").strip()


def _clean_text(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\x00", " ").split()).strip()


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def split_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    """Split determinístico com overlap."""
    txt = _clean_text(text)
    if not txt:
        return []

    chunk_size = max(100, int(chunk_size))
    overlap = max(0, min(int(overlap), chunk_size - 1))

    out: List[str] = []
    start = 0
    n = len(txt)
    while start < n:
        end = min(start + chunk_size, n)
        chunk = txt[start:end].strip()
        if chunk:
            out.append(chunk)
        if end >= n:
            break
        start = max(0, end - overlap)
    return out


def hash_chunk(doc_id: int, chunk_index: int, text_chunk: str) -> str:
    base = f"{int(doc_id)}:{int(chunk_index)}:{_clean_text(text_chunk)}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _infer_params(
    *,
    limit_docs: Optional[int] = None,
    only_with_text: bool = True,
    chunk_size: Optional[int] = None,
    overlap: Optional[int] = None,
    min_text_chars: Optional[int] = None,
    max_chars: Optional[int] = None,
    chunk_overlap: Optional[int] = None,
    max_runtime_s: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Compatibilidade com chamadas antigas e novas.
    Aceita tanto:
      - chunk_size / overlap
      - max_chars / chunk_overlap
      - max_runtime_s
    """
    resolved_chunk_size = chunk_size if chunk_size is not None else max_chars
    resolved_overlap = overlap if overlap is not None else chunk_overlap

    return {
        "limit_docs": _safe_int(limit_docs, DEFAULT_LIMIT_DOCS),
        "only_with_text": bool(only_with_text),
        "chunk_size": _safe_int(resolved_chunk_size, CHUNK_SIZE),
        "overlap": _safe_int(resolved_overlap, CHUNK_OVERLAP),
        "min_text_chars": _safe_int(min_text_chars, MIN_TEXT_CHARS),
        "max_runtime_s": _safe_float(max_runtime_s, DEFAULT_MAX_RUNTIME_S),
    }


def _get_chunk_table_columns(conn) -> set[str]:
    rows = conn.execute(
        text(
            """
            select column_name
            from information_schema.columns
            where table_schema = 'public'
              and table_name = 'docs_corporativos_chunks'
            """
        )
    ).fetchall()
    return {str(r[0]).lower() for r in rows}


def _get_doc_row(conn, doc_id: int):
    return conn.execute(
        text(
            """
            select
                d.id,
                d.ticker,
                coalesce(d.raw_text, d.texto, '') as doc_text,
                coalesce(d.data_doc, d.data) as document_date,
                coalesce(d.tipo, '') as categoria,
                coalesce(d.titulo, '') as titulo,
                coalesce(d.fonte, '') as fonte,
                coalesce(d.url, '') as url
            from public.docs_corporativos d
            where d.id = :id
            """
        ),
        {"id": int(doc_id)},
    ).mappings().fetchone()


def _get_embedder():
    try:
        return get_llm_client()
    except Exception as e:
        logger.warning("Falha ao inicializar cliente de embedding: %s", e)
        return None


def _embed_text(llm: Any, chunk_text: str) -> Optional[Any]:
    if llm is None:
        return None
    try:
        emb = llm.embed([chunk_text])
        if isinstance(emb, Sequence) and emb:
            return emb[0]
    except Exception as e:
        logger.warning("Falha ao gerar embedding do chunk: %s", e)
    return None


def _build_insert_statement(columns_present: set[str]) -> Tuple[str, List[str]]:
    insert_cols = ["doc_id", "ticker", "chunk_index", "chunk_text", "chunk_hash"]
    value_keys = ["doc_id", "ticker", "chunk_index", "chunk_text", "chunk_hash"]

    optional_pairs = [
        ("embedding", "embedding"),
        ("data_doc", "document_date"),
        ("tipo_doc", "categoria"),
        ("context_preview", "context_preview"),
    ]
    for col_name, value_key in optional_pairs:
        if col_name in columns_present:
            insert_cols.append(col_name)
            value_keys.append(value_key)

    cols_sql = ", ".join(insert_cols)
    vals_sql = ", ".join(f":{k}" for k in value_keys)
    sql = f"""
        insert into public.docs_corporativos_chunks ({cols_sql})
        values ({vals_sql})
        on conflict (chunk_hash) do nothing
    """
    return sql, value_keys


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
    Compatível com schemas antigos e novos.
    """
    engine = get_supabase_engine()
    inserted = 0

    with engine.begin() as conn:
        row = _get_doc_row(conn, int(doc_id))
        if not row:
            return 0

        ticker = _norm_ticker(str(row["ticker"] or ""))
        doc_text = _clean_text(row["doc_text"])
        if len(doc_text) < int(min_text_chars):
            return 0

        chunks = split_text(doc_text, chunk_size=int(chunk_size), overlap=int(overlap))
        if not chunks:
            return 0

        columns_present = _get_chunk_table_columns(conn)
        insert_sql, _ = _build_insert_statement(columns_present)
        llm = _get_embedder() if "embedding" in columns_present else None

        for idx, chunk_text in enumerate(chunks):
            chunk_text = _clean_text(chunk_text)
            if not chunk_text:
                continue

            chunk_hash = hash_chunk(int(doc_id), idx, chunk_text)

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

            payload = {
                "doc_id": int(doc_id),
                "ticker": ticker,
                "chunk_index": int(idx),
                "chunk_text": chunk_text,
                "chunk_hash": chunk_hash,
                "embedding": _embed_text(llm, chunk_text),
                "document_date": row["document_date"],
                "categoria": _clean_text(row["categoria"]),
                "context_preview": _clean_text(chunk_text[:280]),
            }

            conn.execute(text(insert_sql), payload)
            inserted += 1

    return inserted


def process_missing_chunks_for_ticker(
    ticker: str,
    *,
    limit_docs: int = DEFAULT_LIMIT_DOCS,
    only_with_text: bool = True,
    chunk_size: Optional[int] = None,
    overlap: Optional[int] = None,
    min_text_chars: Optional[int] = None,
    max_chars: Optional[int] = None,
    chunk_overlap: Optional[int] = None,
    max_runtime_s: Optional[float] = None,
) -> Dict[str, object]:
    """
    Processa docs sem chunks para um ticker.

    Compatibilidade:
    - chamada nova: chunk_size / overlap
    - chamada antiga: max_chars / chunk_overlap / max_runtime_s
    """
    tk = _norm_ticker(ticker)
    params = _infer_params(
        limit_docs=limit_docs,
        only_with_text=only_with_text,
        chunk_size=chunk_size,
        overlap=overlap,
        min_text_chars=min_text_chars,
        max_chars=max_chars,
        chunk_overlap=chunk_overlap,
        max_runtime_s=max_runtime_s,
    )

    if not tk:
        return {
            "ticker": tk,
            "docs": 0,
            "docs_processed": 0,
            "chunks_inserted": 0,
            "reasons": {"invalid_ticker": 1},
            "params": params,
        }

    engine = get_supabase_engine()
    started = time.monotonic()

    reasons = {
        "selected": 0,
        "no_text": 0,
        "too_short": 0,
        "chunked": 0,
        "already_had_chunks": 0,
        "timeout": 0,
        "errors": 0,
    }

    with engine.begin() as conn:
        sql = """
        select
            d.id,
            length(coalesce(d.raw_text, d.texto, ''))::int as text_len
        from public.docs_corporativos d
        where d.ticker = :tk
          and not exists (
              select 1
              from public.docs_corporativos_chunks c
              where c.doc_id = d.id
          )
        """
        if params["only_with_text"]:
            sql += " and coalesce(d.raw_text, d.texto, '') <> '' "

        sql += " order by coalesce(d.data_doc, d.data) desc nulls last, d.id desc limit :lim"
        rows = conn.execute(text(sql), {"tk": tk, "lim": int(params["limit_docs"])}).fetchall()

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
        if time.monotonic() - started > float(params["max_runtime_s"]):
            reasons["timeout"] += 1
            break

        tl = text_lens.get(did, 0)
        if tl <= 0:
            reasons["no_text"] += 1
            continue
        if tl < int(params["min_text_chars"]):
            reasons["too_short"] += 1
            continue

        try:
            n = process_document_chunks(
                did,
                chunk_size=int(params["chunk_size"]),
                overlap=int(params["overlap"]),
                min_text_chars=int(params["min_text_chars"]),
            )
            if n > 0:
                docs_processed += 1
                chunks_total += int(n)
                reasons["chunked"] += 1
        except Exception as e:
            reasons["errors"] += 1
            logger.exception("Erro ao processar chunks do ticker=%s doc_id=%s: %s", tk, did, e)

    return {
        "ticker": tk,
        "docs": len(doc_ids),
        "docs_processed": docs_processed,
        "chunks_inserted": chunks_total,
        "reasons": reasons,
        "params": params,
        "elapsed_s": round(time.monotonic() - started, 3),
    }
