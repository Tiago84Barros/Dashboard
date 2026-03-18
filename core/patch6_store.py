from __future__ import annotations

import hashlib
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence

from sqlalchemy import text

from core.ai_models.llm_client.factory import get_llm_client
from core.db_loader import get_supabase_engine

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1200
CHUNK_OVERLAP = 200
MIN_TEXT_CHARS = 80
DEFAULT_LIMIT_DOCS = 60
DEFAULT_MAX_RUNTIME_S = 90.0


@dataclass(frozen=True)
class SchemaInfo:
    doc_columns: set[str]
    chunk_columns: set[str]
    doc_text_expr: str
    doc_date_expr: Optional[str]
    doc_category_expr: str
    doc_title_expr: str
    doc_source_expr: str
    doc_url_expr: str


def _norm_ticker(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "").strip()


def _clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    txt = str(value).replace("\x00", " ").replace("\u00a0", " ")
    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip()


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


def _coalesce_expr(columns: Iterable[str], candidates: Sequence[str], fallback: str) -> str:
    existing = [c for c in candidates if c in columns]
    if not existing:
        return fallback
    if len(existing) == 1:
        return existing[0]
    return "coalesce(" + ", ".join(existing) + ")"


def _get_table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(
        text(
            """
            select column_name
            from information_schema.columns
            where table_schema = 'public'
              and table_name = :table_name
            """
        ),
        {"table_name": table_name},
    ).fetchall()
    return {str(r[0]).lower() for r in rows}


def _detect_schema(conn) -> SchemaInfo:
    doc_columns = _get_table_columns(conn, "docs_corporativos")
    chunk_columns = _get_table_columns(conn, "docs_corporativos_chunks")

    doc_text_expr = _coalesce_expr(doc_columns, ["raw_text", "texto", "text", "content"], "''")
    doc_date_expr = None
    date_candidates = ["data_doc", "document_date", "data", "created_at"]
    existing_dates = [c for c in date_candidates if c in doc_columns]
    if existing_dates:
        doc_date_expr = _coalesce_expr(doc_columns, date_candidates, "null")

    return SchemaInfo(
        doc_columns=doc_columns,
        chunk_columns=chunk_columns,
        doc_text_expr=doc_text_expr,
        doc_date_expr=doc_date_expr,
        doc_category_expr=_coalesce_expr(doc_columns, ["tipo", "categoria", "doc_type", "type"], "''"),
        doc_title_expr=_coalesce_expr(doc_columns, ["titulo", "title"], "''"),
        doc_source_expr=_coalesce_expr(doc_columns, ["fonte", "source"], "''"),
        doc_url_expr=_coalesce_expr(doc_columns, ["url", "link"], "''"),
    )


def split_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    txt = _clean_text(text)
    if not txt:
        return []

    chunk_size = max(200, int(chunk_size))
    overlap = max(0, min(int(overlap), chunk_size - 1))

    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", txt) if p.strip()]
    if not paragraphs:
        paragraphs = [txt]

    chunks: List[str] = []
    current = ""

    for paragraph in paragraphs:
        if len(paragraph) > chunk_size:
            if current:
                chunks.append(current.strip())
                current = ""
            start = 0
            while start < len(paragraph):
                end = min(start + chunk_size, len(paragraph))
                piece = paragraph[start:end].strip()
                if piece:
                    chunks.append(piece)
                if end >= len(paragraph):
                    break
                start = max(0, end - overlap)
            continue

        candidate = f"{current}\n\n{paragraph}".strip() if current else paragraph
        if len(candidate) <= chunk_size:
            current = candidate
            continue

        if current:
            chunks.append(current.strip())
            tail = current[-overlap:].strip() if overlap > 0 and len(current) > overlap else current
            current = f"{tail}\n\n{paragraph}".strip()
        else:
            current = paragraph

        if len(current) > chunk_size:
            start = 0
            while start < len(current):
                end = min(start + chunk_size, len(current))
                piece = current[start:end].strip()
                if piece:
                    chunks.append(piece)
                if end >= len(current):
                    current = ""
                    break
                start = max(0, end - overlap)

    if current:
        chunks.append(current.strip())

    return [c for c in chunks if c]


def hash_chunk(doc_id: int, chunk_index: int, text_chunk: str) -> str:
    payload = f"{int(doc_id)}:{int(chunk_index)}:{_clean_text(text_chunk)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _get_embedder() -> Optional[Any]:
    try:
        return get_llm_client()
    except Exception as exc:
        logger.warning("Falha ao inicializar cliente de embedding: %s", exc)
        return None


def _embed_text(llm: Any, chunk_text: str) -> Optional[Any]:
    if llm is None:
        return None
    try:
        vectors = llm.embed([chunk_text])
        if isinstance(vectors, Sequence) and vectors:
            return vectors[0]
    except Exception as exc:
        logger.warning("Falha ao gerar embedding: %s", exc)
    return None


def _build_doc_select_sql(schema: SchemaInfo) -> str:
    date_expr = schema.doc_date_expr or "null"
    return f"""
        select
            d.id,
            d.ticker,
            {schema.doc_text_expr} as doc_text,
            {date_expr} as document_date,
            {schema.doc_category_expr} as categoria,
            {schema.doc_title_expr} as titulo,
            {schema.doc_source_expr} as fonte,
            {schema.doc_url_expr} as url
        from public.docs_corporativos d
        where d.id = :id
    """


def _fetch_doc_row(conn, schema: SchemaInfo, doc_id: int):
    return conn.execute(text(_build_doc_select_sql(schema)), {"id": int(doc_id)}).mappings().fetchone()


def _build_missing_docs_sql(schema: SchemaInfo, only_with_text: bool) -> str:
    date_expr = schema.doc_date_expr
    order_clause = f"order by {date_expr} desc nulls last, d.id desc" if date_expr else "order by d.id desc"
    where_text = f" and {schema.doc_text_expr} <> '' " if only_with_text else ""

    return f"""
        select
            d.id,
            length({schema.doc_text_expr})::int as text_len
        from public.docs_corporativos d
        where d.ticker = :tk
          and not exists (
              select 1
              from public.docs_corporativos_chunks c
              where c.doc_id = d.id
          )
          {where_text}
        {order_clause}
        limit :lim
    """


def _build_insert_statement(chunk_columns: set[str]) -> str:
    cols = ["doc_id", "ticker", "chunk_index", "chunk_text", "chunk_hash"]
    values = [":doc_id", ":ticker", ":chunk_index", ":chunk_text", ":chunk_hash"]

    optional_map = [
        ("embedding", ":embedding"),
        ("document_date", ":document_date"),
        ("data_doc", ":document_date"),
        ("categoria", ":categoria"),
        ("tipo_doc", ":categoria"),
        ("context_preview", ":context_preview"),
        ("titulo", ":titulo"),
        ("fonte", ":fonte"),
        ("url", ":url"),
    ]
    for col, val in optional_map:
        if col in chunk_columns:
            cols.append(col)
            values.append(val)

    return f"""
        insert into public.docs_corporativos_chunks ({', '.join(cols)})
        values ({', '.join(values)})
        on conflict (chunk_hash) do nothing
    """


def process_document_chunks(
    doc_id: int,
    *,
    chunk_size: int = CHUNK_SIZE,
    overlap: int = CHUNK_OVERLAP,
    min_text_chars: int = MIN_TEXT_CHARS,
) -> int:
    engine = get_supabase_engine()
    inserted = 0

    with engine.begin() as conn:
        schema = _detect_schema(conn)
        row = _fetch_doc_row(conn, schema, int(doc_id))
        if not row:
            return 0

        ticker = _norm_ticker(str(row.get("ticker") or ""))
        doc_text = _clean_text(row.get("doc_text"))
        if len(doc_text) < int(min_text_chars):
            return 0

        chunks = split_text(doc_text, chunk_size=int(chunk_size), overlap=int(overlap))
        if not chunks:
            return 0

        insert_sql = _build_insert_statement(schema.chunk_columns)
        llm = _get_embedder() if "embedding" in schema.chunk_columns else None

        for idx, chunk_text in enumerate(chunks):
            chunk_text = _clean_text(chunk_text)
            if not chunk_text:
                continue

            chunk_hash = hash_chunk(int(doc_id), idx, chunk_text)
            exists = conn.execute(
                text("select 1 from public.docs_corporativos_chunks where chunk_hash = :h limit 1"),
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
                "document_date": row.get("document_date"),
                "categoria": _clean_text(row.get("categoria")),
                "context_preview": _clean_text(chunk_text[:280]),
                "titulo": _clean_text(row.get("titulo")),
                "fonte": _clean_text(row.get("fonte")),
                "url": _clean_text(row.get("url")),
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
) -> Dict[str, Any]:
    """
    Versão institucional, compatível com chamadas novas e legadas.

    Aceita:
    - chunk_size / overlap
    - max_chars / chunk_overlap
    - max_runtime_s
    """
    tk = _norm_ticker(ticker)
    resolved_chunk_size = _safe_int(chunk_size if chunk_size is not None else max_chars, CHUNK_SIZE)
    resolved_overlap = _safe_int(overlap if overlap is not None else chunk_overlap, CHUNK_OVERLAP)
    resolved_min_text_chars = _safe_int(min_text_chars, MIN_TEXT_CHARS)
    resolved_limit_docs = _safe_int(limit_docs, DEFAULT_LIMIT_DOCS)
    resolved_max_runtime_s = _safe_float(max_runtime_s, DEFAULT_MAX_RUNTIME_S)

    result: Dict[str, Any] = {
        "ticker": tk,
        "docs": 0,
        "docs_processed": 0,
        "chunks_inserted": 0,
        "elapsed_s": 0.0,
        "schema": {},
        "reasons": {
            "selected": 0,
            "no_text": 0,
            "too_short": 0,
            "chunked": 0,
            "timeout": 0,
            "errors": 0,
        },
        "params": {
            "limit_docs": resolved_limit_docs,
            "chunk_size": resolved_chunk_size,
            "overlap": resolved_overlap,
            "min_text_chars": resolved_min_text_chars,
            "max_runtime_s": resolved_max_runtime_s,
            "only_with_text": bool(only_with_text),
        },
    }
    if not tk:
        result["reasons"]["errors"] = 1
        return result

    engine = get_supabase_engine()
    started = time.monotonic()

    with engine.begin() as conn:
        schema = _detect_schema(conn)
        result["schema"] = {
            "doc_columns": sorted(schema.doc_columns),
            "chunk_columns": sorted(schema.chunk_columns),
            "doc_date_expr": schema.doc_date_expr,
        }

        rows = conn.execute(
            text(_build_missing_docs_sql(schema, only_with_text=bool(only_with_text))),
            {"tk": tk, "lim": resolved_limit_docs},
        ).fetchall()

    doc_ids: List[int] = []
    text_lens: Dict[int, int] = {}
    for row in rows:
        doc_id = int(row[0])
        text_len = int(row[1] or 0)
        doc_ids.append(doc_id)
        text_lens[doc_id] = text_len

    result["docs"] = len(doc_ids)
    result["reasons"]["selected"] = len(doc_ids)

    for doc_id in doc_ids:
        if time.monotonic() - started > resolved_max_runtime_s:
            result["reasons"]["timeout"] += 1
            break

        text_len = text_lens.get(doc_id, 0)
        if text_len <= 0:
            result["reasons"]["no_text"] += 1
            continue
        if text_len < resolved_min_text_chars:
            result["reasons"]["too_short"] += 1
            continue

        try:
            inserted = process_document_chunks(
                doc_id,
                chunk_size=resolved_chunk_size,
                overlap=resolved_overlap,
                min_text_chars=resolved_min_text_chars,
            )
            if inserted > 0:
                result["docs_processed"] += 1
                result["chunks_inserted"] += int(inserted)
                result["reasons"]["chunked"] += 1
        except Exception as exc:
            result["reasons"]["errors"] += 1
            logger.exception("Erro ao processar ticker=%s doc_id=%s: %s", tk, doc_id, exc)

    result["elapsed_s"] = round(time.monotonic() - started, 3)
    return result
