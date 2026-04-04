# -*- coding: utf-8 -*-
"""
core/docs_corporativos_store.py

Contrato institucional para persistência documental do corpus corporativo.

Objetivos desta versão:
- reduzir assimetria entre docs_corporativos e docs_corporativos_chunks
- padronizar chunking e rebuild em rerun
- explicitar versões de extraction/chunking quando o schema suportar
- manter compatibilidade com chamadas legadas de leitura e rebuild
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import inspect, text

from core.db_loader import get_supabase_engine

DEFAULT_CHUNK_SIZE = 1000
DEFAULT_CHUNK_OVERLAP = 150
DEFAULT_MIN_TEXT_CHARS = 80
DEFAULT_CHUNKING_VERSION = "docs_chunk_v2"
DEFAULT_EXTRACTION_VERSION = "raw_text_v1"


@dataclass(frozen=True)
class StoreSchema:
    doc_columns: set[str]
    chunk_columns: set[str]
    text_column: str
    text_expr: str


def _dialect_name(conn_or_engine) -> str:
    try:
        return str(conn_or_engine.engine.dialect.name).lower()
    except Exception:
        try:
            return str(conn_or_engine.dialect.name).lower()
        except Exception:
            return ""


def _is_postgres(conn_or_engine) -> bool:
    return _dialect_name(conn_or_engine).startswith("postgres")


def _table_ref(conn_or_engine, table_name: str) -> str:
    return f"public.{table_name}" if _is_postgres(conn_or_engine) else table_name


def _date_text_expr(column_name: str) -> str:
    return f"coalesce(cast(date({column_name}) as text), '')"


def _order_nulls_last(column_name: str, *, descending: bool = True) -> str:
    direction = "desc" if descending else "asc"
    return f"case when {column_name} is null then 1 else 0 end asc, {column_name} {direction}"


def count_docs(ticker: str) -> int:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        r = conn.execute(
            text(f"select count(*) from {_table_ref(conn, 'docs_corporativos')} where ticker = :tk"),
            {"tk": _normalize_ticker(ticker)},
        )
        return int(r.scalar() or 0)


def count_chunks(ticker: str) -> int:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        r = conn.execute(
            text(f"select count(*) from {_table_ref(conn, 'docs_corporativos_chunks')} where ticker = :tk"),
            {"tk": _normalize_ticker(ticker)},
        )
        return int(r.scalar() or 0)


def _normalize_ticker(ticker: str) -> str:
    return (ticker or "").strip().upper()


def _canonical_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    try:
        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(raw)
        scheme = (parsed.scheme or "https").lower()
        netloc = (parsed.netloc or "").lower()
        path = re.sub(r"/{2,}", "/", parsed.path or "").rstrip("/")
        return urlunparse((scheme, netloc, path, "", parsed.query or "", ""))
    except Exception:
        return raw


def _normalize_text(texto: str) -> str:
    if not texto:
        return ""

    txt = str(texto).replace("\x00", " ").replace("\u00a0", " ")
    txt = txt.replace("\r\n", "\n").replace("\r", "\n").replace("\t", " ")
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)

    linhas = [ln.strip() for ln in txt.split("\n")]
    txt = "\n".join(linhas)
    txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
    return txt


def _clean_text(value: Any) -> str:
    return _normalize_text(str(value or "")).strip()


def _sha256(payload: str) -> str:
    return hashlib.sha256((payload or "").encode("utf-8")).hexdigest()


def build_doc_hash(
    *,
    ticker: str,
    titulo: str,
    url: str,
    fonte: str,
    tipo: str,
    data: Optional[Any],
) -> str:
    data_iso = ""
    if data is not None:
        ts = pd.to_datetime(data, errors="coerce")
        if pd.notna(ts):
            data_iso = ts.date().isoformat()
    return _sha256(
        "|".join(
            [
                _normalize_ticker(ticker),
                data_iso,
                (fonte or "").strip().upper(),
                (tipo or "").strip().lower(),
                _clean_text(titulo).lower(),
                _canonical_url(url),
            ]
        )
    )


def _content_hash(texto: str) -> str:
    normalized = _normalize_text(texto)
    return _sha256(normalized) if normalized else ""


def _doc_quality(texto: str, *, is_stub: bool) -> str:
    if is_stub:
        return "stub"
    if not texto:
        return "vazio"
    if len(texto) < DEFAULT_MIN_TEXT_CHARS:
        return "curto"
    return "ok"


def _split_oversized_paragraph(paragraph: str, chunk_size: int, overlap: int) -> List[str]:
    p = (paragraph or "").strip()
    if not p:
        return []

    sentences = re.split(r"(?<=[\.\!\?\;\:])\s+", p)
    if len(sentences) <= 1:
        out: List[str] = []
        start = 0
        while start < len(p):
            end = min(start + chunk_size, len(p))
            out.append(p[start:end].strip())
            if end >= len(p):
                break
            start = max(end - overlap, 0)
        return [x for x in out if x]

    chunks: List[str] = []
    current = ""
    for sent in sentences:
        sent = sent.strip()
        if not sent:
            continue

        candidate = f"{current} {sent}".strip() if current else sent
        if len(candidate) <= chunk_size:
            current = candidate
            continue

        if current:
            chunks.append(current.strip())
            tail = current[-overlap:].strip() if overlap > 0 and len(current) > overlap else current
            candidate = f"{tail} {sent}".strip()
            if len(candidate) <= chunk_size:
                current = candidate
                continue

        brute = _split_oversized_paragraph(sent, chunk_size=chunk_size, overlap=overlap)
        if brute:
            chunks.extend(brute[:-1])
            current = brute[-1]
        else:
            current = ""

    if current:
        chunks.append(current.strip())

    return [x for x in chunks if x]


def split_text(texto: str, chunk_size: int = DEFAULT_CHUNK_SIZE, overlap: int = DEFAULT_CHUNK_OVERLAP) -> List[str]:
    txt = _normalize_text(texto)
    if not txt:
        return []

    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", txt) if p.strip()]
    if not paragraphs:
        paragraphs = [txt]

    chunks: List[str] = []
    current = ""

    for p in paragraphs:
        if len(p) > chunk_size:
            if current:
                chunks.append(current.strip())
                current = ""
            chunks.extend(_split_oversized_paragraph(p, chunk_size=chunk_size, overlap=overlap))
            continue

        candidate = f"{current}\n\n{p}".strip() if current else p
        if len(candidate) <= chunk_size:
            current = candidate
            continue

        if current:
            chunks.append(current.strip())
            tail = current[-overlap:].strip() if overlap > 0 and len(current) > overlap else current
            candidate = f"{tail}\n\n{p}".strip()

        current = candidate if len(candidate) <= chunk_size else p

    if current:
        chunks.append(current.strip())

    return [_normalize_text(chunk) for chunk in chunks if _normalize_text(chunk)]


def _get_table_columns(conn, table_name: str) -> set[str]:
    inspector = inspect(conn)
    schema_name = "public" if _is_postgres(conn) else None
    columns = inspector.get_columns(table_name, schema=schema_name)
    return {str(col["name"]).lower() for col in columns}


def _detect_schema(conn) -> StoreSchema:
    doc_columns = _get_table_columns(conn, "docs_corporativos")
    chunk_columns = _get_table_columns(conn, "docs_corporativos_chunks")

    if "raw_text" in doc_columns:
        text_column = "raw_text"
    elif "texto" in doc_columns:
        text_column = "texto"
    else:
        raise RuntimeError("docs_corporativos não possui coluna raw_text nem texto.")

    return StoreSchema(
        doc_columns=doc_columns,
        chunk_columns=chunk_columns,
        text_column=text_column,
        text_expr=f"coalesce({text_column}, '')",
    )


def _to_db_datetime(value: Optional[Any]) -> Optional[Any]:
    if value is None:
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    if isinstance(ts, pd.Timestamp):
        return ts.to_pydatetime()
    return ts


def _query_optional_expr(column_name: str, columns: set[str], default_sql: str = "''") -> str:
    return f"coalesce({column_name}, '')" if column_name in columns else default_sql


def _query_optional_bool_expr(column_name: str, columns: set[str], default_sql: str = "false") -> str:
    return f"coalesce({column_name}, false)" if column_name in columns else default_sql


def _fetch_existing_document(conn, schema: StoreSchema, *, doc_hash: str, ticker: str, titulo: str, url: str, fonte: str, tipo: str, data: Optional[Any]) -> Optional[Dict[str, Any]]:
    data_iso = ""
    ts = pd.to_datetime(data, errors="coerce")
    if pd.notna(ts):
        data_iso = ts.date().isoformat()

    row = conn.execute(
        text(
            f"""
            select
                id,
                doc_hash,
                {schema.text_expr} as texto,
                {_query_optional_expr('content_hash', schema.doc_columns)} as content_hash,
                {_query_optional_expr('extraction_version', schema.doc_columns)} as extraction_version,
                {_query_optional_expr('ingestion_run_id', schema.doc_columns)} as ingestion_run_id,
                {_query_optional_bool_expr('is_stub', schema.doc_columns)} as is_stub
            from {_table_ref(conn, 'docs_corporativos')}
            where doc_hash = :doc_hash
               or (
                    upper(ticker) = upper(:ticker)
                and lower(coalesce(titulo, '')) = lower(:titulo)
                and lower(coalesce(url, '')) = lower(:url)
                and lower(coalesce(fonte, '')) = lower(:fonte)
                and lower(coalesce(tipo, '')) = lower(:tipo)
                and {_date_text_expr('data')} = coalesce(:data, '')
               )
            order by case when doc_hash = :doc_hash then 0 else 1 end, id desc
            limit 1
            """
        ),
        {
            "doc_hash": doc_hash,
            "ticker": _normalize_ticker(ticker),
            "titulo": (titulo or "")[:4000],
            "url": _canonical_url(url)[:4000],
            "fonte": fonte,
            "tipo": tipo,
            "data": data_iso or None,
        },
    ).mappings().fetchone()
    return dict(row) if row else None


def _build_doc_insert(schema: StoreSchema) -> str:
    cols = ["ticker", "titulo", "url", "fonte", "tipo", "data", schema.text_column, "doc_hash"]
    values = [":ticker", ":titulo", ":url", ":fonte", ":tipo", ":data", ":texto", ":doc_hash"]

    optional_map = [
        ("texto_chars", ":texto_chars"),
        ("texto_qualidade", ":texto_qualidade"),
        ("ingestion_run_id", ":ingestion_run_id"),
        ("extraction_version", ":extraction_version"),
        ("content_hash", ":content_hash"),
        ("is_stub", ":is_stub"),
    ]
    for col, placeholder in optional_map:
        if col in schema.doc_columns:
            cols.append(col)
            values.append(placeholder)

    return f"""
        insert into {{docs_table}} ({", ".join(cols)})
        values ({", ".join(values)})
        on conflict (doc_hash) do nothing
        returning id
    """


def _build_doc_update(schema: StoreSchema) -> str:
    assignments = [f"{schema.text_column} = :texto"]
    optional_map = [
        ("texto_chars", "texto_chars = :texto_chars"),
        ("texto_qualidade", "texto_qualidade = :texto_qualidade"),
        ("ingestion_run_id", "ingestion_run_id = :ingestion_run_id"),
        ("extraction_version", "extraction_version = :extraction_version"),
        ("content_hash", "content_hash = :content_hash"),
        ("is_stub", "is_stub = :is_stub"),
    ]
    for col, assignment in optional_map:
        if col in schema.doc_columns:
            assignments.append(assignment)

    return f"""
        update {{docs_table}}
        set {", ".join(assignments)}
        where id = :doc_id
    """


def _build_chunk_insert(schema: StoreSchema) -> str:
    cols = ["doc_id", "ticker", "chunk_index", "chunk_text", "chunk_hash"]
    values = [":doc_id", ":ticker", ":chunk_index", ":chunk_text", ":chunk_hash"]

    optional_map = [
        ("document_date", ":document_date"),
        ("data_doc", ":document_date"),
        ("categoria", ":categoria"),
        ("tipo_doc", ":categoria"),
        ("context_preview", ":context_preview"),
        ("titulo", ":titulo"),
        ("fonte", ":fonte"),
        ("url", ":url"),
        ("chunking_version", ":chunking_version"),
        ("extraction_version", ":extraction_version"),
        ("ingestion_run_id", ":ingestion_run_id"),
        ("content_hash", ":content_hash"),
        ("is_stub", ":is_stub"),
    ]
    for col, placeholder in optional_map:
        if col in schema.chunk_columns:
            cols.append(col)
            values.append(placeholder)

    return f"""
        insert into {{chunks_table}} ({", ".join(cols)})
        values ({", ".join(values)})
        on conflict (chunk_hash) do nothing
    """


def _fetch_chunk_state(conn, schema: StoreSchema, doc_id: int) -> Dict[str, Any]:
    row = conn.execute(
        text(
            f"""
            select
                cast(count(*) as integer) as chunk_count,
                max({_query_optional_expr('chunking_version', schema.chunk_columns)}) as chunking_version,
                max({_query_optional_expr('extraction_version', schema.chunk_columns)}) as extraction_version,
                max({_query_optional_expr('content_hash', schema.chunk_columns)}) as content_hash
            from {_table_ref(conn, 'docs_corporativos_chunks')}
            where doc_id = :doc_id
            """
        ),
        {"doc_id": int(doc_id)},
    ).mappings().fetchone()
    return dict(row) if row else {"chunk_count": 0, "chunking_version": "", "extraction_version": "", "content_hash": ""}


def _should_replace_text(existing_text: str, new_text: str, *, existing_is_stub: bool, new_is_stub: bool) -> bool:
    current = _normalize_text(existing_text)
    incoming = _normalize_text(new_text)
    if not incoming:
        return False
    if not current:
        return True
    if existing_is_stub and not new_is_stub:
        return True
    return len(incoming) > int(len(current) * 1.05)


def _chunks_need_rebuild(
    *,
    chunk_state: Dict[str, Any],
    raw_text: str,
    chunk_size: int,
    chunking_version: str,
    extraction_version: str,
    content_hash: str,
    force_rechunk: bool,
) -> bool:
    text_len = len(_normalize_text(raw_text))
    chunk_count = int(chunk_state.get("chunk_count") or 0)

    if force_rechunk:
        return True
    if text_len == 0:
        return False
    if chunk_count == 0:
        return True
    if chunk_count == 1 and text_len > int(chunk_size * 1.2):
        return True

    existing_chunking_version = str(chunk_state.get("chunking_version") or "").strip()
    existing_extraction_version = str(chunk_state.get("extraction_version") or "").strip()
    existing_content_hash = str(chunk_state.get("content_hash") or "").strip()

    if existing_chunking_version and existing_chunking_version != chunking_version:
        return True
    if existing_extraction_version and existing_extraction_version != extraction_version:
        return True
    if existing_content_hash and existing_content_hash != content_hash:
        return True
    return False


def _chunk_hash(doc_id: int, chunk_index: int, chunk_text: str, *, chunking_version: str, extraction_version: str, content_hash: str) -> str:
    payload = "|".join(
        [
            str(int(doc_id)),
            str(int(chunk_index)),
            chunking_version,
            extraction_version,
            content_hash,
            _normalize_text(chunk_text),
        ]
    )
    return _sha256(payload)


def _rebuild_chunks(
    conn,
    schema: StoreSchema,
    *,
    doc_id: int,
    ticker: str,
    titulo: str,
    url: str,
    fonte: str,
    tipo: str,
    data: Optional[Any],
    texto: str,
    is_stub: bool,
    chunk_size: int,
    chunk_overlap: int,
    chunking_version: str,
    extraction_version: str,
    run_id: Optional[str],
    force_rechunk: bool,
) -> Dict[str, Any]:
    normalized_text = _normalize_text(texto)
    content_hash = _content_hash(normalized_text)
    chunk_state = _fetch_chunk_state(conn, schema, int(doc_id))

    if not _chunks_need_rebuild(
        chunk_state=chunk_state,
        raw_text=normalized_text,
        chunk_size=int(chunk_size),
        chunking_version=chunking_version,
        extraction_version=extraction_version,
        content_hash=content_hash,
        force_rechunk=force_rechunk,
    ):
        return {"chunks_inserted": 0, "chunks_deleted": 0, "chunk_rebuilt": False}

    chunks = split_text(normalized_text, chunk_size=int(chunk_size), overlap=int(chunk_overlap))
    deleted = 0
    existing_count = int(chunk_state.get("chunk_count") or 0)
    if existing_count > 0:
        conn.execute(
            text(f"delete from {_table_ref(conn, 'docs_corporativos_chunks')} where doc_id = :doc_id"),
            {"doc_id": int(doc_id)},
        )
        deleted = existing_count

    if not chunks:
        return {"chunks_inserted": 0, "chunks_deleted": deleted, "chunk_rebuilt": existing_count > 0}

    insert_sql = text(
        _build_chunk_insert(schema).format(
            chunks_table=_table_ref(conn, "docs_corporativos_chunks")
        )
    )
    inserted = 0
    for idx, chunk in enumerate(chunks):
        payload = {
            "doc_id": int(doc_id),
            "ticker": _normalize_ticker(ticker),
            "chunk_index": int(idx),
            "chunk_text": chunk,
            "chunk_hash": _chunk_hash(
                int(doc_id),
                idx,
                chunk,
                chunking_version=chunking_version,
                extraction_version=extraction_version,
                content_hash=content_hash,
            ),
            "document_date": _to_db_datetime(data),
            "categoria": (tipo or "")[:200],
            "context_preview": chunk[:280],
            "titulo": (titulo or "")[:4000],
            "fonte": fonte,
            "url": _canonical_url(url)[:4000],
            "chunking_version": chunking_version,
            "extraction_version": extraction_version,
            "ingestion_run_id": run_id,
            "content_hash": content_hash,
            "is_stub": bool(is_stub),
        }
        conn.execute(insert_sql, payload)
        inserted += 1

    return {"chunks_inserted": inserted, "chunks_deleted": deleted, "chunk_rebuilt": True}


def persist_document_bundle(
    conn,
    *,
    ticker: str,
    titulo: str,
    url: str,
    fonte: str,
    tipo: str,
    data: Optional[Any],
    texto: str,
    doc_hash: Optional[str] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
    min_text_chars: int = DEFAULT_MIN_TEXT_CHARS,
    chunking_version: str = DEFAULT_CHUNKING_VERSION,
    extraction_version: str = DEFAULT_EXTRACTION_VERSION,
    run_id: Optional[str] = None,
    is_stub: bool = False,
    force_rechunk: bool = False,
) -> Dict[str, Any]:
    schema = _detect_schema(conn)
    ticker_norm = _normalize_ticker(ticker)
    titulo_norm = _clean_text(titulo)[:4000]
    url_norm = _canonical_url(url)[:4000]
    fonte_norm = (fonte or "").strip()[:200]
    tipo_norm = (tipo or "").strip()[:200]
    texto_norm = _normalize_text(texto)
    is_stub_flag = bool(is_stub or not texto_norm)
    doc_hash_value = doc_hash or build_doc_hash(
        ticker=ticker_norm,
        titulo=titulo_norm,
        url=url_norm,
        fonte=fonte_norm,
        tipo=tipo_norm,
        data=data,
    )
    content_hash = _content_hash(texto_norm)
    quality = _doc_quality(texto_norm, is_stub=is_stub_flag)

    payload = {
        "ticker": ticker_norm,
        "titulo": titulo_norm,
        "url": url_norm,
        "fonte": fonte_norm,
        "tipo": tipo_norm,
        "data": _to_db_datetime(data),
        "texto": texto_norm,
        "doc_hash": doc_hash_value,
        "texto_chars": len(texto_norm),
        "texto_qualidade": quality,
        "ingestion_run_id": run_id,
        "extraction_version": extraction_version,
        "content_hash": content_hash,
        "is_stub": is_stub_flag,
    }

    existing = _fetch_existing_document(
        conn,
        schema,
        doc_hash=doc_hash_value,
        ticker=ticker_norm,
        titulo=titulo_norm,
        url=url_norm,
        fonte=fonte_norm,
        tipo=tipo_norm,
        data=data,
    )

    inserted = False
    updated_text = False

    if existing is None:
        row = conn.execute(
            text(
                _build_doc_insert(schema).format(
                    docs_table=_table_ref(conn, "docs_corporativos")
                )
            ),
            payload,
        ).fetchone()
        if row is None:
            existing = _fetch_existing_document(
                conn,
                schema,
                doc_hash=doc_hash_value,
                ticker=ticker_norm,
                titulo=titulo_norm,
                url=url_norm,
                fonte=fonte_norm,
                tipo=tipo_norm,
                data=data,
            )
            if existing is None:
                raise RuntimeError(f"Falha ao persistir documento {ticker_norm} / {url_norm}")
        else:
            existing = {
                "id": int(row[0]),
                "texto": texto_norm,
                "content_hash": content_hash,
                "is_stub": is_stub_flag,
            }
            inserted = True

    if existing is None:
        raise RuntimeError(f"Documento {ticker_norm} não localizado após tentativa de persistência.")

    doc_id = int(existing["id"])
    existing_text = _normalize_text(str(existing.get("texto") or ""))
    existing_is_stub = bool(existing.get("is_stub"))

    if _should_replace_text(existing_text, texto_norm, existing_is_stub=existing_is_stub, new_is_stub=is_stub_flag):
        payload["doc_id"] = doc_id
        conn.execute(
            text(
                _build_doc_update(schema).format(
                    docs_table=_table_ref(conn, "docs_corporativos")
                )
            ),
            payload,
        )
        updated_text = True
        existing_text = texto_norm

    chunks_inserted = 0
    chunks_deleted = 0
    chunk_rebuilt = False
    if len(existing_text) >= int(min_text_chars):
        chunk_result = _rebuild_chunks(
            conn,
            schema,
            doc_id=doc_id,
            ticker=ticker_norm,
            titulo=titulo_norm,
            url=url_norm,
            fonte=fonte_norm,
            tipo=tipo_norm,
            data=data,
            texto=existing_text,
            is_stub=is_stub_flag,
            chunk_size=int(chunk_size),
            chunk_overlap=int(chunk_overlap),
            chunking_version=chunking_version,
            extraction_version=extraction_version,
            run_id=run_id,
            force_rechunk=force_rechunk or updated_text,
        )
        chunks_inserted = int(chunk_result["chunks_inserted"])
        chunks_deleted = int(chunk_result["chunks_deleted"])
        chunk_rebuilt = bool(chunk_result["chunk_rebuilt"])

    duplicate = bool(not inserted and not updated_text and not chunk_rebuilt)
    return {
        "ok": True,
        "doc_id": doc_id,
        "doc_hash": doc_hash_value,
        "inserted": inserted,
        "updated_text": updated_text,
        "duplicate": duplicate,
        "chunks_inserted": chunks_inserted,
        "chunks_deleted": chunks_deleted,
        "chunk_rebuilt": chunk_rebuilt,
        "stub": is_stub_flag,
    }


def process_missing_chunks_for_ticker(
    ticker: str,
    limit_docs: int = 60,
    max_chars: int = DEFAULT_CHUNK_SIZE,
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
    chunking_version: str = DEFAULT_CHUNKING_VERSION,
    extraction_version: str = DEFAULT_EXTRACTION_VERSION,
) -> int:
    tk = _normalize_ticker(ticker)
    if not tk:
        return 0

    engine = get_supabase_engine()
    inserted = 0

    with engine.begin() as conn:
        schema = _detect_schema(conn)
        docs = pd.read_sql_query(
            text(
                f"""
                select
                    id,
                    ticker,
                    titulo,
                    url,
                    fonte,
                    tipo,
                    data,
                    doc_hash,
                    {schema.text_expr} as texto
                from {_table_ref(conn, 'docs_corporativos')}
                where ticker = :tk
                order by {_order_nulls_last('data')}, id desc
                limit :lim
                """
            ),
            conn,
            params={"tk": tk, "lim": int(limit_docs)},
        )

        if docs.empty:
            return 0

        for _, row in docs.iterrows():
            texto = _normalize_text(str(row.get("texto") or ""))
            if not texto:
                continue

            rebuilt = _rebuild_chunks(
                conn,
                schema,
                doc_id=int(row["id"]),
                ticker=str(row.get("ticker") or tk),
                titulo=str(row.get("titulo") or ""),
                url=str(row.get("url") or ""),
                fonte=str(row.get("fonte") or ""),
                tipo=str(row.get("tipo") or ""),
                data=row.get("data"),
                texto=texto,
                is_stub=False,
                chunk_size=int(max_chars),
                chunk_overlap=int(chunk_overlap),
                chunking_version=chunking_version,
                extraction_version=extraction_version,
                run_id=None,
                force_rechunk=False,
            )
            inserted += int(rebuilt["chunks_inserted"])

    return inserted


def fetch_topk_chunks(ticker: str, k: int = 12) -> List[str]:
    tk = _normalize_ticker(ticker)
    if not tk:
        return []

    per_doc_cap = 2 if int(k) <= 12 else 3
    candidate_limit = max(int(k) * 6, 60)

    engine = get_supabase_engine()
    with engine.connect() as conn:
        df = pd.read_sql_query(
            text(
                f"""
                with ranked as (
                    select
                        c.doc_id,
                        c.chunk_index,
                        c.chunk_text,
                        row_number() over (
                            partition by c.doc_id
                            order by c.chunk_index asc
                        ) as rn_doc
                    from {_table_ref(conn, 'docs_corporativos_chunks')} c
                    where c.ticker = :tk
                    order by c.doc_id desc, c.chunk_index asc
                    limit :candidate_limit
                )
                select
                    doc_id,
                    chunk_index,
                    chunk_text
                from ranked
                where rn_doc <= :per_doc_cap
                order by doc_id desc, chunk_index asc
                limit :k
                """
            ),
            conn,
            params={
                "tk": tk,
                "k": int(k),
                "candidate_limit": int(candidate_limit),
                "per_doc_cap": int(per_doc_cap),
            },
        )

    return df["chunk_text"].tolist() if not df.empty else []


def fetch_topk_chunks_diversified(
    ticker: str,
    k: int = 20,
    per_doc_cap: int = 3,
    candidate_multiplier: int = 8,
) -> List[str]:
    tk = _normalize_ticker(ticker)
    if not tk:
        return []

    candidate_limit = max(int(k) * int(candidate_multiplier), 80)

    engine = get_supabase_engine()
    with engine.connect() as conn:
        df = pd.read_sql_query(
            text(
                f"""
                with ranked as (
                    select
                        c.doc_id,
                        c.chunk_index,
                        c.chunk_text,
                        row_number() over (
                            partition by c.doc_id
                            order by c.chunk_index asc
                        ) as rn_doc
                    from {_table_ref(conn, 'docs_corporativos_chunks')} c
                    where c.ticker = :tk
                    order by c.doc_id desc, c.chunk_index asc
                    limit :candidate_limit
                )
                select
                    doc_id,
                    chunk_index,
                    chunk_text
                from ranked
                where rn_doc <= :per_doc_cap
                order by doc_id desc, chunk_index asc
                limit :k
                """
            ),
            conn,
            params={
                "tk": tk,
                "k": int(k),
                "candidate_limit": int(candidate_limit),
                "per_doc_cap": int(per_doc_cap),
            },
        )

    return df["chunk_text"].tolist() if not df.empty else []
