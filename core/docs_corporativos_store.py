# -*- coding: utf-8 -*-
"""
core/docs_corporativos_store.py

Store de documentos e chunks do Patch 6 (CVM/IPE).

Melhorias desta versão:
- Chunking mais robusto para textos extraídos de PDF
- Normalização de quebras de linha e espaços
- Chunk overlap para preservar contexto entre blocos
- Rebuild automático de documentos com chunking anômalo
  (ex.: 1 chunk para documento longo)
- Fallback de retrieval com diversificação automática por documento
"""

from __future__ import annotations

import hashlib
import math
import re
from typing import List

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
            {"tk": (ticker or "").strip().upper()},
        )
        return int(r.scalar() or 0)


def count_chunks(ticker: str) -> int:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        r = conn.execute(
            text("select count(*) from public.docs_corporativos_chunks where ticker = :tk"),
            {"tk": (ticker or "").strip().upper()},
        )
        return int(r.scalar() or 0)


# ============================================================
# Normalização / Chunking
# ============================================================

def _normalize_text(texto: str) -> str:
    """
    Limpa artefatos comuns de extração de PDF sem destruir a estrutura.
    """
    if not texto:
        return ""

    txt = str(texto).replace("\r\n", "\n").replace("\r", "\n")
    txt = txt.replace("\u00a0", " ").replace("\t", " ")
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)

    linhas = [ln.strip() for ln in txt.split("\n")]
    txt = "\n".join(linhas)
    txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
    return txt


def _split_oversized_paragraph(paragraph: str, chunk_size: int, overlap: int) -> List[str]:
    """
    Divide um parágrafo muito grande tentando preservar frases.
    """
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


def _split_text(texto: str, chunk_size: int = 1000, overlap: int = 150) -> List[str]:
    """
    Divide texto em chunks mais adequados para RAG.

    Estratégia:
    - normaliza texto
    - tenta agrupar por parágrafos
    - usa overlap para preservar continuidade
    - faz fallback para cortes robustos quando houver parágrafos enormes
    """
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
            big_parts = _split_oversized_paragraph(p, chunk_size=chunk_size, overlap=overlap)
            chunks.extend(big_parts)
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

    cleaned = []
    for c in chunks:
        c = re.sub(r"\n{3,}", "\n\n", c).strip()
        if c:
            cleaned.append(c)

    return cleaned


def _doc_needs_rebuild(existing_chunks: int, raw_text: str, max_chars: int) -> bool:
    """
    Detecta chunking anômalo.
    Regra principal:
    - se o documento for longo e houver apenas 1 chunk, precisa reconstruir
    - se não houver chunks, também precisa
    """
    txt = _normalize_text(raw_text)
    if not txt:
        return False

    text_len = len(txt)

    if existing_chunks == 0:
        return True

    if existing_chunks == 1 and text_len > int(max_chars * 1.20):
        return True

    return False


# ============================================================
# Chunking
# ============================================================

def process_missing_chunks_for_ticker(
    ticker: str,
    limit_docs: int = 60,
    max_chars: int = 1000,
    chunk_overlap: int = 150,
) -> int:
    """
    Gera chunks para os docs mais recentes do ticker.
    Retorna quantos chunks foram inseridos.

    Comportamento desta versão:
    - cria chunks ausentes
    - reconstrói automaticamente documentos com chunking anômalo
    """
    tk = (ticker or "").strip().upper()
    if not tk:
        return 0

    engine = get_supabase_engine()
    inserted = 0

    with engine.begin() as conn:
        docs = pd.read_sql_query(
            text("""
                select
                    id,
                    coalesce(raw_text, texto, '') as texto
                from public.docs_corporativos
                where ticker = :tk
                order by data desc nulls last, id desc
                limit :lim
            """),
            conn,
            params={"tk": tk, "lim": int(limit_docs)},
        )

        if docs.empty:
            return 0

        for _, row in docs.iterrows():
            doc_id = int(row["id"])
            texto = (row["texto"] or "").strip()
            if not texto:
                continue

            existing_chunks = conn.execute(
                text("""
                    select count(*)
                    from public.docs_corporativos_chunks
                    where doc_id = :doc_id
                """),
                {"doc_id": doc_id},
            ).scalar() or 0
            existing_chunks = int(existing_chunks)

            if not _doc_needs_rebuild(existing_chunks=existing_chunks, raw_text=texto, max_chars=max_chars):
                continue

            if existing_chunks > 0:
                conn.execute(
                    text("""
                        delete from public.docs_corporativos_chunks
                        where doc_id = :doc_id
                    """),
                    {"doc_id": doc_id},
                )

            partes = _split_text(texto, chunk_size=max_chars, overlap=chunk_overlap)

            for idx, chunk in enumerate(partes):
                h = hashlib.md5(f"{doc_id}:{idx}:{chunk}".encode("utf-8")).hexdigest()

                conn.execute(
                    text("""
                        insert into public.docs_corporativos_chunks
                            (doc_id, ticker, chunk_index, chunk_text, chunk_hash)
                        values
                            (:doc_id, :tk, :idx, :txt, :h)
                    """),
                    {
                        "doc_id": doc_id,
                        "tk": tk,
                        "idx": int(idx),
                        "txt": chunk,
                        "h": h,
                    },
                )
                inserted += 1

    return inserted


# ============================================================
# Top-K para RAG (fallback simples com diversificação)
# ============================================================

def fetch_topk_chunks(ticker: str, k: int = 12) -> List[str]:
    """
    Retorna lista de chunks recentes com diversificação automática por documento.

    O objetivo é evitar que o fallback pegue vários chunks quase iguais
    do mesmo PDF recente.

    Estratégia:
    - busca candidatos dos docs mais recentes
    - limita a quantidade por documento
    - ordena priorizando documentos recentes e primeiros chunks relevantes
    """
    tk = (ticker or "").strip().upper()
    if not tk:
        return []

    # para k pequeno, no máximo 2 por documento; para k maior, até 3
    per_doc_cap = 2 if int(k) <= 12 else 3
    candidate_limit = max(int(k) * 6, 60)

    engine = get_supabase_engine()
    with engine.connect() as conn:
        df = pd.read_sql_query(
            text("""
                with ranked as (
                    select
                        c.doc_id,
                        c.chunk_index,
                        c.chunk_text,
                        row_number() over (
                            partition by c.doc_id
                            order by c.chunk_index asc
                        ) as rn_doc
                    from public.docs_corporativos_chunks c
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
            """),
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
    """
    Versão explícita de retrieval diversificado para uso futuro.

    Útil quando se quiser aprofundar o fallback sem depender dos módulos
    de retrieval semântico mais avançados.
    """
    tk = (ticker or "").strip().upper()
    if not tk:
        return []

    candidate_limit = max(int(k) * int(candidate_multiplier), 80)

    engine = get_supabase_engine()
    with engine.connect() as conn:
        df = pd.read_sql_query(
            text("""
                with ranked as (
                    select
                        c.doc_id,
                        c.chunk_index,
                        c.chunk_text,
                        row_number() over (
                            partition by c.doc_id
                            order by c.chunk_index asc
                        ) as rn_doc
                    from public.docs_corporativos_chunks c
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
            """),
            conn,
            params={
                "tk": tk,
                "k": int(k),
                "candidate_limit": int(candidate_limit),
                "per_doc_cap": int(per_doc_cap),
            },
        )

    return df["chunk_text"].tolist() if not df.empty else []
