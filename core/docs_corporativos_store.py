# -*- coding: utf-8 -*-
"""
core/docs_corporativos_store.py

Store de documentos e chunks do Patch 6 (CVM/IPE).

Objetivos:
- Contar docs/chunks por ticker
- Gerar chunks ausentes (chunking) de forma resiliente:
  * Não depende de UNIQUE em chunk_hash
  * Não depende de coluna created_at
  * Lê texto de coalesce(raw_text, texto) quando existir
- Buscar Top-K chunks para RAG
"""

from __future__ import annotations

import hashlib
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
# Chunking
# ============================================================

def _split_text(texto: str, max_chars: int = 1500) -> List[str]:
    """
    Divide texto em blocos aproximados, preservando quebras de linha.
    """
    if not texto:
        return []
    partes: List[str] = []
    atual: List[str] = []
    tam = 0
    for linha in texto.splitlines():
        linha = linha.rstrip()
        if not linha:
            # mantém parágrafos
            linha = ""
        add = len(linha) + 1
        if tam + add <= max_chars and atual:
            atual.append(linha)
            tam += add
        elif not atual and add <= max_chars:
            atual = [linha]
            tam = add
        else:
            # fecha bloco atual
            if atual:
                partes.append("\n".join(atual).strip())
            # inicia novo
            atual = [linha]
            tam = add
    if atual:
        partes.append("\n".join(atual).strip())
    # remove vazios
    return [p for p in partes if p.strip()]


def process_missing_chunks_for_ticker(
    ticker: str,
    limit_docs: int = 60,
    max_chars: int = 1500,
) -> int:
    """
    Gera chunks para os docs mais recentes do ticker.
    Retorna quantos chunks foram inseridos.
    """
    tk = (ticker or "").strip().upper()
    if not tk:
        return 0

    engine = get_supabase_engine()
    inserted = 0

    with engine.begin() as conn:
        # Busca docs recentes; tenta ler texto de raw_text ou texto (qual existir)
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
            doc_id = row["id"]
            texto = (row["texto"] or "").strip()
            if not texto:
                continue

            partes = _split_text(texto, max_chars=max_chars)

            for idx, chunk in enumerate(partes):
                h = hashlib.md5(chunk.encode("utf-8")).hexdigest()

                # Não depende de UNIQUE. Evita duplicar por SELECT existence.
                exists = conn.execute(
                    text("""
                        select 1
                        from public.docs_corporativos_chunks
                        where chunk_hash = :h
                        limit 1
                    """),
                    {"h": h},
                ).fetchone()

                if exists:
                    continue

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
# Top-K para RAG
# ============================================================

def fetch_topk_chunks(ticker: str, k: int = 6) -> List[str]:
    """
    Retorna lista de textos de chunks recentes.
    Não depende de created_at; usa doc_id desc + chunk_index.
    """
    tk = (ticker or "").strip().upper()
    if not tk:
        return []

    engine = get_supabase_engine()
    with engine.connect() as conn:
        df = pd.read_sql_query(
            text("""
                select c.chunk_text
                from public.docs_corporativos_chunks c
                where c.ticker = :tk
                order by c.doc_id desc, c.chunk_index asc
                limit :k
            """),
            conn,
            params={"tk": tk, "k": int(k)},
        )
    return df["chunk_text"].tolist() if not df.empty else []
