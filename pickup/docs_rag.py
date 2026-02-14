from __future__ import annotations

"""
pickup/docs_rag.py
------------------
Leitura de documentos corporativos (CVM IPE / RI / etc.) do Supabase para uso no Patch 6.

Tabelas esperadas (já criadas por você no Supabase):
- public.docs_corporativos
- public.docs_corporativos_chunks  (opcional para Patch 7)

Este módulo NÃO baixa nada da internet. Ele apenas consulta o banco.
"""

from typing import Any, Dict, List, Optional, Sequence
import hashlib

import pandas as pd
import streamlit as st
from sqlalchemy import text

from core.db_loader import get_supabase_engine


def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()


def _as_date_str(x: Any) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "NA"
        d = pd.to_datetime(x, errors="coerce")
        if pd.isna(d):
            return str(x)
        return d.date().isoformat()
    except Exception:
        return str(x)


def _row_to_doc_dict(r: Dict[str, Any]) -> Dict[str, Any]:
    # padrão esperado pelo Patch6: {source,date,text}
    fonte = str(r.get("fonte", "supabase")).strip() or "supabase"
    tipo = str(r.get("tipo", "")).strip()
    titulo = str(r.get("titulo", "")).strip()
    url = str(r.get("url", "")).strip()
    header = " | ".join([x for x in [fonte, tipo, titulo, url] if x])
    raw = str(r.get("raw_text", "") or "").strip()

    # Mantém texto “cru” (Patch6 faz o recorte no client)
    if header:
        text_out = f"{header}\n\n{raw}"
    else:
        text_out = raw

    return {
        "source": fonte,
        "date": _as_date_str(r.get("data")),
        "text": text_out,
        "meta": {
            "tipo": tipo,
            "titulo": titulo,
            "url": url,
            "doc_id": r.get("id"),
        },
    }


@st.cache_data(show_spinner=False, ttl=10 * 60)
def count_docs_by_tickers(tickers: Sequence[str]) -> Dict[str, int]:
    """
    Retorna contagem de docs por ticker em public.docs_corporativos.
    """
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {}

    engine = get_supabase_engine()
    sql = text(
        """
        SELECT ticker, COUNT(*) AS n
        FROM public.docs_corporativos
        WHERE ticker = ANY(:tks)
        GROUP BY ticker
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql_query(sql, conn, params={"tks": tks})

    out = {tk: 0 for tk in tks}
    if df is not None and not df.empty:
        for _, r in df.iterrows():
            out[_norm_ticker(r["ticker"])] = int(r["n"])
    return out


@st.cache_data(show_spinner=False, ttl=10 * 60)
def get_docs_by_ticker(
    ticker: str,
    *,
    limit_docs: int = 12,
    prefer_tipos: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Retorna lista de docs (ordenados por data desc) para um ticker.
    """
    tk = _norm_ticker(ticker)
    if not tk:
        return []

    engine = get_supabase_engine()

    if prefer_tipos:
        # ordena com prioridade (CASE) para tipos preferidos
        tipos = [str(x).strip() for x in prefer_tipos if str(x).strip()]
        sql = text(
            """
            SELECT id, ticker, data, fonte, tipo, titulo, url, raw_text
            FROM public.docs_corporativos
            WHERE ticker = :tk
            ORDER BY
              (CASE
                WHEN tipo = ANY(:tipos) THEN 0
                ELSE 1
              END),
              data DESC NULLS LAST,
              id DESC
            LIMIT :lim
            """
        )
        params = {"tk": tk, "lim": int(limit_docs), "tipos": tipos}
    else:
        sql = text(
            """
            SELECT id, ticker, data, fonte, tipo, titulo, url, raw_text
            FROM public.docs_corporativos
            WHERE ticker = :tk
            ORDER BY data DESC NULLS LAST, id DESC
            LIMIT :lim
            """
        )
        params = {"tk": tk, "lim": int(limit_docs)}

    with engine.connect() as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    if df is None or df.empty:
        return []

    docs: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        docs.append(_row_to_doc_dict(row.to_dict()))
    return docs


@st.cache_data(show_spinner=False, ttl=10 * 60)
def get_docs_by_tickers(
    tickers: Sequence[str],
    *,
    limit_docs_per_ticker: int = 10,
    prefer_tipos: Optional[List[str]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Retorna dict {ticker -> [docs]}.

    Observação: como limit por ticker em SQL puro é mais chato (window functions),
    este método faz N queries (uma por ticker) mas com cache.
    Para universo pequeno (carteira final) é ok.
    """
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    out: Dict[str, List[Dict[str, Any]]] = {}
    for tk in tks:
        out[tk] = get_docs_by_ticker(tk, limit_docs=int(limit_docs_per_ticker), prefer_tipos=prefer_tipos)
    return out


def make_doc_hash(ticker: str, fonte: str, tipo: str, titulo: str, url: str, raw_text: str) -> str:
    """
    Mesmo método de hash usado no ingest para evitar duplicatas.
    """
    base = "|".join([
        _norm_ticker(ticker),
        (fonte or "").strip(),
        (tipo or "").strip(),
        (titulo or "").strip(),
        (url or "").strip(),
        (raw_text or "").strip(),
    ])
    return hashlib.sha256(base.encode("utf-8")).hexdigest()
