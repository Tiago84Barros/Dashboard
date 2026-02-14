# pickup/docs_rag.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
import re
import hashlib

import pandas as pd
from sqlalchemy import text
import streamlit as st


def _norm_ticker(t: str) -> str:
    return (t or "").strip().upper().replace(".SA", "")


def _shorten(s: str, limit: int) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if len(s) <= limit:
        return s
    return s[: limit - 1] + "…"


def _safe_int(x: Any, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _get_engine():
    # usa seu loader já existente
    try:
        from core.db_loader import get_supabase_engine  # type: ignore
    except Exception:
        from db_loader import get_supabase_engine  # type: ignore
    return get_supabase_engine()


@st.cache_data(show_spinner=False, ttl=60 * 30)
def get_docs_by_ticker(
    *,
    tickers: List[str],
    limit_docs: int = 8,
    limit_chars_per_doc: int = 4000,
    only_tipos: Optional[List[str]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Busca documentos em public.docs_corporativos para cada ticker.
    Retorna no formato esperado pelo Patch6:
      { "ROMI3": [ {"source": "...", "date": "YYYY-MM-DD", "text": "..."}, ... ], ... }
    """
    tickers_n = [_norm_ticker(t) for t in (tickers or []) if _norm_ticker(t)]
    tickers_n = list(dict.fromkeys(tickers_n))

    out: Dict[str, List[Dict[str, Any]]] = {tk: [] for tk in tickers_n}
    if not tickers_n:
        return out

    limit_docs = max(1, _safe_int(limit_docs, 8))
    limit_chars_per_doc = max(500, _safe_int(limit_chars_per_doc, 4000))

    engine = _get_engine()

    # filtro opcional por tipo (ex.: ["fato_relevante","comunicado","ipe"])
    tipos_sql = ""
    params: Dict[str, Any] = {
        "tickers": tickers_n,
    }

    if only_tipos:
        tipos_norm = [str(x).strip().lower() for x in only_tipos if str(x).strip()]
        if tipos_norm:
            tipos_sql = " AND lower(tipo) = ANY(:tipos) "
            params["tipos"] = tipos_norm

    sql = f"""
        SELECT
            ticker,
            data,
            fonte,
            tipo,
            titulo,
            url,
            raw_text
        FROM public.docs_corporativos
        WHERE ticker = ANY(:tickers)
        {tipos_sql}
        ORDER BY ticker ASC, data DESC NULLS LAST, id DESC
    """

    # Puxa tudo e depois limita por ticker (mais simples/robusto para começar)
    with engine.connect() as conn:
        df = pd.read_sql_query(text(sql), conn, params=params)

    if df is None or df.empty:
        return out

    # normaliza
    df["ticker"] = df["ticker"].astype(str).map(_norm_ticker)
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.date

    for tk in tickers_n:
        dft = df[df["ticker"] == tk].head(limit_docs)
        rows: List[Dict[str, Any]] = []

        for _, r in dft.iterrows():
            fonte = str(r.get("fonte", "supabase")).strip() or "supabase"
            tipo = str(r.get("tipo", "")).strip()
            titulo = str(r.get("titulo", "")).strip()
            url = str(r.get("url", "")).strip()
            raw = str(r.get("raw_text", "")).strip()

            # monta “source” bem legível
            src_parts = [fonte]
            if tipo:
                src_parts.append(tipo)
            if titulo:
                src_parts.append(titulo)
            if url:
                src_parts.append(url)

            source = " | ".join(src_parts)

            dt = r.get("data", None)
            date_str = str(dt) if dt else "NA"

            rows.append(
                {
                    "source": source,
                    "date": date_str,
                    "text": _shorten(raw, limit_chars_per_doc),
                }
            )

        out[tk] = rows

    return out
