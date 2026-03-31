# core/db.py
# Pure data access layer — no Streamlit dependency.
#
# Engine: singleton via lru_cache (safe outside Streamlit).
# Credentials: resolved via core.secrets.get_secret.
# Callers: raise exceptions on failure (no st.error here).
from __future__ import annotations

import hashlib
from functools import lru_cache
from typing import Any, Dict, List, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from core.secrets import get_secret
from core.ticker_utils import normalize_ticker


# ────────────────────────────────────────────────────────────────────────────────
# Engine
# ────────────────────────────────────────────────────────────────────────────────

def _get_db_url() -> str:
    try:
        return get_secret("SUPABASE_DB_URL")
    except RuntimeError:
        return get_secret("DATABASE_URL")


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Engine singleton — safe to call outside a Streamlit session."""
    return create_engine(_get_db_url(), pool_pre_ping=True)


# ────────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ────────────────────────────────────────────────────────────────────────────────

def _normalize_ticker(ticker: str) -> Tuple[str, str]:
    """
    Returns (tk1, tk2) for queries that must match both stored forms.
      tk1 = upper as-received (covers PETR4.SA if stored that way)
      tk2 = canonical without .SA (covers PETR4)
    """
    tk1 = (ticker or "").strip().upper()
    tk2 = normalize_ticker(ticker)
    return tk1, tk2


def _read_sql_df(sql: str, params: Dict[str, Any] | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


def _coerce_sort_by_data(df: pd.DataFrame | None, ascending: bool = True) -> pd.DataFrame | None:
    if df is None or df.empty:
        return df
    df = df.copy()
    cols_lower = {str(c).strip().lower(): c for c in df.columns}
    data_col = cols_lower.get("data")
    if data_col is not None:
        df[data_col] = pd.to_datetime(df[data_col], errors="coerce")
        df = df.dropna(subset=[data_col])
        df = df.sort_values(data_col, ascending=ascending)
    return df


# ────────────────────────────────────────────────────────────────────────────────
# Loaders — raise on error, no UI side-effects
# ────────────────────────────────────────────────────────────────────────────────

def load_setores_from_db() -> pd.DataFrame | None:
    df = _read_sql_df(
        """
        SELECT
            ticker,
            nome_empresa,
            "SETOR",
            "SUBSETOR",
            "SEGMENTO",
            "LISTAGEM"
        FROM public.setores
        WHERE ticker IS NOT NULL
        """
    )
    df["ticker"] = (
        df["ticker"]
        .astype(str)
        .str.replace(".SA", "", regex=False)
        .str.strip()
        .str.upper()
    )
    for c in ["SETOR", "SUBSETOR", "SEGMENTO", "LISTAGEM", "nome_empresa"]:
        if c not in df.columns:
            df[c] = ""
    return df


def load_data_from_db(ticker: str) -> pd.DataFrame | None:
    tk1, tk2 = _normalize_ticker(ticker)
    df = _read_sql_df(
        """
        SELECT *
        FROM public."Demonstracoes_Financeiras"
        WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
        ORDER BY data ASC
        """,
        {"tk1": tk1, "tk2": tk2},
    )
    return _coerce_sort_by_data(df, ascending=True)


def load_data_tri_from_db(ticker: str) -> pd.DataFrame | None:
    tk1, tk2 = _normalize_ticker(ticker)
    df = _read_sql_df(
        """
        SELECT *
        FROM public."Demonstracoes_Financeiras_TRI"
        WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
        ORDER BY data ASC
        """,
        {"tk1": tk1, "tk2": tk2},
    )
    return _coerce_sort_by_data(df, ascending=True)


def load_multiplos_from_db(ticker: str) -> pd.DataFrame | None:
    tk1, tk2 = _normalize_ticker(ticker)
    df = _read_sql_df(
        """
        SELECT *
        FROM public.multiplos
        WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
        ORDER BY data ASC
        """,
        {"tk1": tk1, "tk2": tk2},
    )
    return _coerce_sort_by_data(df, ascending=True)


def load_multiplos_limitado_from_db(ticker: str, limite: int = 250) -> pd.DataFrame | None:
    tk1, tk2 = _normalize_ticker(ticker)
    df = _read_sql_df(
        """
        SELECT *
        FROM public.multiplos
        WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
        ORDER BY data DESC
        LIMIT :limite
        """,
        {"tk1": tk1, "tk2": tk2, "limite": int(limite)},
    )
    return _coerce_sort_by_data(df, ascending=True)


def load_multiplos_tri_from_db(ticker: str) -> pd.DataFrame | None:
    tk1, tk2 = _normalize_ticker(ticker)
    df = _read_sql_df(
        """
        SELECT *
        FROM public.multiplos_TRI
        WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
        ORDER BY data DESC
        LIMIT 1
        """,
        {"tk1": tk1, "tk2": tk2},
    )
    return _coerce_sort_by_data(df, ascending=True)


def load_multiplos_tri_hist_from_db(ticker: str, limite: int = 250) -> pd.DataFrame | None:
    tk1, tk2 = _normalize_ticker(ticker)
    df = _read_sql_df(
        """
        SELECT *
        FROM public.multiplos_TRI
        WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
        ORDER BY data DESC
        LIMIT :limite
        """,
        {"tk1": tk1, "tk2": tk2, "limite": int(limite)},
    )
    return _coerce_sort_by_data(df, ascending=True)


def load_macro_summary() -> pd.DataFrame | None:
    df = _read_sql_df(
        """
        SELECT *
        FROM public.info_economica
        ORDER BY data ASC
        """
    )
    return _coerce_sort_by_data(df, ascending=True)


def load_macro_mensal() -> pd.DataFrame | None:
    df = _read_sql_df(
        """
        SELECT *
        FROM public.info_economica_mensal
        ORDER BY data ASC
        """
    )
    return _coerce_sort_by_data(df, ascending=True)


# ────────────────────────────────────────────────────────────────────────────────
# Patch 6 — Documentos corporativos
# ────────────────────────────────────────────────────────────────────────────────

def make_doc_hash(ticker: str, data: str | None, url: str | None, raw_text: str) -> str:
    """Hash estável para deduplicação em docs_corporativos."""
    base = (
        f"{normalize_ticker(ticker)}|"
        f"{data or ''}|"
        f"{url or ''}|"
        f"{(raw_text or '')[:20000]}"
    )
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def load_docs_corporativos_by_ticker(
    tickers: List[str],
    limit_per_ticker: int = 8,
    days_back: int = 365,
) -> Dict[str, List[Dict[str, Any]]]:
    tks = [normalize_ticker(t) for t in (tickers or []) if str(t or "").strip()]
    tks = list(dict.fromkeys([t for t in tks if t]))
    if not tks:
        return {}

    df = _read_sql_df(
        """
        SELECT ticker, data, fonte, tipo, titulo, url, raw_text
        FROM public.docs_corporativos
        WHERE ticker = ANY(:tks)
          AND (data IS NULL OR data >= (CURRENT_DATE - (:days_back::int)))
        ORDER BY ticker, data DESC NULLS LAST, id DESC
        """,
        {"tks": tks, "days_back": int(days_back)},
    )

    out: Dict[str, List[Dict[str, Any]]] = {tk: [] for tk in tks}
    if df is None or df.empty:
        return out

    df["ticker"] = df["ticker"].astype(str).str.upper().str.replace(".SA", "", regex=False).str.strip()
    for tk, grp in df.groupby("ticker"):
        rows = grp.head(int(limit_per_ticker)).to_dict("records")
        out[str(tk)] = [
            {
                "source": f"{(r.get('fonte') or 'NA')}:{(r.get('tipo') or 'NA')}",
                "date": str(r.get("data") or "NA"),
                "text": str(r.get("raw_text") or "")[:12000],
            }
            for r in rows
            if str(r.get("raw_text") or "").strip()
        ]
    return out


def load_docs_corporativos_from_db(
    ticker: str,
    *,
    limit: int = 20,
    tipos: list[str] | None = None,
    fontes: list[str] | None = None,
) -> pd.DataFrame | None:
    tk1, tk2 = _normalize_ticker(ticker)
    limit = int(limit)

    wh_tipo = " AND tipo = ANY(:tipos) " if tipos else ""
    wh_fonte = " AND fonte = ANY(:fontes) " if fontes else ""

    df = _read_sql_df(
        f"""
        SELECT
            id, ticker, data, fonte, tipo, titulo, url, raw_text, lang, doc_hash, created_at
        FROM public.docs_corporativos
        WHERE (ticker = :tk2 OR ticker = :tk1)
          {wh_tipo}
          {wh_fonte}
        ORDER BY COALESCE(data, DATE(created_at)) DESC, id DESC
        LIMIT :limit
        """,
        {"tk1": tk1, "tk2": tk2, "limit": limit, "tipos": tipos, "fontes": fontes},
    )
    if df is None or df.empty:
        return df
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.date
    return df


def load_docs_corporativos_chunks_from_db(
    ticker: str,
    *,
    limit_docs: int = 12,
    limit_chunks_per_doc: int = 6,
) -> pd.DataFrame | None:
    tk1, tk2 = _normalize_ticker(ticker)
    df = _read_sql_df(
        """
        WITH docs AS (
            SELECT id, ticker, COALESCE(data, DATE(created_at)) as dt
            FROM public.docs_corporativos
            WHERE (ticker = :tk2 OR ticker = :tk1)
            ORDER BY dt DESC, id DESC
            LIMIT :limit_docs
        ),
        ranked AS (
            SELECT
                c.*,
                d.dt,
                ROW_NUMBER() OVER (PARTITION BY c.doc_id ORDER BY c.chunk_index ASC) as rn
            FROM public.docs_corporativos_chunks c
            JOIN docs d ON d.id = c.doc_id
        )
        SELECT
            id, doc_id, ticker, chunk_index, chunk_text, chunk_hash, created_at
        FROM ranked
        WHERE rn <= :limit_chunks_per_doc
        ORDER BY dt DESC, doc_id DESC, chunk_index ASC
        """,
        {
            "tk1": tk1,
            "tk2": tk2,
            "limit_docs": int(limit_docs),
            "limit_chunks_per_doc": int(limit_chunks_per_doc),
        },
    )
    return df
