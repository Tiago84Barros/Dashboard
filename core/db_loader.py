# core/db_loader.py
from __future__ import annotations

import os
import hashlib
from typing import Any, Dict, List, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ────────────────────────────────────────────────────────────────────────────────
# Supabase / PostgreSQL
# ────────────────────────────────────────────────────────────────────────────────
def _get_supabase_url() -> str:
    # padrão do seu projeto (Streamlit Secrets -> env)
    db_url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Defina SUPABASE_DB_URL (ou DATABASE_URL) nas secrets/env vars.")
    return db_url


@st.cache_resource(show_spinner=False)
def get_supabase_engine() -> Engine:
    """
    Engine singleton em cache (recomendado para Streamlit).
    """
    return create_engine(_get_supabase_url(), pool_pre_ping=True)


def _normalize_ticker(ticker: str) -> Tuple[str, str]:
    """
    Normaliza ticker para cobrir as duas formas armazenadas:
      tk1 = como vier (ex.: PETR4 ou PETR4.SA), em upper
      tk2 = sem sufixo .SA (ex.: PETR4)
    Retorna (tk1, tk2) para uso em WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
    """
    tk1 = (ticker or "").strip().upper()
    tk2 = tk1.replace(".SA", "")
    return tk1, tk2


def _read_sql_df(sql: str, params: Dict[str, Any] | None = None) -> pd.DataFrame:
    """
    Executa SQL no Supabase e retorna DataFrame.
    """
    engine = get_supabase_engine()
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


def _coerce_sort_by_data(df: pd.DataFrame | None, ascending: bool = True) -> pd.DataFrame | None:
    """
    Normaliza a coluna de data para datetime (aceitando 'Data' ou 'data') e ordena.
    Mantém comportamento defensivo para DataFrames vazios.
    """
    if df is None or df.empty:
        return df
    df = df.copy()
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
        df = df.dropna(subset=["Data"])
        df = df.sort_values("Data", ascending=ascending)
    elif "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df = df.dropna(subset=["data"])
        df = df.sort_values("data", ascending=ascending)
    return df


# ════════════════════════════════════════════════════════════════════════════════
# Loaders (Supabase) – em cache para evitar I/O repetido
# ════════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner=False)
def load_setores_from_db() -> pd.DataFrame | None:
    try:
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

        # normaliza ticker sem .SA
        df["ticker"] = (
            df["ticker"]
            .astype(str)
            .str.replace(".SA", "", regex=False)
            .str.strip()
            .str.upper()
        )

        # garante colunas esperadas mesmo se houver nulos
        for c in ["SETOR", "SUBSETOR", "SEGMENTO", "LISTAGEM", "nome_empresa"]:
            if c not in df.columns:
                df[c] = ""

        return df

    except Exception as e:
        st.error(f"Erro ao carregar tabela 'setores' do Supabase: {e}")
        return None


# Alias legado (mantenha se outros módulos importarem esse nome)
@st.cache_data(show_spinner=False)
def load_setores_from_supabase() -> pd.DataFrame | None:
    return load_setores_from_db()


@st.cache_data(show_spinner=False)
def load_data_from_db(ticker: str) -> pd.DataFrame | None:
    """
    Carrega a tabela Demonstracoes_Financeiras (DFP anual) do Supabase para o ticker informado.
    Mantém assinatura legado.
    """
    tk1, tk2 = _normalize_ticker(ticker)
    try:
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
    except Exception as e:
        st.error(f"Erro ao carregar DRE (DFP) para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_data_tri_from_db(ticker: str) -> pd.DataFrame | None:
    """
    Carrega a tabela Demonstracoes_Financeiras_TRI (TRI/ITR) do Supabase para o ticker informado.
    """
    tk1, tk2 = _normalize_ticker(ticker)
    try:
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
    except Exception as e:
        st.error(f"Erro ao carregar TRI para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_multiplos_from_db(ticker: str) -> pd.DataFrame | None:
    """
    Carrega a tabela multiplos (anuais) do Supabase para o ticker.
    """
    tk1, tk2 = _normalize_ticker(ticker)
    try:
        df = _read_sql_df(
            """
            SELECT *
            FROM public.multiplos
            WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
            ORDER BY "Data" ASC
            """,
            {"tk1": tk1, "tk2": tk2},
        )
        return _coerce_sort_by_data(df, ascending=True)
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos (anuais) para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_multiplos_limitado_from_db(ticker: str, limite: int = 250) -> pd.DataFrame | None:
    """
    Carrega os últimos `limite` registros da tabela multiplos (anuais) para gráficos leves.
    Retorna em ordem crescente por Data.
    """
    tk1, tk2 = _normalize_ticker(ticker)
    try:
        limite = int(limite)
        df = _read_sql_df(
            """
            SELECT *
            FROM public.multiplos
            WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
            ORDER BY "Data" DESC
            LIMIT :limite
            """,
            {"tk1": tk1, "tk2": tk2, "limite": limite},
        )
        return _coerce_sort_by_data(df, ascending=True)
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos limitados para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_multiplos_tri_from_db(ticker: str) -> pd.DataFrame | None:
    """
    Carrega o registro mais recente de multiplos_TRI (trimestrais, LTM) no Supabase.
    """
    tk1, tk2 = _normalize_ticker(ticker)
    try:
        df = _read_sql_df(
            """
            SELECT *
            FROM public.multiplos_TRI
            WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
            ORDER BY "Data" DESC
            LIMIT 1
            """,
            {"tk1": tk1, "tk2": tk2},
        )
        return _coerce_sort_by_data(df, ascending=True)
    except Exception as e:
        st.error(f"Erro ao carregar múltiplos TRI para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_multiplos_tri_hist_from_db(ticker: str, limite: int = 250) -> pd.DataFrame | None:
    """
    Carrega histórico (últimos N) de multiplos_TRI para gráficos/inspeção.
    Retorna em ordem crescente por Data.
    """
    tk1, tk2 = _normalize_ticker(ticker)
    try:
        limite = int(limite)
        df = _read_sql_df(
            """
            SELECT *
            FROM public.multiplos_TRI
            WHERE "Ticker" = :tk1 OR "Ticker" = :tk2
            ORDER BY "Data" DESC
            LIMIT :limite
            """,
            {"tk1": tk1, "tk2": tk2, "limite": limite},
        )
        return _coerce_sort_by_data(df, ascending=True)
    except Exception as e:
        st.error(f"Erro ao carregar histórico múltiplos TRI para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False)
def load_macro_summary() -> pd.DataFrame | None:
    """
    Carrega info_economica (macro anual) do Supabase.
    Mantém o nome já usado no projeto.
    """
    try:
        df = _read_sql_df(
            """
            SELECT *
            FROM public.info_economica
            ORDER BY "Data" ASC
            """
        )
        return _coerce_sort_by_data(df, ascending=True)
    except Exception as e:
        st.error(f"Erro ao carregar macro (info_economica): {e}")
        return None


@st.cache_data(show_spinner=False)
def load_macro_mensal() -> pd.DataFrame | None:
    """
    Carrega info_economica_mensal (macro mensal) do Supabase.
    """
    try:
        df = _read_sql_df(
            """
            SELECT *
            FROM public.info_economica_mensal
            ORDER BY "Data" ASC
            """
        )
        return _coerce_sort_by_data(df, ascending=True)
    except Exception as e:
        st.error(f"Erro ao carregar macro mensal (info_economica_mensal): {e}")
        return None


# ════════════════════════════════════════════════════════════════════════════════
# PATCH 6 — Documentos corporativos (CVM/RI) para alimentar docs_by_ticker
# ════════════════════════════════════════════════════════════════════════════════

def make_doc_hash(ticker: str, data: str | None, url: str | None, raw_text: str) -> str:
    """
    Hash estável para deduplicação no Supabase (docs_corporativos).
    Use no ETL: doc_hash = make_doc_hash(...)
    """
    base = (
        f"{(ticker or '').upper().replace('.SA','').strip()}|"
        f"{data or ''}|"
        f"{url or ''}|"
        f"{(raw_text or '')[:20000]}"
    )
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


@st.cache_data(show_spinner=False, ttl=60 * 30)
def load_docs_corporativos_by_ticker(
    tickers: List[str],
    limit_per_ticker: int = 8,
    days_back: int = 365,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Retorna no formato que o Patch6 espera:
      { "PETR4": [{"source": "CVM:fato_relevante", "date": "2026-02-10", "text": "..."}], ... }

    Pré-requisito: tabela public.docs_corporativos existir no Supabase.
    """
    tks = [
        (t or "").strip().upper().replace(".SA", "")
        for t in (tickers or [])
        if str(t or "").strip()
    ]
    tks = list(dict.fromkeys([t for t in tks if t]))
    if not tks:
        return {}

    try:
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
    except Exception as e:
        # Mantém a app viva mesmo se a tabela não existir ainda
        st.error(f"Erro ao carregar docs_corporativos: {e}")
        return {tk: [] for tk in tks}

    out: Dict[str, List[Dict[str, Any]]] = {tk: [] for tk in tks}
    if df is None or df.empty:
        return out

    # normaliza
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

# ════════════════════════════════════════════════════════════════════════════════
# Patch 6 — Documentos corporativos (CVM/RI) para RAG
# Tabelas: public.docs_corporativos / public.docs_corporativos_chunks
# ════════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner=False, ttl=6 * 60 * 60)
def load_docs_corporativos_from_db(
    ticker: str,
    *,
    limit: int = 20,
    tipos: list[str] | None = None,
    fontes: list[str] | None = None,
) -> pd.DataFrame | None:
    """
    Carrega documentos corporativos textuais para um ticker.

    Retorna colunas típicas:
      [id, ticker, data, fonte, tipo, titulo, url, raw_text, lang, doc_hash, created_at]
    """
    tk1, tk2 = _normalize_ticker(ticker)
    limit = int(limit)

    wh_tipo = ""
    if tipos:
        wh_tipo = " AND tipo = ANY(:tipos) "

    wh_fonte = ""
    if fontes:
        wh_fonte = " AND fonte = ANY(:fontes) "

    try:
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
    except Exception as e:
        st.error(f"Erro ao carregar docs corporativos para {ticker}: {e}")
        return None


@st.cache_data(show_spinner=False, ttl=6 * 60 * 60)
def load_docs_corporativos_chunks_from_db(
    ticker: str,
    *,
    limit_docs: int = 12,
    limit_chunks_per_doc: int = 6,
) -> pd.DataFrame | None:
    """
    Carrega chunks de documentos (RAG) para um ticker.
    Estratégia: pega os últimos N docs e retorna até M chunks por doc (ordenados).
    """
    tk1, tk2 = _normalize_ticker(ticker)
    limit_docs = int(limit_docs)
    limit_chunks_per_doc = int(limit_chunks_per_doc)

    try:
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
                "limit_docs": limit_docs,
                "limit_chunks_per_doc": limit_chunks_per_doc,
            },
        )
        return df
    except Exception as e:
        st.error(f"Erro ao carregar chunks para {ticker}: {e}")
        return None


__all__ = [
    "get_supabase_engine",
    "load_setores_from_db",
    "load_setores_from_supabase",
    "load_data_from_db",
    "load_data_tri_from_db",
    "load_multiplos_from_db",
    "load_multiplos_limitado_from_db",
    "load_multiplos_tri_from_db",
    "load_multiplos_tri_hist_from_db",
    "load_macro_summary",
    "load_macro_mensal",
    "load_docs_corporativos_from_db",
    "load_docs_corporativos_chunks_from_db",
]
