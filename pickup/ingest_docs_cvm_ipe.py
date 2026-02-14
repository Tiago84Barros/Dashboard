# pickup/ingest_docs_cvm_ipe.py
# ──────────────────────────────────────────────────────────────────────────────
# Ingestão de documentos CVM (IPE) para Supabase:
# - baixa o ZIP anual do dataset IPE (dados.cvm.gov.br)
# - filtra por tickers (via CD_NEGOCIACAO no próprio dataset, ou via tabela cvm_to_ticker)
# - baixa o PDF do documento (quando houver URL)
# - extrai texto (PyPDF2 ou pdfminer.six, se disponível)
# - grava em public.docs_corporativos e public.docs_corporativos_chunks
#
# API principal esperada pelo app:
#   ingest_ipe_for_tickers(tickers: list[str], ...)
#
# Requisitos:
# - SUPABASE_DB_URL (ou DATABASE_URL) configurado
# - requests, pandas, sqlalchemy
# - (opcional) PyPDF2 ou pdfminer.six para extração de texto
# ──────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import hashlib
import io
import os
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------
# Config / Engine
# ---------------------------------------------------------------------

def _get_supabase_url() -> str:
    db_url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Defina SUPABASE_DB_URL (ou DATABASE_URL) nas secrets/env vars.")
    return db_url


@st.cache_resource(show_spinner=False)
def get_supabase_engine() -> Engine:
    return create_engine(_get_supabase_url(), pool_pre_ping=True)


def _read_sql_df(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    eng = get_supabase_engine()
    with eng.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


def _exec_sql(sql: str, params: Optional[Dict[str, Any]] = None) -> None:
    eng = get_supabase_engine()
    with eng.begin() as conn:
        conn.execute(text(sql), params or {})


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _norm_ticker(t: str) -> str:
    return (t or "").strip().upper().replace(".SA", "")


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        v = int(x)
        return v
    except Exception:
        return None


def _parse_date_any(x: Any) -> Optional[date]:
    if x is None:
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    s = str(x).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    # tenta pandas
    try:
        d = pd.to_datetime(s, errors="coerce")
        if pd.isna(d):
            return None
        return d.date()
    except Exception:
        return None


def _md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8", errors="ignore")).hexdigest()


def _chunk_text(texto: str, *, chunk_size: int = 1200, overlap: int = 200) -> List[str]:
    t = (texto or "").strip()
    if not t:
        return []
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    chunks: List[str] = []
    i = 0
    n = len(t)
    while i < n:
        j = min(n, i + chunk_size)
        chunk = t[i:j].strip()
        if chunk:
            chunks.append(chunk)
        if j >= n:
            break
        i = max(0, j - overlap)
    return chunks


def _requests_get(url: str, *, timeout: int = 60) -> requests.Response:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Patch6Bot/1.0; +https://streamlit.app)",
        "Accept": "*/*",
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """
    Tenta extrair texto do PDF:
    - PyPDF2 (rápido e comum)
    - pdfminer.six (melhor em muitos PDFs, mas pode não estar instalado)
    """
    if not pdf_bytes:
        return ""

    # 1) PyPDF2
    try:
        import PyPDF2  # type: ignore
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for p in reader.pages:
            try:
                parts.append(p.extract_text() or "")
            except Exception:
                continue
        out = "\n".join([x for x in parts if x])
        if out.strip():
            return out
    except Exception:
        pass

    # 2) pdfminer.six
    try:
        from pdfminer.high_level import extract_text  # type: ignore
        return (extract_text(io.BytesIO(pdf_bytes)) or "").strip()
    except Exception:
        return ""


@dataclass
class IpeRow:
    ticker: str
    data: Optional[date]
    fonte: str
    tipo: str
    titulo: str
    url: Optional[str]


# ---------------------------------------------------------------------
# CVM IPE download + parse
# ---------------------------------------------------------------------

def _cvm_ipe_zip_url(year: int) -> str:
    # padrão real observado:
    # https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_2022.zip
    return f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{year}.zip"


def _read_ipe_zip(year: int) -> pd.DataFrame:
    url = _cvm_ipe_zip_url(year)
    r = _requests_get(url, timeout=90)
    z = zipfile.ZipFile(io.BytesIO(r.content))

    # pega o primeiro CSV do zip
    csv_name = None
    for name in z.namelist():
        if name.lower().endswith(".csv"):
            csv_name = name
            break
    if not csv_name:
        raise RuntimeError(f"ZIP IPE {year} não contém CSV.")

    raw = z.read(csv_name)

    # tenta encodings comuns do CVM
    for enc in ("latin1", "cp1252", "utf-8"):
        try:
            df = pd.read_csv(
                io.BytesIO(raw),
                sep=";",
                encoding=enc,
                dtype=str,
                low_memory=False,
            )
            if not df.empty:
                return df
        except Exception:
            continue

    # último fallback
    return pd.read_csv(io.BytesIO(raw), sep=";", dtype=str, low_memory=False)


def _choose_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in cols:
            return cols[key]
    return None


def _build_rows_from_ipe_df(
    df: pd.DataFrame,
    tickers: List[str],
    *,
    fonte: str = "CVM",
) -> List[IpeRow]:
    """
    Tenta mapear colunas de forma robusta.
    Campos típicos do IPE variam; por isso o matcher é defensivo.
    """
    if df is None or df.empty:
        return []

    tickers_norm = sorted({_norm_ticker(t) for t in tickers if _norm_ticker(t)})
    if not tickers_norm:
        return []

    # tenta encontrar ticker direto no dataset
    col_ticker = _choose_col(df, ["CD_NEGOCIACAO", "TICKER", "cd_negociacao", "cod_negociacao"])
    col_data = _choose_col(df, ["DT_REFER", "DT_RECEB", "DT_ENTREGA", "DATA", "dt_refer", "dt_receb"])
    col_titulo = _choose_col(df, ["DS_ASSUNTO", "ASSUNTO", "TITULO", "DS_DOCUMENTO", "ds_assunto"])
    col_tipo = _choose_col(df, ["CATEG_DOC", "TP_DOC", "TIPO", "CATEGORIA", "tp_doc"])
    col_url = _choose_col(df, ["LINK_DOC", "LINK", "URL", "link_doc"])

    out: List[IpeRow] = []

    if col_ticker:
        # filtra por ticker direto
        dfx = df.copy()
        dfx[col_ticker] = dfx[col_ticker].astype(str).map(_norm_ticker)
        dfx = dfx[dfx[col_ticker].isin(tickers_norm)]
        if dfx.empty:
            return []

        for _, r in dfx.iterrows():
            tk = _norm_ticker(str(r.get(col_ticker, "")))
            dt = _parse_date_any(r.get(col_data)) if col_data else None
            titulo = str(r.get(col_titulo, "") or "").strip() if col_titulo else ""
            tipo = str(r.get(col_tipo, "") or "IPE").strip() if col_tipo else "IPE"
            url = str(r.get(col_url, "") or "").strip() if col_url else ""
            url = url or None
            out.append(IpeRow(ticker=tk, data=dt, fonte=fonte, tipo=tipo or "IPE", titulo=titulo or tipo or "IPE", url=url))
        return out

    # se não existir coluna de ticker, tenta mapear por CD_CVM usando tabela cvm_to_ticker
    col_cvm = _choose_col(df, ["CD_CVM", "COD_CVM", "cd_cvm"])
    if not col_cvm:
        return []

    # tenta buscar tabela de mapeamento (se existir)
    # esperado: public.cvm_to_ticker(cd_cvm, ticker)
    try:
        mp = _read_sql_df('SELECT cd_cvm, ticker FROM public.cvm_to_ticker')
        if mp is None or mp.empty:
            return []
        mp["ticker"] = mp["ticker"].astype(str).map(_norm_ticker)
        mp["cd_cvm"] = pd.to_numeric(mp["cd_cvm"], errors="coerce")
        mp = mp.dropna(subset=["cd_cvm", "ticker"])
        mp = mp[mp["ticker"].isin(tickers_norm)]
        if mp.empty:
            return []
        cvm_set = set(mp["cd_cvm"].astype(int).tolist())
    except Exception:
        return []

    dfx = df.copy()
    dfx[col_cvm] = pd.to_numeric(dfx[col_cvm], errors="coerce")
    dfx = dfx.dropna(subset=[col_cvm])
    dfx = dfx[dfx[col_cvm].astype(int).isin({int(x) for x in cvm_set})]
    if dfx.empty:
        return []

    # cria mapa cd_cvm -> ticker (pode haver mais de 1; pega o primeiro)
    cvm_to_tk = (
        mp.sort_values("ticker")
        .drop_duplicates(subset=["cd_cvm"])
        .set_index("cd_cvm")["ticker"]
        .to_dict()
    )

    for _, r in dfx.iterrows():
        cd = _safe_int(r.get(col_cvm))
        if cd is None:
            continue
        tk = _norm_ticker(cvm_to_tk.get(cd, ""))
        if not tk:
            continue
        dt = _parse_date_any(r.get(col_data)) if col_data else None
        titulo = str(r.get(col_titulo, "") or "").strip() if col_titulo else ""
        tipo = str(r.get(col_tipo, "") or "IPE").strip() if col_tipo else "IPE"
        url = str(r.get(col_url, "") or "").strip() if col_url else ""
        url = url or None
        out.append(IpeRow(ticker=tk, data=dt, fonte=fonte, tipo=tipo or "IPE", titulo=titulo or tipo or "IPE", url=url))

    return out


# ---------------------------------------------------------------------
# DB writes (docs_corporativos + chunks)
# ---------------------------------------------------------------------

def _insert_doc_and_chunks(
    *,
    ticker: str,
    data: Optional[date],
    fonte: str,
    tipo: str,
    titulo: str,
    url: Optional[str],
    raw_text: str,
    lang: str = "pt",
) -> Tuple[Optional[int], str, int]:
    """
    Retorna (doc_id, doc_hash, chunks_inserted)
    """
    ticker_n = _norm_ticker(ticker)
    raw_text = (raw_text or "").strip()
    if not ticker_n or not raw_text:
        return None, "", 0

    # doc_hash: amarra por (ticker + url + titulo + data + hash texto)
    base = f"{ticker_n}|{(data.isoformat() if data else 'NA')}|{fonte}|{tipo}|{titulo}|{url or 'NA'}|{_md5(raw_text[:20000])}"
    doc_hash = _md5(base)

    # insere doc (upsert por doc_hash)
    _exec_sql(
        """
        INSERT INTO public.docs_corporativos
          (ticker, data, fonte, tipo, titulo, url, raw_text, lang, doc_hash)
        VALUES
          (:ticker, :data, :fonte, :tipo, :titulo, :url, :raw_text, :lang, :doc_hash)
        ON CONFLICT (doc_hash) DO NOTHING
        """,
        {
            "ticker": ticker_n,
            "data": data.isoformat() if data else None,
            "fonte": fonte,
            "tipo": tipo,
            "titulo": titulo,
            "url": url,
            "raw_text": raw_text,
            "lang": lang,
            "doc_hash": doc_hash,
        },
    )

    # pega doc_id
    df_id = _read_sql_df(
        "SELECT id FROM public.docs_corporativos WHERE doc_hash = :h LIMIT 1",
        {"h": doc_hash},
    )
    if df_id.empty:
        return None, doc_hash, 0
    doc_id = int(df_id.iloc[0]["id"])

    chunks = _chunk_text(raw_text, chunk_size=1200, overlap=200)
    inserted = 0
    for idx, ch in enumerate(chunks):
        chunk_hash = _md5(f"{doc_hash}|{idx}|{_md5(ch)}")
        _exec_sql(
            """
            INSERT INTO public.docs_corporativos_chunks
              (doc_id, ticker, chunk_index, chunk_text, chunk_hash)
            VALUES
              (:doc_id, :ticker, :chunk_index, :chunk_text, :chunk_hash)
            ON CONFLICT (chunk_hash) DO NOTHING
            """,
            {
                "doc_id": doc_id,
                "ticker": ticker_n,
                "chunk_index": int(idx),
                "chunk_text": ch,
                "chunk_hash": chunk_hash,
            },
        )
        inserted += 1

    return doc_id, doc_hash, inserted


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------

def ingest_ipe_for_tickers(
    tickers: List[str],
    *,
    years_back: int = 2,
    max_docs_per_ticker: int = 20,
    sleep_s: float = 0.35,
    only_with_pdf_text: bool = True,
) -> Dict[str, Any]:
    """
    Ingestão “opção 1” (CVM/IPE) – recomendada para começar:
    - baixa anos: [ano_atual .. ano_atual-years_back]
    - filtra por tickers
    - tenta baixar PDF e extrair texto
    - grava no Supabase

    Retorna resumo:
      {
        "years": [...],
        "found_rows": int,
        "inserted_docs": int,
        "inserted_chunks": int,
        "by_ticker": { "ROMI3": {"docs": 3, "chunks": 90}, ... },
        "warnings": [...]
      }
    """
    tickers_norm = sorted({_norm_ticker(t) for t in (tickers or []) if _norm_ticker(t)})
    if not tickers_norm:
        return {"years": [], "found_rows": 0, "inserted_docs": 0, "inserted_chunks": 0, "by_ticker": {}, "warnings": ["tickers vazios"]}

    ano_atual = datetime.now().year
    years = list(range(ano_atual, max(2009, ano_atual - int(years_back)) - 1, -1))

    warnings: List[str] = []
    rows: List[IpeRow] = []

    # 1) Baixa e agrega linhas IPE por ano
    for y in years:
        try:
            dfy = _read_ipe_zip(y)
            rows_y = _build_rows_from_ipe_df(dfy, tickers_norm, fonte="CVM")
            rows.extend(rows_y)
        except Exception as e:
            warnings.append(f"IPE {y}: {type(e).__name__}: {e}")

    if not rows:
        return {"years": years, "found_rows": 0, "inserted_docs": 0, "inserted_chunks": 0, "by_ticker": {}, "warnings": warnings + ["nenhuma linha IPE encontrada para os tickers"]}

    # 2) Ordena por data desc (mais recente primeiro) e limita por ticker
    def _key(r: IpeRow) -> Tuple[int, str]:
        # data mais recente primeiro; se None, vai pro fim
        dscore = int(r.data.strftime("%Y%m%d")) if r.data else 0
        return (dscore, r.titulo or "")

    rows = sorted(rows, key=_key, reverse=True)

    per_ticker: Dict[str, int] = {t: 0 for t in tickers_norm}
    filtered: List[IpeRow] = []
    for r in rows:
        tk = _norm_ticker(r.ticker)
        if tk not in per_ticker:
            continue
        if per_ticker[tk] >= int(max_docs_per_ticker):
            continue
        filtered.append(r)
        per_ticker[tk] += 1

    # 3) Para cada linha, baixa PDF e extrai texto; grava
    inserted_docs = 0
    inserted_chunks = 0
    by_ticker: Dict[str, Dict[str, int]] = {t: {"docs": 0, "chunks": 0} for t in tickers_norm}

    for r in filtered:
        tk = _norm_ticker(r.ticker)
        url = (r.url or "").strip() if r.url else ""
        if not url:
            continue

        # tenta baixar e extrair texto
        raw_text = ""
        try:
            resp = _requests_get(url, timeout=90)
            ctype = (resp.headers.get("Content-Type") or "").lower()
            data_bytes = resp.content or b""

            if "pdf" in ctype or url.lower().endswith(".pdf") or data_bytes[:4] == b"%PDF":
                raw_text = _extract_pdf_text(data_bytes)
            else:
                # fallback: tenta tratar como texto/html
                try:
                    raw_text = data_bytes.decode("utf-8", errors="ignore")
                except Exception:
                    raw_text = ""
        except Exception as e:
            warnings.append(f"{tk}: falha ao baixar doc ({url[:60]}…): {type(e).__name__}: {e}")
            raw_text = ""

        raw_text = (raw_text or "").strip()

        if only_with_pdf_text and len(raw_text) < 200:
            # evita poluir base com vazio / muito curto
            continue

        # enriquece com cabeçalho para o LLM (contexto)
        header = f"[FONTE: {r.fonte}] [TIPO: {r.tipo}] [TICKER: {tk}] [DATA: {(r.data.isoformat() if r.data else 'NA')}] [TITULO: {r.titulo}]\n\n"
        full_text = header + raw_text

        try:
            doc_id, doc_hash, ch_ins = _insert_doc_and_chunks(
                ticker=tk,
                data=r.data,
                fonte=r.fonte,
                tipo=r.tipo or "IPE",
                titulo=r.titulo or (r.tipo or "IPE"),
                url=url,
                raw_text=full_text,
                lang="pt",
            )
            if doc_id is not None and doc_hash:
                inserted_docs += 1
                inserted_chunks += int(ch_ins)
                by_ticker[tk]["docs"] += 1
                by_ticker[tk]["chunks"] += int(ch_ins)
        except Exception as e:
            warnings.append(f"{tk}: falha ao inserir no Supabase: {type(e).__name__}: {e}")

        if sleep_s:
            time.sleep(float(sleep_s))

    return {
        "years": years,
        "found_rows": len(rows),
        "inserted_docs": inserted_docs,
        "inserted_chunks": inserted_chunks,
        "by_ticker": by_ticker,
        "warnings": warnings,
    }


# Alias compatível com alguns imports antigos (se você quiser)
def ingest_ipe_for_ticker(ticker: str, **kwargs) -> Dict[str, Any]:
    return ingest_ipe_for_tickers([ticker], **kwargs)


__all__ = [
    "ingest_ipe_for_tickers",
    "ingest_ipe_for_ticker",
    "get_supabase_engine",
]
