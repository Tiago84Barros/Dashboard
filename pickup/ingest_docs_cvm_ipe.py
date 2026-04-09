from __future__ import annotations
"""
pickup/ingest_docs_cvm_ipe.py  (Patch6 - CVM/IPE) — Heurística A/B/C/D
----------------------------------------------------------------------
Ingestão de documentos do dataset público da CVM (IPE) para a tabela public.docs_corporativos.

Camadas:
- A/B/C/D: ranking e seleção "Somente estratégicos" por score explicável
- PDFs: baixa e extrai texto (sem OCR), aceitando links CVM que não terminam em .pdf (frmDownloadDocumento.aspx)
- Schema-safe: escreve no campo de texto existente (prioridade raw_text, fallback texto)
- Backfill: se doc já existe mas não tem texto e conseguimos extrair, atualiza o texto (sem duplicar doc_hash)

Requer:
- public.cvm_to_ticker ("CVM" int, "Ticker" text)
- public.docs_corporativos com doc_hash unique + coluna raw_text (preferida) ou texto
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
from contextlib import nullcontext
from functools import lru_cache
import hashlib
import io
import json
import re
import time
import urllib.parse
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from sqlalchemy import text

try:
    from auditoria_dados.ingestion_log import IngestionLog as _IngestionLog
    from auditoria_dados.ingestion_log import validate_non_null_columns
    from auditoria_dados.ingestion_log import validate_required_columns
except ImportError:
    _IngestionLog = None
    validate_non_null_columns = None
    validate_required_columns = None

from core.docs_corporativos_store import DEFAULT_CHUNKING_VERSION
from core.docs_corporativos_store import persist_document_bundle
from core.db_loader import get_supabase_engine
from core.ticker_utils import normalize_ticker

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

_TEXT_COL_CACHE: Optional[str] = None
_TABLE_COLS_CACHE: Dict[Tuple[str, str], set[str]] = {}
_RUN_LOG = None
_CHUNKING_VERSION = f"ipe::{DEFAULT_CHUNKING_VERSION}"
_EXTRACTION_VERSION_PDF = "ipe_pdf_text_v1"
_EXTRACTION_VERSION_STUB = "ipe_metadata_stub_v1"


def _log(level: str, event: str, **fields: Any) -> None:
    if _RUN_LOG:
        _RUN_LOG.log(level, event, **fields)
        return
    payload = {"pipeline": "docs_ipe", "level": level, "event": event}
    payload.update(fields)
    print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)

def _engine():
    return get_supabase_engine()

def _norm_ticker(t: str) -> str:
    return normalize_ticker(t)

def _norm_cvm_code(val: Any) -> str:
    s = str(val or "").strip()
    if not s:
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    s = "".join(ch for ch in s if ch.isdigit())
    return s.lstrip("0") or "0"

def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

def _clean_text(s: str) -> str:
    s = (s or "").replace("\x00", " ").strip()
    s = re.sub(r"\s+", " ", s).strip()
    # normaliza "nan" literal
    if s.lower() == "nan":
        return ""
    return s


def _canonical_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    try:
        p = urllib.parse.urlparse(u)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        path = re.sub(r"/{2,}", "/", p.path or "").rstrip("/")
        return urllib.parse.urlunparse((scheme, netloc, path, "", p.query or "", ""))
    except Exception:
        return u

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _now_minus_months(months: int) -> datetime:
    return _utcnow() - timedelta(days=int(months) * 30)

def _parse_date(val: Any) -> Optional[pd.Timestamp]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return pd.to_datetime(val, errors="coerce", dayfirst=True)
    except Exception:
        return None

def _pick_col(cols: Sequence[str], *candidates: str) -> Optional[str]:
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None

def _get_text_column(conn) -> str:
    """
    Detecta coluna de texto em docs_corporativos:
      - preferir raw_text
      - fallback texto
    Cacheado por processo.
    """
    global _TEXT_COL_CACHE
    if _TEXT_COL_CACHE:
        return _TEXT_COL_CACHE

    rows = conn.execute(
        text("""
            select column_name
            from information_schema.columns
            where table_schema = 'public'
              and table_name = 'docs_corporativos'
              and column_name in ('raw_text','texto')
        """)
    ).fetchall()
    cols = {str(r[0]) for r in rows}
    if "raw_text" in cols:
        _TEXT_COL_CACHE = "raw_text"
        return _TEXT_COL_CACHE
    if "texto" in cols:
        _TEXT_COL_CACHE = "texto"
        return _TEXT_COL_CACHE
    raise RuntimeError("docs_corporativos não possui coluna raw_text nem texto.")



def _get_table_columns(conn, schema: str, table: str) -> set[str]:
    key = (schema, table)
    cached = _TABLE_COLS_CACHE.get(key)
    if cached is not None:
        return cached
    rows = conn.execute(
        text("""
            select column_name
            from information_schema.columns
            where table_schema = :schema
              and table_name = :table
        """),
        {"schema": schema, "table": table},
    ).fetchall()
    cols = {str(r[0]) for r in rows}
    _TABLE_COLS_CACHE[key] = cols
    return cols


def _first_existing(cols: set[str], *candidates: str) -> Optional[str]:
    for cand in candidates:
        if cand in cols:
            return cand
    return None
# ──────────────────────────────────────────────────────────────
# Sanitização de texto para Postgres
# ──────────────────────────────────────────────────────────────

def _sanitize_text(s: str) -> str:
    """Remove caracteres inválidos para PostgreSQL (ex.: NUL/\x00)."""
    if s is None:
        return ""
    # Postgres não aceita NUL em strings
    return str(s).replace("\x00", "")


def _empty_ticker_stats(
    *,
    existing_before: int = 0,
    requested_max_docs: int,
    requested_max_pdfs: int,
    stopped_reason: str,
) -> Dict[str, Any]:
    return {
        "source": "CVM/IPE",
        "existing_before": int(existing_before),
        "matched": 0,
        "dataset_candidates": 0,
        "documents_read": 0,
        "considered": 0,
        "inserted": 0,
        "skipped": 0,
        "updated_text": 0,
        "pdf_fetched": 0,
        "pdf_text_ok": 0,
        "chunks_generated": 0,
        "stubs": 0,
        "failures": 0,
        "requested_max_docs": int(requested_max_docs),
        "requested_max_pdfs": int(requested_max_pdfs),
        "selection_truncated": False,
        "pdf_limit_hit": False,
        "stopped_reason": stopped_reason,
        "selection_deduped": 0,
        "duplicate_existing": 0,
        "smart_skipped_complete": 0,
    }


# ──────────────────────────────────────────────────────────────
# Heurística A/B/C/D (Somente estratégicos)
# ──────────────────────────────────────────────────────────────

_POSITIVE_TYPES_HIGH = [
    "fato relevante",
    "comunicado ao mercado",
    "reorganização societ",
    "aquisi",
    "m&a",
    "fusão",
    "cisão",
    "incorp",
    "guidance",
    "proje",
    "plano de investimento",
    "capex",
    "debênt",
    "emissão",
    "recompra",
    "dividend",
    "jcp",
    "acordo",
    "parceria",
    "joint venture",
    "opa",
]
_POSITIVE_TYPES_MED = [
    "conselho de administração",
    "assembleia",
    "ago",
    "age",
    "política de dividend",
    "remuneração",
]

_KEYWORDS = [
    "capex", "invest", "expans", "guidance", "proje", "desalav", "dívida", "divida",
    "debênt", "debent", "aquisi", "fus", "cis", "incorp", "parceria", "contrato",
    "venda de ativo", "desinvest", "recompra", "dividendo", "jcp", "rating",
    "alocação", "alocacao", "plano", "projeto", "estratég", "estrateg",
]

_NOISE = [
    "eleição", "eleicao", "posse", "instalação", "instalacao", "regimento",
    "calendário", "calendario", "atualização cadastral", "atualizacao cadastral",
    "formulário", "formulario", "esclarecimento", "sem efeito", "retificação", "retificacao",
]

def _score_doc(tipo: str, titulo: str, assunto: str, categoria: str) -> int:
    """
    Score explicável para priorização estratégica.
    - A) positivo por tipos e keywords
    - B) penalização por ruído e títulos vazios/nan
    """
    tipo_n = (tipo or "").lower()
    titulo_n = (titulo or "").lower()
    assunto_n = (assunto or "").lower()
    cat_n = (categoria or "").lower()
    blob = f"{tipo_n} {titulo_n} {assunto_n} {cat_n}"

    score = 0

    # Tipos (alto)
    for k in _POSITIVE_TYPES_HIGH:
        if k in blob:
            score += 8
    # Tipos (médio)
    for k in _POSITIVE_TYPES_MED:
        if k in blob:
            score += 4

    # Keywords
    for k in _KEYWORDS:
        if k in blob:
            score += 3

    # Penalizações de ruído
    for k in _NOISE:
        if k in blob:
            score -= 6

    # Penalização por título vazio/nan
    if not (titulo or "").strip() or (titulo or "").strip().lower() == "nan":
        score -= 8

    return score

# ──────────────────────────────────────────────────────────────
# PDF text extraction (sem OCR)
# ──────────────────────────────────────────────────────────────

def _extract_pdf_text(pdf_bytes: bytes, max_pages: int = 25) -> str:
    if not pdf_bytes:
        return ""
    # PyPDF2 (rápido)
    try:
        import PyPDF2  # type: ignore
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        texts: List[str] = []
        for i, page in enumerate(reader.pages):
            if i >= max_pages:
                break
            t = page.extract_text() or ""
            if t:
                texts.append(t)
        out = "\n".join(texts).strip()
        if out:
            return out
    except Exception:
        pass

    # pdfminer.six (fallback)
    try:
        from pdfminer.high_level import extract_text  # type: ignore
        out = extract_text(io.BytesIO(pdf_bytes), maxpages=max_pages) or ""
        return (out or "").strip()
    except Exception:
        return ""

def _is_pdf_response(resp: requests.Response) -> bool:
    ctype = (resp.headers.get("content-type") or "").lower()
    if "pdf" in ctype:
        return True
    b = resp.content or b""
    return b.startswith(b"%PDF")

def _fetch_pdf_bytes(url: str, timeout: int = 25) -> Optional[bytes]:
    """
    CVM geralmente entrega PDF via frmDownloadDocumento.aspx; não confie em extensão.
    """
    if not url:
        return None
    # Heurística de elegibilidade de download (barata)
    u = url.lower()
    if not (u.endswith(".pdf") or "frmdownloaddocumento" in u or "download" in u):
        # ainda pode ser pdf, mas evita gastar em links improváveis
        return None
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, timeout=timeout, headers=headers)
    resp.raise_for_status()
    if not _is_pdf_response(resp):
        return None
    return resp.content

# ──────────────────────────────────────────────────────────────
# Supabase helpers
# ──────────────────────────────────────────────────────────────

def get_cvm_codes_for_tickers(tickers: Sequence[str]) -> Dict[str, int]:
    tks = [_norm_ticker(t) for t in tickers if (t or "").strip()]
    if not tks:
        return {}
    sql = """
        select "Ticker" as ticker, "CVM" as cvm
        from public.cvm_to_ticker
        where "Ticker" = any(:tks)
    """
    with _engine().connect() as conn:
        df = pd.read_sql_query(text(sql), conn, params={"tks": tks})
    out: Dict[str, int] = {}
    for _, r in df.iterrows():
        out[str(r["ticker"]).upper()] = int(r["cvm"])
    return out


def _count_existing_docs_for_ticker(conn, ticker: str) -> int:
    row = conn.execute(
        text("""
            select count(*)
            from public.docs_corporativos
            where upper(ticker) = upper(:ticker)
        """),
        {"ticker": _norm_ticker(ticker)},
    ).fetchone()
    try:
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0

def _get_doc_status(conn, doc_hash: str) -> Dict[str, Any]:
    """
    Retorna:
      exists: bool
      id: Optional[int]
      has_text: bool
    """
    text_col = _get_text_column(conn)
    row = conn.execute(
        text(f"""
            select id, coalesce(nullif(trim({text_col}),''), '') as t
            from public.docs_corporativos
            where doc_hash = :h
            limit 1
        """),
        {"h": doc_hash},
    ).fetchone()
    if not row:
        return {"exists": False, "id": None, "has_text": False}
    return {"exists": True, "id": int(row[0]), "has_text": bool(str(row[1] or "").strip())}


def _get_doc_status_by_metadata(
    conn,
    *,
    ticker: str,
    titulo: str,
    url: str,
    fonte: str,
    tipo: str,
    data: Optional[pd.Timestamp],
) -> Dict[str, Any]:
    text_col = _get_text_column(conn)
    row = conn.execute(
        text(
            f"""
            select id, coalesce(nullif(trim({text_col}),''), '') as t
            from public.docs_corporativos
            where upper(ticker) = upper(:ticker)
              and lower(coalesce(titulo, '')) = lower(:titulo)
              and lower(coalesce(url, '')) = lower(:url)
              and lower(coalesce(fonte, '')) = lower(:fonte)
              and lower(coalesce(tipo, '')) = lower(:tipo)
              and coalesce(data::date::text, '') = coalesce(:data, '')
            limit 1
            """
        ),
        {
            "ticker": ticker,
            "titulo": (titulo or "")[:4000],
            "url": _canonical_url(url)[:4000],
            "fonte": fonte,
            "tipo": (tipo or "")[:200],
            "data": (data.date().isoformat() if isinstance(data, pd.Timestamp) and not pd.isna(data) else None),
        },
    ).fetchone()
    if not row:
        return {"exists": False, "id": None, "has_text": False}
    return {"exists": True, "id": int(row[0]), "has_text": bool(str(row[1] or "").strip())}



def _get_existing_doc_text(conn, doc_id: int) -> str:
    text_col = _get_text_column(conn)
    row = conn.execute(
        text(f"""
            select coalesce({text_col}, '')
            from public.docs_corporativos
            where id = :id
            limit 1
        """),
        {"id": int(doc_id)},
    ).fetchone()
    return str(row[0] or "") if row else ""


def _get_doc_processing_state(
    conn,
    *,
    doc_hash: str,
    extraction_version_current: str,
    chunking_version_current: str,
) -> Dict[str, Any]:
    text_col = _get_text_column(conn)
    doc_cols = _get_table_columns(conn, "public", "docs_corporativos")
    extraction_col = "extraction_version" if "extraction_version" in doc_cols else None

    select_extra = f", {extraction_col} as extraction_version" if extraction_col else ""
    row = conn.execute(
        text(f"""
            select
                id,
                coalesce(nullif(trim({text_col}), ''), '') as txt
                {select_extra}
            from public.docs_corporativos
            where doc_hash = :h
            limit 1
        """),
        {"h": doc_hash},
    ).fetchone()

    if not row:
        return {
            "exists": False,
            "id": None,
            "has_text": False,
            "text": "",
            "extraction_version": None,
            "extraction_version_ok": False,
            "has_chunks": False,
            "chunk_count": 0,
            "chunk_version_ok": False,
            "is_complete": False,
        }

    doc_id = int(row[0])
    txt = str(row[1] or "")
    extraction_version = row[2] if extraction_col else None
    extraction_ok = bool(txt.strip()) if extraction_col is None else (str(extraction_version or "").strip() == extraction_version_current and bool(txt.strip()))

    chunk_cols = _get_table_columns(conn, "public", "docs_corporativos_chunks")
    doc_ref_col = _first_existing(chunk_cols, "doc_id", "document_id", "docs_corporativos_id")
    chunk_ver_col = "chunking_version" if "chunking_version" in chunk_cols else None

    chunk_count = 0
    has_chunks = False
    chunk_version_ok = False
    if doc_ref_col:
        select_ver = (
            f", max(case when {chunk_ver_col} = :chunking_version then 1 else 0 end) as version_ok"
            if chunk_ver_col else
            ""
        )
        row_chunk = conn.execute(
            text(f"""
                select count(*) as cnt
                {select_ver}
                from public.docs_corporativos_chunks
                where {doc_ref_col} = :doc_id
            """),
            {"doc_id": doc_id, "chunking_version": chunking_version_current},
        ).fetchone()
        chunk_count = int((row_chunk[0] or 0) if row_chunk else 0)
        has_chunks = chunk_count > 0
        chunk_version_ok = bool(has_chunks) if chunk_ver_col is None else bool(row_chunk[1]) if row_chunk else False

    is_complete = bool(txt.strip()) and extraction_ok and has_chunks and chunk_version_ok
    return {
        "exists": True,
        "id": doc_id,
        "has_text": bool(txt.strip()),
        "text": txt,
        "extraction_version": extraction_version,
        "extraction_version_ok": extraction_ok,
        "has_chunks": has_chunks,
        "chunk_count": chunk_count,
        "chunk_version_ok": chunk_version_ok,
        "is_complete": is_complete,
    }
def _insert_doc(
    conn,
    *,
    ticker: str,
    titulo: str,
    url: str,
    fonte: str,
    tipo: str,
    data: Optional[pd.Timestamp],
    texto: str,
    doc_hash: str,
) -> Optional[int]:
    text_col = _get_text_column(conn)
    params = {
        "ticker": ticker,
        "titulo": (titulo or "")[:4000],
        "url": _canonical_url(url)[:4000],
        "fonte": fonte,
        "tipo": (tipo or "")[:200],
        "data": (data.to_pydatetime() if isinstance(data, pd.Timestamp) and not pd.isna(data) else None),
        "doc_hash": doc_hash,
        "text_value": _sanitize_text(texto or ""),
    }
    row = conn.execute(
        text(f"""
            insert into public.docs_corporativos
            (ticker, titulo, url, fonte, tipo, data, {text_col}, doc_hash)
            values
            (:ticker, :titulo, :url, :fonte, :tipo, :data, :text_value, :doc_hash)
            on conflict (doc_hash) do nothing
            returning id
        """),
        params,
    ).fetchone()
    return int(row[0]) if row else None

def _update_doc_text(conn, doc_id: int, texto: str) -> bool:
    text_col = _get_text_column(conn)
    if not (texto or "").strip():
        return False
    conn.execute(
        text(f"""
            update public.docs_corporativos
            set {text_col} = :t
            where id = :id
        """),
        {"t": _sanitize_text(texto), "id": int(doc_id)},
    )
    return True

# ──────────────────────────────────────────────────────────────
# Core ingest
# ──────────────────────────────────────────────────────────────

@lru_cache(maxsize=8)
def _load_ipe_csv_cached(year: int, timeout: int = 30) -> pd.DataFrame:
    url_zip = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{year}.zip"
    r = requests.get(url_zip, timeout=timeout)
    r.raise_for_status()

    import zipfile
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if not csv_names:
        raise RuntimeError("ZIP do IPE não contém CSV")
    raw = zf.read(csv_names[0])

    for enc in ("utf-8", "latin1"):
        try:
            return pd.read_csv(io.BytesIO(raw), sep=";", encoding=enc, dtype=str)
        except Exception:
            continue
    return pd.read_csv(io.BytesIO(raw), sep=";", encoding="latin1", dtype=str)

def ingest_ipe_for_tickers(
    tickers: Sequence[str],
    *,
    window_months: int = 12,
    max_docs_per_ticker: int = 60,
    strategic_only: bool = True,
    download_pdfs: bool = True,
    max_pdfs_per_ticker: int = 12,
    pdf_max_pages: int = 25,
    request_timeout: int = 25,
    max_runtime_s: float = 90.0,
    sleep_s: float = 0.0,
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    Ingest por tickers usando o dataset IPE.

    Implementa heurística A/B/C/D quando strategic_only=True:
      A) score por sinal positivo (tipos/keywords)
      B) penalização por ruído
      C) cobertura mínima (fallback) se poucos docs estratégicos
      D) auditoria (top 10 selecionados com score)
    """
    global _RUN_LOG

    tickers_n = [_norm_ticker(t) for t in tickers if (t or "").strip()]
    effective_runtime_s = max(float(max_runtime_s or 0.0), 1.0)
    run_ctx = _IngestionLog("docs_ipe") if _IngestionLog else nullcontext(None)
    if _IngestionLog and run_ctx:
        run_ctx.set_params(
            {
                "tickers": tickers_n,
                "window_months": int(window_months),
                "max_docs_per_ticker": int(max_docs_per_ticker),
                "max_pdfs_per_ticker": int(max_pdfs_per_ticker),
                "pdf_max_pages": int(pdf_max_pages),
                "request_timeout": int(request_timeout),
                "max_runtime_s": effective_runtime_s,
                "chunking_version": _CHUNKING_VERSION,
                "extraction_versions": [_EXTRACTION_VERSION_PDF, _EXTRACTION_VERSION_STUB],
            }
        )

    with run_ctx as run:
        _RUN_LOG = run
        _log(
            "INFO",
            "start",
            tickers=len(tickers_n),
            window_months=window_months,
            max_docs_per_ticker=max_docs_per_ticker,
            max_pdfs_per_ticker=max_pdfs_per_ticker,
        )
        cvm_map = get_cvm_codes_for_tickers(tickers_n)

        now = _utcnow()
        min_dt = _now_minus_months(int(window_months))
        years = list(range(int(min_dt.year), int(now.year) + 1))
        dfs: List[pd.DataFrame] = []
        out_stats: Dict[str, Any] = {}
        out_errors: Dict[str, str] = {}

        for y in years:
            try:
                dfs.append(_load_ipe_csv_cached(y, timeout=request_timeout).copy())
                _log("INFO", "year_loaded", year=y)
            except Exception as e:
                _log("WARN", "year_load_failed", year=y, error=str(e))
                if run:
                    run.increment_metric("year_load_failures")
                if verbose:
                    print(f"[IPE] Falha ao carregar {y}: {e}")

        if verbose:
            print(f"[IPE] anos carregados={years} | arquivos={len(dfs)}")
        if not dfs:
            summary = {
                "ok": False,
                "run_id": getattr(run, "run_id", None),
                "errors": {"__all__": "Nenhum CSV IPE disponível."},
                "stats": {},
                "window_months": int(window_months),
                "max_runtime_s": effective_runtime_s,
            }
            if run:
                run.add_error("Nenhum CSV IPE disponível.")
            _log("ERROR", "summary", **summary)
            _RUN_LOG = None
            return summary

        df = pd.concat(dfs, ignore_index=True)
        cols = list(df.columns)

        col_cvm = _pick_col(cols, "CODIGO_CVM", "CD_CVM", "CVM", "COD_CVM")
        col_data = _pick_col(cols, "DATA_ENTREGA", "DT_RECEB", "DT_REFER", "DATA_REFERENCIA", "DATA_REFER", "DT_ENTREGA")
        col_link = _pick_col(cols, "LINK_DOWNLOAD", "LINK", "LINK_ARQUIVO", "LINK_DOC", "LINK_DOCUMENTO")
        col_assunto = _pick_col(cols, "ASSUNTO", "ASSUNTO_EVENTO", "TITULO", "DESCRICAO")
        col_categoria = _pick_col(cols, "CATEGORIA", "CATEGORIA_DOCUMENTO")
        col_tipo = _pick_col(cols, "TIPO", "TIPO_DOCUMENTO")

        if any(c is None for c in (col_cvm, col_data, col_link, col_assunto)):
            message = f"CSV IPE sem colunas necessárias. Encontradas={cols}"
            if run:
                run.add_error(message)
            summary = {
                "ok": False,
                "run_id": getattr(run, "run_id", None),
                "errors": {"__all__": message},
                "stats": {},
                "window_months": int(window_months),
                "max_runtime_s": effective_runtime_s,
            }
            _log("ERROR", "summary", **summary)
            _RUN_LOG = None
            return summary

        if validate_required_columns:
            validate_required_columns(
                df,
                [col_cvm, col_data, col_link],
                context="CSV IPE normalizado",
                logger=run,
            )

        df[col_assunto] = df[col_assunto].fillna("").astype(str).map(_clean_text)
        df.loc[df[col_assunto].eq(""), col_assunto] = "Sem assunto"

        df["_dt"] = df[col_data].apply(_parse_date)
        df = df[~df["_dt"].isna()].copy()
        df["_url_norm"] = df[col_link].fillna("").map(lambda x: _canonical_url(str(x)))

        min_ts = pd.Timestamp(min_dt).tz_localize(None)
        df["_dt_naive"] = df["_dt"].apply(
            lambda x: x.tz_localize(None)
            if hasattr(x, "tz_localize") and getattr(x, "tzinfo", None) is not None
            else x
        )
        df = df[df["_dt_naive"] >= min_ts].copy()

        if validate_non_null_columns and not df.empty:
            valid_df = df[df["_url_norm"].astype(str).str.strip().ne("")].copy()
            if not valid_df.empty:
                validate_non_null_columns(
                    valid_df,
                    [col_cvm, col_data, col_link],
                    context="CSV IPE filtrado",
                    logger=run,
                )

        if strategic_only:
            df["_tipo"] = df[col_tipo].fillna("").map(_clean_text) if col_tipo else ""
            df["_titulo"] = df[col_assunto].fillna("").map(_clean_text)
            df["_assunto"] = df[col_assunto].fillna("").map(_clean_text)
            df["_categoria"] = df[col_categoria].fillna("").map(_clean_text) if col_categoria else ""
            df["_score"] = df.apply(
                lambda r: _score_doc(
                    str(r.get("_tipo", "") or ""),
                    str(r.get("_titulo", "") or ""),
                    str(r.get("_assunto", "") or ""),
                    str(r.get("_categoria", "") or ""),
                ),
                axis=1,
            )
        else:
            df["_score"] = 0

        df["_cvm_norm"] = df[col_cvm].map(_norm_cvm_code)
        started = time.time()
        min_coverage = 8
        min_score_strategic = 3

        with _engine().begin() as conn:
            _ = _get_text_column(conn)

            for tk in tickers_n:
                if (time.time() - started) > effective_runtime_s:
                    out_errors["__runtime__"] = f"Tempo máximo atingido ({effective_runtime_s}s)."
                    _log("WARN", "runtime_limit_reached", seconds=effective_runtime_s)
                    break

                cvm = cvm_map.get(tk)
                if not cvm:
                    out_errors[tk] = "ticker_sem_mapeamento_cvm (preencha public.cvm_to_ticker)"
                    out_stats[tk] = _empty_ticker_stats(
                        requested_max_docs=max_docs_per_ticker,
                        requested_max_pdfs=max_pdfs_per_ticker,
                        stopped_reason="ticker_sem_mapeamento_cvm",
                    )
                    if run:
                        run.add_source_metrics(
                            source="CVM/IPE",
                            ticker=tk,
                            failures=1,
                        )
                    continue

                existing_before = _count_existing_docs_for_ticker(conn, tk)
                cvm_norm = _norm_cvm_code(cvm)
                dft_all = df[df["_cvm_norm"] == cvm_norm].copy().sort_values("_dt_naive", ascending=False)
                matched = int(len(dft_all))

                if verbose:
                    print(f"[IPE] {tk} | cvm={cvm} | cvm_norm={cvm_norm} | matched={matched} | existing_before={existing_before}")

                if matched == 0:
                    stats = _empty_ticker_stats(
                        existing_before=existing_before,
                        requested_max_docs=max_docs_per_ticker,
                        requested_max_pdfs=max_pdfs_per_ticker,
                        stopped_reason="no_dataset_match",
                    )
                    out_stats[tk] = stats
                    if run:
                        run.add_source_metrics(source="CVM/IPE", ticker=tk, documents_read=0)
                    continue

                fallback_used = False
                selected_strategic = 0
                if strategic_only:
                    dft_ranked = dft_all.sort_values(["_score", "_dt_naive"], ascending=[False, False]).copy()
                    strategic = dft_ranked[dft_ranked["_score"] >= min_score_strategic].copy()
                    selected = strategic.copy()
                    selected_strategic = int(len(selected))
                    if selected_strategic < min_coverage:
                        fallback_used = True
                        remaining = dft_ranked.loc[~dft_ranked.index.isin(selected.index)].sort_values("_dt_naive", ascending=False)
                        need = int(max_docs_per_ticker) - selected_strategic
                        if need > 0:
                            selected = pd.concat([selected, remaining.head(need)], ignore_index=False)
                else:
                    selected = dft_all.copy()

                before_selection_dedup = len(selected)
                selected = selected.drop_duplicates(subset=["_url_norm", col_assunto, "_dt_naive"], keep="first").copy()
                dataset_candidates = int(len(selected))
                selection_deduped = int(before_selection_dedup - dataset_candidates)
                selection_truncated = bool(dataset_candidates > int(max_docs_per_ticker))
                selected = selected.head(int(max_docs_per_ticker)).copy()
                considered = int(len(selected))

                _log(
                    "INFO",
                    "ticker_selection_ready",
                    ticker=tk,
                    matched=matched,
                    dataset_candidates=dataset_candidates,
                    selection_deduped=selection_deduped,
                    selection_truncated=selection_truncated,
                    fallback_used=fallback_used if strategic_only else None,
                )

                audit_top: List[Dict[str, Any]] = []
                for _, rr in selected.head(10).iterrows():
                    audit_top.append(
                        {
                            "data": str(rr.get("_dt_naive") or ""),
                            "tipo": _clean_text(str(rr.get(col_tipo, "") or "")) if col_tipo else "",
                            "titulo": _clean_text(str(rr.get(col_assunto, "") or "")),
                            "categoria": _clean_text(str(rr.get(col_categoria, "") or "")) if col_categoria else "",
                            "score": int(rr.get("_score") or 0),
                            "url": str(rr.get(col_link, "") or "")[:300],
                        }
                    )

                inserted = 0
                skipped = 0
                updated_text = 0
                pdf_fetched = 0
                pdf_text_ok = 0
                pdf_used = 0
                pdf_limit_hit = False
                duplicate_existing = 0
                smart_skipped_complete = 0
                chunks_generated = 0
                stubs = 0
                failures = 0

                for _, r in selected.iterrows():
                    if (time.time() - started) > effective_runtime_s:
                        out_errors["__runtime__"] = f"Tempo máximo atingido ({effective_runtime_s}s)."
                        break

                    url = _canonical_url(str(r.get(col_link, "") or "").strip())
                    if not url:
                        skipped += 1
                        failures += 1
                        continue

                    titulo = _clean_text(str(r.get(col_assunto, "") or "")) or "Documento CVM/IPE"
                    tipo = _clean_text(str(r.get(col_tipo, "") or "")) if col_tipo else ""
                    if not tipo:
                        tipo = "IPE"

                    dt = r.get("_dt")
                    dt_key = dt.date().isoformat() if isinstance(dt, pd.Timestamp) and not pd.isna(dt) else ""
                    doc_hash = _sha256(f"{tk}|{url}|{titulo}|{dt_key}")

                    state = _get_doc_processing_state(
                        conn,
                        doc_hash=doc_hash,
                        extraction_version_current=_EXTRACTION_VERSION_PDF,
                        chunking_version_current=_CHUNKING_VERSION,
                    )
                    if state["is_complete"]:
                        duplicate_existing += 1
                        smart_skipped_complete += 1
                        skipped += 1
                        continue

                    texto = ""
                    extraction_version = _EXTRACTION_VERSION_STUB
                    is_stub = True

                    # Se o documento já existe com texto válido, reutiliza o texto salvo
                    # e evita baixar/processar PDF novamente. Isso permite backfill de chunks
                    # ou reprocessamento por mudança de versão sem custo extra de rede.
                    if state["exists"] and state["has_text"]:
                        texto = _sanitize_text(state["text"])
                        is_stub = False
                        extraction_version = (
                            str(state.get("extraction_version") or "").strip()
                            or _EXTRACTION_VERSION_PDF
                        )
                    elif download_pdfs and pdf_used < int(max_pdfs_per_ticker):
                        try:
                            pdf_bytes = _fetch_pdf_bytes(url, timeout=request_timeout)
                            if pdf_bytes:
                                pdf_fetched += 1
                                pdf_used += 1
                                tpdf = _extract_pdf_text(pdf_bytes, max_pages=int(pdf_max_pages))
                                tpdf = tpdf.strip() if tpdf else ""
                                if tpdf and len(tpdf) >= 200:
                                    texto = tpdf
                                    pdf_text_ok += 1
                                    extraction_version = _EXTRACTION_VERSION_PDF
                                    is_stub = False
                        except Exception as e:
                            failures += 1
                            _log("WARN", "pdf_extract_failed", ticker=tk, url=url[:300], error=str(e))
                    elif download_pdfs and int(max_pdfs_per_ticker) > 0:
                        pdf_limit_hit = True

                    try:
                        persisted = persist_document_bundle(
                            conn,
                            ticker=tk,
                            titulo=titulo,
                            url=url,
                            fonte="CVM/IPE",
                            tipo=tipo,
                            data=dt,
                            texto=_sanitize_text(texto),
                            doc_hash=doc_hash,
                            chunking_version=_CHUNKING_VERSION,
                            extraction_version=extraction_version,
                            run_id=getattr(run, "run_id", None),
                            is_stub=is_stub,
                        )
                    except Exception as e:
                        failures += 1
                        skipped += 1
                        _log("ERROR", "doc_persist_failed", ticker=tk, url=url[:300], error=str(e))
                        if run:
                            run.add_warning(f"{tk}: falha ao persistir doc {url[:120]}: {e}")
                        continue

                    if persisted.get("inserted"):
                        inserted += 1
                    if persisted.get("updated_text"):
                        updated_text += 1
                    if persisted.get("duplicate"):
                        duplicate_existing += 1
                        skipped += 1
                    chunks_generated += int(persisted.get("chunks_inserted", 0) or 0)
                    if persisted.get("stub"):
                        stubs += 1

                    if sleep_s:
                        time.sleep(float(sleep_s))

                stats = {
                    "source": "CVM/IPE",
                    "existing_before": int(existing_before),
                    "matched": matched,
                    "dataset_candidates": dataset_candidates,
                    "documents_read": considered,
                    "considered": considered,
                    "inserted": inserted,
                    "skipped": skipped,
                    "updated_text": updated_text,
                    "pdf_fetched": pdf_fetched,
                    "pdf_text_ok": pdf_text_ok,
                    "chunks_generated": chunks_generated,
                    "stubs": stubs,
                    "failures": failures,
                    "requested_max_docs": int(max_docs_per_ticker),
                    "requested_max_pdfs": int(max_pdfs_per_ticker),
                    "selection_truncated": selection_truncated,
                    "pdf_limit_hit": pdf_limit_hit,
                    "selection_deduped": selection_deduped,
                    "duplicate_existing": duplicate_existing,
                    "smart_skipped_complete": smart_skipped_complete,
                    "selected_strategic": selected_strategic if strategic_only else None,
                    "fallback_used": fallback_used if strategic_only else None,
                    "top_selected": audit_top,
                }
                out_stats[tk] = stats

                if run:
                    run.add_source_metrics(
                        source="CVM/IPE",
                        ticker=tk,
                        documents_read=considered,
                        documents_inserted=inserted,
                        duplicates=duplicate_existing,
                        chunks_generated=chunks_generated,
                        skipped=smart_skipped_complete,
                        stubs=stubs,
                        failures=failures,
                    )

                _log(
                    "INFO",
                    "ticker_summary",
                    ticker=tk,
                    matched=matched,
                    considered=considered,
                    inserted=inserted,
                    skipped=skipped,
                    updated_text=updated_text,
                    pdf_fetched=pdf_fetched,
                    pdf_text_ok=pdf_text_ok,
                    chunks_generated=chunks_generated,
                    stubs=stubs,
                    failures=failures,
                    selection_deduped=selection_deduped,
                    duplicate_existing=duplicate_existing,
                    smart_skipped_complete=smart_skipped_complete,
                    fallback_used=fallback_used if strategic_only else None,
                )

        ok = len(out_errors) == 0
        result = {
            "ok": ok,
            "run_id": getattr(run, "run_id", None),
            "stats": out_stats,
            "errors": out_errors,
        }
        _log(
            "INFO" if ok else "WARN",
            "summary",
            ok=ok,
            run_id=result["run_id"],
            tickers=len(tickers_n),
            tickers_with_stats=len(out_stats),
            errors=len(out_errors),
            inserted=sum(int(v.get("inserted", 0)) for v in out_stats.values()),
            skipped=sum(int(v.get("skipped", 0)) for v in out_stats.values()),
            updated_text=sum(int(v.get("updated_text", 0)) for v in out_stats.values()),
            pdf_fetched=sum(int(v.get("pdf_fetched", 0)) for v in out_stats.values()),
            pdf_text_ok=sum(int(v.get("pdf_text_ok", 0)) for v in out_stats.values()),
            chunks_generated=sum(int(v.get("chunks_generated", 0)) for v in out_stats.values()),
            stubs=sum(int(v.get("stubs", 0)) for v in out_stats.values()),
            failures=sum(int(v.get("failures", 0)) for v in out_stats.values()),
            selection_deduped=sum(int(v.get("selection_deduped", 0)) for v in out_stats.values()),
            duplicate_existing=sum(int(v.get("duplicate_existing", 0)) for v in out_stats.values()),
            smart_skipped_complete=sum(int(v.get("smart_skipped_complete", 0)) for v in out_stats.values()),
        )
        _RUN_LOG = None
        return result
