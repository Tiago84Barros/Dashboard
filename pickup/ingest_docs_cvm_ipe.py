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
from functools import lru_cache
import hashlib
import io
import json
import re
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from sqlalchemy import text

try:
    from auditoria_dados.ingestion_log import validate_required_columns
except ImportError:
    validate_required_columns = None

from core.db_loader import get_supabase_engine
from core.ticker_utils import normalize_ticker

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

_TEXT_COL_CACHE: Optional[str] = None


def _log(level: str, event: str, **fields: Any) -> None:
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

# ──────────────────────────────────────────────────────────────
# Sanitização de texto para Postgres
# ──────────────────────────────────────────────────────────────

def _sanitize_text(s: str) -> str:
    """Remove caracteres inválidos para PostgreSQL (ex.: NUL/\x00)."""
    if s is None:
        return ""
    # Postgres não aceita NUL em strings
    return str(s).replace("\x00", "")


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
        "url": (url or "")[:4000],
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
    _log(
        "INFO",
        "start",
        tickers=len(tickers),
        window_months=window_months,
        max_docs_per_ticker=max_docs_per_ticker,
        max_pdfs_per_ticker=max_pdfs_per_ticker,
    )
    tickers_n = [_norm_ticker(t) for t in tickers if (t or "").strip()]
    cvm_map = get_cvm_codes_for_tickers(tickers_n)

    now = _utcnow()
    min_dt = _now_minus_months(int(window_months))

    years = list(range(int(min_dt.year), int(now.year) + 1))
    dfs: List[pd.DataFrame] = []
    for y in years:
        try:
            dfs.append(_load_ipe_csv_cached(y, timeout=request_timeout).copy())
        except Exception as e:
            _log("WARN", "year_load_failed", year=y, error=str(e))
            if verbose:
                print(f"[IPE] Falha ao carregar {y}: {e}")

    if verbose:
        print(f"[IPE] anos carregados={years} | arquivos={len(dfs)}")
    if not dfs:
        summary = {"ok": False, "errors": {"__all__": "Nenhum CSV IPE disponível."}, "stats": {}, "matched": 0, "considered": 0, "inserted": 0, "skipped": 0, "updated_text": 0, "pdf_fetched": 0, "pdf_text_ok": 0, "requested_max_docs": int(max_docs_per_ticker), "requested_max_pdfs": int(max_pdfs_per_ticker), "window_months": int(window_months), "max_runtime_s": float(max(float(max_runtime_s or 0.0), 1.0))}
        _log("ERROR", "summary", **summary)
        return summary

    df = pd.concat(dfs, ignore_index=True)
    cols = list(df.columns)

    col_cvm = _pick_col(cols, "CODIGO_CVM", "CD_CVM", "CVM", "COD_CVM")
    col_data = _pick_col(cols, "DATA_ENTREGA", "DT_RECEB", "DT_REFER", "DATA_REFERENCIA", "DATA_REFER", "DT_ENTREGA")
    col_link = _pick_col(cols, "LINK_DOWNLOAD", "LINK", "LINK_ARQUIVO", "LINK_DOC", "LINK_DOCUMENTO")
    col_assunto = _pick_col(cols, "ASSUNTO", "ASSUNTO_EVENTO", "TITULO", "DESCRICAO")
    col_categoria = _pick_col(cols, "CATEGORIA", "CATEGORIA_DOCUMENTO")
    col_tipo = _pick_col(cols, "TIPO", "TIPO_DOCUMENTO")
    col_especie = _pick_col(cols, "ESPECIE", "ESPECIE_DOCUMENTO")

    if any(c is None for c in (col_cvm, col_data, col_link, col_assunto)):
        summary = {"ok": False, "errors": {"__all__": f"CSV IPE sem colunas necessárias. Encontradas={cols}"}, "stats": {}, "matched": 0, "considered": 0, "inserted": 0, "skipped": 0, "updated_text": 0, "pdf_fetched": 0, "pdf_text_ok": 0, "requested_max_docs": int(max_docs_per_ticker), "requested_max_pdfs": int(max_pdfs_per_ticker), "window_months": int(window_months), "max_runtime_s": float(max(float(max_runtime_s or 0.0), 1.0))}
        _log("ERROR", "summary", **summary)
        return summary
    if validate_required_columns:
        validate_required_columns(
            df,
            [col_cvm, col_data, col_link, col_assunto],
            context="CSV IPE normalizado",
        )

    df["_dt"] = df[col_data].apply(_parse_date)
    df = df[~df["_dt"].isna()].copy()

    # tz-safe para evitar comparação dtype=datetime64[ns] vs Timestamp tz-aware
    min_ts = pd.Timestamp(min_dt).tz_localize(None)
    df["_dt_naive"] = df["_dt"].apply(lambda x: x.tz_localize(None) if hasattr(x, "tz_localize") and getattr(x, "tzinfo", None) is not None else x)
    df = df[df["_dt_naive"] >= min_ts].copy()

    # precompute campos textuais para scoring
    if strategic_only:
        df["_tipo"] = df[col_tipo].fillna("").map(_clean_text) if col_tipo else ""
        df["_titulo"] = df[col_assunto].fillna("").map(_clean_text)
        df["_assunto"] = df[col_assunto].fillna("").map(_clean_text)
        df["_categoria"] = df[col_categoria].fillna("").map(_clean_text) if col_categoria else ""
        df["_score"] = df.apply(lambda r: _score_doc(
            str(r.get("_tipo","") or ""),
            str(r.get("_titulo","") or ""),
            str(r.get("_assunto","") or ""),
            str(r.get("_categoria","") or "")
        ), axis=1)
    else:
        df["_score"] = 0

    # normaliza código CVM uma vez para evitar falhas por formato no CSV
    df["_cvm_norm"] = df[col_cvm].map(_norm_cvm_code)

    out_stats: Dict[str, Any] = {}
    out_errors: Dict[str, str] = {}
    effective_runtime_s = max(float(max_runtime_s or 0.0), 1.0)
    started = time.time()

    MIN_COVERAGE = 8   # C) cobertura mínima
    MIN_SCORE_STRATEGIC = 3  # threshold leve, evita ficar vazio

    with _engine().begin() as conn:
        # garante cache de coluna de texto inicializado (e falha cedo se schema inválido)
        _ = _get_text_column(conn)

        for tk in tickers_n:
            if (time.time() - started) > effective_runtime_s:
                out_errors["__runtime__"] = f"Tempo máximo atingido ({effective_runtime_s}s)."
                _log("WARN", "runtime_limit_reached", seconds=effective_runtime_s)
                break

            cvm = cvm_map.get(tk)
            if not cvm:
                out_errors[tk] = "ticker_sem_mapeamento_cvm (preencha public.cvm_to_ticker)"
                out_stats[tk] = {
                    "existing_before": 0,
                    "matched": 0,
                    "dataset_candidates": 0,
                    "considered": 0,
                    "inserted": 0,
                    "skipped": 0,
                    "updated_text": 0,
                    "pdf_fetched": 0,
                    "pdf_text_ok": 0,
                    "requested_max_docs": int(max_docs_per_ticker),
                    "requested_max_pdfs": int(max_pdfs_per_ticker),
                    "selection_truncated": False,
                    "pdf_limit_hit": False,
                    "stopped_reason": "ticker_sem_mapeamento_cvm",
                }
                continue

            existing_before = _count_existing_docs_for_ticker(conn, tk)

            cvm_norm = _norm_cvm_code(cvm)
            dft_all = df[df["_cvm_norm"] == cvm_norm].copy()
            dft_all = dft_all.sort_values("_dt_naive", ascending=False)

            matched = int(len(dft_all))
            if verbose:
                print(f"[IPE] {tk} | cvm={cvm} | cvm_norm={cvm_norm} | matched={matched} | existing_before={existing_before}")
            if matched == 0:
                out_stats[tk] = {
                    "existing_before": int(existing_before),
                    "matched": 0,
                    "dataset_candidates": 0,
                    "considered": 0,
                    "inserted": 0,
                    "skipped": 0,
                    "updated_text": 0,
                    "pdf_fetched": 0,
                    "pdf_text_ok": 0,
                    "requested_max_docs": int(max_docs_per_ticker),
                    "requested_max_pdfs": int(max_pdfs_per_ticker),
                    "selection_truncated": False,
                    "pdf_limit_hit": False,
                    "stopped_reason": "no_dataset_match",
                }
                continue

            fallback_used = False
            selected_strategic = 0

            if strategic_only:
                dft_ranked = dft_all.sort_values(["_score", "_dt_naive"], ascending=[False, False]).copy()

                strategic = dft_ranked[dft_ranked["_score"] >= MIN_SCORE_STRATEGIC].copy()
                selected = strategic.copy()
                selected_strategic = int(len(selected))

                if selected_strategic < MIN_COVERAGE:
                    fallback_used = True
                    remaining = dft_ranked.loc[~dft_ranked.index.isin(selected.index)].sort_values("_dt_naive", ascending=False)
                    need = int(max_docs_per_ticker) - selected_strategic
                    if need > 0:
                        selected = pd.concat([selected, remaining.head(need)], ignore_index=False)
            else:
                selected = dft_all.copy()

            selected = selected.drop_duplicates(subset=[col_link, col_assunto, "_dt_naive"], keep="first").copy()
            dataset_candidates = int(len(selected))
            selection_truncated = bool(dataset_candidates > int(max_docs_per_ticker))
            selected = selected.head(int(max_docs_per_ticker)).copy()
            considered = int(len(selected))

            audit_top: List[Dict[str, Any]] = []
            for _, rr in selected.head(10).iterrows():
                audit_top.append({
                    "data": str(rr.get("_dt_naive") or ""),
                    "tipo": _clean_text(str(rr.get(col_tipo, "") or "")) if col_tipo else "",
                    "titulo": _clean_text(str(rr.get(col_assunto, "") or "")),
                    "categoria": _clean_text(str(rr.get(col_categoria, "") or "")) if col_categoria else "",
                    "score": int(rr.get("_score") or 0),
                    "url": str(rr.get(col_link, "") or "")[:300],
                })

            inserted = 0
            skipped = 0
            updated_text = 0
            pdf_fetched = 0
            pdf_text_ok = 0
            pdf_used = 0
            pdf_limit_hit = False

            for _, r in selected.iterrows():
                if (time.time() - started) > effective_runtime_s:
                    out_errors["__runtime__"] = f"Tempo máximo atingido ({effective_runtime_s}s)."
                    break

                url = str(r.get(col_link, "") or "").strip()
                if not url:
                    skipped += 1
                    continue

                titulo = _clean_text(str(r.get(col_assunto, "") or "")) or ""
                tipo = _clean_text(str(r.get(col_tipo, "") or "")) if col_tipo else ""
                if not tipo:
                    tipo = "IPE"

                dt = r.get("_dt")  # Timestamp
                doc_hash = _sha256(f"{tk}|{url}|{titulo}|{dt}")

                status = _get_doc_status(conn, doc_hash)

                texto = ""
                if download_pdfs and pdf_used < int(max_pdfs_per_ticker):
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
                    except Exception:
                        _log("WARN", "pdf_extract_failed", ticker=tk, url=url[:300])
                        pass
                elif download_pdfs and int(max_pdfs_per_ticker) > 0:
                    pdf_limit_hit = True

                # Se já existe:
                if status["exists"]:
                    if (not status["has_text"]) and texto:
                        if _update_doc_text(conn, int(status["id"]), texto):
                            updated_text += 1
                    else:
                        skipped += 1
                    continue

                # Novo documento
                doc_id = _insert_doc(
                    conn,
                    ticker=tk,
                    titulo=titulo or "Documento CVM/IPE",
                    url=url,
                    fonte="CVM/IPE",
                    tipo=tipo,
                    data=dt,
                    texto=texto,
                    doc_hash=doc_hash,
                )
                if doc_id is None:
                    skipped += 1
                else:
                    inserted += 1

                if sleep_s:
                    time.sleep(float(sleep_s))

            out_stats[tk] = {
                "matched": matched,
                "considered": considered,
                "inserted": inserted,
                "skipped": skipped,
                "updated_text": updated_text,
                "pdf_fetched": pdf_fetched,
                "pdf_text_ok": pdf_text_ok,
                # D) auditoria
                "selected_strategic": selected_strategic if strategic_only else None,
                "fallback_used": fallback_used if strategic_only else None,
                "top_selected": audit_top,
            }

    ok = (len(out_errors) == 0)
    result = {"ok": ok, "stats": out_stats, "errors": out_errors}
    _log(
        "INFO" if ok else "WARN",
        "summary",
        ok=ok,
        tickers=len(tickers_n),
        tickers_with_stats=len(out_stats),
        errors=len(out_errors),
        inserted=sum(int(v.get("inserted", 0)) for v in out_stats.values()),
        skipped=sum(int(v.get("skipped", 0)) for v in out_stats.values()),
        updated_text=sum(int(v.get("updated_text", 0)) for v in out_stats.values()),
        pdf_fetched=sum(int(v.get("pdf_fetched", 0)) for v in out_stats.values()),
        pdf_text_ok=sum(int(v.get("pdf_text_ok", 0)) for v in out_stats.values()),
    )
    return result
