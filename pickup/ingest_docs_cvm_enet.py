from __future__ import annotations

"""
pickup/ingest_docs_cvm_enet.py
-----------------------------
Ingestão de documentos corporativos via CVM ENET (Consulta Externa) usando Código CVM.

Estratégia (Opção A):
1) resolve ticker -> codigo_cvm via tabela public.cvm_to_ticker (colunas "Ticker" e "CVM")
2) consulta ENET (JSON) por Código CVM e janela de datas
3) coleta metadados + baixa PDF/HTML quando disponível
4) extrai texto (PDF/HTML) e salva:
   - public.docs_corporativos
   - public.docs_corporativos_chunks (opcional)

Observações:
- Endpoint ENET muda; por isso este módulo tem "payload fallbacks".
- Extração de PDF: tenta pypdf, depois PyPDF2.
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
import hashlib
import json
import re
import time

import pandas as pd
import requests
import streamlit as st
from sqlalchemy import text

from core.db_loader import get_supabase_engine
from core.ticker_utils import normalize_ticker


def _log(level: str, event: str, **fields: Any) -> None:
    payload = {"pipeline": "docs_enet", "level": level, "event": event}
    payload.update(fields)
    print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)


# ─────────────────────────────
# Utils
# ─────────────────────────────
def _norm_ticker(t: str) -> str:
    return normalize_ticker(t)

def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

def _clean_text(s: str) -> str:
    s = (s or "").replace("\x00", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _strip_html(html: str) -> str:
    # remove scripts/styles
    x = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html or "")
    # remove tags
    x = re.sub(r"(?is)<[^>]+>", " ", x)
    # html entities básicos
    x = x.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return _clean_text(x)

def _chunk_text(texto: str, chunk_chars: int = 1500, overlap: int = 200) -> List[str]:
    t = (texto or "").strip()
    if not t:
        return []
    t = t.replace("\r\n", "\n")
    out = []
    i = 0
    n = len(t)
    while i < n:
        j = min(n, i + chunk_chars)
        out.append(t[i:j])
        if j >= n:
            break
        i = max(0, j - overlap)
    return out


# ─────────────────────────────
# Supabase: ticker -> código CVM
# ─────────────────────────────
def _get_codigo_cvm_por_tickers(tickers: Sequence[str]) -> Dict[str, int]:
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {}

    eng = get_supabase_engine()
    sql = text("""
        select upper("Ticker") as ticker, "CVM" as codigo_cvm
        from public.cvm_to_ticker
        where upper("Ticker") = any(:tks)
    """)
    with eng.begin() as conn:
        rows = conn.execute(sql, {"tks": tks}).fetchall()

    out: Dict[str, int] = {}
    for r in rows:
        tk = str(r[0]).upper().strip()
        cvm = int(r[1])
        out[tk] = cvm
    return out


# ─────────────────────────────
# ENET endpoints
# ─────────────────────────────
ENET_JSON = "https://www.rad.cvm.gov.br/ENET/ConsultaExternaCVM/ConsultaExternaCVM.aspx/ConsultarDocumentos"
ENET_DOWNLOAD_BASE = "https://www.rad.cvm.gov.br/ENET/"  # muitos links são relativos

def _post_enet(payload: Dict[str, Any], timeout: int = 45) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }
    r = requests.post(ENET_JSON, json=payload, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _parse_enet_response(resp: Dict[str, Any]) -> Dict[str, Any]:
    """
    A resposta frequentemente vem em resp['d'] como dict ou como string JSON.
    """
    d = resp.get("d") if isinstance(resp, dict) else None
    if isinstance(d, str):
        try:
            d2 = json.loads(d)
            if isinstance(d2, dict):
                d = d2
        except Exception:
            pass
    return d if isinstance(d, dict) else {}

def _build_download_url(x: str) -> str:
    u = (x or "").strip()
    if not u:
        return ""
    if u.startswith("http://") or u.startswith("https://"):
        return u
    # relativo (ex: "frmDownloadDocumento.aspx?Codigo=...")
    return ENET_DOWNLOAD_BASE.rstrip("/") + "/" + u.lstrip("/")


def _consultar_documentos_por_cvm(
    codigo_cvm: int,
    *,
    dt_ini: str,
    dt_fim: str,
    pagina: int,
    registros_por_pagina: int = 50,
    categorias: Optional[Sequence[str]] = None,
    tipos: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Tenta múltiplos formatos de payload (ENET muda muito).
    Retorna 'registros' se encontrar.
    """
    cat = list(categorias or [])
    tip = list(tipos or [])

    # Fallbacks de payload:
    payloads = []

    # payload 1 (mais comum em scrapers)
    payloads.append({
        "data": {
            "parametros": {
                "CodigoCVM": str(codigo_cvm),
                "CodigoInstituicao": str(codigo_cvm),
                "DataIni": dt_ini,
                "DataFim": dt_fim,
                "Categoria": "" if not cat else cat[0],
                "TipoDocumento": "" if not tip else tip[0],
                "PalavraChave": "",
            },
            "pagina": pagina,
            "registrosPorPagina": registros_por_pagina,
        }
    })

    # payload 2 (variações de nomes)
    payloads.append({
        "data": {
            "parametros": {
                "CodigoCVM": str(codigo_cvm),
                "DataIni": dt_ini,
                "DataFim": dt_fim,
                "Categoria": "" if not cat else cat[0],
                "TipoDocumento": "" if not tip else tip[0],
            },
            "pagina": pagina,
            "registrosPorPagina": registros_por_pagina,
        }
    })

    # payload 3 (sem filtros; mais “cru”)
    payloads.append({
        "data": {
            "parametros": {
                "CodigoCVM": str(codigo_cvm),
                "DataIni": dt_ini,
                "DataFim": dt_fim,
            },
            "pagina": pagina,
            "registrosPorPagina": registros_por_pagina,
        }
    })

    last_err = None
    for p in payloads:
        try:
            resp = _post_enet(p)
            d = _parse_enet_response(resp)
            regs = d.get("registros") or d.get("Registros") or d.get("records")
            if isinstance(regs, list):
                return [x for x in regs if isinstance(x, dict)]
        except Exception as e:
            last_err = e

    if last_err:
        raise last_err
    return []


# ─────────────────────────────
# Download + extração de texto
# ─────────────────────────────
def _download_bytes(url: str, timeout: int = 60) -> Tuple[bytes, str]:
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    ctype = (r.headers.get("Content-Type") or "").lower()
    return r.content, ctype

def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    # tenta pypdf
    try:
        from pypdf import PdfReader  # type: ignore
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for p in reader.pages:
            t = p.extract_text() or ""
            if t.strip():
                parts.append(t)
        return _clean_text("\n".join(parts))
    except Exception:
        pass

    # tenta PyPDF2
    try:
        from PyPDF2 import PdfReader  # type: ignore
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for p in reader.pages:
            t = p.extract_text() or ""
            if t.strip():
                parts.append(t)
        return _clean_text("\n".join(parts))
    except Exception:
        return ""


def _extract_text_from_url(url: str) -> Tuple[str, str]:
    """
    Retorna (texto, modo)
    modo: 'pdf' | 'html' | 'unknown' | 'error'
    """
    if not url:
        return "", "unknown"
    try:
        b, ctype = _download_bytes(url)
        if "pdf" in ctype or url.lower().endswith(".pdf"):
            t = _extract_text_from_pdf(b)
            return t, "pdf"
        # tenta tratar como html/text
        try:
            html = b.decode("utf-8", errors="ignore")
        except Exception:
            html = ""
        if html and ("<html" in html.lower() or "<body" in html.lower() or "</" in html):
            return _strip_html(html), "html"
        # fallback
        return _clean_text(html), "unknown"
    except Exception:
        _log("WARN", "extract_text_failed", url=url[:300])
        return "", "error"


# ─────────────────────────────
# Upsert doc + chunks
# ─────────────────────────────
def _upsert_doc_and_chunks(
    *,
    ticker: str,
    data: Optional[str],
    fonte: str,
    tipo: str,
    categoria: str,
    titulo: str,
    url: str,
    raw_text: str,
    chunk_chars: int = 1500,
    overlap: int = 200,
) -> Tuple[bool, str]:
    tk = _norm_ticker(ticker)
    if not tk:
        return False, ""

    fonte = (fonte or "CVM").strip()
    tipo = (tipo or "enet").strip()
    categoria = (categoria or "").strip()
    titulo = (titulo or "").strip()
    url = (url or "").strip()
    raw_text = (raw_text or "").strip()

    doc_hash = _sha256("|".join([tk, fonte, tipo, categoria, titulo, url, raw_text]))

    eng = get_supabase_engine()

    sql_doc = text("""
        INSERT INTO public.docs_corporativos (ticker, data, fonte, tipo, titulo, url, raw_text, doc_hash)
        VALUES (:ticker, :data, :fonte, :tipo, :titulo, :url, :raw_text, :doc_hash)
        ON CONFLICT (doc_hash) DO NOTHING
        RETURNING id
    """)

    with eng.begin() as conn:
        res = conn.execute(sql_doc, {
            "ticker": tk,
            "data": data,
            "fonte": fonte,
            "tipo": f"{tipo}:{categoria}" if categoria else tipo,
            "titulo": titulo,
            "url": url,
            "raw_text": raw_text,
            "doc_hash": doc_hash,
        })
        row = res.first()
        if row is None:
            return False, doc_hash

        doc_id = int(row[0])
        chunks = _chunk_text(raw_text, chunk_chars=int(chunk_chars), overlap=int(overlap))
        if not chunks:
            return True, doc_hash

        sql_chunk = text("""
            INSERT INTO public.docs_corporativos_chunks (doc_id, ticker, chunk_index, chunk_text, chunk_hash)
            VALUES (:doc_id, :ticker, :chunk_index, :chunk_text, :chunk_hash)
            ON CONFLICT (chunk_hash) DO NOTHING
        """)

        for i, ch in enumerate(chunks):
            ch_clean = (ch or "").strip()
            if not ch_clean:
                continue
            ch_hash = _sha256("|".join([str(doc_id), tk, str(i), ch_clean]))
            conn.execute(sql_chunk, {
                "doc_id": doc_id,
                "ticker": tk,
                "chunk_index": int(i),
                "chunk_text": ch_clean,
                "chunk_hash": ch_hash,
            })

    return True, doc_hash


# ─────────────────────────────
# Public API
# ─────────────────────────────
def ingest_enet_for_tickers(
    tickers: Sequence[str],
    *,
    anos: int = 2,
    max_docs_por_ticker: int = 30,
    sleep_s: float = 0.15,
    chunk_chars: int = 1500,
    overlap: int = 200,
    # filtros “estratégicos”
    categorias: Optional[Sequence[str]] = None,
    tipos: Optional[Sequence[str]] = None,
    baixar_e_extrair: bool = True,
) -> Dict[str, Any]:
    """
    Ingestão via ENET por código CVM.

    Retorno:
      {
        "ok": bool,
        "stats": { "TICKER": {"seen":N,"inserted":M,"skipped":K,"downloaded":D,"text_ok":T} },
        "errors": { "TICKER": "msg" },
        "mapping_missing": [tickers_sem_cvm]
      }
    """
    _log(
        "INFO",
        "start",
        tickers=len(tickers),
        anos=anos,
        max_docs_por_ticker=max_docs_por_ticker,
        baixar_e_extrair=baixar_e_extrair,
    )
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        result = {"ok": False, "stats": {}, "errors": {"__all__": "Lista de tickers vazia."}, "mapping_missing": []}
        _log("ERROR", "summary", **result)
        return result

    map_cvm = _get_codigo_cvm_por_tickers(tks)
    missing = [t for t in tks if t not in map_cvm]

    ano_fim = int(pd.Timestamp.utcnow().year)
    ano_ini = int(ano_fim - max(0, int(anos)))

    dt_ini = f"01/01/{ano_ini}"
    dt_fim = f"31/12/{ano_fim}"

    # filtros default (documentos realmente estratégicos)
    cat_default = [
        "Fato Relevante",
        "Comunicado ao Mercado",
        "Aviso aos Acionistas",
        "Assembleia",
        "Edital",
        "Release",
        "Apresentação",
    ]
    categorias = list(categorias) if categorias is not None else cat_default
    tipos = list(tipos) if tipos is not None else []

    stats: Dict[str, Dict[str, int]] = {}
    errors: Dict[str, str] = {}

    for tk in tks:
        stats[tk] = {"seen": 0, "inserted": 0, "skipped": 0, "downloaded": 0, "text_ok": 0}
        if tk not in map_cvm:
            errors[tk] = "Sem código CVM em public.cvm_to_ticker"
            _log("WARN", "missing_cvm_mapping", ticker=tk)
            continue

        codigo_cvm = int(map_cvm[tk])

        try:
            all_docs: List[Dict[str, Any]] = []

            # paginação curta (aumente se quiser)
            for page in range(1, 6):
                regs = _consultar_documentos_por_cvm(
                    codigo_cvm,
                    dt_ini=dt_ini,
                    dt_fim=dt_fim,
                    pagina=page,
                    registros_por_pagina=50,
                    categorias=categorias,
                    tipos=tipos,
                )
                if not regs:
                    break
                all_docs.extend(regs)
                if len(regs) < 50:
                    break
                if sleep_s:
                    time.sleep(float(sleep_s))

            stats[tk]["seen"] = int(len(all_docs))

            # corta para max_docs_por_ticker
            for doc in all_docs[: int(max_docs_por_ticker)]:
                # campos variam muito; tentamos o máximo
                assunto = str(doc.get("Assunto") or doc.get("DescricaoAssunto") or doc.get("Titulo") or "").strip()
                categoria = str(doc.get("Categoria") or doc.get("categoria") or "").strip()
                tipo_doc = str(doc.get("TipoDocumento") or doc.get("Tipo") or doc.get("tipo") or "").strip()

                # datas
                data_iso = None
                for k in ("DataEntrega", "DataReferencia", "DataDocumento", "DataEnvio", "Data"):
                    v = doc.get(k)
                    if isinstance(v, str) and v.strip():
                        d = pd.to_datetime(v, errors="coerce", dayfirst=True)
                        if pd.notna(d):
                            data_iso = d.date().isoformat()
                            break

                # link download/url
                url = (
                    doc.get("LinkDownload")
                    or doc.get("Link_Download")
                    or doc.get("Link")
                    or doc.get("Url")
                    or doc.get("url")
                    or ""
                )
                url = _build_download_url(str(url))

                raw_text = ""

                if baixar_e_extrair and url:
                    txt, mode = _extract_text_from_url(url)
                    if txt:
                        raw_text = txt
                        stats[tk]["downloaded"] += 1
                        stats[tk]["text_ok"] += 1
                    else:
                        # sem texto extraível, ainda assim armazenamos metadados + “stub”
                        stats[tk]["downloaded"] += 1
                        raw_text = _clean_text(f"{assunto}\n{categoria}\n{tipo_doc}\nURL: {url}")
                else:
                    # fallback mínimo
                    raw_text = _clean_text(f"{assunto}\n{categoria}\n{tipo_doc}\nURL: {url}")

                if not raw_text:
                    stats[tk]["skipped"] += 1
                    continue

                inserted, _ = _upsert_doc_and_chunks(
                    ticker=tk,
                    data=data_iso,
                    fonte="CVM",
                    tipo="enet",
                    categoria=categoria or tipo_doc,
                    titulo=assunto,
                    url=url,
                    raw_text=raw_text,
                    chunk_chars=int(chunk_chars),
                    overlap=int(overlap),
                )
                if inserted:
                    stats[tk]["inserted"] += 1
                else:
                    stats[tk]["skipped"] += 1

                if sleep_s:
                    time.sleep(float(sleep_s))

        except Exception as e:
            errors[tk] = f"{type(e).__name__}: {e}"
            _log("WARN", "ticker_failed", ticker=tk, error=str(e))
    result = {"ok": (len(errors) == 0), "stats": stats, "errors": errors, "mapping_missing": missing}
    _log(
        "INFO" if result["ok"] else "WARN",
        "summary",
        ok=result["ok"],
        tickers=len(tks),
        mapping_missing=len(missing),
        inserted=sum(v["inserted"] for v in stats.values()),
        skipped=sum(v["skipped"] for v in stats.values()),
        downloaded=sum(v["downloaded"] for v in stats.values()),
        text_ok=sum(v["text_ok"] for v in stats.values()),
        errors=len(errors),
    )
    return result
