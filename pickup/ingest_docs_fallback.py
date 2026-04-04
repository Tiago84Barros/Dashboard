from __future__ import annotations
"""
pickup/ingest_docs_fallback.py
------------------------------
Pipeline de ingestão em camadas (A -> B -> C) para abastecer o RAG do Patch 6.

A: CVM/IPE (dados abertos/ENET) via pickup.ingest_docs_cvm_ipe.ingest_ipe_for_tickers
B: RI (domínio oficial) via tabela public.ri_map (ticker -> ri_url)
C: Fontes secundárias (mídia/mercado) **desativado por padrão** (exige curadoria de domínios).

Este módulo foi pensado para ser chamado dentro do Streamlit (page.patch6_teste),
mas também pode ser usado em scripts.

Requisitos:
- requests
- sqlalchemy
- core.db_loader.get_supabase_engine
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
from contextlib import nullcontext
import json
import re
import time
import urllib.parse

import requests
from sqlalchemy import text

try:
    from auditoria_dados.ingestion_log import IngestionLog as _IngestionLog
except ImportError:
    _IngestionLog = None

from core.docs_corporativos_store import DEFAULT_CHUNKING_VERSION
from core.docs_corporativos_store import persist_document_bundle
from core.db_loader import get_supabase_engine
from core.ticker_utils import normalize_ticker

_RUN_LOG = None
_CHUNKING_VERSION = f"fallback::{DEFAULT_CHUNKING_VERSION}"
_EXTRACTION_VERSIONS = {
    "ri_html": "ri_html_text_v1",
    "ri_pdf": "ri_pdf_text_v1",
    "ri_stub": "ri_stub_v1",
}


def _log(level: str, event: str, **fields: Any) -> None:
    if _RUN_LOG:
        _RUN_LOG.log(level, event, **fields)
        return
    payload = {"pipeline": "docs_fallback", "level": level, "event": event}
    payload.update(fields)
    print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)

# ---------------------------------
# Util
# ---------------------------------

def _norm_ticker(t: str) -> str:
    return normalize_ticker(t)

def _same_domain(u1: str, u2: str) -> bool:
    try:
        a = urllib.parse.urlparse(u1)
        b = urllib.parse.urlparse(u2)
        return (a.netloc or "").lower() == (b.netloc or "").lower()
    except Exception:
        return False

def _strip_html(html: str) -> str:
    s = (html or "")
    # remove scripts/styles
    s = re.sub(r"(?is)<script.*?>.*?</script>", " ", s)
    s = re.sub(r"(?is)<style.*?>.*?</style>", " ", s)
    # remove tags
    s = re.sub(r"(?is)<[^>]+>", " ", s)
    # decode entities minimally
    s = s.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    s = re.sub(r"\s+", " ", s).strip()
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

# ---------------------------------
# Supabase: RI map
# ---------------------------------

def fetch_ri_map(tickers: Sequence[str], table: str = "public.ri_map") -> Dict[str, str]:
    """Busca ri_url para os tickers informados. Espera colunas: ticker, ri_url."""
    tks = [_norm_ticker(x) for x in (tickers or []) if str(x).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {}

    engine = get_supabase_engine()
    sql = text(f"""
        select upper(ticker) as ticker, ri_url
        from {table}
        where upper(ticker) = any(:tks)
          and coalesce(trim(ri_url),'') <> ''
    """)
    out: Dict[str, str] = {}
    with engine.begin() as conn:
        rows = conn.execute(sql, {"tks": tks}).fetchall()
        for r in rows:
            tk = (r[0] or "").upper()
            url = (r[1] or "").strip()
            if tk and url:
                out[tk] = url
    return out

# ---------------------------------
# Inserção genérica em docs_corporativos
# ---------------------------------

def _sha256(s: str) -> str:
    import hashlib
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _find_existing_doc_id(
    conn,
    *,
    ticker: str,
    data: Optional[str],
    fonte: str,
    tipo: str,
    titulo: str,
    url: str,
) -> Optional[int]:
    row = conn.execute(
        text(
            """
            select id
            from public.docs_corporativos
            where upper(ticker) = upper(:ticker)
              and coalesce(data::date::text, '') = coalesce(:data, '')
              and lower(coalesce(fonte, '')) = lower(:fonte)
              and lower(coalesce(tipo, '')) = lower(:tipo)
              and lower(coalesce(titulo, '')) = lower(:titulo)
              and lower(coalesce(url, '')) = lower(:url)
            limit 1
            """
        ),
        {
            "ticker": _norm_ticker(ticker),
            "data": (data or "").strip() or None,
            "fonte": (fonte or "").strip() or "UNKNOWN",
            "tipo": (tipo or "").strip() or "doc",
            "titulo": (titulo or "").strip(),
            "url": _canonical_url(url),
        },
    ).fetchone()
    return int(row[0]) if row else None

def upsert_doc(
    *,
    ticker: str,
    data: Optional[str],
    fonte: str,
    tipo: str,
    titulo: str,
    url: str,
    raw_text: str,
    extraction_version: str = _EXTRACTION_VERSIONS["ri_stub"],
    is_stub: bool = False,
    chunk_size: int = 1000,
    chunk_overlap: int = 150,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Insere documento via store institucional."""
    tk = _norm_ticker(ticker)
    if not tk:
        return {"ok": False, "error": "ticker_vazio"}

    fonte = (fonte or "").strip() or "UNKNOWN"
    tipo = (tipo or "").strip() or "doc"
    titulo = (titulo or "").strip()
    url = _canonical_url(url)
    raw_text = (raw_text or "").strip()

    engine = get_supabase_engine()
    with engine.begin() as conn:
        persisted = persist_document_bundle(
            conn,
            ticker=tk,
            titulo=titulo,
            url=url,
            fonte=fonte,
            tipo=tipo,
            data=data,
            texto=raw_text,
            chunking_version=_CHUNKING_VERSION,
            extraction_version=extraction_version,
            run_id=run_id,
            is_stub=is_stub,
            chunk_size=int(chunk_size),
            chunk_overlap=int(chunk_overlap),
        )

    inserted = bool(persisted.get("inserted"))
    _log(
        "INFO" if inserted else "WARN",
        "doc_persisted",
        ticker=tk,
        fonte=fonte,
        tipo=tipo,
        data=data,
        inserted=inserted,
        updated_text=bool(persisted.get("updated_text")),
        duplicate=bool(persisted.get("duplicate")),
        chunks_inserted=int(persisted.get("chunks_inserted", 0) or 0),
        extraction_version=extraction_version,
        is_stub=bool(persisted.get("stub")),
        raw_text_chars=len(raw_text),
        url=url[:300],
    )
    reason = "existing_metadata" if bool(persisted.get("duplicate")) else None
    return {
        "ok": True,
        "inserted": inserted,
        "updated_text": bool(persisted.get("updated_text")),
        "duplicate": bool(persisted.get("duplicate")),
        "doc_hash": str(persisted.get("doc_hash") or ""),
        "chunks_inserted": int(persisted.get("chunks_inserted", 0) or 0),
        "stub": bool(persisted.get("stub")),
        "reason": reason,
    }

# ---------------------------------
# B: Ingestão via RI (crawler simples)
# ---------------------------------

DEFAULT_KEYWORDS = (
    "resultados", "releases", "release", "apresenta", "apresentação",
    "fato", "relevante", "comunicado", "guidance", "outlook",
    "capex", "invest", "expans", "projeto", "estrat", "plano"
)

def ingest_from_ri(
    ticker: str,
    ri_url: str,
    *,
    max_pages: int = 30,
    max_depth: int = 2,
    timeout: int = 25,
    sleep_s: float = 0.0,
    keywords: Sequence[str] = DEFAULT_KEYWORDS,
) -> Dict[str, Any]:
    """Crawler minimalista para RI (HTML + PDFs)."""
    tk = _norm_ticker(ticker)
    if not tk:
        return {"ok": False, "error": "ticker_vazio"}

    start = (ri_url or "").strip()
    if not start:
        return {"ok": False, "error": "ri_url_vazio"}

    parsed = urllib.parse.urlparse(start)
    if not parsed.scheme:
        start = "https://" + start.lstrip("/")
    start = _canonical_url(start)

    domain = urllib.parse.urlparse(start).netloc.lower()

    def allowed(u: str) -> bool:
        try:
            pu = urllib.parse.urlparse(u)
            if (pu.netloc or "").lower() != domain:
                return False
            if pu.scheme not in ("http", "https"):
                return False
            return True
        except Exception:
            return False

    kw = tuple([k.lower() for k in (keywords or []) if str(k).strip()])
    seen = set()
    queue: List[Tuple[str,int]] = [(start, 0)]
    pages_fetched = 0
    inserted = 0
    updated_text = 0
    skipped = 0
    pdfs = 0
    stubs = 0
    chunks_generated = 0
    failures = 0
    errors: List[str] = []

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; patch6-ingestor/1.0)"})
    duplicates = 0
    _log("INFO", "ri_start", ticker=tk, ri_url=start, max_pages=max_pages, max_depth=max_depth)

    def score_url(u: str) -> int:
        ul = u.lower()
        s = 0
        for k in kw:
            if k in ul:
                s += 2
        if ul.endswith(".pdf"):
            s += 1
        return s

    while queue and pages_fetched < max_pages:
        url, depth = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)

        try:
            r = session.get(url, timeout=timeout, allow_redirects=True)
            pages_fetched += 1
            ct = (r.headers.get("Content-Type") or "").lower()

            final_url = _canonical_url(r.url)
            if not _same_domain(final_url, start):
                skipped += 1
                continue

            if "application/pdf" in ct or final_url.lower().endswith(".pdf"):
                pdfs += 1
                raw_text = ""
                try:
                    import io
                    from PyPDF2 import PdfReader  # type: ignore
                    reader = PdfReader(io.BytesIO(r.content))
                    chunks = []
                    for page in reader.pages[:20]:
                        t = page.extract_text() or ""
                        if t.strip():
                            chunks.append(t.strip())
                    raw_text = "\n".join(chunks).strip()
                except Exception:
                    raw_text = ""

                titulo = f"PDF RI {tk}"
                if raw_text:
                    resp = upsert_doc(
                        ticker=tk,
                        data=None,
                        fonte="RI",
                        tipo="pdf",
                        titulo=titulo,
                        url=final_url,
                        raw_text=raw_text,
                        extraction_version=_EXTRACTION_VERSIONS["ri_pdf"],
                        is_stub=False,
                        run_id=getattr(_RUN_LOG, "run_id", None),
                    )
                else:
                    resp = upsert_doc(
                        ticker=tk,
                        data=None,
                        fonte="RI",
                        tipo="pdf_meta",
                        titulo=titulo,
                        url=final_url,
                        raw_text=f"[PDF sem texto extraível automaticamente] {final_url}",
                        extraction_version=_EXTRACTION_VERSIONS["ri_stub"],
                        is_stub=True,
                        run_id=getattr(_RUN_LOG, "run_id", None),
                    )
                    stubs += 1
                if resp.get("inserted"):
                    inserted += 1
                if resp.get("updated_text"):
                    updated_text += 1
                chunks_generated += int(resp.get("chunks_inserted", 0) or 0)
                if not resp.get("inserted") and not resp.get("updated_text"):
                    skipped += 1
                    if resp.get("reason") == "existing_metadata":
                        duplicates += 1

            else:
                html = r.text or ""
                text_clean = _strip_html(html)
                if len(text_clean) < 400:
                    skipped += 1
                else:
                    titulo = f"RI {tk}: {urllib.parse.urlparse(final_url).path[:80]}"
                    resp = upsert_doc(
                        ticker=tk,
                        data=None,
                        fonte="RI",
                        tipo="html",
                        titulo=titulo,
                        url=final_url,
                        raw_text=text_clean,
                        extraction_version=_EXTRACTION_VERSIONS["ri_html"],
                        is_stub=False,
                        run_id=getattr(_RUN_LOG, "run_id", None),
                    )
                    if resp.get("inserted"):
                        inserted += 1
                    if resp.get("updated_text"):
                        updated_text += 1
                    chunks_generated += int(resp.get("chunks_inserted", 0) or 0)
                    if not resp.get("inserted") and not resp.get("updated_text"):
                        skipped += 1
                        if resp.get("reason") == "existing_metadata":
                            duplicates += 1

                if depth < max_depth:
                    hrefs = re.findall(r'(?is)\bhref\s*=\s*["\']([^"\']+)["\']', html)
                    links = []
                    for h in hrefs:
                        h = (h or "").strip()
                        if not h or h.startswith("#") or h.lower().startswith("mailto:") or h.lower().startswith("javascript:"):
                            continue
                        absu = urllib.parse.urljoin(final_url, h)
                        if allowed(absu) and absu not in seen:
                            links.append(absu)

                    links = sorted(list(dict.fromkeys(links)), key=score_url, reverse=True)
                    for absu in links[:40]:
                        queue.append((absu, depth + 1))

            if sleep_s and float(sleep_s) > 0:
                time.sleep(float(sleep_s))

        except Exception as e:
            failures += 1
            errors.append(f"{type(e).__name__}: {e}")
            _log("WARN", "ri_page_failed", ticker=tk, url=url[:300], depth=depth, error=str(e))

    result = {
        "ok": True,
        "ticker": tk,
        "ri_url": start,
        "domain": domain,
        "pages_fetched": pages_fetched,
        "inserted": inserted,
        "updated_text": updated_text,
        "skipped": skipped,
        "duplicates": duplicates,
        "pdfs": pdfs,
        "stubs": stubs,
        "chunks_generated": chunks_generated,
        "failures": failures,
        "errors": errors[:10],
    }
    if _RUN_LOG:
        _RUN_LOG.add_source_metrics(
            source="RI",
            ticker=tk,
            documents_read=pages_fetched,
            documents_inserted=inserted,
            duplicates=duplicates,
            chunks_generated=chunks_generated,
            stubs=stubs,
            failures=failures,
        )
    _log("INFO", "ri_summary", **result)
    return result

# ---------------------------------
# Pipeline A->B->C
# ---------------------------------

def ingest_strategy_for_tickers(
    tickers: Sequence[str],
    *,
    anos: int = 1,
    max_docs_por_ticker: int = 12,
    sleep_s: float = 0.0,
    strategy: str = "A",
    ri_map_table: str = "public.ri_map",
    max_runtime_s: float = 25.0,
    enable_c: bool = False,
) -> Dict[str, Any]:
    """Executa ingestão em camadas A/B/C e retorna um dicionário unificado."""
    global _RUN_LOG
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    run_ctx = _IngestionLog("docs_fallback") if _IngestionLog else nullcontext(None)
    if _IngestionLog and run_ctx:
        run_ctx.set_params(
            {
                "tickers": tks,
                "anos": int(anos),
                "max_docs_por_ticker": int(max_docs_por_ticker),
                "strategy": strategy,
                "max_runtime_s": float(max_runtime_s),
                "enable_c": bool(enable_c),
                "chunking_version": _CHUNKING_VERSION,
            }
        )

    with run_ctx as run:
        _RUN_LOG = run
        _log(
            "INFO",
            "start",
            tickers=len(tks),
            anos=anos,
            max_docs_por_ticker=max_docs_por_ticker,
            strategy=strategy,
            enable_c=enable_c,
        )
        if not tks:
            result = {"ok": False, "run_id": getattr(run, "run_id", None), "error": "Lista vazia", "by_ticker": {}}
            if run:
                run.add_error("Lista vazia")
            _log("ERROR", "summary", **result)
            _RUN_LOG = None
            return result

        out: Dict[str, Any] = {"ok": True, "run_id": getattr(run, "run_id", None), "by_ticker": {}}

        if "A" in strategy:
            try:
                from pickup.ingest_docs_cvm_ipe import ingest_ipe_for_tickers  # type: ignore
                resA = ingest_ipe_for_tickers(
                    tks,
                    window_months=max(int(anos) * 12, 1),
                    max_docs_per_ticker=int(max_docs_por_ticker),
                    sleep_s=float(sleep_s),
                    max_runtime_s=float(max_runtime_s),
                )
            except Exception as e:
                resA = {"ok": False, "stats": {}, "errors": {"__A__": f"{type(e).__name__}: {e}"}}
                _log("WARN", "layer_a_failed", error=str(e))

            for tk in tks:
                out["by_ticker"].setdefault(tk, {})
                stats = (resA.get("stats") or {}).get(tk)
                err = (resA.get("errors") or {}).get(tk)
                if not err:
                    errs = (resA.get("errors") or {})
                    for k in ("__ipe__", "__map__", "__A__", "__all__"):
                        if k in errs and errs.get(k):
                            err = errs.get(k)
                            break
                out["by_ticker"][tk]["A"] = {"stats": stats, "error": err}

        if "B" in strategy:
            ri_map = fetch_ri_map(tks, table=ri_map_table)
            for tk in tks:
                out["by_ticker"].setdefault(tk, {})

                need_b = True
                if "A" in out["by_ticker"][tk]:
                    a_stats = out["by_ticker"][tk]["A"].get("stats") or {}
                    if isinstance(a_stats, dict) and int(a_stats.get("inserted") or 0) > 0:
                        need_b = False

                if not need_b:
                    out["by_ticker"][tk]["B"] = {"skipped": True, "reason": "A já inseriu docs"}
                    _log("INFO", "layer_b_skipped", ticker=tk, reason="A já inseriu docs")
                    continue

                ri_url = ri_map.get(tk, "")
                if not ri_url:
                    out["by_ticker"][tk]["B"] = {"ok": False, "error": "ri_url_missing (preencha public.ri_map)"}
                    out["ok"] = False
                    _log("WARN", "layer_b_missing_ri_url", ticker=tk)
                    continue

                out["by_ticker"][tk]["B"] = ingest_from_ri(tk, ri_url, max_pages=30, max_depth=2, sleep_s=sleep_s)

        if "C" in strategy:
            out["C"] = {"enabled": bool(enable_c), "note": "Plano C é opcional e exige curadoria de domínios."}

        _log(
            "INFO" if out.get("ok") else "WARN",
            "summary",
            ok=out.get("ok"),
            run_id=out.get("run_id"),
            tickers=len(tks),
            tickers_with_result=len(out["by_ticker"]),
            duplicates=sum(int((v.get("B") or {}).get("duplicates", 0)) for v in out["by_ticker"].values()),
        )
        _RUN_LOG = None
        return out
