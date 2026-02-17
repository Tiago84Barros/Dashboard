from __future__ import annotations
"""
pickup/ingest_docs_cvm_ipe.py  (Patch6 - CVM/IPE)
-----------------------------------------------
Ingestão de documentos do dataset público da CVM (IPE) para a tabela public.docs_corporativos.

Objetivo do Patch 6:
- Capturar intenção estratégica futura (guidance, capex, expansão, M&A, desalavancagem, alocação de capital etc.)
- Alimentar RAG (chunks + embeddings) em docs_corporativos_chunks.

Notas importantes:
- O CSV do IPE muda nomes de colunas ao longo do tempo. Este módulo é tolerante a variações:
  CODIGO_CVM / CD_CVM / CVM, DATA_ENTREGA / DT_RECEB / DT_REFER / DATA_REFERENCIA etc.
- PDFs: por padrão, **baixamos e tentamos extrair texto** (sem OCR). Muitos PDFs da CVM possuem texto selecionável.
  Se não houver texto, armazenamos o metadado + link mesmo assim.
- Performance: janela padrão é 12 meses e com limites por ticker. Todos os limites são configuráveis pelo chamador.

Requer:
- public.cvm_to_ticker (colunas: CVM int, Ticker text)  (ou ajuste get_cvm_codes_for_tickers)
- public.docs_corporativos com campos (ticker,titulo,url,fonte,tipo,data,texto,doc_hash,created_at...)
- public.docs_corporativos_chunks com campos (doc_id,ticker,chunk_index,chunk_text,embedding,chunk_hash,created_at...)
"""
from typing import Any, Dict, List, Optional, Sequence, Tuple
import hashlib
import io
import re
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from sqlalchemy import text

from core.db_loader import get_supabase_engine


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()

def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

def _clean_text(s: str) -> str:
    s = (s or "").replace("\x00", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _chunk_text(texto: str, chunk_chars: int = 1500, overlap: int = 200) -> List[str]:
    t = (texto or "").strip()
    if not t:
        return []
    t = t.replace("\r\n", "\n")
    out: List[str] = []
    i, n = 0, len(t)
    while i < n:
        j = min(n, i + chunk_chars)
        out.append(t[i:j])
        if j >= n:
            break
        i = max(0, j - overlap)
    return out

def _pick_col(cols: Sequence[str], *candidates: str) -> Optional[str]:
    """Retorna o primeiro candidato existente em cols (case-insensitive)."""
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None

def _parse_date(val: Any) -> Optional[pd.Timestamp]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return pd.to_datetime(val, errors="coerce", dayfirst=True)
    except Exception:
        return None

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _now_minus_months(months: int) -> datetime:
    # Aproximação segura (30 dias/mês) para filtro rápido no ingest
    return _utcnow() - timedelta(days=int(months) * 30)

def _looks_like_pdf(url: str) -> bool:
    """Heurística para decidir se vale tentar baixar como PDF.

    Observação: links da CVM (ENET/frmDownloadDocumento.aspx) NÃO terminam em .pdf.
    Então tratamos esses endpoints como PDF prováveis.
    """
    u = (url or "").lower().strip()
    if not u:
        return False
    if u.endswith(".pdf") or "pdf" in u:
        return True
    # CVM ENET / RAD download endpoints (comuns no IPE)
    if "frmdownloaddocumento.aspx" in u:
        return True
    if "www.rad.cvm.gov.br" in u and "enet" in u and "download" in u:
        return True
    return False


def _is_pdf_bytes(b: bytes) -> bool:
    return bool(b) and b[:4] == b"%PDF"


# ──────────────────────────────────────────────────────────────
# PDF text extraction (sem OCR)
# ──────────────────────────────────────────────────────────────

def _extract_pdf_text(pdf_bytes: bytes, max_pages: int = 25) -> str:
    """
    Tenta extrair texto de PDF sem OCR.
    Preferência: PyPDF2 (rápido). Fallback: pdfminer.six (mais robusto, mas mais lento).
    """
    if not pdf_bytes:
        return ""

    # PyPDF2
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
        out = "\n".join(texts)
        out = _clean_text(out)
        if len(out) >= 200:  # mínimo para ser útil
            return out
    except Exception:
        pass

    # pdfminer.six
    try:
        from pdfminer.high_level import extract_text  # type: ignore
        out = extract_text(io.BytesIO(pdf_bytes), maxpages=max_pages) or ""
        out = _clean_text(out)
        return out
    except Exception:
        return ""


# ──────────────────────────────────────────────────────────────
# Supabase helpers
# ──────────────────────────────────────────────────────────────

def _engine():
    return get_supabase_engine()

def get_cvm_codes_for_tickers(tickers: Sequence[str]) -> Dict[str, int]:
    """
    Lê mapeamento CVM->Ticker na tabela public.cvm_to_ticker.
    """
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


def _get_doc_status(conn, doc_hash: str) -> Optional[Dict[str, Any]]:
    """Retorna status do doc se existir: id e se tem texto.

    Compatível com schemas que usam 'texto' ou 'raw_text'.
    """
    text_col = _get_text_column(conn)
    sql = f"""
        select id,
               (case when coalesce({text_col},'')<>'' then 1 else 0 end) as has_text
        from public.docs_corporativos
        where doc_hash = :h
        limit 1
    """
    row = conn.execute(text(sql), {"h": doc_hash}).fetchone()
    if not row:
        return None
    return {"id": int(row[0]), "has_text": bool(row[1])}


def _update_doc_text(conn, *, doc_id: int, texto: str) -> None:
    text_col = _get_text_column(conn)
    sql = f"""
        update public.docs_corporativos
        set {text_col} = :texto
        where id = :id
    """
    conn.execute(text(sql), {"id": int(doc_id), "texto": texto})




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
    sql = f"""
        insert into public.docs_corporativos
        (ticker, titulo, url, fonte, tipo, data, {text_col}, doc_hash)
        values
        (:ticker, :titulo, :url, :fonte, :tipo, :data, :texto, :doc_hash)
        on conflict (doc_hash) do nothing
        returning id
    """
    row = conn.execute(
        text(sql),
        {
            "ticker": ticker,
            "titulo": titulo[:4000],
            "url": url[:4000],
            "fonte": fonte,
            "tipo": tipo[:200],
            "data": (data.to_pydatetime() if isinstance(data, pd.Timestamp) and not pd.isna(data) else None),
            "texto": texto,
            "doc_hash": doc_hash,
        },
    ).fetchone()
    return int(row[0]) if row else None



# ──────────────────────────────────────────────────────────────
# Core ingest
# ──────────────────────────────────────────────────────────────

def _load_ipe_csv(year: int, timeout: int = 30) -> pd.DataFrame:
    """
    Baixa e lê o CSV do IPE (CIA_ABERTA) do ano informado.
    """
    url_zip = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{year}.zip"
    r = requests.get(url_zip, timeout=timeout)
    r.raise_for_status()

    # o zip contém um único csv com o mesmo nome base
    import zipfile
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    # pega o primeiro CSV do zip
    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if not csv_names:
        raise RuntimeError("ZIP do IPE não contém CSV")
    raw = zf.read(csv_names[0])

    # encoding pode variar; latin-1 costuma funcionar
    for enc in ("utf-8", "latin1"):
        try:
            df = pd.read_csv(io.BytesIO(raw), sep=";", encoding=enc, dtype=str)
            return df
        except Exception:
            continue
    # fallback
    df = pd.read_csv(io.BytesIO(raw), sep=";", encoding="latin1", dtype=str)
    return df


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
    max_runtime_s: float = 25.0,
    sleep_s: float = 0.0,
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    Ingest por tickers usando o dataset IPE.
    Retorna dict com stats por ticker.

    strategic_only:
      filtra por termos-chave em ASSUNTO/CATEGORIA/TIPO para focar documentos estratégicos.
    """
    started = time.time()
    tickers_n = [_norm_ticker(t) for t in tickers if (t or "").strip()]
    cvm_map = get_cvm_codes_for_tickers(tickers_n)

    now = _utcnow()
    min_dt = _now_minus_months(int(window_months))

    # carrega anos necessários (janela pode atravessar ano)
    years = sorted({now.year, (min_dt.year)})
    dfs: List[pd.DataFrame] = []
    for y in years:
        try:
            dfs.append(_load_ipe_csv(y, timeout=request_timeout))
        except Exception as e:
            if verbose:
                print(f"[IPE] Falha ao carregar {y}: {e}")
    if not dfs:
        return {"ok": False, "errors": {"__all__": "Nenhum CSV IPE disponível (falha ao baixar ZIP)."}, "stats": {}}

    df = pd.concat(dfs, ignore_index=True)
    cols = list(df.columns)

    col_cvm = _pick_col(cols, "CODIGO_CVM", "CD_CVM", "CVM", "COD_CVM")
    col_data = _pick_col(cols, "DATA_ENTREGA", "DT_RECEB", "DT_REFER", "DATA_REFERENCIA", "DATA_REFER", "DT_ENTREGA")
    col_link = _pick_col(cols, "LINK_DOWNLOAD", "LINK", "LINK_ARQUIVO", "LINK_DOC", "LINK_DOCUMENTO")
    col_assunto = _pick_col(cols, "ASSUNTO", "ASSUNTO_EVENTO", "TITULO", "DESCRICAO")
    col_categoria = _pick_col(cols, "CATEGORIA", "CATEGORIA_DOCUMENTO")
    col_tipo = _pick_col(cols, "TIPO", "TIPO_DOCUMENTO")
    col_especie = _pick_col(cols, "ESPECIE", "ESPECIE_DOCUMENTO")

    missing = [c for c in (col_cvm, col_data, col_link, col_assunto) if c is None]
    if missing:
        return {
            "ok": False,
            "errors": {"__all__": f"CSV IPE sem colunas necessárias. Encontradas={cols}"},
            "stats": {},
        }

    # parse dates once
    df["_dt"] = df[col_data].apply(_parse_date)
    df = df[~df["_dt"].isna()].copy()
    # Normaliza para evitar comparação tz-aware vs tz-naive (pandas pode lançar TypeError)
    min_ts = pd.Timestamp(min_dt)
    if getattr(min_ts, "tzinfo", None) is not None:
        # remove timezone mantendo o instante em "naive" (UTC)
        min_ts = min_ts.tz_localize(None)
    df = df[df["_dt"] >= min_ts].copy()

    # filtros estratégicos (heurístico)
    if strategic_only:
        # palavras comuns para "intenção estratégica"
        pattern = re.compile(
            r"(guidance|invest|capex|expans|projeto|plano|estrat[eé]g|aquisi|m&a|fus[aã]o|desalav|aloca|"
            r"remunera|dividend|recompra|conselho|assembleia|fato relevante|comunicado|release|"
            r"resultad|earnings|apresenta[cç][aã]o|teleconfer|call)",
            re.IGNORECASE,
        )
        def _is_strategic(row) -> bool:
            txt = " ".join([str(row.get(col_assunto, "") or ""),
                            str(row.get(col_categoria, "") or ""),
                            str(row.get(col_tipo, "") or ""),
                            str(row.get(col_especie, "") or "")])
            return bool(pattern.search(txt))
        df = df[df.apply(_is_strategic, axis=1)].copy()

    out_stats: Dict[str, Any] = {}
    out_errors: Dict[str, str] = {}

    with _engine().begin() as conn:
        for tk in tickers_n:
            if (time.time() - started) > max_runtime_s:
                out_errors["__runtime__"] = f"Tempo máximo atingido ({max_runtime_s}s)."
                break

            cvm = cvm_map.get(tk)
            if not cvm:
                out_errors[tk] = "ticker_sem_mapeamento_cvm (preencha public.cvm_to_ticker)"
                out_stats[tk] = {"matched": 0, "inserted": 0, "skipped": 0, "pdf_fetched": 0, "pdf_text_ok": 0}
                continue

            dft = df[df[col_cvm].astype(str) == str(cvm)].copy()
            # ordena por mais recente
            dft = dft.sort_values("_dt", ascending=False)

            matched = int(len(dft))
            if matched == 0:
                out_stats[tk] = {"matched": 0, "inserted": 0, "skipped": 0, "pdf_fetched": 0, "pdf_text_ok": 0}
                continue

            # limita por ticker
            dft = dft.head(int(max_docs_per_ticker)).copy()

            inserted = 0
            skipped = 0
            pdf_fetched = 0
            pdf_text_ok = 0
            pdf_used = 0
            updated_text = 0

            for _, r in dft.iterrows():
                if (time.time() - started) > max_runtime_s:
                    out_errors["__runtime__"] = f"Tempo máximo atingido ({max_runtime_s}s)."
                    break

                url = str(r.get(col_link, "") or "").strip()
                if not url:
                    skipped += 1
                    continue

                titulo = _clean_text(str(r.get(col_assunto, "") or ""))
                if titulo.lower() == "nan":
                    titulo = ""
                titulo = titulo or "Documento CVM/IPE"
                tipo = _clean_text(str(r.get(col_tipo, "") or "")) or "IPE"
                dt = r.get("_dt")

                doc_hash = _sha256(f"{tk}|{url}|{titulo}|{dt}")
                status = _get_doc_status(conn, doc_hash)
                if status is not None:
                    # Doc já existe. Se ele não tem texto e estamos habilitados a baixar PDFs,
                    # tentamos "backfill" do texto e atualizamos a linha existente.
                    if (not status["has_text"]) and download_pdfs:
                        texto = ""
                        if _looks_like_pdf(url) and pdf_used < int(max_pdfs_per_ticker):
                            try:
                                resp = requests.get(url, timeout=request_timeout, allow_redirects=True)
                                resp.raise_for_status()
                                if _is_pdf_bytes(resp.content) or ("application/pdf" in (resp.headers.get("content-type","").lower())):
                                    pdf_fetched += 1
                                    pdf_used += 1
                                    tpdf = _extract_pdf_text(resp.content, max_pages=int(pdf_max_pages))
                                    if tpdf and len(tpdf) >= 200:
                                        texto = tpdf
                                        pdf_text_ok += 1
                            except Exception:
                                texto = ""
                        if texto:
                            _update_doc_text(conn, doc_id=status["id"], texto=texto)
                            updated_text += 1
                    skipped += 1
                    continue

                texto = ""
                # Se PDF e permitido, tenta baixar e extrair texto
                if download_pdfs and _looks_like_pdf(url) and pdf_used < int(max_pdfs_per_ticker):
                    try:
                        resp = requests.get(url, timeout=request_timeout, allow_redirects=True)
                        resp.raise_for_status()
                        # Links da CVM nem sempre terminam em .pdf; valide pelo header/magic bytes
                        if not (_is_pdf_bytes(resp.content) or ("application/pdf" in (resp.headers.get("content-type","").lower()))):
                            raise ValueError("download não retornou PDF")
                        pdf_fetched += 1
                        pdf_used += 1
                        tpdf = _extract_pdf_text(resp.content, max_pages=int(pdf_max_pages))
                        if tpdf and len(tpdf) >= 200:
                            texto = tpdf
                            pdf_text_ok += 1
                    except Exception:
                        # mantem metadados apenas
                        pass

                # Insere documento mesmo sem texto (metadados são úteis)
                doc_id = _insert_doc(
                    conn,
                    ticker=tk,
                    titulo=titulo,
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
                "considered": int(len(dft)),
                "inserted": inserted,
                "skipped": skipped,
                "pdf_fetched": pdf_fetched,
                "pdf_text_ok": pdf_text_ok,
                "updated_text": updated_text,
            }

    ok = (len(out_errors) == 0)
    return {"ok": ok, "stats": out_stats, "errors": out_errors}
