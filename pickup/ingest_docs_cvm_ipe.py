# scripts/ingest_docs_cvm_ipe.py
from __future__ import annotations

"""
Ingestor de documentos corporativos (CVM IPE / Empresas.NET) para alimentar Patch 6 (RAG).

O que ele faz (MVP robusto):
1) Baixa o ZIP anual do dataset público de IPE (dados.cvm.gov.br) para um ou mais anos.
2) Lê o CSV do ZIP e filtra por tickers de interesse (por coluna de negociação, ou por mapeamento codCVM->ticker).
3) Para cada registro, tenta resolver texto:
   - se houver coluna de "Link"/"URL": baixa e extrai texto (HTML/PDF)
   - se não houver: ainda assim armazena uma linha com metadados e raw_text mínimo
4) Insere em public.docs_corporativos (dedupe por doc_hash)
5) Chunking e inserção em public.docs_corporativos_chunks (dedupe por chunk_hash)

Requisitos:
- requests, pandas, sqlalchemy, bs4 (BeautifulSoup), pypdf (ou PyPDF2)
- SUPABASE_DB_URL (ou DATABASE_URL) configurado no ambiente
"""

import argparse
import hashlib
import io
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from sqlalchemy import text

from core.db_loader import get_supabase_engine


# -----------------------------
# Config do dataset público (CVM)
# -----------------------------
CVM_IPE_ZIP_URL_TEMPLATE = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{year}.zip"


# -----------------------------
# Util
# -----------------------------
def norm_ticker(t: str) -> str:
    t = (t or "").upper().replace(".SA", "").strip()
    # remove espaços e caracteres estranhos
    t = re.sub(r"[^A-Z0-9]", "", t)
    return t


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {str(c).strip(): c for c in df.columns}
    for c in candidates:
        if c in cols:
            return str(cols[c])
    lower = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        key = c.strip().lower()
        if key in lower:
            return str(lower[key])
    return None


def safe_date(x: Any) -> Optional[date]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    try:
        dt = pd.to_datetime(x, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None


def sha256_text(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8", errors="ignore"))
        h.update(b"\n")
    return h.hexdigest()


def chunk_text(text: str, *, chunk_size: int = 1200, overlap: int = 180) -> List[str]:
    """
    Chunking simples (sem embeddings): janela deslizante em caracteres.
    """
    t = re.sub(r"\s+", " ", (text or "")).strip()
    if not t:
        return []
    if chunk_size <= 0:
        return [t]

    chunks: List[str] = []
    i = 0
    n = len(t)
    step = max(1, chunk_size - max(0, overlap))
    while i < n:
        chunks.append(t[i : i + chunk_size].strip())
        i += step
    return [c for c in chunks if c]


def fetch_url_content(url: str, timeout: int = 40) -> Tuple[str, str]:
    """
    Retorna (content_type, raw_bytes_as_text_placeholder_or_empty).

    Para PDF, tentamos extrair texto (pypdf).
    Para HTML, extraímos texto via BeautifulSoup.
    Para outros, tenta decodificar como texto.
    """
    if not url:
        return ("", "")

    headers = {"User-Agent": "dashboard-analise/patch6 (contact: local)"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()

    ctype = (r.headers.get("content-type") or "").lower()
    data = r.content or b""

    # PDF
    if "pdf" in ctype or url.lower().endswith(".pdf"):
        try:
            from pypdf import PdfReader  # type: ignore
        except Exception:
            try:
                from PyPDF2 import PdfReader  # type: ignore
            except Exception:
                return (ctype, "")

        try:
            reader = PdfReader(io.BytesIO(data))
            pages = []
            for p in reader.pages[:50]:  # hard limit (segurança/performance)
                pages.append((p.extract_text() or "").strip())
            txt = "\n".join([x for x in pages if x])
            return (ctype, txt)
        except Exception:
            return (ctype, "")

    # HTML
    if "html" in ctype or url.lower().endswith((".htm", ".html")):
        try:
            soup = BeautifulSoup(data, "html.parser")
            # remove scripts/styles
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            txt = soup.get_text(separator="\n")
            txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
            return (ctype, txt)
        except Exception:
            return (ctype, "")

    # Texto genérico
    try:
        txt = data.decode("utf-8", errors="ignore")
        txt = txt.strip()
        return (ctype, txt)
    except Exception:
        return (ctype, "")


@dataclass
class DocRow:
    ticker: str
    data: Optional[date]
    fonte: str
    tipo: str
    titulo: Optional[str]
    url: Optional[str]
    raw_text: str
    lang: str
    doc_hash: str


def build_doc_row(
    *,
    ticker: str,
    fonte: str,
    tipo: str,
    data: Optional[date],
    titulo: Optional[str],
    url: Optional[str],
    raw_text: str,
    lang: str = "pt",
) -> DocRow:
    dh = sha256_text(ticker, str(data or ""), fonte, tipo, titulo or "", url or "", raw_text)
    return DocRow(
        ticker=ticker,
        data=data,
        fonte=fonte,
        tipo=tipo,
        titulo=titulo,
        url=url,
        raw_text=raw_text,
        lang=lang or "pt",
        doc_hash=dh,
    )


def upsert_doc_and_chunks(engine, doc: DocRow, *, chunk_size: int, overlap: int) -> None:
    """
    Insere doc (dedupe por doc_hash) e insere chunks (dedupe por chunk_hash).
    """
    with engine.begin() as conn:
        # 1) doc (dedupe por doc_hash)
        res = conn.execute(
            text(
                """
                INSERT INTO public.docs_corporativos
                    (ticker, data, fonte, tipo, titulo, url, raw_text, lang, doc_hash)
                VALUES
                    (:ticker, :data, :fonte, :tipo, :titulo, :url, :raw_text, :lang, :doc_hash)
                ON CONFLICT (doc_hash) DO UPDATE
                    SET ticker = EXCLUDED.ticker
                RETURNING id
                """
            ),
            {
                "ticker": doc.ticker,
                "data": doc.data,
                "fonte": doc.fonte,
                "tipo": doc.tipo,
                "titulo": doc.titulo,
                "url": doc.url,
                "raw_text": doc.raw_text,
                "lang": doc.lang,
                "doc_hash": doc.doc_hash,
            },
        )
        doc_id = int(res.scalar() or 0)
        if doc_id <= 0:
            return

        # 2) chunks
        chunks = chunk_text(doc.raw_text, chunk_size=chunk_size, overlap=overlap)
        for idx, ch in enumerate(chunks):
            ch_hash = sha256_text(str(doc_id), doc.ticker, str(idx), ch)
            conn.execute(
                text(
                    """
                    INSERT INTO public.docs_corporativos_chunks
                        (doc_id, ticker, chunk_index, chunk_text, chunk_hash)
                    VALUES
                        (:doc_id, :ticker, :chunk_index, :chunk_text, :chunk_hash)
                    ON CONFLICT (chunk_hash) DO NOTHING
                    """
                ),
                {
                    "doc_id": doc_id,
                    "ticker": doc.ticker,
                    "chunk_index": int(idx),
                    "chunk_text": ch,
                    "chunk_hash": ch_hash,
                },
            )


def load_mapping_csv(path: Optional[str]) -> Dict[str, str]:
    """
    Lê CSV de mapeamento (opcional) com colunas:
      - ticker
      - codigo_cvm (ou cod_cvm / codcvm)
    Retorna dict[codigo_cvm] -> ticker
    """
    if not path:
        return {}
    df = pd.read_csv(path)
    if df is None or df.empty:
        return {}
    c_tk = pick_col(df, ["ticker", "Ticker"])
    c_cv = pick_col(df, ["codigo_cvm", "cod_cvm", "codcvm", "CodigoCVM", "codigoCVM"])
    if not c_tk or not c_cv:
        return {}
    out = {}
    for _, r in df.iterrows():
        tk = norm_ticker(str(r.get(c_tk, "")))
        cv = str(r.get(c_cv, "")).strip()
        if tk and cv:
            out[cv] = tk
    return out


def iter_ipe_rows_from_zip(zip_bytes: bytes) -> pd.DataFrame:
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    # costuma ter 1 csv dentro
    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if not csv_names:
        raise RuntimeError("ZIP não contém CSV.")
    with zf.open(csv_names[0]) as f:
        # encoding típico latin1
        raw = f.read()
    # tenta decodificar
    for enc in ("utf-8", "latin1", "cp1252"):
        try:
            txt = raw.decode(enc)
            break
        except Exception:
            txt = None
    if txt is None:
        txt = raw.decode("latin1", errors="ignore")

    # detecta separador
    sep = ";" if txt.count(";") > txt.count(",") else ","
    df = pd.read_csv(io.StringIO(txt), sep=sep, low_memory=False)
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=str, default=str(datetime.now().year),
                    help="Anos separados por vírgula (ex: 2024,2025) ou intervalo (ex: 2021-2025).")
    ap.add_argument("--tickers", type=str, required=True,
                    help="Tickers separados por vírgula (ex: PETR4,VALE3).")
    ap.add_argument("--mapping_csv", type=str, default="",
                    help="CSV opcional para mapear CodigoCVM -> ticker (se o dataset não tiver ticker).")
    ap.add_argument("--tipos", type=str, default="",
                    help="Filtrar por tipo do documento (separado por |). Ex: FATO RELEVANTE|COMUNICADO AO MERCADO")
    ap.add_argument("--max_docs_per_ticker", type=int, default=25)
    ap.add_argument("--chunk_size", type=int, default=1200)
    ap.add_argument("--overlap", type=int, default=180)
    ap.add_argument("--dry_run", action="store_true")
    args = ap.parse_args()

    years_spec = (args.years or "").strip()
    tickers = [norm_ticker(x) for x in (args.tickers or "").split(",") if norm_ticker(x)]
    tickers = sorted(list(dict.fromkeys(tickers)))
    if not tickers:
        raise SystemExit("Nenhum ticker informado.")

    years: List[int] = []
    if "-" in years_spec:
        a, b = years_spec.split("-", 1)
        years = list(range(int(a), int(b) + 1))
    else:
        years = [int(x.strip()) for x in years_spec.split(",") if x.strip().isdigit()]

    tipos_filter = [t.strip() for t in (args.tipos or "").split("|") if t.strip()]
    map_cvm_to_ticker = load_mapping_csv(args.mapping_csv)

    engine = get_supabase_engine()

    # Baixa e concatena
    dfs: List[pd.DataFrame] = []
    for y in years:
        url = CVM_IPE_ZIP_URL_TEMPLATE.format(year=y)
        print(f"[download] {url}")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        df = iter_ipe_rows_from_zip(r.content)
        df["_year"] = y
        dfs.append(df)

    big = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    if big.empty:
        raise SystemExit("Dataset IPE vazio.")

    # tenta achar colunas importantes
    col_ticker = pick_col(big, ["CodigoNegociacao", "Codigo_Negociacao", "Código de Negociação", "Ticker", "ticker"])
    col_codcvm = pick_col(big, ["CodigoCVM", "CodCVM", "codigo_cvm", "cod_cvm"])
    col_data = pick_col(big, ["DataEntrega", "DataEntregaDocumento", "DataReferencia", "Data", "data"])
    col_tipo = pick_col(big, ["Tipo", "Categoria", "EspecieDocumento", "tipo"])
    col_titulo = pick_col(big, ["Assunto", "Titulo", "Título", "titulo"])
    col_url = pick_col(big, ["LinkDocumento", "URLDocumento", "UrlDocumento", "Link", "url"])

    if not col_tipo:
        print("[warn] Não achei coluna de tipo. Vou setar tipo='ipe'.")
    if not col_url:
        print("[warn] Não achei coluna de URL/Link. Vou ingerir apenas metadados + título (raw_text mínimo).")

    # normaliza ticker por linha
    def resolve_ticker(row) -> str:
        if col_ticker:
            tk = norm_ticker(str(row.get(col_ticker, "")))
            if tk:
                return tk
        if col_codcvm and map_cvm_to_ticker:
            cv = str(row.get(col_codcvm, "")).strip()
            return norm_ticker(map_cvm_to_ticker.get(cv, ""))
        return ""

    big["_tk"] = big.apply(resolve_ticker, axis=1)
    big = big[big["_tk"].isin(tickers)].copy()
    if big.empty:
        raise SystemExit("Nada no IPE para os tickers informados (ou mapeamento ausente).")

    # filtra por tipo se pedido
    if tipos_filter and col_tipo:
        big["_tipo"] = big[col_tipo].astype(str).str.upper().str.strip()
        up = [t.upper() for t in tipos_filter]
        big = big[big["_tipo"].isin(up)].copy()

    # ordena por data desc e limita por ticker
    if col_data:
        big["_dt"] = pd.to_datetime(big[col_data], errors="coerce")
    else:
        big["_dt"] = pd.NaT

    big = big.sort_values(["_tk", "_dt"], ascending=[True, False])
    big = big.groupby("_tk").head(int(args.max_docs_per_ticker)).reset_index(drop=True)

    print(f"[rows] {len(big)} registros selecionados")

    # ingest
    for tk, sub in big.groupby("_tk"):
        sub = sub.copy()
        print(f"\n[ticker] {tk} — {len(sub)} docs")
        for _, r in sub.iterrows():
            fonte = "cvm_ipe"
            tipo = str(r.get(col_tipo, "ipe")).strip() if col_tipo else "ipe"
            titulo = str(r.get(col_titulo, "")).strip() if col_titulo else ""
            dt = safe_date(r.get(col_data)) if col_data else None
            url = str(r.get(col_url, "")).strip() if col_url else ""

            raw_text = ""
            if url:
                try:
                    _, raw_text = fetch_url_content(url)
                except Exception:
                    raw_text = ""

            if not raw_text:
                # fallback mínimo (não inventa)
                raw_text = f"{titulo}\n\n[Fonte={fonte}] [Tipo={tipo}] [Data={dt or 'NA'}] [URL={url or 'NA'}]"

            doc = build_doc_row(
                ticker=tk,
                fonte=fonte,
                tipo=tipo,
                data=dt,
                titulo=titulo or None,
                url=url or None,
                raw_text=raw_text,
                lang="pt",
            )

            if args.dry_run:
                print(f"  - DRY: {tk} | {dt} | {tipo} | {titulo[:60]}")
                continue

            try:
                upsert_doc_and_chunks(engine, doc, chunk_size=int(args.chunk_size), overlap=int(args.overlap))
                print(f"  - ok: {dt} | {tipo} | {titulo[:60]}")
            except Exception as e:
                print(f"  - fail: {type(e).__name__}: {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()
