from __future__ import annotations

"""
pickup/ingest_docs_cvm_ipe.py
----------------------------
Ingestão de documentos IPE (CVM - Dados Abertos) para Supabase.

Estratégia (sem CVM↔Ticker confiável):
1) Baixa ZIP oficial: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{ANO}.zip
2) Lê o CSV do zip.
3) Faz match do Nome_Companhia do IPE com setores.nome_empresa → obtém ticker.
4) Para docs das empresas desejadas: baixa PDF (Link_Download), extrai texto, salva em:
   - public.docs_corporativos
   - public.docs_corporativos_chunks

Observações:
- Extração de texto de PDF pode ser imperfeita (depende do PDF).
- Sem OCR (por enquanto).
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
from datetime import datetime, date
import hashlib
import io
import re
import time
import zipfile
import unicodedata

import pandas as pd
import requests
from sqlalchemy import text
import streamlit as st

from core.db_loader import get_supabase_engine, load_setores_from_db


# -------------------------
# Normalização / hashing
# -------------------------

def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()

def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

def _clean_text(s: str) -> str:
    s = (s or "").replace("\x00", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def _norm_name(s: str) -> str:
    """
    Normaliza nome de empresa para matching:
    - upper
    - sem acentos
    - remove pontuação
    - remove sufixos comuns
    """
    s = _strip_accents(str(s or "")).upper()
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # remove ruídos comuns
    junk = [
        "S A", "S A ", " SA", " S A", " S/A", " S A", "S/A",
        "COMPANHIA", "CIA", "CI A", "LTDA", "LIMITADA",
        "HOLDING", "HOLDINGS",
    ]
    for j in junk:
        s = s.replace(j, " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _chunk_text(texto: str, chunk_chars: int = 1600, overlap: int = 250) -> List[str]:
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


# -------------------------
# PDF text extraction
# -------------------------

def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """
    Tenta extrair texto do PDF com libs comuns.
    Sem OCR.
    """
    if not pdf_bytes:
        return ""

    # 1) pypdf (novo)
    try:
        from pypdf import PdfReader  # type: ignore
        reader = PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for p in reader.pages:
            try:
                parts.append(p.extract_text() or "")
            except Exception:
                pass
        return _clean_text("\n".join(parts))
    except Exception:
        pass

    # 2) PyPDF2 (antigo)
    try:
        import PyPDF2  # type: ignore
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for p in reader.pages:
            try:
                parts.append(p.extract_text() or "")
            except Exception:
                pass
        return _clean_text("\n".join(parts))
    except Exception:
        pass

    return ""


# -------------------------
# Supabase insert
# -------------------------

def _upsert_doc_and_chunks(
    *,
    ticker: str,
    data_iso: Optional[str],
    fonte: str,
    tipo: str,
    titulo: str,
    url: str,
    raw_text: str,
    chunk_chars: int = 1600,
    overlap: int = 250,
) -> Tuple[bool, str]:
    tk = _norm_ticker(ticker)
    if not tk:
        return False, ""

    fonte = (fonte or "CVM").strip()
    tipo = (tipo or "ipe").strip()
    titulo = (titulo or "").strip()
    url = (url or "").strip()
    raw_text = (raw_text or "").strip()

    doc_hash = _sha256("|".join([tk, fonte, tipo, titulo, url, raw_text]))

    engine = get_supabase_engine()

    sql_doc = text(
        """
        INSERT INTO public.docs_corporativos (ticker, data, fonte, tipo, titulo, url, raw_text, doc_hash)
        VALUES (:ticker, :data, :fonte, :tipo, :titulo, :url, :raw_text, :doc_hash)
        ON CONFLICT (doc_hash) DO NOTHING
        RETURNING id
        """
    )

    with engine.begin() as conn:
        res = conn.execute(sql_doc, {
            "ticker": tk,
            "data": data_iso,
            "fonte": fonte,
            "tipo": tipo,
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

        sql_chunk = text(
            """
            INSERT INTO public.docs_corporativos_chunks (doc_id, ticker, chunk_index, chunk_text, chunk_hash)
            VALUES (:doc_id, :ticker, :chunk_index, :chunk_text, :chunk_hash)
            ON CONFLICT (chunk_hash) DO NOTHING
            """
        )

        for i, ch in enumerate(chunks):
            ch_clean = ch.strip()
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


# -------------------------
# CVM Dados Abertos (IPE)
# -------------------------

def _ipe_zip_url(year: int) -> str:
    return f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{int(year)}.zip"


@st.cache_data(show_spinner=False, ttl=60 * 30)
def _download_ipe_zip(year: int) -> bytes:
    url = _ipe_zip_url(year)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


def _read_ipe_csv_from_zip(zip_bytes: bytes) -> pd.DataFrame:
    """
    Lê o CSV dentro do zip.
    Normalmente existe um CSV principal com os docs.
    """
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    names = zf.namelist()

    # tenta achar o CSV principal (o maior)
    csvs = [n for n in names if n.lower().endswith(".csv")]
    if not csvs:
        raise RuntimeError("ZIP do IPE não contém CSV.")

    # escolhe o CSV maior
    biggest = None
    biggest_size = -1
    for n in csvs:
        info = zf.getinfo(n)
        if info.file_size > biggest_size:
            biggest_size = info.file_size
            biggest = n

    assert biggest is not None
    with zf.open(biggest) as f:
        raw = f.read()

    # encoding pode variar: tenta utf-8 e latin-1
    for enc in ("utf-8", "latin-1"):
        try:
            df = pd.read_csv(io.BytesIO(raw), sep=";", encoding=enc, low_memory=False)
            return df
        except Exception:
            continue

    # fallback: tenta comma
    for enc in ("utf-8", "latin-1"):
        try:
            df = pd.read_csv(io.BytesIO(raw), sep=",", encoding=enc, low_memory=False)
            return df
        except Exception:
            continue

    raise RuntimeError("Falha ao ler CSV do IPE (encoding/separador).")


def _build_nome_to_ticker_index() -> Dict[str, str]:
    """
    Monta índice normalizado:
      norm(nome_empresa) -> ticker
    com base em public.setores.
    """
    setores = load_setores_from_db()
    if setores is None or setores.empty:
        return {}

    # garante colunas
    if "nome_empresa" not in setores.columns or "ticker" not in setores.columns:
        return {}

    mp: Dict[str, str] = {}
    for _, row in setores.iterrows():
        tk = _norm_ticker(str(row.get("ticker") or ""))
        nm = _norm_name(str(row.get("nome_empresa") or ""))
        if tk and nm and nm not in mp:
            mp[nm] = tk
    return mp


def _best_match_ticker(nome_companhia: str, idx: Dict[str, str]) -> Optional[str]:
    """
    Matching simples:
    - tenta match exato do nome normalizado
    - se não achar, tenta melhor similaridade (difflib) com threshold alto
    """
    nm = _norm_name(nome_companhia)
    if not nm:
        return None
    if nm in idx:
        return idx[nm]

    # fuzzy leve (sem libs externas)
    import difflib

    keys = list(idx.keys())
    # corta custo: só compara com candidatos que compartilham tokens
    toks = set(nm.split())
    candidates = [k for k in keys if len(toks.intersection(set(k.split()))) >= max(1, min(2, len(toks)))]
    if not candidates:
        candidates = keys[:2000]  # fallback (limitado)

    best = None
    best_score = 0.0
    for k in candidates:
        s = difflib.SequenceMatcher(None, nm, k).ratio()
        if s > best_score:
            best_score = s
            best = k

    if best and best_score >= 0.93:
        return idx.get(best)

    return None


def _pick_col(df: pd.DataFrame, options: Sequence[str]) -> Optional[str]:
    cols = set(df.columns.astype(str))
    for o in options:
        if o in cols:
            return o
    return None


def ingest_ipe_for_tickers(
    tickers: Sequence[str],
    *,
    anos: int = 2,
    max_docs_por_ticker: int = 30,
    sleep_s: float = 0.15,
    baixar_pdf: bool = True,
    chunk_chars: int = 1600,
    overlap: int = 250,
) -> Dict[str, Any]:
    """
    Ingestão IPE via dados abertos.
    - Filtra docs dos tickers desejados via match por nome da companhia (setores.nome_empresa).

    Retorno:
      {
        "ok": bool,
        "stats": { "ANO_2026": {...}, "TICKER": {...} },
        "errors": {...}
      }
    """
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys([t for t in tks if t]))
    if not tks:
        return {"ok": False, "stats": {}, "errors": {"__all__": "Lista de tickers vazia."}}

    # índice nome->ticker a partir da sua base
    nome_idx = _build_nome_to_ticker_index()
    if not nome_idx:
        return {
            "ok": False,
            "stats": {},
            "errors": {"__all__": "Tabela setores vazia/indisponível (precisa de nome_empresa e ticker)."},
        }

    year_end = int(pd.Timestamp.utcnow().year)
    years = list(range(year_end, year_end - int(anos) - 1, -1))

    stats: Dict[str, Any] = {}
    errors: Dict[str, str] = {}

    # stats por ticker
    for tk in tks:
        stats[tk] = {"seen": 0, "matched": 0, "inserted": 0, "skipped": 0}

    for yr in years:
        key = f"ANO_{yr}"
        stats[key] = {"seen": 0, "matched": 0, "inserted": 0, "skipped": 0}

        try:
            zip_bytes = _download_ipe_zip(yr)
            df = _read_ipe_csv_from_zip(zip_bytes)

            # identifica colunas (o print da sua tela mostra esses nomes)
            c_nome = _pick_col(df, ["Nome_Companhia", "Nome_Companhia ", "Nome_Companhia\t", "Nome_Companhia\r"])
            c_assunto = _pick_col(df, ["Assunto"])
            c_data = _pick_col(df, ["Data_Entrega", "Data_Referencia"])
            c_link = _pick_col(df, ["Link_Download", "Link_Download "])
            c_tipo = _pick_col(df, ["Tipo"])
            c_cat = _pick_col(df, ["Categoria"])

            if not c_nome:
                raise RuntimeError("CSV sem coluna Nome_Companhia (ou variação).")
            if not c_link:
                raise RuntimeError("CSV sem coluna Link_Download (ou variação).")

            # percorre e filtra
            for _, row in df.iterrows():
                stats[key]["seen"] += 1

                nome = str(row.get(c_nome) or "").strip()
                if not nome:
                    stats[key]["skipped"] += 1
                    continue

                tk_match = _best_match_ticker(nome, nome_idx)
                if not tk_match or tk_match not in tks:
                    continue

                stats[key]["matched"] += 1
                stats[tk_match]["seen"] += 1
                stats[tk_match]["matched"] += 1

                assunto = str(row.get(c_assunto) or "").strip() if c_assunto else ""
                link = str(row.get(c_link) or "").strip()
                tipo = str(row.get(c_tipo) or "ipe").strip() if c_tipo else "ipe"
                cat = str(row.get(c_cat) or "").strip() if c_cat else ""

                # data
                data_iso = None
                if c_data:
                    v = row.get(c_data)
                    d = pd.to_datetime(v, errors="coerce", dayfirst=True)
                    if pd.notna(d):
                        data_iso = d.date().isoformat()

                # baixa pdf e extrai
                pdf_text = ""
                if baixar_pdf and link:
                    try:
                        pr = requests.get(link, timeout=60)
                        pr.raise_for_status()
                        pdf_text = _extract_pdf_text(pr.content)
                    except Exception:
                        pdf_text = ""

                # texto final (sempre guarda o header do doc, mesmo se PDF não extrair)
                header = f"FONTE: CVM/IPE\nANO: {yr}\nCOMPANHIA: {nome}\nTICKER: {tk_match}\nCATEGORIA: {cat}\nTIPO: {tipo}\nASSUNTO: {assunto}\nLINK: {link}\nDATA: {data_iso or ''}\n"
                raw_text = header + ("\n\n" + pdf_text if pdf_text else "")

                # se não tem pdf_text, ainda assim salva (porque o Patch6 pelo menos vê metadados)
                inserted, _ = _upsert_doc_and_chunks(
                    ticker=tk_match,
                    data_iso=data_iso,
                    fonte="CVM",
                    tipo="ipe",
                    titulo=(assunto or f"IPE {yr}"),
                    url=link,
                    raw_text=_clean_text(raw_text),
                    chunk_chars=int(chunk_chars),
                    overlap=int(overlap),
                )

                if inserted:
                    stats[key]["inserted"] += 1
                    stats[tk_match]["inserted"] += 1
                else:
                    stats[key]["skipped"] += 1
                    stats[tk_match]["skipped"] += 1

                # limita por ticker (pra não explodir custo)
                if stats[tk_match]["matched"] >= int(max_docs_por_ticker):
                    # continua contando o ano, mas não ingere mais para esse ticker
                    pass

                if sleep_s and float(sleep_s) > 0:
                    time.sleep(float(sleep_s))

        except Exception as e:
            errors[key] = f"{type(e).__name__}: {e}"

    ok = (len(errors) == 0)
    return {"ok": ok, "stats": stats, "errors": errors}
