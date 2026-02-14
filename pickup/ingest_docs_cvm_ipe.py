from __future__ import annotations

"""
pickup/ingest_docs_cvm_ipe.py
----------------------------
Ingestão de comunicados IPE (CVM) para Supabase:
- salva doc em public.docs_corporativos
- cria chunks em public.docs_corporativos_chunks

Objetivo: abastecer o Patch 6 (RAG) com fontes oficiais.

Características:
- Pode rodar via botão no Streamlit (page.patch6_teste / patch6)
- Timeout curto + retry curto (para não "travar" o app)
- Baixa PDF (Link_Download) e extrai texto (sem OCR) quando possível
- Se a CVM mudar endpoint/CSV, retorna erros explicativos

Requisitos:
- requests
- pandas
- sqlalchemy
- pdfminer.six  (recomendado; se não tiver, cai para modo "metadados")
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
import hashlib
import io
import re
import time

import pandas as pd
import streamlit as st
import requests
from sqlalchemy import text

from core.db_loader import get_supabase_engine

# ---------------------------------------------------------------------
# Util
# ---------------------------------------------------------------------

def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()

def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

def _clean_text(s: str) -> str:
    s = (s or "").replace("\x00", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _chunk_text(texto: str, chunk_chars: int = 1600, overlap: int = 220) -> List[str]:
    t = (texto or "").strip()
    if not t:
        return []
    t = t.replace("\r\n", "\n")
    out: List[str] = []
    i = 0
    n = len(t)
    while i < n:
        j = min(n, i + chunk_chars)
        out.append(t[i:j])
        if j >= n:
            break
        i = max(0, j - overlap)
    return out

# ---------------------------------------------------------------------
# IPE via CSV (opção 1 — recomendada)
# ---------------------------------------------------------------------

# Configure nas Secrets/Env:
#   IPE_CSV_URL_TEMPLATE="https://.../ipe_{ano}.csv"
IPE_CSV_URL_TEMPLATE = (
    st.secrets.get("IPE_CSV_URL_TEMPLATE")
    if hasattr(st, "secrets") else None
) or (  # env fallback
    __import__("os").getenv("IPE_CSV_URL_TEMPLATE") or ""
).strip()

def _http_get(url: str, *, timeout: int = 15) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
    last_exc: Optional[Exception] = None
    for _ in range(2):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.content
        except Exception as e:
            last_exc = e
            time.sleep(0.35)
    raise RuntimeError(f"GET falhou: {last_exc}")

def _download_ipe_csv(ano: int, *, timeout: int = 20) -> pd.DataFrame:
    if not IPE_CSV_URL_TEMPLATE:
        raise RuntimeError(
            "IPE_CSV_URL_TEMPLATE não definido. "
            "Defina nas Secrets/Env. Ex.: https://.../ipe_{ano}.csv"
        )
    url = IPE_CSV_URL_TEMPLATE.format(ano=ano)
    content = _http_get(url, timeout=timeout)

    # tenta decodificar
    s = ""
    for enc in ("utf-8-sig", "latin1", "cp1252"):
        try:
            s = content.decode(enc)
            break
        except Exception:
            pass
    if not s:
        raise RuntimeError("Falha ao decodificar CSV IPE.")

    # separador pode variar
    try:
        df = pd.read_csv(io.StringIO(s), sep=";", dtype=str)
        if df.shape[1] <= 2:
            df = pd.read_csv(io.StringIO(s), sep=",", dtype=str)
    except Exception:
        df = pd.read_csv(io.StringIO(s), sep=",", dtype=str)

    df.columns = [str(c).strip() for c in df.columns]
    return df

def _col(df: pd.DataFrame, *names: str) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in cols:
            return cols[n.lower()]
    return None

def _row_to_meta(df: pd.DataFrame, row: pd.Series) -> Dict[str, Any]:
    def getv(*names: str) -> str:
        c = _col(df, *names)
        if c and c in row and pd.notna(row[c]):
            return str(row[c]).strip()
        return ""

    return {
        "CNPJ_Companhia": getv("CNPJ_Companhia", "CNPJ"),
        "Nome_Companhia": getv("Nome_Companhia", "Nome Companhia", "Emissor"),
        "Codigo_CVM": getv("Codigo_CVM", "Código_CVM", "Cod_CVM"),
        "Data_Referencia": getv("Data_Referencia", "Data Referencia"),
        "Categoria": getv("Categoria"),
        "Tipo": getv("Tipo"),
        "Especie": getv("Especie", "Espécie"),
        "Assunto": getv("Assunto", "DescricaoAssunto", "Descrição"),
        "Data_Entrega": getv("Data_Entrega", "DataEntrega", "Data Entrega"),
        "Tipo_Apresentacao": getv("Tipo_Apresentacao", "Tipo Apresentacao"),
        "Protocolo_Entrega": getv("Protocolo_Entrega", "Protocolo Entrega"),
        "Versao": getv("Versao", "Versão"),
        "Link_Download": getv("Link_Download", "Link Download", "URL", "Url"),
    }

def _filter_rows_for_ticker(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Sem CVM->ticker, filtragem mínima:
      - se CSV tiver coluna Ticker -> match exato
      - senão retorna vazio (para evitar falsos positivos)
    """
    tk = _norm_ticker(ticker)
    if df.empty:
        return df

    col_ticker = _col(df, "Ticker", "ticker")
    if col_ticker:
        m = df[col_ticker].astype(str).str.upper().str.replace(".SA", "", regex=False).str.strip()
        return df[m == tk]

    return df.iloc[0:0]

# ---------------------------------------------------------------------
# PDF text extraction (sem OCR)
# ---------------------------------------------------------------------

def _extract_pdf_text(pdf_bytes: bytes) -> str:
    if not pdf_bytes:
        return ""
    try:
        from pdfminer.high_level import extract_text  # type: ignore
    except Exception:
        return ""
    try:
        with io.BytesIO(pdf_bytes) as bio:
            txt = extract_text(bio) or ""
        return _clean_text(txt)
    except Exception:
        return ""

# ---------------------------------------------------------------------
# Supabase insert
# ---------------------------------------------------------------------

def _upsert_doc_and_chunks(
    *,
    ticker: str,
    data: Optional[str],
    fonte: str,
    tipo: str,
    titulo: str,
    url: str,
    raw_text: str,
    chunk_chars: int = 1600,
    overlap: int = 220,
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
            "data": data,
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

# ---------------------------------------------------------------------
# Public API: ingest
# ---------------------------------------------------------------------

def ingest_ipe_for_tickers(
    tickers: Sequence[str],
    *,
    anos: int = 1,
    max_docs_por_ticker: int = 12,
    sleep_s: float = 0.0,
    chunk_chars: int = 1600,
    overlap: int = 220,
    timeout_pdf: int = 18,
    timeout_csv: int = 25,
) -> Dict[str, Any]:
    """
    Opção 1 (CSV IPE + PDF):
    - baixa CSV do(s) ano(s)
    - filtra por ticker (se CSV tiver coluna Ticker)
    - baixa PDF do Link_Download
    - extrai texto (pdfminer) ou salva metadados se não conseguir
    - insere no Supabase

    Retorno:
      {
        "ok": bool,
        "stats": { "TICKER": {"matched":M,"inserted":I,"skipped":K} },
        "errors": { "TICKER|ANO_xxxx": "mensagem" }
      }
    """
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {"ok": False, "stats": {}, "errors": {"__all__": "Lista de tickers vazia."}}

    ano_fim = int(pd.Timestamp.utcnow().year)
    ano_ini = int(ano_fim - max(0, int(anos) - 1))
    anos_list = list(range(ano_ini, ano_fim + 1))

    stats: Dict[str, Dict[str, int]] = {tk: {"matched": 0, "inserted": 0, "skipped": 0} for tk in tks}
    errors: Dict[str, str] = {}

    csvs: Dict[int, pd.DataFrame] = {}
    for ano in anos_list:
        try:
            df = _download_ipe_csv(ano, timeout=int(timeout_csv))
            csvs[ano] = df
        except Exception as e:
            errors[f"ANO_{ano}"] = f"{type(e).__name__}: {e}"

    if not csvs:
        return {"ok": False, "stats": stats, "errors": errors or {"__all__": "Nenhum CSV IPE disponível."}}

    for tk in tks:
        try:
            used = 0
            for ano, df in csvs.items():
                f = _filter_rows_for_ticker(df, tk)
                if f.empty:
                    continue

                for _, r in f.iterrows():
                    if used >= int(max_docs_por_ticker):
                        break

                    meta = _row_to_meta(df, r)
                    url = meta.get("Link_Download", "").strip()
                    if not url:
                        stats[tk]["skipped"] += 1
                        continue

                    stats[tk]["matched"] += 1

                    # baixa PDF
                    try:
                        pdf_bytes = _http_get(url, timeout=int(timeout_pdf))
                    except Exception as e:
                        errors[f"{tk}|PDF"] = f"{type(e).__name__}: {e}"
                        stats[tk]["skipped"] += 1
                        continue

                    txt = _extract_pdf_text(pdf_bytes)
                    if not txt:
                        # sem pdfminer ou PDF sem texto
                        txt = _clean_text(
                            f"ASSUNTO: {meta.get('Assunto','')}\n"
                            f"CATEGORIA: {meta.get('Categoria','')}\n"
                            f"TIPO: {meta.get('Tipo','')}\n"
                            f"DATA_ENTREGA: {meta.get('Data_Entrega','')}\n"
                            f"LINK: {url}\n"
                        )

                    data_iso = None
                    for k in ("Data_Entrega", "Data_Referencia"):
                        v = meta.get(k)
                        if isinstance(v, str) and v.strip():
                            d = pd.to_datetime(v, errors="coerce", dayfirst=True)
                            if pd.notna(d):
                                data_iso = d.date().isoformat()
                                break

                    titulo = (meta.get("Assunto") or "").strip()

                    inserted, _ = _upsert_doc_and_chunks(
                        ticker=tk,
                        data=data_iso,
                        fonte="CVM",
                        tipo="ipe",
                        titulo=titulo,
                        url=url,
                        raw_text=txt,
                        chunk_chars=int(chunk_chars),
                        overlap=int(overlap),
                    )
                    if inserted:
                        stats[tk]["inserted"] += 1
                    else:
                        stats[tk]["skipped"] += 1

                    used += 1
                    if sleep_s and float(sleep_s) > 0:
                        time.sleep(float(sleep_s))

        except Exception as e:
            errors[tk] = f"{type(e).__name__}: {e}"

    ok = (len([k for k in errors.keys() if not k.startswith("ANO_")]) == 0)
    return {"ok": ok, "stats": stats, "errors": errors}
