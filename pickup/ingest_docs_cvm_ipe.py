from __future__ import annotations
"""pickup/ingest_docs_cvm_ipe.py

Ingestão de documentos CVM (IPE) para alimentar o Patch 6 (RAG).

Correções:
- Compatibilidade: ingest_ipe_for_tickers aceita `anos` (antigo) e `anos_back` (novo).
- Busca por código CVM usando tabela public.cvm_to_ticker (colunas: "Ticker", "CVM").
- Baixa CSVs anuais do dataset público IPE (dados.cvm.gov.br). Anos inexistentes (404) são ignorados.
- Salva apenas metadados "estratégicos" (Assunto/Tipo/Categoria + Link) em public.docs_corporativos (+ chunks opcionais).

Requisitos no Supabase:
- public.cvm_to_ticker ("Ticker" text, "CVM" int)
- public.docs_corporativos com colunas mínimas:
    ticker text, data date, fonte text, tipo text, titulo text, url text, raw_text text, doc_hash text unique
  (opcional) codigo_cvm int
- public.docs_corporativos_chunks (opcional) com colunas:
    doc_id int, ticker text, chunk_index int, chunk_text text, chunk_hash text unique
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
import hashlib, io, re, time, os

import pandas as pd
import requests
import streamlit as st
from sqlalchemy import text

from core.db_loader import get_supabase_engine

# -------------------------
# util
# -------------------------
def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()

def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

def _clean_text(s: str) -> str:
    s = (s or "").replace("\x00", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _chunk_text(txt: str, chunk_chars: int = 1400, overlap: int = 180) -> List[str]:
    t = (txt or "").strip()
    if not t:
        return []
    out: List[str] = []
    i, n = 0, len(t)
    while i < n:
        j = min(n, i + chunk_chars)
        out.append(t[i:j])
        if j >= n:
            break
        i = max(0, j - overlap)
    return out

# -------------------------
# IPE CSV
# -------------------------
DEFAULT_IPE_CSV_URL_TEMPLATE = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{ano}.csv"

def _ipe_url_template() -> str:
    tpl = None
    try:
        if "IPE_CSV_URL_TEMPLATE" in st.secrets:
            tpl = str(st.secrets["IPE_CSV_URL_TEMPLATE"])
    except Exception:
        tpl = None
    tpl = tpl or os.getenv("IPE_CSV_URL_TEMPLATE") or DEFAULT_IPE_CSV_URL_TEMPLATE
    return tpl.strip()

@st.cache_data(ttl=600, show_spinner=False)
def _download_ipe_csv(year: int, timeout: int = 45) -> Tuple[Optional[pd.DataFrame], Optional[str], str]:
    url = _ipe_url_template().format(ano=year, year=year)
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 404:
            return None, f"404 Not Found", url
        r.raise_for_status()
        bio = io.BytesIO(r.content)
        # geralmente ';' + latin1
        for sep in (";", ","):
            for enc in ("latin1", "utf-8"):
                try:
                    bio.seek(0)
                    df = pd.read_csv(bio, sep=sep, encoding=enc, dtype=str)
                    if df is not None and len(df.columns) >= 5:
                        return df, None, url
                except Exception:
                    pass
        return None, "Falha ao ler CSV", url
    except Exception as e:
        return None, f"{type(e).__name__}: {e}", url

def _col(df: pd.DataFrame, *names: str) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for n in names:
        if n in df.columns:
            return n
        if n.lower() in lower:
            return lower[n.lower()]
    return None

def _colmap(df: pd.DataFrame) -> Dict[str, str]:
    return {
        "codigo_cvm": _col(df, "Codigo_CVM", "CODIGO_CVM", "Código_CVM") or "",
        "assunto": _col(df, "Assunto", "ASSUNTO") or "",
        "categoria": _col(df, "Categoria", "CATEGORIA") or "",
        "tipo": _col(df, "Tipo", "TIPO") or "",
        "especie": _col(df, "Especie", "Espécie", "ESPECIE") or "",
        "data_entrega": _col(df, "Data_Entrega", "DataEntrega") or "",
        "link": _col(df, "Link_Download", "LinkDownload", "LINK_DOWNLOAD") or "",
    }

STRATEGIC_RX = re.compile(
    r"(fato\s+relevante|comunicado\s+ao\s+mercado|aviso\s+aos\s+acionistas|" 
    r"resultado|earnings|itr|dfp|demonstra|balan|release\s+de\s+resultados|" 
    r"formul[aá]rio\s+de\s+refer|guidance|proje)",
    re.IGNORECASE,
)

def _is_strategic(row: pd.Series, m: Dict[str, str]) -> bool:
    blob = " | ".join(str(row.get(m[k], "") or "") for k in ("categoria","tipo","especie","assunto"))
    blob = _clean_text(blob)
    return bool(blob and STRATEGIC_RX.search(blob))

# -------------------------
# Supabase helpers
# -------------------------
def _get_cvm_map(tickers: Sequence[str]) -> Dict[str, int]:
    tks = [_norm_ticker(x) for x in tickers if str(x).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {}
    eng = get_supabase_engine()
    sql = text('SELECT UPPER("Ticker") AS ticker, "CVM"::int AS codigo_cvm FROM public.cvm_to_ticker WHERE UPPER("Ticker") = ANY(:arr)')
    with eng.begin() as conn:
        rows = conn.execute(sql, {"arr": tks}).fetchall()
    return {str(a).upper(): int(b) for a,b in rows}

def _upsert_doc(
    *,
    ticker: str,
    codigo_cvm: Optional[int],
    data: Optional[str],
    titulo: str,
    url: str,
    raw_text: str,
    chunk_chars: int,
    overlap: int,
    gerar_chunks: bool,
) -> bool:
    tk = _norm_ticker(ticker)
    doc_hash = _sha256("|".join([tk, str(codigo_cvm or ""), titulo, url, raw_text[:500]]))
    eng = get_supabase_engine()
    sql_doc = text("""
        INSERT INTO public.docs_corporativos (ticker, data, fonte, tipo, titulo, url, raw_text, doc_hash, codigo_cvm)
        VALUES (:ticker, :data, 'CVM', 'ipe', :titulo, :url, :raw_text, :doc_hash, :codigo_cvm)
        ON CONFLICT (doc_hash) DO NOTHING
        RETURNING id
    """)
    with eng.begin() as conn:
        res = conn.execute(sql_doc, {
            "ticker": tk,
            "data": data,
            "titulo": titulo,
            "url": url,
            "raw_text": raw_text,
            "doc_hash": doc_hash,
            "codigo_cvm": int(codigo_cvm) if codigo_cvm is not None else None,
        })
        row = res.first()
        if row is None:
            return False
        if not gerar_chunks:
            return True
        doc_id = int(row[0])
        chunks = _chunk_text(raw_text, chunk_chars=chunk_chars, overlap=overlap)
        if not chunks:
            return True
        sql_chunk = text("""
            INSERT INTO public.docs_corporativos_chunks (doc_id, ticker, chunk_index, chunk_text, chunk_hash)
            VALUES (:doc_id, :ticker, :chunk_index, :chunk_text, :chunk_hash)
            ON CONFLICT (chunk_hash) DO NOTHING
        """)
        for i, ch in enumerate(chunks):
            ch = ch.strip()
            if not ch:
                continue
            ch_hash = _sha256("|".join([str(doc_id), tk, str(i), ch]))
            conn.execute(sql_chunk, {
                "doc_id": doc_id,
                "ticker": tk,
                "chunk_index": int(i),
                "chunk_text": ch,
                "chunk_hash": ch_hash,
            })
    return True

# -------------------------
# Public API
# -------------------------
def ingest_ipe_for_tickers(
    tickers: Sequence[str],
    *,
    anos: Optional[int] = None,      # compat com a página
    anos_back: Optional[int] = 2,    # nome novo
    max_docs_por_ticker: int = 20,
    sleep_s: float = 0.0,
    chunk_chars: int = 1400,
    overlap: int = 180,
    gerar_chunks: bool = True,
) -> Dict[str, Any]:
    """Ingere docs IPE para tickers, usando código CVM e CSV público por ano."""
    if anos is not None:
        try:
            anos_back = int(anos)
        except Exception:
            pass
    anos_back = int(anos_back or 2)

    tks = [_norm_ticker(x) for x in (tickers or []) if str(x).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {"ok": False, "stats": {}, "errors": {"__all__": "Lista vazia"}}

    cvm_map = _get_cvm_map(tks)
    errors: Dict[str,str] = {}
    miss = [tk for tk in tks if tk not in cvm_map]
    if miss:
        errors["__missing_cvm__"] = "Sem código CVM em public.cvm_to_ticker para: " + ", ".join(miss)

    now_year = int(pd.Timestamp.utcnow().year)
    years = [now_year - i for i in range(0, anos_back + 1)]

    dfs: List[pd.DataFrame] = []
    years_status: Dict[str, Any] = {}
    for y in years:
        df, err, url = _download_ipe_csv(y)
        years_status[str(y)] = {"url": url, "rows": int(len(df)) if df is not None else 0, "error": err}
        if df is None:
            continue
        df["_ipe_year"] = str(y)
        dfs.append(df)

    if not dfs:
        return {"ok": False, "stats": {}, "errors": {**errors, "__ipe__": "Nenhum CSV IPE disponível"}, "years": years_status}

    all_df = pd.concat(dfs, ignore_index=True)
    m = _colmap(all_df)
    if not m.get("codigo_cvm"):
        return {"ok": False, "stats": {}, "errors": {**errors, "__ipe__": "CSV sem Codigo_CVM"}, "years": years_status, "columns": list(all_df.columns)}

    cc = m["codigo_cvm"]
    all_df[cc] = all_df[cc].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()

    stats: Dict[str, Dict[str,int]] = {tk: {"seen":0,"matched":0,"inserted":0,"skipped":0} for tk in tks}

    for tk in tks:
        codigo_cvm = cvm_map.get(tk)
        if not codigo_cvm:
            continue

        sub = all_df[all_df[cc] == str(codigo_cvm)].copy()
        stats[tk]["seen"] = int(len(sub))
        if sub.empty:
            continue

        sub = sub[sub.apply(lambda r: _is_strategic(r, m), axis=1)].copy()
        stats[tk]["matched"] = int(len(sub))
        if sub.empty:
            continue

        de = m.get("data_entrega", "")
        if de and de in sub.columns:
            sub["_dt"] = pd.to_datetime(sub[de], errors="coerce", dayfirst=True)
            sub = sub.sort_values("_dt", ascending=False)

        sub = sub.head(int(max_docs_por_ticker))

        for _, row in sub.iterrows():
            assunto = str(row.get(m["assunto"], "") or "").strip()
            categoria = str(row.get(m["categoria"], "") or "").strip()
            tipo = str(row.get(m["tipo"], "") or "").strip()
            especie = str(row.get(m["especie"], "") or "").strip()
            link = str(row.get(m["link"], "") or "").strip()
            ano = str(row.get("_ipe_year", "") or "").strip()

            data_iso = None
            if de and de in row.index:
                d = pd.to_datetime(str(row.get(de) or ""), errors="coerce", dayfirst=True)
                if pd.notna(d):
                    data_iso = d.date().isoformat()

            titulo = assunto or f"IPE {tk} ({ano})"
            raw_text = _clean_text(
                f"[CVM IPE] {tk} (CVM={codigo_cvm}) | {titulo} | Categoria={categoria} | Tipo={tipo} | Especie={especie} | Ano={ano} | Link={link}"
            )

            ins = _upsert_doc(
                ticker=tk,
                codigo_cvm=codigo_cvm,
                data=data_iso,
                titulo=titulo[:300],
                url=link[:1000],
                raw_text=raw_text,
                chunk_chars=int(chunk_chars),
                overlap=int(overlap),
                gerar_chunks=bool(gerar_chunks),
            )
            if ins:
                stats[tk]["inserted"] += 1
            else:
                stats[tk]["skipped"] += 1

            if sleep_s and float(sleep_s) > 0:
                time.sleep(float(sleep_s))

    return {"ok": (len(errors) == 0), "stats": stats, "errors": errors, "years": years_status, "ipe_url_template": _ipe_url_template()}
