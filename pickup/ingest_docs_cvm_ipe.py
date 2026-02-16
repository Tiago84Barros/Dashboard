from __future__ import annotations

"""
pickup/ingest_docs_cvm_ipe.py
----------------------------
OPÇÃO A (robusta): ingestão de metadados do IPE via *dados.cvm.gov.br* (dataset público),
filtrando por Código CVM a partir da tabela public.cvm_to_ticker (criada por você).

Por que isso existe?
- O endpoint RAD/ENET (ConsultaExternaCVM.aspx/ConsultarDocumentos) costuma mudar e/ou dar 404.
- O dataset de "dados abertos" da CVM é mais estável.

O que é ingerido?
- Por padrão, *metadados* do IPE (Assunto, Categoria, Tipo, Datas, Link_Download etc.).
- Quando o Link_Download apontar para HTML/TXT, tenta baixar e extrair texto.
- Se for PDF, **não faz OCR** (mantém metadados + link, para não ficar lento).

Tabelas esperadas (Supabase/Postgres):
- public.docs_corporativos (com UNIQUE(doc_hash))
- public.docs_corporativos_chunks (com UNIQUE(chunk_hash))
- public.cvm_to_ticker (colunas: "CVM" int, "Ticker" text)  ← você já criou

Dependências:
- requests, pandas, sqlalchemy, streamlit
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
import hashlib
import io
import re
import time
from datetime import datetime

import pandas as pd
import requests
import streamlit as st
from sqlalchemy import text

from core.db_loader import get_supabase_engine


# ───────────────────────── Helpers ─────────────────────────

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


# ───────────────────────── CVM DADOS ABERTOS ─────────────────────────

# Tentativas:
# 1) Tenta descobrir os arquivos anuais via index do diretório (mais robusto).
# 2) Fallback: gera URLs por ano (últimos N anos) e testa.
#
# Observação importante:
# No portal dados.cvm.gov.br, o IPE costuma estar disponível como ZIP anual:
#   https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_YYYY.zip
# e o ZIP contém CSV(s) com metadados e links de download.
IPE_DIR_INDEX_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/"

def _discover_ipe_year_urls(years_back: int = 6, timeout: int = 20) -> Tuple[List[str], Dict[str, Any]]:
    """Descobre URLs anuais do IPE (ZIP) a partir do index do diretório.

    Retorna (urls_ordenadas_desc, debug).
    """
    debug: Dict[str, Any] = {"index_url": IPE_DIR_INDEX_URL, "discovered": [], "generated": []}
    urls: List[str] = []
    try:
        r = requests.get(IPE_DIR_INDEX_URL, timeout=timeout)
        r.raise_for_status()
        html = r.text or ""
        years = sorted({int(y) for y in re.findall(r"ipe_cia_aberta_(\d{4})\.zip", html)}, reverse=True)
        if years:
            # limita ao intervalo desejado (anos_back)
            cur = datetime.utcnow().year
            min_year = max(1900, cur - years_back + 1)
            years = [y for y in years if y >= min_year]
            for y in years:
                urls.append(f"{IPE_DIR_INDEX_URL}ipe_cia_aberta_{y}.zip")
        debug["discovered"] = urls[:]
    except Exception as e:
        debug["index_error"] = f"{type(e).__name__}: {e}"

    if urls:
        return urls, debug

    # fallback: gera URLs por ano
    cur = datetime.utcnow().year
    gen = [f"{IPE_DIR_INDEX_URL}ipe_cia_aberta_{y}.zip" for y in range(cur, cur - years_back, -1)]
    debug["generated"] = gen[:]
    return gen, debug

def _fetch_working_ipe_urls(years_back: int, timeout: int = 45) -> Tuple[List[str], Dict[str, Any]]:
    """Testa URLs anuais do IPE e retorna as que estão disponíveis (HTTP 200)."""
    candidates, debug = _discover_ipe_year_urls(years_back=years_back, timeout=min(20, timeout))
    debug["candidates_tested"] = []
    ok_urls: List[str] = []
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; DashboardAnalise/1.0; +https://dados.cvm.gov.br/)"
    }
    for url in candidates:
        try:
            r = requests.get(url, stream=True, timeout=timeout, headers=headers)
            ok = (r.status_code == 200)
            debug["candidates_tested"].append({"url": url, "status": r.status_code, "ok": ok})
            if ok:
                ok_urls.append(url)
        except Exception as e:
            debug["candidates_tested"].append({"url": url, "error": f"{type(e).__name__}: {e}", "ok": False})
    return ok_urls, debug


@st.cache_data(ttl=60 * 30, show_spinner=False)
def _load_ipe_dataframe(url: str) -> pd.DataFrame:
    """
    Carrega o dataset IPE (CSV) em DataFrame.
    Observação: o dataset costuma ser grande; cache ajuda muito.
    """
    # download em memória
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    content = r.content

    # Se vier ZIP, tenta abrir o primeiro CSV dentro
    if url.lower().endswith(".zip"):
        import zipfile
        z = zipfile.ZipFile(io.BytesIO(content))
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise RuntimeError("ZIP baixado mas nenhum CSV encontrado.")
        with z.open(names[0]) as f:
            content = f.read()

    # CSV da CVM costuma ser ';' e latin1
    # Se falhar, tenta ',' e utf-8.
    for sep, enc in [(";", "latin1"), (";", "utf-8"), (",", "utf-8"), (",", "latin1")]:
        try:
            df = pd.read_csv(io.BytesIO(content), sep=sep, encoding=enc, low_memory=False)
            if len(df.columns) >= 5:
                return df
        except Exception:
            continue

    raise RuntimeError("Falha ao ler CSV do IPE com separadores/encodings testados.")


def _get_cvm_codes_for_tickers(tickers: Sequence[str]) -> Dict[str, int]:
    """
    Busca na tabela public.cvm_to_ticker: retorna {TICKER: CODIGO_CVM}.
    """
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {}

    engine = get_supabase_engine()
    sql = text(
        """
        select upper("Ticker") as ticker, "CVM" as codigo_cvm
        from public.cvm_to_ticker
        where upper("Ticker") = any(:tks)
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql, {"tks": tks}).fetchall()

    out: Dict[str, int] = {}
    for r in rows:
        out[str(r[0]).upper()] = int(r[1])
    return out


def _extract_text_from_link(url: str, timeout: int = 45) -> str:
    """
    Tenta baixar e extrair texto somente quando for HTML/TXT.
    PDF é ignorado (sem OCR).
    """
    u = (url or "").strip()
    if not u:
        return ""
    low = u.lower()
    if any(low.endswith(ext) for ext in [".pdf", ".zip", ".rar"]):
        return ""
    # se não tiver extensão, ainda pode ser HTML — tenta, mas limita tamanho
    try:
        r = requests.get(u, timeout=timeout)
        r.raise_for_status()
        ctype = (r.headers.get("content-type") or "").lower()

        # corta respostas gigantes
        raw = r.text
        raw = raw[:200_000]

        if "html" in ctype:
            # strip tags simples
            raw = re.sub(r"(?is)<(script|style).*?>.*?(</\1>)", " ", raw)
            raw = re.sub(r"(?is)<.*?>", " ", raw)
            raw = re.sub(r"\s+", " ", raw).strip()
            return raw
        if "text" in ctype or low.endswith(".txt"):
            return re.sub(r"\s+", " ", raw).strip()

        # fallback: tenta limpar como texto mesmo
        if len(raw) > 0 and len(raw) < 200_000:
            raw = re.sub(r"\s+", " ", raw).strip()
            return raw
    except Exception:
        return ""
    return ""


def _build_raw_text_from_row(row: Dict[str, Any]) -> str:
    """
    Gera um texto mínimo (metadados) para indexação.
    """
    parts = []
    def add(k: str, label: str):
        v = row.get(k)
        if v is None:
            return
        s = str(v).strip()
        if s and s.lower() != "nan":
            parts.append(f"{label}: {s}")
    add("Nome_Companhia", "Empresa")
    add("Codigo_CVM", "Codigo_CVM")
    add("CNPJ_Companhia", "CNPJ")
    add("Categoria", "Categoria")
    add("Tipo", "Tipo")
    add("Especie", "Especie")
    add("Assunto", "Assunto")
    add("Data_Referencia", "Data_Referencia")
    add("Data_Entrega", "Data_Entrega")
    add("Tipo_Apresentacao", "Tipo_Apresentacao")
    add("Protocolo_Entrega", "Protocolo_Entrega")
    add("Versao", "Versao")
    add("Link_Download", "Link")
    return _clean_text(" | ".join(parts))


def _upsert_doc_and_chunks(
    *,
    ticker: str,
    data: Optional[str],
    fonte: str,
    tipo: str,
    titulo: str,
    url: str,
    raw_text: str,
    chunk_chars: int = 1500,
    overlap: int = 200,
) -> Tuple[bool, str]:
    """
    Insere doc + chunks. Retorna (inseriu, doc_hash).
    """
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
        insert into public.docs_corporativos (ticker, data, fonte, tipo, titulo, url, raw_text, doc_hash)
        values (:ticker, :data, :fonte, :tipo, :titulo, :url, :raw_text, :doc_hash)
        on conflict (doc_hash) do nothing
        returning id
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
            insert into public.docs_corporativos_chunks (doc_id, ticker, chunk_index, chunk_text, chunk_hash)
            values (:doc_id, :ticker, :chunk_index, :chunk_text, :chunk_hash)
            on conflict (chunk_hash) do nothing
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


# ───────────────────────── API pública ─────────────────────────

def ingest_ipe_for_tickers(
    tickers: Sequence[str],
    *,
    years: int = 2,
    max_docs_por_ticker: int = 25,
    sleep_s: float = 0.05,
    chunk_chars: int = 1500,
    overlap: int = 200,
    fetch_html_text: bool = True,
) -> Dict[str, Any]:
    """
    Ingestão IPE por tickers usando:
      1) public.cvm_to_ticker -> Codigo_CVM
      2) dataset IPE (dados.cvm.gov.br) filtrado por Codigo_CVM
      3) upsert em docs_corporativos (+ chunks)

    Retorno:
      {
        "ok": bool,
        "url_ipe": str|None,
        "stats": { "TICKER": {"matched":N,"inserted":M,"skipped":K} },
        "errors": { "TICKER": "...", "__ipe__": "..."},
        "debug": {...}
      }
    """
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {"ok": False, "url_ipe": None, "stats": {}, "errors": {"__all__": "Lista de tickers vazia."}, "debug": {}}

    # 1) map tickers -> codigo_cvm
    tk2cvm = _get_cvm_codes_for_tickers(tks)
    missing = [t for t in tks if t not in tk2cvm]
    if missing:
        return {
            "ok": False,
            "url_ipe": None,
            "stats": {},
            "errors": {"__map__": f"Tickers sem mapeamento em public.cvm_to_ticker: {missing}"},
            "debug": {"have": tk2cvm},
        }

    # 2) encontra URLs anuais do dataset IPE (ZIP por ano) conforme janela desejada
    # years=2 => tenta ano corrente e anterior (e assim por diante).
    years_back = max(1, int(years or 1))
    url_list, debug = _fetch_working_ipe_urls(years_back=years_back)
    if not url_list:
        return {
            "ok": False,
            "url_ipe": None,
            "stats": {},
            "errors": {"__ipe__": "Nenhum ZIP anual do IPE disponível (todas as URLs candidatas falharam)."},
            "debug": debug,
        }
    url_ipe = url_list[0]  # mantém compatibilidade com retornos/telemetria

    # 3) carrega dataframe (concat dos anos disponíveis)
    dfs: List[pd.DataFrame] = []
    last_err: Optional[str] = None
    for u in url_list:
        try:
            dfs.append(_load_ipe_dataframe(u))
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
    if not dfs:
        return {
            "ok": False,
            "url_ipe": url_list[0],
            "stats": {},
            "errors": {"__ipe__": f"Falha ao carregar ZIPs do IPE (ex.: {last_err})"},
            "debug": debug,
        }
    df = pd.concat(dfs, ignore_index=True, sort=False)

    # normaliza nomes de colunas conhecidos
    # (a CVM pode alterar; aqui é tolerante)
    cols = {c.lower(): c for c in df.columns}
    def col(name: str) -> Optional[str]:
        return cols.get(name.lower())

    c_cvm = col("Codigo_CVM") or col("CodigoCVM") or col("codigo_cvm")
    c_dt = col("Data_Entrega") or col("DataEntrega") or col("data_entrega")
    c_assunto = col("Assunto") or col("assunto")
    c_link = col("Link_Download") or col("LinkDownload") or col("link_download")

    if not c_cvm:
        return {
            "ok": False,
            "url_ipe": url_ipe,
            "stats": {},
            "errors": {"__ipe__": f"CSV IPE sem coluna Codigo_CVM. Colunas: {list(df.columns)[:50]}"},
            "debug": debug,
        }

    # filtra por códigos CVM
    cvm_codes = sorted(set(tk2cvm.values()))
    df2 = df[df[c_cvm].isin(cvm_codes)].copy()

    # filtra por janela de anos se Data_Entrega existir
    if c_dt:
        dt = pd.to_datetime(df2[c_dt], errors="coerce", dayfirst=True)
        df2["_dt"] = dt
        cutoff = (pd.Timestamp.utcnow().tz_localize(None).normalize()
                  - pd.Timedelta(days=365 * max(0, int(years))))
        # garante comparação tz-naive (evita: Invalid comparison between dtype=datetime64[ns] and Timestamp)
        df2["_dt"] = pd.to_datetime(df2["_dt"], errors="coerce", utc=True).dt.tz_convert(None)
        df2 = df2[df2["_dt"].isna() | (df2["_dt"] >= cutoff)].copy()

    stats: Dict[str, Dict[str, int]] = {tk: {"matched": 0, "inserted": 0, "skipped": 0} for tk in tks}
    errors: Dict[str, str] = {}

    # index por codigo_cvm -> ticker
    cvm2tk = {v: k for k, v in tk2cvm.items()}

    # ordena por data (desc) se tiver
    if c_dt:
        df2 = df2.sort_values(by=c_dt, ascending=False)

    # loop
    for cvm_code, grp in df2.groupby(c_cvm):
        tk = cvm2tk.get(int(cvm_code))
        if not tk:
            continue

        rows = grp.to_dict(orient="records")
        stats[tk]["matched"] = int(len(rows))

        for row in rows[: int(max_docs_por_ticker)]:
            try:
                titulo = str(row.get(c_assunto) or "").strip() if c_assunto else ""
                url = str(row.get(c_link) or "").strip() if c_link else ""

                # data ISO
                data_iso = None
                if c_dt:
                    d = pd.to_datetime(row.get(c_dt), errors="coerce", dayfirst=True)
                    if pd.notna(d):
                        data_iso = d.date().isoformat()

                raw_text = _build_raw_text_from_row(row)

                # tenta enriquecer com texto do link (só html/txt)
                if fetch_html_text and url:
                    extra = _extract_text_from_link(url)
                    if extra:
                        raw_text = _clean_text(raw_text + " | Conteudo: " + extra)

                if not raw_text:
                    stats[tk]["skipped"] += 1
                    continue

                inserted, _ = _upsert_doc_and_chunks(
                    ticker=tk,
                    data=data_iso,
                    fonte="CVM",
                    tipo="ipe",
                    titulo=titulo,
                    url=url,
                    raw_text=raw_text,
                    chunk_chars=int(chunk_chars),
                    overlap=int(overlap),
                )
                if inserted:
                    stats[tk]["inserted"] += 1
                else:
                    stats[tk]["skipped"] += 1

                if sleep_s and float(sleep_s) > 0:
                    time.sleep(float(sleep_s))
            except Exception as e:
                errors[tk] = f"{type(e).__name__}: {e}"

    ok = (len(errors) == 0)
    return {"ok": ok, "url_ipe": url_ipe, "stats": stats, "errors": errors, "debug": debug}