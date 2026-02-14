from __future__ import annotations

"""
pickup/ingest_docs_cvm_ipe.py
----------------------------
Ingestão de comunicados da CVM (IPE) para Supabase via dataset oficial:
- baixa ZIP anual do IPE em dados.cvm.gov.br
- lê CSV(s) do ZIP
- mapeia companhia -> ticker usando public.setores (nome_empresa)
- salva doc em public.docs_corporativos
- cria chunks em public.docs_corporativos_chunks (opcional)

Objetivo: abastecer o Patch 6 (RAG) com fontes oficiais.

NOTAS IMPORTANTES (MVP):
- O dataset IPE costuma trazer metadados (assunto, categoria, link). Nem sempre traz texto completo.
- Neste MVP, raw_text é composto por metadados + assunto (o suficiente para provar o pipeline).
- Evolução natural: baixar o PDF/HTML do link e extrair texto (mais custo/complexidade).
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
import hashlib
import io
import re
import time
import zipfile
import unicodedata

import pandas as pd
import streamlit as st
import requests
from sqlalchemy import text

from core.db_loader import get_supabase_engine, load_setores_from_db


# =============================================================================
# Util
# =============================================================================

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

def _strip_accents(s: str) -> str:
    s = s or ""
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(ch)
    )

def _norm_name(s: str) -> str:
    """
    Normalização agressiva para casar nome de companhia:
    - upper
    - remove acentos
    - remove pontuação
    - remove termos comuns (S/A, SA, CIA, COMPANHIA etc.)
    - colapsa espaços
    """
    s = _strip_accents(str(s or "").upper())
    s = re.sub(r"[^\w\s]", " ", s)  # tira pontuação
    s = re.sub(r"\b(SA|S A|S A\.|S\/A|S\.A\.|S\.A|CIA|CI A|COMPANHIA|COMPANHIA\s+ABERTA|INDUSTRIA|INDUSTRIAS|HOLDING)\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# =============================================================================
# CVM IPE dataset oficial (dados.cvm.gov.br)
# =============================================================================

# Padrão anual (ex): ipe_cia_aberta_2026.zip
CVM_IPE_ZIP_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{year}.zip"


@st.cache_data(show_spinner=False, ttl=3600)
def _download_ipe_zip(year: int, timeout: int = 60) -> bytes:
    """
    Baixa o ZIP do IPE do ano especificado. Cacheado 1h para não repetir download.
    """
    url = CVM_IPE_ZIP_URL.format(year=int(year))
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content


def _read_ipe_zip_to_df(zip_bytes: bytes) -> pd.DataFrame:
    """
    Lê o(s) CSV(s) do ZIP e concatena num DataFrame.
    Observação: encoding pode variar; tentamos utf-8 e latin1.
    """
    if not zip_bytes:
        return pd.DataFrame()

    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]

    if not csv_names:
        return pd.DataFrame()

    dfs = []
    for name in csv_names:
        raw = zf.read(name)
        # tenta utf-8, depois latin1
        for enc in ("utf-8", "latin1"):
            try:
                df = pd.read_csv(
                    io.BytesIO(raw),
                    sep=";",
                    encoding=enc,
                    dtype=str,
                    low_memory=False,
                )
                dfs.append(df)
                break
            except Exception:
                continue

    if not dfs:
        return pd.DataFrame()

    out = pd.concat(dfs, ignore_index=True)
    return out


def _pick_col(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    cols = {c.upper(): c for c in df.columns}
    for cand in candidates:
        if cand.upper() in cols:
            return cols[cand.upper()]
    return None


def _build_ticker_name_map(tickers: Sequence[str]) -> Dict[str, List[str]]:
    """
    Retorna {TICKER: [variantes_normalizadas_de_nome]} baseado em public.setores.
    Só monta para os tickers solicitados (para ficar rápido).
    """
    setores = load_setores_from_db()
    if setores is None or setores.empty:
        return {}

    tks = {_norm_ticker(t) for t in tickers if str(t).strip()}
    if not tks:
        return {}

    df = setores.copy()
    if "ticker" not in df.columns:
        return {}

    df["ticker"] = df["ticker"].astype(str).str.replace(".SA", "", regex=False).str.upper().str.strip()
    df = df[df["ticker"].isin(tks)]

    # nome_empresa costuma existir no seu loader
    if "nome_empresa" not in df.columns:
        return {tk: [] for tk in tks}

    out: Dict[str, List[str]] = {}
    for tk, g in df.groupby("ticker"):
        nomes = []
        for nm in g["nome_empresa"].dropna().astype(str).tolist():
            nn = _norm_name(nm)
            if nn:
                nomes.append(nn)
        # variantes adicionais (remove palavras finais comuns)
        uniq = list(dict.fromkeys([n for n in nomes if n]))
        out[tk] = uniq

    return out


def _match_ticker_by_company_name(
    company_name_raw: str,
    ticker_name_map: Dict[str, List[str]],
) -> Optional[str]:
    """
    Heurística leve e rápida:
    - normaliza nome do IPE
    - tenta match exato com alguma variante do ticker
    - tenta match por substring (um dentro do outro)
    """
    cn = _norm_name(company_name_raw)
    if not cn:
        return None

    # 1) exato
    for tk, variants in ticker_name_map.items():
        if cn in variants:
            return tk

    # 2) substring (evita casar coisas muito curtas)
    for tk, variants in ticker_name_map.items():
        for v in variants:
            if len(v) >= 10 and (v in cn or cn in v):
                return tk

    return None


# =============================================================================
# Supabase upsert
# =============================================================================

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

    # Hash estável
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

        chunks = _chunk_text(raw_text, chunk_chars=chunk_chars, overlap=overlap)
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


# =============================================================================
# Public API (importada pelas páginas)
# =============================================================================

def ingest_ipe_for_tickers(
    tickers: Sequence[str],
    *,
    anos: int = 2,
    max_docs_por_ticker: int = 50,
    sleep_s: float = 0.0,
    chunk_chars: int = 1500,
    overlap: int = 200,
    timeout_download: int = 60,
) -> Dict[str, Any]:
    """
    Ingestão via dataset oficial anual (dados.cvm.gov.br).

    Retorno:
      {
        "ok": bool,
        "stats": { "TICKER": {"seen":N,"matched":X,"inserted":M,"skipped":K} },
        "errors": { "TICKER/ANO/__all__": "mensagem" }
      }
    """
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {"ok": False, "stats": {}, "errors": {"__all__": "Lista de tickers vazia."}}

    # mapa ticker -> nomes normalizados (do seu public.setores)
    ticker_name_map = _build_ticker_name_map(tks)
    if not ticker_name_map:
        return {
            "ok": False,
            "stats": {tk: {"seen": 0, "matched": 0, "inserted": 0, "skipped": 0} for tk in tks},
            "errors": {"__all__": "Não foi possível carregar/usar public.setores para mapear nome_empresa -> ticker."},
        }

    # Janela de anos
    ano_fim = int(pd.Timestamp.utcnow().year)
    anos = max(0, int(anos))
    years = list(range(ano_fim, max(ano_fim - anos, 1990), -1))
    if not years:
        years = [ano_fim]

    stats: Dict[str, Dict[str, int]] = {
        tk: {"seen": 0, "matched": 0, "inserted": 0, "skipped": 0} for tk in tks
    }
    errors: Dict[str, str] = {}

    # Vamos acumulando docs por ticker para respeitar max_docs_por_ticker
    per_ticker_docs: Dict[str, int] = {tk: 0 for tk in tks}

    try:
        for yr in years:
            # Se todos já atingiram limite, para cedo
            if all(per_ticker_docs[tk] >= int(max_docs_por_ticker) for tk in tks):
                break

            try:
                zip_bytes = _download_ipe_zip(int(yr), timeout=int(timeout_download))
            except Exception as e:
                errors[f"ANO_{yr}"] = f"{type(e).__name__}: {e}"
                continue

            df = _read_ipe_zip_to_df(zip_bytes)
            st.write(f"Colunas encontradas no IPE {yr}:", list(df.columns))
            if df is None or df.empty:
                continue

            # tenta achar colunas relevantes (nome companhia, datas, assunto, link)
            col_nome = _pick_col(df, ["DENOM_CIA", "NOME_CIA", "DENOMINACAO_SOCIAL", "DENOM_SOCIAL", "EMPRESA", "NOME_EMPRESA"])
            col_assunto = _pick_col(df, ["ASSUNTO", "DESCRICAO_ASSUNTO", "TITULO", "TEXTO", "RESUMO"])
            col_dt = _pick_col(df, ["DT_RECEB", "DT_REFER", "DT_ENTREGA", "DATA", "DATA_RECEBIMENTO", "DATA_REFERENCIA"])
            col_link = _pick_col(df, ["LINK_DOC", "LINK", "URL", "URL_DOWNLOAD", "LINK_DOWNLOAD"])
            col_tipo = _pick_col(df, ["CATEG_DOC", "CATEGORIA", "TP_DOC", "TIPO_DOC", "ESPECIE"])

            if not col_nome:
                # sem nome de companhia, não dá pra mapear
                errors[f"ANO_{yr}"] = errors.get(f"ANO_{yr}", "") + " | CSV sem coluna de nome de companhia."
                continue

            # percorre linhas
            for _, row in df.iterrows():
                # limites por ticker
                if all(per_ticker_docs[tk] >= int(max_docs_por_ticker) for tk in tks):
                    break

                nome_cia = str(row.get(col_nome) or "").strip()
                if not nome_cia:
                    continue

                tk = _match_ticker_by_company_name(nome_cia, ticker_name_map)
                if not tk:
                    continue

                # respeita limite do ticker
                if per_ticker_docs[tk] >= int(max_docs_por_ticker):
                    continue

                stats[tk]["seen"] += 1
                stats[tk]["matched"] += 1

                # data
                data_iso = None
                if col_dt:
                    v = row.get(col_dt)
                    if isinstance(v, str) and v.strip():
                        d = pd.to_datetime(v, errors="coerce", dayfirst=True)
                        if pd.notna(d):
                            data_iso = d.date().isoformat()

                assunto = ""
                if col_assunto:
                    assunto = str(row.get(col_assunto) or "").strip()

                link = ""
                if col_link:
                    link = str(row.get(col_link) or "").strip()

                tipo_doc = "ipe"
                if col_tipo:
                    td = str(row.get(col_tipo) or "").strip()
                    if td:
                        tipo_doc = f"ipe:{td[:60]}"

                # raw_text (MVP: metadados + assunto)
                raw_text = _clean_text(
                    "\n".join([
                        f"Fonte: CVM IPE (dataset {yr})",
                        f"Companhia: {nome_cia}",
                        f"Ticker: {tk}",
                        f"Data: {data_iso or ''}",
                        f"Tipo: {tipo_doc}",
                        f"Assunto: {assunto}",
                        f"Link: {link}",
                    ])
                )

                if not raw_text:
                    stats[tk]["skipped"] += 1
                    continue

                inserted, _ = _upsert_doc_and_chunks(
                    ticker=tk,
                    data=data_iso,
                    fonte="CVM",
                    tipo=tipo_doc,
                    titulo=assunto[:240] if assunto else f"IPE {yr}",
                    url=link,
                    raw_text=raw_text,
                    chunk_chars=int(chunk_chars),
                    overlap=int(overlap),
                )

                if inserted:
                    stats[tk]["inserted"] += 1
                else:
                    stats[tk]["skipped"] += 1

                per_ticker_docs[tk] += 1

                if sleep_s and float(sleep_s) > 0:
                    time.sleep(float(sleep_s))

    except Exception as e:
        errors["__all__"] = f"{type(e).__name__}: {e}"

    ok = True
    # ok é “pipeline rodou”; erros de ano podem existir sem travar tudo
    # mas se nada inseriu, consideramos ok=False para chamar atenção
    total_inserted = sum(stats[tk]["inserted"] for tk in stats)
    if total_inserted == 0:
        ok = False

    return {"ok": ok, "stats": stats, "errors": errors}
