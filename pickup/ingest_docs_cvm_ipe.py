from __future__ import annotations

"""
pickup/ingest_docs_cvm_ipe.py
----------------------------
Ingestão (MVP robusto) de comunicados da CVM (IPE) para Supabase:
- salva doc em public.docs_corporativos
- cria chunks em public.docs_corporativos_chunks (opcional)

Estratégia (robusta):
- ticker -> codigo CVM via public.cvm_to_ticker
- baixa CSV IPE por ano (dados.cvm.gov.br)
- filtra APENAS documentos "estratégicos"
- insere metadados + texto curto (Assunto/Tipo/Categoria + link)

Notas:
- O CSV do IPE normalmente NÃO traz o conteúdo do PDF/ZIP, só o link.
  Então o raw_text é "metadata-rich", mas não é o texto integral do documento.
- Pensado para rodar dentro do Streamlit (button): sem loops infinitos,
  com fallback de erros e sem travar a UI.
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
import hashlib
import io
import re
import time

import pandas as pd
import requests
import streamlit as st
from sqlalchemy import text

from core.db_loader import get_supabase_engine


# -----------------------------------------------------------------------------
# Util
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# IPE CSV URLs (padrão + override via secrets/env)
# -----------------------------------------------------------------------------
# Template default que costuma funcionar:
# https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{ano}.csv
DEFAULT_IPE_CSV_URL_TEMPLATE = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{ano}.csv"


def _get_ipe_csv_url_template() -> str:
    """
    Permite override por:
      - st.secrets["IPE_CSV_URL_TEMPLATE"]
      - env IPE_CSV_URL_TEMPLATE
    """
    import os

    tpl = None
    try:
        if "IPE_CSV_URL_TEMPLATE" in st.secrets:
            tpl = str(st.secrets["IPE_CSV_URL_TEMPLATE"])
    except Exception:
        pass

    if not tpl:
        tpl = os.getenv("IPE_CSV_URL_TEMPLATE")

    if not tpl:
        tpl = DEFAULT_IPE_CSV_URL_TEMPLATE

    return tpl.strip()


# -----------------------------------------------------------------------------
# Leitura / Download CSV do IPE
# -----------------------------------------------------------------------------
@st.cache_data(ttl=60 * 10, show_spinner=False)
def _download_ipe_csv(year: int, timeout: int = 45) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Baixa CSV do IPE para um ano.
    Retorna (df, error). Se 404, retorna (None, "404").
    """
    url_tpl = _get_ipe_csv_url_template()
    url = url_tpl.format(ano=year, year=year)

    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 404:
            return None, f"404: {url}"
        r.raise_for_status()

        # CSV da CVM costuma vir com ';' e encoding latin1
        content = r.content
        bio = io.BytesIO(content)

        # tenta alguns formatos
        for sep in (";", ","):
            for enc in ("latin1", "utf-8"):
                try:
                    df = pd.read_csv(bio, sep=sep, encoding=enc, dtype=str)
                    if df is not None and len(df.columns) >= 3:
                        return df, None
                except Exception:
                    bio.seek(0)

        return None, f"Falha ao ler CSV: {url}"

    except requests.exceptions.RequestException as e:
        return None, f"RequestException: {e}"
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def _find_best_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Mapeia colunas esperadas, aceitando variações.
    """
    cols = {c: c for c in df.columns}

    def pick(*names: str) -> Optional[str]:
        for n in names:
            if n in cols:
                return n
        # tentativa case-insensitive
        lower = {c.lower(): c for c in df.columns}
        for n in names:
            k = n.lower()
            if k in lower:
                return lower[k]
        return None

    mapping = {
        "codigo_cvm": pick("Codigo_CVM", "CodigoCVM", "CODIGO_CVM", "Código_CVM"),
        "nome": pick("Nome_Companhia", "NomeCompanhia", "NOME_COMPANHIA"),
        "cnpj": pick("CNPJ_Companhia", "CNPJCompanhia"),
        "data_entrega": pick("Data_Entrega", "DataEntrega"),
        "data_referencia": pick("Data_Referencia", "DataReferencia"),
        "categoria": pick("Categoria", "CATEGORIA"),
        "tipo": pick("Tipo", "TIPO"),
        "especie": pick("Especie", "Espécie", "ESPECIE"),
        "assunto": pick("Assunto", "ASSUNTO"),
        "tipo_apresentacao": pick("Tipo_Apresentacao", "TipoApresentacao"),
        "protocolo": pick("Protocolo_Entrega", "ProtocoloEntrega"),
        "versao": pick("Versao", "VERSAO"),
        "link": pick("Link_Download", "LinkDownload", "LINK_DOWNLOAD"),
    }
    return {k: (v or "") for k, v in mapping.items()}


# -----------------------------------------------------------------------------
# Filtro: documentos estratégicos
# -----------------------------------------------------------------------------
# A CVM muda a nomenclatura em alguns casos; por isso, usamos regex/contains.
STRATEGIC_REGEX = re.compile(
    r"(fato\s+relevante|comunicado\s+ao\s+mercado|"
    r"aviso\s+aos\s+acionistas|"
    r"resultado|earnings|"
    r"itr|dfp|demonstra(ç|c)õ|balan(ç|c)o|"
    r"release\s+de\s+resultados|"
    r"formul[aá]rio\s+de\s+refer(ê|e)ncia|"
    r"guidance|proje(ç|c)(ã|a)o)",
    re.IGNORECASE,
)


def _is_strategic_row(row: pd.Series, colmap: Dict[str, str]) -> bool:
    def get(colkey: str) -> str:
        c = colmap.get(colkey, "")
        if not c:
            return ""
        v = row.get(c, "")
        return str(v or "")

    blob = " | ".join(
        [
            get("categoria"),
            get("tipo"),
            get("especie"),
            get("assunto"),
            get("tipo_apresentacao"),
        ]
    )
    blob = _clean_text(blob)

    if not blob:
        return False

    return bool(STRATEGIC_REGEX.search(blob))


# -----------------------------------------------------------------------------
# Supabase upsert
# -----------------------------------------------------------------------------
def _upsert_doc_and_chunks(
    *,
    ticker: str,
    codigo_cvm: Optional[int],
    data: Optional[str],
    fonte: str,
    tipo: str,
    titulo: str,
    url: str,
    raw_text: str,
    chunk_chars: int = 1500,
    overlap: int = 200,
    gerar_chunks: bool = True,
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

    # doc_hash estável: prioriza protocolo/url + ticker + cvm + título
    doc_hash = _sha256("|".join([tk, str(codigo_cvm or ""), fonte, tipo, titulo, url, raw_text[:500]]))

    engine = get_supabase_engine()

    sql_doc = text(
        """
        INSERT INTO public.docs_corporativos
            (ticker, data, fonte, tipo, titulo, url, raw_text, doc_hash, codigo_cvm)
        VALUES
            (:ticker, :data, :fonte, :tipo, :titulo, :url, :raw_text, :doc_hash, :codigo_cvm)
        ON CONFLICT (doc_hash) DO NOTHING
        RETURNING id
        """
    )

    with engine.begin() as conn:
        res = conn.execute(
            sql_doc,
            {
                "ticker": tk,
                "data": data,
                "fonte": fonte,
                "tipo": tipo,
                "titulo": titulo,
                "url": url,
                "raw_text": raw_text,
                "doc_hash": doc_hash,
                "codigo_cvm": int(codigo_cvm) if codigo_cvm is not None else None,
            },
        )
        row = res.first()
        if row is None:
            return False, doc_hash

        if not gerar_chunks:
            return True, doc_hash

        doc_id = int(row[0])
        chunks = _chunk_text(raw_text, chunk_chars=chunk_chars, overlap=overlap)
        if not chunks:
            return True, doc_hash

        sql_chunk = text(
            """
            INSERT INTO public.docs_corporativos_chunks
                (doc_id, ticker, chunk_index, chunk_text, chunk_hash)
            VALUES
                (:doc_id, :ticker, :chunk_index, :chunk_text, :chunk_hash)
            ON CONFLICT (chunk_hash) DO NOTHING
            """
        )

        for i, ch in enumerate(chunks):
            ch_clean = ch.strip()
            if not ch_clean:
                continue
            ch_hash = _sha256("|".join([str(doc_id), tk, str(i), ch_clean]))
            conn.execute(
                sql_chunk,
                {
                    "doc_id": doc_id,
                    "ticker": tk,
                    "chunk_index": int(i),
                    "chunk_text": ch_clean,
                    "chunk_hash": ch_hash,
                },
            )

    return True, doc_hash


# -----------------------------------------------------------------------------
# Map ticker -> CVM (public.cvm_to_ticker)
# -----------------------------------------------------------------------------
def _get_cvm_codes_for_tickers(tickers: Sequence[str]) -> Dict[str, int]:
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {}

    engine = get_supabase_engine()

    # sua tabela é public.cvm_to_ticker com colunas "CVM" e "Ticker"
    # usamos UPPER para garantir match.
    sql = text(
        """
        SELECT UPPER("Ticker") AS ticker, "CVM"::int AS codigo_cvm
        FROM public.cvm_to_ticker
        WHERE UPPER("Ticker") = ANY(:arr)
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(sql, {"arr": tks}).fetchall()

    out: Dict[str, int] = {}
    for r in rows:
        out[str(r[0]).upper()] = int(r[1])
    return out


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def ingest_ipe_for_tickers(
    tickers: Sequence[str],
    *,
    anos_back: int = 3,
    max_docs_por_ticker: int = 30,
    sleep_s: float = 0.0,
    chunk_chars: int = 1400,
    overlap: int = 180,
    gerar_chunks: bool = True,
    debug_columns: bool = True,
) -> Dict[str, Any]:
    """
    Ingestão IPE baseada em CSV por ano + filtro "estratégico", usando código CVM.

    Retorno:
      {
        "ok": bool,
        "stats": { "TICKER": {"seen":N,"matched":M,"inserted":I,"skipped":K} },
        "errors": { ... },
        "years": { ano: { "ok": bool, "rows": int, "error": str|None } },
        "columns": { ano: [colunas...] }  # se debug_columns=True
      }
    """
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {"ok": False, "stats": {}, "errors": {"__all__": "Lista de tickers vazia."}}

    # 1) ticker -> cvm
    cvm_map = _get_cvm_codes_for_tickers(tks)
    missing = [t for t in tks if t not in cvm_map]
    errors: Dict[str, str] = {}
    if missing:
        errors["__missing_cvm__"] = f"Sem código CVM para: {', '.join(missing)} (verifique public.cvm_to_ticker)."

    # 2) baixar anos (do atual para trás) e manter em cache local
    now_year = int(pd.Timestamp.utcnow().year)
    years = [now_year - i for i in range(0, max(1, int(anos_back)) + 1)]

    years_info: Dict[int, Dict[str, Any]] = {}
    columns_debug: Dict[int, List[str]] = {}

    dfs: List[pd.DataFrame] = []
    for y in years:
        df, err = _download_ipe_csv(y)
        if df is None:
            years_info[y] = {"ok": False, "rows": 0, "error": err}
            continue

        years_info[y] = {"ok": True, "rows": int(len(df)), "error": None}
        if debug_columns:
            columns_debug[y] = [str(c) for c in df.columns]

        df["_ipe_year"] = str(y)
        dfs.append(df)

    if not dfs:
        # nada baixado
        return {
            "ok": False,
            "stats": {},
            "errors": {**errors, "__ipe__": "Não foi possível baixar nenhum CSV IPE (todos falharam)."},
            "years": years_info,
            "columns": columns_debug if debug_columns else {},
        }

    # concat de anos baixados
    all_df = pd.concat(dfs, ignore_index=True)

    # colmap baseado no primeiro df (já aceita variações)
    colmap = _find_best_columns(all_df)

    # valida colunas mínimas
    if not colmap.get("codigo_cvm"):
        return {
            "ok": False,
            "stats": {},
            "errors": {**errors, "__ipe__": "CSV baixado mas sem coluna Codigo_CVM (mapeamento falhou)."},
            "years": years_info,
            "columns": columns_debug if debug_columns else {},
        }

    # garantir string
    cc_col = colmap["codigo_cvm"]
    all_df[cc_col] = all_df[cc_col].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()

    stats: Dict[str, Dict[str, int]] = {
        tk: {"seen": 0, "matched": 0, "inserted": 0, "skipped": 0} for tk in tks
    }

    # para cada ticker, filtra por CVM
    for tk in tks:
        codigo_cvm = cvm_map.get(tk)
        if not codigo_cvm:
            continue

        sub = all_df[all_df[cc_col] == str(codigo_cvm)].copy()
        stats[tk]["seen"] = int(len(sub))

        if sub.empty:
            continue

        # filtra estratégicos
        mask = sub.apply(lambda r: _is_strategic_row(r, colmap), axis=1)
        sub = sub[mask].copy()
        stats[tk]["matched"] = int(len(sub))

        if sub.empty:
            continue

        # ordena por Data_Entrega desc se existir
        de_col = colmap.get("data_entrega", "")
        if de_col and de_col in sub.columns:
            sub["_dt"] = pd.to_datetime(sub[de_col], errors="coerce", dayfirst=True)
            sub = sub.sort_values("_dt", ascending=False)

        # limita
        sub = sub.head(int(max_docs_por_ticker))

        for _, row in sub.iterrows():
            # monta "texto" rico em metadados (sem baixar PDF)
            assunto = str(row.get(colmap.get("assunto", ""), "") or "").strip()
            categoria = str(row.get(colmap.get("categoria", ""), "") or "").strip()
            tipo = str(row.get(colmap.get("tipo", ""), "") or "").strip()
            especie = str(row.get(colmap.get("especie", ""), "") or "").strip()
            tipo_ap = str(row.get(colmap.get("tipo_apresentacao", ""), "") or "").strip()
            protocolo = str(row.get(colmap.get("protocolo", ""), "") or "").strip()
            versao = str(row.get(colmap.get("versao", ""), "") or "").strip()
            link = str(row.get(colmap.get("link", ""), "") or "").strip()
            ano = str(row.get("_ipe_year", "") or "").strip()

            # data (preferência: Data_Entrega)
            data_iso = None
            if de_col and de_col in row.index:
                d = pd.to_datetime(str(row.get(de_col) or ""), errors="coerce", dayfirst=True)
                if pd.notna(d):
                    data_iso = d.date().isoformat()

            titulo = assunto or f"IPE {tk} ({ano})"
            raw_text = _clean_text(
                f"""
                [CVM IPE] {tk} (CVM={codigo_cvm}) - {titulo}
                Categoria: {categoria}
                Tipo: {tipo}
                Espécie: {especie}
                Tipo Apresentação: {tipo_ap}
                Protocolo: {protocolo}
                Versão: {versao}
                Ano CSV: {ano}
                Link: {link}
                """
            )

            # garante hash mais estável se tiver protocolo
            if protocolo:
                raw_text += f" | ProtocoloEntrega={protocolo}"

            inserted, _ = _upsert_doc_and_chunks(
                ticker=tk,
                codigo_cvm=codigo_cvm,
                data=data_iso,
                fonte="CVM",
                tipo="ipe",
                titulo=titulo[:300],
                url=link[:1000],
                raw_text=raw_text,
                chunk_chars=int(chunk_chars),
                overlap=int(overlap),
                gerar_chunks=bool(gerar_chunks),
            )

            if inserted:
                stats[tk]["inserted"] += 1
            else:
                stats[tk]["skipped"] += 1

            if sleep_s and float(sleep_s) > 0:
                time.sleep(float(sleep_s))

    ok = all((v.get("inserted", 0) > 0 or v.get("matched", 0) == 0) for v in stats.values())
    # ok aqui significa: não travou, e processou — pode ser zero inserções se não houver docs estratégicos

    return {
        "ok": ok and (len(errors) == 0),
        "stats": stats,
        "errors": errors,
        "years": years_info,
        "columns": columns_debug if debug_columns else {},
        "ipe_url_template": _get_ipe_csv_url_template(),
    }
