from __future__ import annotations

"""
pickup/ingest_docs_cvm_ipe.py
----------------------------
OPÇÃO A (CVM/IPE) — ingestão *estratégica* para o Patch 6.

Objetivo:
- Capturar APENAS documentos com potencial de "intenção estratégica futura"
  (releases de resultados, fatos relevantes, comunicados estratégicos, etc.)
- Janela temporal: últimos 12 meses (configurável via months_back)
- Inserir em public.docs_corporativos (metadados + raw_text quando possível)
- Sem depender de scraping HTML de diretórios: usa URL determinística por ano:
    https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{YYYY}.zip

Observações de performance:
- Filtra no CSV ANTES de baixar PDFs (reduz drasticamente volume).
- Por padrão tenta extrair texto do PDF (sem OCR). Se falhar, insere só metadados.
- Suporta max_runtime_s para impedir execução "infinita" em Streamlit.

Tabelas esperadas (Supabase/Postgres):
- public.docs_corporativos (colunas: ticker, data, fonte, tipo, titulo, url, raw_text, doc_hash; UNIQUE(doc_hash))
- public.cvm_to_ticker (colunas: "CVM" int, "Ticker" text)

Dependências:
- requests, pandas, sqlalchemy, streamlit
- (opcional) pdfminer.six para extrair texto de PDF (sem OCR)

Compatibilidade:
- Mantém assinatura chamada por pickup.ingest_docs_fallback.ingest_strategy_for_tickers:
    ingest_ipe_for_tickers(tickers, anos=2, max_docs_por_ticker=25, sleep_s=0.2)

"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
from datetime import datetime
import hashlib
import io
import re
import time
import zipfile

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

def _now_utc_naive() -> pd.Timestamp:
    # Evita tz-aware vs tz-naive no pandas
    return pd.Timestamp.utcnow().tz_localize(None)

def _year_zip_url(year: int) -> str:
    return f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{year}.zip"


# ───────────────────────── Filtros Estratégicos ─────────────────────────

# Categorias / espécies "tipicamente estratégicas" (variações comuns)
_CATEGORY_PATTERNS = [
    r"fato\s+relevante",
    r"release",  # release de resultados, press release
    r"apresenta",  # apresentação de resultados
    r"resultado",  # resultados
    r"comunicado\s+ao\s+mercado",
    r"aviso\s+ao\s+mercado",
    r"guidance",
    r"outlook",
    r"investidor",  # apresentações para investidores às vezes
]

# Palavras-chave em assunto/título que sinalizam intenção futura (pt/en)
_KEYWORD_PATTERNS = [
    r"guidance|outlook|proje[cç][aã]o|previs[aã]o",
    r"capex|investimento|investir|expans[aã]o|plano\s+de\s+neg[oó]cios|plano\s+estrat[eé]gico",
    r"aquisi[cç][aã]o|fus[aã]o|m&a|incorpora[cç][aã]o|joint\s+venture|cis[aã]o|spin[-\s]?off",
    r"desalavanc|redu[cç][aã]o\s+de\s+d[ií]vida|refinanci|alongamento\s+de\s+d[ií]vida",
    r"reorganiza[cç][aã]o|reestrutura[cç][aã]o|turnaround|efici[eê]ncia|otimiza[cç][aã]o",
    r"desinvest|venda\s+de\s+ativos|alien[aã]o\s+de\s+ativos",
    r"nova\s+f[aá]brica|nova\s+planta|greenfield|brownfield|expans[aã]o\s+de\s+capacidade",
    r"internacionaliza[cç][aã]o|novo\s+mercado|novos\s+mercados|entrada\s+em\s+segmento",
]

_RE_CATEGORY = re.compile("|".join(_CATEGORY_PATTERNS), re.I)
_RE_KEYWORDS = re.compile("|".join(_KEYWORD_PATTERNS), re.I)


def _is_strategic_row(row: pd.Series, *, category_col: str, subject_col: str, specie_col: Optional[str]) -> bool:
    cat = str(row.get(category_col, "") or "")
    subj = str(row.get(subject_col, "") or "")
    spc = str(row.get(specie_col, "") or "") if specie_col else ""
    hay = " | ".join([cat, spc, subj])
    # "Use ambos": aceita se categoria/especie OU keywords sinalizarem estratégia
    return bool(_RE_CATEGORY.search(hay) or _RE_KEYWORDS.search(hay))


# ───────────────────────── Leitura do IPE ─────────────────────────

@st.cache_data(ttl=60 * 60, show_spinner=False)
def _load_ipe_year_df(year: int) -> pd.DataFrame:
    """
    Baixa e carrega o IPE do ano (ZIP->CSV) em DataFrame.
    Cache por ano para não rebaixar a cada clique.
    """
    url = _year_zip_url(year)
    r = requests.get(url, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"IPE {year} indisponível (HTTP {r.status_code})")
    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv_names = [n for n in z.namelist() if n.lower().endswith(".csv")]
    if not csv_names:
        raise RuntimeError(f"ZIP IPE {year} sem CSV")
    # Escolhe o maior CSV (mais seguro)
    csv_name = sorted(csv_names, key=lambda n: z.getinfo(n).file_size, reverse=True)[0]
    with z.open(csv_name) as f:
        data = f.read()

    # CSV CVM tipicamente usa ';' e latin-1
    # tenta combos com fallback
    last_err: Optional[Exception] = None
    for sep, enc in [(";", "latin1"), (";", "utf-8"), (",", "utf-8"), (",", "latin1")]:
        try:
            df = pd.read_csv(io.BytesIO(data), sep=sep, encoding=enc, low_memory=False)
            return df
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Falha ao ler CSV IPE {year}: {last_err}")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def _pick_col(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    for c in candidates:
        cU = c.strip().upper()
        if cU in df.columns:
            return cU
    return None


def _parse_date_series(s: pd.Series) -> pd.Series:
    # pandas: mantém tz-naive
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True, utc=False)
    # Se vier tz-aware, remove tz
    try:
        if hasattr(dt.dt, "tz_localize"):
            dt = dt.dt.tz_localize(None)  # type: ignore
    except Exception:
        pass
    return dt


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """
    Extrai texto do PDF sem OCR.
    Se pdfminer não estiver disponível, retorna "".
    """
    try:
        from pdfminer.high_level import extract_text  # type: ignore
        return _clean_text(extract_text(io.BytesIO(pdf_bytes)) or "")
    except Exception:
        return ""


# ───────────────────────── Banco / Upsert ─────────────────────────

def _fetch_cvm_map(tickers: Sequence[str]) -> Dict[str, int]:
    """
    Busca CVM code por ticker em public.cvm_to_ticker.
    Espera colunas: "CVM" (int) e "Ticker" (text).
    """
    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {}
    engine = get_supabase_engine()
    sql = text(
        """
        select upper("Ticker") as ticker, "CVM" as cvm
        from public.cvm_to_ticker
        where upper("Ticker") = any(:tks)
        """
    )
    out: Dict[str, int] = {}
    with engine.begin() as conn:
        rows = conn.execute(sql, {"tks": tks}).fetchall()
    for r in rows:
        out[str(r[0]).upper()] = int(r[1])
    return out


def _insert_doc(
    *,
    ticker: str,
    data: Optional[pd.Timestamp],
    fonte: str,
    tipo: str,
    titulo: str,
    url: str,
    raw_text: str,
) -> Tuple[bool, Optional[int]]:
    """
    Insere em docs_corporativos (idempotente por doc_hash).
    Retorna (inserted?, id|None).
    """
    engine = get_supabase_engine()

    doc_hash = _sha256("|".join([_norm_ticker(ticker), (url or "").strip(), (titulo or "").strip()]))

    sql = text(
        """
        insert into public.docs_corporativos (ticker, data, fonte, tipo, titulo, url, raw_text, doc_hash)
        values (:ticker, :data, :fonte, :tipo, :titulo, :url, :raw_text, :doc_hash)
        on conflict (doc_hash) do nothing
        returning id
        """
    )
    payload = {
        "ticker": _norm_ticker(ticker),
        "data": (data.to_pydatetime() if isinstance(data, pd.Timestamp) and pd.notna(data) else None),
        "fonte": fonte,
        "tipo": tipo,
        "titulo": titulo[:500] if titulo else "",
        "url": url,
        "raw_text": raw_text or "",
        "doc_hash": doc_hash,
    }
    with engine.begin() as conn:
        row = conn.execute(sql, payload).fetchone()
    if row is None:
        return (False, None)
    return (True, int(row[0]))


# ───────────────────────── API pública ─────────────────────────

def ingest_ipe_for_tickers(
    tickers: Sequence[str],
    *,
    # compat com caller
    anos: int = 2,
    max_docs_por_ticker: int = 25,
    sleep_s: float = 0.0,
    years: Optional[int] = None,
    # novos parâmetros (podem ser ignorados pelo caller)
    months_back: int = 12,
    max_runtime_s: Optional[int] = None,
    fetch_pdf_text: bool = True,
    request_timeout: int = 35,
) -> Dict[str, Any]:
    """
    Ingestão estratégica do IPE para uma lista de tickers.

    Retorno:
      {
        "ok": bool,
        "stats": { TICKER: {"matched":int,"inserted":int,"skipped":int,"pdf_fetched":int,"pdf_text_ok":int} },
        "errors": { TICKER: "..." }
      }
    """
    start_t = time.time()

    # Compat: alguns callers antigos passam `years=`. Aqui tratamos como anos.
    if years is not None:
        try:
            y = int(years)
            if y > 0:
                months_back = y * 12
        except Exception:
            pass


    tks = [_norm_ticker(t) for t in (tickers or []) if str(t).strip()]
    tks = list(dict.fromkeys(tks))
    if not tks:
        return {"ok": False, "stats": {}, "errors": {"__A__": "Lista vazia"}}

    out_stats: Dict[str, Dict[str, int]] = {tk: {"matched": 0, "inserted": 0, "skipped": 0, "pdf_fetched": 0, "pdf_text_ok": 0} for tk in tks}
    out_errs: Dict[str, str] = {}

    # Janela temporal (últimos N meses)
    now = _now_utc_naive()
    cutoff = (now - pd.DateOffset(months=int(months_back))).normalize()

    # anos necessários para cobrir cutoff..now
    years_needed = list(range(int(cutoff.year), int(now.year) + 1))
    # compat: se user passar anos maior, amplia para trás
    if int(anos) > len(years_needed):
        extra_years = int(anos) - len(years_needed)
        years_needed = list(range(int(cutoff.year) - extra_years + 1, int(now.year) + 1))

    # mapa ticker -> CVM
    tk2cvm = _fetch_cvm_map(tks)
    missing = [tk for tk in tks if tk not in tk2cvm]
    for tk in missing:
        out_errs[tk] = "ticker_sem_mapeamento_cvm (preencha public.cvm_to_ticker)"

    # Carrega DF(s) anuais
    dfs: List[pd.DataFrame] = []
    year_errors: List[str] = []
    for y in years_needed:
        if max_runtime_s and (time.time() - start_t) > max_runtime_s:
            break
        try:
            dfy = _load_ipe_year_df(int(y))
            dfs.append(_normalize_columns(dfy))
        except Exception as e:
            year_errors.append(f"{y}: {type(e).__name__}: {e}")

    if not dfs:
        # Propaga erro global por ticker (para UI enxergar)
        msg = "Nenhum CSV IPE disponível (todas as URLs candidatas falharam)."
        if year_errors:
            msg = "Falha ao carregar IPE: " + " | ".join(year_errors[:5])
        for tk in tks:
            out_errs.setdefault(tk, msg)
        return {"ok": False, "stats": out_stats, "errors": out_errs}

    df = pd.concat(dfs, ignore_index=True)

    # Identifica colunas
    col_cvm = _pick_col(df, ["CD_CVM", "COD_CVM", "CVM", "CD_CVM_CIA"])
    col_assunto = _pick_col(df, ["ASSUNTO", "TITULO", "DESCRICAO"])
    col_categoria = _pick_col(df, ["CATEG_DOC", "CATEGORIA", "CATEG"])
    col_especie = _pick_col(df, ["ESPECIE", "TIPO_DOC", "TIPO"])
    col_data = _pick_col(df, ["DT_REFER", "DT_RECEB", "DT_RECEBIMENTO", "DT_ENTREGA", "DATA"])

    col_link = _pick_col(df, ["LINK_DOC", "LINK_DOWNLOAD", "LINK", "URL"])

    required = [col_cvm, col_assunto, col_categoria, col_data, col_link]
    if any(c is None for c in required):
        msg = f"CSV IPE sem colunas necessárias. Encontradas={list(df.columns)[:40]}"
        for tk in tks:
            out_errs.setdefault(tk, msg)
        return {"ok": False, "stats": out_stats, "errors": out_errs}

    assert col_cvm and col_assunto and col_categoria and col_data and col_link

    # Parse data e filtra janela de 12 meses
    df[col_data] = _parse_date_series(df[col_data])
    df = df[pd.notna(df[col_data])]
    df = df[df[col_data] >= cutoff]

    # Converte CD_CVM para int quando possível
    def to_int(x: Any) -> Optional[int]:
        try:
            if pd.isna(x):
                return None
            return int(str(x).strip())
        except Exception:
            return None

    df["_CVM_INT"] = df[col_cvm].apply(to_int)
    df = df[pd.notna(df["_CVM_INT"])]

    # Pré-filtro estratégico (categoria + keywords)
    df["_IS_STRAT"] = df.apply(lambda r: _is_strategic_row(r, category_col=col_categoria, subject_col=col_assunto, specie_col=col_especie), axis=1)
    df = df[df["_IS_STRAT"] == True]  # noqa: E712

    # Para cada ticker: filtra pelo CVM e ingere
    for tk in tks:
        if max_runtime_s and (time.time() - start_t) > max_runtime_s:
            out_errs.setdefault(tk, f"timeout (max_runtime_s={max_runtime_s})")
            break

        if tk not in tk2cvm:
            continue

        cvm = tk2cvm[tk]
        dft = df[df["_CVM_INT"] == cvm].copy()
        if dft.empty:
            # sem docs estratégicos no período
            out_stats[tk]["matched"] = 0
            continue

        # Ordena por data desc
        dft = dft.sort_values(col_data, ascending=False)

        matched = int(len(dft))
        out_stats[tk]["matched"] = matched

        # Mesmo que você não queira "limitar tanto", ainda precisamos de um freio
        # para evitar ingest enorme em empresas extremamente comunicativas.
        # max_docs_por_ticker vem do caller; se vier alto, tudo bem.
        if isinstance(max_docs_por_ticker, int) and max_docs_por_ticker > 0:
            dft = dft.head(int(max_docs_por_ticker))

        for _, row in dft.iterrows():
            if max_runtime_s and (time.time() - start_t) > max_runtime_s:
                out_errs.setdefault(tk, f"timeout (max_runtime_s={max_runtime_s})")
                break

            url = str(row.get(col_link, "") or "").strip()
            titulo = _clean_text(str(row.get(col_assunto, "") or ""))[:500]
            data_ref = row.get(col_data)
            tipo = "IPE"
            fonte = "CVM_IPE"

            raw_text = ""

            # (Opcional) baixa PDF e extrai texto
            if fetch_pdf_text and url and url.lower().endswith(".pdf"):
                try:
                    r = requests.get(url, timeout=request_timeout)
                    if r.status_code == 200 and r.content:
                        out_stats[tk]["pdf_fetched"] += 1
                        raw_text = _extract_pdf_text(r.content)
                        if raw_text:
                            out_stats[tk]["pdf_text_ok"] += 1
                except Exception:
                    # mantém metadados mesmo sem texto
                    raw_text = ""

            inserted, _id = _insert_doc(
                ticker=tk,
                data=data_ref if isinstance(data_ref, pd.Timestamp) else None,
                fonte=fonte,
                tipo=tipo,
                titulo=titulo,
                url=url,
                raw_text=raw_text,
            )
            if inserted:
                out_stats[tk]["inserted"] += 1
            else:
                out_stats[tk]["skipped"] += 1

            if sleep_s:
                time.sleep(float(sleep_s))

    ok = all((out_stats.get(tk, {}).get("inserted", 0) > 0) for tk in tks if tk in tk2cvm) if tk2cvm else False
    return {"ok": bool(ok), "stats": out_stats, "errors": out_errs}
