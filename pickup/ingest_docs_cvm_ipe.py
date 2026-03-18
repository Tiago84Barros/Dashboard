from __future__ import annotations
"""
pickup/ingest_docs_cvm_ipe.py — versão estável

Objetivo:
- restaurar a ingestão sem depender de igualdade frágil entre código CVM do CSV e do banco
- manter o fluxo IPE como fonte primária
- usar ENET como fallback automático por ticker quando o IPE vier zerado
- preservar a assinatura esperada por analises_portfolio.py: ingest_ipe_for_tickers(...)
"""

from typing import Any, Dict, List, Optional, Sequence
import hashlib
import importlib.util
import io
import re
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import text

from core.db_loader import get_supabase_engine

_TEXT_COL_CACHE: Optional[str] = None


def _engine():
    return get_supabase_engine()


def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()


def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _clean_text(s: str) -> str:
    s = (s or "").replace("\x00", " ").strip()
    s = re.sub(r"\s+", " ", s).strip()
    return "" if s.lower() == "nan" else s


def _sanitize_text(s: str) -> str:
    return "" if s is None else str(s).replace("\x00", "")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _now_minus_months(months: int) -> datetime:
    return _utcnow() - timedelta(days=int(months) * 30)


def _parse_date(val: Any) -> Optional[pd.Timestamp]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return pd.to_datetime(val, errors="coerce", dayfirst=True)
    except Exception:
        return None


def _pick_col(cols: Sequence[str], *candidates: str) -> Optional[str]:
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def _norm_cvm_code(val: Any) -> str:
    s = str(val or "").strip()
    if not s:
        return ""
    s = s.replace(".0", "")
    s = "".join(ch for ch in s if ch.isdigit())
    return s.lstrip("0") or "0"


def _get_text_column(conn) -> str:
    global _TEXT_COL_CACHE
    if _TEXT_COL_CACHE:
        return _TEXT_COL_CACHE

    rows = conn.execute(
        text(
            """
            select column_name
            from information_schema.columns
            where table_schema = 'public'
              and table_name = 'docs_corporativos'
              and column_name in ('raw_text','texto')
            """
        )
    ).fetchall()
    cols = {str(r[0]) for r in rows}
    if "raw_text" in cols:
        _TEXT_COL_CACHE = "raw_text"
        return _TEXT_COL_CACHE
    if "texto" in cols:
        _TEXT_COL_CACHE = "texto"
        return _TEXT_COL_CACHE
    raise RuntimeError("docs_corporativos não possui coluna raw_text nem texto.")


def _get_doc_status(conn, doc_hash: str) -> Dict[str, Any]:
    text_col = _get_text_column(conn)
    row = conn.execute(
        text(
            f"""
            select id, coalesce(nullif(trim({text_col}),''), '') as t
            from public.docs_corporativos
            where doc_hash = :h
            limit 1
            """
        ),
        {"h": doc_hash},
    ).fetchone()
    if not row:
        return {"exists": False, "id": None, "has_text": False}
    return {"exists": True, "id": int(row[0]), "has_text": bool(str(row[1] or "").strip())}


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
    params = {
        "ticker": ticker,
        "titulo": (titulo or "")[:4000],
        "url": (url or "")[:4000],
        "fonte": fonte,
        "tipo": (tipo or "")[:200],
        "data": (data.to_pydatetime() if isinstance(data, pd.Timestamp) and not pd.isna(data) else None),
        "doc_hash": doc_hash,
        "text_value": _sanitize_text(texto or ""),
    }
    row = conn.execute(
        text(
            f"""
            insert into public.docs_corporativos
            (ticker, titulo, url, fonte, tipo, data, {text_col}, doc_hash)
            values
            (:ticker, :titulo, :url, :fonte, :tipo, :data, :text_value, :doc_hash)
            on conflict (doc_hash) do nothing
            returning id
            """
        ),
        params,
    ).fetchone()
    return int(row[0]) if row else None


def _update_doc_text(conn, doc_id: int, texto: str) -> bool:
    text_col = _get_text_column(conn)
    if not (texto or "").strip():
        return False
    conn.execute(
        text(f"update public.docs_corporativos set {text_col} = :t where id = :id"),
        {"t": _sanitize_text(texto), "id": int(doc_id)},
    )
    return True


_POSITIVE_TYPES_HIGH = [
    "fato relevante", "comunicado ao mercado", "reorganização societ", "aquisi", "m&a",
    "fusão", "cisão", "incorp", "guidance", "proje", "plano de investimento", "capex",
    "debênt", "emissão", "recompra", "dividend", "jcp", "acordo", "parceria",
    "joint venture", "opa",
]
_POSITIVE_TYPES_MED = ["conselho de administração", "assembleia", "ago", "age", "política de dividend", "remuneração"]
_KEYWORDS = [
    "capex", "invest", "expans", "guidance", "proje", "desalav", "dívida", "divida", "debênt",
    "debent", "aquisi", "fus", "cis", "incorp", "parceria", "contrato", "venda de ativo",
    "desinvest", "recompra", "dividendo", "jcp", "rating", "alocação", "alocacao", "plano",
    "projeto", "estratég", "estrateg",
]
_NOISE = [
    "eleição", "eleicao", "posse", "instalação", "instalacao", "regimento", "calendário", "calendario",
    "atualização cadastral", "atualizacao cadastral", "formulário", "formulario", "esclarecimento",
    "sem efeito", "retificação", "retificacao",
]


def _score_doc(tipo: str, titulo: str, assunto: str, categoria: str) -> int:
    blob = f"{(tipo or '').lower()} {(titulo or '').lower()} {(assunto or '').lower()} {(categoria or '').lower()}"
    score = 0
    for k in _POSITIVE_TYPES_HIGH:
        if k in blob:
            score += 8
    for k in _POSITIVE_TYPES_MED:
        if k in blob:
            score += 4
    for k in _KEYWORDS:
        if k in blob:
            score += 3
    for k in _NOISE:
        if k in blob:
            score -= 6
    if not (titulo or "").strip() or (titulo or "").strip().lower() == "nan":
        score -= 8
    return score


def _extract_pdf_text(pdf_bytes: bytes, max_pages: int = 120) -> str:
    if not pdf_bytes:
        return ""
    try:
        import pdfplumber  # type: ignore
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            texts: List[str] = []
            for i, page in enumerate(pdf.pages):
                if i >= max_pages:
                    break
                t = page.extract_text() or ""
                if t.strip():
                    texts.append(t)
        out = "\n".join(texts).strip()
        if len(out) >= 800:
            return out
    except Exception:
        pass
    try:
        import PyPDF2  # type: ignore
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        texts = []
        for i, page in enumerate(reader.pages):
            if i >= max_pages:
                break
            t = page.extract_text() or ""
            if t.strip():
                texts.append(t)
        out = "\n".join(texts).strip()
        if len(out) >= 800:
            return out
    except Exception:
        pass
    try:
        from pdfminer.high_level import extract_text  # type: ignore
        return (extract_text(io.BytesIO(pdf_bytes), maxpages=max_pages) or "").strip()
    except Exception:
        return ""


def _is_pdf_response(resp: requests.Response) -> bool:
    ctype = (resp.headers.get("content-type") or "").lower()
    return "pdf" in ctype or (resp.content or b"").startswith(b"%PDF")


def _fetch_pdf_bytes(url: str, timeout: int = 25) -> Optional[bytes]:
    if not url:
        return None
    u = url.lower()
    if not (u.endswith(".pdf") or "frmdownloaddocumento" in u or "download" in u):
        return None
    resp = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    return resp.content if _is_pdf_response(resp) else None


def _load_ipe_csv(year: int, timeout: int = 30) -> pd.DataFrame:
    url_zip = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{year}.zip"
    r = requests.get(url_zip, timeout=timeout)
    r.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    csv_names = [n for n in zf.namelist() if n.lower().endswith('.csv')]
    if not csv_names:
        raise RuntimeError("ZIP do IPE não contém CSV")
    raw = zf.read(csv_names[0])
    for enc in ("utf-8", "latin1"):
        try:
            return pd.read_csv(io.BytesIO(raw), sep=';', encoding=enc, dtype=str)
        except Exception:
            continue
    return pd.read_csv(io.BytesIO(raw), sep=';', encoding='latin1', dtype=str)


def get_cvm_codes_for_tickers(tickers: Sequence[str]) -> Dict[str, int]:
    tks = [_norm_ticker(t) for t in tickers if (t or "").strip()]
    if not tks:
        return {}
    sql = 'select "Ticker" as ticker, "CVM" as cvm from public.cvm_to_ticker where "Ticker" = any(:tks)'
    with _engine().connect() as conn:
        df = pd.read_sql_query(text(sql), conn, params={"tks": tks})
    out: Dict[str, int] = {}
    for _, r in df.iterrows():
        out[str(r["ticker"]).upper()] = int(r["cvm"])
    return out


def _load_enet_module():
    try:
        here = Path(__file__).resolve().parent
        path = here / 'ingest_docs_cvm_enet.py'
        if not path.exists():
            return None
        spec = importlib.util.spec_from_file_location('ingest_docs_cvm_enet', str(path))
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        fn = getattr(module, 'ingest_enet_for_tickers', None)
        return fn if callable(fn) else None
    except Exception:
        return None


def _summarize_top_level(stats: Dict[str, Dict[str, Any]], errors: Dict[str, str]) -> Dict[str, Any]:
    total_matched = sum(int(v.get('matched', 0) or 0) for v in stats.values())
    total_considered = sum(int(v.get('considered', 0) or 0) for v in stats.values())
    total_inserted = sum(int(v.get('inserted', 0) or 0) for v in stats.values())
    total_skipped = sum(int(v.get('skipped', 0) or 0) for v in stats.values())
    total_pdf_fetched = sum(int(v.get('pdf_fetched', 0) or 0) for v in stats.values())
    total_pdf_text_ok = sum(int(v.get('pdf_text_ok', 0) or 0) for v in stats.values())
    return {
        'matched': total_matched,
        'considered': total_considered,
        'inserted': total_inserted,
        'skipped': total_skipped,
        'pdf_fetched': total_pdf_fetched,
        'pdf_text_ok': total_pdf_text_ok,
        'error': '; '.join(f'{k}: {v}' for k, v in errors.items()) if errors else '',
    }


def ingest_ipe_for_tickers(
    tickers: Sequence[str],
    *,
    window_months: int = 60,
    max_docs_per_ticker: int = 800,
    strategic_only: bool = True,
    download_pdfs: bool = True,
    max_pdfs_per_ticker: int = 240,
    pdf_max_pages: int = 120,
    request_timeout: int = 60,
    max_runtime_s: float = 900.0,
    sleep_s: float = 0.05,
    verbose: bool = False,
    **kwargs,
) -> Dict[str, Any]:
    started = time.time()
    tickers_n = list(dict.fromkeys(_norm_ticker(t) for t in tickers if (t or '').strip()))
    cvm_map = get_cvm_codes_for_tickers(tickers_n)

    now = _utcnow()
    min_dt = _now_minus_months(int(window_months))
    years = list(range(min_dt.year, now.year + 1))

    dfs: List[pd.DataFrame] = []
    for y in years:
        try:
            dfs.append(_load_ipe_csv(y, timeout=request_timeout))
        except Exception as e:
            if verbose:
                print(f'[IPE] Falha ao carregar {y}: {e}')

    if not dfs:
        return {'ok': False, 'errors': {'__all__': 'Nenhum CSV IPE disponível.'}, 'stats': {}, **_summarize_top_level({}, {'__all__': 'Nenhum CSV IPE disponível.'})}

    df = pd.concat(dfs, ignore_index=True)
    cols = list(df.columns)

    col_cvm = _pick_col(cols, 'CODIGO_CVM', 'CD_CVM', 'CVM', 'COD_CVM')
    col_data = _pick_col(cols, 'DATA_ENTREGA', 'DT_RECEB', 'DT_REFER', 'DATA_REFERENCIA', 'DATA_REFER', 'DT_ENTREGA')
    col_link = _pick_col(cols, 'LINK_DOWNLOAD', 'LINK', 'LINK_ARQUIVO', 'LINK_DOC', 'LINK_DOCUMENTO')
    col_assunto = _pick_col(cols, 'ASSUNTO', 'ASSUNTO_EVENTO', 'TITULO', 'DESCRICAO')
    col_categoria = _pick_col(cols, 'CATEGORIA', 'CATEGORIA_DOCUMENTO')
    col_tipo = _pick_col(cols, 'TIPO', 'TIPO_DOCUMENTO')

    if any(c is None for c in (col_cvm, col_data, col_link, col_assunto)):
        err = {'__all__': f'CSV IPE sem colunas necessárias. Encontradas={cols}'}
        return {'ok': False, 'errors': err, 'stats': {}, **_summarize_top_level({}, err)}

    df['_dt'] = df[col_data].apply(_parse_date)
    df = df[~df['_dt'].isna()].copy()
    min_ts = pd.Timestamp(min_dt).tz_localize(None)
    df['_dt_naive'] = df['_dt'].apply(lambda x: x.tz_localize(None) if hasattr(x, 'tz_localize') and getattr(x, 'tzinfo', None) is not None else x)
    df = df[df['_dt_naive'] >= min_ts].copy()
    df['_cvm_norm'] = df[col_cvm].map(_norm_cvm_code)

    if strategic_only:
        df['_tipo'] = df[col_tipo].fillna('').map(_clean_text) if col_tipo else ''
        df['_titulo'] = df[col_assunto].fillna('').map(_clean_text)
        df['_assunto'] = df[col_assunto].fillna('').map(_clean_text)
        df['_categoria'] = df[col_categoria].fillna('').map(_clean_text) if col_categoria else ''
        df['_score'] = df.apply(lambda r: _score_doc(str(r.get('_tipo','') or ''), str(r.get('_titulo','') or ''), str(r.get('_assunto','') or ''), str(r.get('_categoria','') or '')), axis=1)
    else:
        df['_score'] = 0

    out_stats: Dict[str, Any] = {}
    out_errors: Dict[str, str] = {}
    min_coverage = 25
    min_score_strategic = 2

    with _engine().begin() as conn:
        _ = _get_text_column(conn)

        for tk in tickers_n:
            if (time.time() - started) > max_runtime_s:
                out_errors['__runtime__'] = f'Tempo máximo atingido ({max_runtime_s}s).'
                break

            cvm = cvm_map.get(tk)
            if not cvm:
                out_errors[tk] = 'ticker_sem_mapeamento_cvm (preencha public.cvm_to_ticker)'
                out_stats[tk] = {'matched': 0, 'considered': 0, 'inserted': 0, 'skipped': 0, 'updated_text': 0, 'pdf_fetched': 0, 'pdf_text_ok': 0, 'fallback_source': ''}
                continue

            cvm_norm = _norm_cvm_code(cvm)
            dft_all = df[df['_cvm_norm'] == cvm_norm].copy()
            dft_all = dft_all.sort_values('_dt_naive', ascending=False)
            matched = int(len(dft_all))

            if verbose:
                print(f'[IPE] {tk} cvm={cvm} norm={cvm_norm} matched={matched}')

            # fallback ENET quando IPE zera
            if matched == 0:
                enet_fn = _load_enet_module()
                if enet_fn is not None:
                    try:
                        anos = max(1, int(round(window_months / 12.0)))
                        enet_r = enet_fn(
                            tickers=[tk],
                            anos=anos,
                            max_docs_por_ticker=min(int(max_docs_per_ticker), 60),
                            baixar_e_extrair=download_pdfs,
                        )
                        tk_stats = (enet_r.get('stats') or {}).get(tk, {}) if isinstance(enet_r, dict) else {}
                        out_stats[tk] = {
                            'matched': int(tk_stats.get('seen', 0) or 0),
                            'considered': int(tk_stats.get('seen', 0) or 0),
                            'inserted': int(tk_stats.get('inserted', 0) or 0),
                            'skipped': int(tk_stats.get('skipped', 0) or 0),
                            'updated_text': 0,
                            'pdf_fetched': int(tk_stats.get('downloaded', 0) or 0),
                            'pdf_text_ok': int(tk_stats.get('text_ok', 0) or 0),
                            'fallback_source': 'ENET',
                        }
                        tk_err = (enet_r.get('errors') or {}).get(tk) if isinstance(enet_r, dict) else None
                        if tk_err:
                            out_errors[tk] = str(tk_err)
                        if verbose:
                            print(f'[ENET] {tk} stats={out_stats[tk]} err={tk_err}')
                        continue
                    except Exception as e:
                        out_errors[tk] = f'fallback_enet_failed: {e}'
                out_stats[tk] = {'matched': 0, 'considered': 0, 'inserted': 0, 'skipped': 0, 'updated_text': 0, 'pdf_fetched': 0, 'pdf_text_ok': 0, 'fallback_source': ''}
                continue

            fallback_used = False
            selected_strategic = 0
            if strategic_only:
                dft_ranked = dft_all.sort_values(['_score', '_dt_naive'], ascending=[False, False]).copy()
                strategic = dft_ranked[dft_ranked['_score'] >= min_score_strategic].head(int(max_docs_per_ticker)).copy()
                selected = strategic.copy()
                selected_strategic = int(len(selected))
                if selected_strategic < min_coverage:
                    fallback_used = True
                    remaining = dft_ranked.loc[~dft_ranked.index.isin(selected.index)].sort_values('_dt_naive', ascending=False)
                    need = int(max_docs_per_ticker) - selected_strategic
                    if need > 0:
                        selected = pd.concat([selected, remaining.head(need)], ignore_index=False)
                selected = selected.head(int(max_docs_per_ticker)).copy()
            else:
                selected = dft_all.head(int(max_docs_per_ticker)).copy()

            considered = int(len(selected))
            inserted = skipped = updated_text = pdf_fetched = pdf_text_ok = pdf_used = 0

            for _, r in selected.iterrows():
                if (time.time() - started) > max_runtime_s:
                    out_errors['__runtime__'] = f'Tempo máximo atingido ({max_runtime_s}s).'
                    break

                url = str(r.get(col_link, '') or '').strip()
                if not url:
                    skipped += 1
                    continue
                titulo = _clean_text(str(r.get(col_assunto, '') or '')) or ''
                tipo = _clean_text(str(r.get(col_tipo, '') or '')) if col_tipo else ''
                if not tipo:
                    tipo = 'IPE'
                dt = r.get('_dt')
                doc_hash = _sha256(f'{tk}|{url}|{titulo}|{dt}')
                status = _get_doc_status(conn, doc_hash)
                texto = ''
                if download_pdfs and pdf_used < int(max_pdfs_per_ticker):
                    try:
                        pdf_bytes = _fetch_pdf_bytes(url, timeout=request_timeout)
                        if pdf_bytes:
                            pdf_fetched += 1
                            pdf_used += 1
                            tpdf = (_extract_pdf_text(pdf_bytes, max_pages=int(pdf_max_pages)) or '').strip()
                            if tpdf and len(tpdf) >= 200:
                                texto = tpdf
                                pdf_text_ok += 1
                    except Exception:
                        pass
                if status['exists']:
                    if (not status['has_text']) and texto:
                        if _update_doc_text(conn, int(status['id']), texto):
                            updated_text += 1
                    else:
                        skipped += 1
                    continue
                doc_id = _insert_doc(conn, ticker=tk, titulo=titulo or 'Documento CVM/IPE', url=url, fonte='CVM/IPE', tipo=tipo, data=dt, texto=texto, doc_hash=doc_hash)
                if doc_id is None:
                    skipped += 1
                else:
                    inserted += 1
                if sleep_s:
                    time.sleep(float(sleep_s))

            out_stats[tk] = {
                'matched': matched,
                'considered': considered,
                'inserted': inserted,
                'skipped': skipped,
                'updated_text': updated_text,
                'pdf_fetched': pdf_fetched,
                'pdf_text_ok': pdf_text_ok,
                'selected_strategic': selected_strategic if strategic_only else None,
                'fallback_used': fallback_used if strategic_only else None,
                'fallback_source': '',
            }

    ok = len(out_errors) == 0
    top = _summarize_top_level(out_stats, out_errors)
    return {'ok': ok, 'stats': out_stats, 'errors': out_errors, **top}
