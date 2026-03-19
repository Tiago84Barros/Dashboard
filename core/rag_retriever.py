from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import math
import re

from sqlalchemy import text

from core.db_loader import get_supabase_engine
from core.rag_multitopic import retrieve_multitopic_chunks
import core.ai_models.llm_client.factory as llm_factory


@dataclass
class RagHit:
    doc_id: Optional[int]
    ticker: str
    chunk_text: str
    dist: Optional[float] = None
    data_doc: Optional[str] = None
    tipo_doc: str = ""
    strategic_theme: str = ""


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _extract_year(data_doc: Any) -> str:
    s = str(data_doc or "")[:4]
    return s if s.isdigit() else ""


def _materiality_bonus(txt: str) -> float:
    score = 0.0
    if re.search(r"R\$\s?[\d\.,]+", txt):
        score += 1.1
    if re.search(r"\d+[\.,]?\d*%", txt):
        score += 0.8
    if re.search(r"\d+[\.,]?\d*\s*(milh|milhão|milhoes|milhões|bilh|bilhão|bilhoes|bilhões)", txt):
        score += 1.1
    return score


def _score_text_quality(texto: str, theme: str = "", tipo_doc: str = "", dist: Optional[float] = None, data_doc: str = "") -> float:
    txt = (texto or "").strip().lower()
    if not txt:
        return -999.0
    score = 0.0
    if dist is not None:
        score -= _safe_float(dist, 9.0) * 1.1

    # materialidade / números
    if any(ch.isdigit() for ch in txt):
        score += 1.4
    score += _materiality_bonus(txt)

    key_terms_strong = [
        "capex", "dividend", "jcp", "guidance", "endivid", "dívida", "divida", "alavanc",
        "margem", "receita", "lucro", "ebitda", "recompra", "aquisi", "fusão", "fusao",
        "governan", "conting", "risco", "covenant", "invest", "payout", "caixa", "debênt", "debent",
        "emissão", "emissao", "rating", "desinvest", "m&a", "capital social"
    ]
    if any(k in txt for k in key_terms_strong):
        score += 2.2

    event_terms = ["aprov", "delibera", "anunci", "conclus", "revis", "renegoci", "reestrutur", "incorpora", "cisão", "aliena"]
    if any(k in txt for k in event_terms):
        score += 0.9

    generic_terms = [
        "a companhia segue", "busca continuamente", "estratégia de longo prazo",
        "melhores práticas", "criação de valor", "valor aos acionistas", "fortalecer sua posição",
        "foco na geração de valor"
    ]
    if any(g in txt for g in generic_terms):
        score -= 1.7

    boilerplate_terms = ["ordem do dia", "convocação", "edital", "ata da assembleia", "presença dos acionistas"]
    if any(g in txt for g in boilerplate_terms):
        score -= 0.8

    if len(txt) < 80:
        score -= 0.9
    elif len(txt) > 160:
        score += 0.4
    elif len(txt) > 260:
        score += 0.2

    td = (tipo_doc or "").lower()
    if any(k in td for k in ["fato relevante", "comunicado", "itr", "dfp", "formulário de referência", "formulario de referencia", "release"]):
        score += 1.2
    elif any(k in td for k in ["ata", "aviso", "edital"]):
        score -= 0.2

    th = (theme or "").lower().strip() or "geral"
    if th != 'geral':
        score += 0.35

    yr = _extract_year(data_doc)
    if yr:
        score += 0.1

    return score


def _load_doc_metadata(doc_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not doc_ids:
        return {}
    q = text("""
        select id as doc_id,
               coalesce(data::text,'') as data_doc,
               coalesce(tipo,'') as tipo_doc,
               coalesce(titulo,'') as titulo
        from public.docs_corporativos
        where id = any(:ids)
    """)
    with get_supabase_engine().connect() as conn:
        rows = conn.execute(q, {"ids": doc_ids}).mappings().all()
    return {int(r['doc_id']): dict(r) for r in rows}


def _distribute_hits(rows: List[Dict[str, Any]], top_k: int) -> List[Dict[str, Any]]:
    if not rows:
        return []

    # round-robin por (ano, tema) para evitar concentração excessiva em poucos documentos/anos
    by_bucket: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        year = _extract_year(r.get('data_doc')) or 'sem_ano'
        topic = (r.get('topic') or r.get('strategic_theme') or 'geral').strip() or 'geral'
        by_bucket.setdefault((year, topic), []).append(r)

    for bucket_rows in by_bucket.values():
        bucket_rows.sort(key=lambda x: x.get('_score', -999.0), reverse=True)

    ordered_buckets = sorted(
        by_bucket.keys(),
        key=lambda b: max([x.get('_score', -999.0) for x in by_bucket[b]] or [-999.0]),
        reverse=True,
    )

    selected: List[Dict[str, Any]] = []
    seen = set()
    doc_counts: Dict[Any, int] = {}
    year_counts: Dict[str, int] = {}
    max_per_doc = 3
    max_per_year_soft = max(6, int(math.ceil(top_k * 0.55)))

    while len(selected) < top_k:
        progressed = False
        for bucket in ordered_buckets:
            bucket_rows = by_bucket.get(bucket) or []
            if not bucket_rows:
                continue
            cand = bucket_rows.pop(0)
            key = (cand.get('doc_id'), cand.get('chunk_text'))
            if key in seen:
                continue
            doc_id = cand.get('doc_id')
            year = _extract_year(cand.get('data_doc')) or 'sem_ano'
            if doc_counts.get(doc_id, 0) >= max_per_doc:
                continue
            if year_counts.get(year, 0) >= max_per_year_soft and len(year_counts) > 1:
                continue
            selected.append(cand)
            seen.add(key)
            doc_counts[doc_id] = doc_counts.get(doc_id, 0) + 1
            year_counts[year] = year_counts.get(year, 0) + 1
            progressed = True
            if len(selected) >= top_k:
                break
        if not progressed:
            break

    # backfill caso o soft cap por ano tenha reduzido demais o conjunto
    if len(selected) < top_k:
        all_rows = sorted(rows, key=lambda x: x.get('_score', -999.0), reverse=True)
        for cand in all_rows:
            key = (cand.get('doc_id'), cand.get('chunk_text'))
            if key in seen:
                continue
            doc_id = cand.get('doc_id')
            if doc_counts.get(doc_id, 0) >= max_per_doc:
                continue
            selected.append(cand)
            seen.add(key)
            doc_counts[doc_id] = doc_counts.get(doc_id, 0) + 1
            if len(selected) >= top_k:
                break

    return selected[:top_k]


def get_topk_chunks_inteligente(ticker: str, top_k: int = 24, months_window: int = 36, debug: bool = False) -> List[RagHit]:
    client = llm_factory.get_llm_client()
    engine = get_supabase_engine()
    with engine.connect() as conn:
        hits, _stats = retrieve_multitopic_chunks(
            conn=conn,
            llm_client=client,
            ticker=str(ticker).strip().upper(),
            period_ref=None,
            months_back=int(months_window),
            top_k_total=max(int(top_k) * 6, 96),
            per_topic_k=max(14, min(24, int(math.ceil(top_k * 0.75)))),
            topics=None,
        )
    doc_ids = [int(h.get('doc_id')) for h in hits if h.get('doc_id') is not None]
    meta = _load_doc_metadata(doc_ids)
    enriched: List[Dict[str, Any]] = []
    for h in hits:
        doc_id = h.get('doc_id')
        m = meta.get(int(doc_id)) if doc_id is not None and int(doc_id) in meta else {}
        theme = str(h.get('topic') or 'geral')
        row = {
            'doc_id': doc_id,
            'ticker': h.get('ticker') or str(ticker).strip().upper(),
            'chunk_text': str(h.get('chunk_text') or '').strip(),
            'dist': h.get('dist'),
            'data_doc': m.get('data_doc', ''),
            'tipo_doc': m.get('tipo_doc', ''),
            'strategic_theme': theme,
            'topic': theme,
        }
        row['_score'] = _score_text_quality(
            row['chunk_text'],
            theme=theme,
            tipo_doc=row['tipo_doc'],
            dist=row['dist'],
            data_doc=row['data_doc'],
        )
        if row['chunk_text']:
            enriched.append(row)

    selected = _distribute_hits(enriched, int(top_k))
    selected.sort(key=lambda x: (x.get('_score', -999.0), str(x.get('data_doc') or '')), reverse=True)
    return [
        RagHit(
            doc_id=s.get('doc_id'),
            ticker=str(s.get('ticker') or ''),
            chunk_text=str(s.get('chunk_text') or ''),
            dist=s.get('dist'),
            data_doc=s.get('data_doc'),
            tipo_doc=str(s.get('tipo_doc') or ''),
            strategic_theme=str(s.get('strategic_theme') or ''),
        )
        for s in selected
    ]


def summarize_retrieval_mix(hits: List[Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        'themes': {},
        'tipos_doc': {},
        'years': {},
        'docs': 0,
        'chunks': 0,
    }
    if not hits:
        return out
    docs = set()
    for h in hits:
        docs.add(getattr(h, 'doc_id', None))
        out['chunks'] += 1
        theme = str(getattr(h, 'strategic_theme', '') or 'geral')
        tipo = str(getattr(h, 'tipo_doc', '') or 'não informado')
        year = str(getattr(h, 'data_doc', '') or '')[:4]
        out['themes'][theme] = out['themes'].get(theme, 0) + 1
        out['tipos_doc'][tipo] = out['tipos_doc'].get(tipo, 0) + 1
        if year.isdigit():
            out['years'][year] = out['years'].get(year, 0) + 1
    out['docs'] = len([d for d in docs if d is not None])
    return out
