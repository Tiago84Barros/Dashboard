from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import math
import re
from collections import defaultdict

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
    score: float = 0.0


def _extract_year(data_doc: Any) -> str:
    s = str(data_doc or "")[:4]
    return s if s.isdigit() else ""


def _bucket_for_months(data_doc: Any) -> str:
    s = str(data_doc or "")[:10]
    if len(s) < 7:
        return "sem_data"
    try:
        year = int(s[:4])
        month = int(s[5:7])
        # aproximação robusta sem depender de timezone do container
        # bucket relativo pelos últimos 36 meses; anos mais recentes tendem a ficar em buckets menores
        from datetime import datetime
        now = datetime.utcnow()
        months_delta = (now.year - year) * 12 + (now.month - month)
        if months_delta <= 12:
            return "0_12m"
        if months_delta <= 24:
            return "12_24m"
        if months_delta <= 36:
            return "24_36m"
        return "fora_janela"
    except Exception:
        return "sem_data"


def _tipo_bucket(tipo_doc: str) -> str:
    blob = (tipo_doc or "").lower()
    if "fato relevante" in blob or "comunicado" in blob:
        return "evento"
    if any(k in blob for k in ["itr", "dfp", "fre", "formulário", "formulario"]):
        return "financeiro"
    if any(k in blob for k in ["assembleia", "governança", "governanca", "estatuto", "conselho"]):
        return "governanca"
    return "outros"


def _materiality_bonus(txt: str) -> float:
    score = 0.0
    if re.search(r"R\$\s?[\d\.,]+", txt):
        score += 1.4
    if re.search(r"\b\d+[\.,]?\d*%", txt):
        score += 1.0
    if re.search(r"\b\d+[\.,]?\d*\s*(milh|milhão|milhoes|milhões|bilh|bilhão|bilhoes|bilhões)\b", txt, flags=re.I):
        score += 1.2
    if re.search(r"\b(capex|guidance|payout|dividendos?|jcp|alavanc|dívida|divida|margem|ebitda|covenant|reestrutura|aquisi|cisão|fusão|incorporação|desinvest)\b", txt, flags=re.I):
        score += 1.4
    return score


def _strategic_bonus(txt: str, theme: str = "", tipo_doc: str = "") -> float:
    blob = f"{txt} {theme} {tipo_doc}".lower()
    kws = [
        "guidance", "capex", "dividend", "jcp", "payout", "dívida", "divida", "alavanc", "desalavanc",
        "reestrutura", "aquisi", "fusão", "cisão", "incorp", "joint venture", "governan", "conting",
        "covenant", "emissão", "debênt", "debent", "margem", "eficiência", "eficiencia", "produção", "producao",
        "retorno ao acionista", "alocação de capital", "alocacao de capital", "desinvest", "meta", "execução", "execucao"
    ]
    return 0.35 * sum(1 for k in kws if k in blob)


def _generic_penalty(txt: str) -> float:
    blob = txt.lower()
    bad = [
        "a companhia segue", "busca continuamente", "estratégia de longo prazo", "estrategia de longo prazo",
        "permanece comprometida", "segue focada", "reforça seu compromisso", "fortalecer sua atuação",
        "criação de valor no longo prazo", "geração de valor no longo prazo", "geracao de valor no longo prazo"
    ]
    return 1.6 if any(k in blob for k in bad) else 0.0


def _score_text_quality(texto: str, theme: str = "", tipo_doc: str = "", dist: Optional[float] = None) -> float:
    txt = (texto or "").strip()
    if not txt:
        return -999.0
    score = 0.0
    if dist is not None and not (isinstance(dist, float) and math.isnan(dist)):
        score += max(0.0, 3.8 - float(dist))
    score += _materiality_bonus(txt)
    score += _strategic_bonus(txt, theme=theme, tipo_doc=tipo_doc)
    score -= _generic_penalty(txt)
    n = len(txt)
    if 220 <= n <= 1500:
        score += 0.9
    elif 120 <= n < 220:
        score += 0.3
    elif n < 100:
        score -= 1.0

    tipo_blob = (tipo_doc or "").lower()
    if "fato relevante" in tipo_blob:
        score += 1.2
    elif "comunicado" in tipo_blob:
        score += 0.8
    elif any(k in tipo_blob for k in ["itr", "dfp", "fre"]):
        score += 0.6
    return score


def _take_best(
    pool: List[RagHit],
    selected: List[RagHit],
    used: set,
    target: int,
    per_doc: Dict[int, int],
    per_year: Dict[str, int],
    per_tipo: Dict[str, int],
    max_per_doc: int,
) -> None:
    for cand in pool:
        if len(selected) >= target:
            return
        key = (cand.doc_id, cand.chunk_text)
        if key in used:
            continue
        if cand.doc_id is not None and per_doc[int(cand.doc_id)] >= max_per_doc:
            continue
        year = _extract_year(cand.data_doc) or "sem_ano"
        tipo = _tipo_bucket(cand.tipo_doc)
        selected.append(cand)
        used.add(key)
        if cand.doc_id is not None:
            per_doc[int(cand.doc_id)] += 1
        per_year[year] += 1
        per_tipo[tipo] += 1


def _select_diverse_hits(hits: List[RagHit], top_k: int) -> List[RagHit]:
    top_k = max(12, int(top_k))
    year_buckets: Dict[str, List[RagHit]] = defaultdict(list)
    tipo_buckets: Dict[str, List[RagHit]] = defaultdict(list)
    by_all: List[RagHit] = list(hits)

    for h in hits:
        year_buckets[_bucket_for_months(h.data_doc)].append(h)
        tipo_buckets[_tipo_bucket(h.tipo_doc)].append(h)

    for bucket in year_buckets.values():
        bucket.sort(key=lambda h: (-h.score, str(h.data_doc or ""), -(int(h.doc_id) if h.doc_id else 0), h.chunk_text[:48]))
    for bucket in tipo_buckets.values():
        bucket.sort(key=lambda h: (-h.score, str(h.data_doc or ""), -(int(h.doc_id) if h.doc_id else 0), h.chunk_text[:48]))

    selected: List[RagHit] = []
    used = set()
    per_doc: Dict[int, int] = defaultdict(int)
    per_year: Dict[str, int] = defaultdict(int)
    per_tipo: Dict[str, int] = defaultdict(int)

    # quota temporal agressiva, mas segura
    temporal_targets = {
        "0_12m": 3,
        "12_24m": 3,
        "24_36m": 2,
    }
    if top_k >= 14:
        temporal_targets["24_36m"] = 3
    for bucket_name, target in temporal_targets.items():
        _take_best(year_buckets.get(bucket_name, []), selected, used, len(selected) + target, per_doc, per_year, per_tipo, max_per_doc=2)

    # quota por tipo documental
    tipo_targets = {
        "financeiro": 3,
        "evento": 3,
        "governanca": 1,
    }
    for bucket_name, target in tipo_targets.items():
        _take_best(tipo_buckets.get(bucket_name, []), selected, used, len(selected) + target, per_doc, per_year, per_tipo, max_per_doc=2)

    # completar com melhores scores, permitindo mais densidade mas mantendo diversidade
    _take_best(by_all, selected, used, top_k, per_doc, per_year, per_tipo, max_per_doc=3)

    selected.sort(key=lambda h: (-h.score, str(h.data_doc or ""), -(int(h.doc_id) if h.doc_id else 0), h.chunk_text[:48]))
    return selected[:top_k]


def get_topk_chunks_inteligente(
    ticker: str,
    top_k: int = 24,
    months_window: int = 36,
    debug: bool = False,
    period_ref: Optional[str] = None,
):
    engine = get_supabase_engine()
    llm_client = llm_factory.get_llm_client()
    topics = None
    top_k = max(14, int(top_k))
    recall_total = max(top_k * 5, 120)
    per_topic_k = max(18, math.ceil(recall_total / 10))

    with engine.connect() as conn:
        rows, _stats = retrieve_multitopic_chunks(
            conn=conn,
            llm_client=llm_client,
            ticker=str(ticker).strip().upper(),
            period_ref=period_ref,
            months_back=int(months_window),
            top_k_total=recall_total,
            per_topic_k=per_topic_k,
            topics=topics,
        )

    hits: List[RagHit] = []
    for row in rows:
        txt = str(row.get("chunk_text") or "").strip()
        if not txt:
            continue
        hit = RagHit(
            doc_id=row.get("doc_id"),
            ticker=str(row.get("ticker") or ticker).strip().upper(),
            chunk_text=txt,
            dist=row.get("dist"),
            data_doc=str(row.get("data_doc") or ""),
            tipo_doc=str(row.get("tipo_doc") or ""),
            strategic_theme=str(row.get("topic") or ""),
        )
        hit.score = _score_text_quality(
            hit.chunk_text,
            theme=hit.strategic_theme,
            tipo_doc=hit.tipo_doc,
            dist=hit.dist,
        )
        hits.append(hit)

    hits.sort(key=lambda h: (-h.score, str(h.data_doc or ""), -(int(h.doc_id) if h.doc_id else 0), h.chunk_text[:48]))
    final_hits = _select_diverse_hits(hits, top_k=top_k)
    return final_hits


def summarize_retrieval_mix(hits: List[Any]) -> Dict[str, Any]:
    years: Dict[str, int] = defaultdict(int)
    tipos: Dict[str, int] = defaultdict(int)
    temas: Dict[str, int] = defaultdict(int)
    buckets: Dict[str, int] = defaultdict(int)
    docs = set()
    for h in hits or []:
        year = _extract_year(getattr(h, "data_doc", "") or "") or "sem_ano"
        years[year] += 1
        tipo = str(getattr(h, "tipo_doc", "") or "sem_tipo").strip() or "sem_tipo"
        tipos[tipo] += 1
        tema = str(getattr(h, "strategic_theme", "") or "geral").strip() or "geral"
        temas[tema] += 1
        buckets[_bucket_for_months(getattr(h, "data_doc", "") or "")] += 1
        doc_id = getattr(h, "doc_id", None)
        if doc_id is not None:
            docs.add(doc_id)
    return {
        "total_hits": len(hits or []),
        "docs": len(docs),
        "years": dict(sorted(years.items(), reverse=True)),
        "time_buckets": dict(sorted(buckets.items())),
        "tipos": dict(sorted(tipos.items(), key=lambda kv: (-kv[1], kv[0]))),
        "temas": dict(sorted(temas.items(), key=lambda kv: (-kv[1], kv[0]))),
    }
