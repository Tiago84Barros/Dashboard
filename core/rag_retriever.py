
# core/rag_retriever.py
from __future__ import annotations

import math
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from core.db import get_engine
from sqlalchemy import text


INTENT_PATTERNS: List[Tuple[str, float]] = [
    (r"\bcapex\b|\binvestiment", 1.20),
    (r"\bexpans", 1.00),
    (r"\bguidance\b|\bproje", 1.00),
    (r"\bd[ií]vida\b|\balavanc", 1.25),
    (r"\bamortiza\b|\brefinancia\b|\bcovenant\b", 1.10),
    (r"\bdividend\b|\bjcp\b|\bpayout\b|\bdistribui", 1.20),
    (r"\brecompra\b|\bbuyback\b", 1.10),
    (r"\baquisi", 1.10),
    (r"\bfus[aã]o\b|\bincorpora\b|\bcis[aã]o\b", 1.00),
    (r"\bdesinvest\b|\baliena\b|\bvenda de ativo\b", 1.10),
    (r"\baloca(c|ç)[aã]o de capital\b", 1.20),
    (r"\bplano estrat", 0.90),
    (r"\bgovernan", 0.85),
    (r"\befici[êe]ncia operacional\b|\bprodutividade\b|\bmargin", 0.90),
]

DOC_WEIGHT: Dict[str, float] = {
    "FATO RELEVANTE": 1.20,
    "ATA": 1.10,
    "COMUNICADO AO MERCADO": 1.00,
    "AVISO AOS ACIONISTAS": 1.00,
    "FORMULÁRIO DE REFERÊNCIA": 0.95,
    "RELEASE": 0.80,
    "APRESENTAÇÃO": 0.80,
}

STRATEGIC_THEMES: Dict[str, str] = {
    "capital_allocation": r"dividend|jcp|payout|recompra|buyback|aloca(c|ç)[aã]o de capital|retorno ao acionista",
    "debt": r"d[ií]vida|alavanc|amortiza|refinancia|covenant|desalavanc",
    "growth_capex": r"capex|investiment|expans|nova planta|aumento de capacidade",
    "mna_portfolio": r"aquisi|fus[aã]o|incorpora|cis[aã]o|desinvest|aliena|venda de ativo",
    "guidance_execution": r"guidance|proje|meta|entrega|cronograma|execu(c|ç)[aã]o",
    "governance": r"governan|comit[eê]|conselho|tag along|minorit[aá]rio",
}


def _norm_tipo(tipo: Optional[str]) -> str:
    return (tipo or "").strip().upper()


def _norm_text(texto: str) -> str:
    return re.sub(r"\s+", " ", (texto or "").strip())


def _intent_score(texto: str) -> float:
    t = (texto or "").lower()
    score = 0.0
    for pat, w in INTENT_PATTERNS:
        if re.search(pat, t, flags=re.IGNORECASE):
            score += w
    return 1.0 - math.exp(-score)


def _recency_score(data_doc: Optional[datetime], half_life_days: int = 180) -> float:
    if not data_doc:
        return 0.20
    now = datetime.now(timezone.utc)
    if data_doc.tzinfo is None:
        data_doc = data_doc.replace(tzinfo=timezone.utc)
    days = max(0.0, (now - data_doc).total_seconds() / 86400.0)
    return math.exp(-days / float(half_life_days))


def _doc_weight(tipo_doc: Optional[str]) -> float:
    t = _norm_tipo(tipo_doc)
    for k, w in DOC_WEIGHT.items():
        if t.startswith(k):
            return w
    return 0.60


def _theme_name(texto: str) -> str:
    t = (texto or "").lower()
    for theme, pat in STRATEGIC_THEMES.items():
        if re.search(pat, t, re.IGNORECASE):
            return theme
    return "general"


def _text_fingerprint(texto: str, max_len: int = 220) -> str:
    base = _norm_text(texto).lower()
    base = re.sub(r"[^a-z0-9áàâãéèêíïóôõöúç ]+", " ", base)
    base = re.sub(r"\s+", " ", base)
    return base[:max_len]


@dataclass
class ChunkHit:
    chunk_id: str
    doc_id: str
    ticker: str
    tipo_doc: str
    data_doc: Optional[datetime]
    chunk_text: str
    score_final: float
    score_intent: float
    score_recency: float
    weight_tipo: float
    temporal_bucket: str = "recent"
    strategic_theme: str = "general"


def _fetch_candidate_rows(ticker: str, months_window: int) -> list:
    engine = get_engine()
    sql = text(
        """
        SELECT
            c.id::text AS chunk_id,
            c.doc_id::text AS doc_id,
            c.ticker,
            COALESCE(c.tipo_doc, d.tipo, d.tipo_doc) AS tipo_doc,
            COALESCE(c.document_date, c.data_doc, d.data, d.data_doc) AS data_doc,
            c.chunk_text
        FROM public.docs_corporativos_chunks c
        JOIN public.docs_corporativos d ON d.id = c.doc_id
        WHERE c.ticker = :ticker
          AND COALESCE(c.document_date, c.data_doc, d.data, d.data_doc) >= (NOW() - (:months || ' months')::interval)
          AND c.chunk_text IS NOT NULL
          AND length(c.chunk_text) > 80
        LIMIT 5000
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(sql, {"ticker": ticker, "months": months_window}).mappings().all()

    return rows


def _bucketize_by_recency(rows: list) -> Dict[str, list]:
    dated = [r for r in rows if r.get("data_doc") is not None]
    undated = [r for r in rows if r.get("data_doc") is None]

    if not dated:
        return {"recent": rows, "intermediate": [], "historical": []}

    dated_sorted = sorted(dated, key=lambda r: r["data_doc"], reverse=True)
    n = len(dated_sorted)

    cut_recent = max(1, math.ceil(n * 0.35))
    cut_intermediate = max(cut_recent + 1, math.ceil(n * 0.70))

    recent = dated_sorted[:cut_recent]
    intermediate = dated_sorted[cut_recent:cut_intermediate]
    historical = dated_sorted[cut_intermediate:]

    if undated:
        historical.extend(undated)

    return {
        "recent": recent,
        "intermediate": intermediate,
        "historical": historical,
    }


def _score_rows(rows: list, half_life_days: int) -> List[ChunkHit]:
    hits: List[ChunkHit] = []

    for r in rows:
        txt = r["chunk_text"]
        tipo = r["tipo_doc"]
        dt = r["data_doc"]

        s_int = _intent_score(txt)
        s_rec = _recency_score(dt, half_life_days=half_life_days)
        w_tipo = _doc_weight(tipo)
        theme = _theme_name(txt)

        final = 0.55 * s_int + 0.30 * s_rec + 0.15 * w_tipo

        hits.append(
            ChunkHit(
                chunk_id=r["chunk_id"],
                doc_id=r["doc_id"],
                ticker=r["ticker"],
                tipo_doc=_norm_tipo(tipo),
                data_doc=dt,
                chunk_text=txt,
                score_final=final,
                score_intent=s_int,
                score_recency=s_rec,
                weight_tipo=w_tipo,
                strategic_theme=theme,
            )
        )

    return hits


def _dedupe_hits(hits: List[ChunkHit]) -> List[ChunkHit]:
    seen = set()
    out: List[ChunkHit] = []

    for h in hits:
        fp = (h.doc_id, _text_fingerprint(h.chunk_text))
        if fp in seen:
            continue
        seen.add(fp)
        out.append(h)

    return out


def _enforce_diversity(pool: List[ChunkHit], limit: int) -> List[ChunkHit]:
    selected: List[ChunkHit] = []
    used_docs = set()
    theme_counts = defaultdict(int)

    for h in pool:
        if len(selected) >= limit:
            break

        same_doc_pressure = h.doc_id in used_docs
        overloaded_theme = theme_counts[h.strategic_theme] >= 2 and h.strategic_theme != "general"

        if same_doc_pressure and overloaded_theme:
            continue

        selected.append(h)
        used_docs.add(h.doc_id)
        theme_counts[h.strategic_theme] += 1

    if len(selected) < limit:
        for h in pool:
            if len(selected) >= limit:
                break
            if h not in selected:
                selected.append(h)

    return selected


def _select_balanced_hits(
    bucket_hits: Dict[str, List[ChunkHit]],
    top_k: int,
    bucket_weights: Optional[Dict[str, float]] = None,
) -> List[ChunkHit]:
    bucket_weights = bucket_weights or {
        "recent": 0.40,
        "intermediate": 0.35,
        "historical": 0.25,
    }

    quotas = {}
    allocated = 0
    ordered_buckets = ["recent", "intermediate", "historical"]

    for i, name in enumerate(ordered_buckets):
        if i < len(ordered_buckets) - 1:
            q = int(round(top_k * bucket_weights.get(name, 0.0)))
            quotas[name] = q
            allocated += q
        else:
            quotas[name] = max(0, top_k - allocated)

    selected: List[ChunkHit] = []

    for name in ordered_buckets:
        pool = sorted(bucket_hits.get(name, []), key=lambda x: x.score_final, reverse=True)
        pool = _dedupe_hits(pool)
        pool = _enforce_diversity(pool, quotas[name])

        for h in pool[: quotas[name]]:
            h.temporal_bucket = name
            selected.append(h)

    if len(selected) < top_k:
        leftovers: List[ChunkHit] = []
        for name in ordered_buckets:
            for h in sorted(bucket_hits.get(name, []), key=lambda x: x.score_final, reverse=True):
                h.temporal_bucket = name
                leftovers.append(h)

        leftovers = _dedupe_hits(leftovers)
        leftovers = [h for h in leftovers if h not in selected]

        for h in leftovers:
            if len(selected) >= top_k:
                break
            selected.append(h)

    selected.sort(
        key=lambda x: (
            {"recent": 0, "intermediate": 1, "historical": 2}.get(x.temporal_bucket, 9),
            -x.score_final,
        )
    )
    return selected[:top_k]


def get_topk_chunks_inteligente(
    ticker: str,
    top_k: int = 8,
    months_window: int = 36,
    half_life_days: int = 240,
    debug: bool = False,
    bucket_weights: Optional[Dict[str, float]] = None,
) -> List[ChunkHit] | List[str]:
    """
    Recupera Top-K chunks com balanceamento temporal e diversidade temática.

    Melhorias vs v1:
    - bucket temporal balanceado: recente/intermediário/histórico
    - score híbrido: intenção + recência + tipo documental
    - deduplicação por documento/texto
    - diversidade por tema estratégico
    - compatível com a interface anterior
    """

    rows = _fetch_candidate_rows(ticker=ticker, months_window=months_window)

    if not rows:
        return [] if debug else []

    bucket_rows = _bucketize_by_recency(rows)
    bucket_hits = {name: _score_rows(items, half_life_days=half_life_days) for name, items in bucket_rows.items()}
    selected = _select_balanced_hits(bucket_hits, top_k=top_k, bucket_weights=bucket_weights)

    if debug:
        return selected

    return [h.chunk_text for h in selected]


def summarize_retrieval_mix(hits: List[ChunkHit]) -> Dict[str, object]:
    """
    Função auxiliar para auditoria do RAG.
    Útil para logging e para exibir cobertura do retriever no dashboard.
    """
    buckets = defaultdict(int)
    themes = defaultdict(int)
    tipos = defaultdict(int)

    min_date = None
    max_date = None

    for h in hits:
        buckets[h.temporal_bucket] += 1
        themes[h.strategic_theme] += 1
        tipos[h.tipo_doc] += 1

        if h.data_doc is not None:
            min_date = h.data_doc if min_date is None else min(min_date, h.data_doc)
            max_date = h.data_doc if max_date is None else max(max_date, h.data_doc)

    return {
        "n_chunks": len(hits),
        "bucket_mix": dict(buckets),
        "theme_mix": dict(themes),
        "tipo_doc_mix": dict(tipos),
        "date_range": {
            "min": min_date.isoformat() if min_date else None,
            "max": max_date.isoformat() if max_date else None,
        },
    }
