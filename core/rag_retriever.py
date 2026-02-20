# core/rag_retriever.py
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from core.db import get_engine  # ajuste se seu helper tiver outro nome
from sqlalchemy import text


INTENT_PATTERNS: List[Tuple[str, float]] = [
    (r"\bcapex\b|\binvestiment", 1.2),
    (r"\bexpans", 1.0),
    (r"\bguidance\b|\bproje", 1.0),
    (r"\bd[ií]vida\b|\balavanc", 1.2),
    (r"\bamortiza\b|\brefinancia\b|\bcovenant\b", 1.1),
    (r"\bdividend\b|\bjcp\b|\bpayout\b|\bdistribui", 1.2),
    (r"\brecompra\b|\bbuyback\b", 1.1),
    (r"\baquisi", 1.1),
    (r"\bfus[aã]o\b|\bincorpora\b|\bcis[aã]o\b", 1.0),
    (r"\bdesinvest\b|\baliena\b|\bvenda de ativo\b", 1.1),
    (r"\baloca(c|ç)[aã]o de capital\b", 1.2),
    (r"\bplano estrat", 0.9),
]

DOC_WEIGHT: Dict[str, float] = {
    "FATO RELEVANTE": 1.20,
    "ATA": 1.10,
    "COMUNICADO AO MERCADO": 1.00,
    "AVISO AOS ACIONISTAS": 1.00,
    "RELEASE": 0.80,
    "APRESENTAÇÃO": 0.80,
}


def _norm_tipo(tipo: Optional[str]) -> str:
    return (tipo or "").strip().upper()


def _intent_score(texto: str) -> float:
    t = (texto or "").lower()
    score = 0.0
    for pat, w in INTENT_PATTERNS:
        if re.search(pat, t, flags=re.IGNORECASE):
            score += w
    # normalização simples (evita score infinito)
    return 1.0 - math.exp(-score)  # 0..~1


def _recency_score(data_doc: Optional[datetime], half_life_days: int = 180) -> float:
    if not data_doc:
        return 0.2
    now = datetime.now(timezone.utc)
    if data_doc.tzinfo is None:
        data_doc = data_doc.replace(tzinfo=timezone.utc)
    days = max(0.0, (now - data_doc).total_seconds() / 86400.0)
    return math.exp(-days / float(half_life_days))


def _doc_weight(tipo_doc: Optional[str]) -> float:
    t = _norm_tipo(tipo_doc)
    # match “ATA …”, “FATO RELEVANTE …”
    for k, w in DOC_WEIGHT.items():
        if t.startswith(k):
            return w
    return 0.60


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


def get_topk_chunks_inteligente(
    ticker: str,
    top_k: int = 8,
    months_window: int = 18,
    half_life_days: int = 180,
    debug: bool = False,
) -> List[ChunkHit] | List[str]:
    """
    Recupera Top-K chunks com foco em intenção futura / alocação de capital.
    Versão v1: sem embeddings. (Já resolve 80% do problema de ruído.)
    """

    engine = get_engine()
    sql = text("""
        SELECT
            c.id::text AS chunk_id,
            c.doc_id::text AS doc_id,
            c.ticker,
            COALESCE(c.tipo_doc, d.tipo) AS tipo_doc,
            COALESCE(c.data_doc, d.data_doc) AS data_doc,
            c.chunk_text
        FROM public.docs_corporativos_chunks c
        JOIN public.docs_corporativos d ON d.id = c.doc_id
        WHERE c.ticker = :ticker
          AND COALESCE(d.data_doc, c.data_doc) >= (NOW() - (:months || ' months')::interval)
          AND c.chunk_text IS NOT NULL
          AND length(c.chunk_text) > 80
        LIMIT 5000
    """)

    rows = []
    with engine.begin() as conn:
        rows = conn.execute(sql, {"ticker": ticker, "months": months_window}).mappings().all()

    # pontua
    hits: List[ChunkHit] = []
    for r in rows:
        txt = r["chunk_text"]
        tipo = r["tipo_doc"]
        dt = r["data_doc"]

        s_int = _intent_score(txt)
        s_rec = _recency_score(dt, half_life_days=half_life_days)
        w_tipo = _doc_weight(tipo)

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
            )
        )

    # rank
    hits.sort(key=lambda x: x.score_final, reverse=True)

    # diversidade mínima (por tipo/“assunto”)
    selected: List[ChunkHit] = []

    def pick_first(pred):
        for h in hits:
            if h in selected:
                continue
            if pred(h):
                selected.append(h)
                return

    # garante pelo menos 1 “formal” se existir
    pick_first(lambda h: h.tipo_doc.startswith("FATO RELEVANTE") or h.tipo_doc.startswith("COMUNICADO"))
    # garante 1 dívida se existir
    pick_first(lambda h: h.score_intent > 0.0 and re.search(r"d[ií]vida|alavanc|amortiza|refinancia|covenant", h.chunk_text, re.I))
    # garante 1 payout/recompra se existir
    pick_first(lambda h: h.score_intent > 0.0 and re.search(r"dividend|jcp|payout|recompra|buyback", h.chunk_text, re.I))

    # completa até top_k
    for h in hits:
        if len(selected) >= top_k:
            break
        if h not in selected:
            selected.append(h)

    if debug:
        return selected
    return [h.chunk_text for h in selected]
