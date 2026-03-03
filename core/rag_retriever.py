
# core/rag_retriever.py
from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple
from sqlalchemy.engine import Engine

from .rag_multitopic import ChunkEvidence, fetch_chunks_for_ticker, build_topic_batches


DEFAULT_TOPICS: List[str] = [
    "Tese e drivers",
    "Riscos e pontos de atenção",
    "Resultados e rentabilidade",
    "Endividamento e liquidez",
    "Governança e eventos",
    "Perspectiva 12 meses",
]


def retrieve_multitopic(
    engine: Engine,
    ticker: str,
    janela_meses: int = 12,
    period_ref: Optional[str] = None,
    top_k_por_topico: Optional[int] = None,
    max_docs: Optional[int] = None,
    max_pdfs: Optional[int] = None,
    max_chunks_total: Optional[int] = None,
    topics: Optional[Sequence[str]] = None,
) -> Tuple[Dict[str, List[ChunkEvidence]], List[ChunkEvidence]]:
    """Returns (topic->evidences, all_evidences)."""
    all_evid = fetch_chunks_for_ticker(
        engine=engine,
        ticker=ticker,
        janela_meses=janela_meses,
        period_ref=period_ref,
        max_docs=max_docs,
        max_pdfs=max_pdfs,
        max_chunks_total=max_chunks_total,
    )
    tlist = list(topics) if topics else DEFAULT_TOPICS
    by_topic = build_topic_batches(all_evid, tlist, top_k_por_topico=top_k_por_topico)
    return by_topic, all_evid
