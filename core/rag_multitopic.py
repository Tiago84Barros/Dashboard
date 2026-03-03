
# core/rag_multitopic.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
from datetime import datetime, timedelta, date

from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class ChunkEvidence:
    chunk_id: str
    doc_id: str
    ticker: str
    created_at: Optional[datetime]
    doc_date: Optional[date]
    title: Optional[str]
    source: Optional[str]
    url: Optional[str]
    chunk_text: str


def _months_ago(ref: Optional[date], months: int) -> date:
    # Simple month window: months*31 days (good enough for filtering)
    ref_date = ref or date.today()
    return ref_date - timedelta(days=int(months) * 31)


def fetch_chunks_for_ticker(
    engine: Engine,
    ticker: str,
    janela_meses: int = 12,
    period_ref: Optional[str] = None,
    max_docs: Optional[int] = None,
    max_pdfs: Optional[int] = None,
    max_chunks_total: Optional[int] = None,
) -> List[ChunkEvidence]:
    """Fetch chunk evidences for a ticker.

    Philosophy (DEEP):
      - No hard Top-K by default (caller may pass max_chunks_total=None for 'unbounded').
      - No forced single-quarter filtering. period_ref is optional and not required.

    Notes:
      - This function relies only on SQL ordering by recency. If you later add vector search,
        keep this as a robust fallback.
    """

    since = _months_ago(None, max(1, int(janela_meses)))

    # We try to join docs + chunks; tolerate different column names using COALESCE.
    # Expected tables:
    #   public.docs_corporativos (id, ticker, data_ref/date_ref, titulo/title, fonte/source, url/link, created_at)
    #   public.docs_corporativos_chunks (id, doc_id, ticker, chunk_text/text, created_at, chunk_index)
    #
    # If your column names differ, adjust the COALESCE list.
    sql = f"""
    with docs as (
        select
            d.id as doc_id,
            d.ticker as ticker,
            coalesce(d.data_ref, d.date_ref, d.dt_ref) as doc_date,
            coalesce(d.titulo, d.title, d.nome, d.document_title) as title,
            coalesce(d.fonte, d.source, d.origem) as source,
            coalesce(d.url, d.link, d.document_url) as url,
            d.created_at as doc_created_at
        from public.docs_corporativos d
        where upper(d.ticker) = upper(:ticker)
          and coalesce(d.data_ref, d.date_ref, d.dt_ref, d.created_at::date) >= :since
        order by coalesce(d.data_ref, d.date_ref, d.dt_ref, d.created_at::date) desc,
                 d.created_at desc
        {"limit " + str(int(max_docs)) if max_docs else ""}
    )
    select
        c.id::text as chunk_id,
        c.doc_id::text as doc_id,
        upper(c.ticker) as ticker,
        c.created_at as created_at,
        docs.doc_date as doc_date,
        docs.title as title,
        docs.source as source,
        docs.url as url,
        coalesce(c.chunk_text, c.text, c.conteudo, c.content) as chunk_text
    from public.docs_corporativos_chunks c
    join docs on docs.doc_id = c.doc_id
    where upper(c.ticker) = upper(:ticker)
    order by
        docs.doc_date desc nulls last,
        c.created_at desc nulls last,
        c.id
    {"limit " + str(int(max_chunks_total)) if max_chunks_total else ""}
    """

    evidences: List[ChunkEvidence] = []
    with engine.connect() as conn:
        rows = conn.execute(
            text(sql),
            {
                "ticker": ticker,
                "since": since,
            },
        ).mappings().all()

    for r in rows:
        txt = (r.get("chunk_text") or "").strip()
        if not txt:
            continue
        evidences.append(
            ChunkEvidence(
                chunk_id=r.get("chunk_id"),
                doc_id=r.get("doc_id"),
                ticker=r.get("ticker") or ticker,
                created_at=r.get("created_at"),
                doc_date=r.get("doc_date"),
                title=r.get("title"),
                source=r.get("source"),
                url=r.get("url"),
                chunk_text=txt,
            )
        )
    return evidences


def build_topic_batches(
    evidences: Sequence[ChunkEvidence],
    topics: Sequence[str],
    top_k_por_topico: Optional[int] = None,
) -> Dict[str, List[ChunkEvidence]]:
    """Distribute evidences into topics.

    Without embeddings we cannot truly classify; we provide a deterministic split:
      - If top_k_por_topico is None -> use all evidences for every topic (max depth).
      - Else -> take the first K evidences (recency-ordered by fetch) for every topic.
    """
    out: Dict[str, List[ChunkEvidence]] = {}
    for t in topics:
        if top_k_por_topico is None:
            out[t] = list(evidences)
        else:
            out[t] = list(evidences)[: int(top_k_por_topico)]
    return out
