# core/rag_multitopic.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from sqlalchemy import text
from core.db_loader import get_supabase_engine

@dataclass
class ChunkHit:
    doc_id: int
    ticker: str
    chunk_text: str
    score: float
    tag: str

DEFAULT_TOPICS: Dict[str, str] = {
    'resultado': 'resultado trimestral, receita, ebitda, margem, lucro, principais variações',
    'divida': 'dívida, alavancagem, caixa, liquidez, covenant, rolagem',
    'capex': 'capex, investimentos, projetos, expansão, manutenção',
    'dividendos': 'dividendos, payout, juros sobre capital, política, recompras',
    'guidance': 'guidance, projeções, outlook, metas, expectativas',
    'riscos': 'riscos, contingências, processos, regulação, câmbio, juros, commodities',
    'eventos': 'fatos relevantes, aquisições, desinvestimentos, reestruturação, M&A',
}

def _norm_ticker(t: str) -> str:
    return (t or '').strip().upper().replace('.SA', '')

def _emb_to_pgvector_str(emb: list) -> str:
    return '[' + ','.join(f'{float(x):.10f}' for x in emb) + ']'

def _search_chunks_sql() -> str:
    return '''
        select
            c.doc_id,
            c.ticker,
            c.chunk_text,
            (c.embedding <-> (:emb)::vector) as dist
        from public.docs_corporativos_chunks c
        where c.ticker = :ticker
        order by c.embedding <-> (:emb)::vector asc
        limit :lim
    '''

def retrieve_multitopic_chunks(
    *,
    ticker: str,
    llm_client,
    period_ref: str,
    top_k_total: int = 24,
    per_topic_k: int = 8,
    topics: Optional[Dict[str, str]] = None,
) -> Tuple[List[ChunkHit], Dict[str, int]]:

    tk = _norm_ticker(ticker)
    topics = topics or DEFAULT_TOPICS
    engine = get_supabase_engine()
    all_hits: List[ChunkHit] = []
    stats = {'topics': len(topics), 'raw_hits': 0, 'selected': 0}
    sql = _search_chunks_sql()

    with engine.begin() as conn:
        for tag, q in topics.items():
            query_text = f'{tk}: {q} no contexto de relatórios e fatos corporativos.'
            emb = llm_client.embed([query_text])[0]
            emb_str = _emb_to_pgvector_str(emb)

            rows = conn.execute(
                text(sql),
                {'ticker': tk, 'emb': emb_str, 'lim': int(per_topic_k)},
            ).fetchall()

            for row in rows:
                doc_id, tkr, chunk_text, dist = row
                if not chunk_text:
                    continue
                if dist is None:
                    continue
                try:
                    score = 1.0 / (1.0 + float(dist))
                except Exception:
                    continue
                all_hits.append(
                    ChunkHit(
                        doc_id=int(doc_id),
                        ticker=str(tkr),
                        chunk_text=str(chunk_text),
                        score=score,
                        tag=tag,
                    )
                )

    stats['raw_hits'] = len(all_hits)
    selected = sorted(all_hits, key=lambda x: x.score, reverse=True)[:top_k_total]
    stats['selected'] = len(selected)
    return selected, stats
