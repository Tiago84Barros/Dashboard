# core/rag_multitopic.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from sqlalchemy import text

# Você já tem isso no projeto
from core.db_loader import get_supabase_engine


@dataclass
class ChunkHit:
    doc_id: int
    ticker: str
    chunk_text: str
    score: float
    tag: str  # qual query/tópico trouxe


DEFAULT_TOPICS: Dict[str, str] = {
    "resultado": "resultado trimestral, receita, ebitda, margem, lucro, principais variações",
    "divida": "dívida, alavancagem, caixa, liquidez, covenant, rolagem",
    "capex": "capex, investimentos, projetos, expansão, manutenção",
    "dividendos": "dividendos, payout, juros sobre capital, política, recompras",
    "guidance": "guidance, projeções, outlook, metas, expectativas",
    "riscos": "riscos, contingências, processos, regulação, câmbio, juros, commodities",
    "eventos": "fatos relevantes, aquisições, desinvestimentos, reestruturação, M&A",
}

def _norm_ticker(t: str) -> str:
    return (t or "").strip().upper().replace(".SA", "").strip()

def _period_to_key(period_ref: str) -> str:
    # seu period_ref é algo como "2024Q4"; usamos só para filtrar por "data" (quando existir)
    return (period_ref or "").strip().upper()

def _search_chunks_sql(
    *,
    ticker: str,
    query_emb: list,
    limit: int,
    period_ref: Optional[str] = None,
    quarters_back: int = 0,
) -> str:
    """
    Observação: sua tabela docs_corporativos_chunks tem embedding e chunk_text (confirmado no chunking).:contentReference[oaicite:2]{index=2}
    Aqui fazemos um similarity search padrão em pgvector: (embedding <-> :emb).
    Se você não tiver pgvector no Supabase, adapte para o método que já usa hoje.
    """
    base = """
        select
            c.doc_id,
            c.ticker,
            c.chunk_text,
            (c.embedding <-> :emb) as dist
        from public.docs_corporativos_chunks c
        where c.ticker = :ticker
    """

    # Se sua tabela docs_corporativos (docs) tem coluna "data" preenchida,
    # dá pra filtrar por janela. Se não tiver, remova esse join.
    # No ENET você salva "data" no docs_corporativos. :contentReference[oaicite:3]{index=3}
    if period_ref:
        # janela por "quarters_back" é heurística; você pode evoluir para datas reais.
        base += """
          and exists (
            select 1
            from public.docs_corporativos d
            where d.id = c.doc_id
              and coalesce(d.data,'') <> ''
          )
        """

    base += """
        order by c.embedding <-> :emb asc
        limit :lim
    """
    return base

def _mmr_select(hits: List[ChunkHit], k: int) -> List[ChunkHit]:
    """
    MMR simples sem depender de embeddings dos chunks.
    Heurística: garante diversidade por doc_id e por tag.
    """
    out: List[ChunkHit] = []
    used_docs = set()
    used_tags = set()

    # primeiro passa: garante variedade
    for h in sorted(hits, key=lambda x: x.score, reverse=True):
        if len(out) >= k:
            break
        if h.doc_id in used_docs and h.tag in used_tags:
            continue
        out.append(h)
        used_docs.add(h.doc_id)
        used_tags.add(h.tag)

    # completa se faltar
    if len(out) < k:
        for h in sorted(hits, key=lambda x: x.score, reverse=True):
            if len(out) >= k:
                break
            if h in out:
                continue
            out.append(h)

    return out[:k]

def retrieve_multitopic_chunks(
    *,
    ticker: str,
    llm_client,
    period_ref: str,
    top_k_total: int = 24,
    per_topic_k: int = 8,
    topics: Optional[Dict[str, str]] = None,
) -> Tuple[List[ChunkHit], Dict[str, int]]:
    """
    Estratégia:
    - gera N queries por tópico
    - puxa per_topic_k por tópico (recall)
    - combina tudo e aplica MMR para fechar top_k_total
    """
    tk = _norm_ticker(ticker)
    topics = topics or DEFAULT_TOPICS

    engine = get_supabase_engine()
    all_hits: List[ChunkHit] = []
    stats = {"topics": len(topics), "raw_hits": 0, "selected": 0}

    with engine.begin() as conn:
        for tag, q in topics.items():
            query_text = f"{tk}: {q} no contexto de relatórios e fatos corporativos."
            emb = llm_client.embed([query_text])[0]

            sql = _search_chunks_sql(
                ticker=tk,
                query_emb=emb,
                limit=int(per_topic_k),
                period_ref=_period_to_key(period_ref),
            )

            rows = conn.execute(
                text(sql),
                {"ticker": tk, "emb": emb, "lim": int(per_topic_k)},
            ).fetchall()

            for r in rows:
                doc_id, tkr, chunk_text, dist = r
                # score invertido (dist menor = melhor)
                score = 1.0 / (1.0 + float(dist))
                all_hits.append(
                    ChunkHit(
                        doc_id=int(doc_id),
                        ticker=str(tkr),
                        chunk_text=str(chunk_text),
                        score=score,
                        tag=tag,
                    )
                )

    stats["raw_hits"] = len(all_hits)
    selected = _mmr_select(all_hits, int(top_k_total))
    stats["selected"] = len(selected)
    return selected, stats
