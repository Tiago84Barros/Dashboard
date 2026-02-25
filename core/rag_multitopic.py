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
    "resultado": "resultado trimestral, receita, ebitda, margem, lucro, principais variações",
    "divida": "dívida, alavancagem, caixa, liquidez, covenant, rolagem",
    "capex": "capex, investimentos, projetos, expansão, manutenção",
    "dividendos": "dividendos, payout, juros sobre capital, política, recompras",
    "guidance": "guidance, projeções, outlook, metas, expectativas",
    "riscos": "riscos, contingências, processos, regulação, câmbio, juros, commodities",
    "eventos": "fatos relevantes, aquisições, desinvestimentos, reestruturação, M&A",
}


def _norm_ticker(t: str) -> str:
    return (t or "").strip().upper().replace(".SA", "")


def _emb_to_pgvector_str(emb: list) -> str:
    """Converte lista de floats em literal compatível com pgvector: [0.1,0.2,...]."""
    return "[" + ",".join(f"{float(x):.10f}" for x in emb) + "]"


def _search_chunks_sql() -> str:
    # IMPORTANTE: cast explícito (:emb)::vector para evitar vector <-> numeric[].
    return """
        select
            c.doc_id,
            c.ticker,
            c.chunk_text,
            (c.embedding <-> (:emb)::vector) as dist
        from public.docs_corporativos_chunks c
        where c.ticker = :ticker
        order by c.embedding <-> (:emb)::vector asc
        limit :lim
    """


def _mmr_select(hits: List[ChunkHit], k: int) -> List[ChunkHit]:
    """Seleção simples para diversidade: tenta variar doc_id e tag."""
    out: List[ChunkHit] = []
    used_docs = set()
    used_tags = set()

    for h in sorted(hits, key=lambda x: x.score, reverse=True):
        if len(out) >= k:
            break
        # evita repetir o mesmo doc e mesma tag ao mesmo tempo
        if h.doc_id in used_docs and h.tag in used_tags:
            continue
        out.append(h)
        used_docs.add(h.doc_id)
        used_tags.add(h.tag)

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
    Recupera chunks por múltiplos tópicos (resultado, dívida, capex...) e retorna
    um conjunto mais diversificado, para relatórios mais ricos.

    Observação: period_ref é mantido na assinatura para compatibilidade com o app,
    mas o filtro temporal deve ser aplicado via metadados (doc.data) no SQL em
    uma próxima iteração (ex.: d.data >= ...), quando você definir regra.
    """
    tk = _norm_ticker(ticker)
    topics = topics or DEFAULT_TOPICS

    engine = get_supabase_engine()
    all_hits: List[ChunkHit] = []
    stats = {"topics": len(topics), "raw_hits": 0, "selected": 0}

    sql = _search_chunks_sql()

    with engine.begin() as conn:
        for tag, q in topics.items():
            query_text = f"{tk}: {q} no contexto de relatórios, fatos relevantes e comunicados corporativos."
            emb = llm_client.embed([query_text])[0]
            emb_str = _emb_to_pgvector_str(emb)

            rows = conn.execute(
                text(sql),
                {"ticker": tk, "emb": emb_str, "lim": int(per_topic_k)},
            ).fetchall()

            for doc_id, tkr, chunk_text, dist in rows:
                # dist menor = mais similar; convertemos para score monotônico
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
