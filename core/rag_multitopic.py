# core/rag_multitopic.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import math


DEFAULT_TOPICS = [
    "resultados e guidance",
    "capex e investimentos",
    "endividamento e custo financeiro",
    "dividendos e payout",
    "riscos e contingências",
    "governança e eventos corporativos",
]


@dataclass
class RagStats:
    ticker: str
    period_ref: Optional[str]
    months_back: Optional[int]
    top_k_total: int
    per_topic_k: int
    topics: List[str]
    total_hits: int


def _to_pgvector_literal(emb: List[float]) -> str:
    """
    pgvector aceita literal no formato: '[0.1,0.2,...]'
    """
    # garante float e formato compacto
    vals = []
    for x in emb:
        try:
            vals.append(f"{float(x):.10f}")
        except Exception:
            vals.append("0.0")
    return "[" + ",".join(vals) + "]"


def embed_text(llm_client: Any, text: str) -> List[float]:
    """
    Espera llm_client.embed(text) -> List[float]
    """
    emb = llm_client.embed(text)
    if not isinstance(emb, list) or not emb:
        raise ValueError("Embedding inválido retornado por llm_client.embed()")
    return emb


def _sql_search_chunks(
    conn: Any,
    ticker: str,
    emb_vec_literal: str,
    lim: int,
    period_ref: Optional[str],
    months_back: Optional[int],
) -> List[Dict[str, Any]]:
    """
    Busca chunks por similaridade vetorial com filtros opcionais.
    Requer:
      - public.docs_corporativos_chunks (doc_id, ticker, chunk_text, embedding)
      - public.docs_corporativos (id, data, period_ref)
    """

    # Monta WHERE condicional
    where_period = ""
    if period_ref:
        where_period = " AND d.period_ref = %(period_ref)s "

    where_months = ""
    if months_back and months_back > 0:
        # d.data costuma estar como string YYYY-MM-DD em muitos setups
        # Filtra só quando o formato está ok
        where_months = """
        AND (
            d.data ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            AND to_date(d.data,'YYYY-MM-DD') >= (CURRENT_DATE - (%(months_back)s || ' months')::interval)
        )
        """

    sql = f"""
        SELECT
            c.doc_id,
            c.ticker,
            c.chunk_text,
            (c.embedding <-> (%(emb)s)::vector) AS dist
        FROM public.docs_corporativos_chunks c
        WHERE c.ticker = %(ticker)s
          AND EXISTS (
              SELECT 1
              FROM public.docs_corporativos d
              WHERE d.id = c.doc_id
                AND COALESCE(d.data,'') <> ''
                {where_period}
                {where_months}
          )
        ORDER BY (c.embedding <-> (%(emb)s)::vector) ASC
        LIMIT %(lim)s
    """

    params = {
        "emb": emb_vec_literal,
        "ticker": ticker,
        "lim": int(lim),
    }
    if period_ref:
        params["period_ref"] = period_ref
    if months_back and months_back > 0:
        params["months_back"] = int(months_back)

    rows = conn.execute(sql, params).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        dist = r["dist"] if isinstance(r, dict) else getattr(r, "dist", None)
        try:
            dist_f = float(dist) if dist is not None else 1e9
        except Exception:
            dist_f = None

        out.append(
            {
                "doc_id": r["doc_id"] if isinstance(r, dict) else getattr(r, "doc_id", None),
                "ticker": r["ticker"] if isinstance(r, dict) else getattr(r, "ticker", None),
                "chunk_text": r["chunk_text"] if isinstance(r, dict) else getattr(r, "chunk_text", ""),
                "dist": dist_f,
            }
        )
    return out


def retrieve_multitopic_chunks(
    conn: Any,
    llm_client: Any,
    ticker: str,
    period_ref: Optional[str] = None,
    months_back: Optional[int] = 24,
    top_k_total: int = 32,
    per_topic_k: int = 8,
    topics: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Retorna lista única de hits (dedup por doc_id+chunk_text) e stats.

    - period_ref: se None, NÃO filtra trimestre.
    - months_back: janela móvel (ex.: 24 meses).
    - top_k_total: limite final para contexto.
    - per_topic_k: recupera por tema e depois consolida.
    """

    topics = topics or DEFAULT_TOPICS
    top_k_total = int(max(8, top_k_total))
    per_topic_k = int(max(4, per_topic_k))

    hits_all: List[Dict[str, Any]] = []

    for t in topics:
        query = f"{ticker} — {t}"
        emb = embed_text(llm_client, query)
        emb_lit = _to_pgvector_literal(emb)

        rows = _sql_search_chunks(
            conn=conn,
            ticker=ticker,
            emb_vec_literal=emb_lit,
            lim=per_topic_k,
            period_ref=period_ref,
            months_back=months_back,
        )
        for row in rows:
            row["topic"] = t
        hits_all.extend(rows)

    # Dedup por (doc_id, chunk_text)
    seen = set()
    dedup: List[Dict[str, Any]] = []
    for h in hits_all:
        key = (h.get("doc_id"), h.get("chunk_text"))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(h)

    # Ordena por dist (menor = melhor); None vai pro fim
    def _dist_key(x: Dict[str, Any]) -> float:
        d = x.get("dist")
        if d is None or (isinstance(d, float) and math.isnan(d)):
            return 1e9
        return float(d)

    dedup.sort(key=_dist_key)

    # corta no total
    final_hits = dedup[:top_k_total]

    stats = RagStats(
        ticker=ticker,
        period_ref=period_ref,
        months_back=months_back,
        top_k_total=top_k_total,
        per_topic_k=per_topic_k,
        topics=list(topics),
        total_hits=len(final_hits),
    )

    return final_hits, stats.__dict__
