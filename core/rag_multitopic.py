from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import math
from sqlalchemy import text


DEFAULT_TOPICS = [
    "resultados e guidance",
    "capex e investimentos",
    "endividamento e custo financeiro",
    "dividendos e payout",
    "riscos e contingências",
    "governança e eventos corporativos",
    "eficiência operacional e margens",
    "alocação de capital e retorno ao acionista",
    "reestruturações, M&A e desinvestimentos",
    "execução operacional e metas estratégicas",
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
    vals: List[str] = []
    for x in emb:
        try:
            vals.append(f"{float(x):.10f}")
        except Exception:
            vals.append("0.0")
    return "[" + ",".join(vals) + "]"


def embed_text(llm_client: Any, text_value: str) -> List[float]:
    emb = llm_client.embed(text_value)
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
    where_period = ""
    if period_ref:
        where_period = " AND COALESCE(d.period_ref,'') = %(period_ref)s "

    where_months = ""
    if months_back and months_back > 0:
        where_months = """
        AND (
            COALESCE(d.data::text,'') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            AND to_date(substr(d.data::text,1,10),'YYYY-MM-DD') >= (CURRENT_DATE - (%(months_back)s || ' months')::interval)
        )
        """

    sql = f"""
        SELECT
            c.doc_id,
            c.ticker,
            c.chunk_text,
            COALESCE(d.data::text, '') AS data_doc,
            COALESCE(d.tipo, '') AS tipo_doc,
            (c.embedding <-> (%(emb)s)::vector) AS dist
        FROM public.docs_corporativos_chunks c
        JOIN public.docs_corporativos d
          ON d.id = c.doc_id
        WHERE c.ticker = %(ticker)s
          AND COALESCE(d.data::text,'') <> ''
          {where_period}
          {where_months}
        ORDER BY (c.embedding <-> (%(emb)s)::vector) ASC,
                 COALESCE(d.data::text,'') DESC,
                 c.doc_id DESC,
                 md5(COALESCE(c.chunk_text,'')) ASC
        LIMIT %(lim)s
    """

    params = {"emb": emb_vec_literal, "ticker": ticker, "lim": int(lim)}
    if period_ref:
        params["period_ref"] = period_ref
    if months_back and months_back > 0:
        params["months_back"] = int(months_back)

    rows = conn.execute(text(sql), params).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        dist = r.dist if hasattr(r, "dist") else r["dist"]
        try:
            dist_f = float(dist) if dist is not None else None
        except Exception:
            dist_f = None
        out.append(
            {
                "doc_id": r.doc_id if hasattr(r, "doc_id") else r["doc_id"],
                "ticker": r.ticker if hasattr(r, "ticker") else r["ticker"],
                "chunk_text": r.chunk_text if hasattr(r, "chunk_text") else r["chunk_text"],
                "data_doc": r.data_doc if hasattr(r, "data_doc") else r["data_doc"],
                "tipo_doc": r.tipo_doc if hasattr(r, "tipo_doc") else r["tipo_doc"],
                "dist": dist_f,
            }
        )
    return out


def retrieve_multitopic_chunks(
    conn: Any,
    llm_client: Any,
    ticker: str,
    period_ref: Optional[str] = None,
    months_back: Optional[int] = 36,
    top_k_total: int = 96,
    per_topic_k: int = 20,
    topics: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    topics = topics or DEFAULT_TOPICS
    top_k_total = int(max(24, top_k_total))
    per_topic_k = int(max(12, per_topic_k))

    hits_all: List[Dict[str, Any]] = []
    for t in topics:
        query = (
            f"{ticker}. {t}. "
            f"Foque em fatos materiais, números, guidance, dívida, capex, dividendos, retorno sobre capital, "
            f"governança, reestruturações, M&A, riscos, mudanças concretas de estratégia e execução versus promessa."
        )
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

    seen = set()
    dedup: List[Dict[str, Any]] = []
    for h in hits_all:
        key = (h.get("doc_id"), h.get("chunk_text"))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(h)

    def _dist_key(x: Dict[str, Any]):
        d = x.get("dist")
        if d is None or (isinstance(d, float) and math.isnan(d)):
            return (1e9, "", 0, "")
        return (
            float(d),
            str(x.get("data_doc") or ""),
            -int(x.get("doc_id") or 0),
            str(x.get("chunk_text") or "")[:32],
        )

    dedup.sort(key=_dist_key)
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
