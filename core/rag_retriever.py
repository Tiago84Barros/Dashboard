from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from core.ai_models.llm_client import factory as llm_factory
from core.db_loader import get_supabase_engine
from core.docs_corporativos_store import fetch_topk_chunks_diversified
from core.rag_multitopic import retrieve_multitopic_chunks


@dataclass
class RetrievalHit:
    doc_id: Optional[int]
    ticker: str
    chunk_text: str
    dist: Optional[float] = None
    data_doc: Optional[str] = None
    tipo_doc: Optional[str] = None
    strategic_theme: Optional[str] = None
    topic: Optional[str] = None
    evidence_score: float = 0.0


def _score_chunk_text(text_value: str, topic: str = "") -> float:
    txt = (text_value or "").lower()
    score = 0.0

    if any(ch.isdigit() for ch in txt):
        score += 1.2

    strong_terms = [
        "capex", "guidance", "dividend", "jcp", "endivid", "dívida", "divida",
        "margem", "lucro", "receita", "aquisi", "fusão", "fusao", "desinvest",
        "recompra", "payout", "conting", "governan", "covenant", "debênt", "debent",
    ]
    if any(term in txt for term in strong_terms):
        score += 1.0

    weak_terms = [
        "a companhia segue", "permanece comprometida", "estratégia de longo prazo",
        "busca continuamente", "visa criar valor", "melhorar eficiência",
    ]
    if any(term in txt for term in weak_terms):
        score -= 1.1

    if topic and topic.lower() in txt:
        score += 0.4

    return score


def _enrich_hits(hits: List[Dict[str, Any]]) -> List[RetrievalHit]:
    doc_ids = sorted({int(h["doc_id"]) for h in hits if h.get("doc_id")})
    meta_map: Dict[int, Dict[str, Any]] = {}
    if doc_ids:
        engine = get_supabase_engine()
        q = text(
            """
            select id, ticker, tipo, data, titulo
            from public.docs_corporativos
            where id = any(:ids)
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(q, {"ids": doc_ids}).mappings().all()
        meta_map = {int(r["id"]): dict(r) for r in rows}

    out: List[RetrievalHit] = []
    for h in hits:
        meta = meta_map.get(int(h.get("doc_id") or 0), {})
        dist = h.get("dist")
        evidence_score = _score_chunk_text(h.get("chunk_text", ""), h.get("topic", ""))
        if dist is not None:
            try:
                evidence_score -= float(dist)
            except Exception:
                pass
        out.append(
            RetrievalHit(
                doc_id=h.get("doc_id"),
                ticker=str(h.get("ticker") or meta.get("ticker") or "").upper(),
                chunk_text=str(h.get("chunk_text") or "").strip(),
                dist=dist,
                data_doc=str(meta.get("data") or "") if meta.get("data") is not None else None,
                tipo_doc=str(meta.get("tipo") or "") if meta.get("tipo") is not None else None,
                strategic_theme=str(h.get("topic") or "") or None,
                topic=str(h.get("topic") or "") or None,
                evidence_score=evidence_score,
            )
        )
    return out


def get_topk_chunks_inteligente(
    ticker: str,
    top_k: int = 24,
    months_window: int = 36,
    debug: bool = False,
) -> List[RetrievalHit]:
    tk = (ticker or "").strip().upper()
    if not tk:
        return []

    try:
        client = llm_factory.get_llm_client()
        engine = get_supabase_engine()
        with engine.connect() as conn:
            raw_hits, _stats = retrieve_multitopic_chunks(
                conn=conn,
                llm_client=client,
                ticker=tk,
                period_ref=None,
                months_back=int(months_window),
                top_k_total=max(int(top_k) * 2, 24),
                per_topic_k=max(6, min(12, int(top_k) // 3 + 2)),
            )
        hits = _enrich_hits(raw_hits)
        if not hits:
            raise RuntimeError("Sem hits vetoriais")

        # diversidade real: até 2 chunks por documento, preservando score.
        hits.sort(key=lambda x: x.evidence_score, reverse=True)
        selected: List[RetrievalHit] = []
        per_doc: Dict[Any, int] = {}
        for hit in hits:
            doc_key = hit.doc_id or hit.chunk_text[:80]
            if per_doc.get(doc_key, 0) >= 2:
                continue
            selected.append(hit)
            per_doc[doc_key] = per_doc.get(doc_key, 0) + 1
            if len(selected) >= int(top_k):
                break
        return selected
    except Exception:
        fallback = fetch_topk_chunks_diversified(tk, k=int(top_k), per_doc_cap=2)
        out: List[RetrievalHit] = []
        for txt in fallback:
            out.append(
                RetrievalHit(
                    doc_id=None,
                    ticker=tk,
                    chunk_text=str(txt).strip(),
                    dist=None,
                    data_doc=None,
                    tipo_doc=None,
                    strategic_theme="fallback",
                    topic="fallback",
                    evidence_score=_score_chunk_text(str(txt)),
                )
            )
        return out


def summarize_retrieval_mix(hits: List[Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "total_hits": len(hits or []),
        "themes": {},
        "tipos": {},
        "years": {},
    }
    for item in hits or []:
        theme = str(getattr(item, "strategic_theme", "") or getattr(item, "topic", "") or "sem_tema")
        out["themes"][theme] = out["themes"].get(theme, 0) + 1

        tipo = str(getattr(item, "tipo_doc", "") or "sem_tipo")
        out["tipos"][tipo] = out["tipos"].get(tipo, 0) + 1

        year = str(getattr(item, "data_doc", "") or "")[:4]
        if year.isdigit():
            out["years"][year] = out["years"].get(year, 0) + 1
    return out
