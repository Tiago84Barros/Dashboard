# core/patch6_rag.py
# Topic-aware evidence selection for Patch6 RAG context.
#
# Problem with recency-only selection:
#   The most recent document might repeat the same topic multiple times,
#   while other important topics (risks, governance, strategy) go unrepresented.
#
# Solution: topic budget
#   Assign a max number of documents per topic. Fill topic slots greedily,
#   then fill remaining budget with unclassified documents by recency.
#
# Also used to classify and score the LLM-returned evidencias list.
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from core.db import get_engine
from sqlalchemy import text


# ────────────────────────────────────────────────────────────────────────────────
# Topic taxonomy
# ────────────────────────────────────────────────────────────────────────────────

# Default budget per topic (max documents selected from docs_corporativos)
DEFAULT_TOPIC_BUDGET: Dict[str, int] = {
    "resultados_financeiros": 3,
    "estrategia_negocio": 2,
    "execucao_operacional": 2,
    "riscos_e_macro": 2,
    "governance_e_esg": 1,
}

TOPIC_KEYWORDS: Dict[str, List[str]] = {
    "resultados_financeiros": [
        "receita", "ebitda", "lucro", "margem", "resultado", "crescimento",
        "guidance", "geração de caixa", "fluxo de caixa", "dívida líquida",
        "alavancagem", "dividend", "distribuição", "proventos",
    ],
    "estrategia_negocio": [
        "expansão", "aquisição", "investimento", "capex", "estratégia",
        "posicionamento", "mercado", "competição", "diferencial", "inovação",
        "lançamento", "parceria", "joint venture", "fusão",
    ],
    "execucao_operacional": [
        "execução", "entrega", "projeto", "operação", "eficiência",
        "produtividade", "capacidade", "planta", "volume", "throughput",
        "ramp-up", "concluímos", "inauguramos",
    ],
    "riscos_e_macro": [
        "risco", "inflação", "juros", "câmbio", "regulatório", "incerteza",
        "pressão", "desaceleração", "macro", "cenário", "headwind",
        "commodities", "energia", "inadimplência", "volatilidade",
    ],
    "governance_e_esg": [
        "governança", "dividendos", "esg", "ambiental", "social",
        "conselho", "sustentabilidade", "compensação", "transparência",
        "auditoria", "gestão", "compliance",
    ],
}

# Topic not recognized
_UNKNOWN_TOPIC = "outros"


# ────────────────────────────────────────────────────────────────────────────────
# Result dataclass
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class RagContext:
    """Evidence context ready to send to the LLM."""
    ticker: str
    selected_docs: List[Dict[str, Any]] = field(default_factory=list)
    topic_distribution: Dict[str, int] = field(default_factory=dict)   # topic → count
    total_selected: int = 0
    total_available: int = 0
    budget_used: Dict[str, int] = field(default_factory=dict)          # topic → slots used

    def as_text(self, max_chars_per_doc: int = 3000) -> str:
        """Formats selected documents as plain text for prompt injection."""
        lines = []
        for doc in self.selected_docs:
            topic = doc.get("_topic", _UNKNOWN_TOPIC)
            date = str(doc.get("date") or doc.get("data") or "—")
            fonte = str(doc.get("fonte") or doc.get("source") or "—")
            text = str(doc.get("raw_text") or doc.get("text") or "")[:max_chars_per_doc]
            lines.append(f"[{topic.upper()} | {fonte} | {date}]\n{text}")
        return "\n\n---\n\n".join(lines)


# ────────────────────────────────────────────────────────────────────────────────
# Topic classifier
# ────────────────────────────────────────────────────────────────────────────────

def classify_topic(text: str) -> str:
    """
    Classify a document or evidence item into one of the predefined topics.
    Returns the topic with the most keyword hits. Ties → first matching topic.
    Returns _UNKNOWN_TOPIC if no keywords match.
    """
    if not text:
        return _UNKNOWN_TOPIC

    text_lower = text.lower()
    best_topic = _UNKNOWN_TOPIC
    best_count = 0

    for topic, keywords in TOPIC_KEYWORDS.items():
        count = sum(1 for kw in keywords if kw in text_lower)
        if count > best_count:
            best_count = count
            best_topic = topic

    return best_topic if best_count > 0 else _UNKNOWN_TOPIC


# ────────────────────────────────────────────────────────────────────────────────
# Evidence enrichment (for post-LLM evidencias from result_json)
# ────────────────────────────────────────────────────────────────────────────────

def enrich_evidencias_with_topics(evidencias: List[Any]) -> List[Dict[str, Any]]:
    """
    Tags each evidence item from result_json with a topic label.
    Returns a new list — does not mutate the input.
    """
    enriched = []
    for item in (evidencias or []):
        if not isinstance(item, dict):
            enriched.append({"_topic": _UNKNOWN_TOPIC, "raw": item})
            continue
        text = " ".join(
            str(v) for v in (
                item.get("trecho"),
                item.get("citacao"),
                item.get("interpretacao"),
                item.get("leitura"),
                item.get("topico"),
            )
            if v
        )
        enriched.append({**item, "_topic": classify_topic(text)})
    return enriched


# ────────────────────────────────────────────────────────────────────────────────
# DB-backed evidence selection
# ────────────────────────────────────────────────────────────────────────────────

def load_docs_for_rag(
    ticker: str,
    *,
    days_back: int = 730,
    limit_fetch: int = 60,
) -> List[Dict[str, Any]]:
    """
    Loads raw documents from public.docs_corporativos for a ticker.
    Returns a list of dicts with date, fonte, tipo, titulo, raw_text.
    """
    from core.ticker_utils import normalize_ticker
    tk = normalize_ticker(ticker)
    if not tk:
        return []

    engine = get_engine()
    q = text(
        """
        SELECT
            ticker, data, fonte, tipo, titulo, url,
            LEFT(raw_text, 8000) AS raw_text
        FROM public.docs_corporativos
        WHERE ticker = :tk
          AND (data IS NULL OR data >= (CURRENT_DATE - (:days_back::int)))
          AND coalesce(trim(raw_text), '') != ''
        ORDER BY COALESCE(data, DATE(created_at)) DESC, id DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        import pandas as pd
        df = pd.read_sql_query(q, conn, params={"tk": tk, "days_back": days_back, "limit": limit_fetch})

    if df is None or df.empty:
        return []

    return df.to_dict("records")


def build_rag_context(
    ticker: str,
    docs: Optional[List[Dict[str, Any]]] = None,
    budget: Optional[Dict[str, int]] = None,
    max_total: int = 10,
    days_back: int = 730,
) -> RagContext:
    """
    Selects evidence from docs_corporativos using a topic budget.

    Args:
        ticker:    Ticker to retrieve docs for.
        docs:      Pre-fetched docs list (skips DB call if provided).
        budget:    Per-topic doc count limits (defaults to DEFAULT_TOPIC_BUDGET).
        max_total: Hard cap on total documents returned.
        days_back: How many days back to search (used only if docs is None).

    Returns:
        RagContext with topic-distributed evidence.
    """
    if docs is None:
        docs = load_docs_for_rag(ticker, days_back=days_back, limit_fetch=max_total * 6)

    if not docs:
        return RagContext(ticker=ticker)

    topic_budget = dict(budget or DEFAULT_TOPIC_BUDGET)
    ctx = RagContext(ticker=ticker, total_available=len(docs))

    # Classify all docs
    classified: List[Tuple[str, Dict[str, Any]]] = []
    for doc in docs:
        text = str(doc.get("raw_text") or "")
        topic = classify_topic(text)
        classified.append((topic, doc))

    # Fill topic slots
    topic_used: Dict[str, int] = {t: 0 for t in topic_budget}
    selected: List[Dict[str, Any]] = []
    used_indices: set = set()

    # First pass: fill each topic up to its budget
    for idx, (topic, doc) in enumerate(classified):
        if len(selected) >= max_total:
            break
        if topic in topic_budget and topic_used.get(topic, 0) < topic_budget[topic]:
            selected.append({**doc, "_topic": topic})
            topic_used[topic] = topic_used.get(topic, 0) + 1
            used_indices.add(idx)

    # Second pass: fill remaining slots with unclassified (outros) or overflow
    for idx, (topic, doc) in enumerate(classified):
        if len(selected) >= max_total:
            break
        if idx not in used_indices:
            selected.append({**doc, "_topic": topic})
            used_indices.add(idx)

    # Build distribution
    distribution: Dict[str, int] = {}
    for doc in selected:
        t = doc.get("_topic", _UNKNOWN_TOPIC)
        distribution[t] = distribution.get(t, 0) + 1

    ctx.selected_docs = selected
    ctx.topic_distribution = distribution
    ctx.total_selected = len(selected)
    ctx.budget_used = {t: v for t, v in topic_used.items() if v > 0}
    return ctx
