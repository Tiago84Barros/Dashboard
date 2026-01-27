from __future__ import annotations

from dataclasses import asdict
from typing import Dict, List, Optional

from ..config import AIConfig
from ..llm_client.factory import get_llm_client
from ..prompts import SYSTEM_GUARDRAILS, NEWS_ANALYSIS_PROMPT, NEWS_SIGNAL_SCHEMA_HINT
from ..signals.news_signal import NewsSignal
from ..signals.validators import coerce_news_signal
from ..governance.rules import apply_governance_caps
from .cache import TTLCache

from core.news.schema import NewsItem
from core.news.store import NewsStore


def _news_to_context(items: List[NewsItem]) -> List[Dict]:
    ctx: List[Dict] = []
    for it in items:
        ctx.append(
            {
                "source": it.source,
                "title": it.title,
                "url": it.url,
                "published_at": it.published_at.isoformat(),
                "text": (it.text or "")[:1500],
            }
        )
    return ctx


def build_news_signals(
    *,
    tickers: List[str],
    store: NewsStore,
    window_days: int = 90,
    top_k: int = 20,
    config: Optional[AIConfig] = None,
    cache: Optional[TTLCache] = None,
) -> Dict[str, NewsSignal]:
    """Gera NewsSignal por ticker usando LLM.

    - Desacoplado do provider (OpenAI/Local)
    - Saída validada e com governança (caps)
    """

    cfg = config or AIConfig()
    llm = get_llm_client(cfg)
    _cache = cache or TTLCache(ttl_seconds=3600)

    out: Dict[str, NewsSignal] = {}
    for tk in tickers:
        cache_key = f"news_signal::{tk}::{window_days}::{top_k}::{cfg.provider}::{cfg.model}"
        cached = _cache.get(cache_key)
        if isinstance(cached, NewsSignal):
            out[tk] = cached
            continue

        items = store.fetch_by_ticker(tk, days=window_days, limit=top_k)
        context = _news_to_context(items)

        # Se não há notícias, retorne neutro (sem chamar LLM)
        if not context:
            sig = NewsSignal(ticker=tk, sentiment=0.0, confidence=0.0)
            out[tk] = sig
            _cache.set(cache_key, sig)
            continue

        payload = llm.generate_json(
            system=SYSTEM_GUARDRAILS,
            user=f"Ticker: {tk}\n\n{NEWS_ANALYSIS_PROMPT}",
            schema_hint=NEWS_SIGNAL_SCHEMA_HINT,
            context=context,
        )

        sig = coerce_news_signal(payload, fallback_ticker=tk)
        sig = apply_governance_caps(sig)
        out[tk] = sig
        _cache.set(cache_key, sig)

    return out
