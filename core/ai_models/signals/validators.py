from __future__ import annotations

from typing import Any, Dict, List

from .news_signal import NewsSignal


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def coerce_news_signal(data: Dict[str, Any], fallback_ticker: str) -> NewsSignal:
    """Converte JSON do LLM em NewsSignal, de forma defensiva."""

    ticker = str(data.get("ticker") or fallback_ticker)
    sentiment = float(data.get("sentiment") or 0.0)
    confidence = float(data.get("confidence") or 0.0)

    risks = data.get("risks") or []
    catalysts = data.get("catalysts") or []
    event_flags = data.get("event_flags") or []
    justification = data.get("justification") or []

    def _as_str_list(v: Any) -> List[str]:
        if isinstance(v, list):
            return [str(x)[:200] for x in v if str(x).strip()]
        if isinstance(v, str):
            return [v[:200]] if v.strip() else []
        return []

    return NewsSignal(
        ticker=ticker,
        sentiment=_clamp(sentiment, -1.0, 1.0),
        confidence=_clamp(confidence, 0.0, 1.0),
        risks=_as_str_list(risks)[:8],
        catalysts=_as_str_list(catalysts)[:8],
        event_flags=_as_str_list(event_flags)[:12],
        justification=_as_str_list(justification)[:6],
    )
