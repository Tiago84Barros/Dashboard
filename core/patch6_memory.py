# core/patch6_memory.py
# Structured historical memory for Patch6 tickers.
#
# Builds a TickerMemory by comparing signal lists across periods stored in
# TemporalData.result_jsons (populated by patch6_temporal.load_temporal_batch).
#
# "Recurring" = a signal appears in >= 2 periods (current + at least one prior).
# Matching is case-insensitive substring: if the normalized text of a current
# signal appears inside any prior period's signal, it counts as recurring.
#
# No Streamlit, no DB access. Pure function over TemporalData.
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from core.patch6_temporal import TemporalData


# ────────────────────────────────────────────────────────────────────────────────
# Data structures
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class HistoricalSignal:
    """A single signal text with recurrence metadata."""
    text: str
    periods_seen: int = 1           # how many periods this signal appeared in
    is_recurring: bool = False      # True if periods_seen >= 2


@dataclass
class RecurringTheme:
    """A topic with its most frequent recurring signals."""
    topic: str                                              # e.g. "riscos", "catalisadores"
    signals: List[HistoricalSignal] = field(default_factory=list)
    frequency: int = 0                                     # total recurring signal count


@dataclass
class TickerMemory:
    """Consolidated historical memory for one ticker."""
    ticker: str

    # Narrative continuity
    memory_summary: str = ""                                # prose summary of tese evolution
    tese_history: List[str] = field(default_factory=list)  # tese text per period (newest first)

    # Signal recurrence
    recurring_promises: List[str] = field(default_factory=list)    # catalisadores appearing in 2+ periods
    delivered_promises: List[str] = field(default_factory=list)    # entregas_confirmadas recurring
    persistent_risks: List[str] = field(default_factory=list)      # riscos appearing in 2+ periods
    persistent_catalysts: List[str] = field(default_factory=list)  # catalisadores recurring

    # Change signals
    tese_changed: bool = False
    periods_analyzed: int = 0


# ────────────────────────────────────────────────────────────────────────────────
# Text normalization (lightweight — no NLP deps)
# ────────────────────────────────────────────────────────────────────────────────

_STOP_WORDS = {
    "de", "da", "do", "das", "dos", "e", "em", "a", "o", "as", "os",
    "com", "para", "por", "que", "uma", "um", "no", "na", "nos", "nas",
    "ao", "aos", "à", "às", "se", "ou", "mas", "já",
}

_PUNCT_RE = re.compile(r"[^\w\s]")


def _normalize(text: str) -> str:
    """Lowercase, remove punctuation, drop stop words, collapse whitespace."""
    t = (text or "").lower()
    t = _PUNCT_RE.sub(" ", t)
    words = [w for w in t.split() if w not in _STOP_WORDS and len(w) > 2]
    return " ".join(words)


def _is_recurring(text: str, prior_signals_normalized: List[str], min_overlap: int = 3) -> bool:
    """
    Returns True if at least min_overlap tokens from text are found in any
    prior period's signal (after normalization).
    """
    tokens = set(_normalize(text).split())
    if len(tokens) < 2:
        return False
    for prior_norm in prior_signals_normalized:
        prior_tokens = set(prior_norm.split())
        overlap = len(tokens & prior_tokens)
        if overlap >= min_overlap or (overlap >= 2 and len(tokens) <= 4):
            return True
    return False


# ────────────────────────────────────────────────────────────────────────────────
# Field extractors (from result_json dicts)
# ────────────────────────────────────────────────────────────────────────────────

def _extract_list(rj: Dict[str, Any], *keys: str) -> List[str]:
    for key in keys:
        val = rj.get(key)
        if isinstance(val, list):
            return [str(v).strip() for v in val if str(v).strip()]
        if isinstance(val, str) and val.strip():
            return [val.strip()]
    return []


def _extract_tese(rj: Dict[str, Any]) -> str:
    for key in ("tese_sintese", "tese_final", "resumo", "tese"):
        v = rj.get(key)
        if isinstance(v, str) and len(v.strip()) > 15:
            return v.strip()
    return ""


def _extract_delivered(rj: Dict[str, Any]) -> List[str]:
    execucao = rj.get("execucao_vs_promessa") or {}
    if not isinstance(execucao, dict):
        return []
    for key in ("entregas_confirmadas", "entregas_realizadas", "realizacoes"):
        v = execucao.get(key)
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        if isinstance(v, str) and v.strip():
            return [v.strip()]
    return []


# ────────────────────────────────────────────────────────────────────────────────
# Recurring signal finder
# ────────────────────────────────────────────────────────────────────────────────

def _find_recurring(
    current_signals: List[str],
    prior_all_signals: List[List[str]],
) -> List[str]:
    """
    Returns signals from current_signals that appear (approx.) in at least one
    prior-period signal list.
    """
    if not current_signals or not prior_all_signals:
        return []

    # Flatten all prior-period signals into normalized form
    prior_normalized: List[str] = []
    for period_sigs in prior_all_signals:
        for s in period_sigs:
            norm = _normalize(s)
            if norm:
                prior_normalized.append(norm)

    return [s for s in current_signals if _is_recurring(s, prior_normalized)]


# ────────────────────────────────────────────────────────────────────────────────
# Memory summary
# ────────────────────────────────────────────────────────────────────────────────

def _build_memory_summary(
    ticker: str,
    tese_history: List[str],
    narrative_shift: str,
    periods: List[str],
) -> str:
    n = len(tese_history)
    if n == 0:
        return "Sem histórico de tese disponível para comparação."

    span = f"{periods[-1]}–{periods[0]}" if len(periods) >= 2 else (periods[0] if periods else "—")

    if narrative_shift == "estável":
        return (
            f"A tese de {ticker} permaneceu estável ao longo dos {n} período(s) analisado(s) ({span}). "
            f"O discurso manteve consistência com a narrativa anterior."
        )
    if narrative_shift == "significativo":
        return (
            f"Mudança significativa de tese detectada para {ticker} no período mais recente. "
            f"A perspectiva de compra ou o posicionamento estratégico se alterou em relação ao ciclo anterior ({span})."
        )
    if narrative_shift == "moderado":
        return (
            f"Mudança moderada identificada na tese de {ticker} ao longo dos {n} período(s) ({span}). "
            f"A narrativa evoluiu, mas sem ruptura completa com o posicionamento anterior."
        )
    return (
        f"Histórico de tese disponível para {n} período(s) ({span}). "
        f"Insuficiente para avaliar continuidade narrativa."
    )


# ────────────────────────────────────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────────────────────────────────────

def build_ticker_memory(
    ticker: str,
    temporal: Optional[TemporalData],
    current_result_json: Optional[Dict[str, Any]] = None,
) -> TickerMemory:
    """
    Builds structured memory for one ticker from its historical TemporalData.

    Args:
        ticker:              Ticker symbol.
        temporal:            TemporalData with result_jsons list (populated by load_temporal_batch).
        current_result_json: The current period's result_json (most recent, not yet in temporal).

    Returns:
        TickerMemory with recurring signals, tese history, and a prose summary.
        Returns a minimal TickerMemory if no data is available (safe degradation).
    """
    mem = TickerMemory(ticker=ticker)

    if temporal is None or not temporal.result_jsons:
        return mem

    # result_jsons[0] = most recent prior period, result_jsons[-1] = oldest
    all_jsons = temporal.result_jsons

    # Include current period's JSON as the most recent for signal extraction
    if current_result_json:
        all_jsons = [current_result_json] + list(all_jsons)

    mem.periods_analyzed = len(temporal.periods)

    # Collect tese history (newest first)
    mem.tese_history = [_extract_tese(rj) for rj in all_jsons if _extract_tese(rj)]

    # Extract signals per period for each category
    risks_per_period = [_extract_list(rj, "riscos_identificados", "riscos") for rj in all_jsons]
    cats_per_period = [_extract_list(rj, "catalisadores", "gatilhos_futuros") for rj in all_jsons]
    promises_per_period = [_extract_list(rj, "catalisadores", "o_que_monitorar") for rj in all_jsons]
    delivered_per_period = [_extract_delivered(rj) for rj in all_jsons]

    # Recurring = appears in current period AND in at least one prior period
    if len(all_jsons) >= 2:
        current_risks = risks_per_period[0]
        current_cats = cats_per_period[0]
        current_promises = promises_per_period[0]
        current_delivered = delivered_per_period[0]

        prior_risks = risks_per_period[1:]
        prior_cats = cats_per_period[1:]
        prior_promises = promises_per_period[1:]
        prior_delivered = delivered_per_period[1:]

        mem.persistent_risks = _find_recurring(current_risks, prior_risks)
        mem.persistent_catalysts = _find_recurring(current_cats, prior_cats)
        mem.recurring_promises = _find_recurring(current_promises, prior_promises)
        mem.delivered_promises = _find_recurring(current_delivered, prior_delivered)

    # Tese changed?
    persp = [p for p in temporal.perspectivas if p.strip()]
    mem.tese_changed = len(set(persp)) > 1 if len(persp) >= 2 else False

    # Memory summary
    mem.memory_summary = _build_memory_summary(
        ticker=ticker,
        tese_history=mem.tese_history,
        narrative_shift=temporal.narrative_shift,
        periods=temporal.periods,
    )

    return mem
