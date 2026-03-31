# core/patch6_temporal.py
# Multi-period temporal analysis for Patch6.
#
# Loads historical patch6_runs for a batch of tickers and computes:
#   - execution_trend:  score trajectory across periods
#   - narrative_shift:  perspectiva_compra stability across periods
#
# Uses core.db.get_engine() directly — no Streamlit dependency.
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import text

from core.db import get_engine


# ────────────────────────────────────────────────────────────────────────────────
# Result dataclass
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class TemporalData:
    """Historical analysis data for one ticker across multiple periods."""
    ticker: str
    periods: List[str] = field(default_factory=list)                # ordered newest → oldest
    scores: List[int] = field(default_factory=list)                 # score per period
    perspectivas: List[str] = field(default_factory=list)           # perspectiva per period
    result_jsons: List[Dict[str, Any]] = field(default_factory=list)  # parsed result_json per period
    execution_trend: str = "—"                                      # melhorando | estável | deteriorando | —
    narrative_shift: str = "—"                                      # significativo | moderado | estável | —
    prev_score: Optional[int] = None                                # immediately prior period score
    prev_perspectiva: Optional[str] = None                          # immediately prior perspectiva

    @property
    def has_history(self) -> bool:
        return len(self.periods) >= 2


_EMPTY = TemporalData(ticker="")


# ────────────────────────────────────────────────────────────────────────────────
# DB access
# ────────────────────────────────────────────────────────────────────────────────

def load_temporal_batch(
    tickers: List[str],
    current_period_ref: str,
    n_periods: int = 4,
) -> Dict[str, TemporalData]:
    """
    Loads the last n_periods distinct period_refs per ticker (excluding current).
    Returns a dict keyed by ticker with TemporalData objects.

    Single SQL query for all tickers — avoids N+1 pattern.
    """
    tickers = [str(t).strip().upper() for t in (tickers or []) if str(t).strip()]
    if not tickers:
        return {}

    engine = get_engine()
    q = text(
        """
        WITH all_runs AS (
            SELECT
                ticker,
                period_ref,
                created_at,
                perspectiva_compra,
                result_json,
                ROW_NUMBER() OVER (
                    PARTITION BY ticker, period_ref
                    ORDER BY created_at DESC
                ) AS rn
            FROM public.patch6_runs
            WHERE ticker = ANY(:tks)
              AND period_ref != :current_pr
        ),
        latest_per_period AS (
            SELECT ticker, period_ref, created_at, perspectiva_compra, result_json
            FROM all_runs
            WHERE rn = 1
        ),
        ranked_periods AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY ticker
                       ORDER BY period_ref DESC
                   ) AS pr_rank
            FROM latest_per_period
        )
        SELECT ticker, period_ref, perspectiva_compra, result_json
        FROM ranked_periods
        WHERE pr_rank <= :n_periods
        ORDER BY ticker ASC, period_ref DESC
        """
    )

    with engine.connect() as conn:
        df = pd.read_sql_query(
            q, conn,
            params={
                "tks": tickers,
                "current_pr": str(current_period_ref).strip(),
                "n_periods": int(n_periods),
            },
        )

    result: Dict[str, TemporalData] = {}
    for tk in tickers:
        result[tk] = TemporalData(ticker=tk)

    if df is None or df.empty:
        return result

    for ticker, grp in df.groupby("ticker"):
        td = TemporalData(ticker=str(ticker))
        for _, row in grp.iterrows():
            td.periods.append(str(row.get("period_ref", "")))
            td.perspectivas.append(str(row.get("perspectiva_compra", "") or "").strip().lower())
            rj = _parse_result_json(row.get("result_json"))
            score = _safe_int(rj.get("score_qualitativo"), 0)
            td.scores.append(score)
            td.result_jsons.append(rj)  # store parsed JSON for memory/regime modules

        if td.periods:
            td.prev_score = td.scores[0] if td.scores else None
            td.prev_perspectiva = td.perspectivas[0] if td.perspectivas else None
            td.execution_trend = compute_execution_trend(td.scores)
            td.narrative_shift = compute_narrative_shift(td.perspectivas)

        result[str(ticker)] = td

    return result


# ────────────────────────────────────────────────────────────────────────────────
# Trend computation
# ────────────────────────────────────────────────────────────────────────────────

def compute_execution_trend(scores: List[int]) -> str:
    """
    Classify score trajectory as melhorando / estável / deteriorando.

    Uses the last 3 periods (scores[0] = newest, scores[-1] = oldest).
    Requires at least 2 valid (> 0) scores.

    Threshold: average slope > +5 → melhorando; < -5 → deteriorando.
    """
    valid = [s for s in (scores or []) if s > 0]
    if len(valid) < 2:
        return "—"

    sample = valid[:3]  # newest first
    # Compute slope as mean difference (newest - oldest) / steps
    n = len(sample)
    slope = (sample[0] - sample[-1]) / max(1, n - 1)

    if slope > 5:
        return "melhorando"
    if slope < -5:
        return "deteriorando"
    return "estável"


def compute_narrative_shift(perspectivas: List[str]) -> str:
    """
    Classify narrative stability based on perspectiva_compra history.

    perspectivas[0] = most recent prior period (just before current).

    - Changed in the most recent period          → "significativo"
    - Changed but not in the most recent period  → "moderado"
    - No change across all periods               → "estável"
    - Insufficient data                          → "—"
    """
    clean = [p.strip().lower() for p in (perspectivas or []) if p.strip()]
    if len(clean) < 2:
        return "—"

    if len(set(clean)) == 1:
        return "estável"

    # Check if the most recent transition happened (i.e. clean[0] ≠ clean[1])
    if clean[0] != clean[1]:
        return "significativo"

    return "moderado"


# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _parse_result_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        if isinstance(value, str) and value.strip():
            return json.loads(value)
    except Exception:
        pass
    return {}


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default
