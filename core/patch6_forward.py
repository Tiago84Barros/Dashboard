# core/patch6_forward.py
# Forward-looking signal for Patch6 — estimates future score direction.
#
# Combines backward-looking signals (execution_trend, robustez, score history)
# with current balance indicators (catalyst vs risk count, narrative dispersion)
# to produce a probabilistic forward estimate.
#
# Deliberately conservative: forward is an *indicator*, not a prediction.
# No Streamlit, no DB access.
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ────────────────────────────────────────────────────────────────────────────────
# Output
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class ForwardSignalResult:
    forward_score: int = 0                              # 0-100 estimated prospective quality
    direction: str = "—"                                # melhorando | estável | deteriorando | —
    confidence_forward: float = 0.0                     # 0-1 reliability of this estimate
    key_drivers: List[str] = field(default_factory=list)  # top factors explaining direction


# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

_TREND_SCORE: Dict[str, int] = {
    "melhorando": 80,
    "estável":    55,
    "deteriorando": 20,
    "—": 50,
}


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


# ────────────────────────────────────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────────────────────────────────────

def compute_forward_signal(
    *,
    score: int,
    execution_trend: str = "—",
    robustez: float = 0.0,
    narrative_dispersion: float = 0.0,
    riscos: Optional[List[str]] = None,
    catalisadores: Optional[List[str]] = None,
    confianca: float = 0.0,
    regime: str = "—",
    persistent_risks_count: int = 0,
    persistent_catalysts_count: int = 0,
    temporal_years_count: int = 0,
) -> ForwardSignalResult:
    """
    Estimates a forward-looking quality score (0-100) and direction.

    Formula (weights sum to 1.0):
      forward = 0.30 * current_score
              + 0.28 * trend_score
              + 0.20 * robustez_score
              + 0.22 * catalyst_balance_score
              - dispersion_penalty

    direction:
      forward > score + 5  → melhorando
      forward < score - 5  → deteriorando
      else                 → estável

    Returns safe defaults if inputs are all zero.
    """
    if score <= 0 and robustez == 0.0:
        return ForwardSignalResult()

    risks = riscos or []
    cats = catalisadores or []
    n_risks = len(risks)
    n_cats = len(cats)

    # Trend component
    trend_score = _TREND_SCORE.get(execution_trend, 50)

    # Catalyst-risk balance (0-100)
    total_signals = n_risks + n_cats
    if total_signals > 0:
        balance_score = int((n_cats / total_signals) * 100)
    else:
        balance_score = 50

    # Persistent signals adjustment
    if persistent_catalysts_count > persistent_risks_count:
        balance_score = min(100, balance_score + 8)
    elif persistent_risks_count > persistent_catalysts_count:
        balance_score = max(0, balance_score - 8)

    # Robustez component (0-100)
    robustez_score = int(_safe_float(robustez) * 100)

    # Dispersion penalty (high dispersion = harder to predict) — max 15 pts
    dispersion_penalty = int(_safe_float(narrative_dispersion) * 15)

    # Composite
    forward = round(
        score * 0.30
        + trend_score * 0.28
        + robustez_score * 0.20
        + balance_score * 0.22
        - dispersion_penalty
    )
    forward = max(0, min(100, forward))

    # Direction
    delta = forward - score
    if delta > 5:
        direction = "melhorando"
    elif delta < -5:
        direction = "deteriorando"
    else:
        direction = "estável"

    # Confidence in the forward estimate
    # Higher temporal coverage + robustez = more reliable estimate
    conf = (
        0.50 * _safe_float(robustez)
        + 0.30 * min(1.0, temporal_years_count / 3.0)
        + 0.20 * _safe_float(confianca)
    )
    conf = round(min(1.0, conf), 3)

    # Key drivers (ordered by influence)
    drivers: List[str] = []
    if execution_trend == "melhorando":
        drivers.append("Tendência de execução positiva nos últimos períodos")
    elif execution_trend == "deteriorando":
        drivers.append("Tendência de execução negativa impacta prospecto")

    if n_cats > 0 and balance_score >= 60:
        drivers.append(f"{n_cats} catalisador(es) superam riscos — favorece recuperação")
    elif n_risks > 0 and balance_score <= 40:
        drivers.append(f"{n_risks} risco(s) superam catalisadores — pressiona prospecto")

    if persistent_risks_count >= 3:
        drivers.append(f"{persistent_risks_count} risco(s) persistindo entre períodos")
    if persistent_catalysts_count >= 2:
        drivers.append(f"{persistent_catalysts_count} catalisador(es) recorrente(s) fortalecem tese")

    if robustez >= 0.70:
        drivers.append("Alta robustez analítica sustenta confiança prospectiva")
    elif robustez < 0.35:
        drivers.append("Baixa robustez limita confiabilidade do sinal prospectivo")

    if narrative_dispersion > 0.60:
        drivers.append("Alta dispersão narrativa reduz previsibilidade da tese")

    regime_penalized = {"pressão financeira", "execução deteriorando", "defensivo"}
    regime_boosted = {"expansão", "disciplina de capital", "desalavancagem"}
    if regime in regime_penalized:
        drivers.append(f"Regime '{regime}' associado a pressão prospectiva")
    elif regime in regime_boosted:
        drivers.append(f"Regime '{regime}' associado a trajetória construtiva")

    return ForwardSignalResult(
        forward_score=forward,
        direction=direction,
        confidence_forward=conf,
        key_drivers=drivers[:6],   # cap at 6 for readability
    )
