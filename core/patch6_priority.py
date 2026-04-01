# core/patch6_priority.py
# Attention-based priority ranking for Patch6 portfolio monitoring.
#
# Calculates an attention_score (0-100) per ticker based on deterioration
# and uncertainty signals. Higher score = needs review sooner.
#
# Used by patch6_analysis to populate CompanyAnalysis v3 fields and
# by build_portfolio_analysis to rank tickers and compute portfolio-level alerts.
#
# No Streamlit, no DB access.
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


# ────────────────────────────────────────────────────────────────────────────────
# Output
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class PriorityResult:
    attention_score: float = 0.0            # 0-100 composite signal
    attention_level: str = "baixa"          # alta | média | baixa
    drivers: List[str] = field(default_factory=list)  # signals that raised the score
    recommended_action: str = "manter monitoramento regular"


# ────────────────────────────────────────────────────────────────────────────────
# Scoring table
# ────────────────────────────────────────────────────────────────────────────────

# Each entry: (contribution_points, driver_label)
# Caller fills a subset based on signal presence.

_SIGNAL_WEIGHTS = {
    "execution_trend_deteriorando": (25, "Tendência de execução deteriorando"),
    "narrative_shift_significativo": (20, "Mudança significativa de narrativa"),
    "forward_deteriorando": (20, "Sinal prospectivo negativo"),
    "regime_mudou_significativo": (15, "Mudança significativa de regime qualitativo"),
    "robustez_baixa": (15, "Robustez analítica insuficiente (< 0.40)"),
    "regime_mudou_moderado": (8, "Mudança moderada de regime qualitativo"),
    "dispersao_alta": (10, "Alta dispersão narrativa (> 0.60)"),
    "riscos_persistentes": (10, "Múltiplos riscos persistindo entre períodos"),
    "schema_baixo": (5, "Baixa cobertura de schema de resultado"),
    "perspectiva_fraca": (8, "Perspectiva de compra fraca"),
    "confianca_baixa": (5, "Baixa confiança na análise"),
}


# ────────────────────────────────────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────────────────────────────────────

def compute_priority(
    *,
    execution_trend: str = "—",
    narrative_shift: str = "—",
    robustez: float = 0.0,
    narrative_dispersion: float = 0.0,
    regime_change_intensity: str = "—",
    forward_direction: str = "—",
    persistent_risks_count: int = 0,
    schema_score: int = 0,
    perspectiva: str = "",
    confianca: float = 0.0,
) -> PriorityResult:
    """
    Computes attention score for one ticker.

    All arguments have safe defaults — partial data never raises exceptions.
    Returns PriorityResult with score, level, drivers, and recommended action.
    """
    score = 0.0
    drivers: List[str] = []

    def add(key: str, condition: bool) -> None:
        nonlocal score
        if condition:
            pts, label = _SIGNAL_WEIGHTS[key]
            score += pts
            drivers.append(label)

    add("execution_trend_deteriorando", execution_trend == "deteriorando")
    add("narrative_shift_significativo", narrative_shift == "significativo")
    add("forward_deteriorando", forward_direction == "deteriorando")
    add("regime_mudou_significativo", regime_change_intensity == "significativo")
    add("regime_mudou_moderado", regime_change_intensity == "moderado")
    add("robustez_baixa", robustez < 0.40)
    add("dispersao_alta", narrative_dispersion > 0.60)
    add("riscos_persistentes", persistent_risks_count > 2)
    add("schema_baixo", 0 < schema_score < 50)
    add("perspectiva_fraca", (perspectiva or "").strip().lower() == "fraca")
    add("confianca_baixa", 0 < confianca < 0.45)

    score = min(100.0, score)

    # Classify
    if score >= 60:
        level = "alta"
        action = "revisar imediatamente"
    elif score >= 30:
        level = "média"
        action = "acompanhar no próximo ciclo"
    else:
        level = "baixa"
        action = "manter monitoramento regular"

    return PriorityResult(
        attention_score=round(score, 1),
        attention_level=level,
        drivers=drivers,
        recommended_action=action,
    )
