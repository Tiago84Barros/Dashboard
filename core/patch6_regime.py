# core/patch6_regime.py
# Qualitative regime detection for Patch6.
#
# Infers the strategic "regime" of a company's thesis by scoring keyword
# presence in its result_json text against 8 regime profiles.
# Also detects whether the regime changed vs the previous period.
#
# No Streamlit, no DB access.
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from core.patch6_temporal import TemporalData


# ────────────────────────────────────────────────────────────────────────────────
# Regime profiles
# ────────────────────────────────────────────────────────────────────────────────

# Each profile: keyword list + bonuses for execution_trend and perspectiva
_REGIME_PROFILES: Dict[str, Dict[str, Any]] = {
    "expansão": {
        "keywords": [
            "expansão", "crescimento acelerado", "novo mercado", "internacionalização",
            "aquisição", "lançamento", "capex elevado", "capacidade adicional", "nova planta",
            "ganhar mercado", "expansão orgânica", "crescimento de receita",
        ],
        "trend_bonus": {"melhorando": 12, "estável": 4},
        "perspectiva_bonus": {"forte": 15, "moderada": 5},
    },
    "desalavancagem": {
        "keywords": [
            "desalavancagem", "redução de dívida", "geração de caixa", "deleverage",
            "dívida líquida", "amortização", "queima de dívida", "alavancagem caindo",
            "fluxo livre de caixa", "pagamento de dívida",
        ],
        "trend_bonus": {"melhorando": 8, "estável": 6},
        "perspectiva_bonus": {"moderada": 8, "forte": 4},
    },
    "pressão financeira": {
        "keywords": [
            "pressão de caixa", "queima de caixa", "fluxo negativo", "covenant",
            "alavancagem elevada", "refinanciamento", "dívida crescente", "inadimplência",
            "capital de giro", "compressão de margem", "endividamento",
        ],
        "trend_bonus": {"deteriorando": 15},
        "perspectiva_bonus": {"fraca": 15, "moderada": 3},
    },
    "disciplina de capital": {
        "keywords": [
            "disciplina de capital", "alocação de capital", "retorno sobre capital",
            "dividendos", "buyback", "recompra de ações", "eficiência de capital",
            "roic", "retorno ao acionista", "payout",
        ],
        "trend_bonus": {"estável": 8, "melhorando": 6},
        "perspectiva_bonus": {"forte": 10, "moderada": 6},
    },
    "narrativa promocional": {
        "keywords": [
            "guidance otimista", "perspectiva muito positiva", "crescimento excepcional",
            "transformação", "revolução", "disruptivo", "potencial enorme", "game changer",
        ],
        "trend_bonus": {},
        "perspectiva_bonus": {},
        # Only wins if dispersion is high — checked in scoring logic
        "requires_dispersion": True,
    },
    "execução deteriorando": {
        "keywords": [
            "atraso", "entrega abaixo", "miss de guidance", "revisão para baixo",
            "capacidade ociosa", "execução fraca", "operação prejudicada",
            "piora operacional", "margem pressionada",
        ],
        "trend_bonus": {"deteriorando": 20},
        "perspectiva_bonus": {"fraca": 12, "moderada": 4},
    },
    "reancoragem estratégica": {
        "keywords": [
            "mudança estratégica", "nova direção", "pivô", "reposicionamento",
            "novo ceo", "desinvestimento", "saída de mercado", "foco em core",
            "simplificação", "reestruturação", "mudança de modelo",
        ],
        "trend_bonus": {},
        "perspectiva_bonus": {},
    },
    "defensivo": {
        "keywords": [
            "cautela", "proteção", "preservação de capital", "postura defensiva",
            "redução de exposição", "hedge", "conservador", "menor risco",
            "esperar", "aguardar", "incerteza macro",
        ],
        "trend_bonus": {"deteriorando": 8, "estável": 5},
        "perspectiva_bonus": {"fraca": 12, "moderada": 6},
    },
}

_REGIME_UNKNOWN = "indefinido"


# ────────────────────────────────────────────────────────────────────────────────
# Output
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class RegimeChangeResult:
    current_regime: str = "—"
    previous_regime: str = "—"
    regime_changed: bool = False
    change_intensity: str = "—"        # significativo | moderado | estável | —
    explanation: str = ""
    regime_scores: Dict[str, int] = field(default_factory=dict)  # debug: all scores


# ────────────────────────────────────────────────────────────────────────────────
# Text extraction helpers
# ────────────────────────────────────────────────────────────────────────────────

_TAG_RE = re.compile(r"<[^>]+>")


def _flatten_text(obj: Any, depth: int = 3) -> str:
    """Recursively flatten a dict/list/str into a single lowercase text blob."""
    if depth <= 0:
        return ""
    if obj is None:
        return ""
    if isinstance(obj, str):
        return _TAG_RE.sub(" ", obj).lower()
    if isinstance(obj, list):
        return " ".join(_flatten_text(v, depth - 1) for v in obj)
    if isinstance(obj, dict):
        return " ".join(_flatten_text(v, depth - 1) for v in obj.values())
    return str(obj).lower()


def _score_regime(
    text: str,
    profile: Dict[str, Any],
    execution_trend: str = "—",
    perspectiva: str = "",
    narrative_dispersion: float = 0.0,
) -> int:
    """Score a text blob against one regime profile. Returns 0-100+."""
    score = 0
    for kw in profile.get("keywords", []):
        if kw in text:
            score += 10

    trend_bonus = profile.get("trend_bonus", {})
    score += trend_bonus.get(execution_trend, 0)

    persp_bonus = profile.get("perspectiva_bonus", {})
    score += persp_bonus.get((perspectiva or "").strip().lower(), 0)

    # Narrative promotional regime requires high dispersion as pre-condition
    if profile.get("requires_dispersion") and narrative_dispersion < 0.5:
        score = score // 3

    return score


def _infer_regime_from_json(
    result_obj: Dict[str, Any],
    execution_trend: str = "—",
    perspectiva: str = "",
    narrative_dispersion: float = 0.0,
) -> Tuple[str, Dict[str, int]]:
    """Returns (regime_name, scores_dict)."""
    text = _flatten_text(result_obj)
    scores: Dict[str, int] = {}
    for regime, profile in _REGIME_PROFILES.items():
        scores[regime] = _score_regime(
            text, profile, execution_trend, perspectiva, narrative_dispersion
        )

    best_regime = max(scores, key=lambda r: scores[r])
    if scores[best_regime] < 8:
        return _REGIME_UNKNOWN, scores
    return best_regime, scores


# ────────────────────────────────────────────────────────────────────────────────
# Change intensity
# ────────────────────────────────────────────────────────────────────────────────

_HIGH_CONTRAST_PAIRS = {
    frozenset({"expansão", "pressão financeira"}),
    frozenset({"expansão", "execução deteriorando"}),
    frozenset({"expansão", "defensivo"}),
    frozenset({"desalavancagem", "pressão financeira"}),
    frozenset({"disciplina de capital", "pressão financeira"}),
    frozenset({"reancoragem estratégica", "expansão"}),
    frozenset({"defensivo", "expansão"}),
}


def _change_intensity(prev: str, curr: str) -> str:
    if prev == curr or prev == "—" or curr == "—":
        return "estável"
    if prev == _REGIME_UNKNOWN or curr == _REGIME_UNKNOWN:
        return "moderado"
    pair = frozenset({prev, curr})
    if pair in _HIGH_CONTRAST_PAIRS:
        return "significativo"
    return "moderado"


def _build_explanation(prev: str, curr: str, intensity: str) -> str:
    if intensity == "estável":
        return f"Regime de '{curr}' mantido em relação ao período anterior."
    if intensity == "significativo":
        return (
            f"Mudança significativa de regime detectada: '{prev}' → '{curr}'. "
            f"Indica ruptura relevante na dinâmica qualitativa da empresa."
        )
    return (
        f"Mudança moderada de regime: '{prev}' → '{curr}'. "
        f"Evolução da tese com alteração de ênfase sem ruptura completa."
    )


# ────────────────────────────────────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────────────────────────────────────

def infer_regime(
    current_result_json: Dict[str, Any],
    temporal: Optional[TemporalData] = None,
    execution_trend: str = "—",
    perspectiva: str = "",
    narrative_dispersion: float = 0.0,
) -> RegimeChangeResult:
    """
    Infers the current qualitative regime and compares it with the previous period.

    Args:
        current_result_json:  The current period's parsed result_json.
        temporal:             TemporalData with result_jsons of prior periods.
        execution_trend:      From patch6_temporal (melhorando / estável / deteriorando / —).
        perspectiva:          perspectiva_compra for the current period.
        narrative_dispersion: narrative_dispersion_score from patch6_scoring.

    Returns:
        RegimeChangeResult — safe defaults if data is absent.
    """
    if not current_result_json:
        return RegimeChangeResult()

    current_regime, scores = _infer_regime_from_json(
        current_result_json, execution_trend, perspectiva, narrative_dispersion
    )

    previous_regime = "—"
    if temporal and temporal.result_jsons:
        prev_json = temporal.result_jsons[0]  # most recent prior
        prev_perspectiva = temporal.perspectivas[0] if temporal.perspectivas else ""
        previous_regime, _ = _infer_regime_from_json(
            prev_json,
            execution_trend="—",   # prior trend not tracked separately
            perspectiva=prev_perspectiva,
            narrative_dispersion=0.0,
        )

    intensity = _change_intensity(previous_regime, current_regime)
    changed = intensity != "estável"
    explanation = _build_explanation(previous_regime, current_regime, intensity)

    return RegimeChangeResult(
        current_regime=current_regime,
        previous_regime=previous_regime,
        regime_changed=changed,
        change_intensity=intensity,
        explanation=explanation,
        regime_scores=scores,
    )
