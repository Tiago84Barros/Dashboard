# core/patch6_scoring.py
# Hybrid score engine for Patch6.
#
# Replaces the raw LLM score with a weighted composite:
#   final_score = 0.55 * llm_score + 0.30 * structural_score + 0.15 * evidence_quality
#
# When LLM score is absent, falls back to:
#   final_score = 0.65 * structural_score + 0.35 * evidence_quality
#
# Hysteresis: classification band thresholds include a ±7pt dead zone to
# prevent score oscillation near category boundaries.
#
# New metrics produced:
#   - robustez_qualitativa (0-1)
#   - narrative_dispersion_score (0-1)
#   - execution_trend is received from patch6_temporal, not computed here.
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from core.patch6_validation import ValidationResult

# ────────────────────────────────────────────────────────────────────────────────
# Thresholds and hysteresis
# ────────────────────────────────────────────────────────────────────────────────

_THRESHOLD_FORTE = 70       # score >= 70 → forte
_THRESHOLD_FRACA = 45       # score < 45  → fraca
_HYSTERESIS = 7             # dead zone width at each boundary

# Sections used for narrative dispersion (key → expected min char length)
_NARRATIVE_SECTIONS: Dict[str, int] = {
    "tese_sintese": 150,
    "evolucao_estrategica": 100,
    "execucao_vs_promessa": 100,
    "consistencia_discurso": 100,
    "riscos_identificados": 80,
    "catalisadores": 80,
    "evidencias": 200,
    "qualidade_narrativa": 80,
    "leitura_direcionalidade": 80,
}


# ────────────────────────────────────────────────────────────────────────────────
# Output
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class ScoreResult:
    final_score: int                        # 0-100 hybrid score
    score_source: str                       # "llm" | "heuristic"
    perspectiva_compra: str                 # "forte" | "moderada" | "fraca"
    robustez_qualitativa: float             # 0-1 composite quality metric
    narrative_dispersion_score: float       # 0-1 (high = uneven narrative coverage)


# ────────────────────────────────────────────────────────────────────────────────
# Hysteresis classifier
# ────────────────────────────────────────────────────────────────────────────────

def classify_perspectiva(score: int, prev_perspectiva: str = "") -> str:
    """
    Classify score into forte/moderada/fraca with hysteresis.

    The dead zone prevents rapid oscillation near boundaries:
      - fraca → moderada requires score >= THRESHOLD_FRACA + HYSTERESIS (52)
      - moderada → fraca requires score <  THRESHOLD_FRACA - HYSTERESIS (38)
      - moderada → forte  requires score >= THRESHOLD_FORTE + HYSTERESIS (77)
      - forte → moderada  requires score <  THRESHOLD_FORTE - HYSTERESIS (63)
    """
    p = (prev_perspectiva or "").strip().lower()

    if p == "fraca":
        if score >= _THRESHOLD_FRACA + _HYSTERESIS:
            return "moderada"
        return "fraca"

    if p == "forte":
        if score < _THRESHOLD_FORTE - _HYSTERESIS:
            return "moderada"
        return "forte"

    if p == "moderada":
        if score >= _THRESHOLD_FORTE + _HYSTERESIS:
            return "forte"
        if score < _THRESHOLD_FRACA - _HYSTERESIS:
            return "fraca"
        return "moderada"

    # No prior → standard thresholds
    if score >= _THRESHOLD_FORTE:
        return "forte"
    if score >= _THRESHOLD_FRACA:
        return "moderada"
    return "fraca"


# ────────────────────────────────────────────────────────────────────────────────
# Sub-scores
# ────────────────────────────────────────────────────────────────────────────────

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def compute_structural_score(result_obj: Dict[str, Any], validation: ValidationResult) -> int:
    """
    Structural quality score (0-100) based on:
      - Required field presence: 40 pts
      - Recommended field presence: 30 pts
      - Evidence count: 15 pts (1pt/evidence, max 15)
      - Tese depth: 15 pts (1pt/30 chars, max 15)
    """
    from core.patch6_validation import REQUIRED_SCHEMA, RECOMMENDED_SCHEMA

    n_req = len(REQUIRED_SCHEMA)
    n_req_missing = len(validation.missing_required)
    req_pts = int(40 * (n_req - n_req_missing) / n_req) if n_req > 0 else 40

    n_rec = len(RECOMMENDED_SCHEMA)
    n_rec_missing = len(validation.missing_recommended)
    rec_pts = int(30 * (n_rec - n_rec_missing) / n_rec) if n_rec > 0 else 30

    evidencias = result_obj.get("evidencias") or []
    evid_pts = min(15, len(evidencias) if isinstance(evidencias, list) else 0)

    tese = str(
        result_obj.get("tese_sintese")
        or result_obj.get("tese_final")
        or result_obj.get("tese")
        or ""
    ).strip()
    tese_pts = min(15, len(tese) // 30)

    return min(100, req_pts + rec_pts + evid_pts + tese_pts)


def compute_evidence_quality(evidencias: List[Any]) -> int:
    """
    Evidence quality score (0-100) based on depth of individual evidence items.

    Scoring per item (max 100 pts each, averaged):
      - trecho/citacao present: 25 pts
      - interpretacao/leitura present: 25 pts
      - topico/ano present: 25 pts
      - trecho length bonus: up to 25 pts (1pt/10 chars, max 25)
    """
    if not evidencias:
        return 0

    quality_sum = 0
    sample = evidencias[:15]
    for item in sample:
        if not isinstance(item, dict):
            quality_sum += 10
            continue
        pts = 0
        trecho = str(item.get("trecho") or item.get("citacao") or "")
        interp = str(item.get("interpretacao") or item.get("leitura") or "")
        topico = str(item.get("topico") or item.get("ano") or "")
        if trecho.strip():
            pts += 25
        if interp.strip():
            pts += 25
        if topico.strip():
            pts += 25
        pts += min(25, len(trecho.strip()) // 10)
        quality_sum += pts

    return min(100, quality_sum // max(1, len(sample)))


# ────────────────────────────────────────────────────────────────────────────────
# Narrative dispersion
# ────────────────────────────────────────────────────────────────────────────────

def compute_narrative_dispersion(result_obj: Dict[str, Any]) -> float:
    """
    Measures how unevenly the LLM distributed content across narrative sections.

    Computed as the coefficient of variation (std/mean) of section completeness scores.
    High dispersion → uneven coverage (some sections deep, others empty).
    Low dispersion  → balanced coverage (focused analysis).

    Returns 0-1 (1 = maximally dispersed / uneven).
    """
    completeness = []
    for key, expected_len in _NARRATIVE_SECTIONS.items():
        value = result_obj.get(key)
        if isinstance(value, str):
            length = len(value.strip())
        elif isinstance(value, (list, dict)):
            length = len(str(value))
        else:
            length = 0
        completeness.append(min(1.0, length / max(1, expected_len)))

    if not completeness:
        return 1.0

    mean = sum(completeness) / len(completeness)
    if mean < 0.001:
        return 1.0

    variance = sum((x - mean) ** 2 for x in completeness) / len(completeness)
    std = math.sqrt(variance)
    cv = std / mean   # coefficient of variation — scale-independent dispersion
    return round(min(1.0, cv), 3)


# ────────────────────────────────────────────────────────────────────────────────
# Robustez qualitativa
# ────────────────────────────────────────────────────────────────────────────────

def compute_robustez(
    result_obj: Dict[str, Any],
    validation: ValidationResult,
    coverage_years: List[Any],
) -> float:
    """
    Composite robustness metric (0-1) from four equal-weight components:

      0.25 — evidence count   (normalized against 10 evidences)
      0.25 — schema coverage  (validation.field_coverage)
      0.25 — temporal coverage (normalized against 4 years)
      0.25 — LLM confidence   (confianca_analise, clamped to 0-1)
    """
    evidencias = result_obj.get("evidencias") or []
    evid_score = min(1.0, len(evidencias) / 10.0) * 0.25

    schema_score = validation.field_coverage * 0.25

    years = len(coverage_years) if isinstance(coverage_years, list) else 0
    temporal_score = min(1.0, years / 4.0) * 0.25

    conf = _safe_float(result_obj.get("confianca_analise"), 0.0)
    conf_score = min(1.0, max(0.0, conf)) * 0.25

    return round(evid_score + schema_score + temporal_score + conf_score, 3)


# ────────────────────────────────────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────────────────────────────────────

def compute_hybrid_score(
    result_obj: Dict[str, Any],
    validation: ValidationResult,
    prev_score: Optional[int] = None,
    prev_perspectiva: Optional[str] = None,
) -> ScoreResult:
    """
    Computes the final hybrid score and classification for one company.

    Args:
        result_obj:       Parsed result_json dict.
        validation:       Output of patch6_validation.validate_result().
        prev_score:       Score from the immediately prior period (for hysteresis seed).
        prev_perspectiva: perspectiva_compra from the prior period (for hysteresis).

    Returns:
        ScoreResult with final_score, score_source, perspectiva_compra,
        robustez_qualitativa, narrative_dispersion_score.
    """
    llm_score = _safe_int(result_obj.get("score_qualitativo"), 0)
    structural = compute_structural_score(result_obj, validation)
    evid_quality = compute_evidence_quality(result_obj.get("evidencias") or [])

    if llm_score > 0:
        hybrid = round(llm_score * 0.55 + structural * 0.30 + evid_quality * 0.15)
        source = "llm"
    else:
        # LLM did not return a score — fully heuristic
        hybrid = round(structural * 0.65 + evid_quality * 0.35)
        source = "heuristic"

    hybrid = max(0, min(100, hybrid))

    # Hysteresis: if we have a prior perspectiva use it; otherwise seed from prior score
    prior_perspectiva = prev_perspectiva or ""
    if not prior_perspectiva and prev_score is not None:
        prior_perspectiva = classify_perspectiva(prev_score)

    perspectiva = classify_perspectiva(hybrid, prior_perspectiva)

    # Companion metrics
    sd = result_obj.get("strategy_detector") or {}
    coverage_years = sd.get("coverage_years") if isinstance(sd.get("coverage_years"), list) else []

    dispersion = compute_narrative_dispersion(result_obj)
    robustez = compute_robustez(result_obj, validation, coverage_years)

    return ScoreResult(
        final_score=hybrid,
        score_source=source,
        perspectiva_compra=perspectiva,
        robustez_qualitativa=robustez,
        narrative_dispersion_score=dispersion,
    )
