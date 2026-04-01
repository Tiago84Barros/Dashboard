# core/patch6_validation.py
# Schema validation for patch6_runs.result_json.
#
# Validates field presence, types, and value constraints.
# Returns a ValidationResult used by patch6_scoring to weight
# the structural component of the hybrid score.
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Set

# ────────────────────────────────────────────────────────────────────────────────
# Schema definitions
# ────────────────────────────────────────────────────────────────────────────────

# Each entry: field_name -> (expected_types, constraints)
# constraints keys: valid_values, min_len, min_items

REQUIRED_SCHEMA: Dict[str, Dict[str, Any]] = {
    "perspectiva_compra": {
        "types": (str,),
        "valid_values": {"forte", "moderada", "fraca"},
    },
    "score_qualitativo": {
        "types": (int, float),
        "min_value": 1,
        "max_value": 100,
    },
    "confianca_analise": {
        "types": (int, float),
        "min_value": 0.0,
        "max_value": 1.0,
    },
    "tese_sintese": {
        "types": (str,),
        "min_len": 20,
        "fallbacks": ["tese_final", "resumo", "tese"],
    },
}

RECOMMENDED_SCHEMA: Dict[str, Dict[str, Any]] = {
    "evolucao_estrategica": {"types": (dict,)},
    "execucao_vs_promessa": {"types": (dict,)},
    "consistencia_discurso": {"types": (dict,)},
    "riscos_identificados": {"types": (list,), "min_items": 1},
    "catalisadores": {"types": (list,), "min_items": 1},
    "evidencias": {"types": (list,), "min_items": 2},
    "leitura_direcionalidade": {"types": (str,), "min_len": 10},
}

OPTIONAL_FIELDS: Set[str] = {
    "qualidade_narrativa",
    "strategy_detector",
    "o_que_monitorar",
    "mudancas_estrategicas",
    "pontos_chave",
    "papel_estrategico",
    "sensibilidades_macro",
    "fragilidade_regime_atual",
    "dependencias_cenario",
    "alocacao_sugerida_faixa",
    "racional_alocacao",
    "consideracoes_llm",
    "contradicoes",
    "sinais_de_ruido",
}


# ────────────────────────────────────────────────────────────────────────────────
# Result dataclass
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    is_valid: bool = True                              # all required fields present and valid
    field_coverage: float = 0.0                        # 0-1 fraction of expected fields present
    schema_score: int = 0                              # 0-100 structural quality score
    missing_required: List[str] = field(default_factory=list)
    missing_recommended: List[str] = field(default_factory=list)
    invalid_values: List[str] = field(default_factory=list)  # field: issue description
    warnings: List[str] = field(default_factory=list)
    optional_present: List[str] = field(default_factory=list)


# ────────────────────────────────────────────────────────────────────────────────
# Validation helpers
# ────────────────────────────────────────────────────────────────────────────────

def _resolve_with_fallbacks(obj: Dict[str, Any], key: str, schema: Dict[str, Any]) -> Any:
    """Try primary key, then any fallbacks declared in the schema."""
    value = obj.get(key)
    if value is None or value == "" or value == [] or value == {}:
        for alt in (schema.get("fallbacks") or []):
            value = obj.get(alt)
            if value not in (None, "", [], {}):
                break
    return value


def _check_field(
    key: str,
    value: Any,
    schema: Dict[str, Any],
    missing_list: List[str],
    invalid_list: List[str],
    warnings_list: List[str],
) -> bool:
    """Returns True if the field is present and valid."""
    types = schema.get("types", ())
    valid_values = schema.get("valid_values")
    min_len = schema.get("min_len", 0)
    min_items = schema.get("min_items", 0)
    min_value = schema.get("min_value")
    max_value = schema.get("max_value")

    # Absent
    if value is None or value == "" or value == [] or value == {}:
        missing_list.append(key)
        return False

    # Type check
    if types and not isinstance(value, tuple(types)):
        # Try coercion
        if (int, float) == tuple(types) or (int,) == tuple(types) or (float,) == tuple(types):
            try:
                float(value)
            except (TypeError, ValueError):
                invalid_list.append(f"{key}: tipo inválido ({type(value).__name__})")
                return False
        else:
            invalid_list.append(f"{key}: tipo inválido ({type(value).__name__})")
            return False

    # valid_values
    if valid_values and isinstance(value, str):
        if value.strip().lower() not in valid_values:
            invalid_list.append(f"{key}: valor '{value}' não está em {valid_values}")
            return False

    # min_len (strings)
    if isinstance(value, str) and len(value.strip()) < min_len:
        warnings_list.append(f"{key}: texto curto ({len(value.strip())} chars, mínimo {min_len})")

    # min_items (lists)
    if isinstance(value, list) and len(value) < min_items:
        warnings_list.append(f"{key}: lista com {len(value)} item(s), mínimo {min_items}")

    # numeric range
    if min_value is not None or max_value is not None:
        try:
            fv = float(value)
            if min_value is not None and fv < min_value:
                invalid_list.append(f"{key}: valor {fv} abaixo do mínimo {min_value}")
                return False
            if max_value is not None and fv > max_value:
                invalid_list.append(f"{key}: valor {fv} acima do máximo {max_value}")
                return False
        except (TypeError, ValueError):
            pass

    return True


# ────────────────────────────────────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────────────────────────────────────

def validate_result(result_obj: Dict[str, Any]) -> ValidationResult:
    """
    Validates a parsed result_json dict against the Patch6 schema.
    Returns a ValidationResult with scores and diagnostics.
    """
    vr = ValidationResult()
    if not result_obj:
        vr.is_valid = False
        vr.missing_required = list(REQUIRED_SCHEMA.keys())
        vr.missing_recommended = list(RECOMMENDED_SCHEMA.keys())
        vr.warnings.append("result_json está vazio ou ausente")
        return vr

    # Required fields
    required_ok = 0
    for key, schema in REQUIRED_SCHEMA.items():
        value = _resolve_with_fallbacks(result_obj, key, schema)
        ok = _check_field(key, value, schema, vr.missing_required, vr.invalid_values, vr.warnings)
        if ok:
            required_ok += 1

    vr.is_valid = len(vr.missing_required) == 0 and len(vr.invalid_values) == 0

    # Recommended fields
    rec_ok = 0
    for key, schema in RECOMMENDED_SCHEMA.items():
        value = result_obj.get(key)
        ok = _check_field(key, value, schema, vr.missing_recommended, vr.invalid_values, vr.warnings)
        if ok:
            rec_ok += 1

    # Optional fields presence
    for key in OPTIONAL_FIELDS:
        value = result_obj.get(key)
        if value not in (None, "", [], {}):
            vr.optional_present.append(key)

    # Field coverage: fraction of (required + recommended) present
    total_expected = len(REQUIRED_SCHEMA) + len(RECOMMENDED_SCHEMA)
    total_present = required_ok + rec_ok
    vr.field_coverage = round(total_present / total_expected, 3)

    # Schema score (0-100)
    # Required fields: 50 pts (weighted by count)
    # Recommended fields: 35 pts
    # Optional fields: 15 pts (up to count)
    req_pts = int(50 * required_ok / len(REQUIRED_SCHEMA)) if REQUIRED_SCHEMA else 50
    rec_pts = int(35 * rec_ok / len(RECOMMENDED_SCHEMA)) if RECOMMENDED_SCHEMA else 35
    opt_pts = min(15, len(vr.optional_present))
    vr.schema_score = req_pts + rec_pts + opt_pts

    return vr
