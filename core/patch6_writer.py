# core/patch6_writer.py
# PATCH6 WRITER ENRIQUECIDO (COM PATCH7 + SCORE HEURÍSTICO + COBERTURA TEMPORAL)

from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, Any, List

try:
    from core.patch7_strategy_detector import enrich_patch6_result
except Exception:
    enrich_patch6_result = None


_YEAR_RE = re.compile(r"\b(20\d{2})\b")


def _as_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _as_list(v: Any) -> List[Any]:
    if isinstance(v, list):
        return v
    if isinstance(v, str) and v.strip():
        return [v.strip()]
    return []


def _count_nonempty(items: List[Any]) -> int:
    total = 0
    for item in items:
        if isinstance(item, dict):
            if any(_as_str(x) for x in item.values()):
                total += 1
        elif _as_str(item):
            total += 1
    return total


def _pick_list(obj: Dict[str, Any], *keys: str) -> List[Any]:
    for key in keys:
        value = obj.get(key)
        if isinstance(value, list) and value:
            return value
        if isinstance(value, str) and value.strip():
            return [value.strip()]
    return []


def _pick_dict(obj: Dict[str, Any], *keys: str) -> Dict[str, Any]:
    for key in keys:
        value = obj.get(key)
        if isinstance(value, dict) and value:
            return value
    return {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _score_base(leitura: Any) -> int:
    leitura = _as_str(leitura).lower()
    if leitura in {"positiva", "construtiva", "bullish"}:
        return 72
    if leitura in {"moderada", "equilibrada", "neutra", "neutral"}:
        return 55
    if leitura in {"negativa", "cautelosa", "bearish"}:
        return 38
    return 50


def _bounded_int(value: int, low: int = 20, high: int = 95) -> int:
    return max(low, min(high, int(round(value))))


def _extract_years_from_result(result: Dict[str, Any]) -> List[str]:
    years = set()

    meta = result.get("_meta")
    if isinstance(meta, dict):
        for y in meta.get("context_years", []) or []:
            ys = _as_str(y)
            if ys:
                years.add(ys)

    for ev in _as_list(result.get("evidencias")):
        if isinstance(ev, dict):
            for k in ("topico", "ano", "year", "trecho", "interpretacao", "leitura"):
                for yy in _YEAR_RE.findall(_as_str(ev.get(k))):
                    years.add(yy)
        else:
            for yy in _YEAR_RE.findall(_as_str(ev)):
                years.add(yy)

    topicos = result.get("topicos")
    if isinstance(topicos, dict):
        for topic_name, payload in topicos.items():
            for yy in _YEAR_RE.findall(_as_str(topic_name)):
                years.add(yy)
            if isinstance(payload, dict):
                for value in payload.values():
                    if isinstance(value, dict):
                        for vv in value.values():
                            for yy in _YEAR_RE.findall(_as_str(vv)):
                                years.add(yy)
                    elif isinstance(value, list):
                        for item in value:
                            for yy in _YEAR_RE.findall(_as_str(item)):
                                years.add(yy)
                    else:
                        for yy in _YEAR_RE.findall(_as_str(value)):
                            years.add(yy)

    return sorted(years)


def estimate_score(result: Dict[str, Any]) -> int:
    base = _score_base(result.get("leitura_direcionalidade") or result.get("direcionalidade"))

    execucao = _pick_dict(result, "execucao_vs_promessa")
    consistencia = _pick_dict(result, "consistencia_discurso", "consistencia_narrativa")

    exec_label = _as_str(execucao.get("avaliacao_execucao")).lower()
    if exec_label == "forte":
        base += 8
    elif exec_label == "moderada":
        base += 2
    elif exec_label in {"fraca", "inconsistente"}:
        base -= 8

    cons_label = _as_str(consistencia.get("grau_consistencia") or consistencia.get("grau")).lower()
    if cons_label == "alto":
        base += 6
    elif cons_label == "baixo":
        base -= 6

    riscos = _count_nonempty(_pick_list(result, "riscos_identificados", "riscos"))
    catalisadores = _count_nonempty(_pick_list(result, "catalisadores", "gatilhos_futuros"))
    evidencias = _count_nonempty(_pick_list(result, "evidencias"))
    pontos = _count_nonempty(_pick_list(result, "pontos_chave"))
    mudancas = _count_nonempty(_pick_list(result, "mudancas_estrategicas"))

    base += min(catalisadores, 6) * 2
    base += min(evidencias, 8) * 1
    base += min(pontos, 6) * 1
    base += min(mudancas, 5) * 1
    base -= min(riscos, 6) * 2

    contradicoes = _count_nonempty(_pick_list(consistencia, "contradicoes", "contradicoes_ou_ruidos"))
    base -= min(contradicoes, 5) * 2

    years = _extract_years_from_result(result)
    if len(years) >= 2:
        base += 3
    elif len(years) == 0:
        base -= 2

    return _bounded_int(base)


def estimate_confidence(result: Dict[str, Any]) -> float:
    evidencias = _count_nonempty(_pick_list(result, "evidencias"))
    riscos = _count_nonempty(_pick_list(result, "riscos_identificados", "riscos"))
    pontos = _count_nonempty(_pick_list(result, "pontos_chave"))
    catalisadores = _count_nonempty(_pick_list(result, "catalisadores", "gatilhos_futuros"))
    monitorar = _count_nonempty(_pick_list(result, "o_que_monitorar"))

    topicos = result.get("topicos")
    n_topicos = len(topicos) if isinstance(topicos, dict) else 0

    detector = result.get("strategy_detector")
    coverage_years = []
    if isinstance(detector, dict):
        coverage_years = detector.get("coverage_years") if isinstance(detector.get("coverage_years"), list) else []

    if not coverage_years:
        coverage_years = _extract_years_from_result(result)

    n_years = len(coverage_years)

    conf = 0.35
    conf += min(evidencias, 8) * 0.05
    conf += min(n_topicos, 6) * 0.03
    conf += min(n_years, 5) * 0.04
    conf += min(pontos, 6) * 0.015
    conf += min(monitorar, 5) * 0.01
    conf += min(catalisadores, 5) * 0.01

    if evidencias == 0:
        conf -= 0.10
    elif evidencias <= 2:
        conf -= 0.04

    if riscos > evidencias and evidencias > 0:
        conf -= 0.03

    tese = _as_str(result.get("tese_sintese") or result.get("tese") or result.get("resumo"))
    if len(tese) >= 120:
        conf += 0.03

    return round(max(0.25, min(0.95, conf)), 2)


def normalize_result(result: Dict[str, Any]) -> Dict[str, Any]:
    score = result.get("score_qualitativo")
    confianca = result.get("confianca_analise")

    if enrich_patch6_result is not None:
        try:
            result = enrich_patch6_result(result or {})
        except Exception:
            result = result or {}
    else:
        result = result or {}

    if score is None or _safe_int(score, 0) <= 0:
        score = estimate_score(result)

    if confianca is None or _safe_float(confianca, 0.0) <= 0:
        confianca = estimate_confidence(result)

    result["score_qualitativo"] = _bounded_int(_safe_int(score, 50))
    result["confianca_analise"] = round(max(0.25, min(0.95, _safe_float(confianca, 0.5))), 2)

    strategy_detector = result.get("strategy_detector")
    if not isinstance(strategy_detector, dict):
        strategy_detector = {}
        result["strategy_detector"] = strategy_detector

    if not strategy_detector.get("coverage_years"):
        strategy_detector["coverage_years"] = _extract_years_from_result(result)

    strategy_detector.setdefault("events_detected", 0)
    strategy_detector.setdefault("yearly_timeline", [])
    strategy_detector.setdefault("detected_changes", [])
    strategy_detector.setdefault("summary", "")

    return result


def build_result_json(llm_output: Dict[str, Any]) -> Dict[str, Any]:
    result = normalize_result(llm_output)
    result["generated_at"] = datetime.utcnow().isoformat()
    return result
