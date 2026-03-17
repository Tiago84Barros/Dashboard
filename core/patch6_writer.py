# PATCH6 WRITER FINAL (COM PATCH7 + SCORE HEURÍSTICO)

import json
from datetime import datetime
from typing import Dict, Any, List


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


def _bounded_float(value: float, low: float = 0.25, high: float = 0.95) -> float:
    return max(low, min(high, float(value)))


def estimate_score(result: Dict[str, Any]) -> int:
    base = _score_base(result.get("leitura_direcionalidade") or result.get("direcionalidade"))

    execucao = _pick_dict(result, "execucao_vs_promessa")
    consistencia = _pick_dict(result, "consistencia_discurso", "consistencia_narrativa")

    exec_label = _as_str(execucao.get("avaliacao_execucao")).lower()
    if exec_label == "forte":
        base += 8
    elif exec_label == "fraca":
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

    base += min(catalisadores, 4) * 2
    base += min(evidencias, 6) * 1
    base += min(pontos, 5) * 1
    base += min(mudancas, 4) * 1
    base -= min(riscos, 5) * 2

    contradicoes = _count_nonempty(_pick_list(consistencia, "contradicoes", "contradicoes_ou_ruidos"))
    base -= min(contradicoes, 4) * 2

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
    n_years = len(coverage_years)

    conf = 0.35
    conf += min(evidencias, 6) * 0.05
    conf += min(n_topicos, 5) * 0.03
    conf += min(n_years, 4) * 0.03
    conf += min(pontos, 5) * 0.015
    conf += min(monitorar, 4) * 0.01
    conf += min(catalisadores, 4) * 0.01

    if evidencias == 0:
        conf -= 0.08
    if riscos > evidencias and evidencias > 0:
        conf -= 0.03

    tese = _as_str(result.get("tese_sintese") or result.get("tese") or result.get("resumo"))
    if len(tese) >= 120:
        conf += 0.03

    return round(_bounded_float(conf), 2)


def normalize_result(result: Dict[str, Any]) -> Dict[str, Any]:
    score = result.get("score_qualitativo")
    confianca = result.get("confianca_analise")

    if score is None or _safe_int(score, 0) <= 0:
        score = estimate_score(result)

    if confianca is None or _safe_float(confianca, 0.0) <= 0:
        confianca = estimate_confidence(result)

    result["score_qualitativo"] = _bounded_int(_safe_int(score, 50))
    result["confianca_analise"] = round(_bounded_float(_safe_float(confianca, 0.5)), 2)

    if "strategy_detector" not in result:
        result["strategy_detector"] = {
            "events_detected": 0,
            "coverage_years": []
        }

    return result


def build_result_json(llm_output: Dict[str, Any]) -> Dict[str, Any]:
    result = normalize_result(llm_output)
    result["generated_at"] = datetime.utcnow().isoformat()
    return result


def save_patch6_run(db, ticker: str, result_json: Dict[str, Any]):
    query = """
    INSERT INTO patch6_runs (ticker, result_json)
    VALUES (%s, %s)
    """
    db.execute(query, (ticker, json.dumps(result_json)))
    db.commit()
