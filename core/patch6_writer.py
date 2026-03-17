
# PATCH6 WRITER FINAL (COM PATCH7 + FALLBACK)

import json
from datetime import datetime
from typing import Dict, Any

def normalize_result(result: Dict[str, Any]) -> Dict[str, Any]:
    score = result.get("score_qualitativo")
    confianca = result.get("confianca_analise")

    if score is None:
        score = 50

    if confianca is None:
        confianca = 0.5

    result["score_qualitativo"] = score
    result["confianca_analise"] = confianca

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
