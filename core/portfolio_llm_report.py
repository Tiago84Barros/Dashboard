from __future__ import annotations

import json
from typing import Any, Dict, List

from core.analysis_policy import AnalysisPolicy
from core.prompts.portfolio_rigid import PROMPT_RIGID
from core.prompts.portfolio_flexible import PROMPT_FLEXIBLE


PORTFOLIO_REPORT_SCHEMA_HINT = json.dumps(
    {
        "analysis_mode": "rigid|flexible",
        "analytical_basis": "string — explicar que bases foram usadas: quanti (patches 1-5), qualitativo (documentos) e macro",
        "executive_summary": "string — síntese das três camadas: o que o snapshot quantitativo indicou, o que o qualitativo confirma ou contradiz, e como o macro posiciona a carteira",
        "portfolio_identity": "string — o que essa carteira REALMENTE é, com base no perfil quantitativo e qualitativo combinados",
        "quantitative_profile": "string — resumo do perfil quantitativo: força dominante, fator prevalente, penalizações relevantes, ranking da carteira no snapshot",
        "quanti_quali_macro_convergences": [
            "string — para cada convergência relevante: ex: 'BBAS3: quant FORTE (rank 1) + perspectiva forte + Selic ↑ favorece spread → convicção alta'"
        ],
        "quanti_quali_macro_conflicts": [
            "string — para cada conflito relevante: ex: 'DIRR3: quant FORTE mas deterioração de execução + Selic alta pressiona crédito → convicção reduzida'"
        ],
        "current_market_context": "string",
        "macro_reading": "string — macro com valores numéricos explícitos conectados ao perfil quantitativo da carteira",
        "international_risk_links": ["string"],
        "macro_scenario_dependencies": ["string"],
        "portfolio_vulnerabilities_under_current_regime": ["string"],
        "what_the_portfolio_is_implicitly_betting_on": ["string"],
        "portfolio_concentration_analysis": "string — análise de concentração por fator quantitativo dominante, não apenas por setor",
        "allocation_adjustment_rationale": "string — explicar o critério integrado de alocação: quant + quali + macro",
        "key_strengths": ["string"],
        "key_weaknesses": ["string"],
        "hidden_risks": ["string"],
        "asset_roles": [
            {
                "ticker": "string",
                "role": "string",
                "rationale": "string — citar quant_classe, perspectiva qualitativa e encaixe macro"
            }
        ],
        "suggested_allocations": [
            {
                "ticker": "string",
                "suggested_range": "string — ex: 8%–12%",
                "quant_basis": "string — ex: FORTE, rank 2, score 74.3, driver: dividendos + qualidade",
                "quali_basis": "string — ex: tese confirmada, execução melhorando, perspectiva forte",
                "macro_basis": "string — ex: Selic ↑ favorece spread bancário; câmbio neutro",
                "rationale": "string — síntese dos três vetores + penalizações + convicção resultante"
            }
        ],
        "misalignments": ["string"],
        "action_plan": ["string"],
        "final_insight": "string"
    },
    ensure_ascii=False,
    indent=2,
)


def generate_portfolio_report(
    *,
    llm_client: Any,
    context_payload: Dict[str, Any],
    policy: AnalysisPolicy,
) -> Dict[str, Any]:
    system_prompt = PROMPT_RIGID if policy.mode == "rigid" else PROMPT_FLEXIBLE

    user_prompt = (
        "Gere um relatório consolidado de portfólio em formato JSON, "
        "seguindo exatamente o schema informado."
    )

    context_list: List[Dict[str, Any]] = [
        {
            "analysis_policy": {
                "mode": policy.mode,
                "label": policy.label,
                "allow_external_inference": policy.allow_external_inference,
                "require_strict_grounding": policy.require_strict_grounding,
                "allow_macro_generalization": policy.allow_macro_generalization,
                "allow_behavioral_inference": policy.allow_behavioral_inference,
                "temperature": policy.temperature,
            }
        },
        {
            "portfolio_context": context_payload
        },
    ]

    return llm_client.generate_json(
        system=system_prompt,
        user=user_prompt,
        schema_hint=PORTFOLIO_REPORT_SCHEMA_HINT,
        context=context_list,
    )
