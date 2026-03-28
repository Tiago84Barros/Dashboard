from __future__ import annotations

import json
from typing import Any, Dict, List

from core.analysis_policy import AnalysisPolicy
from core.prompts.portfolio_flexible import PROMPT_FLEXIBLE
from core.prompts.portfolio_rigid import PROMPT_RIGID


PORTFOLIO_REPORT_SCHEMA_HINT = json.dumps(
    {
        "analysis_mode": "rigid|flexible",
        "analytical_basis": "string",
        "executive_summary": "string",
        "portfolio_identity": "string",
        "key_strengths": ["string"],
        "key_weaknesses": ["string"],
        "macro_reading": "string",
        "hidden_risks": ["string"],
        "asset_roles": [
            {
                "ticker": "string",
                "role": "string",
                "rationale": "string",
            }
        ],
        "misalignments": ["string"],
        "action_plan": ["string"],
        "final_insight": "string",
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
