
from core.analysis_policy import AnalysisPolicy
from core.prompts.portfolio_rigid import PROMPT_RIGID
from core.prompts.portfolio_flexible import PROMPT_FLEXIBLE

def generate_portfolio_report(llm_client, context: dict, policy: AnalysisPolicy):
    prompt = PROMPT_RIGID if policy.mode == "rigid" else PROMPT_FLEXIBLE

    response = llm_client.generate_json(
        prompt=prompt,
        data=context,
        temperature=policy.temperature
    )
    return response
