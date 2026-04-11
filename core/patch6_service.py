# core/patch6_service.py
# LLM orchestration layer for Patch6 portfolio reports.
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from core.ai_models.config import AIConfig
from core.patch6_schema import PortfolioAnalysis

logger = logging.getLogger(__name__)


def safe_call_llm(llm_client: Any, prompt: str) -> Optional[str]:
    """Calls any supported LLM client interface. Returns None on failure."""
    try:
        if llm_client is None:
            return None

        model = AIConfig().model

        if hasattr(llm_client, "responses") and hasattr(llm_client.responses, "create") and callable(llm_client.responses.create):
            resp = llm_client.responses.create(model=model, input=prompt)
            txt = getattr(resp, "output_text", None)
            if txt:
                return txt
            try:
                return resp.output[0].content[0].text
            except Exception:
                return str(resp)

        if hasattr(llm_client, "chat") and hasattr(llm_client.chat, "completions") and hasattr(llm_client.chat.completions, "create"):
            resp = llm_client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
            )
            return resp.choices[0].message.content

        if hasattr(llm_client, "complete") and callable(getattr(llm_client, "complete")):
            return llm_client.complete(prompt)
        if hasattr(llm_client, "chat") and callable(getattr(llm_client, "chat")):
            return llm_client.chat(prompt)
        if hasattr(llm_client, "invoke") and callable(getattr(llm_client, "invoke")):
            return llm_client.invoke(prompt)
        if callable(llm_client):
            return llm_client(prompt)

        return None

    except Exception as exc:
        logger.warning("safe_call_llm falhou [%s]: %s", type(exc).__name__, exc)
        return None


def _build_company_macro_map(companies: Dict[str, Any], macro_context: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    try:
        from core.asset_macro_profile import get_asset_macro_profile
    except Exception:
        return {}

    trends = macro_context.get("trends", {}) or {}
    market_summary = (macro_context.get("macro_summary") or {}).copy()
    output: Dict[str, Dict[str, Any]] = {}

    for ticker, company in companies.items():
        profile = get_asset_macro_profile(ticker)
        sensitivities = list(profile.get("macro_sensitivities") or [])
        company_sensitivities = list(getattr(company, "sensibilidades_macro", []) or [])
        merged_sensitivities = []
        for item in sensitivities + company_sensitivities:
            key = str(item).strip()
            if key and key not in merged_sensitivities:
                merged_sensitivities.append(key)

        macro_flags: List[str] = []
        if "juros" in merged_sensitivities:
            macro_flags.append(f"selic_{(trends.get('selic') or {}).get('trend', 'indefinido')}")
        if "cambio" in merged_sensitivities:
            macro_flags.append(f"cambio_{(trends.get('cambio') or {}).get('trend', 'indefinido')}")
        if "inflacao" in merged_sensitivities:
            macro_flags.append(f"ipca_{(trends.get('ipca_12m') or {}).get('trend', 'indefinido')}")
        if "atividade_domestica" in merged_sensitivities or "consumo" in merged_sensitivities:
            macro_flags.append(f"icc_{(trends.get('icc') or {}).get('trend', 'indefinido')}")
        if any(k in merged_sensitivities for k in ("minerio", "petroleo", "commodities")):
            macro_flags.append("commodity_global_sensivel")

        output[ticker] = {
            "role_hint": profile.get("role_hint"),
            "style": profile.get("style"),
            "risk_bucket": profile.get("risk_bucket"),
            "macro_sensitivities": merged_sensitivities,
            "macro_flags": macro_flags,
            "company_specific_notes": [
                f"Ativo com sensibilidade a: {', '.join(merged_sensitivities)}" if merged_sensitivities else "Sensibilidades macro não mapeadas",
                f"Fragilidade sob regime atual: {getattr(company, 'fragilidade_regime_atual', '')}" if getattr(company, 'fragilidade_regime_atual', '') else "",
                f"Dependências de cenário: {', '.join(getattr(company, 'dependencias_cenario', []) or [])}" if getattr(company, 'dependencias_cenario', []) else "",
            ],
            "macro_summary_snapshot": market_summary,
        }
        output[ticker]["company_specific_notes"] = [x for x in output[ticker]["company_specific_notes"] if x]
    return output


def build_portfolio_context_payload(
    analysis: PortfolioAnalysis,
    macro_context: Dict[str, Any],
    market_context: Dict[str, Any],
    allocation_rows_dicts: List[Dict[str, Any]],
) -> Dict[str, Any]:
    stats = analysis.stats
    ticker_rows: List[Dict[str, Any]] = []
    company_macro_map = _build_company_macro_map(analysis.companies, macro_context)
    for company in analysis.companies.values():
        ticker_rows.append(
            {
                "ticker": company.ticker,
                "perspectiva_compra": company.perspectiva_compra,
                "score_qualitativo": company.score_qualitativo,
                "confianca": company.confianca,
                "robustez_qualitativa": company.robustez_qualitativa,
                "execution_trend": company.execution_trend,
                "narrative_shift": company.narrative_shift,
                "narrative_dispersion_score": company.narrative_dispersion_score,
                "tese": company.tese,
                "leitura": company.leitura,
                "papel_estrategico": company.papel_estrategico,
                "sensibilidades_macro": company.sensibilidades_macro,
                "fragilidade_regime_atual": company.fragilidade_regime_atual,
                "dependencias_cenario": company.dependencias_cenario,
                "alocacao_sugerida_faixa": company.alocacao_sugerida_faixa,
                "racional_alocacao": company.racional_alocacao,
                "riscos": company.riscos,
                "catalisadores": company.catalisadores,
                "pontos_chave": company.pontos_chave,
                "monitorar": company.monitorar,
                "mudancas": company.mudancas,
                "macro_profile": company_macro_map.get(company.ticker, {}),
            }
        )

    return {
        "period_ref": analysis.period_ref,
        "portfolio_stats": {
            "fortes": stats.fortes,
            "moderadas": stats.moderadas,
            "fracas": stats.fracas,
            "desconhecidas": stats.desconhecidas,
            "qualidade": stats.label_qualidade(),
            "perspectiva": stats.label_perspectiva(),
            "cobertura": analysis.cobertura,
            "confianca_media": analysis.confianca_media,
            "score_medio": analysis.score_medio,
        },
        "tickers": ticker_rows,
        "macro_context": macro_context,
        "market_context": market_context,
        "macro_company_map": company_macro_map,
        "current_allocations": allocation_rows_dicts,
    }


def run_portfolio_llm_report(
    llm_factory: Any,
    analysis: PortfolioAnalysis,
    analysis_mode: str = "rigid",
) -> Optional[Dict[str, Any]]:
    if llm_factory is None:
        return None

    try:
        llm_client = llm_factory.get_llm_client()
    except Exception:
        llm_client = None

    if llm_client is None:
        return None

    macro_context: Dict[str, Any] = {}
    market_context: Dict[str, Any] = {}
    try:
        from core.macro_context import load_latest_macro_context
        from core.market_context import build_market_context
        macro_context = load_latest_macro_context()
        market_context = build_market_context(macro_context)
    except Exception as exc:
        logger.warning("Falha ao carregar contexto macro para Patch6 [%s]: %s", type(exc).__name__, exc)

    allocation_rows_dicts = [
        {
            "ticker": r.ticker,
            "perspectiva": r.perspectiva,
            "allocation_pct": r.allocation_pct,
            "score": r.score,
            "confianca": r.confianca,
            "robustez": r.robustez,
            "execution_trend": r.execution_trend,
        }
        for r in analysis.allocation_rows
    ]

    rag_context_by_ticker: Dict[str, str] = {}
    try:
        from core.patch6_rag import build_rag_context
        for company in analysis.companies.values():
            rag_ctx = build_rag_context(company.ticker, max_total=8, days_back=730)
            if rag_ctx.total_selected > 0:
                rag_context_by_ticker[company.ticker] = rag_ctx.as_text(max_chars_per_doc=2000)
    except Exception:
        pass

    try:
        from core.analysis_policy import get_analysis_policy
        from core.portfolio_llm_report import generate_portfolio_report
        policy = get_analysis_policy(analysis_mode)
        context_payload = build_portfolio_context_payload(
            analysis=analysis,
            macro_context=macro_context,
            market_context=market_context,
            allocation_rows_dicts=allocation_rows_dicts,
        )
        if rag_context_by_ticker:
            context_payload["rag_evidence_by_ticker"] = rag_context_by_ticker
        return generate_portfolio_report(
            llm_client=llm_client,
            context_payload=context_payload,
            policy=policy,
        )
    except Exception as exc:
        logger.warning("run_portfolio_llm_report falhou [%s]: %s", type(exc).__name__, exc)
        return None
