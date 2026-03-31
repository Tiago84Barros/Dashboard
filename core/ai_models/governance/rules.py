from __future__ import annotations

from ..signals.news_signal import NewsSignal


def apply_governance_caps(sig: NewsSignal) -> NewsSignal:
    """Guardrails para limitar impacto do LLM.

    - sentiment já vem clampado (-1..+1)
    - confidence já vem clampado (0..1)
    Aqui você pode impor regras adicionais.
    """

    # Se confiança for muito baixa, zera sentimento para reduzir ruído.
    if sig.confidence < 0.25:
        sig.sentiment = 0.0

    # Limita tamanho das listas (protege UI e logs)
    sig.risks = sig.risks[:8]
    sig.catalysts = sig.catalysts[:8]
    sig.event_flags = sig.event_flags[:12]
    sig.justification = sig.justification[:6]
    return sig
