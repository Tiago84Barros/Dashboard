# core/ai_models/prompts/schemas.py
from __future__ import annotations

# ======================================================================
# Schema hints (strings) para o LLM gerar JSON no formato esperado.
# Observação: No projeto, `llm.generate_json(..., schema_hint=...)` aceita string.
# ======================================================================

NEWS_SIGNAL_SCHEMA_HINT = """
{
  "ticker": "STRING",
  "sentiment": 0.0,
  "confidence": 0.0,
  "risks": ["..."],
  "catalysts": ["..."],
  "event_flags": ["..."],
  "justification": ["..."]
}
""".strip()

# ----------------------------------------------------------------------
# Patch 7 — Validação por evidências (notícias)
# O Patch 7 está importando SCHEMA_PATCH7.
# Para manter compatibilidade, expomos um schema hint próprio.
# ----------------------------------------------------------------------

SCHEMA_PATCH7 = """
{
  "resumo": "STRING (4-6 linhas, simples e direto)",
  "catalisadores": ["STRING", "..."],
  "riscos": ["STRING", "..."],
  "veredito": "STRING (fortalece|neutro|enfraquece)",
  "observacoes": ["STRING", "..."]
}
""".strip()
