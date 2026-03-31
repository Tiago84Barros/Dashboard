"""Camada de IA/LLM.

Objetivo: desacoplar o projeto do provedor de IA (OpenAI, LLM local, etc.).
O restante do sistema deve consumir apenas contratos estáveis (signals/schemas).
"""

from .config import AIConfig
from .llm_client.factory import get_llm_client

__all__ = ["AIConfig", "get_llm_client"]
