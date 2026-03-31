from __future__ import annotations

from .base import LLMClient
from .openai_client import OpenAIChatClient
from .dummy_client import DummyLLMClient
from ..config import AIConfig


def get_llm_client() -> LLMClient:
    """Única fonte de verdade para criação do cliente LLM.

    Lê provider e model de AIConfig (variáveis AI_PROVIDER / AI_MODEL).
    """
    cfg = AIConfig()
    provider = (cfg.provider or "openai").lower()

    if provider == "openai":
        return OpenAIChatClient(model=cfg.model)

    return DummyLLMClient()
