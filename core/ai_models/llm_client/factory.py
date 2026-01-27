from __future__ import annotations

from ..config import AIConfig
from .base import LLMClient
from .dummy_client import DummyLLMClient
from .openai_client import OpenAIChatClient


def get_llm_client(config: AIConfig | None = None) -> LLMClient:
    """Factory de LLM.

    Controlado por env AI_PROVIDER.
    """

    cfg = config or AIConfig()
    provider = (cfg.provider or "").lower().strip()

    if provider == "openai":
        return OpenAIChatClient(config=cfg)

    if provider in {"ollama", "local", "local_ollama"}:
        from .ollama_client import OllamaChatClient

        return OllamaChatClient(config=cfg)

    if provider in {"dummy", "off", "disabled"}:
        return DummyLLMClient()

    # fallback seguro
    return DummyLLMClient(error=f"AI_PROVIDER inválido: {cfg.provider!r}")
