from __future__ import annotations
import os

from .base import LLMClient
from .openai_client import OpenAIChatClient
from .dummy_client import DummyLLMClient


def get_llm_client() -> LLMClient:
    provider = (os.getenv("AI_PROVIDER") or "openai").lower()

    if provider == "openai":
        return OpenAIChatClient()

    return DummyLLMClient()
