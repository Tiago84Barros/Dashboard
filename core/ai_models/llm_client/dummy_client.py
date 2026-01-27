from __future__ import annotations

from typing import Any, Dict, List, Optional

from .base import LLMClient


class DummyLLMClient(LLMClient):
    """Cliente de desenvolvimento: falha com mensagem clara (ou retorna neutro se desejar)."""

    def __init__(self, error: str | None = None) -> None:
        self._error = error or (
            "LLM não configurado. Defina AI_PROVIDER=openai e OPENAI_API_KEY, "
            "ou use AI_PROVIDER=ollama para LLM local."
        )

    def generate_json(
        self,
        *,
        system: str,
        user: str,
        schema_hint: str,
        context: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        raise RuntimeError(self._error)

    def embed(self, texts: List[str]) -> List[List[float]]:
        raise NotImplementedError(self._error)
