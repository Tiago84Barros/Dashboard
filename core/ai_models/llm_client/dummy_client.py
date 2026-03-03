from __future__ import annotations
from typing import Any, Dict, List, Optional

from .base import LLMClient


class DummyLLMClient(LLMClient):
    def generate_json(
        self,
        *,
        system: str,
        user: str,
        schema_hint: str,
        context: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        return {}

    def embed(self, texts: List[str]) -> List[List[float]]:
        raise NotImplementedError
