from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class LLMClient(ABC):
    """Contrato único de LLM.

    O projeto inteiro deve depender apenas desta interface.
    """

    @abstractmethod
    def generate_json(
        self,
        *,
        system: str,
        user: str,
        schema_hint: str,
        context: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Gera uma resposta estruturada em JSON.

        - system: instruções fixas e guardrails
        - user: tarefa principal
        - schema_hint: JSON esperado (texto)
        - context: lista de itens (ex.: notícias) já pré-filtradas
        """

    @abstractmethod
    def embed(self, texts: List[str]) -> List[List[float]]:
        """Opcional: embeddings.

        Nem todo provider terá embeddings via API. Se não suportar,
        deve lançar NotImplementedError.
        """
