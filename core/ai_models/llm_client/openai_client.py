from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from openai import OpenAI

from core.ai_models.llm_client.base import LLMClient
from core.secrets import get_secret


def _get_api_key() -> str:
    """
    Resolve a API Key da OpenAI com prioridade correta:
    1) Streamlit Secrets (Cloud)
    2) Variável de ambiente
    """
    return get_secret("OPENAI_API_KEY")


class OpenAIChatClient(LLMClient):
    """
    Implementação OpenAI do contrato LLMClient.

    - Compatível com Patch 6 e Patch 7
    - Não quebra se o chamador não passar api_key explicitamente
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        temperature: float = 0.2,
        timeout: int = 60,
    ):
        self.api_key = api_key or _get_api_key()
        # Resolve model: argumento explícito → AI_MODEL env var → default seguro
        self.model = model if model is not None else os.getenv("AI_MODEL", "gpt-4.1-mini")
        self.temperature = temperature
        self.timeout = timeout

        self._client = OpenAI(api_key=self.api_key)

    # ------------------------------------------------------------------
    # GERAÇÃO DE JSON ESTRUTURADO (Patch 6 e 7)
    # ------------------------------------------------------------------
    def generate_json(
        self,
        *,
        system: str,
        user: str,
        schema_hint: str,
        context: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        messages = [{"role": "system", "content": system}]

        if context:
            messages.append(
                {
                    "role": "system",
                    "content": f"Contexto adicional:\n{context}",
                }
            )

        messages.append(
            {
                "role": "user",
                "content": (
                    f"{user}\n\n"
                    f"Responda OBRIGATORIAMENTE em JSON válido no seguinte formato:\n"
                    f"{schema_hint}"
                ),
            }
        )

        try:
            resp = self._client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                timeout=self.timeout,
                response_format={"type": "json_object"},
            )

            content = resp.choices[0].message.content
            return self._safe_json_load(content)

        except Exception as e:
            raise RuntimeError(f"Falha ao chamar OpenAI: {e}") from e

    # ------------------------------------------------------------------
    # EMBEDDINGS (opcional – Patch 7 pode usar)
    # ------------------------------------------------------------------
    def embed(self, texts: List[str]) -> List[List[float]]:
        try:
            resp = self._client.embeddings.create(
                model="text-embedding-3-small",
                input=texts,
            )
            return [d.embedding for d in resp.data]
        except Exception as e:
            raise RuntimeError(f"Falha ao gerar embeddings: {e}") from e

    # ------------------------------------------------------------------
    # UTIL
    # ------------------------------------------------------------------
    @staticmethod
    def _safe_json_load(text: str) -> Dict[str, Any]:
        import json

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            raise ValueError(f"Resposta não é JSON válido:\n{text}")
