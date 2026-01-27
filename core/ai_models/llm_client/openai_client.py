from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from ..config import AIConfig
from .base import LLMClient


class OpenAIChatClient(LLMClient):
    """Implementação OpenAI (openai>=1.x).

    Mantém contrato estável para o restante do projeto.
    """

    def __init__(self, config: AIConfig | None = None) -> None:
        cfg = config or AIConfig()
        self.model = cfg.model
        self.timeout_s = cfg.request_timeout_s
        self.api_key = cfg.openai_api_key
        self.base_url = cfg.openai_base_url

        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY não encontrado no ambiente.")

        try:
            from openai import OpenAI  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError(
                "Pacote 'openai' não instalado. Adicione 'openai>=1.0.0' ao requirements.txt."
            ) from e

        kwargs: Dict[str, Any] = {"api_key": self.api_key}
        if self.base_url:
            kwargs["base_url"] = self.base_url
        self._client = OpenAI(**kwargs)

    def generate_json(
        self,
        *,
        system: str,
        user: str,
        schema_hint: str,
        context: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        messages: List[Dict[str, Any]] = [
            {"role": "system", "content": system},
        ]

        if context:
            # Contexto como payload JSON compactado (notícias já selecionadas).
            messages.append(
                {
                    "role": "system",
                    "content": "CONTEXTO(JSON):\n" + json.dumps(context, ensure_ascii=False),
                }
            )

        messages.append(
            {
                "role": "user",
                "content": user + "\n\nSAÍDA_DESEJADA(JSON):\n" + schema_hint,
            }
        )

        try:
            resp = self._client.chat.completions.create(
                model=self.model,
                messages=messages,
                response_format={"type": "json_object"},
                timeout=self.timeout_s,
            )
            content = resp.choices[0].message.content or "{}"
            return json.loads(content)
        except Exception as e:
            raise RuntimeError(f"Falha ao chamar OpenAI: {e}") from e

    def embed(self, texts: List[str]) -> List[List[float]]:
        # Nem todo deployment libera embeddings; implemente quando precisar.
        raise NotImplementedError("Embeddings via OpenAI não habilitados neste client.")
