from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import requests

from ..config import AIConfig
from .base import LLMClient


class OllamaChatClient(LLMClient):
    """Implementação simples para LLM local via Ollama.

    Requer Ollama rodando (default: http://localhost:11434).

    Endpoint usado: /api/chat
    Docs: https://github.com/ollama/ollama
    """

    def __init__(self, config: AIConfig | None = None) -> None:
        cfg = config or AIConfig()
        self.model = cfg.model  # ex.: "llama3.1" ou "qwen2.5"
        self.base_url = cfg.ollama_base_url.rstrip("/")
        self.timeout_s = cfg.request_timeout_s

    def generate_json(
        self,
        *,
        system: str,
        user: str,
        schema_hint: str,
        context: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        messages: List[Dict[str, str]] = [
            {"role": "system", "content": system},
        ]

        if context:
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

        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            # Força tendência a JSON (nem todo modelo respeita 100%)
            "format": "json",
        }

        url = f"{self.base_url}/api/chat"
        try:
            r = requests.post(url, json=payload, timeout=self.timeout_s)
            r.raise_for_status()
            data = r.json()
            content = (data.get("message") or {}).get("content") or "{}"
            return json.loads(content)
        except Exception as e:
            raise RuntimeError(f"Falha ao chamar Ollama em {url}: {e}") from e

    def embed(self, texts: List[str]) -> List[List[float]]:
        raise NotImplementedError("Embeddings via Ollama não implementados neste client.")
