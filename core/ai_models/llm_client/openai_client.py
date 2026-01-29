from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from core.ai_models.llm_client.base import LLMClient

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None


class OpenAIChatClient(LLMClient):
    def __init__(
        self,
        *,
        api_key: str,
        model: str = "gpt-4o-mini",
        timeout_s: int = 60,
        max_retries: int = 3,
    ) -> None:
        if OpenAI is None:
            raise RuntimeError("SDK openai não disponível. Verifique requirements.")
        self.client = OpenAI(api_key=api_key, timeout=timeout_s)  # ✅ timeout maior
        self.model = model
        self.max_retries = max_retries

    def generate_json(
        self,
        *,
        system: str,
        user: str,
        schema_hint: str,
        context: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        ctx = context or []
        # ✅ compacta contexto para não explodir tokens
        # (você pode ajustar limite abaixo)
        if len(ctx) > 20:
            ctx = ctx[:20]

        payload_user = (
            f"{user}\n\n"
            f"=== CONTEXTO (itens pré-filtrados) ===\n"
            f"{json.dumps(ctx, ensure_ascii=False)[:12000]}\n\n"
            f"=== FORMATO JSON ESPERADO ===\n{schema_hint}\n"
            f"Responda SOMENTE com JSON válido."
        )

        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": payload_user},
                    ],
                    temperature=0.2,
                )
                txt = (resp.choices[0].message.content or "").strip()
                return json.loads(txt)
            except Exception as e:
                last_err = e
                # ✅ retry com backoff exponencial
                if attempt < self.max_retries:
                    time.sleep(1.5 * (2 ** attempt))
                    continue
                raise RuntimeError(f"Falha ao chamar OpenAI: {e}") from e

    def embed(self, texts: List[str]) -> List[List[float]]:
        raise NotImplementedError("Embeddings não habilitados neste projeto.")
