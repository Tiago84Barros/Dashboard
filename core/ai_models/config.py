from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class AIConfig:
    """Configuração central para uso de LLM.

    Trocar de IA deve exigir, no máximo, alterar variáveis de ambiente.
    """

    provider: str = os.getenv("AI_PROVIDER", "openai")  # openai | ollama | dummy
    model: str = os.getenv("AI_MODEL", "gpt-4.1-mini")

    # OpenAI
    openai_api_key: str | None = os.getenv("OPENAI_API_KEY")
    openai_base_url: str | None = os.getenv("OPENAI_BASE_URL")

    # Ollama (local)
    ollama_base_url: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

    # Comportamento
    request_timeout_s: float = float(os.getenv("AI_TIMEOUT_S", "45"))
    max_retries: int = int(os.getenv("AI_MAX_RETRIES", "2"))
