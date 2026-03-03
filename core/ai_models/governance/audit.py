from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict

from ..signals.news_signal import NewsSignal


def signal_audit_payload(sig: NewsSignal, *, provider: str, model: str) -> Dict[str, Any]:
    """Payload pronto para persistir em DB/logs."""
    d = asdict(sig)
    d["ai_provider"] = provider
    d["ai_model"] = model
    return d
