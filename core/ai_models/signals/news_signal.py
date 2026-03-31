from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class NewsSignal:
    ticker: str
    sentiment: float = 0.0  # -1..+1
    confidence: float = 0.0  # 0..1
    risks: List[str] = field(default_factory=list)
    catalysts: List[str] = field(default_factory=list)
    event_flags: List[str] = field(default_factory=list)
    justification: List[str] = field(default_factory=list)
