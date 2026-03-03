from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple


@dataclass
class TTLCache:
    ttl_seconds: int = 3600
    _data: Dict[str, Tuple[datetime, Any]] = None  # type: ignore

    def __post_init__(self) -> None:
        if self._data is None:
            self._data = {}

    def get(self, key: str) -> Optional[Any]:
        item = self._data.get(key)
        if not item:
            return None
        ts, value = item
        if datetime.utcnow() - ts > timedelta(seconds=self.ttl_seconds):
            self._data.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        self._data[key] = (datetime.utcnow(), value)
