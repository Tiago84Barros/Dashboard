# core/macro_audit.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import streamlit as st


@dataclass
class AuditLogger:
    title: str = "Auditoria Macro (BCB)"
    lines: List[str] = field(default_factory=list)
    _box: Optional[st.delta_generator.DeltaGenerator] = None

    def bind(self, box: st.delta_generator.DeltaGenerator) -> "AuditLogger":
        self._box = box
        self._render()
        return self

    def log(self, msg: str) -> None:
        self.lines.append(msg)
        self._render()

    def _render(self) -> None:
        if self._box is None:
            return
        text = "\n".join(self.lines[-500:])  # evita ficar gigante
        self._box.code(text, language="text")
