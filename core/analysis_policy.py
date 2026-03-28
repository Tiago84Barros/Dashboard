from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

AnalysisMode = Literal["rigid", "flexible"]


@dataclass(frozen=True)
class AnalysisPolicy:
    mode: AnalysisMode
    label: str
    allow_external_inference: bool
    require_strict_grounding: bool
    allow_macro_generalization: bool
    allow_behavioral_inference: bool
    temperature: float


def get_analysis_policy(mode: str) -> AnalysisPolicy:
    mode_norm = str(mode or "rigid").strip().lower()

    if mode_norm == "flexible":
        return AnalysisPolicy(
            mode="flexible",
            label="Análise Flexível",
            allow_external_inference=True,
            require_strict_grounding=False,
            allow_macro_generalization=True,
            allow_behavioral_inference=True,
            temperature=0.30,
        )

    return AnalysisPolicy(
        mode="rigid",
        label="Análise Rígida",
        allow_external_inference=False,
        require_strict_grounding=True,
        allow_macro_generalization=False,
        allow_behavioral_inference=True,
        temperature=0.15,
    )
