# core/patch6_schema.py
# Typed output contract for the Patch6 analysis pipeline.
# No logic — only data structures.
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class PortfolioStats:
    fortes: int = 0
    moderadas: int = 0
    fracas: int = 0
    desconhecidas: int = 0

    @property
    def total(self) -> int:
        return self.fortes + self.moderadas + self.fracas + self.desconhecidas

    def label_qualidade(self) -> str:
        if self.total == 0:
            return "—"
        if self.fortes >= max(1, int(0.4 * self.total)):
            return "Alta"
        if self.fracas >= max(1, int(0.4 * self.total)):
            return "Baixa"
        return "Moderada"

    def label_perspectiva(self) -> str:
        if self.total == 0:
            return "—"
        if self.fortes > self.fracas and self.fortes >= self.moderadas:
            return "Construtiva"
        if self.fracas > self.fortes and self.fracas >= self.moderadas:
            return "Cautelosa"
        return "Neutra"


@dataclass
class CompanyAnalysis:
    """Parsed and computed view of a single company's Patch6 result."""
    ticker: str
    period_ref: str
    created_at: Any

    perspectiva_compra: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    # Narrative
    tese: str = ""
    leitura: str = ""
    consideracoes: str = ""

    # Structured sections
    evolucao: Dict[str, Any] = field(default_factory=dict)
    consistencia: Dict[str, Any] = field(default_factory=dict)
    execucao: Dict[str, Any] = field(default_factory=dict)
    qualidade_narrativa: Dict[str, Any] = field(default_factory=dict)
    strategy_detector: Dict[str, Any] = field(default_factory=dict)

    # Lists
    riscos: List[str] = field(default_factory=list)
    catalisadores: List[str] = field(default_factory=list)
    monitorar: List[str] = field(default_factory=list)
    mudancas: List[str] = field(default_factory=list)
    pontos_chave: List[str] = field(default_factory=list)
    contradicoes: List[str] = field(default_factory=list)
    sinais_ruido: List[str] = field(default_factory=list)
    evidencias: List[Any] = field(default_factory=list)

    # Macro/allocation
    papel_estrategico: str = ""
    sensibilidades_macro: List[str] = field(default_factory=list)
    fragilidade_regime_atual: str = ""
    dependencias_cenario: List[str] = field(default_factory=list)
    alocacao_sugerida_faixa: str = ""
    racional_alocacao: str = ""

    # Scores
    score_qualitativo: int = 0
    confianca: float = 0.0
    score_source: str = "llm"   # "llm" | "heuristic"

    # v2 — stability and robustness metrics
    robustez_qualitativa: float = 0.0           # 0-1 composite quality (evidence + schema + temporal + confidence)
    narrative_dispersion_score: float = 0.0     # 0-1 (high = uneven section coverage)
    execution_trend: str = "—"                  # melhorando | estável | deteriorando | —
    narrative_shift: str = "—"                  # significativo | moderado | estável | —
    schema_score: int = 0                        # 0-100 structural field coverage
    validation_warnings: List[str] = field(default_factory=list)

    # v3 — historical memory
    memory_summary: str = ""
    recurring_promises: List[str] = field(default_factory=list)
    delivered_promises: List[str] = field(default_factory=list)
    persistent_risks: List[str] = field(default_factory=list)
    persistent_catalysts: List[str] = field(default_factory=list)

    # v3 — regime detection
    current_regime: str = "—"
    previous_regime: str = "—"
    regime_change_intensity: str = "—"          # significativo | moderado | estável | —
    regime_change_explanation: str = ""

    # v3 — priority queue
    attention_score: float = 0.0
    attention_level: str = "baixa"              # alta | média | baixa
    recommended_action: str = ""
    attention_drivers: List[str] = field(default_factory=list)

    # v3 — forward signal
    forward_score: int = 0
    forward_direction: str = "—"               # melhorando | estável | deteriorando | —
    forward_confidence: float = 0.0
    forward_drivers: List[str] = field(default_factory=list)

    # v4 — derived decision fields (runtime computed, never stored in DB)
    # Derivados de perspectiva_compra + forward_direction + execution_trend.
    decision_score: int = 0                    # -2 | -1 | 0 | +1 | +2
    decision_label: str = "—"                 # reduzir | revisar | manter | aumentar
    risk_rank: List[str] = field(default_factory=list)  # riscos ordenados por prioridade

    # v5 — macro exposure (runtime derived from asset_macro_profile + macro trends)
    macro_exposure: str = ""                   # "Favorecido" | "Pressionado" | "Misto" | "Neutro"
    macro_exposure_tone: str = "neutral"       # good | warn | bad | neutral
    macro_exposure_detail: str = ""            # e.g. "Selic ↑ favorece spread bancário"

    # v6 — quantitative snapshot integration (runtime, from portfolio_snapshot_analysis)
    quant_classe: str = ""                     # "FORTE" | "MODERADA" | "FRACA"
    quant_rank_geral: int = 0                  # rank overall in snapshot
    quant_score_final: float = 0.0             # score_final from snapshot
    quant_context_text: str = ""               # formatted text block for LLM
    quant_convergence: str = ""                # convergence/conflict assessment text
    quant_allocation_multiplier: float = 1.0   # allocation adjustment factor (0.70–1.30)


@dataclass
class AllocationRow:
    ticker: str
    perspectiva: str
    raw_weight: float
    allocation_pct: float
    score: int
    confianca: float
    robustez: float = 0.0           # v2: from robustez_qualitativa
    execution_trend: str = "—"      # v2: from temporal analysis


@dataclass
class PortfolioAnalysis:
    """Aggregated result for a full portfolio in a given period."""
    period_ref: str
    tickers_requested: List[str]
    stats: PortfolioStats
    companies: Dict[str, CompanyAnalysis]       # keyed by ticker
    allocation_rows: List[AllocationRow]
    confianca_media: float
    score_medio: int
    cobertura: str                              # e.g. "5/8"
    temporal_covered: int
    contexto_portfolio: str                     # pre-built bullet text for LLM

    # v3 — portfolio-level aggregates
    priority_ranking: List[str] = field(default_factory=list)   # tickers sorted by attention_score desc
    alta_prioridade_count: int = 0
    forward_score_medio: int = 0
    regime_summary: str = ""                    # short summary of regime changes across portfolio

    # v4 — portfolio trend (runtime derived, never stored)
    # Keys: qualidade | execucao | governanca | capital
    # Values: "favorável" | "estável" | "atenção" | "deteriorando" | "neutro" | "cauteloso"
    portfolio_trend: Dict[str, str] = field(default_factory=dict)

    # v5 — macro narrative (runtime derived)
    macro_narrative: str = ""                  # "Ambiente restritivo. Favorece: BBAS3, ITUB4. Pressiona: DIRR3."

    # v6 — quantitative portfolio summary (runtime, from portfolio_snapshot_analysis)
    quant_portfolio_summary: str = ""          # aggregated text for LLM context
