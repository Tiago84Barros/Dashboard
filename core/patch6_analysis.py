# core/patch6_analysis.py
# Pure computation layer for Patch6 — no Streamlit, no rendering.
#
# v3 pipeline per company:
#   1. parse result_json
#   2. validate schema          (patch6_validation)
#   3. compute hybrid score     (patch6_scoring)
#   4. enrich temporal metrics  (patch6_temporal)
#   5. tag evidence topics      (patch6_rag)
#   6. build historical memory  (patch6_memory)
#   7. detect regime change     (patch6_regime)
#   8. compute forward signal   (patch6_forward)
#   9. compute priority         (patch6_priority)
#  10. assemble CompanyAnalysis with all v3 fields
#
# Uses core.db.get_engine() directly — never core.db_loader.
# All v3 calls are wrapped in try/except — failure degrades safely.
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text

from core.db import get_engine
from core.patch6_rag import enrich_evidencias_with_topics
from core.patch6_schema import (
    AllocationRow,
    CompanyAnalysis,
    PortfolioAnalysis,
    PortfolioStats,
)
from core.patch6_scoring import compute_hybrid_score
from core.patch6_temporal import TemporalData, load_temporal_batch
from core.patch6_validation import validate_result


# ────────────────────────────────────────────────────────────────────────────────
# Text / JSON helpers (imported by patch6_report and patch6_service)
# ────────────────────────────────────────────────────────────────────────────────

_TAG_RE = re.compile(r"<[^>]+>")


def strip_html(value: Any) -> str:
    if value is None:
        return ""
    txt = str(value)
    txt = _TAG_RE.sub("", txt)
    txt = txt.replace("&nbsp;", " ")
    txt = re.sub(r"\s+\n", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def as_result_obj(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        if isinstance(value, str) and value.strip():
            return json.loads(value)
    except Exception:
        return {}
    return {}


def pick_text(obj: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = obj.get(key)
        if isinstance(value, str) and value.strip():
            return strip_html(value)
        if isinstance(value, dict):
            nested = " ".join(
                strip_html(v) for v in value.values() if isinstance(v, str) and strip_html(v)
            ).strip()
            if nested:
                return nested
    return ""


def pick_list(obj: Dict[str, Any], *keys: str) -> List[str]:
    for key in keys:
        value = obj.get(key)
        if isinstance(value, list):
            out = []
            for item in value:
                if isinstance(item, str) and item.strip():
                    out.append(strip_html(item))
                elif isinstance(item, dict):
                    txt = " — ".join(
                        strip_html(v) for v in item.values() if isinstance(v, str) and strip_html(v)
                    ).strip(" —")
                    if txt:
                        out.append(txt)
            if out:
                return out
        if isinstance(value, str) and value.strip():
            return [strip_html(value)]
    return []


def pick_dict(obj: Dict[str, Any], *keys: str) -> Dict[str, Any]:
    for key in keys:
        value = obj.get(key)
        if isinstance(value, dict) and value:
            return value
    return {}


# ────────────────────────────────────────────────────────────────────────────────
# DB access
# ────────────────────────────────────────────────────────────────────────────────

def load_latest_runs(tickers: List[str], period_ref: str) -> pd.DataFrame:
    """Fetches the most recent patch6_runs row per (ticker, period_ref)."""
    tickers = [str(t).strip().upper() for t in (tickers or []) if str(t).strip()]
    if not tickers:
        return pd.DataFrame()

    engine = get_engine()
    q = text(
        """
        WITH ranked AS (
            SELECT
                ticker,
                period_ref,
                created_at,
                perspectiva_compra,
                resumo,
                result_json,
                ROW_NUMBER() OVER (
                    PARTITION BY ticker, period_ref
                    ORDER BY created_at DESC
                ) AS rn
            FROM public.patch6_runs
            WHERE period_ref = :pr AND ticker = ANY(:tks)
        )
        SELECT ticker, period_ref, created_at, perspectiva_compra, resumo, result_json
        FROM ranked
        WHERE rn = 1
        ORDER BY ticker ASC
        """
    )
    with engine.connect() as conn:
        return pd.read_sql_query(q, conn, params={"pr": str(period_ref).strip(), "tks": tickers})


# ────────────────────────────────────────────────────────────────────────────────
# Parsing: row + temporal → CompanyAnalysis
# ────────────────────────────────────────────────────────────────────────────────

def _resolve_company(row: Any, temporal: Optional[TemporalData] = None) -> CompanyAnalysis:
    result_obj = as_result_obj(getattr(row, "result_json", None))
    ticker = str(getattr(row, "ticker", "")).strip().upper()

    # 1. Validate schema
    validation = validate_result(result_obj)

    # 2. Hybrid score with hysteresis
    prev_score = getattr(temporal, "prev_score", None) if temporal else None
    prev_perspectiva = getattr(temporal, "prev_perspectiva", None) if temporal else None
    score_result = compute_hybrid_score(
        result_obj,
        validation,
        prev_score=prev_score,
        prev_perspectiva=prev_perspectiva,
    )

    # 3. Narrative sections
    tese = pick_text(result_obj, "tese_sintese", "tese_final", "resumo", "tese") \
        or strip_html(getattr(row, "resumo", ""))
    evolucao = pick_dict(result_obj, "evolucao_estrategica", "evolucao_temporal")
    consistencia = pick_dict(result_obj, "consistencia_discurso", "consistencia_narrativa")
    execucao = pick_dict(result_obj, "execucao_vs_promessa")
    qualidade_narrativa = pick_dict(result_obj, "qualidade_narrativa")
    strategy_detector = pick_dict(result_obj, "strategy_detector")

    leitura = pick_text(result_obj, "leitura_direcionalidade", "direcionalidade")
    if not leitura:
        leitura = str(getattr(row, "perspectiva_compra", "") or "").strip().lower()

    contradicoes = pick_list(consistencia, "contradicoes", "contradicoes_ou_ruidos")
    sinais_ruido = pick_list(qualidade_narrativa, "sinais_de_ruido")

    riscos_list = pick_list(result_obj, "riscos_identificados", "riscos")
    catalisadores_list = pick_list(result_obj, "catalisadores", "gatilhos_futuros")

    # 4. Evidence enriched with topic tags
    raw_evidencias = (
        result_obj.get("evidencias")
        if isinstance(result_obj.get("evidencias"), list)
        else []
    )
    evidencias = enrich_evidencias_with_topics(raw_evidencias)

    # 5. Temporal signals
    exec_trend = getattr(temporal, "execution_trend", "—") if temporal else "—"
    narr_shift = getattr(temporal, "narrative_shift", "—") if temporal else "—"

    # 6. Perspectiva — use hybrid score result (already hysteresis-adjusted)
    perspectiva = score_result.perspectiva_compra or \
        str(getattr(row, "perspectiva_compra", "") or "").strip().lower()

    confianca = safe_float(result_obj.get("confianca_analise"), 0.0)

    sd = strategy_detector or {}
    temporal_years = sd.get("coverage_years") if isinstance(sd.get("coverage_years"), list) else []

    # ── v3: memory ──────────────────────────────────────────────────────────────
    from core.patch6_memory import TickerMemory, build_ticker_memory
    memory = TickerMemory(ticker=ticker)
    try:
        memory = build_ticker_memory(ticker, temporal, current_result_json=result_obj)
    except Exception:
        pass

    # ── v3: regime ──────────────────────────────────────────────────────────────
    from core.patch6_regime import RegimeChangeResult, infer_regime
    regime = RegimeChangeResult()
    try:
        regime = infer_regime(
            current_result_json=result_obj,
            temporal=temporal,
            execution_trend=exec_trend,
            perspectiva=perspectiva,
            narrative_dispersion=score_result.narrative_dispersion_score,
        )
    except Exception:
        pass

    # ── v3: forward signal ──────────────────────────────────────────────────────
    from core.patch6_forward import ForwardSignalResult, compute_forward_signal
    forward = ForwardSignalResult()
    try:
        forward = compute_forward_signal(
            score=score_result.final_score,
            execution_trend=exec_trend,
            robustez=score_result.robustez_qualitativa,
            narrative_dispersion=score_result.narrative_dispersion_score,
            riscos=riscos_list,
            catalisadores=catalisadores_list,
            confianca=confianca,
            regime=regime.current_regime,
            persistent_risks_count=len(memory.persistent_risks),
            persistent_catalysts_count=len(memory.persistent_catalysts),
            temporal_years_count=len(temporal_years),
        )
    except Exception:
        pass

    # ── v3: priority ────────────────────────────────────────────────────────────
    from core.patch6_priority import PriorityResult, compute_priority
    priority = PriorityResult()
    try:
        priority = compute_priority(
            execution_trend=exec_trend,
            narrative_shift=narr_shift,
            robustez=score_result.robustez_qualitativa,
            narrative_dispersion=score_result.narrative_dispersion_score,
            regime_change_intensity=regime.change_intensity,
            forward_direction=forward.direction,
            persistent_risks_count=len(memory.persistent_risks),
            schema_score=validation.schema_score,
            perspectiva=perspectiva,
            confianca=confianca,
        )
    except Exception:
        pass

    # ── v4: derived decision fields (runtime, no DB) ───────────────────────────
    _dec_score = _compute_decision_score(
        perspectiva=perspectiva,
        forward_direction=forward.direction,
        execution_trend=exec_trend,
    )

    # CompanyAnalysis é construído incompleto por um instante para poder chamar
    # _compute_risk_rank, que precisa do objeto com riscos e persistent_risks.
    _company = CompanyAnalysis(
        ticker=ticker,
        period_ref=str(getattr(row, "period_ref", "")),
        created_at=getattr(row, "created_at", None),
        perspectiva_compra=perspectiva,
        raw=result_obj,
        tese=tese,
        leitura=leitura,
        consideracoes=pick_text(result_obj, "consideracoes_llm"),
        evolucao=evolucao,
        consistencia=consistencia,
        execucao=execucao,
        qualidade_narrativa=qualidade_narrativa,
        strategy_detector=strategy_detector,
        riscos=riscos_list,
        catalisadores=catalisadores_list,
        monitorar=pick_list(result_obj, "o_que_monitorar"),
        mudancas=pick_list(result_obj, "mudancas_estrategicas"),
        pontos_chave=pick_list(result_obj, "pontos_chave"),
        contradicoes=contradicoes,
        sinais_ruido=sinais_ruido,
        evidencias=evidencias,
        papel_estrategico=pick_text(result_obj, "papel_estrategico"),
        sensibilidades_macro=pick_list(result_obj, "sensibilidades_macro"),
        fragilidade_regime_atual=pick_text(result_obj, "fragilidade_regime_atual"),
        dependencias_cenario=pick_list(result_obj, "dependencias_cenario"),
        alocacao_sugerida_faixa=pick_text(result_obj, "alocacao_sugerida_faixa"),
        racional_alocacao=pick_text(result_obj, "racional_alocacao"),
        # Scores — hybrid
        score_qualitativo=score_result.final_score,
        confianca=confianca,
        score_source=score_result.score_source,
        # v2
        robustez_qualitativa=score_result.robustez_qualitativa,
        narrative_dispersion_score=score_result.narrative_dispersion_score,
        execution_trend=exec_trend,
        narrative_shift=narr_shift,
        schema_score=validation.schema_score,
        validation_warnings=validation.warnings,
        # v3 — memory
        memory_summary=memory.memory_summary,
        recurring_promises=memory.recurring_promises,
        delivered_promises=memory.delivered_promises,
        persistent_risks=memory.persistent_risks,
        persistent_catalysts=memory.persistent_catalysts,
        # v3 — regime
        current_regime=regime.current_regime,
        previous_regime=regime.previous_regime,
        regime_change_intensity=regime.change_intensity,
        regime_change_explanation=regime.explanation,
        # v3 — priority
        attention_score=priority.attention_score,
        attention_level=priority.attention_level,
        recommended_action=priority.recommended_action,
        attention_drivers=priority.drivers,
        # v3 — forward
        forward_score=forward.forward_score,
        forward_direction=forward.direction,
        forward_confidence=forward.confidence_forward,
        forward_drivers=forward.key_drivers,
        # v4 — derived decision fields
        decision_score=_dec_score,
        decision_label=_DECISION_LABELS.get(_dec_score, "—"),
        risk_rank=[],   # preenchido abaixo após objeto criado
    )
    _company.risk_rank = _compute_risk_rank(_company)
    return _company


# ────────────────────────────────────────────────────────────────────────────────
# Stats
# ────────────────────────────────────────────────────────────────────────────────

def _compute_stats(companies: Dict[str, CompanyAnalysis]) -> PortfolioStats:
    stats = PortfolioStats()
    for company in companies.values():
        p = (company.perspectiva_compra or "").strip().lower()
        if p == "forte":
            stats.fortes += 1
        elif p == "moderada":
            stats.moderadas += 1
        elif p == "fraca":
            stats.fracas += 1
        else:
            stats.desconhecidas += 1
    return stats


# ────────────────────────────────────────────────────────────────────────────────
# Allocation — v2: incorporates robustez_qualitativa
# ────────────────────────────────────────────────────────────────────────────────

def _allocation_base(company: CompanyAnalysis) -> float:
    score = company.score_qualitativo
    conf = company.confianca
    evid = len(company.evidencias)
    execucao = pick_text(company.execucao, "avaliacao_execucao").lower()
    robustez = company.robustez_qualitativa   # v2: 0-1

    mult = 1.0
    p = company.perspectiva_compra.strip().lower()
    if p == "forte":
        mult *= 1.20
    elif p == "moderada":
        mult *= 1.00
    elif p == "fraca":
        mult *= 0.72

    if "forte" in execucao:
        mult *= 1.08
    elif "fraca" in execucao or "inconsistente" in execucao:
        mult *= 0.84

    # v2: reward robustez, penalize high dispersion
    robustez_mult = 0.85 + 0.30 * robustez          # 0.85..1.15

    # v6: multiply by quantitative snapshot multiplier (1.0 if no snapshot data)
    quant_mult = company.quant_allocation_multiplier  # set by enrich_quant_snapshot()

    return max(
        0.5,
        (score / 100.0) * (0.65 + conf) * mult * robustez_mult
        * (1.0 + min(evid, 14) / 40.0)
        * quant_mult,
    )


def _normalize_allocations(rows: List[AllocationRow]) -> List[AllocationRow]:
    total = sum(max(0.0, r.raw_weight) for r in rows)
    if total <= 0:
        n = max(1, len(rows))
        for r in rows:
            r.allocation_pct = round(100.0 / n, 2)
        return rows
    acc = 0.0
    for r in rows:
        pct = round((max(0.0, r.raw_weight) / total) * 100.0, 2)
        r.allocation_pct = pct
        acc += pct
    if rows:
        rows[-1].allocation_pct = round(rows[-1].allocation_pct + (100.0 - acc), 2)
    return rows


def _build_allocation_rows(companies: Dict[str, CompanyAnalysis]) -> List[AllocationRow]:
    rows = [
        AllocationRow(
            ticker=company.ticker,
            perspectiva=company.perspectiva_compra.strip().lower(),
            raw_weight=_allocation_base(company),
            allocation_pct=0.0,
            score=company.score_qualitativo,
            confianca=company.confianca,
            robustez=company.robustez_qualitativa,
            execution_trend=company.execution_trend,
        )
        for company in companies.values()
    ]
    rows = sorted(_normalize_allocations(rows), key=lambda x: (-x.allocation_pct, x.ticker))
    return rows


# ────────────────────────────────────────────────────────────────────────────────
# Context text for LLM
# ────────────────────────────────────────────────────────────────────────────────

def _fmt_score(value: int) -> str:
    if value <= 0:
        return "—"
    return f"{max(0, min(100, value))}/100"


def _company_context_line(company: CompanyAnalysis) -> str:
    tese = company.tese or "sem tese consolidada"
    historico = pick_text(company.evolucao, "historico")
    atual = pick_text(company.evolucao, "fase_atual")
    execucao_txt = pick_text(company.execucao, "analise")
    riscos = "; ".join(company.riscos[:3])
    catalisadores = "; ".join(company.catalisadores[:3])
    score = _fmt_score(company.score_qualitativo)
    years = company.strategy_detector.get("coverage_years", []) \
        if isinstance(company.strategy_detector.get("coverage_years"), list) else []
    years_txt = ",".join([str(y) for y in years[:4]]) if years else ""
    return (
        f"- {company.ticker}: perspectiva={company.perspectiva_compra}; score={score}; "
        f"forward={company.forward_score}({company.forward_direction}); "
        f"prioridade={company.attention_level}; regime={company.current_regime}; "
        f"robustez={company.robustez_qualitativa:.2f}; trend={company.execution_trend}; "
        f"shift={company.narrative_shift}; tese={tese}; "
        f"historico={historico}; fase_atual={atual}; execucao={execucao_txt}; "
        f"riscos={riscos}; catalisadores={catalisadores}; cobertura_temporal={years_txt}"
    )


# ────────────────────────────────────────────────────────────────────────────────
# v4 — Derived decision fields (runtime, no DB dependency)
# ────────────────────────────────────────────────────────────────────────────────

def _compute_decision_score(
    perspectiva: str,
    forward_direction: str,
    execution_trend: str,
) -> int:
    """Escala discreta [-2, +2] derivada de três sinais qualitativos.

    base:     forte=+1 | moderada=0 | fraca=-1
    fwd_adj:  melhorando=+1 | deteriorando=-1
    exec_adj: reforço quando exec_trend aponta na mesma direção que base (±1)
    resultado: clamped em [-2, +2]
    """
    p = (perspectiva or "").strip().lower()
    base = {"forte": 1, "moderada": 0, "fraca": -1}.get(p, 0)

    fwd = (forward_direction or "—").strip().lower()
    fwd_adj = 1 if fwd == "melhorando" else (-1 if fwd == "deteriorando" else 0)

    exec_t = (execution_trend or "—").strip().lower()
    # exec_trend só reforça quando alinha com base (evita dupla penalidade cruzada)
    exec_adj = 0
    if exec_t == "melhorando" and base >= 0:
        exec_adj = 1
    elif exec_t == "deteriorando" and base <= 0:
        exec_adj = -1

    return max(-2, min(2, base + fwd_adj + exec_adj))


_DECISION_LABELS = {
    2:  "aumentar",
    1:  "aumentar",
    0:  "manter",
    -1: "revisar",
    -2: "reduzir",
}


def _compute_risk_rank(company: "CompanyAnalysis") -> List[str]:
    """Lista ordenada por prioridade: persistent_risks → riscos → fragilidade.

    Sem duplicatas, limitado a 5 itens.
    """
    seen: set = set()
    ranked: List[str] = []

    for src in (company.persistent_risks, company.riscos):
        for r in src:
            r_c = r.strip()
            if r_c and r_c not in seen:
                seen.add(r_c)
                ranked.append(r_c)

    if company.fragilidade_regime_atual:
        f = company.fragilidade_regime_atual.strip()
        if f and f not in seen:
            ranked.append(f)

    return ranked[:5]


def _compute_portfolio_trend(
    companies: Dict[str, "CompanyAnalysis"],
    stats: "PortfolioStats",
) -> Dict[str, str]:
    """Dict curto com tendências do portfólio por dimensão.

    Derivado inteiramente dos campos já computados em CompanyAnalysis.
    Chaves: qualidade | execucao | governanca | capital
    """
    total = max(stats.total, 1)
    n = max(len(companies), 1)

    # qualidade — via PortfolioStats
    qualidade = stats.label_qualidade().lower()   # "alta" | "moderada" | "baixa"

    # execucao — proporção de execution_trend
    exec_mel = sum(1 for c in companies.values() if c.execution_trend == "melhorando")
    exec_det = sum(1 for c in companies.values() if c.execution_trend == "deteriorando")
    if exec_mel > exec_det and exec_mel >= n * 0.35:
        execucao = "melhorando"
    elif exec_det > exec_mel and exec_det >= n * 0.35:
        execucao = "deteriorando"
    else:
        execucao = "estável"

    # governança — baseado em narrative_shift + regime_change_intensity
    gov_issues = sum(
        1 for c in companies.values()
        if c.narrative_shift == "significativo"
        or c.regime_change_intensity == "significativo"
    )
    if gov_issues == 0:
        governanca = "estável"
    elif gov_issues <= max(1, round(n * 0.25)):
        governanca = "atenção"
    else:
        governanca = "deteriorando"

    # capital — baseado em forward_direction do portfólio
    fwd_mel = sum(1 for c in companies.values() if c.forward_direction == "melhorando")
    fwd_det = sum(1 for c in companies.values() if c.forward_direction == "deteriorando")
    if fwd_mel >= n * 0.50:
        capital = "favorável"
    elif fwd_det >= n * 0.40:
        capital = "cauteloso"
    else:
        capital = "neutro"

    return {
        "qualidade":   qualidade,
        "execucao":    execucao,
        "governanca":  governanca,
        "capital":     capital,
    }


# ────────────────────────────────────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────────────────────────────────────

def build_portfolio_analysis(
    tickers: List[str],
    period_ref: str,
    n_temporal_periods: int = 4,
) -> Optional[PortfolioAnalysis]:
    """
    Fetches DB data and computes the full PortfolioAnalysis for a period.

    v2 pipeline:
      1. Load current-period runs
      2. Load historical runs (batch) for temporal analysis
      3. Validate + score each company with hysteresis
      4. Aggregate stats and allocations

    Returns None if no data is available.
    """
    df = load_latest_runs(tickers, period_ref)
    if df is None or df.empty:
        return None

    # Batch-load temporal data for all tickers in a single query
    temporal_map: Dict[str, TemporalData] = {}
    try:
        temporal_map = load_temporal_batch(tickers, period_ref, n_periods=n_temporal_periods)
    except Exception:
        pass   # temporal data is enrichment — failure must not break the report

    companies: Dict[str, CompanyAnalysis] = {}
    for row in df.itertuples(index=False):
        tk = str(getattr(row, "ticker", "")).strip().upper()
        temporal = temporal_map.get(tk)
        company = _resolve_company(row, temporal=temporal)
        companies[company.ticker] = company

    stats = _compute_stats(companies)
    coverage_total = max(len([t for t in tickers if str(t).strip()]), 1)

    confidence_values = [c.confianca for c in companies.values() if c.confianca > 0]
    score_values = [c.score_qualitativo for c in companies.values() if c.score_qualitativo > 0]
    temporal_covered = sum(
        1 for c in companies.values()
        if isinstance(c.strategy_detector.get("coverage_years"), list)
        and c.strategy_detector["coverage_years"]
    )

    confianca_media = sum(confidence_values) / len(confidence_values) if confidence_values else 0.0
    score_medio = round(sum(score_values) / len(score_values)) if score_values else 0

    allocation_rows = _build_allocation_rows(companies)
    contexto_portfolio = "\n".join(_company_context_line(c) for c in companies.values())

    # v3 portfolio aggregates
    priority_ranking = sorted(
        companies.keys(),
        key=lambda tk: companies[tk].attention_score,
        reverse=True,
    )
    alta_prioridade = [tk for tk in companies if companies[tk].attention_level == "alta"]
    forward_values = [c.forward_score for c in companies.values() if c.forward_score > 0]
    forward_score_medio = round(sum(forward_values) / len(forward_values)) if forward_values else 0

    regime_changes = [
        f"{c.ticker}: {c.previous_regime} → {c.current_regime}"
        for c in companies.values()
        if c.regime_change_intensity in ("significativo", "moderado")
        and c.current_regime not in ("—", "indefinido")
    ]
    regime_summary = "; ".join(regime_changes) if regime_changes else ""

    # v4: portfolio_trend derivado dos companies já computados
    _ptx = _compute_portfolio_trend(companies, stats)

    return PortfolioAnalysis(
        period_ref=period_ref,
        tickers_requested=tickers,
        stats=stats,
        companies=companies,
        allocation_rows=allocation_rows,
        confianca_media=confianca_media,
        score_medio=score_medio,
        cobertura=f"{stats.total}/{coverage_total}",
        temporal_covered=temporal_covered,
        contexto_portfolio=contexto_portfolio,
        # v3
        priority_ranking=priority_ranking,
        alta_prioridade_count=len(alta_prioridade),
        forward_score_medio=forward_score_medio,
        regime_summary=regime_summary,
        # v4
        portfolio_trend=_ptx,
    )


# ────────────────────────────────────────────────────────────────────────────────
# v5 — Macro-impact enrichment (runtime, never stored)
# ────────────────────────────────────────────────────────────────────────────────
#
# Regras de impacto macro por role_hint × tendência.
# Chave: (role_hint, macro_factor, trend_direction)  →  impact (+1 / -1)
# +1 = favorecido, -1 = pressionado.
# Fatores ausentes ou sem regra → impacto zero (neutro).
#
_MACRO_IMPACT_RULES: Dict[tuple, int] = {
    # ── Selic subindo ────────────────────────────────────────────────────────
    ("nucleo_renda",          "selic", "subindo"):  +1,   # banco: spread ↑
    ("qualidade_financeira",  "selic", "subindo"):  +1,
    ("defensivo_renda",       "selic", "subindo"):  +1,   # seguradora: float ↑
    ("fii_papel",             "selic", "subindo"):  +1,   # CRI indexado CDI
    ("fii_logistica",         "selic", "subindo"):  -1,   # valuation ↓
    ("fii_shopping",          "selic", "subindo"):  -1,
    ("fii_lajes",             "selic", "subindo"):  -1,
    ("smallcap_domestica",    "selic", "subindo"):  -1,   # custo capital ↑
    ("industria_ciclica",     "selic", "subindo"):  -1,
    ("growth_domestico",      "selic", "subindo"):  -1,
    ("utility_defensiva",     "selic", "subindo"):  -1,   # dívida mais cara
    ("infra_mercado_capitais","selic", "subindo"):  -1,   # atividade mkt ↓
    # ── Selic caindo ─────────────────────────────────────────────────────────
    ("nucleo_renda",          "selic", "caindo"):   -1,
    ("qualidade_financeira",  "selic", "caindo"):   -1,
    ("fii_logistica",         "selic", "caindo"):   +1,
    ("fii_shopping",          "selic", "caindo"):   +1,
    ("fii_lajes",             "selic", "caindo"):   +1,
    ("smallcap_domestica",    "selic", "caindo"):   +1,
    ("industria_ciclica",     "selic", "caindo"):   +1,
    ("infra_mercado_capitais","selic", "caindo"):   +1,
    # ── Câmbio subindo (BRL depreciando) ─────────────────────────────────────
    ("exportadora_commodity", "cambio", "subindo"):  +1,
    ("holding_commodity",     "cambio", "subindo"):  +1,
    ("ciclico_renda",         "cambio", "subindo"):  +1,  # Petro
    ("exportadora_alimentos", "cambio", "subindo"):  +1,
    ("industria_exportadora", "cambio", "subindo"):  +1,
    ("qualidade_crescimento", "cambio", "subindo"):  +1,  # WEG mix export
    # ── Câmbio caindo (BRL apreciando) ───────────────────────────────────────
    ("exportadora_commodity", "cambio", "caindo"):   -1,
    ("holding_commodity",     "cambio", "caindo"):   -1,
    ("ciclico_renda",         "cambio", "caindo"):   -1,
    ("exportadora_alimentos", "cambio", "caindo"):   -1,
    ("industria_exportadora", "cambio", "caindo"):   -1,
}

# Rótulos amigáveis para os fatores macro
_FACTOR_LABELS: Dict[str, Dict[str, str]] = {
    "selic":  {"subindo": "Selic ↑",  "caindo": "Selic ↓",  "estavel": "Selic →"},
    "cambio": {"subindo": "R$ ↓",     "caindo": "R$ ↑",     "estavel": "R$ →"},
    "ipca":   {"subindo": "IPCA ↑",   "caindo": "IPCA ↓",   "estavel": "IPCA →"},
}

# Rótulos de impacto por role_hint quando o impacto é conhecido
_IMPACT_DETAIL: Dict[tuple, str] = {
    ("nucleo_renda",          "selic", +1): "spread bancário favorecido",
    ("qualidade_financeira",  "selic", +1): "spread bancário favorecido",
    ("defensivo_renda",       "selic", +1): "float de prêmios valorizado",
    ("fii_papel",             "selic", +1): "indexação a CDI favorece rendimentos",
    ("fii_logistica",         "selic", -1): "valuation pressionado por juros altos",
    ("fii_shopping",          "selic", -1): "custo de capital ↑, demanda consumo ↓",
    ("fii_lajes",             "selic", -1): "valuation pressionado, vacância sensível",
    ("smallcap_domestica",    "selic", -1): "custo de capital mais alto",
    ("industria_ciclica",     "selic", -1): "investimento e crédito retraídos",
    ("utility_defensiva",     "selic", -1): "dívida indexada mais cara",
    ("infra_mercado_capitais","selic", -1): "volume de mercado tende a cair",
    ("exportadora_commodity", "cambio", +1): "receita em USD convertida a BRL ↑",
    ("holding_commodity",     "cambio", +1): "exposição indireta a exportadoras",
    ("ciclico_renda",         "cambio", +1): "receita dolarizada de petróleo ↑",
    ("exportadora_alimentos", "cambio", +1): "proteína e grãos em USD ↑",
    ("industria_exportadora", "cambio", +1): "receita exportadora ampliada",
    ("qualidade_crescimento", "cambio", +1): "mix internacional valorizado",
    ("exportadora_commodity", "cambio", -1): "receita dolarizada comprimida",
    ("ciclico_renda",         "cambio", -1): "receita petróleo em BRL ↓",
    ("exportadora_alimentos", "cambio", -1): "preço de exportação comprimido",
}


def _normalize_macro_trend(value: str) -> str:
    v = (value or "").strip().lower()
    if v in ("alta", "subindo"):
        return "subindo"
    if v in ("queda", "caindo"):
        return "caindo"
    if v in ("estavel", "estável", "neutro"):
        return "estavel"
    return v


def _fmt_macro_num(value: Any, prefix: str = "", suffix: str = "") -> str:
    try:
        if value is None:
            return ""
        return f"{prefix}{float(value):.2f}{suffix}"
    except Exception:
        return ""


def _classify_macro_exposure(
    company: "CompanyAnalysis",
    trends: Dict[str, Any],
    summary: Dict[str, Any],
) -> tuple:
    """Retorna (label, tone, detail) para macro_exposure de uma empresa.

    Usa role_hint do asset_macro_profile + tendências macro reais.
    Fallback: sensibilidades_macro do LLM + fragilidade_regime_atual.
    """
    try:
        from core.asset_macro_profile import get_asset_macro_profile
        profile = get_asset_macro_profile(company.ticker)
    except Exception:
        profile = {}

    role = (profile.get("role_hint") or "indefinido").strip().lower()
    sensitivities_profile = list(profile.get("macro_sensitivities") or [])
    sensitivities_llm = list(company.sensibilidades_macro or [])

    # Merge sensitivities (profile takes priority)
    merged = []
    for s in sensitivities_profile + sensitivities_llm:
        k = s.strip().lower()
        if k and k not in merged:
            merged.append(k)

    # Extract trends
    selic_trend = _normalize_macro_trend((trends.get("selic") or {}).get("trend") or "")
    cambio_trend = _normalize_macro_trend((trends.get("cambio") or {}).get("trend") or "")

    # Collect impacts
    positive: List[str] = []
    negative: List[str] = []

    # Check selic
    if any(s in merged for s in ("juros", "credito")):
        impact = _MACRO_IMPACT_RULES.get((role, "selic", selic_trend), 0)
        if impact != 0:
            factor_lbl = _FACTOR_LABELS.get("selic", {}).get(selic_trend, "Selic")
            detail_key = (role, "selic", impact)
            detail = _IMPACT_DETAIL.get(detail_key, factor_lbl)
            selic_val = summary.get("selic_current")
            val_str = f" ({selic_val}%)" if selic_val else ""
            entry = f"{factor_lbl}{val_str}: {detail}"
            (positive if impact > 0 else negative).append(entry)

    # Check câmbio
    if any(s in merged for s in ("cambio",)):
        impact = _MACRO_IMPACT_RULES.get((role, "cambio", cambio_trend), 0)
        if impact != 0:
            factor_lbl = _FACTOR_LABELS.get("cambio", {}).get(cambio_trend, "Câmbio")
            detail_key = (role, "cambio", impact)
            detail = _IMPACT_DETAIL.get(detail_key, factor_lbl)
            cambio_val = summary.get("cambio_current")
            val_str = f" (R$ {cambio_val})" if cambio_val else ""
            entry = f"{factor_lbl}{val_str}: {detail}"
            (positive if impact > 0 else negative).append(entry)

    # Fallback: usar fragilidade_regime_atual como sinal negativo se nada foi detectado
    if not positive and not negative:
        frag = (company.fragilidade_regime_atual or "").strip()
        if frag:
            frag_lower = frag.lower()
            neg_kws = ("endividamento", "alavancagem", "custo financeiro", "pressionad",
                       "vulnerável", "deteriora", "risco fiscal", "exposição negativa")
            if any(kw in frag_lower for kw in neg_kws):
                negative.append(frag[:120] if len(frag) > 120 else frag)
            else:
                # fragilidade exists but not clearly negative — treat as attention
                pass

    # Classify
    if positive and not negative:
        return ("Favorecido", "good", "; ".join(positive))
    elif negative and not positive:
        return ("Pressionado", "bad", "; ".join(negative))
    elif positive and negative:
        return ("Misto", "warn", f"{positive[0]}. Porém: {negative[0]}")
    else:
        return ("Neutro", "neutral", "")


def _build_macro_narrative(
    analysis: "PortfolioAnalysis",
    trends: Dict[str, Any],
    summary: Dict[str, Any],
) -> str:
    """Gera narrativa macro-portfólio conectando números reais aos ativos."""
    selic_val = summary.get("selic_current")
    selic_trend = _normalize_macro_trend((trends.get("selic") or {}).get("trend") or "")
    ipca_val = summary.get("ipca_12m_current")
    ipca_trend = _normalize_macro_trend((trends.get("ipca_12m") or {}).get("trend") or "")
    cambio_val = summary.get("cambio_current")
    cambio_trend = _normalize_macro_trend((trends.get("cambio") or {}).get("trend") or "")

    restrictive = (selic_trend == "subindo") or (selic_val is not None and float(selic_val) > 12.0)

    arrow_map = {"subindo": "↑", "caindo": "↓", "estavel": "→"}
    parts = []
    if selic_val is not None:
        parts.append(f"Selic {_fmt_macro_num(selic_val, suffix='%')} {arrow_map.get(selic_trend, '→')}")
    if ipca_val is not None:
        parts.append(f"IPCA 12m {_fmt_macro_num(ipca_val, suffix='%')} {arrow_map.get(ipca_trend, '→')}")
    if cambio_val is not None:
        parts.append(f"Câmbio R$ {_fmt_macro_num(cambio_val)} {arrow_map.get(cambio_trend, '→')}")

    env_label = "restritivo" if restrictive else ("expansivo" if selic_trend == "caindo" else "neutro")
    header = f"Ambiente {env_label}"
    if parts:
        header += f" ({', '.join(parts)})"

    favorecidos = [
        c.ticker for c in analysis.companies.values()
        if c.macro_exposure == "Favorecido"
    ]
    pressionados = [
        c.ticker for c in analysis.companies.values()
        if c.macro_exposure == "Pressionado"
    ]
    mistos = [
        c.ticker for c in analysis.companies.values()
        if c.macro_exposure == "Misto"
    ]

    segments = [header + "."]
    if favorecidos:
        segments.append(f"Favorecidos: {', '.join(sorted(favorecidos))}.")
    if pressionados:
        segments.append(f"Pressionados: {', '.join(sorted(pressionados))}.")
    if mistos:
        segments.append(f"Exposição mista: {', '.join(sorted(mistos))}.")

    return " ".join(segments)


def enrich_macro_impact(
    analysis: "PortfolioAnalysis",
    macro_context: Dict[str, Any],
) -> None:
    """v5 — enriquece PortfolioAnalysis com campos de impacto macro.

    Muta o objeto in-place (padrão v4).
    Seguro para chamar com macro_context vazio — nada será alterado.
    """
    if not macro_context:
        return

    trends = macro_context.get("trends", {}) or {}
    summary = macro_context.get("macro_summary", {}) or {}

    if not trends and not summary:
        return

    # Enriquecer cada empresa
    for company in analysis.companies.values():
        label, tone, detail = _classify_macro_exposure(company, trends, summary)
        company.macro_exposure = label
        company.macro_exposure_tone = tone
        company.macro_exposure_detail = detail

    # Narrativa macro consolidada do portfólio
    analysis.macro_narrative = _build_macro_narrative(analysis, trends, summary)


# ────────────────────────────────────────────────────────────────────────────────
# v6 — Quantitative snapshot enrichment (runtime, never stored)
# ────────────────────────────────────────────────────────────────────────────────

def enrich_quant_snapshot(
    analysis: "PortfolioAnalysis",
    snapshot_map: Dict[str, Any],
) -> None:
    """v6 — Enriquece PortfolioAnalysis com dados de portfolio_snapshot_analysis.

    Muta o objeto in-place. Seguro: degrada silenciosamente se snapshot_map vazio.

    Atualiza por empresa:
      - quant_classe, quant_rank_geral, quant_score_final
      - quant_context_text (bloco para o LLM)
      - quant_convergence (diagnóstico de convergência quali/quanti)
      - quant_allocation_multiplier (ajuste de alocação)

    Atualiza no portfólio:
      - quant_portfolio_summary (resumo agregado para o LLM)
    """
    if not snapshot_map:
        return

    try:
        from core.patch6_snapshot_integration import (
            build_snapshot_quant_context,
            build_portfolio_snapshot_quant_summary,
            compute_quant_allocation_multiplier,
            assess_quant_quali_convergence,
            _safe_float,
        )
    except Exception:
        return  # integration module unavailable — skip silently

    for company in analysis.companies.values():
        tk = company.ticker
        row = snapshot_map.get(tk) or {}

        # Basic quant fields
        company.quant_classe      = (row.get("classe_forca") or "").strip().upper()
        company.quant_rank_geral  = int(row.get("rank_geral") or 0)
        company.quant_score_final = _safe_float(row.get("score_final")) or 0.0

        # Context text for LLM
        company.quant_context_text = build_snapshot_quant_context(tk, row)

        # Convergence assessment
        company.quant_convergence = assess_quant_quali_convergence(
            ticker=tk,
            snapshot_row=row,
            perspectiva=company.perspectiva_compra,
            execution_trend=company.execution_trend,
            narrative_shift=company.narrative_shift,
            forward_direction=company.forward_direction,
        )

        # Allocation multiplier (applied in _allocation_base)
        company.quant_allocation_multiplier = compute_quant_allocation_multiplier(row)

    # Portfolio-level quantitative summary
    analysis.quant_portfolio_summary = build_portfolio_snapshot_quant_summary(snapshot_map)
