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

    return CompanyAnalysis(
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
    )


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

    return max(
        0.5,
        (score / 100.0) * (0.65 + conf) * mult * robustez_mult
        * (1.0 + min(evid, 14) / 40.0),
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
    )
