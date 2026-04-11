"""core/patch6_report.py

Renderização Streamlit do Patch6 (relatório estilo casa de análise).

Responsabilidades DESTA camada:
  - Cards, badges, tabelas, markdown, CSS
  - Orquestrar patch6_analysis → patch6_service → render

NÃO faz:
  - Acesso ao banco de dados
  - Cálculos de score / alocação
  - Chamadas LLM (delegado a patch6_service)
"""

from __future__ import annotations

import html
import re
from statistics import mean
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from core.patch6_analysis import (
    build_portfolio_analysis,
    pick_text,
    safe_float,
    safe_int,
    strip_html,
)
from core.patch6_schema import (
    AllocationRow,
    CompanyAnalysis,
    PortfolioAnalysis,
    PortfolioStats,
)
from core.patch6_service import run_portfolio_llm_report, safe_call_llm
from core.portfolio_snapshot_store import get_latest_snapshot
from core.portfolio_snapshot_analysis_store import load_snapshot_analysis
from core.macro_context import load_latest_macro_context
from core.market_context import build_market_context


# ────────────────────────────────────────────────────────────────────────────────
# Formatting helpers
# ────────────────────────────────────────────────────────────────────────────────

def _esc(value: Any) -> str:
    return html.escape(strip_html(value))


def _fmt_confidence(value: float) -> str:
    if value <= 0:
        return "—"
    pct = round(max(0.0, min(1.0, value)) * 100)
    return f"{pct}%"


def _fmt_score(value: int) -> str:
    if value <= 0:
        return "—"
    return f"{max(0, min(100, value))}/100"


def _badge(texto: str, tone: str = "neutral") -> str:
    tone_map = {
        "good": "#0ea5e9",
        "warn": "#f59e0b",
        "bad": "#ef4444",
        "neutral": "#94a3b8",
    }
    color = tone_map.get(tone, "#94a3b8")
    return (
        f"<span style='display:inline-block;padding:2px 10px;border-radius:999px;"
        f"border:1px solid {color};color:{color};font-weight:600;font-size:12px'>{texto}</span>"
    )


def _tone_from_perspectiva(p: str) -> str:
    p = (p or "").strip().lower()
    if p == "forte":
        return "good"
    if p == "moderada":
        return "warn"
    if p == "fraca":
        return "bad"
    return "neutral"


def _box_html(text: str) -> str:
    return (
        "<div style=\"border:1px solid rgba(255,255,255,0.08);"
        "background:rgba(255,255,255,0.03);border-radius:14px;"
        "padding:14px 16px;box-shadow:0 10px 24px rgba(0,0,0,0.18);"
        "margin-top:8px;line-height:1.5;\">"
        + _esc(text).replace("\n", "<br/>")
        + "</div>"
    )


_ACTION_VERBS = {
    "aumentar": "Aumentar",
    "manter": "Manter",
    "reduzir": "Reduzir",
    "revisar": "Revisar",
    "acompanhar": "Acompanhar",
}


def _clean_action_label(value: str) -> str:
    txt = strip_html(value).strip().lower()
    if not txt:
        return "manter"
    if "reduz" in txt or "diminu" in txt or "cortar" in txt:
        return "reduzir"
    if "aument" in txt or "elevar" in txt or "refor" in txt:
        return "aumentar"
    if "revis" in txt:
        return "revisar"
    if "acompanh" in txt or "monitor" in txt:
        return "acompanhar"
    if "manter" in txt:
        return "manter"
    return "manter"


def _tone_from_action(action: str) -> str:
    mapping = {
        "aumentar": "good",
        "manter": "neutral",
        "acompanhar": "warn",
        "revisar": "warn",
        "reduzir": "bad",
    }
    return mapping.get(action, "neutral")


def _icon_from_action(action: str) -> str:
    mapping = {
        "aumentar": "🟢",
        "manter": "🔵",
        "acompanhar": "🟡",
        "revisar": "🟠",
        "reduzir": "🔴",
    }
    return mapping.get(action, "⚪")


def _label_from_attention(level: str) -> str:
    lvl = strip_html(level).strip().lower()
    return {"alta": "Alta", "média": "Média", "media": "Média", "baixa": "Baixa"}.get(lvl, "Baixa")


def _risk_tone(company: CompanyAnalysis) -> Tuple[str, str]:
    level = strip_html(company.attention_level).strip().lower()
    if level == "alta" or company.attention_score >= 70:
        return "ALTO", "bad"
    if level in {"média", "media"} or company.attention_score >= 35:
        return "MÉDIO", "warn"
    return "CONTROLADO", "good"


def _main_risk(company: CompanyAnalysis) -> str:
    for bucket in [company.persistent_risks, company.riscos, company.attention_drivers, company.validation_warnings]:
        if bucket:
            for item in bucket:
                cleaned = strip_html(item)
                if cleaned:
                    return cleaned
    return "Sem risco dominante explicitado no recorte atual."


def _main_strength(company: CompanyAnalysis) -> str:
    for bucket in [company.persistent_catalysts, company.catalisadores, company.pontos_chave]:
        if bucket:
            for item in bucket:
                cleaned = strip_html(item)
                if cleaned:
                    return cleaned
    return strip_html(company.tese)[:140] or "Sem força dominante explicitada no recorte atual."


def _decision_from_company(company: CompanyAnalysis) -> str:
    action = _clean_action_label(company.recommended_action)
    if action != "manter":
        return action

    forward = strip_html(company.forward_direction).strip().lower()
    perspectiva = strip_html(company.perspectiva_compra).strip().lower()
    risk_level, _ = _risk_tone(company)

    if risk_level == "ALTO":
        return "reduzir"
    if perspectiva == "forte" and forward != "deteriorando" and company.robustez_qualitativa >= 0.70:
        return "aumentar"
    if forward == "deteriorando" and company.attention_score >= 35:
        return "revisar"
    if forward == "melhorando" and company.robustez_qualitativa >= 0.65:
        return "aumentar"
    return "manter"


def _render_signal_chip(label: str, value: str, tone: str = "neutral") -> str:
    return (
        f"<div class='p6-signal-chip {tone}'>"
        f"<span class='p6-signal-label'>{_esc(label)}</span>"
        f"<span class='p6-signal-value'>{_esc(value)}</span>"
        f"</div>"
    )


def _render_hero_stat(title: str, value: str, subtitle: str, tone: str = "neutral") -> str:
    return f"""
    <div class="p6-hero-card {tone}">
      <div class="p6-hero-label">{_esc(title)}</div>
      <div class="p6-hero-value">{_esc(value)}</div>
      <div class="p6-hero-sub">{_esc(subtitle)}</div>
    </div>
    """


def _render_banner(title: str, body: str, tone: str = "neutral", icon: str = "📌") -> None:
    st.markdown(
        f"""
        <div class="p6-banner {tone}">
          <div class="p6-banner-icon">{icon}</div>
          <div>
            <div class="p6-banner-title">{_esc(title)}</div>
            <div class="p6-banner-body">{_esc(body)}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _split_report_highlights(report: Dict[str, Any]) -> Dict[str, List[str]]:
    strengths = [strip_html(x) for x in (report.get("key_strengths") or []) if strip_html(x)]
    weaknesses = [strip_html(x) for x in (report.get("key_weaknesses") or []) if strip_html(x)]
    hidden = [strip_html(x) for x in (report.get("hidden_risks") or []) if strip_html(x)]
    action = [strip_html(x) for x in (report.get("action_plan") or []) if strip_html(x)]
    misalign = [strip_html(x) for x in (report.get("misalignments") or []) if strip_html(x)]
    return {
        "strengths": strengths[:3],
        "weaknesses": weaknesses[:3],
        "hidden": hidden[:3],
        "action": action[:4],
        "misalign": misalign[:3],
    }




def _safe_float_num(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _safe_load_selection_context(tickers: List[str]) -> Dict[str, Dict[str, Any]]:
    try:
        snapshot = get_latest_snapshot()
        if not snapshot:
            return {}
        snapshot_id = str(snapshot.get("id") or "").strip()
        if not snapshot_id:
            return {}
        df = load_snapshot_analysis(snapshot_id)
        if df is None or df.empty:
            return {}
        wanted = {str(t).strip().upper() for t in (tickers or []) if str(t).strip()}
        out: Dict[str, Dict[str, Any]] = {}
        for row in df.to_dict(orient="records"):
            tk = str(row.get("ticker") or "").strip().upper()
            if tk and (not wanted or tk in wanted):
                out[tk] = row
        return out
    except Exception:
        return {}


def _fmt_decimal(value: Any, digits: int = 2, suffix: str = "") -> str:
    try:
        if value in (None, ""):
            return "—"
        return f"{float(value):.{digits}f}{suffix}"
    except Exception:
        return "—"


def _fmt_pct_mixed(value: Any, digits: int = 1) -> str:
    try:
        if value in (None, ""):
            return "—"
        v = float(value)
        if abs(v) <= 1.5:
            v *= 100.0
        return f"{v:.{digits}f}%"
    except Exception:
        return "—"


def _render_selection_context_summary(selection_context: Dict[str, Dict[str, Any]]) -> None:
    if not selection_context:
        return

    rows = list(selection_context.values())
    scores = [_safe_float_num(r.get("score_final")) for r in rows if _safe_float_num(r.get("score_final")) > 0]
    penalties = [_safe_float_num(r.get("penal_total")) for r in rows if r.get("penal_total") is not None]
    leaders = sum(1 for r in rows if str(r.get("rank_segmento") or "") == "1")

    top_reasons: List[str] = []
    for r in rows:
        motivos = r.get("motivos_selecao") or []
        if isinstance(motivos, list):
            for item in motivos:
                txt = strip_html(item)
                if txt and txt not in top_reasons:
                    top_reasons.append(txt)
                if len(top_reasons) >= 4:
                    break
        if len(top_reasons) >= 4:
            break

    st.markdown("## 🧮 Contexto Quantitativo da Seleção")
    cols = st.columns(4)
    cols[0].markdown(_render_hero_stat("Score quant. médio", _fmt_decimal(mean(scores), 1) if scores else "—", "Fotografia do momento de seleção.", "neutral"), unsafe_allow_html=True)
    cols[1].markdown(_render_hero_stat("Líderes setoriais", str(leaders), "Ativos que entraram como nº 1 do segmento.", "good" if leaders else "neutral"), unsafe_allow_html=True)
    cols[2].markdown(_render_hero_stat("Penalização média", _fmt_decimal(mean(penalties), 1) if penalties else "0.0", "Descontos por crowding, liderança e platô.", "warn"), unsafe_allow_html=True)
    cols[3].markdown(_render_hero_stat("Cobertura quantitativa", f"{len(rows)} ativo(s)", "Contexto salvo junto ao snapshot da criação.", "neutral"), unsafe_allow_html=True)

    if top_reasons:
        _render_banner("O que mais pesou na seleção", " • ".join(top_reasons[:4]), "neutral", "📊")


def _render_selection_context_detail(quant_row: Optional[Dict[str, Any]]) -> None:
    if not quant_row:
        return

    st.markdown("**Contexto quantitativo da seleção**")
    metric_items = [
        ("Score final da seleção", _fmt_decimal(quant_row.get("score_final"), 1)),
        ("Rank no segmento", str(quant_row.get("rank_segmento") or "—")),
        ("Rank geral", str(quant_row.get("rank_geral") or "—")),
        ("Classe de força", str(quant_row.get("classe_forca") or "—")),
        ("Penalização total", _fmt_decimal(quant_row.get("penal_total"), 1)),
        ("P/VP", _fmt_decimal(quant_row.get("p_vp"), 2)),
        ("Dividend Yield", _fmt_pct_mixed(quant_row.get("dividend_yield"))),
        ("Slope de receita", _fmt_decimal(quant_row.get("slope_receita"), 3)),
    ]
    _render_metric_cards(metric_items, columns_per_row=4)

    breakdown_items = [
        ("Qualidade", _fmt_decimal(quant_row.get("score_qualidade"), 1)),
        ("Valuation", _fmt_decimal(quant_row.get("score_valuation"), 1)),
        ("Dividendos", _fmt_decimal(quant_row.get("score_dividendos"), 1)),
        ("Crescimento", _fmt_decimal(quant_row.get("score_crescimento"), 1)),
        ("Consistência", _fmt_decimal(quant_row.get("score_consistencia"), 1)),
        ("Crowding", _fmt_decimal(quant_row.get("penal_crowding"), 1)),
        ("Liderança recorrente", _fmt_decimal(quant_row.get("penal_lideranca"), 1)),
        ("Platô", _fmt_decimal(quant_row.get("penal_plato"), 1)),
    ]
    _render_metric_cards(breakdown_items, columns_per_row=4)

    _render_section_list("Motivos de seleção", quant_row.get("motivos_selecao") or [], limit=6)
    _render_section_list("Drivers positivos", quant_row.get("drivers_positivos") or [], limit=6)
    _render_section_list("Drivers negativos", quant_row.get("drivers_negativos") or [], limit=6)



def _safe_load_macro_and_market_context() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    try:
        macro_context = load_latest_macro_context() or {}
    except Exception:
        macro_context = {}
    try:
        market_context = build_market_context(macro_context) if macro_context else {}
    except Exception:
        market_context = {}
    return macro_context, market_context


def _macro_value(macro_context: Dict[str, Any], section: str, key: str) -> Any:
    try:
        return (macro_context.get(section) or {}).get(key)
    except Exception:
        return None


def _fmt_macro_number(value: Any, digits: int = 2, suffix: str = '') -> str:
    try:
        if value in (None, ''):
            return '—'
        return f"{float(value):.{digits}f}{suffix}"
    except Exception:
        return '—'


def _build_macro_reading_from_context(macro_context: Dict[str, Any], market_context: Dict[str, Any]) -> str:
    mensal = macro_context.get('mensal') or {}
    anual = macro_context.get('anual') or {}

    selic = mensal.get('selic_final') if mensal.get('selic_final') is not None else anual.get('selic')
    cambio = mensal.get('cambio_final') if mensal.get('cambio_final') is not None else anual.get('cambio')
    ipca_12m = mensal.get('ipca_12m')
    juros_real = mensal.get('juros_real_ex_ante_12m') if mensal.get('juros_real_ex_ante_12m') is not None else anual.get('juros_real_ex_ante')
    icc_delta = mensal.get('icc_delta_12m') if mensal.get('icc_delta_12m') is not None else anual.get('icc_delta')

    parts = []
    if selic is not None:
        parts.append(f"Selic em {_fmt_macro_number(selic, 2, '%')}, mantendo custo de capital pressionado")
    if juros_real is not None:
        parts.append(f"juro real em {_fmt_macro_number(juros_real, 2, '%')}, reforçando o caráter restritivo do ambiente")
    if ipca_12m is not None:
        parts.append(f"IPCA em 12 meses em {_fmt_macro_number(ipca_12m, 2, '%')}")
    if cambio is not None:
        parts.append(f"câmbio em {_fmt_macro_number(cambio, 4)}, com efeito relevante sobre exportadoras e insumos importados")
    if icc_delta is not None:
        direction = 'melhora' if float(icc_delta) > 0 else 'deterioração' if float(icc_delta) < 0 else 'estabilidade'
        parts.append(f"confiança do consumidor em {direction} no horizonte de 12 meses")

    summary = market_context.get('regime_summary')
    if summary:
        return strip_html(summary) + '. ' + '; '.join(parts) + '.' if parts else strip_html(summary)
    return '; '.join(parts) + '.' if parts else ''


def _build_macro_dependencies_from_context(market_context: Dict[str, Any]) -> List[str]:
    deps: List[str] = []
    for key in ('domestic_risk_factors', 'portfolio_tailwinds', 'portfolio_headwinds', 'international_links'):
        values = market_context.get(key) or []
        if isinstance(values, list):
            for item in values:
                txt = strip_html(item)
                if txt and txt not in deps:
                    deps.append(txt)
    return deps[:6]


def _render_macro_context_summary(macro_context: Dict[str, Any], market_context: Dict[str, Any]) -> None:
    if not macro_context and not market_context:
        return

    mensal = macro_context.get('mensal') or {}
    anual = macro_context.get('anual') or {}
    selic = mensal.get('selic_final') if mensal.get('selic_final') is not None else anual.get('selic')
    ipca_12m = mensal.get('ipca_12m')
    cambio = mensal.get('cambio_final') if mensal.get('cambio_final') is not None else anual.get('cambio')
    juros_real = mensal.get('juros_real_ex_ante_12m') if mensal.get('juros_real_ex_ante_12m') is not None else anual.get('juros_real_ex_ante')
    data_ref = mensal.get('data') or anual.get('data') or '—'

    st.markdown('## 🌍 Cenário macro real do banco')
    cols = st.columns(4)
    cols[0].markdown(_render_hero_stat('Selic', _fmt_macro_number(selic, 2, '%'), f'Referência: {data_ref}', 'bad' if safe_float(selic, 0) >= 12 else 'warn'), unsafe_allow_html=True)
    cols[1].markdown(_render_hero_stat('IPCA 12m', _fmt_macro_number(ipca_12m, 2, '%'), 'Inflação corrente do sistema.', 'warn'), unsafe_allow_html=True)
    cols[2].markdown(_render_hero_stat('Câmbio', _fmt_macro_number(cambio, 4), 'USD/BRL salvo na base macro.', 'neutral'), unsafe_allow_html=True)
    cols[3].markdown(_render_hero_stat('Juro real ex-ante', _fmt_macro_number(juros_real, 2, '%'), 'Pressão de custo de capital.', 'bad' if safe_float(juros_real, 0) >= 4 else 'warn'), unsafe_allow_html=True)

    reading = _build_macro_reading_from_context(macro_context, market_context)
    if reading:
        _render_banner('Leitura macro aplicada à carteira', reading, 'warn', '🌐')

    deps = _build_macro_dependencies_from_context(market_context)
    if deps:
        _render_section_list('Vetores macro e de mercado monitorados', deps, limit=6)

def _render_decision_cycle(analysis: PortfolioAnalysis, stats: PortfolioStats) -> None:
    groups = {"aumentar": [], "manter": [], "revisar": [], "reduzir": []}
    for company in analysis.companies.values():
        groups.setdefault(_decision_from_company(company), []).append(company.ticker)

    forward_values = [c.forward_score for c in analysis.companies.values() if c.forward_score > 0]
    avg_forward = round(mean(forward_values)) if forward_values else 0
    if avg_forward >= max(55, analysis.score_medio):
        exec_status, exec_tone = "Melhorando", "good"
    elif avg_forward and avg_forward <= max(45, analysis.score_medio - 8):
        exec_status, exec_tone = "Deteriorando", "bad"
    else:
        exec_status, exec_tone = "Estável", "warn"

    high_attention = sum(1 for c in analysis.companies.values() if strip_html(c.attention_level).lower() == "alta")
    risk_status = "Controlado" if high_attention == 0 else ("Elevado" if high_attention >= 2 else "Em alta")
    risk_tone = "good" if high_attention == 0 else ("bad" if high_attention >= 2 else "warn")

    st.markdown("## 🧭 Decisão do Ciclo")
    cols = st.columns(3)
    decision_specs = [
        ("Aumentar", groups.get("aumentar", []), "good"),
        ("Manter / Revisar", groups.get("manter", []) + groups.get("revisar", []), "warn"),
        ("Reduzir", groups.get("reduzir", []), "bad"),
    ]
    for col, (label, tickers, tone) in zip(cols, decision_specs):
        tickers_text = ", ".join(tickers) if tickers else "Nenhum ativo neste grupo"
        subtitle = "Movimento sugerido para o próximo ciclo" if tickers else "Sem urgência identificada"
        col.markdown(_render_hero_stat(label, tickers_text, subtitle, tone), unsafe_allow_html=True)

    st.markdown(
        "<div class='p6-signal-row'>"
        + _render_signal_chip("Qualidade", stats.label_qualidade(), "good" if stats.label_qualidade() == "Alta" else "warn")
        + _render_signal_chip("Execução", exec_status, exec_tone)
        + _render_signal_chip("Risco agregado", risk_status, risk_tone)
        + _render_signal_chip("Forward médio", f"{avg_forward}/100" if avg_forward else "—", "neutral")
        + "</div>",
        unsafe_allow_html=True,
    )


def _render_risk_ranking(analysis: PortfolioAnalysis) -> None:
    ranking = sorted(analysis.companies.values(), key=lambda c: (c.attention_score, c.forward_score, c.score_qualitativo), reverse=True)
    st.markdown("## ⚠️ Ranking de Risco")
    for idx, company in enumerate(ranking[:5], start=1):
        risk_label, tone = _risk_tone(company)
        risk_text = _main_risk(company)
        action = _decision_from_company(company)
        st.markdown(
            f"""
            <div class="p6-risk-card {tone}">
              <div class="p6-risk-rank">{idx}</div>
              <div class="p6-risk-main">
                <div class="p6-risk-top">
                  <span class="p6-risk-ticker">{_esc(company.ticker)}</span>
                  <span class="p6-pill {tone}">{_esc(risk_label)}</span>
                  <span class="p6-pill neutral">{_icon_from_action(action)} {_esc(_ACTION_VERBS.get(action, 'Manter'))}</span>
                </div>
                <div class="p6-risk-text">{_esc(risk_text)}</div>
                <div class="p6-risk-meta">Score {_fmt_score(company.score_qualitativo)} • Conf. {_fmt_confidence(company.confianca)} • Atenção {company.attention_score:.0f}/100</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_portfolio_dynamics(analysis: PortfolioAnalysis) -> None:
    improving = sum(1 for c in analysis.companies.values() if strip_html(c.forward_direction).lower() == "melhorando")
    worsening = sum(1 for c in analysis.companies.values() if strip_html(c.forward_direction).lower() == "deteriorando")
    dispersion_high = sum(1 for c in analysis.companies.values() if c.narrative_dispersion_score >= 0.70)
    st.markdown("## 📈 Dinâmica da Carteira")
    cols = st.columns(3)
    cols[0].markdown(_render_hero_stat("Qualidade", analysis.stats.label_qualidade(), f"Score médio {_fmt_score(analysis.score_medio)}", "good"), unsafe_allow_html=True)
    cols[1].markdown(_render_hero_stat("Sinal prospectivo", f"{improving} ↑ / {worsening} ↓", "Direção consolidada das teses", "warn" if worsening >= improving else "good"), unsafe_allow_html=True)
    cols[2].markdown(_render_hero_stat("Dispersão narrativa", f"{dispersion_high} ativo(s)", "Quanto maior, menor previsibilidade", "bad" if dispersion_high >= 2 else "warn"), unsafe_allow_html=True)


def _render_company_executive_summary(company: CompanyAnalysis, quant_row: Optional[Dict[str, Any]] = None) -> None:
    action = _decision_from_company(company)
    action_label = _ACTION_VERBS.get(action, "Manter")
    action_tone = _tone_from_action(action)
    risk_label, risk_tone = _risk_tone(company)
    strength = _main_strength(company)
    risk_text = _main_risk(company)
    signal = strip_html(company.forward_direction).capitalize() if strip_html(company.forward_direction) else "Estável"
    quant_badges = ""
    if quant_row:
        quant_badges += f'<span class="p6-pill neutral">Quant {_esc(_fmt_decimal(quant_row.get("score_final"), 1))}</span>'
        if quant_row.get("rank_segmento") not in (None, ""):
            quant_badges += f'<span class="p6-pill neutral">Rank seg. {_esc(quant_row.get("rank_segmento"))}</span>'
        if quant_row.get("penal_total") not in (None, ""):
            quant_badges += f'<span class="p6-pill warn">Penal {_esc(_fmt_decimal(quant_row.get("penal_total"), 1))}</span>'

    st.markdown(
        f"""
        <div class="p6-exec-card">
          <div class="p6-exec-header">
            <div>
              <div class="p6-exec-ticker">{_esc(company.ticker)}</div>
              <div class="p6-exec-sub">Score {_fmt_score(company.score_qualitativo)} • Conf. {_fmt_confidence(company.confianca)} • {(_esc(company.perspectiva_compra.upper()) if company.perspectiva_compra else '—')}</div>
            </div>
            <div class="p6-pill-stack">
              <span class="p6-pill {action_tone}">{_icon_from_action(action)} {_esc(action_label)}</span>
              <span class="p6-pill {risk_tone}">Risco {risk_label}</span>
              <span class="p6-pill neutral">Sinal { _esc(signal) }</span>{quant_badges}
            </div>
          </div>
          <div class="p6-exec-grid">
            <div class="p6-exec-box"><span>Tese</span><strong>{_esc((strip_html(company.tese)[:180] + '…') if len(strip_html(company.tese)) > 180 else strip_html(company.tese) or '—')}</strong></div>
            <div class="p6-exec-box"><span>Força dominante</span><strong>{_esc(strength[:160] + ('…' if len(strength) > 160 else ''))}</strong></div>
            <div class="p6-exec-box"><span>Risco principal</span><strong>{_esc(risk_text[:160] + ('…' if len(risk_text) > 160 else ''))}</strong></div>
            <div class="p6-exec-box"><span>Decisão</span><strong>{_esc(action_label)} no próximo ciclo</strong></div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ────────────────────────────────────────────────────────────────────────────────
# Score / confidence explanation helpers
# ────────────────────────────────────────────────────────────────────────────────

def _explicar_score(company: CompanyAnalysis) -> str:
    score = company.score_qualitativo
    riscos = len(company.riscos)
    evidencias = len(company.evidencias)
    execucao = strip_html(company.execucao.get("avaliacao_execucao", "")) or "não classificada" \
        if isinstance(company.execucao, dict) else "não classificada"

    if score >= 75:
        faixa = "🟢 Forte"
    elif score >= 55:
        faixa = "🟡 Moderada"
    elif score >= 40:
        faixa = "🟠 Atenção"
    else:
        faixa = "🔴 Fraca"

    return f"{_fmt_score(score)} • {faixa} | Execução: {execucao} | {riscos} riscos | {evidencias} evidências"


def _explicar_confianca(company: CompanyAnalysis) -> str:
    conf = company.confianca
    pct = _fmt_confidence(conf)
    evidencias = len(company.evidencias)
    anos = len(company.strategy_detector.get("coverage_years", [])) \
        if isinstance(company.strategy_detector.get("coverage_years"), list) else 0

    if conf >= 0.75:
        faixa = "🟢 Alta"
    elif conf >= 0.55:
        faixa = "🟡 Média"
    else:
        faixa = "🔴 Baixa"

    return f"{pct} • {faixa} | {evidencias} evidências | {anos} ano(s) analisado(s)"


# ────────────────────────────────────────────────────────────────────────────────
# Rendering primitives
# ────────────────────────────────────────────────────────────────────────────────

def _render_metric_cards(items: List[tuple], columns_per_row: int = 3) -> None:
    clean_items = [(str(label), str(value)) for label, value in items if str(label).strip()]
    if not clean_items:
        return
    for i in range(0, len(clean_items), columns_per_row):
        row_items = clean_items[i:i + columns_per_row]
        cols = st.columns(len(row_items))
        for col, (label, value) in zip(cols, row_items):
            col.markdown(
                f"""
                <div style="border:1px solid rgba(255,255,255,0.08);
                    background:rgba(255,255,255,0.025);border-radius:12px;
                    padding:10px 12px;min-height:78px;margin-bottom:8px;">
                    <div style="font-size:11px;opacity:.70;margin-bottom:4px;">{_esc(label)}</div>
                    <div style="font-size:20px;font-weight:800;">{_esc(value)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _render_section_text(title: str, text_value: str) -> None:
    if not strip_html(text_value):
        return
    st.markdown(f"**{title}**")
    st.markdown(_box_html(text_value), unsafe_allow_html=True)


def _render_section_list(title: str, values: List[str], limit: Optional[int] = None) -> None:
    clean = [strip_html(v) for v in values if strip_html(v)]
    if limit is not None:
        clean = clean[:limit]
    if not clean:
        return
    st.markdown(f"**{title}**")
    for item in clean:
        st.markdown(
            f"<div style='font-size:15px;line-height:1.6;margin:4px 0;'>• {_esc(item)}</div>",
            unsafe_allow_html=True,
        )


def _render_key_value_section(title: str, data: Dict[str, Any], label_map: List[tuple]) -> None:
    if not data:
        return
    blocks: List[str] = []
    for key, label in label_map:
        value = data.get(key)
        if isinstance(value, str) and strip_html(value):
            blocks.append(f"**{label}:** {_esc(value)}")
        elif isinstance(value, list):
            clean = [strip_html(v) for v in value if strip_html(v)]
            if clean:
                blocks.append(f"**{label}:** " + " • ".join(_esc(v) for v in clean))
    if not blocks:
        return
    st.markdown(f"**{title}**")
    st.markdown(
        "<div style='border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.03);"
        "border-radius:14px;padding:14px 16px;box-shadow:0 10px 24px rgba(0,0,0,0.18);"
        "margin-top:8px;line-height:1.5;'>"
        + "<br/><br/>".join(blocks)
        + "</div>",
        unsafe_allow_html=True,
    )


def _render_evidence_section(evidences: List[Any], limit: int = 6) -> None:
    normalized: List[Dict[str, str]] = []
    for item in evidences[:limit]:
        if isinstance(item, dict):
            normalized.append(
                {
                    "topico": strip_html(item.get("topico") or item.get("ano") or ""),
                    "trecho": strip_html(item.get("trecho") or item.get("citacao") or ""),
                    "interpretacao": strip_html(item.get("interpretacao") or item.get("leitura") or ""),
                }
            )
        elif isinstance(item, str) and item.strip():
            normalized.append({"topico": "", "trecho": strip_html(item), "interpretacao": ""})

    normalized = [n for n in normalized if n["trecho"] or n["interpretacao"]]
    if not normalized:
        return

    st.markdown("**Evidências**")
    for item in normalized:
        head = item["topico"] or "Evidência"
        body_parts = []
        if item["trecho"]:
            body_parts.append(f"**Trecho:** {_esc(item['trecho'])}")
        if item["interpretacao"]:
            body_parts.append(f"**Leitura:** {_esc(item['interpretacao'])}")
        st.markdown(
            f"""
            <div style="border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.025);
                        border-radius:12px;padding:12px 14px;margin:8px 0;line-height:1.45;">
                <div style="font-size:12px;opacity:0.7;margin-bottom:6px;">{_esc(head)}</div>
                {'<br/>'.join(body_parts)}
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_strategy_detector(detector: Dict[str, Any]) -> None:
    if not detector:
        return

    summary = strip_html(detector.get("summary"))
    years = detector.get("coverage_years") if isinstance(detector.get("coverage_years"), list) else []
    changes = detector.get("detected_changes") if isinstance(detector.get("detected_changes"), list) else []
    timeline = detector.get("yearly_timeline") if isinstance(detector.get("yearly_timeline"), list) else []
    n_events = safe_int(detector.get("n_events"), 0)

    if not (summary or years or changes or timeline or n_events):
        return

    st.markdown("**Detector de Mudança Estratégica**")
    _render_metric_cards(
        [
            ("Cobertura temporal", ", ".join([str(y) for y in years]) if years else "—"),
            ("Eventos detectados", str(n_events) if n_events > 0 else "—"),
        ],
        columns_per_row=2,
    )
    if summary:
        st.markdown(_box_html(summary), unsafe_allow_html=True)
    if changes:
        _render_section_list("Mudanças detectadas", [strip_html(v) for v in changes], limit=10)
    if timeline:
        st.markdown("**Linha do Tempo Estratégica**")
        for item in timeline[:6]:
            if not isinstance(item, dict):
                continue
            year = strip_html(item.get("year") or "—")
            summary_line = strip_html(item.get("summary") or "")
            evidences = item.get("evidences") if isinstance(item.get("evidences"), list) else []
            extra = ""
            if evidences:
                extra = (
                    "<br/><span style='opacity:.75;font-size:12px;'>"
                    + _esc(" | ".join([strip_html(x) for x in evidences[:2] if strip_html(x)]))
                    + "</span>"
                )
            st.markdown(
                f"""
                <div style="border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.025);
                            border-radius:12px;padding:12px 14px;margin:8px 0;line-height:1.45;">
                    <div style="font-size:13px;opacity:0.80;margin-bottom:6px;font-weight:700;
                                letter-spacing:.2px;">{_esc(year)}</div>
                    <div style="font-size:16px;line-height:1.55;font-weight:700;">
                        {_esc(summary_line or 'Sem resumo temporal consolidado.')}
                    </div>
                    {extra}
                </div>
                """,
                unsafe_allow_html=True,
            )


def _render_score_explanations(company: CompanyAnalysis) -> None:
    score = company.score_qualitativo
    conf = company.confianca
    execucao = strip_html(company.execucao.get("avaliacao_execucao", "")) or "não classificada" \
        if isinstance(company.execucao, dict) else "não classificada"
    riscos = company.riscos
    evidencias = company.evidencias
    anos = company.strategy_detector.get("coverage_years", []) \
        if isinstance(company.strategy_detector.get("coverage_years"), list) else []

    if score >= 75:
        score_txt = (
            f"Qualidade alta: a leitura qualitativa está mais favorável. A execução foi classificada como '{execucao}', "
            f"com {len(evidencias)} evidência(s) documentais e {len(riscos)} risco(s) explícito(s) no recorte."
        )
    elif score >= 55:
        score_txt = (
            f"Qualidade moderada: há sinais positivos, mas com pontos de atenção. A execução foi classificada como '{execucao}', "
            f"com {len(riscos)} risco(s) e {len(evidencias)} evidência(s) sustentando a análise."
        )
    elif score >= 40:
        score_txt = (
            f"Qualidade de atenção: a tese ainda mostra fragilidades. A execução aparece como '{execucao}', "
            f"com {len(riscos)} risco(s) relevantes frente a {len(evidencias)} evidência(s) disponíveis."
        )
    else:
        score_txt = (
            f"Qualidade fraca: a leitura qualitativa está pressionada por riscos e/ou baixa consistência. "
            f"A execução foi classificada como '{execucao}'."
        )

    if conf >= 0.75:
        conf_txt = (
            f"Confiança alta: a leitura se apoia em base documental mais robusta, com {len(evidencias)} evidência(s) "
            f"e cobertura temporal de {len(anos)} ano(s)."
        )
    elif conf >= 0.55:
        conf_txt = (
            f"Confiança média: há base documental útil, mas ainda incompleta. O resultado usa {len(evidencias)} evidência(s) "
            f"e cobertura temporal de {len(anos)} ano(s)."
        )
    else:
        conf_txt = (
            f"Confiança baixa: a leitura depende de base documental mais limitada. Neste caso, há {len(evidencias)} evidência(s) "
            f"e cobertura temporal de {len(anos)} ano(s), o que recomenda cautela adicional."
        )

    st.caption("Como interpretar os scores")
    st.markdown(_box_html(score_txt + "\n\n" + conf_txt), unsafe_allow_html=True)
    if company.score_source == "heuristic":
        st.markdown(
            _badge("Score heurístico — LLM não retornou valor; score estimado por estrutura do JSON", "warn"),
            unsafe_allow_html=True,
        )


# ────────────────────────────────────────────────────────────────────────────────
# Portfolio-level sections
# ────────────────────────────────────────────────────────────────────────────────

def _render_allocation_section(allocation_rows: List[AllocationRow]) -> None:
    st.markdown("## 💼 Alocação Sugerida")
    st.caption("Distribuição percentual heurística entre todos os ativos cobertos no portfólio. Soma total = 100%.")
    cols_per_row = 4
    for i in range(0, len(allocation_rows), cols_per_row):
        row_items = allocation_rows[i:i + cols_per_row]
        cols = st.columns(len(row_items))
        for col, item in zip(cols, row_items):
            col.markdown(
                f"""
                <div class="p6-card">
                  <div class="p6-card-label">{_esc(item.ticker)}</div>
                  <div class="p6-card-value" style="font-size:24px">{item.allocation_pct:.2f}%</div>
                  <div class="p6-card-extra">
                    {_esc((item.perspectiva or '—').upper())} • Score {_fmt_score(item.score)} • Conf. {_fmt_confidence(item.confianca)}
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _render_structured_portfolio_report(report: Dict[str, Any], mode_label: str, analysis: PortfolioAnalysis, macro_context: Dict[str, Any], market_context: Dict[str, Any]) -> None:
    st.markdown("## 🧠 Relatório Estratégico do Portfólio")
    st.caption(f"Modo utilizado: {mode_label}")

    highlights = _split_report_highlights(report)

    _render_banner(
        "Leitura executiva",
        strip_html(report.get("executive_summary", "")) or "Sem diagnóstico executivo consolidado.",
        "neutral",
        "🧠",
    )

    macro_reading = report.get("macro_reading", "") or _build_macro_reading_from_context(macro_context, market_context)
    macro_deps = report.get("macro_scenario_dependencies", []) or _build_macro_dependencies_from_context(market_context)

    col1, col2 = st.columns([1.2, 1.0])
    with col1:
        _render_section_text("Base analítica", report.get("analytical_basis", ""))
        _render_section_text("Identidade da carteira", report.get("portfolio_identity", ""))
        _render_section_text("Impacto macro dominante", macro_reading)
    with col2:
        if highlights["strengths"]:
            _render_section_list("🟢 Forças principais", highlights["strengths"], limit=3)
        if highlights["weaknesses"]:
            _render_section_list("🟠 Fragilidades principais", highlights["weaknesses"], limit=3)
        if highlights["hidden"]:
            _render_section_list("🔴 Riscos invisíveis", highlights["hidden"], limit=3)

    if macro_deps:
        _render_section_list("Dependências de cenário", macro_deps, limit=4)

    misalign = highlights["misalign"]
    if misalign:
        _render_banner("Desalinhamentos identificados", " • ".join(misalign), "warn", "⚠️")

    action_plan = highlights["action"]
    if action_plan:
        st.markdown("### 🎯 Plano de ação")
        for item in action_plan:
            st.markdown(f"<div class='p6-action-line'>✅ {_esc(item)}</div>", unsafe_allow_html=True)

    roles = report.get("asset_roles", []) or []
    if roles:
        st.markdown("### 🧩 Papel estratégico dos ativos")
        for item in roles[: min(6, len(analysis.companies))]:
            if not isinstance(item, dict):
                continue
            ticker = strip_html(item.get("ticker") or "—")
            role = strip_html(item.get("role") or "")
            rationale = strip_html(item.get("rationale") or "")
            st.markdown(
                f"<div class='p6-role-card'><strong>{_esc(ticker)}</strong>"
                f"<span>{_esc(role)}</span><p>{_esc(rationale or '—')}</p></div>",
                unsafe_allow_html=True,
            )

    suggested_allocations = report.get("suggested_allocations", []) or []
    if suggested_allocations:
        st.markdown("### 📌 Faixas de alocação sugeridas")
        for item in suggested_allocations[: min(8, len(analysis.companies))]:
            if not isinstance(item, dict):
                continue
            ticker = strip_html(item.get("ticker") or "—")
            suggested_range = strip_html(item.get("suggested_range") or "")
            rationale = strip_html(item.get("rationale") or "")
            st.markdown(
                f"<div class='p6-allocation-line'><strong>{_esc(ticker)}</strong>"
                f"<span>{_esc(suggested_range or '—')}</span><p>{_esc(rationale or '—')}</p></div>",
                unsafe_allow_html=True,
            )

    final_insight = strip_html(report.get("final_insight", ""))
    if final_insight:
        _render_banner("Insight final", final_insight, "good", "🚀")


# ────────────────────────────────────────────────────────────────────────────────
# Company detail section
# ────────────────────────────────────────────────────────────────────────────────

def _render_company_expander(company: CompanyAnalysis, quant_row: Optional[Dict[str, Any]] = None) -> None:
    tk = company.ticker
    p = company.perspectiva_compra.strip().lower()
    badge = _badge((p or "—").upper(), _tone_from_perspectiva(p))
    heuristic_badge = (
        "  " + _badge("Score heurístico", "warn")
        if company.score_source == "heuristic"
        else ""
    )

    with st.expander(tk, expanded=False):
        _render_company_executive_summary(company, quant_row)
        st.markdown(f"### {tk}  {badge}{heuristic_badge}", unsafe_allow_html=True)
        st.caption(
            f"Período analisado: {company.period_ref} • Atualizado em: {company.created_at}"
            + (f" • Confiança: {_fmt_confidence(company.confianca)}" if company.confianca > 0 else "")
            + (f" • Score: {_fmt_score(company.score_qualitativo)}" if company.score_qualitativo > 0 else "")
            + (" • ⚠️ Score calculado sem LLM" if company.score_source == "heuristic" else "")
        )

        metric_items = [
            ("Score qualitativo", _explicar_score(company)),
            ("Confiança", _explicar_confianca(company)),
        ]
        years = company.strategy_detector.get("coverage_years", []) \
            if isinstance(company.strategy_detector.get("coverage_years"), list) else []
        if years:
            metric_items.append(("Cobertura temporal", ", ".join([str(y) for y in years[:4]])))

        _render_metric_cards(metric_items, columns_per_row=2)
        _render_score_explanations(company)

        with st.expander("Ver análise detalhada", expanded=False):
            _render_section_text("Tese (síntese)", company.tese or "—")

            if company.leitura:
                _render_section_text("Leitura / Direcionalidade", company.leitura)
            elif p == "forte":
                _render_section_text(
                    "Leitura / Direcionalidade",
                    "Viés construtivo, com sinais qualitativos favoráveis no recorte analisado. Mantém assimetria positiva, com monitoramento de riscos.",
                )
            elif p == "moderada":
                _render_section_text(
                    "Leitura / Direcionalidade",
                    "Leitura equilibrada, com pontos positivos e ressalvas. Indica acompanhamento de gatilhos de execução, guidance e alocação de capital.",
                )
            elif p == "fraca":
                _render_section_text(
                    "Leitura / Direcionalidade",
                    "Leitura cautelosa, com sinais qualitativos desfavoráveis no recorte analisado. Recomenda postura defensiva e foco em mitigação de risco.",
                )

            _render_section_text("Papel estratégico", company.papel_estrategico)
            _render_section_list("Sensibilidades macro", company.sensibilidades_macro, limit=8)
            _render_section_text("Fragilidade sob o regime atual", company.fragilidade_regime_atual)
            _render_section_list("Dependências de cenário", company.dependencias_cenario, limit=6)
            _render_section_text("Evolução Estratégica", pick_text(company.evolucao, "historico", "fase_atual", "tendencia") or str(company.evolucao or ""))
            _render_strategy_detector(company.strategy_detector)
            _render_key_value_section(
                "Consistência do Discurso",
                company.consistencia,
                [("analise", "Análise"), ("grau", "Grau"), ("contradicoes", "Contradições")],
            )
            _render_key_value_section(
                "Execução vs Promessa",
                company.execucao,
                [("analise", "Análise"), ("avaliacao_execucao", "Avaliação"), ("entregas_confirmadas", "Entregas confirmadas"), ("entregas_pendentes_ou_incertas", "Entregas pendentes ou incertas")],
            )
            _render_section_list("Mudanças Estratégicas", company.mudancas, limit=8)
            _render_section_list("Pontos-chave", company.pontos_chave, limit=8)
            _render_section_list("Catalisadores", company.catalisadores, limit=8)
            _render_section_list("Riscos", company.riscos, limit=8)
            _render_section_list("O que monitorar", company.monitorar, limit=8)
            _render_section_list("Ruídos e Contradições", company.contradicoes + company.sinais_ruido, limit=8)
            _render_key_value_section(
                "Qualidade Narrativa",
                company.qualidade_narrativa,
                [("clareza", "Clareza"), ("coerencia", "Coerência"), ("sinais_de_ruido", "Sinais de ruído")],
            )
            _render_evidence_section(company.evidencias, limit=10)
            _render_section_text("Considerações da LLM", company.consideracoes)

            _render_metric_cards([
                ("Score qualitativo (híbrido)", _fmt_score(company.score_qualitativo)),
                ("Robustez qualitativa", f"{round(company.robustez_qualitativa * 100)}%"),
                ("Dispersão narrativa", f"{round(company.narrative_dispersion_score * 100)}%"),
                ("Schema score", f"{company.schema_score}/100"),
            ], columns_per_row=4)

            evol_items = [
                ("Trend de execução", company.execution_trend),
                ("Mudança de narrativa", company.narrative_shift),
            ]
            if company.forward_score > 0:
                evol_items.append(("Sinal prospectivo", f"{company.forward_score}/100 ({company.forward_direction})"))
            _render_metric_cards(evol_items, columns_per_row=3)

            if company.memory_summary:
                _render_section_text("Memória histórica da tese", company.memory_summary)
            _render_section_list("Promessas recorrentes", company.recurring_promises, limit=5)
            _render_section_list("Entregas confirmadas (recorrentes)", company.delivered_promises, limit=5)
            _render_section_list("Riscos persistentes entre períodos", company.persistent_risks, limit=5)
            _render_section_list("Catalisadores persistentes", company.persistent_catalysts, limit=5)

            if company.current_regime not in ("—", "indefinido", ""):
                st.markdown("**Mudança de Regime Qualitativo**")
                regime_tone = (
                    "bad" if company.regime_change_intensity == "significativo"
                    else "warn" if company.regime_change_intensity == "moderado"
                    else "neutral"
                )
                badges = _badge(f"Atual: {company.current_regime}", "neutral")
                if company.previous_regime not in ("—", "indefinido", ""):
                    badges += "  " + _badge(f"Anterior: {company.previous_regime}", "neutral")
                badges += "  " + _badge(f"Intensidade: {company.regime_change_intensity}", regime_tone)
                st.markdown(badges, unsafe_allow_html=True)
                if company.regime_change_explanation:
                    st.markdown(_box_html(company.regime_change_explanation), unsafe_allow_html=True)

            if company.attention_score > 0:
                attn_tone = (
                    "bad" if company.attention_level == "alta"
                    else "warn" if company.attention_level == "média"
                    else "neutral"
                )
                st.markdown("**Prioridade de Acompanhamento**")
                st.markdown(
                    _badge(f"Nível: {company.attention_level.upper()}", attn_tone)
                    + "  " + _badge(f"Score: {company.attention_score:.0f}/100", attn_tone)
                    + "  " + _badge(company.recommended_action, attn_tone),
                    unsafe_allow_html=True,
                )
                _render_section_list("Drivers da prioridade", company.attention_drivers, limit=6)

            if company.forward_score > 0:
                fwd_tone = (
                    "good" if company.forward_direction == "melhorando"
                    else "bad" if company.forward_direction == "deteriorando"
                    else "neutral"
                )
                st.markdown("**Sinal Prospectivo**")
                st.markdown(
                    _badge(f"Forward score: {_fmt_score(company.forward_score)}", fwd_tone)
                    + "  " + _badge(f"Direção: {company.forward_direction}", fwd_tone)
                    + "  " + _badge(f"Confiança: {_fmt_confidence(company.forward_confidence)}", "neutral"),
                    unsafe_allow_html=True,
                )
                _render_section_list("Fatores prospectivos", company.forward_drivers, limit=6)


# ────────────────────────────────────────────────────────────────────────────────
# Entry point
# ────────────────────────────────────────────────────────────────────────────────

_P6_CSS = """
<style>
:root{
  --p6-bg:#0b1220;
  --p6-card:#111827;
  --p6-card-2:#172033;
  --p6-border:rgba(148,163,184,.20);
  --p6-text:#e5eefc;
  --p6-muted:#94a3b8;
  --p6-good:#22c55e;
  --p6-warn:#f59e0b;
  --p6-bad:#ef4444;
  --p6-neutral:#3b82f6;
}
.p6-card,.p6-hero-card,.p6-exec-card,.p6-risk-card,.p6-banner,.p6-role-card,.p6-allocation-line,.p6-action-line{
  border:1px solid var(--p6-border);
  color:var(--p6-text);
}
.p6-card{
  background:linear-gradient(180deg, rgba(17,24,39,.96), rgba(15,23,42,.92));
  border-radius:18px;
  padding:16px 18px;
  box-shadow:0 12px 28px rgba(2,8,23,.26);
  min-height:110px;
}
.p6-card-label{font-size:12px;opacity:.76;margin-bottom:6px;letter-spacing:.3px;text-transform:uppercase;}
.p6-card-value{font-size:28px;font-weight:900;margin-bottom:6px;line-height:1.05;}
.p6-card-extra{font-size:12px;opacity:.70;line-height:1.45;}
.p6-hero-card{
  background:linear-gradient(135deg, rgba(17,24,39,.96), rgba(23,32,51,.96));
  border-radius:18px;padding:18px;min-height:138px;box-shadow:0 12px 30px rgba(2,8,23,.24);
}
.p6-hero-card.good{border-color:rgba(34,197,94,.40);box-shadow:0 12px 30px rgba(34,197,94,.12)}
.p6-hero-card.warn{border-color:rgba(245,158,11,.40);box-shadow:0 12px 30px rgba(245,158,11,.12)}
.p6-hero-card.bad{border-color:rgba(239,68,68,.45);box-shadow:0 12px 30px rgba(239,68,68,.12)}
.p6-hero-card.neutral{border-color:rgba(59,130,246,.35)}
.p6-hero-label{font-size:12px;text-transform:uppercase;letter-spacing:.3px;color:var(--p6-muted);margin-bottom:10px}
.p6-hero-value{font-size:28px;font-weight:900;line-height:1.15;margin-bottom:8px}
.p6-hero-sub{font-size:13px;line-height:1.5;color:#cbd5e1}
.p6-signal-row{display:grid;grid-template-columns:repeat(4, minmax(0, 1fr));gap:10px;margin:14px 0 8px}
.p6-signal-chip{border-radius:14px;padding:12px 14px;background:rgba(255,255,255,.03);border:1px solid var(--p6-border);display:flex;flex-direction:column;gap:6px}
.p6-signal-chip.good{border-color:rgba(34,197,94,.35)}
.p6-signal-chip.warn{border-color:rgba(245,158,11,.35)}
.p6-signal-chip.bad{border-color:rgba(239,68,68,.35)}
.p6-signal-chip.neutral{border-color:rgba(59,130,246,.30)}
.p6-signal-label{font-size:11px;letter-spacing:.3px;text-transform:uppercase;color:var(--p6-muted)}
.p6-signal-value{font-size:18px;font-weight:800}
.p6-banner{display:flex;gap:12px;align-items:flex-start;background:linear-gradient(135deg, rgba(17,24,39,.96), rgba(23,32,51,.95));border-radius:16px;padding:16px 18px;margin:10px 0}
.p6-banner.good{border-color:rgba(34,197,94,.40)}
.p6-banner.warn{border-color:rgba(245,158,11,.40)}
.p6-banner.bad{border-color:rgba(239,68,68,.45)}
.p6-banner-icon{font-size:24px;line-height:1}
.p6-banner-title{font-size:14px;font-weight:800;margin-bottom:4px}
.p6-banner-body{font-size:14px;line-height:1.6;color:#dbe7f7}
.p6-risk-card{display:flex;gap:14px;align-items:flex-start;background:linear-gradient(180deg, rgba(17,24,39,.95), rgba(15,23,42,.92));border-radius:16px;padding:14px 16px;margin:10px 0}
.p6-risk-card.good{border-color:rgba(34,197,94,.35)}
.p6-risk-card.warn{border-color:rgba(245,158,11,.35)}
.p6-risk-card.bad{border-color:rgba(239,68,68,.45)}
.p6-risk-rank{width:34px;height:34px;border-radius:999px;background:rgba(255,255,255,.06);display:flex;align-items:center;justify-content:center;font-weight:900}
.p6-risk-main{flex:1}
.p6-risk-top{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:8px}
.p6-risk-ticker{font-size:18px;font-weight:900}
.p6-risk-text{font-size:14px;line-height:1.55;margin-bottom:6px;color:#dbe7f7}
.p6-risk-meta{font-size:12px;color:var(--p6-muted)}
.p6-pill{display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:700;border:1px solid var(--p6-border);background:rgba(255,255,255,.03)}
.p6-pill.good{border-color:rgba(34,197,94,.40);color:#bbf7d0}
.p6-pill.warn{border-color:rgba(245,158,11,.40);color:#fde68a}
.p6-pill.bad{border-color:rgba(239,68,68,.45);color:#fecaca}
.p6-pill.neutral{border-color:rgba(59,130,246,.35);color:#bfdbfe}
.p6-pill-stack{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}
.p6-exec-card{background:linear-gradient(135deg, rgba(17,24,39,.97), rgba(20,29,47,.95));border-radius:18px;padding:18px;margin-bottom:14px;box-shadow:0 12px 30px rgba(2,8,23,.22)}
.p6-exec-header{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;margin-bottom:14px}
.p6-exec-ticker{font-size:24px;font-weight:900;line-height:1}
.p6-exec-sub{font-size:13px;color:var(--p6-muted);margin-top:8px}
.p6-exec-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.p6-exec-box{border:1px solid rgba(148,163,184,.18);background:rgba(255,255,255,.03);border-radius:14px;padding:12px 14px}
.p6-exec-box span{display:block;font-size:11px;text-transform:uppercase;letter-spacing:.3px;color:var(--p6-muted);margin-bottom:6px}
.p6-exec-box strong{font-size:14px;line-height:1.5;color:#eaf2ff}
.p6-role-card,.p6-allocation-line{background:rgba(255,255,255,.03);border-radius:14px;padding:12px 14px;margin:8px 0}
.p6-role-card strong,.p6-allocation-line strong{display:block;font-size:15px;margin-bottom:4px}
.p6-role-card span,.p6-allocation-line span{display:block;font-size:12px;color:var(--p6-muted);margin-bottom:6px}
.p6-role-card p,.p6-allocation-line p{margin:0;font-size:14px;line-height:1.55;color:#dbe7f7}
.p6-action-line{background:rgba(255,255,255,.03);border-radius:12px;padding:10px 12px;margin:8px 0;font-size:14px;line-height:1.5}
@media (max-width: 900px){
  .p6-signal-row,.p6-exec-grid{grid-template-columns:repeat(2,minmax(0,1fr));}
  .p6-exec-header{flex-direction:column}
}
</style>
"""


def render_patch6_report(
    tickers: List[str],
    period_ref: str,
    llm_factory: Optional[Any] = None,
    show_company_details: bool = True,
    analysis_mode: str = "rigid",
) -> None:
    st.markdown(_P6_CSS, unsafe_allow_html=True)

    # ── Data + computation (pure, no Streamlit) ──────────────────────────────
    analysis = build_portfolio_analysis(tickers, period_ref)
    if analysis is None or not analysis.companies:
        st.warning(
            "Não há execuções salvas em patch6_runs para este period_ref e tickers do portfólio. "
            "Rode a LLM e salve os resultados primeiro."
        )
        return

    stats = analysis.stats
    selection_context = _safe_load_selection_context(list(analysis.companies.keys()))
    macro_context, market_context = _safe_load_macro_and_market_context()

    # ── Portfolio summary cards ───────────────────────────────────────────────
    top_cols = st.columns(5)
    top_cols[0].markdown(_render_hero_stat("Qualidade", stats.label_qualidade(), "Leitura qualitativa agregada da carteira.", "good" if stats.label_qualidade() == "Alta" else "warn"), unsafe_allow_html=True)
    top_cols[1].markdown(_render_hero_stat("Perspectiva 12m", stats.label_perspectiva(), "Direcionalidade consolidada do conjunto.", "neutral"), unsafe_allow_html=True)
    top_cols[2].markdown(_render_hero_stat("Cobertura", analysis.cobertura, "Ativos cobertos com evidência suficiente.", "neutral"), unsafe_allow_html=True)
    top_cols[3].markdown(_render_hero_stat("Confiança média", _fmt_confidence(analysis.confianca_media), "Robustez média das leituras individuais.", "neutral"), unsafe_allow_html=True)
    top_cols[4].markdown(_render_hero_stat("Score médio", _fmt_score(analysis.score_medio), "Média qualitativa consolidada do portfólio.", "neutral"), unsafe_allow_html=True)
    st.caption(
        "🛈 A leitura abaixo prioriza decisão, risco e direção antes do detalhe. "
        f"Cobertura temporal do detector estratégico presente em {analysis.temporal_covered} ativo(s)."
    )

    _render_decision_cycle(analysis, stats)
    _render_macro_context_summary(macro_context, market_context)
    _render_selection_context_summary(selection_context)
    _render_risk_ranking(analysis)
    _render_portfolio_dynamics(analysis)

    # ── v3 portfolio signals ──────────────────────────────────────────────────
    v3_items = []
    if analysis.alta_prioridade_count > 0:
        v3_items.append(
            _badge(f"{analysis.alta_prioridade_count} ativo(s) em alta prioridade", "bad")
        )
    if analysis.forward_score_medio > 0:
        fdir_overall = "—"
        fscores = [c.forward_score for c in analysis.companies.values() if c.forward_score > 0]
        if fscores and analysis.score_medio > 0:
            avg_delta = sum(fscores) / len(fscores) - analysis.score_medio
            fdir_overall = "melhorando" if avg_delta > 5 else ("deteriorando" if avg_delta < -5 else "estável")
        v3_items.append(
            _badge(f"Forward score médio: {analysis.forward_score_medio}/100 ({fdir_overall})", "neutral")
        )
    if analysis.regime_summary:
        v3_items.append(_badge("Mudanças de regime detectadas", "warn"))

    if v3_items:
        st.markdown("&nbsp;&nbsp;".join(v3_items) + (
            f"<br/><span style='font-size:11px;opacity:0.6'>{_esc(analysis.regime_summary)}</span>"
            if analysis.regime_summary else ""
        ), unsafe_allow_html=True)
        st.markdown("")  # spacing

    # ── v3 priority ranking (compact) ────────────────────────────────────────
    if analysis.priority_ranking:
        alta = [tk for tk in analysis.priority_ranking if analysis.companies[tk].attention_level == "alta"]
        media = [tk for tk in analysis.priority_ranking if analysis.companies[tk].attention_level == "média"]
        if alta or media:
            with st.expander("📋 Fila de Atenção do Portfólio", expanded=False):
                if alta:
                    st.markdown("**Alta prioridade**")
                    for tk in alta:
                        c = analysis.companies[tk]
                        st.markdown(
                            f"- **{tk}** — score {c.attention_score:.0f} | {c.recommended_action}"
                            + (f" | drivers: {', '.join(c.attention_drivers[:3])}" if c.attention_drivers else ""),
                        )
                if media:
                    st.markdown("**Média prioridade**")
                    for tk in media:
                        c = analysis.companies[tk]
                        st.markdown(f"- **{tk}** — score {c.attention_score:.0f} | {c.recommended_action}")

    # ── LLM portfolio report (optional) ──────────────────────────────────────
    portfolio_report = run_portfolio_llm_report(llm_factory, analysis, analysis_mode)

    if portfolio_report:
        mode_label = "Análise Rígida" if analysis_mode == "rigid" else "Análise Flexível"
        _render_structured_portfolio_report(portfolio_report, mode_label, analysis, macro_context, market_context)
    else:
        st.markdown("## 🧠 Resumo Executivo")
        st.write(
            f"O portfólio apresenta leitura **{stats.label_perspectiva().lower()}** para 12 meses, com distribuição: "
            f"**{stats.fortes}** forte, **{stats.moderadas}** moderada e **{stats.fracas}** fraca. "
            f"Cobertura: **{analysis.cobertura}** ativos, confiança média **{_fmt_confidence(analysis.confianca_media)}**, "
            f"score qualitativo médio **{_fmt_score(analysis.score_medio)}**."
        )

    _render_allocation_section(analysis.allocation_rows)

    # ── Per-company detail ────────────────────────────────────────────────────
    if show_company_details:
        st.markdown("## 🏢 Relatórios por Empresa")
        for company in analysis.companies.values():
            _render_company_expander(company, selection_context.get(company.ticker))

    # ── Conclusão estratégica ─────────────────────────────────────────────────
    st.markdown("## 🔎 Conclusão Estratégica")
    llm_client = None
    if llm_factory is not None:
        try:
            llm_client = llm_factory.get_llm_client()
        except Exception:
            pass

    prompt_conc = (
        "Escreva uma conclusão estratégica (research) para o portfólio, em até 10 linhas, com foco em:\n"
        "- coerência do conjunto do portfólio\n"
        "- principais alavancas para melhora ou deterioração\n"
        "- recomendação de acompanhamento nos próximos trimestres\n\n"
        f"Use SOMENTE os bullets abaixo.\n\nBULLETS:\n{analysis.contexto_portfolio}"
    )
    llm_conc = safe_call_llm(llm_client, prompt_conc)
    if llm_conc:
        st.write(llm_conc)
    else:
        st.write(
            "A carteira deve ser acompanhada por gatilhos de execução, evolução da narrativa corporativa, score qualitativo, "
            "mudanças estratégicas detectadas e sinais de alocação de capital. Reforce o monitoramento de resultados trimestrais, "
            "consistência entre discurso e entrega, dívida/custo financeiro e manutenção dos catalisadores já visíveis nas evidências do RAG."
        )
