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
    if action in {"aumentar", "reduzir", "revisar"}:
        return action

    forward = strip_html(company.forward_direction).strip().lower()
    perspectiva = strip_html(company.perspectiva_compra).strip().lower()
    risk_level, _ = _risk_tone(company)

    if risk_level == "ALTO":
        return "reduzir"
    if forward == "deteriorando":
        return "revisar" if company.attention_score < 55 else "reduzir"
    if perspectiva == "forte" and company.robustez_qualitativa >= 0.60 and company.confianca >= 0.65:
        return "aumentar"
    if forward == "melhorando" and company.robustez_qualitativa >= 0.55:
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


def _render_decision_cycle(analysis: PortfolioAnalysis, stats: PortfolioStats) -> None:
    groups = {"aumentar": [], "manter": [], "revisar": [], "reduzir": []}
    for company in analysis.companies.values():
        groups.setdefault(_decision_from_company(company), []).append(company.ticker)

    def _group_card(label: str, tickers: List[str], tone: str, empty_label: str) -> str:
        count = len(tickers)
        names = ", ".join(tickers) if tickers else empty_label
        subtitle = f"{count} ativo(s)" if count else "Sem destaque relevante neste ciclo"
        return f"""
        <div class="p6-hero-card {tone} p6-decision-card">
          <div class="p6-hero-label">{_esc(label)}</div>
          <div class="p6-decision-list">{_esc(names)}</div>
          <div class="p6-hero-sub">{_esc(subtitle)}</div>
        </div>
        """

    st.markdown("## 🧭 Decisão do Ciclo")
    left, right = st.columns([1.05, 0.95])
    with left:
        cols = st.columns(3)
        cols[0].markdown(_group_card("🟢 Oportunidade", groups.get("aumentar", []), "good", "Nenhum ativo"), unsafe_allow_html=True)
        cols[1].markdown(_group_card("🔵 Neutralidade", groups.get("manter", []) + groups.get("revisar", []), "neutral", "Nenhum ativo"), unsafe_allow_html=True)
        cols[2].markdown(_group_card("🔴 Perigo", groups.get("reduzir", []), "bad", "Nenhum ativo"), unsafe_allow_html=True)

    with right:
        _render_risk_ranking(analysis, compact=True)


def _render_risk_ranking(analysis: PortfolioAnalysis, compact: bool = False) -> None:
    ranking = sorted(analysis.companies.values(), key=lambda c: (c.attention_score, c.forward_score, c.score_qualitativo), reverse=True)
    st.markdown("## ⚠️ Ranking de Risco")

    topn = 5 if compact else 8
    rows = ranking[:topn]
    if compact:
        for idx, company in enumerate(rows, start=1):
            risk_label, tone = _risk_tone(company)
            risk_text = _main_risk(company)
            action = _decision_from_company(company)
            st.markdown(
                f"""
                <div class="p6-risk-card {tone} compact">
                  <div class="p6-risk-rank">{idx}</div>
                  <div class="p6-risk-main">
                    <div class="p6-risk-top compact">
                      <span class="p6-risk-ticker compact">{_esc(company.ticker)}</span>
                      <span class="p6-pill {tone}">{_esc(risk_label)}</span>
                      <span class="p6-pill neutral">{_icon_from_action(action)} {_esc(_ACTION_VERBS.get(action, 'Manter'))}</span>
                    </div>
                    <div class="p6-risk-text compact">{_esc(risk_text)}</div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        return

    for idx, company in enumerate(rows, start=1):
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
                <div class="p6-risk-meta">Ação sugerida: {_esc(_ACTION_VERBS.get(action, "Manter"))}</div>
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


def _render_company_executive_summary(company: CompanyAnalysis) -> None:
    action = _decision_from_company(company)
    action_label = _ACTION_VERBS.get(action, "Manter")
    action_tone = _tone_from_action(action)
    risk_label, risk_tone = _risk_tone(company)
    strength = _main_strength(company)
    risk_text = _main_risk(company)
    signal = strip_html(company.forward_direction).capitalize() if strip_html(company.forward_direction) else "Estável"

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
              <span class="p6-pill neutral">Sinal { _esc(signal) }</span>
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


def _render_structured_portfolio_report(report: Dict[str, Any], mode_label: str, analysis: PortfolioAnalysis) -> None:
    with st.expander("🧠 Ver relatório estratégico completo", expanded=False):
        st.markdown("## 🧠 Relatório Estratégico do Portfólio")
        st.caption(f"Modo utilizado: {mode_label}")

        highlights = _split_report_highlights(report)

        _render_banner(
            "Leitura executiva",
            strip_html(report.get("executive_summary", "")) or "Sem diagnóstico executivo consolidado.",
            "neutral",
            "🧠",
        )

        col1, col2 = st.columns([1.2, 1.0])
        with col1:
            _render_section_text("Base analítica", report.get("analytical_basis", ""))
            _render_section_text("Identidade da carteira", report.get("portfolio_identity", ""))
            _render_section_text("Impacto macro dominante", report.get("macro_reading", ""))
        with col2:
            if highlights["strengths"]:
                _render_section_list("🟢 Forças principais", highlights["strengths"], limit=3)
            if highlights["weaknesses"]:
                _render_section_list("🟠 Fragilidades principais", highlights["weaknesses"], limit=3)
            if highlights["hidden"]:
                _render_section_list("🔴 Riscos invisíveis", highlights["hidden"], limit=3)

        macro_deps = report.get("macro_scenario_dependencies", []) or []
        if macro_deps:
            _render_section_list("Dependências de cenário", macro_deps, limit=4)

        misalign = highlights["misalign"]
        if misalign:
            _render_banner("Desalinhamentos identificados", " • ".join(misalign), "warn", "⚠️")

        action_plan = highlights["action"]
        if action_plan:
            with st.expander("Ver plano de ação", expanded=False):
                for item in action_plan:
                    st.markdown(f"<div class='p6-action-line'>✅ {_esc(item)}</div>", unsafe_allow_html=True)

        roles = report.get("asset_roles", []) or []
        if roles:
            with st.expander("Ver papel estratégico dos ativos", expanded=False):
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
            with st.expander("Ver faixas de alocação", expanded=False):
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

def _render_company_expander(company: CompanyAnalysis) -> None:
    tk = company.ticker
    p = company.perspectiva_compra.strip().lower()
    badge = _badge((p or "—").upper(), _tone_from_perspectiva(p))
    heuristic_badge = (
        "  " + _badge("Score heurístico", "warn")
        if company.score_source == "heuristic"
        else ""
    )

    with st.expander(tk, expanded=False):
        _render_company_executive_summary(company)
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
.p6-decision-card{min-height:150px}
.p6-decision-list{font-size:18px;font-weight:900;line-height:1.35;word-break:break-word;white-space:normal;margin-bottom:8px}
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
.p6-risk-card.compact{padding:10px 12px;margin:8px 0;border-radius:14px}
.p6-risk-card.good{border-color:rgba(34,197,94,.35)}
.p6-risk-card.warn{border-color:rgba(245,158,11,.35)}
.p6-risk-card.bad{border-color:rgba(239,68,68,.45)}
.p6-risk-rank{width:34px;height:34px;border-radius:999px;background:rgba(255,255,255,.06);display:flex;align-items:center;justify-content:center;font-weight:900}
.p6-risk-main{flex:1}
.p6-risk-top{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:8px}
.p6-risk-top.compact{margin-bottom:5px}
.p6-risk-ticker{font-size:18px;font-weight:900}
.p6-risk-ticker.compact{font-size:16px}
.p6-risk-text{font-size:14px;line-height:1.55;margin-bottom:6px;color:#dbe7f7}
.p6-risk-text.compact{font-size:12px;line-height:1.4;margin-bottom:0}
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

    # ── Camada principal orientada à decisão ─────────────────────────────────
    _render_decision_cycle(analysis, stats)

    # ── LLM portfolio report (optional) ──────────────────────────────────────
    portfolio_report = run_portfolio_llm_report(llm_factory, analysis, analysis_mode)

    if portfolio_report:
        mode_label = "Análise Rígida" if analysis_mode == "rigid" else "Análise Flexível"
        _render_structured_portfolio_report(portfolio_report, mode_label, analysis)
    else:
        st.markdown("## 🧠 Resumo Executivo")
        st.write(
            f"O portfólio apresenta distribuição de leituras em **{stats.fortes}** ativo(s) forte, "
            f"**{stats.moderadas}** moderado(s) e **{stats.fracas}** fraco(s). "
            "Use a decisão do ciclo, os riscos principais e a alocação sugerida como eixo da tomada de decisão."
        )

    with st.expander("📌 Ver faixas de alocação", expanded=False):
        _render_allocation_section(analysis.allocation_rows)

    # ── Per-company detail ────────────────────────────────────────────────────
    if show_company_details:
        st.markdown("## 🏢 Relatórios por Empresa")
        for company in analysis.companies.values():
            _render_company_expander(company)

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
