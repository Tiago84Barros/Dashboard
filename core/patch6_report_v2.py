from __future__ import annotations

import html
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from core.patch6_analysis import build_portfolio_analysis, strip_html
from core.patch6_report import _render_company_expander, _render_structured_portfolio_report
from core.patch6_schema import CompanyAnalysis, PortfolioAnalysis
from core.patch6_service import run_portfolio_llm_report


_V2_CSS = """
<style>
:root {
  --p6-bg: #0f172a;
  --p6-panel: #111827;
  --p6-panel-soft: #1e293b;
  --p6-border: rgba(148,163,184,0.18);
  --p6-text: #e5eef9;
  --p6-muted: #94a3b8;
  --p6-green: #22c55e;
  --p6-yellow: #eab308;
  --p6-red: #ef4444;
  --p6-blue: #38bdf8;
}
.p6v2-hero {
  background: linear-gradient(135deg, rgba(15,23,42,0.98), rgba(30,41,59,0.96));
  border: 1px solid var(--p6-border);
  border-radius: 22px;
  padding: 22px 24px;
  margin-bottom: 14px;
  box-shadow: 0 14px 32px rgba(2,6,23,0.28);
}
.p6v2-title { color: var(--p6-text); font-size: 28px; font-weight: 900; margin: 0 0 6px 0; }
.p6v2-subtitle { color: var(--p6-muted); font-size: 13px; margin-bottom: 14px; }
.p6v2-chip-row { display:flex; gap:8px; flex-wrap:wrap; }
.p6v2-chip {
  display:inline-flex; align-items:center; gap:6px;
  border-radius:999px; padding:6px 11px; font-size:12px; font-weight:700;
  border:1px solid transparent;
}
.p6v2-chip.green { background: rgba(34,197,94,0.12); color:#bbf7d0; border-color:rgba(34,197,94,0.30); }
.p6v2-chip.yellow { background: rgba(234,179,8,0.12); color:#fde68a; border-color:rgba(234,179,8,0.30); }
.p6v2-chip.red { background: rgba(239,68,68,0.12); color:#fecaca; border-color:rgba(239,68,68,0.30); }
.p6v2-chip.blue { background: rgba(56,189,248,0.12); color:#bae6fd; border-color:rgba(56,189,248,0.30); }
.p6v2-section-title {
  color: var(--p6-text);
  font-size: 19px;
  font-weight: 900;
  margin: 18px 0 10px 0;
}
.p6v2-card {
  border: 1px solid var(--p6-border);
  background: linear-gradient(180deg, rgba(17,24,39,0.98), rgba(15,23,42,0.98));
  border-radius: 18px;
  padding: 16px 18px;
  min-height: 132px;
  box-shadow: 0 10px 24px rgba(2,6,23,0.20);
}
.p6v2-card.kpi { min-height: 118px; }
.p6v2-card.decision { min-height: 170px; }
.p6v2-card.green { border-color: rgba(34,197,94,0.32); box-shadow: inset 0 0 0 1px rgba(34,197,94,0.08); }
.p6v2-card.yellow { border-color: rgba(234,179,8,0.32); box-shadow: inset 0 0 0 1px rgba(234,179,8,0.08); }
.p6v2-card.red { border-color: rgba(239,68,68,0.32); box-shadow: inset 0 0 0 1px rgba(239,68,68,0.08); }
.p6v2-card.blue { border-color: rgba(56,189,248,0.32); box-shadow: inset 0 0 0 1px rgba(56,189,248,0.08); }
.p6v2-label { font-size: 12px; letter-spacing: .3px; color: var(--p6-muted); margin-bottom: 6px; }
.p6v2-value { color: var(--p6-text); font-size: 28px; font-weight: 900; line-height:1.05; margin-bottom: 8px; }
.p6v2-value.small { font-size: 22px; }
.p6v2-body { color: var(--p6-text); font-size: 14px; line-height: 1.55; }
.p6v2-muted { color: var(--p6-muted); font-size: 12px; line-height: 1.45; }
.p6v2-list { margin: 8px 0 0 0; padding-left: 18px; }
.p6v2-list li { color: var(--p6-text); margin-bottom: 4px; }
.p6v2-risk-row {
  display:flex; justify-content:space-between; gap:14px;
  border:1px solid var(--p6-border); border-radius:14px; padding:12px 14px; margin-bottom:8px;
  background: rgba(15,23,42,0.52);
}
.p6v2-risk-left { color: var(--p6-text); font-size: 14px; font-weight: 800; }
.p6v2-risk-right { color: var(--p6-muted); font-size: 12px; text-align:right; max-width: 48%; }
.p6v2-company {
  border:1px solid var(--p6-border); border-radius:18px; padding:16px 18px;
  background: linear-gradient(180deg, rgba(17,24,39,0.98), rgba(15,23,42,0.98));
  margin-bottom: 12px;
}
.p6v2-company-top { display:flex; justify-content:space-between; gap:10px; align-items:flex-start; margin-bottom:10px; }
.p6v2-company-title { color:var(--p6-text); font-size:22px; font-weight:900; }
.p6v2-company-sub { color:var(--p6-muted); font-size:12px; }
.p6v2-pill {
  display:inline-flex; align-items:center; gap:6px; padding:5px 9px; border-radius:999px;
  font-size:11px; font-weight:800; margin-right:6px; margin-bottom:6px;
}
.p6v2-pill.green { background: rgba(34,197,94,0.12); color:#bbf7d0; }
.p6v2-pill.yellow { background: rgba(234,179,8,0.12); color:#fde68a; }
.p6v2-pill.red { background: rgba(239,68,68,0.12); color:#fecaca; }
.p6v2-pill.blue { background: rgba(56,189,248,0.12); color:#bae6fd; }
.p6v2-grid { display:grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.p6v2-panel {
  border:1px solid rgba(148,163,184,0.14);
  background: rgba(255,255,255,0.03);
  border-radius: 14px;
  padding: 13px 14px;
}
.p6v2-panel-label { font-size:12px; font-weight:800; color:var(--p6-muted); margin-bottom:5px; }
.p6v2-panel-body { color:var(--p6-text); font-size:14px; line-height:1.55; }
@media (max-width: 900px) {
  .p6v2-grid { grid-template-columns: 1fr; }
}
</style>
"""


def _esc(value: Any) -> str:
    return html.escape(strip_html(value))


def _pick_first_text(*values: Any) -> str:
    for value in values:
        txt = strip_html(value)
        if txt:
            return txt
    return ""


def _listify(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [strip_html(v) for v in value if strip_html(v)]
    if isinstance(value, str):
        txt = strip_html(value)
        return [txt] if txt else []
    return []


def _attention_tone(level: str) -> str:
    level = (level or "").strip().lower()
    if level == "alta":
        return "red"
    if level == "média":
        return "yellow"
    return "green"


def _decision_bucket(company: CompanyAnalysis) -> str:
    level = (company.attention_level or "").lower()
    action = (company.recommended_action or "").lower()
    perspectiva = (company.perspectiva_compra or "").lower()
    exec_trend = (company.execution_trend or "").lower()
    forward_dir = (company.forward_direction or "").lower()

    if level == "alta" or any(x in action for x in ["reduz", "diminu", "cortar"]):
        return "reduce"
    if perspectiva == "forte" and level != "alta" and forward_dir != "deteriorando" and exec_trend != "deteriorando":
        return "increase"
    if perspectiva == "fraca":
        return "reduce"
    return "maintain"


def _decision_meta(company: CompanyAnalysis) -> Tuple[str, str, str]:
    bucket = _decision_bucket(company)
    if bucket == "increase":
        return (
            "Aumentar",
            "green",
            _pick_first_text(
                company.recommended_action,
                company.racional_alocacao,
                "Leitura atual mais favorável dentro do portfólio.",
            ),
        )
    if bucket == "reduce":
        return (
            "Reduzir",
            "red",
            _pick_first_text(
                company.recommended_action,
                company.fragilidade_regime_atual,
                "Leitura atual mais frágil e com risco de piora.",
            ),
        )
    return (
        "Manter",
        "yellow",
        _pick_first_text(
            company.recommended_action,
            company.racional_alocacao,
            "Caso ainda investível, mas exigindo confirmação do próximo ciclo.",
        ),
    )


def _leitura_atual(company: CompanyAnalysis) -> str:
    return _pick_first_text(
        company.leitura,
        company.tese,
        company.raw.get("resumo") if isinstance(company.raw, dict) else "",
        company.consideracoes,
        "Sem leitura consolidada disponível.",
    )


def _risco_principal(company: CompanyAnalysis) -> str:
    riscos = _listify(company.riscos)
    return _pick_first_text(
        riscos[0] if riscos else "",
        company.fragilidade_regime_atual,
        (company.contradicoes[0] if company.contradicoes else ""),
        "Sem risco dominante explícito no recorte atual.",
    )


def _monitorar_principal(company: CompanyAnalysis) -> str:
    monitorar = _listify(company.monitorar)
    return _pick_first_text(
        monitorar[0] if monitorar else "",
        (company.forward_drivers[0] if company.forward_drivers else ""),
        "Acompanhar o próximo ciclo para validar a leitura atual.",
    )


def _papel_carteira(company: CompanyAnalysis) -> str:
    return _pick_first_text(
        company.papel_estrategico,
        company.racional_alocacao,
        "Função estratégica ainda não detalhada no material salvo.",
    )


def _decision_cards(companies: Dict[str, CompanyAnalysis]) -> Dict[str, List[str]]:
    buckets = {"increase": [], "maintain": [], "reduce": []}
    for ticker, company in companies.items():
        buckets[_decision_bucket(company)].append(ticker)
    for key in buckets:
        buckets[key] = sorted(buckets[key])
    return buckets


def _aggregate_labels(analysis: PortfolioAnalysis) -> Dict[str, str]:
    decisions = _decision_cards(analysis.companies)
    high_risk = sum(1 for c in analysis.companies.values() if (c.attention_level or "").lower() == "alta")
    deteriorating = sum(
        1
        for c in analysis.companies.values()
        if (c.forward_direction or c.execution_trend or "").lower() == "deteriorando"
    )

    if decisions["reduce"] and len(decisions["reduce"]) >= max(1, len(analysis.companies) // 3):
        carteira_hoje = "Boa, mas com fragilidades relevantes"
    elif decisions["increase"] and len(decisions["increase"]) >= len(decisions["reduce"]):
        carteira_hoje = "Construtiva e relativamente equilibrada"
    else:
        carteira_hoje = "Mista, com seletividade maior"

    if high_risk >= 2:
        postura = "Mais defensiva"
    elif deteriorating >= 2:
        postura = "Cautelosa"
    else:
        postura = "Construtiva"

    ranking = analysis.priority_ranking or list(analysis.companies.keys())
    maior_ponto = _risco_principal(analysis.companies[ranking[0]]) if ranking else "Sem risco relevante destacado"
    monitorar = _monitorar_principal(analysis.companies[ranking[0]]) if ranking else "Acompanhar o próximo ciclo."

    return {
        "carteira_hoje": carteira_hoje,
        "postura": postura,
        "maior_ponto": maior_ponto,
        "monitorar": monitorar,
    }


def _macro_impact_text(report: Optional[Dict[str, Any]]) -> Tuple[List[str], str]:
    if not report:
        return ([], "Sem leitura macro consolidada")

    bullets: List[str] = []
    for key in [
        "cenario_macro_atual",
        "leitura_macro",
        "vulnerabilidades_carteira_regime_atual",
        "o_que_carteira_aposta_implicitamente",
    ]:
        value = report.get(key)
        if isinstance(value, str) and strip_html(value):
            bullets.append(strip_html(value))
        elif isinstance(value, list):
            bullets.extend([strip_html(x) for x in value if strip_html(x)])
    bullets = bullets[:3]

    impact = "Neutro"
    corpus = " ".join(bullets).lower()
    if any(k in corpus for k in ["pressiona", "desafio", "cautela", "deteriora"]):
        impact = "Levemente negativo"
    if any(k in corpus for k in ["favorece", "beneficia", "resiliência", "oportunidade"]):
        impact = "Misto com viés seletivo"
    return bullets, impact


def _render_hero(analysis: PortfolioAnalysis, mode_label: str) -> None:
    agg = _aggregate_labels(analysis)
    st.markdown(
        _V2_CSS +
        f"""
<div class="p6v2-hero">
  <div class="p6v2-title">🧭 Relatório Estratégico do Portfólio</div>
  <div class="p6v2-subtitle">Modo utilizado: {_esc(mode_label)} • Período: {_esc(analysis.period_ref)} • Cobertura: {_esc(analysis.cobertura)}</div>
  <div class="p6v2-chip-row">
    <span class="p6v2-chip green">📌 Carteira hoje: {_esc(agg['carteira_hoje'])}</span>
    <span class="p6v2-chip yellow">🧭 Postura: {_esc(agg['postura'])}</span>
    <span class="p6v2-chip red">⚠️ Atenção principal: {_esc(agg['maior_ponto'])}</span>
    <span class="p6v2-chip blue">🔎 Cobertura: {_esc(analysis.cobertura)}</span>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )


def _render_decision_row(analysis: PortfolioAnalysis) -> None:
    buckets = _decision_cards(analysis.companies)
    c1, c2, c3 = st.columns(3)
    cards = [
        (c1, "🟢 Aumentar", "green", buckets["increase"], "Ativos com leitura mais favorável para reforço seletivo."),
        (c2, "🟡 Manter", "yellow", buckets["maintain"], "Casos ainda válidos, mas sem gatilho forte para aumento."),
        (c3, "🔴 Reduzir", "red", buckets["reduce"], "Ativos com risco mais sensível ou visibilidade pior no ciclo."),
    ]
    for col, title, tone, tickers, reason in cards:
        ticker_text = ", ".join(tickers) if tickers else "Nenhum ativo nesta faixa"
        col.markdown(
            f"""
<div class="p6v2-card decision {tone}">
  <div class="p6v2-label">Decisão do ciclo</div>
  <div class="p6v2-value small">{_esc(title)}</div>
  <div class="p6v2-body"><strong>{_esc(ticker_text)}</strong></div>
  <div class="p6v2-muted" style="margin-top:8px">{_esc(reason)}</div>
</div>
""",
            unsafe_allow_html=True,
        )


def _render_kpis(analysis: PortfolioAnalysis) -> None:
    agg = _aggregate_labels(analysis)
    ranking = analysis.priority_ranking or list(analysis.companies.keys())
    top_company = analysis.companies[ranking[0]] if ranking else None
    cards = [
        ("Carteira hoje", agg["carteira_hoje"], "green"),
        ("Postura sugerida", agg["postura"], "yellow"),
        ("Maior ponto de atenção", agg["maior_ponto"], "red"),
        (
            "O que monitorar",
            _monitorar_principal(top_company) if top_company else "Acompanhar próximo ciclo.",
            "blue",
        ),
    ]
    cols = st.columns(4)
    for col, (label, value, tone) in zip(cols, cards):
        col.markdown(
            f"""
<div class="p6v2-card kpi {tone}">
  <div class="p6v2-label">{_esc(label)}</div>
  <div class="p6v2-value small">{_esc(value)}</div>
  <div class="p6v2-muted">Resumo executivo do ciclo atual.</div>
</div>
""",
            unsafe_allow_html=True,
        )


def _render_macro_and_risk(analysis: PortfolioAnalysis, report: Optional[Dict[str, Any]]) -> None:
    left, right = st.columns([1.15, 1.0])
    bullets, impact = _macro_impact_text(report)
    with left:
        st.markdown('<div class="p6v2-section-title">🌍 Impacto macro na carteira</div>', unsafe_allow_html=True)
        items_html = ''.join(f'<li>{_esc(item)}</li>' for item in bullets) if bullets else '<li>Leitura macro não disponível no relatório consolidado.</li>'
        st.markdown(
            f"""
<div class="p6v2-card blue">
  <div class="p6v2-label">Impacto líquido</div>
  <div class="p6v2-value small">{_esc(impact)}</div>
  <ul class="p6v2-list">{items_html}</ul>
</div>
""",
            unsafe_allow_html=True,
        )
    with right:
        st.markdown('<div class="p6v2-section-title">⚠️ Onde estão os principais riscos</div>', unsafe_allow_html=True)
        ranking = analysis.priority_ranking or list(analysis.companies.keys())
        for idx, tk in enumerate(ranking[:5], start=1):
            company = analysis.companies[tk]
            tone = _attention_tone(company.attention_level)
            st.markdown(
                f"""
<div class="p6v2-risk-row">
  <div class="p6v2-risk-left">{idx}. {_esc(tk)} <span class="p6v2-pill {tone}">{_esc((company.attention_level or 'baixa').upper())}</span></div>
  <div class="p6v2-risk-right">{_esc(_risco_principal(company))}</div>
</div>
""",
                unsafe_allow_html=True,
            )


def _render_action_table(analysis: PortfolioAnalysis) -> None:
    st.markdown('<div class="p6v2-section-title">📌 Mapa de ação por ativo</div>', unsafe_allow_html=True)
    rows: List[Dict[str, Any]] = []
    for tk, company in sorted(analysis.companies.items()):
        decision_label, _, _reason = _decision_meta(company)
        rows.append(
            {
                "Ativo": tk,
                "Decisão sugerida": decision_label,
                "Leitura atual": _leitura_atual(company),
                "Risco principal": _risco_principal(company),
                "O que monitorar": _monitorar_principal(company),
                "Papel na carteira": _papel_carteira(company),
            }
        )
    st.dataframe(rows, use_container_width=True, hide_index=True)


def _render_llm_summary(report: Optional[Dict[str, Any]], mode_label: str) -> None:
    if not report:
        return

    st.markdown('<div class="p6v2-section-title">🧠 Resumo executivo</div>', unsafe_allow_html=True)
    diagnosis = _pick_first_text(
        report.get("diagnostico_executivo"),
        report.get("insight_final"),
        report.get("identidade_carteira"),
    )
    plan = report.get("plano_de_acao") or []
    if diagnosis:
        st.markdown(
            f"""
<div class="p6v2-card blue">
  <div class="p6v2-body">{_esc(diagnosis)}</div>
</div>
""",
            unsafe_allow_html=True,
        )
    if isinstance(plan, list) and any(strip_html(x) for x in plan):
        items = ''.join(f'<li>{_esc(x)}</li>' for x in plan if strip_html(x))
        st.markdown(
            f"""
<div class="p6v2-card blue" style="margin-top:10px;">
  <div class="p6v2-label">Plano de ação</div>
  <ul class="p6v2-list">{items}</ul>
</div>
""",
            unsafe_allow_html=True,
        )
    with st.expander("Ver relatório narrativo completo da LLM", expanded=False):
        _render_structured_portfolio_report(report, mode_label)


def _render_company_cards(analysis: PortfolioAnalysis) -> None:
    st.markdown('<div class="p6v2-section-title">🏢 Leitura por empresa</div>', unsafe_allow_html=True)
    ordered = list(analysis.priority_ranking or [])
    for tk in analysis.companies:
        if tk not in ordered:
            ordered.append(tk)

    for tk in ordered:
        company = analysis.companies[tk]
        decision_label, decision_tone, decision_reason = _decision_meta(company)
        created_at = _pick_first_text(company.created_at, "—")
        st.markdown(
            f"""
<div class="p6v2-company">
  <div class="p6v2-company-top">
    <div>
      <div class="p6v2-company-title">{_esc(tk)}</div>
      <div class="p6v2-company-sub">Período: {_esc(company.period_ref or analysis.period_ref)} • Atualizado em: {_esc(created_at)}</div>
    </div>
    <div>
      <span class="p6v2-pill {decision_tone}">{_esc(decision_label)}</span>
      <span class="p6v2-pill blue">Prioridade: {_esc((company.attention_level or 'baixa').upper())}</span>
      <span class="p6v2-pill blue">Perspectiva: {_esc((company.perspectiva_compra or '—').upper())}</span>
    </div>
  </div>
  <div class="p6v2-grid">
    <div class="p6v2-panel">
      <div class="p6v2-panel-label">Leitura atual</div>
      <div class="p6v2-panel-body">{_esc(_leitura_atual(company))}</div>
    </div>
    <div class="p6v2-panel">
      <div class="p6v2-panel-label">Risco principal</div>
      <div class="p6v2-panel-body">{_esc(_risco_principal(company))}</div>
    </div>
    <div class="p6v2-panel">
      <div class="p6v2-panel-label">O que monitorar</div>
      <div class="p6v2-panel-body">{_esc(_monitorar_principal(company))}</div>
    </div>
    <div class="p6v2-panel">
      <div class="p6v2-panel-label">Papel na carteira</div>
      <div class="p6v2-panel-body">{_esc(_papel_carteira(company))}</div>
    </div>
  </div>
  <div class="p6v2-muted" style="margin-top:10px;">Motivo da decisão: {_esc(decision_reason)}</div>
</div>
""",
            unsafe_allow_html=True,
        )
        with st.expander(f"Ver análise completa — {tk}", expanded=False):
            _render_company_expander(company)


def _render_action_plan(analysis: PortfolioAnalysis) -> None:
    decisions = _decision_cards(analysis.companies)
    actions: List[str] = []
    if decisions["reduce"]:
        actions.append(f"Reduzir exposição em {', '.join(decisions['reduce'][:3])}.")
    if decisions["increase"]:
        actions.append(f"Reforçar alocação em {', '.join(decisions['increase'][:3])}.")
    if decisions["maintain"]:
        actions.append(f"Manter sob vigilância: {', '.join(decisions['maintain'][:4])}.")
    if not actions:
        actions.append("Sem mudança relevante de alocação sugerida no ciclo atual.")

    st.markdown('<div class="p6v2-section-title">🎯 Plano de ação</div>', unsafe_allow_html=True)
    items = ''.join(f'<li>{_esc(item)}</li>' for item in actions)
    st.markdown(
        f"""
<div class="p6v2-card blue">
  <ul class="p6v2-list">{items}</ul>
</div>
""",
        unsafe_allow_html=True,
    )


def render_patch6_report_v2(
    tickers: List[str],
    period_ref: str,
    llm_factory: Optional[Any] = None,
    show_company_details: bool = True,
    analysis_mode: str = "rigid",
    show_legacy_structured_report: bool = False,
) -> None:
    analysis = build_portfolio_analysis(tickers, period_ref)
    if analysis is None or not analysis.companies:
        st.warning(
            "Não há execuções salvas em patch6_runs para este period_ref e tickers do portfólio. "
            "Rode a LLM e salve os resultados primeiro."
        )
        return

    mode_label = "Análise Rígida" if analysis_mode == "rigid" else "Análise Flexível"
    portfolio_report = run_portfolio_llm_report(llm_factory, analysis, analysis_mode)

    _render_hero(analysis, mode_label)
    _render_decision_row(analysis)
    _render_kpis(analysis)
    _render_macro_and_risk(analysis, portfolio_report)
    _render_action_table(analysis)

    if portfolio_report and not show_legacy_structured_report:
        _render_llm_summary(portfolio_report, mode_label)
    elif show_legacy_structured_report and portfolio_report:
        with st.expander("Ver relatório narrativo completo da LLM", expanded=False):
            _render_structured_portfolio_report(portfolio_report, mode_label)

    if show_company_details:
        _render_company_cards(analysis)

    _render_action_plan(analysis)


render_patch6_report_v2_real = render_patch6_report_v2
