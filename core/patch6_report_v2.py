from __future__ import annotations

import html
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from core.patch6_analysis import build_portfolio_analysis, strip_html
from core.patch6_schema import CompanyAnalysis, PortfolioAnalysis
from core.patch6_service import run_portfolio_llm_report


def _esc(value: Any) -> str:
    return html.escape(strip_html(value))


def _fmt_pct01(value: float) -> str:
    try:
        return f"{max(0.0, min(1.0, float(value))) * 100:.0f}%"
    except Exception:
        return "—"


def _fmt_score(value: Any) -> str:
    try:
        return f"{int(float(value))}/100"
    except Exception:
        return "—"


def _decision_for_company(c: CompanyAnalysis) -> Tuple[str, int, str]:
    score = int(c.score_qualitativo or 0)
    conf = float(c.confianca or 0.0)
    forward = int(c.forward_score or 0)
    attn = float(c.attention_score or 0.0)
    robust = float(c.robustez_qualitativa or 0.0)
    direction = (c.forward_direction or c.execution_trend or "—").lower()
    perspectiva = (c.perspectiva_compra or "").lower()

    decision_score = 0
    reasons: List[str] = []

    if score >= 78:
        decision_score += 1
        reasons.append("score qualitativo alto")
    elif score <= 72:
        decision_score -= 1
        reasons.append("score qualitativo pressionado")

    if conf >= 0.82:
        decision_score += 1
        reasons.append("confiança alta")
    elif conf <= 0.65:
        decision_score -= 1
        reasons.append("confiança limitada")

    if forward >= 58:
        decision_score += 1
        reasons.append("forward favorável")
    elif forward and forward <= 48:
        decision_score -= 1
        reasons.append("forward fraco")

    if direction == "melhorando":
        decision_score += 1
        reasons.append("tendência melhorando")
    elif direction == "deteriorando":
        decision_score -= 1
        reasons.append("tendência deteriorando")

    if attn >= 70 or c.attention_level == "alta":
        decision_score -= 2
        reasons.append("alta prioridade de acompanhamento")
    elif attn >= 45:
        decision_score -= 1
        reasons.append("atenção elevada")

    if robust >= 0.82:
        decision_score += 1
        reasons.append("robustez qualitativa")
    elif robust and robust <= 0.58:
        decision_score -= 1
        reasons.append("robustez limitada")

    if perspectiva == "forte":
        decision_score += 1
    elif perspectiva == "fraca":
        decision_score -= 1

    decision_score = max(-2, min(2, decision_score))

    if decision_score >= 2:
        return "Aumentar", decision_score, ", ".join(reasons[:3]) or "convicção favorável"
    if decision_score <= -2:
        return "Reduzir", decision_score, ", ".join(reasons[:3]) or "riscos acima do aceitável"
    return "Manter", decision_score, ", ".join(reasons[:3]) or "sinais mistos"


def _aggregate_status(analysis: PortfolioAnalysis) -> Dict[str, str]:
    companies = list(analysis.companies.values())
    if not companies:
        return {
            "qualidade": "—",
            "execucao": "—",
            "risco": "—",
            "conviccao": "—",
        }

    avg_robust = sum(float(c.robustez_qualitativa or 0.0) for c in companies) / len(companies)
    avg_conf = sum(float(c.confianca or 0.0) for c in companies) / len(companies)
    avg_forward = sum(int(c.forward_score or 0) for c in companies if (c.forward_score or 0) > 0)
    avg_forward = avg_forward / max(1, len([c for c in companies if (c.forward_score or 0) > 0]))
    high_risk = len([c for c in companies if c.attention_level == "alta" or (c.attention_score or 0) >= 70])
    deteriorating = len([c for c in companies if (c.forward_direction or c.execution_trend) == "deteriorando"])

    qualidade = "Estável" if avg_robust >= 0.72 else "Em revisão"
    execucao = "Deteriorando" if deteriorating >= max(1, len(companies)//3) else "Estável"
    if high_risk >= 2:
        risco = "Em alta"
    elif high_risk == 1:
        risco = "Atenção"
    else:
        risco = "Controlado"

    if avg_conf >= 0.85 and avg_forward >= 55:
        conv = "Alta"
    elif avg_conf >= 0.72:
        conv = "Moderada"
    else:
        conv = "Baixa"

    return {
        "qualidade": qualidade,
        "execucao": execucao,
        "risco": risco,
        "conviccao": conv,
    }


def _risk_rows(analysis: PortfolioAnalysis) -> List[Tuple[str, str, str]]:
    rows = []
    for c in analysis.companies.values():
        risk_text = c.riscos[0] if c.riscos else (c.fragilidade_regime_atual or "Sem risco explícito dominante")
        level = "alto" if c.attention_level == "alta" or (c.attention_score or 0) >= 70 else (
            "médio" if c.attention_level == "média" or (c.attention_score or 0) >= 40 else "controlado"
        )
        rows.append((c.ticker, level, strip_html(risk_text)))
    order = {"alto": 0, "médio": 1, "controlado": 2}
    return sorted(rows, key=lambda x: (order.get(x[1], 9), x[0]))


def _color_chip(label: str) -> str:
    mapping = {
        "Aumentar": ("#166534", "#dcfce7", "#22c55e"),
        "Manter": ("#854d0e", "#fef9c3", "#eab308"),
        "Reduzir": ("#991b1b", "#fee2e2", "#ef4444"),
        "alto": ("#991b1b", "#fee2e2", "#ef4444"),
        "médio": ("#854d0e", "#fef9c3", "#eab308"),
        "controlado": ("#166534", "#dcfce7", "#22c55e"),
    }
    fg, bg, border = mapping.get(label, ("#334155", "#f8fafc", "#cbd5e1"))
    return (
        f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;"
        f"background:{bg};color:{fg};border:1px solid {border};font-weight:700;font-size:12px'>{label}</span>"
    )


_P6_V2_CSS = """
<style>
.p6v2-hero{padding:16px 18px;border-radius:18px;background:linear-gradient(135deg,#0f172a 0%,#1e293b 100%);color:white;border:1px solid rgba(255,255,255,.08);margin-bottom:14px}
.p6v2-title{font-size:28px;font-weight:800;margin-bottom:4px}
.p6v2-sub{font-size:13px;opacity:.8}
.p6v2-card{border:1px solid #e2e8f0;background:#ffffff;border-radius:18px;padding:16px;box-shadow:0 10px 24px rgba(15,23,42,.06);height:100%}
.p6v2-card h4{margin:0 0 8px 0;font-size:15px}
.p6v2-kpi{border:1px solid #e2e8f0;background:#f8fafc;border-radius:16px;padding:12px 14px;min-height:92px}
.p6v2-kpi-label{font-size:12px;color:#475569;margin-bottom:6px}
.p6v2-kpi-value{font-size:24px;font-weight:800;color:#0f172a}
.p6v2-small{font-size:12px;color:#64748b}
.p6v2-company{border:1px solid #e2e8f0;background:#fff;border-radius:18px;padding:16px;box-shadow:0 10px 24px rgba(15,23,42,.05)}
.p6v2-muted{color:#64748b}
</style>
"""


def render_patch6_report_v2(
    tickers: List[str],
    period_ref: str,
    llm_factory: Optional[Any] = None,
    show_company_details: bool = True,
    analysis_mode: str = "rigid",
) -> None:
    st.markdown(_P6_V2_CSS, unsafe_allow_html=True)

    analysis = build_portfolio_analysis(tickers, period_ref)
    if analysis is None or not analysis.companies:
        st.warning(
            "Não há execuções salvas em patch6_runs para este period_ref e tickers do portfólio. "
            "Rode a LLM e salve os resultados primeiro."
        )
        return

    llm_report = run_portfolio_llm_report(llm_factory, analysis, analysis_mode)
    status = _aggregate_status(analysis)
    decisions: Dict[str, List[Tuple[CompanyAnalysis, int, str]]] = {"Aumentar": [], "Manter": [], "Reduzir": []}
    for c in analysis.companies.values():
        label, score, reason = _decision_for_company(c)
        decisions[label].append((c, score, reason))

    st.markdown(
        f"""
        <div class='p6v2-hero'>
          <div class='p6v2-title'>🧭 Relatório Estratégico do Portfólio — V2</div>
          <div class='p6v2-sub'>Modo utilizado: {'Análise Rígida' if analysis_mode == 'rigid' else 'Análise Flexível'} • Período: {_esc(period_ref)} • Cobertura: {_esc(analysis.cobertura)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### Decisão do ciclo")
    cols = st.columns(3)
    for col, key in zip(cols, ["Aumentar", "Manter", "Reduzir"]):
        items = decisions[key]
        bullet = "<br/>".join([f"<b>{c.ticker}</b> <span class='p6v2-muted'>— {html.escape(reason)}</span>" for c, _, reason in items]) or "<span class='p6v2-muted'>—</span>"
        with col:
            st.markdown(
                f"<div class='p6v2-card'><h4>{_color_chip(key)}</h4><div style='margin-top:10px;line-height:1.55'>{bullet}</div></div>",
                unsafe_allow_html=True,
            )

    st.markdown("### Status da carteira")
    k1, k2, k3, k4 = st.columns(4)
    kpis = [
        (k1, "Qualidade", status["qualidade"], "robustez média da carteira"),
        (k2, "Execução", status["execucao"], "com base em forward/execution trend"),
        (k3, "Risco agregado", status["risco"], "fila de atenção e riscos dominantes"),
        (k4, "Convicção", status["conviccao"], f"confiança média {_fmt_pct01(analysis.confianca_media)}"),
    ]
    for col, label, value, extra in kpis:
        col.markdown(
            f"<div class='p6v2-kpi'><div class='p6v2-kpi-label'>{label}</div><div class='p6v2-kpi-value'>{html.escape(value)}</div><div class='p6v2-small'>{html.escape(extra)}</div></div>",
            unsafe_allow_html=True,
        )

    st.markdown("### Ranking de risco")
    for idx, (ticker, level, text) in enumerate(_risk_rows(analysis), start=1):
        st.markdown(
            f"**{idx}. {ticker}** { _color_chip(level) }  ",
            unsafe_allow_html=True,
        )
        st.caption(text)

    st.markdown("### Mapa de ação por ativo")
    action_rows = []
    for c in analysis.companies.values():
        label, dscore, reason = _decision_for_company(c)
        action_rows.append({
            "Ativo": c.ticker,
            "Score": c.score_qualitativo,
            "Confiança": _fmt_pct01(c.confianca),
            "Forward": _fmt_score(c.forward_score) if c.forward_score else "—",
            "Execução": c.execution_trend or "—",
            "Decisão": label,
            "Motivo central": reason,
        })
    st.dataframe(action_rows, use_container_width=True, hide_index=True)

    if llm_report:
        st.markdown("### Resumo executivo")
        diagnostic = llm_report.get("diagnostico_executivo") or llm_report.get("insight_final") or ""
        st.info(diagnostic)
        plan = llm_report.get("plano_de_acao") or []
        if plan:
            st.markdown("**Plano de ação**")
            for item in plan[:5]:
                st.markdown(f"- {strip_html(item)}")

    if show_company_details:
        st.markdown("### Empresas")
        ordered = sorted(analysis.companies.values(), key=lambda c: (_decision_for_company(c)[1], c.score_qualitativo), reverse=True)
        for c in ordered:
            label, _, reason = _decision_for_company(c)
            with st.container():
                st.markdown(
                    f"""
                    <div class='p6v2-company'>
                      <div style='display:flex;justify-content:space-between;gap:12px;align-items:flex-start'>
                        <div>
                          <div style='font-size:22px;font-weight:800;color:#0f172a'>{c.ticker}</div>
                          <div class='p6v2-small'>{_esc(c.period_ref)} • Atualizado em: {_esc(c.created_at)}</div>
                        </div>
                        <div>{_color_chip(label)}</div>
                      </div>
                      <div style='margin-top:12px;display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px'>
                        <div class='p6v2-kpi'><div class='p6v2-kpi-label'>Score</div><div class='p6v2-kpi-value'>{c.score_qualitativo}</div><div class='p6v2-small'>{_esc(c.perspectiva_compra or '—')}</div></div>
                        <div class='p6v2-kpi'><div class='p6v2-kpi-label'>Confiança</div><div class='p6v2-kpi-value'>{_fmt_pct01(c.confianca)}</div><div class='p6v2-small'>robustez {_fmt_pct01(c.robustez_qualitativa)}</div></div>
                        <div class='p6v2-kpi'><div class='p6v2-kpi-label'>Forward</div><div class='p6v2-kpi-value'>{c.forward_score or '—'}</div><div class='p6v2-small'>{_esc(c.forward_direction or '—')}</div></div>
                        <div class='p6v2-kpi'><div class='p6v2-kpi-label'>Atenção</div><div class='p6v2-kpi-value'>{int(c.attention_score or 0)}</div><div class='p6v2-small'>{_esc(c.attention_level or '—')}</div></div>
                      </div>
                      <div style='margin-top:12px'><b>Tese:</b> {_esc(c.tese or '—')}</div>
                      <div style='margin-top:6px'><b>Risco principal:</b> {_esc((c.riscos[0] if c.riscos else c.fragilidade_regime_atual) or '—')}</div>
                      <div style='margin-top:6px'><b>Motivo da decisão:</b> {_esc(reason)}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                with st.expander(f"Ver análise completa — {c.ticker}", expanded=False):
                    if c.pontos_chave:
                        st.markdown("**Pontos-chave**")
                        for x in c.pontos_chave[:6]:
                            st.markdown(f"- {strip_html(x)}")
                    if c.catalisadores:
                        st.markdown("**Catalisadores**")
                        for x in c.catalisadores[:5]:
                            st.markdown(f"- {strip_html(x)}")
                    if c.monitorar:
                        st.markdown("**O que monitorar**")
                        for x in c.monitorar[:5]:
                            st.markdown(f"- {strip_html(x)}")
                    if c.evidencias:
                        st.markdown("**Evidências**")
                        for ev in c.evidencias[:8]:
                            if isinstance(ev, dict):
                                trecho = strip_html(ev.get("trecho") or ev.get("texto") or "")
                                leitura = strip_html(ev.get("leitura") or "")
                                ano = strip_html(ev.get("ano") or ev.get("data") or "")
                                prefix = f"**{ano}** — " if ano else ""
                                st.markdown(f"- {prefix}{trecho}")
                                if leitura:
                                    st.caption(leitura)
                            else:
                                st.markdown(f"- {strip_html(ev)}")
                    if c.consideracoes:
                        st.markdown("**Considerações da LLM**")
                        st.write(c.consideracoes)
