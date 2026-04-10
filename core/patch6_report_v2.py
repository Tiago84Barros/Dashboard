from __future__ import annotations

import html
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from core.patch6_analysis import build_portfolio_analysis, strip_html
from core.patch6_schema import CompanyAnalysis, PortfolioAnalysis
from core.patch6_service import run_portfolio_llm_report


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _esc(value: Any) -> str:
    return html.escape(strip_html(value))


def _as_list(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [strip_html(v) for v in value if strip_html(v)]
    if isinstance(value, str) and strip_html(value):
        return [strip_html(value)]
    return []


def _fmt_pct01(value: Any) -> str:
    try:
        return f"{max(0.0, min(1.0, float(value))) * 100:.0f}%"
    except Exception:
        return "—"


def _fmt_num(value: Any, default: str = "—") -> str:
    try:
        return str(int(float(value)))
    except Exception:
        return default


def _chip(label: str, kind: str = "neutral") -> str:
    palette = {
        "buy": ("#166534", "#dcfce7", "#86efac"),
        "hold": ("#854d0e", "#fef3c7", "#fcd34d"),
        "sell": ("#991b1b", "#fee2e2", "#fca5a5"),
        "risk_high": ("#991b1b", "#fee2e2", "#fca5a5"),
        "risk_med": ("#854d0e", "#fef3c7", "#fcd34d"),
        "risk_low": ("#166534", "#dcfce7", "#86efac"),
        "neutral": ("#334155", "#f8fafc", "#cbd5e1"),
        "blue": ("#1d4ed8", "#dbeafe", "#93c5fd"),
    }
    fg, bg, border = palette.get(kind, palette["neutral"])
    return (
        f"<span style='display:inline-block;padding:6px 12px;border-radius:999px;"
        f"background:{bg};color:{fg};border:1px solid {border};"
        f"font-weight:700;font-size:12px;white-space:nowrap'>{html.escape(label)}</span>"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Decision engine
# ──────────────────────────────────────────────────────────────────────────────

def _decision_for_company(c: CompanyAnalysis) -> Tuple[str, int, str]:
    """
    Motor interno de decisão.
    Continua usando métricas técnicas, mas sem expô-las como mensagem principal.
    """
    score = int(getattr(c, "score_qualitativo", 0) or 0)
    conf = float(getattr(c, "confianca", 0.0) or 0.0)
    forward = int(getattr(c, "forward_score", 0) or 0)
    attn = float(getattr(c, "attention_score", 0.0) or 0.0)
    robust = float(getattr(c, "robustez_qualitativa", 0.0) or 0.0)
    direction = (
        getattr(c, "forward_direction", None)
        or getattr(c, "execution_trend", None)
        or ""
    ).strip().lower()
    perspectiva = (getattr(c, "perspectiva_compra", "") or "").strip().lower()

    decision_score = 0
    motivos: List[str] = []

    if score >= 78:
        decision_score += 1
        motivos.append("qualidade qualitativa consistente")
    elif score <= 72:
        decision_score -= 1
        motivos.append("qualidade qualitativa pressionada")

    if conf >= 0.82:
        decision_score += 1
        motivos.append("base analítica mais robusta")
    elif conf <= 0.65:
        decision_score -= 1
        motivos.append("base analítica mais limitada")

    if forward >= 58:
        decision_score += 1
        motivos.append("sinais futuros mais favoráveis")
    elif forward and forward <= 48:
        decision_score -= 1
        motivos.append("sinais futuros mais fracos")

    if direction == "melhorando":
        decision_score += 1
        motivos.append("trajetória melhorando")
    elif direction == "deteriorando":
        decision_score -= 1
        motivos.append("trajetória deteriorando")

    if attn >= 70 or (getattr(c, "attention_level", "") or "").lower() == "alta":
        decision_score -= 2
        motivos.append("risco exige prioridade alta")
    elif attn >= 45:
        decision_score -= 1
        motivos.append("risco pede acompanhamento")

    if robust >= 0.82:
        decision_score += 1
        motivos.append("leitura qualitativa robusta")
    elif robust and robust <= 0.58:
        decision_score -= 1
        motivos.append("leitura qualitativa menos estável")

    if perspectiva == "forte":
        decision_score += 1
    elif perspectiva == "fraca":
        decision_score -= 1

    decision_score = max(-2, min(2, decision_score))

    if decision_score >= 2:
        return "Aumentar", decision_score, ", ".join(motivos[:3]) or "leitura favorece aumento"
    if decision_score <= -2:
        return "Reduzir", decision_score, ", ".join(motivos[:3]) or "leitura favorece redução"
    return "Manter", decision_score, ", ".join(motivos[:3]) or "leitura ainda mista"


def _risk_level(c: CompanyAnalysis) -> str:
    attn = float(getattr(c, "attention_score", 0.0) or 0.0)
    lvl = (getattr(c, "attention_level", "") or "").strip().lower()
    if lvl == "alta" or attn >= 70:
        return "Alto"
    if lvl == "média" or attn >= 40:
        return "Médio"
    return "Controlado"


# ──────────────────────────────────────────────────────────────────────────────
# User-facing text
# ──────────────────────────────────────────────────────────────────────────────

def _leitura_atual(c: CompanyAnalysis) -> str:
    candidates = [
        getattr(c, "tese_investimento", None),
        getattr(c, "tese", None),
        getattr(c, "resumo", None),
        getattr(c, "consideracoes", None),
    ]
    for item in candidates:
        txt = strip_html(item)
        if txt:
            return txt
    return "Sem leitura consolidada disponível para este ativo."


def _risco_principal(c: CompanyAnalysis) -> str:
    risks = _as_list(getattr(c, "riscos", None))
    if risks:
        return risks[0]
    txt = strip_html(getattr(c, "fragilidade_regime_atual", None))
    if txt:
        return txt
    return "Sem risco dominante explícito no recorte atual."


def _o_que_monitorar(c: CompanyAnalysis) -> str:
    items = _as_list(getattr(c, "monitorar", None))
    if items:
        return items[0]
    forward_dir = strip_html(getattr(c, "forward_direction", None))
    if forward_dir:
        return f"Confirmar se a trajetória {forward_dir} se mantém no próximo ciclo."
    return "Acompanhar o próximo ciclo para confirmar a consistência da leitura."


def _papel_na_carteira(c: CompanyAnalysis) -> str:
    candidates = [
        getattr(c, "papel_estrategico", None),
        getattr(c, "role_in_portfolio", None),
        getattr(c, "funcao_na_carteira", None),
    ]
    for item in candidates:
        txt = strip_html(item)
        if txt:
            return txt

    decision, _, _ = _decision_for_company(c)
    risk = _risk_level(c)

    if decision == "Aumentar" and risk == "Controlado":
        return "Âncora de qualidade para reforçar a carteira."
    if decision == "Aumentar":
        return "Posição com espaço para aumento, mas exigindo monitoramento."
    if decision == "Reduzir":
        return "Posição sob revisão, com menor prioridade relativa."
    return "Posição de manutenção até nova confirmação de cenário."


def _company_summary_reason(c: CompanyAnalysis) -> str:
    decision, _, internal_reason = _decision_for_company(c)
    risk = _risco_principal(c).lower()
    leitura = _leitura_atual(c).lower()

    if decision == "Aumentar":
        return "Leitura atual mais favorável, com maior capacidade de sustentar a posição na carteira."
    if decision == "Reduzir":
        return "Leitura atual mais frágil, com risco relevante para a previsibilidade da posição."
    if "deterior" in leitura or "deterior" in internal_reason:
        return "Caso ainda investível, mas exigindo confirmação antes de novo aumento."
    if risk:
        return "Caso equilibrado, mas com ponto de atenção relevante no cenário atual."
    return "Caso ainda neutro, sem gatilho forte para aumento ou redução."

def _aggregate_status(analysis: PortfolioAnalysis) -> Dict[str, str]:
    companies = list(analysis.companies.values())
    if not companies:
        return {
            "carteira_hoje": "Sem dados",
            "postura": "Aguardando atualização",
            "maior_ponto": "Sem leitura agregada",
            "monitoramento": "Sem base suficiente",
        }

    decisions = [_decision_for_company(c)[0] for c in companies]
    high_risk = len([c for c in companies if _risk_level(c) == "Alto"])
    deteriorating = len([
        c for c in companies
        if (getattr(c, "forward_direction", None) or getattr(c, "execution_trend", None) or "").lower() == "deteriorando"
    ])

    if decisions.count("Reduzir") >= max(1, len(companies) // 3):
        carteira_hoje = "Boa, mas com fragilidades relevantes"
    elif decisions.count("Aumentar") >= decisions.count("Reduzir"):
        carteira_hoje = "Construtiva e relativamente equilibrada"
    else:
        carteira_hoje = "Mista, com necessidade de seletividade"

    if high_risk >= 2:
        postura = "Mais defensiva"
    elif deteriorating >= 2:
        postura = "Cautelosa"
    else:
        postura = "Construtiva"

    major_risks = [_risco_principal(c) for c in companies if _risk_level(c) == "Alto"]
    maior_ponto = major_risks[0] if major_risks else "Nenhum risco dominante isolado"

    monitor_items = []
    for c in companies:
        item = _o_que_monitorar(c)
        if item and item not in monitor_items:
            monitor_items.append(item)
    monitoramento = monitor_items[0] if monitor_items else "Acompanhar o próximo ciclo."

    return {
        "carteira_hoje": carteira_hoje,
        "postura": postura,
        "maior_ponto": maior_ponto,
        "monitoramento": monitoramento,
    }


def _risk_rows(analysis: PortfolioAnalysis) -> List[Tuple[str, str, str]]:
    rows = []
    for c in analysis.companies.values():
        rows.append((c.ticker, _risk_level(c), _risco_principal(c)))
    order = {"Alto": 0, "Médio": 1, "Controlado": 2}
    return sorted(rows, key=lambda x: (order.get(x[1], 9), x[0]))


# ──────────────────────────────────────────────────────────────────────────────
# CSS
# ──────────────────────────────────────────────────────────────────────────────

_P6_V2_CSS = """
<style>
.p6v2-hero{
    padding:18px 20px;
    border-radius:20px;
    background:linear-gradient(135deg,#0f172a 0%, #1e293b 100%);
    color:white;
    border:1px solid rgba(255,255,255,.08);
    margin-bottom:16px;
}
.p6v2-title{
    font-size:26px;
    font-weight:800;
    margin-bottom:4px;
    color:#ffffff;
}
.p6v2-sub{
    font-size:13px;
    color:rgba(255,255,255,.78);
}
.p6v2-block{
    border:1px solid #e5e7eb;
    background:#ffffff;
    border-radius:20px;
    padding:16px;
    box-shadow:0 8px 24px rgba(15,23,42,.05);
    height:100%;
}
.p6v2-block-title{
    font-size:14px;
    font-weight:800;
    color:#0f172a;
    margin-bottom:10px;
}
.p6v2-list{
    color:#0f172a;
    font-size:14px;
    line-height:1.6;
}
.p6v2-kpi{
    border:1px solid #e2e8f0;
    background:#f8fafc;
    border-radius:16px;
    padding:14px 16px;
    min-height:110px;
}
.p6v2-kpi-label{
    font-size:12px;
    color:#64748b;
    margin-bottom:8px;
}
.p6v2-kpi-value{
    font-size:20px;
    font-weight:800;
    color:#0f172a;
    line-height:1.25;
}
.p6v2-kpi-sub{
    font-size:13px;
    color:#475569;
    margin-top:6px;
    line-height:1.45;
}
.p6v2-company{
    border:1px solid #e5e7eb;
    background:#ffffff;
    border-radius:20px;
    padding:18px;
    box-shadow:0 8px 24px rgba(15,23,42,.06);
    margin-bottom:14px;
}
.p6v2-head{
    display:flex;
    justify-content:space-between;
    align-items:flex-start;
    gap:12px;
    margin-bottom:12px;
}
.p6v2-ticker{
    font-size:30px;
    font-weight:800;
    color:#0f172a;
    line-height:1;
    margin:0;
}
.p6v2-meta{
    font-size:13px;
    color:#64748b;
    margin-top:6px;
}
.p6v2-grid{
    display:grid;
    grid-template-columns:repeat(2,minmax(0,1fr));
    gap:12px;
    margin-top:12px;
}
.p6v2-panel{
    background:#f8fafc;
    border:1px solid #e2e8f0;
    border-radius:16px;
    padding:14px;
}
.p6v2-panel-label{
    font-size:12px;
    font-weight:800;
    color:#475569;
    text-transform:uppercase;
    letter-spacing:.04em;
    margin-bottom:6px;
}
.p6v2-panel-body{
    font-size:15px;
    line-height:1.6;
    color:#0f172a;
}
.p6v2-panel-risk{
    color:#7f1d1d;
    font-weight:600;
}
.p6v2-table-note{
    font-size:12px;
    color:#64748b;
    margin-top:4px;
}
@media (max-width: 900px){
    .p6v2-grid{
        grid-template-columns:1fr;
    }
    .p6v2-ticker{
        font-size:24px;
    }
}
</style>
"""


# ──────────────────────────────────────────────────────────────────────────────
# Main renderer
# ──────────────────────────────────────────────────────────────────────────────

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

    decisions: Dict[str, List[Tuple[CompanyAnalysis, int, str]]] = {
        "Aumentar": [],
        "Manter": [],
        "Reduzir": [],
    }
    for c in analysis.companies.values():
        label, score, reason = _decision_for_company(c)
        decisions[label].append((c, score, reason))

    st.markdown(
        f"""
        <div class='p6v2-hero'>
          <div class='p6v2-title'>🧭 Relatório Estratégico do Portfólio</div>
          <div class='p6v2-sub'>
            Modo utilizado: {'Análise Rígida' if analysis_mode == 'rigid' else 'Análise Flexível'}
            • Período: {_esc(period_ref)}
            • Cobertura: {_esc(getattr(analysis, 'cobertura', '—'))}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Decisão do ciclo
    st.markdown("### Decisão do ciclo")
    cols = st.columns(3)

    for col, key, kind in zip(
        cols,
        ["Aumentar", "Manter", "Reduzir"],
        ["buy", "hold", "sell"],
    ):
        items = decisions[key]
        lines = []
        for c, _, _reason in items:
            leitura = _leitura_atual(c)
            resumo = leitura[:120] + "..." if len(leitura) > 120 else leitura
            lines.append(f"<b>{_esc(c.ticker)}</b> — {_esc(resumo)}")
        content = "<br/><br/>".join(lines) if lines else "<span style='color:#64748b'>Nenhum ativo nesta faixa.</span>"

        with col:
            st.markdown(
                f"""
                <div class='p6v2-block'>
                    <div class='p6v2-block-title'>{_chip(key, kind)}</div>
                    <div class='p6v2-list'>{content}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # Visão rápida da carteira
    st.markdown("### Visão rápida da carteira")
    k1, k2, k3, k4 = st.columns(4)
    cards = [
        ("Carteira hoje", status["carteira_hoje"], "Leitura sintética do momento da carteira."),
        ("Postura sugerida", status["postura"], "Tom geral da alocação neste ciclo."),
        ("Maior ponto de atenção", status["maior_ponto"], "Risco mais sensível na leitura atual."),
        ("O que monitorar", status["monitoramento"], "Fato que pode mudar a decisão adiante."),
    ]
    for col, (label, value, sub) in zip([k1, k2, k3, k4], cards):
        col.markdown(
            f"""
            <div class='p6v2-kpi'>
                <div class='p6v2-kpi-label'>{_esc(label)}</div>
                <div class='p6v2-kpi-value'>{_esc(value)}</div>
                <div class='p6v2-kpi-sub'>{_esc(sub)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Ranking de risco
    st.markdown("### Onde estão os principais riscos")
    for idx, (ticker, level, text) in enumerate(_risk_rows(analysis), start=1):
        kind = "risk_high" if level == "Alto" else "risk_med" if level == "Médio" else "risk_low"
        st.markdown(
            f"**{idx}. {ticker}** { _chip(level, kind) }",
            unsafe_allow_html=True,
        )
        st.caption(text)

    # Mapa de ação
    st.markdown("### Mapa de ação por ativo")
    action_rows = []
    for c in analysis.companies.values():
        label, _, _reason = _decision_for_company(c)
        action_rows.append({
            "Ativo": c.ticker,
            "Decisão": label,
            "Leitura atual": _leitura_atual(c),
            "Risco principal": _risco_principal(c),
            "O que monitorar": _o_que_monitorar(c),
            "Papel na carteira": _papel_na_carteira(c),
        })
    st.dataframe(action_rows, use_container_width=True, hide_index=True)

    # Resumo executivo da carteira
    if llm_report:
        st.markdown("### Resumo executivo")
        diagnostico = (
            llm_report.get("diagnostico_executivo")
            or llm_report.get("insight_final")
            or ""
        )
        if strip_html(diagnostico):
            st.info(strip_html(diagnostico))

        plano = llm_report.get("plano_de_acao") or []
        plano_clean = [strip_html(x) for x in plano if strip_html(x)]
        if plano_clean:
            st.markdown("**Plano de ação**")
            for item in plano_clean[:5]:
                st.markdown(f"- {item}")

    # Empresas
    if show_company_details:
        st.markdown("### Empresas")
        ordered = sorted(
            analysis.companies.values(),
            key=lambda c: (_decision_for_company(c)[1], getattr(c, "ticker", "")),
            reverse=True,
        )

        for c in ordered:
            label, _, internal_reason = _decision_for_company(c)

            chip_kind = {
                "Aumentar": "buy",
                "Manter": "hold",
                "Reduzir": "sell",
            }.get(label, "neutral")

            leitura_atual = _leitura_atual(c)
            risco_principal = _risco_principal(c)
            monitorar = _o_que_monitorar(c)
            papel = _papel_na_carteira(c)
            motivo_decisao = _company_summary_reason(c)

            st.markdown(
                f"""
                <div class='p6v2-company'>
                    <div class='p6v2-head'>
                        <div>
                            <div class='p6v2-ticker'>{_esc(c.ticker)}</div>
                            <div class='p6v2-meta'>
                                {_esc(getattr(c, 'period_ref', period_ref))} • Atualizado em: {_esc(getattr(c, 'created_at', '—'))}
                            </div>
                        </div>
                        <div>{_chip(label, chip_kind)}</div>
                    </div>

                    <div class='p6v2-grid'>
                        <div class='p6v2-panel'>
                            <div class='p6v2-panel-label'>Leitura atual</div>
                            <div class='p6v2-panel-body'>{_esc(leitura_atual)}</div>
                        </div>

                        <div class='p6v2-panel'>
                            <div class='p6v2-panel-label'>Risco principal</div>
                            <div class='p6v2-panel-body p6v2-panel-risk'>{_esc(risco_principal)}</div>
                        </div>

                        <div class='p6v2-panel'>
                            <div class='p6v2-panel-label'>O que monitorar</div>
                            <div class='p6v2-panel-body'>{_esc(monitorar)}</div>
                        </div>

                        <div class='p6v2-panel'>
                            <div class='p6v2-panel-label'>Papel na carteira</div>
                            <div class='p6v2-panel-body'>{_esc(papel)}</div>
                        </div>
                    </div>

                    <div class='p6v2-table-note' style='margin-top:12px;'>
                        Motivo da decisão: {_esc(motivo_decisao)}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            with st.expander(f"Ver análise completa — {c.ticker}", expanded=False):
                pontos_chave = _as_list(getattr(c, "pontos_chave", None))
                if pontos_chave:
                    st.markdown("**Pontos-chave**")
                    for x in pontos_chave[:6]:
                        st.markdown(f"- {x}")

                catalisadores = _as_list(getattr(c, "catalisadores", None))
                if catalisadores:
                    st.markdown("**Catalisadores**")
                    for x in catalisadores[:5]:
                        st.markdown(f"- {x}")

                monitor_list = _as_list(getattr(c, "monitorar", None))
                if monitor_list:
                    st.markdown("**Monitoramento detalhado**")
                    for x in monitor_list[:5]:
                        st.markdown(f"- {x}")

                evidencias = getattr(c, "evidencias", None) or []
                if evidencias:
                    st.markdown("**Evidências**")
                    for ev in evidencias[:8]:
                        if isinstance(ev, dict):
                            trecho = strip_html(ev.get("trecho") or ev.get("texto") or "")
                            leitura = strip_html(ev.get("leitura") or "")
                            ano = strip_html(ev.get("ano") or ev.get("data") or "")
                            prefix = f"**{ano}** — " if ano else ""
                            if trecho:
                                st.markdown(f"- {prefix}{trecho}")
                            if leitura:
                                st.caption(leitura)
                        else:
                            txt = strip_html(ev)
                            if txt:
                                st.markdown(f"- {txt}")

                consideracoes = strip_html(getattr(c, "consideracoes", None))
                if consideracoes:
                    st.markdown("**Considerações da LLM**")
                    st.write(consideracoes)

                # camada técnica escondida
                st.markdown("---")
                st.markdown("**Detalhes técnicos**")
                tech_rows = [{
                    "Score qualitativo": getattr(c, "score_qualitativo", None),
                    "Confiança": _fmt_pct01(getattr(c, "confianca", None)),
                    "Robustez qualitativa": _fmt_pct01(getattr(c, "robustez_qualitativa", None)),
                    "Forward score": _fmt_num(getattr(c, "forward_score", None)),
                    "Direção forward": strip_html(getattr(c, "forward_direction", None)) or "—",
                    "Attention score": _fmt_num(getattr(c, "attention_score", None)),
                    "Attention level": strip_html(getattr(c, "attention_level", None)) or "—",
                    "Motivo interno": internal_reason,
                }]
                st.dataframe(tech_rows, use_container_width=True, hide_index=True)


# Compatibilidade com nome antigo, caso algum import ainda use "_real"
render_patch6_report_v2_real = render_patch6_report_v2
