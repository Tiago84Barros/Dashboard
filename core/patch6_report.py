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
from typing import Any, Dict, List, Optional

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




def _fmt_macro_value(value: Any, suffix: str = "", decimals: int = 2) -> str:
    try:
        if value is None or value == "":
            return "—"
        num = float(value)
        return f"{num:.{decimals}f}{suffix}"
    except Exception:
        return "—"


def _extract_macro_panel_data(macro: Dict[str, Any]) -> List[tuple]:
    summary = macro.get("macro_summary", {}) if isinstance(macro, dict) else {}
    anual = macro.get("anual", {}) if isinstance(macro, dict) else {}
    return [
        ("Selic atual", _fmt_macro_value(summary.get("selic_current"), "%")),
        ("Dólar atual", _fmt_macro_value(summary.get("cambio_current"))),
        ("IPCA 12m", _fmt_macro_value(summary.get("ipca_12m_current"), "%")),
        ("IPCA anual", _fmt_macro_value(anual.get("ipca"), "%")),
    ]


def _render_macro_panel() -> None:
    macro = st.session_state.get("macro_context_run") or st.session_state.get("macro_context") or {}
    if not isinstance(macro, dict) or not macro:
        return

    cards = _extract_macro_panel_data(macro)
    if not any(value != "—" for _, value in cards):
        return

    st.markdown("## 🌎 Painel Macro")
    _render_metric_cards(cards, columns_per_row=4)

    interpretation = macro.get("macro_interpretation") if isinstance(macro.get("macro_interpretation"), list) else []
    clean_interp = [strip_html(x) for x in interpretation if strip_html(x)]
    if clean_interp:
        _render_section_list("Leitura macro", clean_interp, limit=6)

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


def _render_structured_portfolio_report(report: Dict[str, Any], mode_label: str) -> None:
    st.markdown("## 🧠 Relatório Estratégico do Portfólio")
    st.caption(f"Modo utilizado: {mode_label}")

    _render_section_text("Base analítica", report.get("analytical_basis", ""))
    _render_section_text("Diagnóstico executivo", report.get("executive_summary", ""))
    _render_section_text("Identidade da carteira", report.get("portfolio_identity", ""))
    _render_section_text("Cenário macro atual", report.get("current_market_context", ""))
    _render_section_text("Leitura macro", report.get("macro_reading", ""))
    _render_section_list("Riscos internacionais relevantes", report.get("international_risk_links", []), limit=8)
    _render_section_list("Dependências de cenário macro", report.get("macro_scenario_dependencies", []), limit=8)
    _render_section_list(
        "Vulnerabilidades da carteira sob o regime atual",
        report.get("portfolio_vulnerabilities_under_current_regime", []),
        limit=8,
    )
    _render_section_list(
        "O que a carteira está apostando implicitamente",
        report.get("what_the_portfolio_is_implicitly_betting_on", []),
        limit=8,
    )
    _render_section_text("Análise de concentração econômica", report.get("portfolio_concentration_analysis", ""))
    _render_section_text("Racional de ajuste de alocação", report.get("allocation_adjustment_rationale", ""))
    _render_section_list("Forças principais", report.get("key_strengths", []), limit=8)
    _render_section_list("Fragilidades principais", report.get("key_weaknesses", []), limit=8)
    _render_section_list("Riscos invisíveis", report.get("hidden_risks", []), limit=8)

    asset_roles = report.get("asset_roles", []) or []
    if asset_roles:
        st.markdown("**Papel estratégico dos ativos**")
        for item in asset_roles[:12]:
            if not isinstance(item, dict):
                continue
            ticker = strip_html(item.get("ticker") or "—")
            role = strip_html(item.get("role") or "")
            rationale = strip_html(item.get("rationale") or "")
            st.markdown(
                f"""
                <div style="border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.025);
                            border-radius:12px;padding:12px 14px;margin:8px 0;line-height:1.45;">
                    <div style="font-size:13px;opacity:0.80;margin-bottom:6px;font-weight:700;letter-spacing:.2px;">
                        {_esc(ticker)} {("• " + _esc(role)) if role else ""}
                    </div>
                    <div style="font-size:15px;line-height:1.55;">{_esc(rationale or "—")}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    suggested_allocations = report.get("suggested_allocations", []) or []
    if suggested_allocations:
        st.markdown("**Alocação sugerida (visão estratégica)**")
        for item in suggested_allocations[:15]:
            if not isinstance(item, dict):
                continue
            ticker = strip_html(item.get("ticker") or "—")
            suggested_range = strip_html(item.get("suggested_range") or "")
            rationale = strip_html(item.get("rationale") or "")
            st.markdown(
                f"""
                <div style="border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.025);
                            border-radius:12px;padding:12px 14px;margin:8px 0;line-height:1.45;">
                    <div style="font-size:13px;opacity:0.80;margin-bottom:6px;font-weight:700;letter-spacing:.2px;">
                        {_esc(ticker)} {("• " + _esc(suggested_range)) if suggested_range else ""}
                    </div>
                    <div style="font-size:15px;line-height:1.55;">{_esc(rationale or "—")}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    _render_section_list("Desalinhamentos", report.get("misalignments", []), limit=8)
    _render_section_list("Plano de ação", report.get("action_plan", []), limit=10)
    _render_section_text("Insight final", report.get("final_insight", ""))


# ────────────────────────────────────────────────────────────────────────────────
# Company detail section
# ────────────────────────────────────────────────────────────────────────────────

def _render_company_expander(company: CompanyAnalysis) -> None:
    """Card executivo por empresa.

    Hierarquia de informação:
      1. Decisão   — badge de perspectiva (FORTE / MODERADA / FRACA)
      2. Risco     — risco principal em destaque
      3. Síntese   — primeira frase da tese (sempre visível)
      4. Detalhe   — tudo mais dentro de st.expander (escondido por padrão)
    """
    tk = company.ticker
    p = company.perspectiva_compra.strip().lower()
    badge_decisao = _badge((p or "—").upper(), _tone_from_perspectiva(p))
    heuristic_badge = (
        "  " + _badge("Score heurístico", "warn")
        if company.score_source == "heuristic"
        else ""
    )

    # ── Síntese curta: primeira frase da tese (máx 220 chars) ────────────────
    _sintese_src = (company.tese or company.leitura or "").strip()
    if _sintese_src:
        _primeiro = _sintese_src.split(".")[0].strip()
        _sintese = (_primeiro + ".") if _primeiro else _sintese_src[:220]
        if len(_sintese) > 220:
            _sintese = _sintese[:217] + "…"
    else:
        _sintese = "—"

    # ── Risco principal: primeiro da lista ou fragilidade (máx 160 chars) ────
    _risco_src = (
        company.riscos[0] if company.riscos
        else company.fragilidade_regime_atual or ""
    ).strip()
    _risco_html = (
        f'<div style="font-size:11px;opacity:.6;margin-top:5px">'
        f'⚠️ {html.escape(_risco_src[:160])}{"…" if len(_risco_src) > 160 else ""}'
        f'</div>'
    ) if _risco_src else ""

    # ── Badge de atenção v3 (só se relevante) ────────────────────────────────
    _attn_badge = ""
    if company.attention_level in ("alta", "média"):
        _attn_tone = "bad" if company.attention_level == "alta" else "warn"
        _attn_badge = "  " + _badge(f"⚡ {company.attention_level.upper()}", _attn_tone)

    # ── Card header: sempre visível ───────────────────────────────────────────
    st.markdown(
        f"""<div class="p6-card" style="margin-bottom:6px">
          <div class="p6-head" style="margin-bottom:8px">
            <div class="p6-title-sm">{html.escape(tk)}</div>
            <div class="p6-badges">{badge_decisao}{_attn_badge}{heuristic_badge}</div>
          </div>
          <div style="font-size:13px;opacity:.85;line-height:1.45">{html.escape(_sintese)}</div>
          {_risco_html}
        </div>""",
        unsafe_allow_html=True,
    )

    # ── Detalhes: escondidos por padrão ──────────────────────────────────────
    with st.expander(f"Ver análise completa — {tk}", expanded=False):
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

        _render_section_text("Tese (síntese)", company.tese or "—")

        if company.leitura:
            _render_section_text("Leitura / Direcionalidade", company.leitura)
        elif p == "forte":
            _render_section_text(
                "Leitura / Direcionalidade",
                "Viés construtivo, com sinais qualitativos favoráveis no recorte analisado. "
                "Mantém assimetria positiva, com monitoramento de riscos.",
            )
        elif p == "moderada":
            _render_section_text(
                "Leitura / Direcionalidade",
                "Leitura equilibrada, com pontos positivos e ressalvas. "
                "Indica acompanhamento de gatilhos de execução, guidance e alocação de capital.",
            )
        elif p == "fraca":
            _render_section_text(
                "Leitura / Direcionalidade",
                "Leitura cautelosa, com sinais qualitativos desfavoráveis no recorte analisado. "
                "Recomenda postura defensiva e foco em mitigação de risco.",
            )

        _render_section_text("Papel estratégico", company.papel_estrategico)
        _render_section_list("Sensibilidades macro", company.sensibilidades_macro, limit=8)
        _render_section_text("Fragilidade sob o regime atual", company.fragilidade_regime_atual)
        _render_section_list("Dependências de cenário", company.dependencias_cenario, limit=8)
        _render_section_text("Faixa de alocação sugerida", company.alocacao_sugerida_faixa)
        _render_section_text("Racional de alocação", company.racional_alocacao)

        _render_key_value_section(
            "Evolução Estratégica", company.evolucao,
            [("historico", "Histórico"), ("fase_atual", "Fase atual"), ("tendencia", "Tendência")],
        )
        _render_strategy_detector(company.strategy_detector)
        _render_key_value_section(
            "Consistência do Discurso", company.consistencia,
            [
                ("analise", "Análise"),
                ("grau_consistencia", "Grau"),
                ("contradicoes", "Contradições"),
                ("sinais_positivos", "Sinais positivos"),
            ],
        )
        _render_key_value_section(
            "Execução vs Promessa", company.execucao,
            [
                ("analise", "Análise"),
                ("avaliacao_execucao", "Avaliação"),
                ("entregas_confirmadas", "Entregas confirmadas"),
                ("entregas_pendentes_ou_incertas", "Entregas pendentes ou incertas"),
                ("entregas_pendentes", "Entregas pendentes"),
            ],
        )
        _render_section_list("Mudanças Estratégicas", company.mudancas, limit=6)
        _render_section_list("Pontos-chave", company.pontos_chave, limit=8)
        _render_section_list("Catalisadores", company.catalisadores, limit=6)
        _render_section_list("Riscos", company.riscos, limit=6)
        _render_section_list("O que monitorar", company.monitorar, limit=6)
        _render_section_list("Ruídos e Contradições", company.contradicoes + company.sinais_ruido, limit=6)
        _render_key_value_section(
            "Qualidade Narrativa", company.qualidade_narrativa,
            [("clareza", "Clareza"), ("coerencia", "Coerência"), ("sinais_de_ruido", "Sinais de ruído")],
        )
        _render_evidence_section(company.evidencias, limit=10)
        _render_section_text("Considerações da LLM", company.consideracoes)

        # ── v3: Estabilidade e Robustez ───────────────────────────────────────
        _render_metric_cards([
            ("Score qualitativo (híbrido)", _fmt_score(company.score_qualitativo)),
            ("Robustez qualitativa", f"{round(company.robustez_qualitativa * 100)}%"),
            ("Dispersão narrativa", f"{round(company.narrative_dispersion_score * 100)}%"),
            ("Schema score", f"{company.schema_score}/100"),
        ], columns_per_row=4)

        # ── v3: Evolução da Tese ──────────────────────────────────────────────
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

        # ── v3: Mudança de Regime ─────────────────────────────────────────────
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
            badges += "  " + _badge(
                f"Intensidade: {company.regime_change_intensity}", regime_tone
            )
            st.markdown(badges, unsafe_allow_html=True)
            if company.regime_change_explanation:
                st.markdown(_box_html(company.regime_change_explanation), unsafe_allow_html=True)

        # ── v3: Prioridade de Acompanhamento ──────────────────────────────────
        if company.attention_score > 0:
            attn_tone = (
                "bad" if company.attention_level == "alta"
                else "warn" if company.attention_level == "média"
                else "neutral"
            )
            st.markdown("**Prioridade de Acompanhamento**")
            st.markdown(
                _badge(f"Nível: {company.attention_level.upper()}", attn_tone)
                + "  "
                + _badge(f"Score: {company.attention_score:.0f}/100", attn_tone)
                + "  "
                + _badge(company.recommended_action, attn_tone),
                unsafe_allow_html=True,
            )
            _render_section_list("Drivers da prioridade", company.attention_drivers, limit=6)

        # ── v3: Sinal Prospectivo ─────────────────────────────────────────────
        if company.forward_score > 0:
            fwd_tone = (
                "good" if company.forward_direction == "melhorando"
                else "bad" if company.forward_direction == "deteriorando"
                else "neutral"
            )
            st.markdown("**Sinal Prospectivo**")
            st.markdown(
                _badge(f"Forward score: {_fmt_score(company.forward_score)}", fwd_tone)
                + "  "
                + _badge(f"Direção: {company.forward_direction}", fwd_tone)
                + "  "
                + _badge(f"Confiança: {_fmt_confidence(company.forward_confidence)}", "neutral"),
                unsafe_allow_html=True,
            )
            _render_section_list("Fatores prospectivos", company.forward_drivers, limit=6)


# ────────────────────────────────────────────────────────────────────────────────
# Entry point
# ────────────────────────────────────────────────────────────────────────────────

_P6_CSS = """
<style>
.p6-card{
  border:1px solid rgba(255,255,255,0.08);
  background:rgba(255,255,255,0.03);
  border-radius:16px;
  padding:16px 18px;
  box-shadow:0 10px 24px rgba(0,0,0,0.25);
  min-height:110px;
}
.p6-card-label{font-size:12px;opacity:0.7;margin-bottom:6px;letter-spacing:0.3px;}
.p6-card-value{font-size:28px;font-weight:900;margin-bottom:6px;}
.p6-card-extra{font-size:12px;opacity:0.65;}
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

    # ── Portfolio summary cards ───────────────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.markdown(
        f'<div class="p6-card"><div class="p6-card-label">Qualidade (heurística)</div>'
        f'<div class="p6-card-value">{stats.label_qualidade()}</div>'
        f'<div class="p6-card-extra">Heurística agregada a partir dos sinais do RAG.</div></div>',
        unsafe_allow_html=True,
    )
    col2.markdown(
        f'<div class="p6-card"><div class="p6-card-label">Perspectiva 12m</div>'
        f'<div class="p6-card-value">{stats.label_perspectiva()}</div>'
        f'<div class="p6-card-extra">Direcionalidade consolidada para os próximos 12 meses.</div></div>',
        unsafe_allow_html=True,
    )
    col3.markdown(
        f'<div class="p6-card"><div class="p6-card-label">Cobertura</div>'
        f'<div class="p6-card-value">{analysis.cobertura}</div>'
        f'<div class="p6-card-extra">Ativos com evidências suficientes no período analisado.</div></div>',
        unsafe_allow_html=True,
    )
    col4.markdown(
        f'<div class="p6-card"><div class="p6-card-label">Confiança média</div>'
        f'<div class="p6-card-value">{_fmt_confidence(analysis.confianca_media)}</div>'
        f'<div class="p6-card-extra">Média do campo confianca_analise nas leituras individuais.</div></div>',
        unsafe_allow_html=True,
    )
    col5.markdown(
        f'<div class="p6-card"><div class="p6-card-label">Score qualitativo médio</div>'
        f'<div class="p6-card-value">{_fmt_score(analysis.score_medio)}</div>'
        f'<div class="p6-card-extra">Média do score_qualitativo salvo pela LLM.</div></div>',
        unsafe_allow_html=True,
    )
    st.caption(
        "🛈 Como a qualidade é estimada: combinação de cobertura do portfólio, perspectiva 12m agregada e distribuição de sinais. "
        f"A cobertura temporal do detector estratégico está presente em {analysis.temporal_covered} ativo(s)."
    )

    _render_macro_panel()

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
        _render_structured_portfolio_report(portfolio_report, mode_label)
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
