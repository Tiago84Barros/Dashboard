from __future__ import annotations

import html
from typing import Any, Dict, List, Optional

import streamlit as st

from core.patch6_analysis import (
    build_portfolio_analysis,
    pick_text,
    safe_int,
    strip_html,
)
from core.patch6_schema import AllocationRow, CompanyAnalysis
from core.patch6_service import run_portfolio_llm_report, safe_call_llm

try:
    from core.helpers import get_logo_url
except Exception:  # pragma: no cover
    def get_logo_url(ticker: str) -> str:
        return ""


def _esc(value: Any) -> str:
    return html.escape(strip_html(value))


def _fmt_confidence(value: float) -> str:
    try:
        if value <= 0:
            return "—"
        pct = round(max(0.0, min(1.0, float(value))) * 100)
        return f"{pct}%"
    except Exception:
        return "—"


def _fmt_score(value: int) -> str:
    try:
        if value <= 0:
            return "—"
        return f"{max(0, min(100, int(value)))}/100"
    except Exception:
        return "—"


def _fmt_pct(value: Any) -> str:
    try:
        if value is None:
            return "—"
        return f"{float(value):.2f}%"
    except Exception:
        return "—"


def _fmt_num(value: Any, decimals: int = 2, prefix: str = "") -> str:
    try:
        if value is None:
            return "—"
        return f"{prefix}{float(value):.{decimals}f}"
    except Exception:
        return "—"


def _as_clean_list(values: List[Any], limit: Optional[int] = None) -> List[str]:
    out = [strip_html(v) for v in (values or []) if strip_html(v)]
    if limit is not None:
        out = out[:limit]
    return out


def _tone_emoji(label: str) -> str:
    v = (label or "").strip().lower()
    if v in ("forte", "alta", "aumentar", "melhorando", "favorável", "favorecido"):
        return "🟢"
    if v in ("moderada", "média", "manter", "revisar", "atenção", "cauteloso", "misto", "neutro", "estável"):
        return "🟡"
    if v in ("fraca", "baixa", "reduzir", "deteriorando", "pressionado", "negativa"):
        return "🔴"
    return "⚪"


def _section_card(title: str, body: str, *, icon: str = "📌") -> None:
    body = strip_html(body)
    if not body:
        return
    with st.container(border=True):
        st.markdown(f"### {icon} {title}")
        st.write(body)


def _list_card(title: str, values: List[str], *, icon: str = "📌", limit: Optional[int] = None) -> None:
    clean = _as_clean_list(values, limit=limit)
    if not clean:
        return
    with st.container(border=True):
        st.markdown(f"### {icon} {title}")
        for item in clean:
            st.markdown(f"- {item}")


def _render_metric_cards(items: List[tuple], columns_per_row: int = 3) -> None:
    clean = [(str(a), str(b)) for a, b in items if str(a).strip()]
    if not clean:
        return
    for i in range(0, len(clean), columns_per_row):
        row = clean[i:i + columns_per_row]
        cols = st.columns(len(row))
        for col, (label, value) in zip(cols, row):
            with col:
                with st.container(border=True):
                    st.caption(label)
                    st.markdown(f"## {value}")


def _render_score_explanations(company: CompanyAnalysis) -> None:
    score = company.score_qualitativo
    conf = company.confianca
    evidencias = len(company.evidencias)
    riscos = len(company.riscos)
    anos = len(company.strategy_detector.get("coverage_years", [])) \
        if isinstance(company.strategy_detector.get("coverage_years"), list) else 0
    execucao = strip_html(company.execucao.get("avaliacao_execucao", "")) or "não classificada" \
        if isinstance(company.execucao, dict) else "não classificada"

    if score >= 75:
        score_txt = (
            f"Qualidade alta. A execução foi classificada como '{execucao}', com "
            f"{evidencias} evidência(s) documentais e {riscos} risco(s) explícito(s)."
        )
    elif score >= 55:
        score_txt = (
            f"Qualidade moderada. Há sinais positivos, mas com pontos de atenção. "
            f"Execução '{execucao}', {riscos} risco(s) e {evidencias} evidência(s)."
        )
    else:
        score_txt = (
            f"Qualidade fraca ou de atenção. A execução foi classificada como '{execucao}', "
            f"com {riscos} risco(s) e {evidencias} evidência(s)."
        )

    if conf >= 0.75:
        conf_txt = f"Confiança alta, com {evidencias} evidência(s) e cobertura temporal de {anos} ano(s)."
    elif conf >= 0.55:
        conf_txt = f"Confiança média, com {evidencias} evidência(s) e cobertura temporal de {anos} ano(s)."
    else:
        conf_txt = f"Confiança baixa, com base documental ainda limitada ({evidencias} evidência(s), {anos} ano(s))."

    with st.container(border=True):
        st.caption("Como interpretar os scores")
        st.write(score_txt)
        st.write(conf_txt)
        if company.score_source == "heuristic":
            st.info("Score heurístico: a LLM não retornou valor final, então o sistema estimou o score pela estrutura do JSON.")


def _render_evidence_section(evidences: List[Any], limit: int = 6) -> None:
    if not evidences:
        return
    st.markdown("**Evidências**")
    count = 0
    for item in evidences:
        if count >= limit:
            break
        if isinstance(item, dict):
            topico = strip_html(item.get("topico") or item.get("ano") or "Evidência")
            trecho = strip_html(item.get("trecho") or item.get("citacao") or "")
            leitura = strip_html(item.get("interpretacao") or item.get("leitura") or "")
            if not trecho and not leitura:
                continue
            with st.container(border=True):
                st.caption(topico)
                if trecho:
                    st.markdown(f"**Trecho:** {trecho}")
                if leitura:
                    st.markdown(f"**Leitura:** {leitura}")
            count += 1
        elif isinstance(item, str) and item.strip():
            with st.container(border=True):
                st.write(strip_html(item))
            count += 1


def _render_logo_ticker(ticker: str, subtitle: str = "") -> None:
    cols = st.columns([1, 6])
    with cols[0]:
        logo = get_logo_url(ticker)
        if logo:
            try:
                st.image(logo, width=44)
            except Exception:
                st.markdown("🏢")
        else:
            st.markdown("🏢")
    with cols[1]:
        st.markdown(f"### {ticker}")
        if subtitle:
            st.caption(subtitle)


def _render_macro_panel() -> None:
    macro = st.session_state.get("macro_context_run") or st.session_state.get("macro_context") or {}
    if not isinstance(macro, dict) or not macro:
        return

    summary = macro.get("macro_summary", {}) or {}
    anual = macro.get("anual", {}) or {}

    ref_date = str(summary.get("reference_date") or "")[:7]
    st.markdown("## 🌎 Painel Macro")
    if ref_date:
        st.caption(f"Ref. {ref_date}")

    ipca_interp = str(anual.get("ipca_interpretation") or "")
    ipca_year = anual.get("ipca_reference_year")
    ipca_month = anual.get("ipca_reference_month")
    if ipca_interp == "anual_fechado":
        ipca_label = f"IPCA {ipca_year or 'anual'}"
        ipca_sub = "Fechamento anual"
    else:
        ipca_label = f"IPCA {ipca_year or ''}".strip()
        mes_str = f"{int(ipca_month):02d}" if ipca_month else "?"
        ipca_sub = f"Acumulado até {mes_str}/{ipca_year or ''}".strip("/")

    cards = [
        ("Selic (a.a.)", _fmt_pct(summary.get("selic_current")), f"Tendência: {summary.get('selic_trend') or '—'}"),
        ("Câmbio (R$/USD)", _fmt_num(summary.get("cambio_current"), 4, "R$ "), f"Tendência: {summary.get('cambio_trend') or '—'}"),
        ("IPCA 12m", _fmt_pct(summary.get("ipca_12m_current")), f"Tendência: {summary.get('ipca_12m_trend') or '—'}"),
        (ipca_label, _fmt_pct(anual.get("ipca")), ipca_sub or "—"),
    ]

    cols = st.columns(4)
    for col, (label, value, extra) in zip(cols, cards):
        with col:
            with st.container(border=True):
                st.caption(label)
                st.markdown(f"## {value}")
                st.caption(extra)

    interp = _as_clean_list(macro.get("macro_interpretation") or [], limit=6)
    if interp:
        _list_card("Leitura macro complementar", interp, icon="🌐", limit=6)


def _render_allocation_section(allocation_rows: List[AllocationRow]) -> None:
    st.markdown("## 💼 Alocação Sugerida")
    st.caption("Distribuição percentual heurística entre os ativos cobertos no portfólio.")
    for row in allocation_rows:
        with st.container(border=True):
            _render_logo_ticker(
                row.ticker,
                f"{(row.perspectiva or '—').upper()} • Score {_fmt_score(row.score)} • Conf. {_fmt_confidence(row.confianca)}",
            )
            st.metric("Faixa heurística", f"{row.allocation_pct:.2f}%")
            extra = []
            if row.robustez > 0:
                extra.append(f"Robustez {round(row.robustez * 100)}%")
            if row.execution_trend and row.execution_trend != "—":
                extra.append(f"Execução: {row.execution_trend}")
            if extra:
                st.caption(" • ".join(extra))


def _render_asset_role_cards(items: List[Dict[str, Any]]) -> None:
    if not items:
        return
    st.markdown("## 🧩 Papel estratégico dos ativos")
    for item in items[:15]:
        if not isinstance(item, dict):
            continue
        ticker = strip_html(item.get("ticker") or "—")
        role = strip_html(item.get("role") or "")
        rationale = strip_html(item.get("rationale") or "")
        with st.container(border=True):
            _render_logo_ticker(ticker, role)
            if rationale:
                st.write(rationale)


def _render_suggested_alloc_cards(items: List[Dict[str, Any]]) -> None:
    if not items:
        return
    st.markdown("## 🎯 Alocação sugerida (visão estratégica)")
    for item in items[:15]:
        if not isinstance(item, dict):
            continue
        ticker = strip_html(item.get("ticker") or "—")
        suggested_range = strip_html(item.get("suggested_range") or "")
        rationale = strip_html(item.get("rationale") or "")
        with st.container(border=True):
            _render_logo_ticker(ticker, suggested_range)
            if rationale:
                st.write(rationale)


def _render_structured_portfolio_report(report: Dict[str, Any], mode_label: str) -> None:
    st.markdown("## 🧠 Relatório Estratégico do Portfólio")
    st.caption(f"Modo utilizado: {mode_label}")

    _section_card("Base analítica", report.get("analytical_basis", ""), icon="🧪")
    _section_card("Diagnóstico executivo", report.get("executive_summary", ""), icon="🧠")
    _section_card("Identidade da carteira", report.get("portfolio_identity", ""), icon="🪪")
    _section_card("Cenário macro atual", report.get("current_market_context", ""), icon="🌎")
    _section_card("Leitura macro", report.get("macro_reading", ""), icon="📡")

    _list_card("Riscos internacionais relevantes", report.get("international_risk_links", []), icon="🌍", limit=8)
    _list_card("Dependências de cenário macro", report.get("macro_scenario_dependencies", []), icon="🧭", limit=8)
    _list_card("Vulnerabilidades da carteira sob o regime atual", report.get("portfolio_vulnerabilities_under_current_regime", []), icon="⚠️", limit=8)
    _list_card("O que a carteira está apostando implicitamente", report.get("what_the_portfolio_is_implicitly_betting_on", []), icon="🎯", limit=8)

    _section_card("Análise de concentração econômica", report.get("portfolio_concentration_analysis", ""), icon="📊")
    _section_card("Racional de ajuste de alocação", report.get("allocation_adjustment_rationale", ""), icon="⚖️")

    _list_card("Forças principais", report.get("key_strengths", []), icon="✅", limit=8)
    _list_card("Fragilidades principais", report.get("key_weaknesses", []), icon="🟠", limit=8)
    _list_card("Riscos invisíveis", report.get("hidden_risks", []), icon="🕳️", limit=8)

    _render_asset_role_cards(report.get("asset_roles", []) or [])
    _render_suggested_alloc_cards(report.get("suggested_allocations", []) or [])

    _list_card("Desalinhamentos", report.get("misalignments", []), icon="🔀", limit=8)
    _list_card("Plano de ação", report.get("action_plan", []), icon="🛠️", limit=10)
    _section_card("Insight final", report.get("final_insight", ""), icon="💡")


def _render_company_expander(company: CompanyAnalysis) -> None:
    ticker = (company.ticker or "").strip() or "—"
    perspectiva = (company.perspectiva_compra or "").strip()
    decision_label = getattr(company, "decision_label", "—") or "—"
    attention = getattr(company, "attention_level", "baixa") or "baixa"

    tese_src = (company.tese or company.leitura or "").strip()
    first_sentence = tese_src.split(".")[0].strip() if tese_src else ""
    summary = (first_sentence + ".") if first_sentence else tese_src[:220]
    if len(summary) > 220:
        summary = summary[:217] + "…"

    risk_rank = getattr(company, "risk_rank", []) or []
    risk_main = (risk_rank[0] if risk_rank else (company.riscos[0] if company.riscos else company.fragilidade_regime_atual or "")).strip()
    action = (company.recommended_action or "").strip()

    with st.container(border=True):
        _render_logo_ticker(ticker)
        badges = [
            f"{_tone_emoji(perspectiva)} {perspectiva.upper() or '—'}",
            f"{_tone_emoji(decision_label)} {decision_label.upper() or '—'}",
            f"{_tone_emoji(attention)} ATENÇÃO {attention.upper()}",
        ]
        st.caption(" • ".join(badges))
        if summary:
            st.write(summary)
        if risk_main:
            st.warning(risk_main, icon="⚠️")
        if action:
            st.info(action, icon="➡️")

    with st.expander(f"Ver análise completa — {ticker}", expanded=False):
        st.caption(
            f"Período analisado: {company.period_ref} • Atualizado em: {company.created_at}"
            + (f" • Confiança: {_fmt_confidence(company.confianca)}" if company.confianca > 0 else "")
            + (f" • Score: {_fmt_score(company.score_qualitativo)}" if company.score_qualitativo > 0 else "")
        )

        metric_items = [
            ("Score qualitativo", f"{_fmt_score(company.score_qualitativo)} • {_tone_emoji(company.perspectiva_compra)} {company.perspectiva_compra.title() or '—'}"),
            ("Confiança", _fmt_confidence(company.confianca)),
        ]
        years = company.strategy_detector.get("coverage_years", []) if isinstance(company.strategy_detector.get("coverage_years"), list) else []
        if years:
            metric_items.append(("Cobertura temporal", ", ".join([str(y) for y in years[:4]])))
        _render_metric_cards(metric_items, columns_per_row=3)
        _render_score_explanations(company)

        _section_card("Tese (síntese)", company.tese or "—", icon="🧠")
        if company.leitura:
            _section_card("Leitura / Direcionalidade", company.leitura, icon="🧭")
        _section_card("Papel estratégico", company.papel_estrategico, icon="🧩")
        _list_card("Sensibilidades macro", company.sensibilidades_macro, icon="🌐", limit=8)
        _section_card("Fragilidade sob o regime atual", company.fragilidade_regime_atual, icon="⚠️")
        _list_card("Dependências de cenário", company.dependencias_cenario, icon="🧭", limit=8)
        _section_card("Faixa de alocação sugerida", company.alocacao_sugerida_faixa, icon="🎯")
        _section_card("Racional de alocação", company.racional_alocacao, icon="⚖️")

        if company.evolucao:
            lines = []
            for key, label in [("historico", "Histórico"), ("fase_atual", "Fase atual"), ("tendencia", "Tendência")]:
                value = strip_html(company.evolucao.get(key))
                if value:
                    lines.append(f"**{label}:** {value}")
            if lines:
                with st.container(border=True):
                    st.markdown("### 🔄 Evolução Estratégica")
                    for line in lines:
                        st.markdown(line)

        if company.strategy_detector:
            years = company.strategy_detector.get("coverage_years", []) if isinstance(company.strategy_detector.get("coverage_years"), list) else []
            n_events = safe_int(company.strategy_detector.get("n_events"), 0)
            _render_metric_cards([
                ("Cobertura temporal", ", ".join([str(y) for y in years]) if years else "—"),
                ("Eventos detectados", str(n_events) if n_events > 0 else "—"),
            ], columns_per_row=2)
            _section_card("Resumo do detector estratégico", company.strategy_detector.get("summary", ""), icon="🛰️")
            _list_card("Mudanças detectadas", company.strategy_detector.get("detected_changes", []) or [], icon="🔄", limit=10)

        if company.consistencia:
            lines = []
            for key, label in [("analise", "Análise"), ("grau_consistencia", "Grau"), ("contradicoes", "Contradições"), ("sinais_positivos", "Sinais positivos")]:
                value = company.consistencia.get(key)
                if isinstance(value, list):
                    value = " • ".join(_as_clean_list(value))
                value = strip_html(value)
                if value:
                    lines.append(f"**{label}:** {value}")
            if lines:
                with st.container(border=True):
                    st.markdown("### 🗣️ Consistência do Discurso")
                    for line in lines:
                        st.markdown(line)

        if company.execucao:
            lines = []
            for key, label in [
                ("analise", "Análise"),
                ("avaliacao_execucao", "Avaliação"),
                ("entregas_confirmadas", "Entregas confirmadas"),
                ("entregas_pendentes_ou_incertas", "Entregas pendentes ou incertas"),
                ("entregas_pendentes", "Entregas pendentes"),
            ]:
                value = company.execucao.get(key)
                if isinstance(value, list):
                    value = " • ".join(_as_clean_list(value))
                value = strip_html(value)
                if value:
                    lines.append(f"**{label}:** {value}")
            if lines:
                with st.container(border=True):
                    st.markdown("### 🏗️ Execução vs Promessa")
                    for line in lines:
                        st.markdown(line)

        _list_card("Mudanças Estratégicas", company.mudancas, icon="🔄", limit=6)
        _list_card("Pontos-chave", company.pontos_chave, icon="📌", limit=8)
        _list_card("Catalisadores", company.catalisadores, icon="✨", limit=6)
        _list_card("Riscos (prioritários)" if risk_rank else "Riscos", risk_rank if risk_rank else company.riscos, icon="⚠️", limit=6)
        _list_card("O que monitorar", company.monitorar, icon="👀", limit=6)
        _list_card("Ruídos e Contradições", (company.contradicoes or []) + (company.sinais_ruido or []), icon="🧩", limit=6)

        if company.qualidade_narrativa:
            lines = []
            for key, label in [("clareza", "Clareza"), ("coerencia", "Coerência"), ("sinais_de_ruido", "Sinais de ruído")]:
                value = company.qualidade_narrativa.get(key)
                if isinstance(value, list):
                    value = " • ".join(_as_clean_list(value))
                value = strip_html(value)
                if value:
                    lines.append(f"**{label}:** {value}")
            if lines:
                with st.container(border=True):
                    st.markdown("### 🧾 Qualidade Narrativa")
                    for line in lines:
                        st.markdown(line)

        _render_evidence_section(company.evidencias, limit=10)
        _section_card("Considerações da LLM", company.consideracoes, icon="🤖")

        _render_metric_cards([
            ("Score qualitativo (híbrido)", _fmt_score(company.score_qualitativo)),
            ("Robustez qualitativa", f"{round(company.robustez_qualitativa * 100)}%"),
            ("Dispersão narrativa", f"{round(company.narrative_dispersion_score * 100)}%"),
            ("Schema score", f"{company.schema_score}/100"),
        ], columns_per_row=4)

        evol_items = [
            ("Trend de execução", company.execution_trend or "—"),
            ("Mudança de narrativa", company.narrative_shift or "—"),
        ]
        if company.forward_score > 0:
            evol_items.append(("Sinal prospectivo", f"{company.forward_score}/100 ({company.forward_direction})"))
        _render_metric_cards(evol_items, columns_per_row=3)

        _section_card("Memória histórica da tese", company.memory_summary, icon="🧠")
        _list_card("Promessas recorrentes", company.recurring_promises, icon="🔁", limit=5)
        _list_card("Entregas confirmadas (recorrentes)", company.delivered_promises, icon="✅", limit=5)
        _list_card("Riscos persistentes entre períodos", company.persistent_risks, icon="♻️", limit=5)
        _list_card("Catalisadores persistentes", company.persistent_catalysts, icon="🚀", limit=5)

        if company.current_regime not in ("—", "indefinido", ""):
            with st.container(border=True):
                st.markdown("### 🧭 Mudança de Regime Qualitativo")
                st.write(f"Atual: {company.current_regime}")
                if company.previous_regime not in ("—", "indefinido", ""):
                    st.write(f"Anterior: {company.previous_regime}")
                st.write(f"Intensidade: {company.regime_change_intensity}")
                if company.regime_change_explanation:
                    st.write(company.regime_change_explanation)

        if company.attention_score > 0:
            with st.container(border=True):
                st.markdown("### 🚨 Prioridade de Acompanhamento")
                st.write(f"Nível: {company.attention_level.upper()}")
                st.write(f"Score: {company.attention_score:.0f}/100")
                if company.recommended_action:
                    st.write(f"Ação sugerida: {company.recommended_action}")
                for driver in _as_clean_list(company.attention_drivers, limit=6):
                    st.markdown(f"- {driver}")

        if company.forward_score > 0:
            with st.container(border=True):
                st.markdown("### 🔮 Sinal Prospectivo")
                st.write(f"Forward score: {_fmt_score(company.forward_score)}")
                st.write(f"Direção: {company.forward_direction}")
                st.write(f"Confiança: {_fmt_confidence(company.forward_confidence)}")
                for factor in _as_clean_list(company.forward_drivers, limit=6):
                    st.markdown(f"- {factor}")


def render_patch6_report(
    tickers: List[str],
    period_ref: str,
    llm_factory: Optional[Any] = None,
    show_company_details: bool = True,
    analysis_mode: str = "rigid",
) -> None:
    analysis = build_portfolio_analysis(tickers, period_ref)
    if analysis is None or not analysis.companies:
        st.warning(
            "Não há execuções salvas em patch6_runs para este period_ref e tickers do portfólio. "
            "Rode a LLM e salve os resultados primeiro."
        )
        return

    stats = analysis.stats

    _render_macro_panel()

    st.markdown("## 📘 Relatório consolidado do portfólio")
    _render_metric_cards([
        ("Qualidade (heurística)", stats.label_qualidade()),
        ("Perspectiva 12m", stats.label_perspectiva()),
        ("Cobertura", analysis.cobertura),
        ("Confiança média", _fmt_confidence(analysis.confianca_media)),
        ("Score qualitativo médio", _fmt_score(analysis.score_medio)),
    ], columns_per_row=5)

    st.caption(
        "Como a qualidade é estimada: combinação de cobertura do portfólio, perspectiva 12m agregada e distribuição de sinais."
        f" A cobertura temporal do detector estratégico está presente em {analysis.temporal_covered} ativo(s)."
    )

    portfolio_trend = getattr(analysis, "portfolio_trend", {}) or {}
    if portfolio_trend:
        with st.container(border=True):
            st.markdown("### 📊 Tendências do portfólio")
            for key, label in [
                ("qualidade", "Qualidade"),
                ("execucao", "Execução"),
                ("governanca", "Governança"),
                ("capital", "Capital"),
            ]:
                val = portfolio_trend.get(key, "")
                if val:
                    st.markdown(f"- **{label}:** {val}")

    if getattr(analysis, "macro_narrative", ""):
        _section_card("Narrativa macro do portfólio", analysis.macro_narrative, icon="🌎")

    v3_lines = []
    if analysis.alta_prioridade_count > 0:
        v3_lines.append(f"{analysis.alta_prioridade_count} ativo(s) em alta prioridade")
    if analysis.forward_score_medio > 0:
        fscores = [c.forward_score for c in analysis.companies.values() if c.forward_score > 0]
        direction = "estável"
        if fscores and analysis.score_medio > 0:
            avg_delta = sum(fscores) / len(fscores) - analysis.score_medio
            direction = "melhorando" if avg_delta > 5 else ("deteriorando" if avg_delta < -5 else "estável")
        v3_lines.append(f"Forward score médio: {analysis.forward_score_medio}/100 ({direction})")
    if analysis.regime_summary:
        v3_lines.append("Mudanças de regime detectadas")
    if v3_lines:
        with st.container(border=True):
            st.markdown("### 📋 Fila de Atenção do Portfólio")
            for line in v3_lines:
                st.markdown(f"- {line}")
            if analysis.regime_summary:
                st.caption(analysis.regime_summary)

    if analysis.priority_ranking:
        alta = [tk for tk in analysis.priority_ranking if analysis.companies[tk].attention_level == "alta"]
        media = [tk for tk in analysis.priority_ranking if analysis.companies[tk].attention_level == "média"]
        if alta or media:
            with st.expander("Ver ranking de prioridade", expanded=False):
                if alta:
                    st.markdown("**Alta prioridade**")
                    for tk in alta:
                        c = analysis.companies[tk]
                        st.markdown(f"- **{tk}** — score {c.attention_score:.0f} | {c.recommended_action}")
                if media:
                    st.markdown("**Média prioridade**")
                    for tk in media:
                        c = analysis.companies[tk]
                        st.markdown(f"- **{tk}** — score {c.attention_score:.0f} | {c.recommended_action}")

    portfolio_report = run_portfolio_llm_report(llm_factory, analysis, analysis_mode)
    if portfolio_report:
        mode_label = "Análise Rígida" if analysis_mode == "rigid" else "Análise Flexível"
        _render_structured_portfolio_report(portfolio_report, mode_label)
    else:
        _section_card(
            "Resumo Executivo",
            (
                f"O portfólio apresenta leitura {stats.label_perspectiva().lower()} para 12 meses, com distribuição: "
                f"{stats.fortes} forte, {stats.moderadas} moderada e {stats.fracas} fraca. "
                f"Cobertura: {analysis.cobertura}, confiança média {_fmt_confidence(analysis.confianca_media)} "
                f"e score qualitativo médio {_fmt_score(analysis.score_medio)}."
            ),
            icon="🧠",
        )

    _render_allocation_section(analysis.allocation_rows)

    if show_company_details:
        st.markdown("## 🏢 Relatórios por Empresa")
        for company in analysis.companies.values():
            _render_company_expander(company)

    st.markdown("## 🔎 Conclusão Estratégica")
    llm_client = None
    if llm_factory is not None:
        try:
            llm_client = llm_factory.get_llm_client()
        except Exception:
            llm_client = None

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
