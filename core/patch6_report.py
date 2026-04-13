# patch6_report_final_fixed.py

import streamlit as st
from html import escape
from core.patch6_analysis import build_portfolio_analysis

def _fmt_pct(v):
    try:
        if v is None:
            return "—"
        return f"{float(v):.2f}%"
    except:
        return "—"

def _fmt_num(v):
    try:
        if v is None:
            return "—"
        return f"{float(v):.2f}"
    except:
        return "—"

def _safe_get(obj, attr, default=None):
    return getattr(obj, attr, default)

def render_patch6_report(
    tickers,
    period_ref,
    llm_factory=None,
    show_company_details=True,
    analysis_mode="rigid",
):
    st.markdown("## 📘 Relatório consolidado do portfólio")

    # ✅ CORREÇÃO PRINCIPAL: reconstruir análise corretamente
    analysis = build_portfolio_analysis(
        tickers=tickers,
        period_ref=period_ref,
        llm_factory=llm_factory,
        analysis_mode=analysis_mode,
    )

    if not analysis:
        st.warning("Relatório indisponível.")
        return

    # Safe access
    portfolio_trend = _safe_get(analysis, "portfolio_trend", {})

    # Macro context
    macro = st.session_state.get("macro_context_run") or st.session_state.get("macro_context") or {}
    summary = macro.get("macro_summary", {})
    anual = macro.get("anual", {})

    selic = summary.get("selic_current")
    cambio = summary.get("cambio_current")
    ipca_12m = summary.get("ipca_12m_current")
    ipca_anual = anual.get("ipca")

    st.markdown("### 🌎 Cenário macro")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Selic", _fmt_pct(selic))
    with col2:
        st.metric("Câmbio", f"R$ {_fmt_num(cambio)}")
    with col3:
        st.metric("IPCA 12m", _fmt_pct(ipca_12m))
    with col4:
        st.metric("IPCA anual", _fmt_pct(ipca_anual))

    # Portfolio trend
    if portfolio_trend:
        st.markdown("### 📊 Tendências do portfólio")
        st.write(portfolio_trend)

    # Companies
    st.markdown("### 🏢 Relatórios por Empresa")

    companies = _safe_get(analysis, "companies", [])

    for c in companies:
        ticker = escape(str(_safe_get(c, "ticker", "")))

        decision = _safe_get(c, "decision_label", "manter")
        score = _safe_get(c, "decision_score", 0)
        risco = _safe_get(c, "risk_rank", [])

        st.markdown(f"### {ticker}")

        st.write(f"Decisão: {decision} ({score})")

        if risco:
            st.write("Riscos:", ", ".join(map(str, risco)))

        if show_company_details:
            resumo = _safe_get(c, "summary", "")
            if resumo:
                st.write(resumo)
