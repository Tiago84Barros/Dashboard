# patch6_report.py (HTML FIX ONLY)

import streamlit as st

def render_patch6_report(
    tickers,
    period_ref,
    llm_factory=None,
    show_company_details=True,
    analysis_mode="rigid",
):
    st.markdown("## 📘 Relatório consolidado do portfólio")

    # IMPORT ORIGINAL (mantido)
    from core.patch6_analysis import build_portfolio_analysis

    analysis = build_portfolio_analysis(
        tickers=tickers,
        period_ref=period_ref,
    )

    if not analysis:
        st.warning("Relatório indisponível.")
        return

    st.markdown("### 🏢 Relatórios por Empresa")

    companies = getattr(analysis, "companies", [])

    for c in companies:
        ticker = getattr(c, "ticker", "—")
        perspectiva = getattr(c, "perspectiva_compra", "")
        risco = getattr(c, "risk_rank", [])
        resumo = getattr(c, "tese", "") or getattr(c, "leitura", "")

        # 🔥 REMOVIDO QUALQUER HTML
        with st.container(border=True):

            col1, col2 = st.columns([1, 3])

            with col1:
                st.markdown(f"### {ticker}")

            with col2:
                st.markdown(f"**{perspectiva.upper()}**")

            if resumo:
                st.write(resumo[:200] + "..." if len(resumo) > 200 else resumo)

            if risco:
                st.warning("⚠ " + " | ".join(risco[:2]))

        if show_company_details:
            with st.expander(f"Ver análise completa — {ticker}"):
                st.write(resumo or "Sem detalhes disponíveis.")
