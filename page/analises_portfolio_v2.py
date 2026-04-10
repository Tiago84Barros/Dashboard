import streamlit as st
from core.patch6_report_v2 import render_patch6_report_v2


def render():
    st.title("📊 Análise de Portfólio V2")

    tickers_portfolio = ["TAEE3", "PETR3", "CSMG3"]
    period_ref = "2024Q4"

    render_patch6_report_v2(
        tickers=tickers_portfolio,
        period_ref=period_ref,
        llm_factory=None,
        show_company_details=True,
        analysis_mode="rigid",
        show_legacy_structured_report=False,
    )
