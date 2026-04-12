# core/patch6_report.py
# VERSÃO FINAL ESTÁVEL

from __future__ import annotations

import html
from typing import Any, Dict

import streamlit as st

from core.patch6_analysis import build_portfolio_analysis
from core.patch6_service import run_portfolio_llm_report, safe_call_llm


_P6_CSS = """
<style>
body {color: #ecf3ff;}
.p6-card {border:1px solid rgba(148,163,184,.18); border-radius:12px; padding:12px; margin-bottom:10px;}
</style>
"""


def _esc(x):
    return html.escape(str(x)) if x is not None else ""


def _render_macro(macro: Dict[str, Any]):
    st.markdown("## 📊 Cenário Macro Atual")
    cols = st.columns(4)
    keys = ["selic", "dolar", "ipca", "pib"]
    labels = ["Selic", "Dólar", "IPCA", "PIB"]

    for col, k, l in zip(cols, keys, labels):
        val = macro.get(k)
        if val:
            col.metric(l, val)


def _render_decisao(analysis):
    grupos = {"aumentar": [], "manter": [], "revisar": [], "reduzir": []}

    for c in analysis.companies.values():
        grupos.get(c.acao, []).append(c.ticker)

    st.markdown("## 🧭 Decisão do Ciclo")
    cols = st.columns(3)

    cols[0].metric("Aumentar", ", ".join(grupos["aumentar"]))
    cols[1].metric("Manter/Revisar", ", ".join(grupos["manter"] + grupos["revisar"]))
    cols[2].metric("Reduzir", ", ".join(grupos["reduzir"]))


def _render_risco(analysis):
    st.markdown("## ⚠️ Ranking de Risco")

    ranking = sorted(
        analysis.companies.values(),
        key=lambda x: x.score_qualitativo,
        reverse=True
    )

    for c in ranking[:5]:
        st.markdown(f"**{c.ticker}** — Score {c.score_qualitativo}")


def _render_empresa(c):
    st.markdown(f"### {c.ticker}")
    st.markdown(f"**Tese:** {c.tese}")
    st.markdown(f"**Risco:** {c.riscos[0] if c.riscos else '-'}")


def render_patch6_report(
    tickers,
    period_ref,
    llm_factory=None,
    show_company_details=True,
    analysis_mode="rigid",
):
    st.markdown(_P6_CSS, unsafe_allow_html=True)

    analysis = build_portfolio_analysis(tickers, period_ref)

    if not analysis or not analysis.companies:
        st.warning("Sem dados")
        return

    macro = st.session_state.get("macro_context", {})

    _render_macro(macro)
    _render_decisao(analysis)
    _render_risco(analysis)

    if show_company_details:
        st.markdown("## 🏢 Empresas")
        for c in analysis.companies.values():
            _render_empresa(c)
