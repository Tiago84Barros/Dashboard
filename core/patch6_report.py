# =========================
# PATCH6 REPORT — FINAL CONSOLIDADO (ETAPA 12)
# =========================

import streamlit as st
import math
import html

# ==========================================
# 🔹 UTILITÁRIOS
# ==========================================

def _esc(x):
    return html.escape(str(x)) if x is not None else ""

def _is_missing(v):
    try:
        return v is None or str(v).lower() in ["nan", "none", "-", ""]
    except:
        return True

def _safe_float(v):
    try:
        return float(v)
    except:
        return None

# ==========================================
# 🔹 MACRO SNAPSHOT
# ==========================================

def _macro_snapshot(m):
    return {
        "selic": _safe_float(m.get("selic")),
        "dolar": _safe_float(m.get("dolar")),
        "ipca": _safe_float(m.get("ipca")),
        "pib": _safe_float(m.get("pib")),
    }

# ==========================================
# 🔹 MACRO SCORE
# ==========================================

def _macro_score(company, macro):

    snap = _macro_snapshot(macro)
    score = 0
    drivers = []

    if snap["selic"]:
        if snap["selic"] > 12:
            score -= 2
            drivers.append("Selic elevada pressiona custo de capital")

    if snap["dolar"]:
        if company["ticker"].startswith(("PETR", "VALE")):
            score += 2
            drivers.append("Dólar favorece receita externa")

    if snap["pib"]:
        if snap["pib"] < 1:
            score -= 1
            drivers.append("PIB fraco limita crescimento")

    return score, drivers[:2]

# ==========================================
# 🔹 DECISÃO AJUSTADA
# ==========================================

def _final_action(base, macro_score):
    if base == "manter" and macro_score >= 2:
        return "aumentar"
    if base == "manter" and macro_score <= -2:
        return "revisar"
    return base

# ==========================================
# 🔹 RENDER MACRO TOPO
# ==========================================

def _render_macro(m):

    snap = _macro_snapshot(m)

    st.markdown("## 📊 Cenário Macro Atual")

    cols = st.columns(4)

    labels = ["Selic", "Dólar", "IPCA", "PIB"]
    keys = ["selic", "dolar", "ipca", "pib"]

    for col, k, l in zip(cols, keys, labels):
        if snap[k] is None:
            continue
        col.metric(l, snap[k])

# ==========================================
# 🔹 DECISÃO DO CICLO
# ==========================================

def _render_decisao(companies, macro):

    grupos = {"aumentar": [], "manter": [], "revisar": [], "reduzir": []}

    for c in companies:
        base = c["acao"]
        mscore, _ = _macro_score(c, macro)
        final = _final_action(base, mscore)
        grupos[final].append(c["ticker"])

    st.markdown("## 🧭 Decisão do Ciclo")

    cols = st.columns(3)

    cols[0].metric("Aumentar", ", ".join(grupos["aumentar"]))
    cols[1].metric("Manter/Revisar", ", ".join(grupos["manter"] + grupos["revisar"]))
    cols[2].metric("Reduzir", ", ".join(grupos["reduzir"]))

# ==========================================
# 🔹 RANKING RISCO
# ==========================================

def _render_risco(companies, macro):

    ranking = []

    for c in companies:
        base = c["risco"]
        mscore, drivers = _macro_score(c, macro)
        adj = base + (mscore * 5)
        ranking.append((c, adj, drivers))

    ranking.sort(key=lambda x: x[1], reverse=True)

    st.markdown("## ⚠️ Ranking de Risco")

    for c, score, drivers in ranking[:5]:
        st.markdown(f"""
        **{c['ticker']}**
        - Risco: {score}
        - Macro: {drivers[0] if drivers else "Neutro"}
        """)

# ==========================================
# 🔹 HISTÓRICO
# ==========================================

def _render_hist(c):

    delivered = len(c.get("delivered", []))
    risks = len(c.get("risks", []))

    if delivered > risks:
        label = "Histórico favorável"
    elif risks > delivered:
        label = "Histórico pressionado"
    else:
        label = "Histórico misto"

    st.markdown(f"🧠 **{label}**")

# ==========================================
# 🔹 EMPRESA
# ==========================================

def _render_empresa(c, macro):

    st.markdown(f"### {c['ticker']}")

    mscore, drivers = _macro_score(c, macro)

    st.markdown(f"""
    **Tese:** {c['tese']}

    **Força:** {c['forca']}

    **Risco:** {c['risco_txt']}

    **Macro:** {drivers[0] if drivers else "Neutro"}
    """)

    _render_hist(c)

# ==========================================
# 🔹 MAIN
# ==========================================
def render_patch6_report(
    tickers,
    period_ref,
    llm_factory=None,
    show_company_details=True,
    analysis_mode="rigid",
):
    st.markdown(_P6_CSS, unsafe_allow_html=True)

    analysis = build_portfolio_analysis(tickers, period_ref)
    if analysis is None or not analysis.companies:
        st.warning(
            "Não há execuções salvas em patch6_runs para este period_ref e tickers do portfólio. "
            "Rode a LLM e salve os resultados primeiro."
        )
        return

    stats = analysis.stats
    macro_context = (
        st.session_state.get("macro_context_run")
        or st.session_state.get("macro_context")
        or {}
    )

    _render_macro_strip(macro_context)
    _render_decision_cycle(analysis, stats, macro_context=macro_context)
    _render_risk_ranking(analysis, macro_context=macro_context)

    portfolio_report = run_portfolio_llm_report(llm_factory, analysis, analysis_mode)
    if portfolio_report:
        mode_label = "Análise Rígida" if analysis_mode == "rigid" else "Análise Flexível"
        _render_structured_portfolio_report(portfolio_report, mode_label, analysis)
    else:
        _render_banner(
            "Leitura executiva",
            f"O portfólio concentra {stats.fortes} ativo(s) forte, {stats.moderadas} moderado(s) e {stats.fracas} fraco(s). "
            "Use decisão do ciclo, ranking de risco e bloco macro como eixo principal.",
            "neutral",
            "🧠",
        )

    with st.expander("📌 Ver faixas de alocação", expanded=False):
        _render_allocation_section(analysis.allocation_rows)

    if show_company_details:
        st.markdown("## 🏢 Relatórios por Empresa")
        for company in analysis.companies.values():
            _render_company_expander(company, macro_context=macro_context)

    with st.expander("🔎 Ver conclusão estratégica", expanded=False):
        llm_client = None
        if llm_factory is not None:
            try:
                llm_client = llm_factory.get_llm_client()
            except Exception:
                pass

        prompt_conc = (
            "Escreva uma conclusão estratégica para o portfólio, em até 8 linhas, com foco em:\n"
            "- coerência do conjunto do portfólio\n"
            "- principais alavancas para melhora ou deterioração\n"
            "- recomendação de acompanhamento nos próximos trimestres\n\n"
            f"Use SOMENTE os bullets abaixo.\n\nBULLETS:\n{analysis.contexto_portfolio}"
        )
        llm_conc = safe_call_llm(llm_client, prompt_conc)
        st.write(
            llm_conc
            or "Acompanhe principalmente execução, custo de capital, narrativa corporativa e manutenção dos catalisadores mais relevantes do ciclo."
        )
