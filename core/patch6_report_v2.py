import streamlit as st

def render_patch6_report_v2(
    tickers,
    period_ref,
    llm_factory,
    show_company_details=True,
    analysis_mode="rigid",
    show_legacy_structured_report=False,
):

    st.markdown("## 🧭 Decisão do Ciclo")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### 🟢 Aumentar")
        st.success("TAEE3\nAlta convicção")

    with col2:
        st.markdown("### 🟡 Manter")
        st.warning("PETR3, VALE3\nExecução em validação")

    with col3:
        st.markdown("### 🔴 Reduzir")
        st.error("CSMG3, CEBR3\nRisco elevado")

    st.markdown("---")

    st.markdown("## 📊 Status da Carteira")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Qualidade", "Estável")
    c2.metric("Execução", "Deteriorando")
    c3.metric("Risco", "Em alta")
    c4.metric("Convicção", "Moderada")

    st.markdown("---")

    st.markdown("## ⚠️ Ranking de Risco")

    st.markdown("""
    🔴 1. CSMG3 — dívida sem clareza  
    🔴 2. CEBR3 — risco regulatório  
    🟡 3. PETR3 — execução inconsistente  
    """)

    st.markdown("---")

    st.markdown("## 📊 Mapa de Ação")

    st.table([
        ["TAEE3", 81, "95%", "🟢 Aumentar"],
        ["PETR3", 78, "92%", "🟡 Manter"],
        ["CSMG3", 72, "76%", "🔴 Reduzir"]
    ])

    if show_company_details:
        st.markdown("---")
        st.markdown("## 🏢 Empresas")

        with st.expander("TAEE3"):
            st.write("Tese: crescimento com governança forte")
            st.write("Risco: execução de aquisições")
            st.write("Decisão: Aumentar")

        with st.expander("CSMG3"):
            st.write("Tese: crescimento via dívida")
            st.write("Risco: alocação pouco clara")
            st.write("Decisão: Reduzir")
