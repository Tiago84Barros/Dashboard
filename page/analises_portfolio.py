# analises_portfolio.py (VERSÃO V2 - UI ORIENTADA À DECISÃO)

import streamlit as st

def render_decision_block():
    st.markdown("## 🧭 DECISÃO DO CICLO")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.success("🟢 AUMENTAR\n\nTAEE3")

    with col2:
        st.warning("🟡 MANTER\n\nPETR3 | VALE3")

    with col3:
        st.error("🔴 REDUZIR\n\nCSMG3 | CEBR3")

def render_risk_block():
    st.markdown("## ⚠️ RISCO PRIORITÁRIO")

    st.error("🔴 ALTO: CSMG3, CEBR3")
    st.warning("🟡 MÉDIO: PETR3, UGPA3")
    st.success("🟢 CONTROLADO: TAEE3, BRAP3")

def render_summary():
    st.markdown("## 📌 RESUMO EXECUTIVO")

    st.markdown("""
- 🟢 Carteira sólida  
- 🟡 Sinais de deterioração futura  
- 🔴 Riscos concentrados  
- 🟢 Exportadoras favorecidas  
- ⚠️ Ajuste necessário  
""")

def render_company(name, decision, color):
    with st.expander(f"{name}"):
        if color == "green":
            st.success(f"🎯 Decisão: {decision}")
        elif color == "yellow":
            st.warning(f"🎯 Decisão: {decision}")
        else:
            st.error(f"🎯 Decisão: {decision}")

        st.markdown("📌 Tese: descrição curta")
        st.markdown("⚠️ Risco: ponto principal")
        st.markdown("📉 Sinal: direção")

def render():
    st.title("📊 Análise de Portfólio V2")

    render_decision_block()
    render_summary()
    render_risk_block()

    st.markdown("## 🏢 Empresas")

    render_company("TAEE3", "Aumentar", "green")
    render_company("PETR3", "Manter", "yellow")
    render_company("CSMG3", "Reduzir", "red")
