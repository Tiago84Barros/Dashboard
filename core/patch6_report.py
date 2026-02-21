
# Arquivo: patch6_report.py
# Distribuição institucional profissional

import streamlit as st

def render_distribution(stats):
    st.markdown("### 📌 Distribuição Estratégica")
    st.metric(
        "Distribuição",
        f"Construtivas {stats.fortes} | Neutras {stats.moderadas} | Cautelosas {stats.fracas}"
    )

    st.caption(
        "Classificação baseada na leitura qualitativa consolidada das evidências econômicas "
        "extraídas via RAG."
    )
