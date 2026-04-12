
import streamlit as st
from core.macro_context import get_macro_context

def render_macro_global():
    macro = get_macro_context()
    st.markdown("## 📊 Cenário Macro Atual")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Selic", f"{macro.get('selic','-')}%", macro.get("selic_trend",""))
    col2.metric("IPCA", f"{macro.get('ipca','-')}%", macro.get("ipca_trend",""))
    col3.metric("Dólar", f"{macro.get('dolar','-')}", macro.get("dolar_trend",""))
    col4.metric("PIB", f"{macro.get('pib','-')}%", macro.get("pib_trend",""))

def render_macro_empresa(setor=None):
    macro = get_macro_context()
    impactos = []
    if setor == "exportadora":
        impactos.append(f"🟢 Dólar em {macro.get('dolar','-')} favorece receitas")
    if setor in ["varejo","construcao"]:
        impactos.append(f"🔴 Selic em {macro.get('selic','-')}% pressiona consumo")
    if setor == "energia":
        impactos.append(f"🟡 Selic em {macro.get('selic','-')}% impacta custo de capital")
    if not impactos:
        impactos.append("🟡 Impacto macro neutro")
    st.markdown("### 🔗 Impacto Macro")
    for i in impactos:
        st.markdown(f"- {i}")
