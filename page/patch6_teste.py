
from __future__ import annotations

import streamlit as st
from core.portfolio_snapshot_store import get_latest_snapshot


def render():
    st.title("📊 Análises de Portfólio")

    snapshot = get_latest_snapshot()
    if not snapshot:
        st.info("Nenhum portfólio encontrado. Execute primeiro a Criação de Portfólio.")
        return

    st.subheader("📌 Snapshot Carregado")
    st.write(f"Criado em: {snapshot['created_at']}")
    st.write(f"Margem vs Selic: {snapshot['margem_superior']}%")
    st.write(f"Tipo de empresa: {snapshot['tipo_empresa']}")

    st.divider()
    st.subheader("📂 Empresas do Portfólio")

    for item in snapshot["items"]:
        st.markdown(
            f"""
**{item['ticker']}**
- Segmento: {item['segmento']}
- Peso atual: {item['peso']*100:.2f}%
"""
        )
