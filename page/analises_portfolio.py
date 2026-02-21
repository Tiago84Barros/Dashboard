
# Arquivo: analises_portfolio.py
# Upgrade visual + métricas avançadas (mantém pipeline original)

import streamlit as st
import pandas as pd

def render_metricas_avancadas(df_latest):
    st.markdown("## 📊 Métricas Estratégicas Avançadas")

    mapping = {"forte": 1.0, "moderada": 0.5, "fraca": 0.0}
    df_latest["score"] = df_latest["perspectiva_compra"].str.lower().map(mapping).fillna(0.5)
    conviccao = round(df_latest["score"].mean() * 100, 1)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Score médio de convicção", f"{conviccao}%")
        st.caption("Média ponderada das perspectivas (forte=100%, moderada=50%, fraca=0%).")

    st.markdown("### 📈 Ranking por Assimetria Qualitativa")
    df_rank = df_latest.sort_values("score", ascending=False)
    st.dataframe(df_rank[["ticker", "perspectiva_compra", "score"]],
                 use_container_width=True)

    if "segmento" in df_latest.columns:
        st.markdown("### 🔥 Heatmap Setor x Perspectiva")
        heat = pd.crosstab(df_latest["segmento"], df_latest["perspectiva_compra"])
        st.dataframe(heat.style.background_gradient(cmap="Blues"),
                     use_container_width=True)
