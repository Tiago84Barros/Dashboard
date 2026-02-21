
from __future__ import annotations
import streamlit as st
from typing import List

class PortfolioStats:
    def __init__(self, fortes=3, moderadas=3, fracas=3):
        self.fortes = fortes
        self.moderadas = moderadas
        self.fracas = fracas

    @property
    def total(self):
        return self.fortes + self.moderadas + self.fracas

    def label_qualidade(self):
        if self.fortes >= max(1,int(0.4*self.total)): return "Alta"
        if self.fracas >= max(1,int(0.4*self.total)): return "Baixa"
        return "Moderada"

    def label_perspectiva(self):
        if self.fortes > self.fracas: return "Construtiva"
        if self.fracas > self.fortes: return "Cautelosa"
        return "Neutra"

def render_patch6_report(tickers: List[str], period_ref: str) -> None:

    stats = PortfolioStats()

    st.markdown("## 📘 Relatório de Análise de Portfólio")

    col1,col2,col3,col4 = st.columns(4)

    col1.metric("Qualidade estrutural", stats.label_qualidade())
    col2.metric("Perspectiva 12 meses", stats.label_perspectiva())
    col3.metric("Cobertura analítica", f"{stats.total}/{len(tickers)}")
    col4.metric("Distribuição",
                f"{stats.fortes} Construtivas • {stats.moderadas} Equilibradas • {stats.fracas} Cautelosas")
