
from __future__ import annotations
import streamlit as st
from typing import List
import pandas as pd

class PortfolioStats:
    def __init__(self, fortes=0, moderadas=0, fracas=0):
        self.fortes = fortes
        self.moderadas = moderadas
        self.fracas = fracas

    @property
    def total(self):
        return self.fortes + self.moderadas + self.fracas

    def label_qualidade(self):
        if self.total == 0: return "—"
        if self.fortes >= max(1,int(0.4*self.total)): return "Alta"
        if self.fracas >= max(1,int(0.4*self.total)): return "Baixa"
        return "Moderada"

    def label_perspectiva(self):
        if self.fortes > self.fracas: return "Construtiva"
        if self.fracas > self.fortes: return "Cautelosa"
        return "Neutra"

def render_patch6_report(tickers: List[str], period_ref: str) -> None:

    stats = PortfolioStats(fortes=3, moderadas=3, fracas=3)

    st.markdown("""
    <style>
    .cf-header {display:flex;justify-content:space-between;align-items:center;margin-bottom:25px;}
    .cf-title {font-size:28px;font-weight:800;}
    .cf-subtitle {opacity:.7;font-size:14px;}
    .cf-pill {padding:6px 14px;border-radius:999px;background:rgba(59,130,246,.15);
              border:1px solid rgba(59,130,246,.35);font-size:12px;font-weight:600;}
    .cf-card {padding:18px;border-radius:16px;background:rgba(255,255,255,.04);
              border:1px solid rgba(255,255,255,.08);}
    .cf-card-label {font-size:13px;opacity:.7;}
    .cf-card-value {font-size:26px;font-weight:800;margin:6px 0;}
    .cf-card-extra {font-size:12px;opacity:.6;}
    </style>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="cf-header">
        <div>
            <div class="cf-title">📘 Relatório de Análise de Portfólio</div>
            <div class="cf-subtitle">Consolidação qualitativa baseada em evidências estruturadas</div>
        </div>
        <div><span class="cf-pill">Period_ref: {period_ref}</span></div>
    </div>
    """, unsafe_allow_html=True)

    col1,col2,col3,col4 = st.columns(4)

    col1.markdown(f"""
    <div class="cf-card">
        <div class="cf-card-label">Qualidade estrutural</div>
        <div class="cf-card-value">{stats.label_qualidade()}</div>
        <div class="cf-card-extra">Predominância de direcionalidade construtiva.</div>
    </div>
    """, unsafe_allow_html=True)

    col2.markdown(f"""
    <div class="cf-card">
        <div class="cf-card-label">Perspectiva 12 meses</div>
        <div class="cf-card-value">{stats.label_perspectiva()}</div>
        <div class="cf-card-extra">Direcionalidade agregada do conjunto.</div>
    </div>
    """, unsafe_allow_html=True)

    col3.markdown(f"""
    <div class="cf-card">
        <div class="cf-card-label">Cobertura analítica</div>
        <div class="cf-card-value">{stats.total}/{len(tickers)}</div>
        <div class="cf-card-extra">Ativos avaliados qualitativamente.</div>
    </div>
    """, unsafe_allow_html=True)

    col4.markdown(f"""
    <div class="cf-card">
        <div class="cf-card-label">Distribuição de teses</div>
        <div class="cf-card-value">
            {stats.fortes} Construtivas • {stats.moderadas} Equilibradas • {stats.fracas} Cautelosas
        </div>
        <div class="cf-card-extra">Classificação por intensidade qualitativa.</div>
    </div>
    """, unsafe_allow_html=True)
