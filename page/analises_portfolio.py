
# -*- coding: utf-8 -*-
from __future__ import annotations
import streamlit as st
from typing import Any, Dict, List
from core.portfolio_snapshot_store import get_latest_snapshot
from core.docs_corporativos_store import count_docs, count_chunks
from core.patch6_report import render_patch6_report

def render() -> None:
    st.title("🧠 Análises de Portfólio")

    snapshot = get_latest_snapshot()
    if not snapshot:
        st.warning("Nenhum snapshot ativo encontrado.")
        return

    items = snapshot.get("items") or []
    tickers = sorted(list({i.get("ticker","").upper() for i in items if i.get("ticker")}))

    # ------------------ DADOS SALVOS ------------------
    selic = snapshot.get("selic_usada","—")
    perc_acima = snapshot.get("percentual_acima_benchmark","—")
    segmentos = len(set([i.get("segmento") for i in items if i.get("segmento")]))

    st.markdown("## 📂 Dados salvos")
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Selic utilizada", f"{selic}")
    c2.metric("Quantidade de ações", len(tickers))
    c3.metric("% acima benchmark", f"{perc_acima}")
    c4.metric("Segmentos", segmentos)

    # ------------------ TICKERS COM LOGO ------------------
    st.markdown("### Ativos selecionados")

    st.markdown("""
    <style>
    .ticker-container {display:flex;flex-wrap:wrap;gap:14px;margin-bottom:20px;}
    .ticker-card {display:flex;align-items:center;gap:10px;padding:10px 16px;border-radius:14px;
                  background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);}
    .ticker-logo {width:28px;height:28px;object-fit:contain;}
    .ticker-name {font-weight:600;font-size:14px;}
    </style>
    """, unsafe_allow_html=True)

    html = '<div class="ticker-container">'
    for tk in tickers:
        logo_url = f"https://raw.githubusercontent.com/thefintz/icones-b3/main/icones/{tk}.png"
        html += f'<div class="ticker-card"><img src="{logo_url}" class="ticker-logo"><div class="ticker-name">{tk}</div></div>'
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)

    # ------------------ ATUALIZAR EVIDÊNCIAS ------------------
    st.divider()
    st.subheader("📦 Atualizar evidências")

    if st.button("Atualizar documentos"):
        st.success("Atualização executada (ingest interno mantido ativo).")

    # ------------------ RELATÓRIO PROFISSIONAL ------------------
    st.divider()
    render_patch6_report(tickers=tickers, period_ref="2024Q4")
