
# -*- coding: utf-8 -*-
from __future__ import annotations
import streamlit as st
from typing import Any, Dict, List
from core.portfolio_snapshot_store import get_latest_snapshot
from core.patch6_report import render_patch6_report
from core.ai_models.llm_client.factory import get_llm_client
from core.docs_corporativos_store import fetch_topk_chunks

def render() -> None:
    st.title("🧠 Análises de Portfólio")

    snapshot = get_latest_snapshot()
    if not snapshot:
        st.warning("Nenhum snapshot ativo encontrado.")
        return

    items = snapshot.get("items") or []
    tickers = sorted(list({i.get("ticker","").upper() for i in items if i.get("ticker")}))

    # ---------------- DADOS SALVOS (PADRÃO CONTROLE FINANCEIRO) ----------------

    selic = snapshot.get("selic_usada") or snapshot.get("selic") or "—"
    perc_acima = snapshot.get("percentual_acima_benchmark") or snapshot.get("acima_benchmark") or "—"
    segmentos = len(set([i.get("segmento") for i in items if i.get("segmento")]))

    st.markdown("""
    <style>
    .cf-card {padding:18px;border-radius:16px;background:rgba(255,255,255,.04);
              border:1px solid rgba(255,255,255,.08);}
    .cf-card-label {font-size:13px;opacity:.7;}
    .cf-card-value {font-size:26px;font-weight:800;margin:6px 0;}
    .cf-card-extra {font-size:12px;opacity:.6;}
    </style>
    """, unsafe_allow_html=True)

    st.markdown("## 📂 Dados salvos")

    col1,col2,col3,col4 = st.columns(4)

    col1.markdown(f"""
    <div class="cf-card">
        <div class="cf-card-label">Selic utilizada</div>
        <div class="cf-card-value">{selic}</div>
        <div class="cf-card-extra">Taxa usada como referência no cálculo.</div>
    </div>
    """, unsafe_allow_html=True)

    col2.markdown(f"""
    <div class="cf-card">
        <div class="cf-card-label">Quantidade de ações</div>
        <div class="cf-card-value">{len(tickers)}</div>
        <div class="cf-card-extra">Total de ativos selecionados.</div>
    </div>
    """, unsafe_allow_html=True)

    col3.markdown(f"""
    <div class="cf-card">
        <div class="cf-card-label">% acima benchmark</div>
        <div class="cf-card-value">{perc_acima}</div>
        <div class="cf-card-extra">Diferença percentual projetada vs índice base.</div>
    </div>
    """, unsafe_allow_html=True)

    col4.markdown(f"""
    <div class="cf-card">
        <div class="cf-card-label">Segmentos</div>
        <div class="cf-card-value">{segmentos}</div>
        <div class="cf-card-extra">Diversificação setorial do portfólio.</div>
    </div>
    """, unsafe_allow_html=True)

    # ---------------- TICKERS COM LOGO ----------------
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

    # ---------------- RELATÓRIO PROFISSIONAL ----------------
    st.divider()
    render_patch6_report(tickers=tickers, period_ref="2024Q4")

    # ---------------- RESTAURAÇÃO LLM ----------------
    st.divider()
    st.subheader("🤖 Análise por LLM")

    if st.button("Rodar LLM agora"):
        client = get_llm_client()

        for tk in tickers:
            chunks = fetch_topk_chunks(tk, 6)
            if not chunks:
                st.warning(f"{tk} sem evidências suficientes.")
                continue

            contexto = "\n".join(chunks)

            prompt = f"""
Você é analista fundamentalista. Avalie o contexto abaixo e responda:
- Direcionalidade
- Principais pontos positivos
- Principais riscos
- Conclusão resumida

Contexto:
{contexto}
"""

            try:
                resposta = client.complete(prompt)
            except:
                resposta = "Erro ao consultar LLM."

            with st.expander(f"Resultado LLM - {tk}", expanded=False):
                st.write(resposta)
