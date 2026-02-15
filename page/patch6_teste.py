from __future__ import annotations

"""
page/patch6_teste.py
--------------------
Página de teste para rodar o Patch 6 sem passar por criação de portfólio/score/backtest.

Fluxo:
1) Informar tickers
2) Contar docs no Supabase (public.docs_corporativos)
3) Ingerir IPE (CVM) via dados abertos (opção A) -> public.docs_corporativos
4) Rodar Patch 6 com RAG do Supabase

Requisitos:
- pickup/docs_rag.py com get_docs_by_tickers / count_docs_by_tickers
- pickup/ingest_docs_cvm_ipe.py com ingest_ipe_for_tickers
"""

from typing import List

import streamlit as st

from pickup.docs_rag import count_docs_by_tickers, get_docs_by_tickers
from pickup.ingest_docs_cvm_ipe import ingest_ipe_for_tickers


def _parse_tickers(s: str) -> List[str]:
    if not s:
        return []
    raw = [x.strip().upper().replace(".SA", "") for x in s.split(",")]
    out = [x for x in raw if x]
    # dedupe
    seen = set()
    res = []
    for t in out:
        if t not in seen:
            seen.add(t)
            res.append(t)
    return res


def render():
    st.markdown("# Patch 6 — Modo Teste")
    st.caption("Roda o Patch 6 sem executar criação de portfólio, score ou backtest.")

    tickers_txt = st.text_input("Tickers (separados por vírgula)", value="BBAS3")
    tks = _parse_tickers(tickers_txt)

    st.markdown("## Parâmetros do teste")
    years = st.number_input("Anos (janela)", min_value=0, max_value=10, value=2, step=1)
    max_docs = st.number_input("Máx docs por ticker", min_value=1, max_value=200, value=25, step=1)
    fetch_html = st.checkbox("Baixar texto quando houver HTML/TXT (mais lento)", value=True)

    st.divider()
    st.markdown("## 1) Verificar docs já existentes")
    if st.button("Contar docs no Supabase", use_container_width=True, disabled=not tks):
        counts = count_docs_by_tickers(tks)
        total = sum(counts.values()) if counts else 0
        st.success(f"Docs carregados do Supabase: {total}")
        with st.expander("Ver contagem por ticker", expanded=True):
            for tk in tks:
                st.write(f"**{tk}**: {counts.get(tk, 0)} docs")

    st.divider()
    st.markdown("## 2) Ingerir docs IPE (CVM) para os tickers")
    st.caption("Usa dados.cvm.gov.br e filtra por Código CVM via public.cvm_to_ticker.")
    if st.button("Ingerir IPE (CVM) agora", use_container_width=True, disabled=not tks):
        with st.spinner("Ingerindo IPE..."):
            result = ingest_ipe_for_tickers(
                tks,
                years=int(years),
                max_docs_por_ticker=int(max_docs),
                fetch_html_text=bool(fetch_html),
            )
        st.json(result)

    st.divider()
    st.markdown("## 3) Rodar Patch 6 com RAG do Supabase")
    st.caption("Aqui o Patch6 puxa automaticamente os docs do Supabase e monta contexto.")

    # Placeholder: o seu Patch 6 real provavelmente está em outro módulo.
    # Aqui apenas mostramos o contexto que seria passado para o modelo.
    if st.button("Montar contexto (preview)", use_container_width=True, disabled=not tks):
        docs = get_docs_by_tickers(tks, limit_per_ticker=25)
        st.info(f"Total de docs retornados: {len(docs)}")
        if docs:
            st.json(docs[:5])
            st.caption("Mostrando apenas os 5 primeiros docs para preview.")
