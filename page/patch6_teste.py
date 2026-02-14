from __future__ import annotations

import streamlit as st

from pickup.ingest_docs_cvm_ipe import ingest_ipe_for_tickers
from pickup.docs_rag import count_docs_by_tickers, get_docs_by_tickers

# Patch6 renderer
from page.portfolio_patches import render_patch6_perspectivas_factibilidade


def render():
    st.markdown("# 🧪 Patch 6 — Modo Teste")
    st.caption("Página isolada para testar ingestão + RAG + LLM sem rodar o pipeline completo da criação de portfólio.")

    setores_df = st.session_state.get("setores_df")
    if setores_df is None or getattr(setores_df, "empty", True):
        st.warning("setores_df não encontrado em session_state. Abra o dashboard normalmente para ele carregar.")
        return

    all_tickers = sorted(set([str(x).upper().replace(".SA","").strip() for x in setores_df["ticker"].astype(str).tolist() if str(x).strip()]))

    st.markdown("## 1) Selecionar tickers para teste")
    tickers = st.multiselect("Tickers", options=all_tickers, default=all_tickers[:3])

    st.markdown("## 2) Verificar docs já existentes no Supabase")
    if st.button("🔎 Contar docs no Supabase"):
        counts = count_docs_by_tickers(tickers)
        st.write(counts)

    st.markdown("## 3) Ingerir docs IPE (CVM) para os tickers selecionados")
    st.caption("Se a CVM bloquear/alterar endpoint, esta etapa pode falhar. Ainda assim dá para testar colando texto manual no Patch6.")
    if st.button("⬇️ Ingerir IPE (CVM) agora"):
        with st.spinner("Ingerindo IPE..."):
            out = ingest_ipe_for_tickers(tickers, anos=2, max_docs_por_ticker=20)
        st.json(out)

    st.markdown("## 4) Rodar Patch 6 com RAG do Supabase")
    st.caption("Aqui o Patch6 já puxa automaticamente docs_by_ticker do Supabase.")
    if st.button("🧠 Rodar Patch 6 (usar RAG Supabase)"):
        docs_by_ticker = get_docs_by_tickers(tickers, limit_docs_per_ticker=10, prefer_tipos=["ipe"])
        empresas_lideres_finais = [{"ticker": tk, "nome": tk, "setor": "", "subsetor": "", "segmento": "", "peso": 1.0/ max(1,len(tickers))} for tk in tickers]
        render_patch6_perspectivas_factibilidade(
            empresas_lideres_finais,
            indicadores_por_ticker=None,
            docs_by_ticker=docs_by_ticker,
            ativar_ajuste_peso=False,
            cache_horas_default=6,
        )
