import streamlit as st
import time
import json

from portfolio_snapshot_store import get_latest_snapshot
from patch6_runs_store import save_patch6_run
from core.docs_corporativos_store import (
    count_docs,
    count_chunks,
    process_missing_chunks_for_ticker,
    fetch_topk_chunks,
)

st.title("🧠 Análises de Portfólio (Patch 6)")

snapshot = get_latest_snapshot()

if not snapshot:
    st.warning("Nenhum snapshot ativo encontrado.")
    st.stop()

tickers = [item["ticker"] for item in snapshot["items"]]

st.subheader("📊 Estado Atual")

for tk in tickers:
    st.write(
        f"{tk} | Docs: {count_docs(tk)} | Chunks: {count_chunks(tk)}"
    )

if st.button("Atualizar chunks"):
    inicio = time.time()
    ok = 0
    fail = 0
    erros = {}

    for tk in tickers:
        try:
            process_missing_chunks_for_ticker(tk)
            ok += 1
        except Exception as e:
            fail += 1
            erros[tk] = str(e)

    tempo = time.time() - inicio

    st.success(f"Atualização concluída. OK: {ok} | Falhas: {fail} | Tempo: {tempo:.1f}s")

    if erros:
        st.error("Erros encontrados:")
        st.json(erros)


# ============================================================
# Rodar LLM
# ============================================================

import core.ai_models.llm_client.factory as llm_factory

st.subheader("🤖 Rodar LLM")

ticker_escolhido = st.selectbox("Ticker", tickers)
top_k = st.slider("Top-K Chunks", 3, 10, 6)

if st.button("Rodar LLM agora"):
    chunks = fetch_topk_chunks(ticker_escolhido, top_k)

    if not chunks:
        st.error("Sem chunks no Supabase. Rode o chunking primeiro.")
        st.stop()

    contexto = "\n\n".join(chunks)

    client = llm_factory.get_llm_client()

    prompt = f"""
    Com base no CONTEXTO abaixo, responda em JSON:

    CONTEXTO:
    {contexto}

    Estrutura obrigatória:
    {{
      "perspectiva_compra": "...",
      "resumo": "...",
      "pontos_chave": [],
      "riscos": [],
      "evidencias": []
    }}
    """

    resposta = client.complete(prompt)
    resultado = json.loads(resposta)

    save_patch6_run(
        snapshot_id=snapshot["id"],
        ticker=ticker_escolhido,
        period_ref="2024Q4",
        result=resultado,
    )

    st.success("LLM executada e resultado salvo.")
    st.json(resultado)
