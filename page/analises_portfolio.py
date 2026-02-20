# -*- coding: utf-8 -*-
"""
pages/analises_portfolio.py

Página padrão "Análises de Portfólio" (Patch 6) com:
- Snapshot (entrada) via core.portfolio_snapshot_store
- Docs/Chunks (RAG) via core.docs_corporativos_store
- Histórico de resultados via core.patch6_runs_store
- Progresso por ticker: mostra carregamento individual e resultado por ação
"""

from __future__ import annotations

import json
import time
from typing import Dict, Any, List

import streamlit as st

from core.portfolio_snapshot_store import get_latest_snapshot
from core.patch6_runs_store import save_patch6_run, list_patch6_history
from core.docs_corporativos_store import (
    count_docs,
    count_chunks,
    process_missing_chunks_for_ticker,
    fetch_topk_chunks,
)

import core.ai_models.llm_client.factory as llm_factory


st.title("🧠 Análises de Portfólio (LLM + RAG)")

snapshot = get_latest_snapshot()
if not snapshot:
    st.warning("Nenhum snapshot ativo encontrado. Execute primeiro a Criação de Portfólio.")
    st.stop()

items = snapshot.get("items") or []
tickers = [str(it.get("ticker") or "").strip().upper() for it in items if (it.get("ticker") or "").strip()]
tickers = sorted(list(dict.fromkeys(tickers)))  # unique + stable

if not tickers:
    st.warning("Snapshot existe, mas não contém tickers.")
    st.stop()

# ------------------------------------------------------------------
# Estado
# ------------------------------------------------------------------
st.subheader("📊 Estado no Supabase (sanidade)")

cols = st.columns([1.2, 1, 1])
with cols[0]:
    st.caption(f"Snapshot ativo: {snapshot.get('id')}")
with cols[1]:
    st.caption(f"Tickers: {len(tickers)}")
with cols[2]:
    st.caption("Fonte: docs_corporativos / docs_corporativos_chunks")

status_rows: List[Dict[str, Any]] = []
for tk in tickers:
    status_rows.append({"ticker": tk, "docs": count_docs(tk), "chunks": count_chunks(tk)})

st.dataframe(status_rows, use_container_width=True)

st.divider()

# ------------------------------------------------------------------
# Atualizar documentos + chunks (apenas chunks aqui; ingest fica no pickup)
# ------------------------------------------------------------------
st.subheader("📦 Atualizar chunks (varredura por ticker)")

limit_docs = st.number_input("Limite de docs por ticker", min_value=5, max_value=200, value=60, step=5)
max_chars = st.number_input("Tamanho do chunk (chars)", min_value=600, max_value=4000, value=1500, step=100)

run_btn = st.button("Atualizar chunks agora", type="primary")

# placeholders (progresso)
progress_box = st.empty()
ticker_box = st.empty()
table_box = st.empty()

if run_btn:
    t0 = time.time()
    ok = 0
    fail = 0

    results: List[Dict[str, Any]] = []

    progress = st.progress(0, text="Iniciando varredura...")

    for i, tk in enumerate(tickers, start=1):
        pct = int((i - 1) / max(1, len(tickers)) * 100)
        progress.progress(pct, text=f"Processando {i}/{len(tickers)} — {tk}")

        with ticker_box.container():
            st.info(f"🔎 Varredura: **{tk}** ({i}/{len(tickers)})")

        try:
            inserted = process_missing_chunks_for_ticker(
                tk,
                limit_docs=int(limit_docs),
                max_chars=int(max_chars),
            )
            ok += 1
            results.append({
                "ticker": tk,
                "status": "OK",
                "chunks_inseridos": int(inserted),
                "docs": count_docs(tk),
                "chunks_total": count_chunks(tk),
                "erro": "",
            })
        except Exception as e:
            fail += 1
            results.append({
                "ticker": tk,
                "status": "FALHA",
                "chunks_inseridos": 0,
                "docs": None,
                "chunks_total": None,
                "erro": f"{type(e).__name__}: {e}",
            })

        # Atualiza tabela a cada ticker (efeito "um por vez")
        table_box.dataframe(results, use_container_width=True)

    progress.progress(100, text="Concluído")
    elapsed = time.time() - t0

    # Resumo final
    with progress_box.container():
        if fail == 0:
            st.success(f"Atualização concluída. OK: {ok} | Falhas: {fail} | Tempo: {elapsed:.1f}s")
        else:
            st.warning(f"Atualização concluída. OK: {ok} | Falhas: {fail} | Tempo: {elapsed:.1f}s")

    # Mostra erros (se houver)
    errors = {r["ticker"]: r["erro"] for r in results if r["status"] == "FALHA" and r["erro"]}
    if errors:
        st.error("Erros por ticker (para correção imediata):")
        st.json(errors)

st.divider()

# ------------------------------------------------------------------
# Rodar LLM
# ------------------------------------------------------------------
st.subheader("🤖 Análise qualitativa (RAG + LLM)")

ticker_escolhido = st.selectbox("Ticker", tickers, index=0)
top_k = st.slider("Top-K chunks (contexto)", min_value=3, max_value=12, value=6, step=1)

period_ref = st.text_input("period_ref (ex.: 2024Q4)", value="2024Q4")

if st.button("Rodar LLM agora"):
    chunks = fetch_topk_chunks(ticker_escolhido, int(top_k))
    if not chunks:
        st.error("Sem chunks no Supabase para este ticker. Rode o chunking primeiro.")
        st.stop()

    contexto = "\n\n".join(chunks)

    client = llm_factory.get_llm_client()

    prompt = f"""
Você é um analista fundamentalista. Use somente o CONTEXTO (evidências) abaixo.
Devolva APENAS JSON válido na estrutura:

{{
  "perspectiva_compra": "forte|moderada|fraca",
  "resumo": "texto curto",
  "pontos_chave": ["..."],
  "riscos": ["..."],
  "evidencias": ["trechos literais do contexto"]
}}

CONTEXTO:
{contexto}
"""

    with st.status("Chamando LLM...", expanded=False) as stt:
        raw = client.complete(prompt)
        stt.update(label="LLM respondeu. Validando JSON...", state="running")

    try:
        resultado = json.loads(raw)
    except Exception:
        st.error("A LLM não retornou JSON válido. Veja o texto bruto abaixo:")
        st.code(raw)
        st.stop()

    save_patch6_run(
        snapshot_id=str(snapshot.get("id")),
        ticker=ticker_escolhido,
        period_ref=period_ref,
        result=resultado,
    )

    st.success("Resultado salvo em public.patch6_runs.")
    st.json(resultado)

st.subheader("📜 Histórico (patch6_runs)")
try:
    hist = list_patch6_history(ticker_escolhido, limit=8)
    st.dataframe(hist, use_container_width=True)
except Exception as e:
    st.caption(f"Não foi possível carregar histórico: {type(e).__name__}: {e}")
