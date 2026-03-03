# -*- coding: utf-8 -*-
"""page/analises_portfolio.py

Patch6 — Análises de Portfólio (DEEP padrão)

Objetivo:
- Ingest (CVM/IPE) amplo (60 meses) para todos os tickers do snapshot
- Chunking + Embedding (pgvector)
- RAG multi-tópico (janela longa, top-k alto, diversidade por documento)
- Writer MAP/REDUCE por tópico → JSON institucional
- Persistência em public.patch6_runs
- Visualização do último relatório salvo (sem duplicidade de seções)

Observação:
- period_ref é opcional. Se vazio, usamos period_ref='AUTO' na gravação e exibimos "último disponível".
"""

from __future__ import annotations

import time
import traceback
from typing import Any, Dict, List, Optional

import streamlit as st

from core.helpers import get_logo_url
from core.portfolio_snapshot_store import get_latest_snapshot
from core.docs_corporativos_store import count_docs, count_chunks
from core.patch6_store import process_missing_chunks_for_ticker as process_missing_chunks_embed
from core.rag_multitopic import retrieve_multitopic_chunks
from core.patch6_writer import build_rich_report_json
from core.patch6_runs_store import save_patch6_run, list_patch6_history
from core.patch6_report import render_patch6_report

import core.ai_models.llm_client.factory as llm_factory


def _safe_upper(x: Any) -> str:
    return str(x or "").strip().upper()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fmt_s(ms: int) -> str:
    return f"{ms/1000:.1f}s"


def _import_ipe_ingest():
    # import tardio para reduzir custo na carga da página
    from pickup.ingest_docs_cvm_ipe import ingest_ipe_for_tickers
    return ingest_ipe_for_tickers


def _group_hits_by_topic(hits) -> Dict[str, List[str]]:
    by: Dict[str, List[str]] = {}
    for h in hits or []:
        by.setdefault(getattr(h, "tag", "geral"), []).append(getattr(h, "chunk_text", ""))
    return by


def render() -> None:
    st.title("🧠 Análises de Portfólio — Patch6 (DEEP)")

    snapshot = get_latest_snapshot()
    if not snapshot:
        st.warning("Nenhum snapshot de portfólio encontrado. Rode primeiro a página de Criação de Portfólio.")
        return

    snapshot_id = str(snapshot.get("snapshot_id") or snapshot.get("id") or "").strip()

    # Compatibilidade: snapshots antigos podem trazer "tickers",
    # mas o store atual traz "items"
    raw_list = snapshot.get("tickers") or snapshot.get("items") or []

    tickers = [_safe_upper(t.get("ticker")) for t in raw_list if _safe_upper(t.get("ticker"))]
    tickers = sorted(list(dict.fromkeys(tickers)))

    if not tickers:
        st.warning("Snapshot não possui tickers. Refaça a criação do portfólio.")
        return

    # ---- Controles DEEP
    st.markdown("### Configurações (DEEP padrão)")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        janela_meses = st.selectbox("Janela (meses)", options=[24, 36, 48, 60], index=3)
    with col2:
        top_k_total = st.slider("Top-K total (chunks)", min_value=30, max_value=160, value=80, step=10)
    with col3:
        per_topic_k = st.slider("Recall por tópico", min_value=8, max_value=40, value=20, step=2)
    with col4:
        max_per_doc = st.slider("Máx por documento", min_value=3, max_value=15, value=8, step=1)

    period_ref_in = st.text_input("period_ref (opcional, ex: 2024Q4) — vazio = último disponível", value="").strip()
    period_ref: Optional[str] = period_ref_in or None

    st.markdown("### Universo do portfólio")
    st.caption(f"Snapshot: {snapshot_id} • Tickers: {len(tickers)}")
    st.write(", ".join(tickers))

    # ---- Botão principal
    run_btn = st.button("Atualizar relatório com LLM agora", type="primary")

    if run_btn:
        llm_client = llm_factory.get_llm_client()
        ingest_fn = _import_ipe_ingest()

        period_key = period_ref or "AUTO"

        prog = st.progress(0)
        log = st.empty()

        for i, tk in enumerate(tickers, start=1):
            t0 = _now_ms()
            try:
                log.info(f"[{i}/{len(tickers)}] Ingest IPE (DEEP) — {tk}")
                ingest_fn(
                    [tk],
                    window_months=int(janela_meses),
                    max_docs_per_ticker=300,
                    strategic_only=True,
                    download_pdfs=True,
                    max_pdfs_per_ticker=120,
                    pdf_max_pages=120,
                    request_timeout=60,
                    max_runtime_s=900.0,
                    sleep_s=0.05,
                    verbose=False,
                )

                log.info(f"[{i}/{len(tickers)}] Chunking+Embedding — {tk}")
                process_missing_chunks_embed(ticker=tk, llm_client=llm_client)

                log.info(f"[{i}/{len(tickers)}] RAG multi-tópico — {tk}")
                hits, rag_stats = retrieve_multitopic_chunks(
                    ticker=tk,
                    llm_client=llm_client,
                    period_ref=period_ref,
                    months_back=int(janela_meses),
                    top_k_total=int(top_k_total),
                    per_topic_k=int(per_topic_k),
                    max_per_doc=int(max_per_doc),
                )

                chunks_by_topic = _group_hits_by_topic(hits)

                log.info(f"[{i}/{len(tickers)}] Writer (MAP/REDUCE) — {tk}")
                result = build_rich_report_json(
                    ticker=tk,
                    llm_client=llm_client,
                    chunks_by_topic=chunks_by_topic,
                    per_topic_chars=8000,
                )

                # adiciona metadados de auditoria
                result["_rag_stats"] = rag_stats
                result["_evidencias_total"] = sum(len(v) for v in chunks_by_topic.values())

                save_patch6_run(snapshot_id=snapshot_id, ticker=tk, period_ref=period_key, result=result)

                dt = _fmt_s(_now_ms() - t0)
                log.success(f"[{i}/{len(tickers)}] OK — {tk} • {dt} • Evidências: {result.get('_evidencias_total', 0)}")
            except Exception as e:
                log.error(f"[{i}/{len(tickers)}] Falhou — {tk}: {e}")
                st.code(traceback.format_exc())
            prog.progress(i / len(tickers))

        st.success("Relatório atualizado e salvo. Carregando a última execução...")
        st.rerun()

    # ---- Relatório salvo (única seção)
    st.markdown("---")
    st.markdown("## 📌 Relatório salvo do portfólio (última execução)")

    # Mostra histórico rápido
    with st.expander("Histórico Patch6 (debug)", expanded=False):
        try:
            hist = list_patch6_history(snapshot_id=snapshot_id, limit=50)
            st.dataframe(hist, use_container_width=True)
        except Exception as e:
            st.warning(f"Não foi possível carregar histórico: {e}")

    render_patch6_report(
        tickers=tickers,
        period_ref=(period_ref or None),
        llm_factory=llm_factory,
        show_company_details=True,
    )

