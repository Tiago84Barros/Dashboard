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
import math
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
    return str(x or "").str

def _budget_topk_total(num_chunks: int, peso: float, base_cap: int) -> int:
    """Budget adaptativo (institucional) para top_k_total.

    - Base por tamanho do corpus (chunks).
    - Multiplicador por peso do ticker no portfólio.
    - Cap final controlado pelo slider (base_cap).
    """
    n = int(num_chunks or 0)
    # base por faixa
    if n < 120:
        base = 10
    elif n < 500:
        base = 20
    elif n < 1500:
        base = 35
    else:
        base = 50

    # boost por peso
    w = float(peso or 0.0)
    if w >= 0.15:
        mult = 1.20
    elif w >= 0.05:
        mult = 1.10
    else:
        mult = 1.00

    budget = int(round(base * mult))
    # garante mínimo razoável e respeita cap
    budget = max(8, budget)
    budget = min(int(base_cap), budget)
    return budget


def _budget_per_topic(top_k_total: int, n_topics: int, base_min: int = 4, cap: int = 40) -> int:
    if n_topics <= 0:
        return max(base_min, 4)
    v = int(math.ceil(float(top_k_total) / float(n_topics)))
    v = max(base_min, v)
    v = min(cap, v)
    return v
ip().upper()


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
    tickers_raw = snapshot.get("items") or snapshot.get("tickers") or []
    # Normaliza para lista de dicts com chave "ticker"
    if tickers_raw and isinstance(tickers_raw, list) and tickers_raw and isinstance(tickers_raw[0], str):
        tickers_raw = [{"ticker": t} for t in tickers_raw]

    items = tickers_raw if isinstance(tickers_raw, list) else []
    tickers = [_safe_upper(it.get("ticker")) for it in items if _safe_upper(it.get("ticker"))]
    tickers = sorted(list(dict.fromkeys(tickers)))

    # Mapa de peso (quando existir em portfolio_snapshot_items)
    peso_map = {}
    for it in items:
        tk = _safe_upper(it.get("ticker"))
        if not tk:
            continue
        try:
            peso_map[tk] = float(it.get("peso") or it.get("weight") or 0.0)
        except Exception:
            peso_map[tk] = 0.0

    if not tickers:
        st.warning("Snapshot não possui tickers. Refaça a criação do portfólio.")
        return

    # ---- Controles DEEP
    st.markdown("### Configurações (DEEP padrão)")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        janela_meses = st.selectbox("Janela (meses)", options=[24, 36, 48, 60], index=3)
    with col2:
        top_k_total = st.slider("Top-K total (cap máximo por ticker)", min_value=30, max_value=160, value=80, step=10)
    with col3:
        per_topic_k = st.slider("Recall por tópico (mínimo)", min_value=8, max_value=40, value=20, step=2)
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
        # Determina número de tópicos (para distribuir budget por tópico)
        try:
            import core.rag_multitopic as _rm
            _topics = getattr(_rm, "DEFAULT_TOPICS", None) or []
            n_topics = int(len(_topics)) if _topics else 6
        except Exception:
            n_topics = 6

        manifest_rows = []  # para auditoria e _meta por ticker

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
                # Portfolio Manifest (por ticker)
                ndocs = int(count_docs(tk) or 0)
                nchunks = int(count_chunks(tk) or 0)
                peso = float(peso_map.get(tk, 0.0) or 0.0)

                # Budget adaptativo (capado pelo slider)
                budget_total = _budget_topk_total(nchunks, peso, base_cap=int(top_k_total))
                budget_per_topic = _budget_per_topic(budget_total, n_topics=n_topics, base_min=int(per_topic_k))

                # Retrieval multi-tópico (budgeted)
                hits, rag_stats = retrieve_multitopic_chunks(
                    ticker=tk,
                    llm_client=llm_client,
                    period_ref=period_ref,
                    months_back=int(janela_meses),
                    top_k_total=int(budget_total),
                    per_topic_k=int(budget_per_topic),
                    max_per_doc=int(max_per_doc),
                )

                # Quality Gate (evita relatório vago por falta de evidências)
                if len(hits) < 8 and int(budget_total) < int(top_k_total):
                    budget_total2 = min(int(top_k_total), int(budget_total) + 10)
                    budget_per_topic2 = _budget_per_topic(budget_total2, n_topics=n_topics, base_min=int(per_topic_k))
                    hits, rag_stats = retrieve_multitopic_chunks(
                        ticker=tk,
                        llm_client=llm_client,
                        period_ref=period_ref,
                        months_back=int(janela_meses),
                        top_k_total=int(budget_total2),
                        per_topic_k=int(budget_per_topic2),
                        max_per_doc=int(max_per_doc),
                    )
                    budget_total = budget_total2
                    budget_per_topic = budget_per_topic2

                manifest_rows.append({
                    "ticker": tk,
                    "peso": peso,
                    "num_docs": ndocs,
                    "num_chunks": nchunks,
                    "top_k_total": int(budget_total),
                    "per_topic_k": int(budget_per_topic),
                    "months_back": int(janela_meses),
                    "period_ref": (period_ref or ""),
                })


                chunks_by_topic = _group_hits_by_topic(hits)

                log.info(f"[{i}/{len(tickers)}] Writer (MAP/REDUCE) — {tk}")
                result = build_rich_report_json(
                    ticker=tk,
                    llm_client=llm_client,
                    chunks_by_topic=chunks_by_topic,
                    per_topic_chars=8000,
                )

                # adiciona metadados de auditoria (institucional)
                evid_total = int(sum(len(v) for v in chunks_by_topic.values()))
                result["_rag_stats"] = rag_stats
                result["_evidencias_total"] = evid_total

                # _meta padronizado (para comparação e debug sem olhar código)
                try:
                    _m = manifest_rows[-1] if manifest_rows else {}
                except Exception:
                    _m = {}
                result["_meta"] = {
                    **(_m or {}),
                    "evidence_final_count": evid_total,
                    "quality_gate_used": bool(evid_total < 8),
                }

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

