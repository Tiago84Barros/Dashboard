# -*- coding: utf-8 -*-
"""
page/analises_portfolio.py

Patch 6 — Página padrão com LOGS completos (Ingest + Chunking) por ticker.

Por que isso existe:
- "chunks = 0" geralmente NÃO é erro do chunking, é falta de documentos no Supabase.
- O botão anterior estava rodando apenas chunking, então tickers com docs=0 "passavam rápido"
  e não mostravam motivo.

Agora:
- Para cada ticker: roda Ingest (CVM/IPE) -> mostra relatório -> roda Chunking -> mostra resultado
- Se docs continuar 0 após ingest, você verá isso explicitamente e o relatório do ingest
"""

from __future__ import annotations

import json
import time
import traceback
import importlib
import inspect
from typing import Any, Dict, List, Optional, Callable, Tuple

import streamlit as st

from core.portfolio_snapshot_store import get_latest_snapshot
from core.docs_corporativos_store import (
    count_docs,
    count_chunks,
    process_missing_chunks_for_ticker,
)
from core.patch6_runs_store import save_patch6_run, list_patch6_history

import core.ai_models.llm_client.factory as llm_factory


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)

def _fmt_s(ms: int) -> str:
    return f"{ms/1000:.1f}s"

def _safe_upper(x: Any) -> str:
    return str(x or "").strip().upper()

def _import_first(*module_paths: str):
    errors = []
    for p in module_paths:
        try:
            return importlib.import_module(p)
        except Exception as e:
            errors.append((p, e))
    msg = "Falha ao importar módulos. Tentativas:\n" + "\n".join([f"- {p}: {repr(e)}" for p, e in errors])
    raise ImportError(msg)

def _import_ingest():
    """
    Carrega ingest diretamente do arquivo físico,
    ignorando problemas de PYTHONPATH no Streamlit Cloud.
    """
    import importlib.util
    from pathlib import Path

    # sobe de page/ para raiz do projeto
    base_dir = Path(__file__).resolve().parents[1]
    ingest_path = base_dir / "pickup" / "ingest_docs_cvm_ipe.py"

    if not ingest_path.exists():
        raise ImportError(f"Arquivo não encontrado: {ingest_path}")

    spec = importlib.util.spec_from_file_location(
        "ingest_docs_cvm_ipe",
        str(ingest_path)
    )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    fn = getattr(module, "ingest_ipe_for_tickers", None)

    if not callable(fn):
        raise ImportError(
            "Função ingest_ipe_for_tickers não encontrada em ingest_docs_cvm_ipe.py"
        )

    return fn
    raise ImportError("Não encontrei função de ingest no módulo pickup.ingest_docs_cvm_ipe (ou fallbacks).")

def _safe_call(fn: Callable[..., Any], **kwargs):
    """
    Chama função adaptando para assinaturas diferentes.
    """
    try:
        sig = inspect.signature(fn)
        accepted = {k: v for k, v in kwargs.items() if k in sig.parameters}

        # alias comuns
        # ticker
        if "ticker" in kwargs and "ticker" not in accepted:
            for alt in ("tk", "symbol", "ticker_str"):
                if alt in sig.parameters:
                    accepted[alt] = kwargs["ticker"]
                    break



        # tickers (lista)
        if "tickers" in kwargs and "tickers" not in accepted:
            for alt in ("symbols", "ticker_list"):
                if alt in sig.parameters:
                    accepted[alt] = kwargs["tickers"]
                    break

        # months window
        if "window_months" in kwargs and "window_months" not in accepted:
            for alt in ("months", "months_window", "janela_meses"):
                if alt in sig.parameters:
                    accepted[alt] = kwargs["window_months"]
                    break

        # max docs
        if "max_docs" in kwargs and "max_docs" not in accepted:
            for alt in ("limit_docs", "max_docs_per_ticker", "limite_docs"):
                if alt in sig.parameters:
                    accepted[alt] = kwargs["max_docs"]
                    break

        # max runtime
        if "max_runtime_s" in kwargs and "max_runtime_s" not in accepted:
            for alt in ("timeout_s", "runtime_s", "time_budget_s"):
                if alt in sig.parameters:
                    accepted[alt] = kwargs["max_runtime_s"]
                    break

        # max pdfs
        if "max_pdfs" in kwargs and "max_pdfs" not in accepted:
            for alt in ("max_pdfs_per_ticker", "limite_pdfs"):
                if alt in sig.parameters:
                    accepted[alt] = kwargs["max_pdfs"]
                    break

        return fn(**accepted)
    except Exception:
        # fallback: tenta direto
        return fn(**kwargs)


def render() -> None:
    st.title("🧠 Análises de Portfólio (LLM + RAG)")

    snapshot = get_latest_snapshot()
    if not snapshot:
        st.warning("Nenhum snapshot ativo encontrado. Execute primeiro a Criação de Portfólio.")
        st.stop()

    snapshot_id = str(snapshot.get("id") or "")
    st.caption(f"Snapshot: `{snapshot_id}`")

    items = snapshot.get("items") or []
    tickers = [_safe_upper(it.get("ticker")) for it in items if _safe_upper(it.get("ticker"))]
    tickers = sorted(list(dict.fromkeys(tickers)))

    with st.expander("Ver composição do portfólio"):
        st.dataframe(items, use_container_width=True)

    st.divider()

    # ------------------------------------------------------------------
    # Estado (sanidade)
    # ------------------------------------------------------------------
    st.subheader("📊 Sanidade no Supabase")
    status_rows: List[Dict[str, Any]] = [{"ticker": tk, "docs": count_docs(tk), "chunks": count_chunks(tk)} for tk in tickers]
    st.dataframe(status_rows, use_container_width=True)

    st.divider()

    # ------------------------------------------------------------------
    # Ingest + Chunking com logs por ticker
    # ------------------------------------------------------------------
    st.subheader("📦 Atualizar evidências (CVM/IPE) — Ingest + Chunks (com logs)")

    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    with col1:
        window_months = st.number_input("Janela (meses)", min_value=1, max_value=60, value=12, step=1)
    with col2:
        max_docs = st.number_input("Máx docs/ticker", min_value=5, max_value=300, value=80, step=5)
    with col3:
        max_pdfs = st.number_input("Máx PDFs/ticker", min_value=0, max_value=80, value=20, step=1)
    with col4:
        max_runtime_s = st.number_input("Tempo máx total (s)", min_value=5, max_value=180, value=60, step=5)

    only_missing_docs = st.checkbox("Rodar ingest só quando docs=0", value=True)
    show_traceback = st.checkbox("Mostrar traceback completo", value=False)

    btn = st.button("Atualizar documentos + chunks", type="primary")

    log_panel = st.empty()
    table_panel = st.empty()
    err_panel = st.empty()

    if btn:
        # carrega ingest uma vez
        try:
            ingest_fn = _import_ingest()
        except Exception as e:
            st.error("Não consegui importar o módulo de ingest do CVM/IPE no deploy.")
            st.code(str(e))
            st.stop()

        t0 = _now_ms()
        results: List[Dict[str, Any]] = []
        errors: Dict[str, str] = {}

        progress = st.progress(0, text="Iniciando...")

        for i, tk in enumerate(tickers, start=1):
            start = _now_ms()
            before_docs = count_docs(tk)
            before_chunks = count_chunks(tk)

            progress.progress(int((i - 1) / max(1, len(tickers)) * 100), text=f"Processando {i}/{len(tickers)} — {tk}")

            with log_panel.container():
                st.info(f"🔎 {tk} — início | docs={before_docs} | chunks={before_chunks}")

            ingest_report: Optional[Dict[str, Any]] = None
            ingest_ran = False

            # ---- Ingest
            try:
                if (not only_missing_docs) or (before_docs == 0):
                    ingest_ran = True
                    r = _safe_call(
                        ingest_fn,
                        tickers=[tk],
                        window_months=int(window_months),
                        max_docs_per_ticker=int(max_docs),
                        max_runtime_s=float(max_runtime_s),
                        max_pdfs_per_ticker=int(max_pdfs),
                    )
                    # normaliza relatório
                    if isinstance(r, dict):
                        ingest_report = r
                    else:
                        ingest_report = {"result": str(r)}
                else:
                    ingest_report = {"skipped": True, "reason": "docs já existem"}
            except Exception as e:
                tb = traceback.format_exc()
                msg = f"Ingest {type(e).__name__}: {e}"
                errors[f"{tk}::ingest"] = tb if show_traceback else msg
                ingest_report = {"error": msg}
                with log_panel.container():
                    st.error(f"❌ {tk} — ingest falhou | {msg}")

            mid_docs = count_docs(tk)
            mid_chunks = count_chunks(tk)

            with log_panel.container():
                if ingest_ran:
                    st.write(f"📥 {tk} — ingest concluído | docs agora={mid_docs} | chunks={mid_chunks}")
                    if ingest_report:
                        st.caption("Relatório ingest (resumo):")
                        st.json({k: ingest_report[k] for k in ingest_report.keys() if k in {"matched","inserted","skipped","pdf_fetched","pdf_text_ok","error","result","skipped","reason"}})
                else:
                    st.write(f"📥 {tk} — ingest não executado (docs já existiam) | docs={mid_docs}")

            # Se ainda não tem docs, explique claramente e pule chunking
            if mid_docs == 0:
                results.append({
                    "ticker": tk,
                    "status": "SEM_DOCS",
                    "docs_before": before_docs,
                    "chunks_before": before_chunks,
                    "docs_after_ingest": mid_docs,
                    "chunks_after_ingest": mid_chunks,
                    "chunks_inseridos": 0,
                    "chunks_after": mid_chunks,
                    "tempo": _fmt_s(_now_ms() - start),
                    "motivo": (ingest_report.get("reason") if isinstance(ingest_report, dict) else "") or "Sem documentos retornados para a janela/fonte atual.",
                })
                with log_panel.container():
                    st.warning(
                        f"⚠️ {tk} — sem docs após ingest. "
                        f"Isso explica a execução rápida e ausência de chunks. "
                        f"Verifique janela (meses), filtros do ingest e disponibilidade de documentos no CVM/IPE."
                    )
                table_panel.dataframe(results, use_container_width=True)
                continue

            # ---- Chunking
            try:
                inserted = process_missing_chunks_for_ticker(tk, limit_docs=int(max_docs), max_chars=1500)
                after_docs = count_docs(tk)
                after_chunks = count_chunks(tk)

                results.append({
                    "ticker": tk,
                    "status": "OK",
                    "docs_before": before_docs,
                    "chunks_before": before_chunks,
                    "docs_after_ingest": mid_docs,
                    "chunks_after_ingest": mid_chunks,
                    "chunks_inseridos": int(inserted),
                    "chunks_after": after_chunks,
                    "tempo": _fmt_s(_now_ms() - start),
                    "motivo": "",
                })

                with log_panel.container():
                    st.success(f"✅ {tk} — chunking ok | +{inserted} chunks | chunks={after_chunks} | {_fmt_s(_now_ms()-start)}")

            except Exception as e:
                tb = traceback.format_exc()
                msg = f"Chunking {type(e).__name__}: {e}"
                errors[f"{tk}::chunking"] = tb if show_traceback else msg

                results.append({
                    "ticker": tk,
                    "status": "FALHA_CHUNK",
                    "docs_before": before_docs,
                    "chunks_before": before_chunks,
                    "docs_after_ingest": mid_docs,
                    "chunks_after_ingest": mid_chunks,
                    "chunks_inseridos": 0,
                    "chunks_after": None,
                    "tempo": _fmt_s(_now_ms() - start),
                    "motivo": msg,
                })

                with log_panel.container():
                    st.error(f"❌ {tk} — chunking falhou | {msg} | {_fmt_s(_now_ms()-start)}")

            table_panel.dataframe(results, use_container_width=True)

        progress.progress(100, text="Concluído")
        st.success(f"Fim. Tempo total: {_fmt_s(_now_ms() - t0)}")

        if errors:
            with err_panel.container():
                st.subheader("🧾 Logs de erro (por etapa)")
                for key, tb in errors.items():
                    with st.expander(key):
                        st.code(tb)

    st.divider()

    # ------------------------------------------------------------------
    # LLM
    # ------------------------------------------------------------------
    st.subheader("🤖 Análise qualitativa (LLM + RAG)")
    if not tickers:
        st.info("Sem tickers no snapshot.")
        return

    ticker_escolhido = st.selectbox("Ticker", tickers, index=0)

    # --- Recuperação de contexto (Top-K)
    use_topk_inteligente = st.checkbox("Usar Top-K inteligente (intenção futura)", value=True)
    top_k = st.slider("Top-K chunks (contexto)", min_value=3, max_value=12, value=6, step=1)

    # parâmetros do Top-K inteligente (só aparecem se habilitado)
    months_window = None
    debug_topk = False
    if use_topk_inteligente:
        c1, c2 = st.columns([1, 1])
        with c1:
            months_window = st.slider("Janela (meses) p/ Top-K inteligente", min_value=3, max_value=36, value=18, step=1)
        with c2:
            debug_topk = st.checkbox("Debug Top-K (score detalhado)", value=False)

    period_ref = st.text_input("period_ref (ex.: 2024Q4)", value="2024Q4")

    if st.button("Rodar LLM agora"):
        # --- Seleção de chunks
        chunks = []
        topk_debug_rows = None

        if use_topk_inteligente:
            # Import local com fallback (não quebra deploy se o módulo não existir)
            try:
                from core.rag_retriever import get_topk_chunks_inteligente as _get_topk_inteligente
            except Exception as e:
                st.warning(f"Top-K inteligente indisponível ({e}). Caindo para fetch_topk_chunks.")
                _get_topk_inteligente = None

            if _get_topk_inteligente is not None:
                # debug=True retorna objetos (ChunkHit); debug=False retorna lista de textos
                result = _get_topk_inteligente(
                    ticker_escolhido,
                    top_k=int(top_k),
                    months_window=int(months_window or 18),
                    debug=bool(debug_topk),
                )
                if debug_topk:
                    topk_debug_rows = [{
                        "chunk_id": h.chunk_id,
                        "doc_id": h.doc_id,
                        "tipo_doc": h.tipo_doc,
                        "data_doc": h.data_doc,
                        "score_final": round(h.score_final, 4),
                        "intent": round(h.score_intent, 4),
                        "recency": round(h.score_recency, 4),
                        "peso_tipo": round(h.weight_tipo, 4),
                    } for h in result]
                    chunks = [h.chunk_text for h in result]
                else:
                    chunks = result

        # fallback padrão (mantém compatibilidade total)
        if not chunks:
            from core.docs_corporativos_store import fetch_topk_chunks
            chunks = fetch_topk_chunks(ticker_escolhido, int(top_k))

        if not chunks:
            st.error("Sem chunks no Supabase para este ticker. Rode o ingest+chunking primeiro.")
            st.stop()

        if topk_debug_rows:
            st.subheader("🔎 Debug Top-K inteligente")
            st.dataframe(topk_debug_rows, use_container_width=True)

        contexto = "\n\n".join(chunks)
        client = llm_factory.get_llm_client()

        prompt = f"""
Você é um analista fundamentalista focado em direcionalidade estratégica e criação de valor ao acionista minoritário.
Importante: NÃO use DFP/ITR como base principal; foque em intenção futura (capex, expansão, dívida, dividendos, M&A, guidance).
Use somente o CONTEXTO abaixo. Devolva APENAS JSON válido na estrutura:

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
            snapshot_id=str(snapshot_id),
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
