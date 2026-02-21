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

    # CSS para cards (auditáveis)
    st.markdown(
        """
        <style>
        .p6-card {{
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.10);
            padding: 14px 16px;
            border-radius: 12px;
            margin: 10px 0 14px 0;
        }}
        .p6-head {{
            display:flex; justify-content:space-between; align-items:center;
            gap: 12px;
        }}
        .p6-ticker {{
            font-weight: 800; font-size: 18px;
        }}
        .p6-badge {{
            padding: 4px 10px;
            border-radius: 999px;
            font-weight: 700;
            font-size: 12px;
            border: 1px solid rgba(255,255,255,0.18);
        }}
        .p6-badge-forte {{ background: rgba(34,197,94,0.18); }}
        .p6-badge-moderada {{ background: rgba(234,179,8,0.18); }}
        .p6-badge-fraca {{ background: rgba(239,68,68,0.18); }}
        .p6-small {{
            color: rgba(255,255,255,0.75);
            font-size: 13px;
            line-height: 1.45;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

    def _llm_complete(client, prompt: str) -> str:
        """Compat: tenta complete/chat/invoke/generate."""
        for fn_name in ("complete", "chat", "invoke", "generate"):
            fn = getattr(client, fn_name, None)
            if callable(fn):
                out = fn(prompt)
                if isinstance(out, str):
                    return out
                try:
                    return json.dumps(out, ensure_ascii=False)
                except Exception:
                    return str(out)
        raise AttributeError("Cliente LLM não expõe complete/chat/invoke/generate")

    if not tickers:
        st.info("Sem tickers no snapshot.")
        return

    colA, colB, colC = st.columns([1.2, 1.0, 1.0])
    with colA:
        modo_lote = st.checkbox("Rodar LLM para todo o portfólio (recomendado)", value=True)
    with colB:
        use_topk_inteligente = st.checkbox("Usar Top-K inteligente (intenção futura)", value=True)
    with colC:
        top_k = st.slider("Top-K chunks", min_value=3, max_value=12, value=6, step=1)

    months_window = 18
    debug_topk = False
    if use_topk_inteligente:
        c1, c2 = st.columns([1, 1])
        with c1:
            months_window = st.slider("Janela (meses) p/ Top-K inteligente", min_value=3, max_value=36, value=18, step=1)
        with c2:
            debug_topk = st.checkbox("Debug Top-K (score detalhado)", value=False)

    period_ref = st.text_input("period_ref (ex.: 2024Q4)", value="2024Q4")

    if not modo_lote:
        ticker_escolhido = st.selectbox("Ticker (modo unitário)", tickers, index=0)
    else:
        ticker_escolhido = None

    def _get_chunks_para_ticker(tkr: str):
        chunks = []
        topk_debug_rows = None
        if use_topk_inteligente:
            try:
                from core.rag_retriever import get_topk_chunks_inteligente as _get_topk_inteligente
            except Exception as err:
                st.warning(f"Top-K inteligente indisponível ({err}). Caindo para fetch_topk_chunks.")
                _get_topk_inteligente = None
            if _get_topk_inteligente is not None:
                result = _get_topk_inteligente(
                    tkr,
                    top_k=int(top_k),
                    months_window=int(months_window),
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
        if not chunks:
            from core.docs_corporativos_store import fetch_topk_chunks
            chunks = fetch_topk_chunks(tkr, int(top_k))
        return chunks, topk_debug_rows

    def _badge_class(p: str) -> str:
        p = (p or "").lower()
        if "fort" in p:
            return "p6-badge p6-badge-forte"
        if "moder" in p:
            return "p6-badge p6-badge-moderada"
        return "p6-badge p6-badge-fraca"

    if st.button("Rodar LLM agora"):
        tickers_alvo = tickers if modo_lote else [ticker_escolhido]
        progresso = st.progress(0.0)
        resumo_portfolio = {"forte": 0, "moderada": 0, "fraca": 0, "erros": 0}

        client = llm_factory.get_llm_client()

        for idx, tkr in enumerate(tickers_alvo, start=1):
            with st.status(f"Processando {tkr} ({idx}/{len(tickers_alvo)})...", expanded=False):
                chunks, topk_debug_rows = _get_chunks_para_ticker(tkr)

                if not chunks:
                    resumo_portfolio["erros"] += 1
                    st.warning(f"{tkr}: sem chunks no Supabase (rode ingest+chunking).")
                    progresso.progress(idx / len(tickers_alvo))
                    continue

                contexto = "\n\n".join(chunks)

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
""".strip()

                try:
                    raw = _llm_complete(client, prompt)
                except Exception as err:
                    resumo_portfolio["erros"] += 1
                    st.error(f"{tkr}: falha ao chamar LLM: {err}")
                    progresso.progress(idx / len(tickers_alvo))
                    continue

                try:
                    resultado = json.loads(raw)
                except Exception:
                    resumo_portfolio["erros"] += 1
                    st.error(f"{tkr}: LLM não retornou JSON válido.")
                    st.code(raw)
                    progresso.progress(idx / len(tickers_alvo))
                    continue

                try:
                    save_patch6_run(
                        snapshot_id=str(snapshot_id),
                        ticker=tkr,
                        period_ref=period_ref,
                        result=resultado,
                    )
                except Exception as err:
                    st.caption(f"{tkr}: aviso ao salvar patch6_runs: {err}")

                perspectiva = (resultado.get("perspectiva_compra") or "").lower()
                if "fort" in perspectiva:
                    resumo_portfolio["forte"] += 1
                elif "moder" in perspectiva:
                    resumo_portfolio["moderada"] += 1
                else:
                    resumo_portfolio["fraca"] += 1

                badge = _badge_class(perspectiva)
                resumo = resultado.get("resumo", "")

                st.markdown(
                    f"""
<div class="p6-card">
  <div class="p6-head">
    <div class="p6-ticker">{tkr}</div>
    <div class="{badge}">{perspectiva or "—"}</div>
  </div>
  <div class="p6-small" style="margin-top:8px;"><b>Resumo:</b> {resumo}</div>
</div>
""",
                    unsafe_allow_html=True
                )

                with st.expander(f"Detalhes – {tkr}", expanded=False):
                    if topk_debug_rows:
                        st.caption("Debug Top-K inteligente")
                        st.dataframe(topk_debug_rows, use_container_width=True)
                    st.markdown("**Pontos-chave**")
                    for p in (resultado.get("pontos_chave") or [])[:8]:
                        st.write(f"- {p}")
                    st.markdown("**Riscos**")
                    for r in (resultado.get("riscos") or [])[:8]:
                        st.write(f"- {r}")
                    st.markdown("**Evidências (trechos)**")
                    for ev in (resultado.get("evidencias") or [])[:6]:
                        st.write(f"• {ev}")

            progresso.progress(idx / len(tickers_alvo))

        st.markdown("---")
        st.subheader("📌 Parecer resumido do portfólio")
        st.write(
            f"Forte: {resumo_portfolio['forte']} | Moderada: {resumo_portfolio['moderada']} | "
            f"Fraca: {resumo_portfolio['fraca']} | Erros/sem dados: {resumo_portfolio['erros']}"
        )

    st.subheader("📜 Histórico (patch6_runs)")
    try:
        hist = list_patch6_history(ticker_escolhido, limit=8)
        st.dataframe(hist, use_container_width=True)
    except Exception as e:
        st.caption(f"Não foi possível carregar histórico: {type(e).__name__}: {e}")
