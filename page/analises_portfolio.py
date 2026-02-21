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
    # LLM (Batch para todo o portfólio)
    # ------------------------------------------------------------------
    st.subheader("🤖 Análise qualitativa (LLM + RAG)")

    if not tickers:
        st.info("Sem tickers no snapshot.")
        return

    # Preferência: rodar para todo o portfólio (com cards por ticker)
    rodar_todo_portfolio = st.checkbox("Rodar LLM para todo o portfólio (recomendado)", value=True)
    usar_topk_inteligente = st.checkbox("Usar Top-K inteligente (intenção futura)", value=True)
    debug_topk = st.checkbox("Debug Top-K (score detalhado)", value=False)

    top_k = st.slider("Top-K chunks", min_value=3, max_value=12, value=6, step=1)
    janela_meses = st.slider("Janela (meses) p/ Top-K inteligente", min_value=3, max_value=24, value=12, step=1)
    period_ref = st.text_input("period_ref (ex.: 2024Q4)", value="2024Q4")

    if not rodar_todo_portfolio:
        ticker_escolhido = st.selectbox("Ticker", tickers, index=0)
        tickers_alvo = [ticker_escolhido]
    else:
        tickers_alvo = list(tickers)

    # CSS simples para cards
    st.markdown(
    """
    <style>
    /* ---------- Patch6 Cards (Profissional) ---------- */
    .p6-wrap { margin: 14px 0 18px 0; }
    .p6-card{
        border: 1px solid rgba(255,255,255,0.10);
        border-radius: 16px;
        padding: 16px 18px;
        background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.03));
        box-shadow: 0 10px 26px rgba(0,0,0,0.22);
    }
    .p6-top{
        display:flex; align-items:flex-start; justify-content:space-between;
        gap: 12px; margin-bottom: 10px;
    }
    .p6-title{
        display:flex; align-items:center; gap:10px; flex-wrap:wrap;
        margin:0; font-size:18px; font-weight:700;
    }
    .p6-sub{
        margin: 2px 0 0 0; opacity:0.85; font-size:13px;
    }
    .p6-badges{ display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
    .p6-pill{
        display:inline-flex; align-items:center; gap:6px;
        padding: 4px 10px; border-radius:999px;
        font-size:12px; font-weight:600;
        border: 1px solid rgba(255,255,255,0.14);
        background: rgba(255,255,255,0.05);
    }
    .p6-pill strong{ font-weight:800; }
    .p6-forte{ background: rgba(34,197,94,0.16); border-color: rgba(34,197,94,0.35); }
    .p6-moderada{ background: rgba(234,179,8,0.16); border-color: rgba(234,179,8,0.35); }
    .p6-fraca{ background: rgba(239,68,68,0.16); border-color: rgba(239,68,68,0.35); }
    .p6-meta{ opacity:0.78; font-size:12px; }
    .p6-grid{
        display:grid;
        grid-template-columns: 1fr 1fr;
        gap: 12px;
        margin-top: 12px;
    }
    @media (max-width: 900px){
        .p6-grid{ grid-template-columns: 1fr; }
    }
    .p6-box{
        border: 1px solid rgba(255,255,255,0.10);
        border-radius: 14px;
        padding: 12px 12px;
        background: rgba(255,255,255,0.03);
    }
    .p6-box h4{
        margin: 0 0 8px 0;
        font-size: 13px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        opacity: 0.85;
    }
    .p6-divider{
        height: 1px;
        background: rgba(255,255,255,0.10);
        margin: 12px 0;
    }
    .p6-cons{
        font-size: 13px;
        opacity: 0.9;
        line-height: 1.45;
    }
    .p6-evid{
        border-left: 3px solid rgba(255,255,255,0.18);
        padding-left: 10px;
        margin: 8px 0;
        opacity: 0.92;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
def _pill_class(persp: str) -> str:
        p = (persp or "").strip().lower()
        if "fort" in p:
            return "pill-forte"
        if "mod" in p:
            return "pill-moderada"
        return "pill-fraca"

    def _render_card(ticker: str, result: Dict[str, Any]) -> None:
    """Renderiza um card profissional por ticker com resultado da LLM."""
    persp = str(result.get("perspectiva_compra", "")).strip()
    resumo = str(result.get("resumo", "")).strip()

    pontos = result.get("pontos_chave") or result.get("pontos-chave") or result.get("pontos") or []
    riscos = result.get("riscos") or []
    evids = result.get("evidencias") or result.get("evidências") or []
    consideracoes = (
        result.get("consideracoes_llm")
        or result.get("considerações_llm")
        or result.get("consideracoes")
        or result.get("observacoes")
        or ""
    )
    confianca = result.get("confianca") or result.get("confidence") or ""

    if isinstance(pontos, str): pontos = [pontos]
    if isinstance(riscos, str): riscos = [riscos]
    if isinstance(evids, str): evids = [evids]

    pill_cls = _pill_class(persp)
    persp_label = persp or "—"

    st.markdown('<div class="p6-wrap"><div class="p6-card">', unsafe_allow_html=True)

    st.markdown(
        f'''
        <div class="p6-top">
          <div>
            <div class="p6-title">📌 {ticker}
              <span class="p6-pill {pill_cls}"><strong>{persp_label}</strong></span>
            </div>
            <div class="p6-sub">Intenção futura e alocação de capital (capex, dívida, expansão, dividendos, M&A) com evidências CVM/IPE.</div>
          </div>
          <div class="p6-badges">
            <span class="p6-pill"><span class="p6-meta">Top-K</span> <strong>{int(top_k)}</strong></span>
            <span class="p6-pill"><span class="p6-meta">Janela</span> <strong>{int(janela_meses)}m</strong></span>
            <span class="p6-pill"><span class="p6-meta">period_ref</span> <strong>{period_ref}</strong></span>
          </div>
        </div>
        ''',
        unsafe_allow_html=True,
    )

    if resumo:
        st.markdown(f'<div class="p6-cons">{resumo}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="p6-cons">—</div>', unsafe_allow_html=True)

    if consideracoes:
        st.markdown('<div class="p6-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="p6-box"><h4>Considerações da LLM</h4>', unsafe_allow_html=True)
        st.markdown(f'<div class="p6-cons">{consideracoes}</div></div>', unsafe_allow_html=True)

    if str(confianca).strip():
        st.markdown(
            f'<div class="p6-divider"></div><div class="p6-meta">Confiança (auto-relatada): <strong>{confianca}</strong></div>',
            unsafe_allow_html=True,
        )

    st.markdown('<div class="p6-divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="p6-grid">', unsafe_allow_html=True)

    st.markdown('<div class="p6-box"><h4>Pontos-chave</h4>', unsafe_allow_html=True)
    st.write(pontos if pontos else ["—"])
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="p6-box"><h4>Riscos</h4>', unsafe_allow_html=True)
    st.write(riscos if riscos else ["—"])
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

    if evids:
        with st.expander("📎 Evidências (trechos literais)"):
            for ev in evids[:10]:
                st.markdown(f'<div class="p6-evid">{ev}</div>', unsafe_allow_html=True)

    st.markdown("</div></div>", unsafe_allow_html=True)

def _get_chunks_para_ticker(ticker: str) -> Tuple[List[str], str]:
        """Retorna (chunks, origem)."""
        from core.docs_corporativos_store import fetch_topk_chunks

        if not usar_topk_inteligente:
            return fetch_topk_chunks(ticker, int(top_k)), "fetch_topk_chunks"

        try:
            from core.rag_retriever import get_topk_chunks_inteligente  # type: ignore
            chunks, _debug = get_topk_chunks_inteligente(
                ticker=ticker,
                top_k=int(top_k),
                months=int(janela_meses),
                debug=bool(debug_topk),
            )
            if debug_topk and _debug:
                with st.expander(f"Debug Top-K — {ticker}"):
                    st.json(_debug)
            return chunks, "topk_inteligente"
        except Exception as e:
            st.warning(
                f"Top-K inteligente indisponível ({type(e).__name__}: {e}). "
                "Caindo para fetch_topk_chunks."
            )
            return fetch_topk_chunks(ticker, int(top_k)), "fetch_topk_chunks_fallback"

    def _run_llm_for_ticker(ticker: str) -> Dict[str, Any]:
        chunks, origem = _get_chunks_para_ticker(ticker)
        if not chunks:
            raise RuntimeError(f"Sem chunks para {ticker} (origem={origem}). Rode ingest+chunking.")

        context_payload = [{"chunk_index": i, "text": c} for i, c in enumerate(chunks)]

        system = (
            "Você é um analista fundamentalista focado em direcionalidade estratégica. "
            "Seu objetivo é julgar a intenção futura e alocação de capital com base nas evidências fornecidas."
        )

        user = (
            f"Analise o ticker {ticker} e responda com foco em:\n"
            "- capex / expansão\n"
            "- guidance / prioridades do management\n"
            "- desalavancagem / gestão de dívida\n"
            "- M&A / desinvestimentos\n"
            "- dividendos / recompra / payout\n\n"
            "Regras:\n"
            "1) Use SOMENTE o contexto (RAG).\n"
            "2) Evidências devem ser trechos literais do contexto.\n"
            "3) Seja objetivo e útil para o acionista minoritário.\n"
        )

        schema_hint = """{
  "perspectiva_compra": "forte|moderada|fraca",
  "resumo": "texto curto",
  "pontos_chave": ["..."],
  "riscos": ["..."],
  "evidencias": ["trechos literais do contexto"],
  "consideracoes_llm": "como a LLM interpretou as evidências e limitações",
  "confianca": "alta|média|baixa"
}"""

        client = llm_factory.get_llm_client()
        return client.generate_json(
            system=system,
            user=user,
            schema_hint=schema_hint,
            context=context_payload,
        )

    if st.button("Rodar LLM agora"):
        progress = st.progress(0, text="Iniciando...")
        status_rows: List[Dict[str, Any]] = []

        fortes = moderadas = fracas = 0
        erros_sem_dados = 0

        for i, tkr in enumerate(tickers_alvo, start=1):
            progress.progress(int((i - 1) / max(len(tickers_alvo), 1) * 100), text=f"Processando {tkr} ({i}/{len(tickers_alvo)})...")

            try:
                with st.status(f"Processando {tkr} ({i}/{len(tickers_alvo)})...", expanded=False) as stt:
                    result = _run_llm_for_ticker(tkr)
                    stt.update(label=f"{tkr}: LLM OK. Salvando...", state="running")

                save_patch6_run(
                    snapshot_id=str(snapshot_id),
                    ticker=tkr,
                    period_ref=period_ref,
                    result=result,
                )

                _render_card(tkr, result)

                persp = str(result.get("perspectiva_compra", "")).lower()
                if "fort" in persp:
                    fortes += 1
                elif "mod" in persp:
                    moderadas += 1
                else:
                    fracas += 1

                status_rows.append({"ticker": tkr, "status": "OK", "erro": ""})

            except Exception as e:
                erros_sem_dados += 1
                status_rows.append({"ticker": tkr, "status": "ERRO_LLM", "erro": f"{type(e).__name__}: {e}"})

        progress.progress(100, text="Concluído")

        st.subheader("📌 Parecer resumido do portfólio")
        st.write(f"Forte: **{fortes}** | Moderada: **{moderadas}** | Fraca: **{fracas}** | Erros/sem dados: **{erros_sem_dados}**")

        st.subheader("🧾 Status por ticker")
        st.dataframe(status_rows, use_container_width=True)

    st.subheader("📜 Histórico (patch6_runs)")
    try:
        ticker_hist = tickers_alvo[0] if tickers_alvo else tickers[0]
        hist = list_patch6_history(ticker_hist, limit=12)
        st.dataframe(hist, use_container_width=True)
    except Exception as e:
        st.caption(f"Não foi possível carregar histórico: {type(e).__name__}: {e}")
