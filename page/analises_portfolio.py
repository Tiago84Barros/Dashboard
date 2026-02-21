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
    
    # ------------------------------------------------------------------
    # LLM (RAG + julgamento qualitativo)
    # ------------------------------------------------------------------
    st.subheader("🤖 Análise qualitativa (LLM + RAG)")
    if not tickers:
        st.info("Sem tickers no snapshot.")
        return

    # CSS dos cards
    st.markdown(
        """
        <style>
          .p6-card{border:1px solid rgba(255,255,255,.10);border-radius:16px;padding:16px 16px 12px 16px;
                   background:rgba(255,255,255,.03);margin:12px 0;}
          .p6-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:10px}
          .p6-title{font-size:18px;font-weight:700;letter-spacing:.2px}
          .p6-badges{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}
          .p6-pill{font-size:12px;padding:4px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.10);opacity:.95}
          .p6-pill-forte{background:rgba(34,197,94,.15);border-color:rgba(34,197,94,.35)}
          .p6-pill-moderada{background:rgba(234,179,8,.15);border-color:rgba(234,179,8,.35)}
          .p6-pill-fraca{background:rgba(239,68,68,.15);border-color:rgba(239,68,68,.35)}
          .p6-pill-info{background:rgba(59,130,246,.12);border-color:rgba(59,130,246,.30)}
          .p6-grid{display:grid;grid-template-columns:1fr;gap:10px}
          .p6-k{font-weight:700}
          .p6-muted{opacity:.75}
          .p6-list{margin:6px 0 0 18px}
          .p6-hr{height:1px;background:rgba(255,255,255,.08);border:none;margin:12px 0}
        </style>
        """,
        unsafe_allow_html=True,
    )

    def _pill_class(p: str) -> str:
        p = (p or "").strip().lower()
        if p == "forte":
            return "p6-pill p6-pill-forte"
        if p == "moderada":
            return "p6-pill p6-pill-moderada"
        return "p6-pill p6-pill-fraca"

    def _as_list(x: Any) -> List[str]:
        if x is None:
            return []
        if isinstance(x, list):
            return [str(i) for i in x if str(i).strip()]
        if isinstance(x, str):
            s = x.strip()
            return [s] if s else []
        return [str(x)]

    def _render_card(ticker: str, result: Dict[str, Any], top_k_used: int, period_ref: str) -> None:
        persp = str(result.get("perspectiva_compra", "")).strip()
        resumo = str(result.get("resumo", "")).strip()

        consider = (
            result.get("consideracoes_llm")
            or result.get("consideracoes")
            or result.get("observacoes")
            or result.get("rationale")
            or ""
        )
        consider = str(consider).strip()

        confianca = result.get("confianca", result.get("confidence", ""))
        confianca = "" if confianca is None else str(confianca).strip()

        pontos = _as_list(result.get("pontos_chave") or result.get("pontos-chave") or result.get("pontos"))
        riscos = _as_list(result.get("riscos"))
        evid = _as_list(result.get("evidencias") or result.get("evidence") or result.get("citacoes"))

        # Card (HTML)
        st.markdown(
            f"""
            <div class="p6-card">
              <div class="p6-head">
                <div class="p6-title">{ticker}</div>
                <div class="p6-badges">
                  <span class="{_pill_class(persp)}">{(persp or "—").upper()}</span>
                  <span class="p6-pill p6-pill-info">Top-K: {top_k_used}</span>
                  <span class="p6-pill p6-pill-info">period_ref: {period_ref}</span>
                </div>
              </div>

              <div class="p6-grid">
                <div><span class="p6-k">Resumo:</span> <span class="p6-muted">{resumo or "—"}</span></div>
                {f'<div><span class="p6-k">Considerações da LLM:</span> <span class="p6-muted">{consider}</span></div>' if consider else ''}
                {f'<div><span class="p6-k">Confiança:</span> <span class="p6-muted">{confianca}</span></div>' if confianca else ''}
              </div>

              <hr class="p6-hr"/>

              <div class="p6-grid">
                <div>
                  <span class="p6-k">Pontos-chave</span>
                  <ul class="p6-list">
                    {''.join([f'<li>{st._utils.escape_markdown(p)}</li>' for p in pontos]) if pontos else '<li class="p6-muted">—</li>'}
                  </ul>
                </div>

                <div>
                  <span class="p6-k">Riscos</span>
                  <ul class="p6-list">
                    {''.join([f'<li>{st._utils.escape_markdown(r)}</li>' for r in riscos]) if riscos else '<li class="p6-muted">—</li>'}
                  </ul>
                </div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if evid:
            with st.expander(f"📌 Evidências (trechos) — {ticker}", expanded=False):
                for i, e in enumerate(evid[:12], start=1):
                    st.markdown(f"**{i}.** {e}")

    # Controles
    rodar_todo = st.checkbox("Rodar LLM para todo o portfólio (recomendado)", value=True)
    usar_topk_inteligente = st.checkbox("Usar Top-K inteligente (intenção futura)", value=True)
    debug_topk = st.checkbox("Debug Top-K (score detalhado)", value=False)

    top_k = st.slider("Top-K chunks", min_value=3, max_value=12, value=6, step=1)
    st.number_input("Janela (meses) p/ Top-K inteligente", value=12, step=1, disabled=True)
    window_months = 12

    period_ref = st.text_input("period_ref (ex.: 2024Q4)", value="2024Q4")

    # Wrappers
    def _call_llm(client: Any, prompt: str) -> str:
        # tenta métodos conhecidos sem acoplar ao SDK
        if hasattr(client, "complete") and callable(getattr(client, "complete")):
            return client.complete(prompt)
        if hasattr(client, "chat") and callable(getattr(client, "chat")):
            return client.chat(prompt)
        if hasattr(client, "invoke") and callable(getattr(client, "invoke")):
            return client.invoke(prompt)
        if callable(client):
            return client(prompt)
        raise AttributeError("Cliente LLM não expõe complete/chat/invoke nem é callável.")

    def _get_chunks_for_ticker(t: str) -> Tuple[List[str], str]:
        # preferir Top-K inteligente; fallback para fetch_topk_chunks
        try:
            if usar_topk_inteligente:
                from core.rag_retriever import get_topk_chunks_inteligente  # type: ignore
                chunks, meta = get_topk_chunks_inteligente(
                    ticker=t,
                    top_k=int(top_k),
                    window_months=int(window_months),
                    debug=bool(debug_topk),
                )
                return chunks or [], "topk_inteligente"
        except Exception:
            # cai no fetch simples
            pass

        from core.docs_corporativos_store import fetch_topk_chunks
        chunks = fetch_topk_chunks(t, int(top_k))
        return chunks or [], "fetch_topk_chunks"

    def _build_prompt(contexto: str) -> str:
        return f"""
Você é um analista fundamentalista focado em direcionalidade estratégica e alocação de capital.
Use SOMENTE o CONTEXTO abaixo (RAG). Avalie o caminho futuro (capex/expansão, dívida/desalavancagem,
guidance, M&A/desinvestimentos, dividendos/recompra) e impacto potencial no acionista minoritário.

Devolva APENAS JSON válido no formato:

{{
  "perspectiva_compra": "forte|moderada|fraca",
  "resumo": "2-4 linhas objetivas",
  "consideracoes_llm": "1-3 linhas com ressalvas/hipóteses (ex.: falta de dados, ambiguidade, dependências)",
  "confianca": "alta|media|baixa",
  "pontos_chave": ["..."],
  "riscos": ["..."],
  "evidencias": ["trechos literais do contexto (curtos)"]
}}

CONTEXTO:
{contexto}
"""

    if st.button("Rodar LLM agora"):
        client = llm_factory.get_llm_client()

        tickers_run = tickers if rodar_todo else [st.selectbox("Ticker", tickers, index=0)]
        total = len(tickers_run)

        st.info("Iniciando leitura qualitativa… os cards aparecem à medida que cada ticker finalizar.")
        prog = st.progress(0)
        status_box = st.empty()

        fortes = moderadas = fracas = erros = 0
        status_rows: List[Dict[str, Any]] = []

        for i, t in enumerate(tickers_run, start=1):
            status_box.markdown(f"✅ Processando **{t}** ({i}/{total})…")
            t0 = time.time()

            try:
                chunks, fonte_chunks = _get_chunks_for_ticker(t)
                if not chunks:
                    erros += 1
                    status_rows.append({"ticker": t, "status": "SEM_CHUNKS", "erro": "Sem chunks no Supabase"})
                    prog.progress(int(i / total * 100))
                    continue

                contexto = "\n\n".join(chunks)
                raw = _call_llm(client, _build_prompt(contexto))

                try:
                    result = json.loads(raw)
                except Exception:
                    erros += 1
                    status_rows.append({"ticker": t, "status": "JSON_INVALIDO", "erro": "LLM não retornou JSON"})
                    with st.expander(f"⚠️ Resposta bruta (debug) — {t}", expanded=False):
                        st.code(raw)
                    prog.progress(int(i / total * 100))
                    continue

                # salva
                save_patch6_run(
                    snapshot_id=str(snapshot_id),
                    ticker=t,
                    period_ref=period_ref,
                    result=result,
                )

                # conta perspectiva
                p = str(result.get("perspectiva_compra", "")).strip().lower()
                if p == "forte":
                    fortes += 1
                elif p == "moderada":
                    moderadas += 1
                elif p == "fraca":
                    fracas += 1
                else:
                    erros += 1

                # mostra card (IMEDIATO)
                _render_card(ticker=t, result=result, top_k_used=int(top_k), period_ref=period_ref)

                status_rows.append(
                    {
                        "ticker": t,
                        "status": "OK",
                        "metodo_chunks": fonte_chunks,
                        "tempo_s": round(time.time() - t0, 1),
                    }
                )

            except Exception as e:
                erros += 1
                status_rows.append({"ticker": t, "status": "ERRO_LLM", "erro": str(e)})
                with st.expander(f"❌ Erro (traceback) — {t}", expanded=False):
                    st.code(traceback.format_exc())

            prog.progress(int(i / total * 100))

        status_box.markdown("✅ Concluído.")
        st.subheader("📌 Parecer resumido do portfólio")
        st.write(f"Forte: **{fortes}** | Moderada: **{moderadas}** | Fraca: **{fracas}** | Erros/sem dados: **{erros}**")

        st.subheader("🧾 Status por ticker")
        st.dataframe(status_rows, use_container_width=True)

    # Histórico
    st.subheader("📜 Histórico (patch6_runs)")
    try:
        hist = list_patch6_history(ticker_escolhido, limit=8)
        st.dataframe(hist, use_container_width=True)
    except Exception as e:
        st.caption(f"Não foi possível carregar histórico: {type(e).__name__}: {e}")
