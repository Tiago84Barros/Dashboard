# -*- coding: utf-8 -*-
"""
dashboard/page/analises_portfolio.py

Página "Análises de Portfólio" (Patch 6) — com LOGS detalhados por ticker.

IMPORTANTE:
- O dashboard.py do seu projeto carrega páginas via função render().
- Portanto, TODO o código Streamlit desta página fica dentro de render().

Dependências:
- core.portfolio_snapshot_store
- core.docs_corporativos_store
- core.patch6_runs_store
- core.ai_models.llm_client.factory
"""

from __future__ import annotations

import json
import time
import traceback
from typing import Any, Dict, List

import streamlit as st

from core.portfolio_snapshot_store import get_latest_snapshot
from core.docs_corporativos_store import (
    count_docs,
    count_chunks,
    process_missing_chunks_for_ticker,
    fetch_topk_chunks,
)
from core.patch6_runs_store import save_patch6_run, list_patch6_history

import core.ai_models.llm_client.factory as llm_factory


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fmt_s(ms: int) -> str:
    return f"{ms/1000:.1f}s"


def _safe_upper(x: Any) -> str:
    return str(x or "").strip().upper()


def render() -> None:
    st.title("🧠 Análises de Portfólio (LLM + RAG)")

    snapshot = get_latest_snapshot()
    if not snapshot:
        st.warning("Nenhum snapshot ativo encontrado. Execute primeiro a Criação de Portfólio.")
        st.stop()

    snapshot_id = str(snapshot.get("id") or "")
    created_at = str(snapshot.get("created_at") or "")
    selic_ref = snapshot.get("selic_ref")
    margem_superior = snapshot.get("margem_superior")
    tipo_empresa = snapshot.get("tipo_empresa")

    st.caption(f"Snapshot: `{snapshot_id}`")
    st.caption(f"Criado em: {created_at}")
    st.caption(f"Margem vs Selic: {margem_superior} | Selic ref: {selic_ref} | Tipo empresa: {tipo_empresa}")

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
    status_rows: List[Dict[str, Any]] = []
    for tk in tickers:
        status_rows.append({"ticker": tk, "docs": count_docs(tk), "chunks": count_chunks(tk)})
    st.dataframe(status_rows, use_container_width=True)

    st.divider()

    # ------------------------------------------------------------------
    # Atualizar evidências -> chunking com logs por ticker
    # ------------------------------------------------------------------
    st.subheader("📦 Atualizar evidências (CVM/IPE)")

    colA, colB, colC = st.columns([1, 1, 1])
    with colA:
        limit_docs = st.number_input("Limite de docs por ticker", min_value=5, max_value=200, value=60, step=5)
    with colB:
        max_chars = st.number_input("Tamanho do chunk (chars)", min_value=600, max_value=4000, value=1500, step=100)
    with colC:
        show_traceback = st.checkbox("Mostrar traceback completo", value=True)

    btn = st.button("Atualizar documentos + chunks", type="primary")

    log_panel = st.empty()
    table_panel = st.empty()
    err_panel = st.empty()

    if btn:
        t0 = _now_ms()
        ok = 0
        fail = 0
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

            try:
                inserted = process_missing_chunks_for_ticker(
                    tk,
                    limit_docs=int(limit_docs),
                    max_chars=int(max_chars),
                )
                after_docs = count_docs(tk)
                after_chunks = count_chunks(tk)
                ok += 1

                results.append({
                    "ticker": tk,
                    "status": "OK",
                    "docs_before": before_docs,
                    "chunks_before": before_chunks,
                    "chunks_inseridos": int(inserted),
                    "docs_after": after_docs,
                    "chunks_after": after_chunks,
                    "tempo": _fmt_s(_now_ms() - start),
                    "erro": "",
                })

                with log_panel.container():
                    st.success(f"✅ {tk} — ok | +{inserted} chunks | docs={after_docs} | chunks={after_chunks} | {_fmt_s(_now_ms()-start)}")

            except Exception as e:
                fail += 1
                tb = traceback.format_exc()
                msg = f"{type(e).__name__}: {e}"
                errors[tk] = tb if show_traceback else msg

                results.append({
                    "ticker": tk,
                    "status": "FALHA",
                    "docs_before": before_docs,
                    "chunks_before": before_chunks,
                    "chunks_inseridos": 0,
                    "docs_after": None,
                    "chunks_after": None,
                    "tempo": _fmt_s(_now_ms() - start),
                    "erro": msg,
                })

                with log_panel.container():
                    st.error(f"❌ {tk} — falhou | {msg} | {_fmt_s(_now_ms()-start)}")

            table_panel.dataframe(results, use_container_width=True)

        progress.progress(100, text="Concluído")

        elapsed = _fmt_s(_now_ms() - t0)
        if fail == 0:
            st.success(f"Atualização concluída. OK: {ok} | Falhas: {fail} | Tempo: {elapsed}")
        else:
            st.warning(f"Atualização concluída. OK: {ok} | Falhas: {fail} | Tempo: {elapsed}")

        if errors:
            with err_panel.container():
                st.subheader("🧾 Logs de erro por ticker")
                for tk, tb in errors.items():
                    with st.expander(f"Erro — {tk}"):
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
    top_k = st.slider("Top-K chunks (contexto)", min_value=3, max_value=12, value=6, step=1)
    period_ref = st.text_input("period_ref (ex.: 2024Q4)", value="2024Q4")

    if st.button("Rodar LLM agora"):
        chunks = fetch_topk_chunks(ticker_escolhido, int(top_k))
        if not chunks:
            st.error("Sem chunks no Supabase para este ticker. Rode o chunking primeiro.")
            st.stop()

        contexto = "\n\n".join(chunks)
        client = llm_factory.get_llm_client()

        prompt = f"""\
Você é um analista fundamentalista focado em direcionalidade estratégica.
Use somente o CONTEXTO abaixo. Devolva APENAS JSON válido na estrutura:

{{
  \"perspectiva_compra\": \"forte|moderada|fraca\",
  \"resumo\": \"texto curto\",
  \"pontos_chave\": [\"...\"],
  \"riscos\": [\"...\"],
  \"evidencias\": [\"trechos literais do contexto\"]
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
            snapshot_id=snapshot_id,
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
