# dashboard/page/patch6_teste.py
# Patch 6 — Teste (Ingest + LLM)
#
# Arquivo corrigido e consolidado
# Compatível com:
# - core.db_loader
# - ingest fallback A/B/C
# - runner LLM em core.ai_models.pipelines

from __future__ import annotations

import importlib
import inspect
import json
import pkgutil
from typing import Any, Callable, Dict, List, Optional, Tuple

import streamlit as st


# -------------------------------
# Helpers
# -------------------------------
def _parse_tickers(raw: str) -> List[str]:
    if not raw:
        return []
    out: List[str] = []
    for p in raw.replace(";", ",").split(","):
        t = (p or "").strip().upper()
        if t:
            out.append(t)
    seen = set()
    uniq = []
    for t in out:
        if t not in seen:
            uniq.append(t)
            seen.add(t)
    return uniq


def _safe_call(fn: Callable[..., Any], **kwargs) -> Any:
    try:
        sig = inspect.signature(fn)
    except Exception:
        return fn(**kwargs)

    accepted = {}
    for k, v in kwargs.items():
        if k in sig.parameters:
            accepted[k] = v

    return fn(**accepted)


# -------------------------------
# Supabase
# -------------------------------
def _get_supabase_client() -> Any:
    candidates = [
        ("core.db_loader", ["get_supabase", "get_supabase_client"]),
    ]

    for mod_name, attrs in candidates:
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            continue

        for a in attrs:
            obj = getattr(mod, a, None)
            if callable(obj):
                return obj()

    raise RuntimeError("Não consegui obter supabase client.")


def count_docs_by_tickers(tickers: List[str]) -> Tuple[int, Dict[str, int]]:
    sb = _get_supabase_client()
    total = 0
    by: Dict[str, int] = {}

    for tk in tickers:
        res = (
            sb.table("docs_corporativos")
            .select("id", count="exact")
            .eq("ticker", tk)
            .execute()
        )
        cnt = int(getattr(res, "count", None) or 0)
        by[tk] = cnt
        total += cnt

    return total, by


def get_chunks_for_rag(ticker: str, categoria: Optional[str], top_k: int):
    sb = _get_supabase_client()
    q = sb.table("docs_corporativos_chunks").select(
        "id,doc_id,ticker,categoria,chunk_text"
    ).eq("ticker", ticker)

    if categoria:
        q = q.eq("categoria", categoria)

    res = q.order("id", desc=True).limit(int(top_k)).execute()
    return getattr(res, "data", []) or []


# -------------------------------
# Ingest
# -------------------------------
def _try_find_ingest_runner():
    candidates = [
        ("pickup.ingest_docs_fallback", ["ingest_strategy_for_tickers"]),
        ("core.ingest_docs_fallback", ["ingest_strategy_for_tickers"]),
        ("pickup.ingest_docs_cvm_ipe", ["ingest_ipe_for_tickers"]),
    ]

    for mod_name, fn_names in candidates:
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            continue

        for fn in fn_names:
            f = getattr(mod, fn, None)
            if callable(f):
                return f

    return None


# -------------------------------
# LLM Runner
# -------------------------------
def _try_find_llm_runner():
    fn_candidates = {
        "run_patch6_llm",
        "run_llm",
        "run_rag",
        "judge_company",
    }

    try:
        pkg = importlib.import_module("core.ai_models.pipelines")
    except Exception:
        return None

    for mi in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + "."):
        try:
            m = importlib.import_module(mi.name)
        except Exception:
            continue

        for fn in fn_candidates:
            f = getattr(m, fn, None)
            if callable(f):
                return f

    return None


def _run_llm(ticker: str, categoria: Optional[str], top_k: int):
    chunks = get_chunks_for_rag(ticker, categoria, top_k)
    if not chunks:
        return {"ok": False, "error": "Sem chunks para RAG."}

    context = "\n\n---\n\n".join(
        [(c.get("chunk_text") or "")[:2000] for c in chunks]
    )

    runner = _try_find_llm_runner()
    if not runner:
        return {"ok": False, "error": "Runner LLM não encontrado."}

    return _safe_call(runner, ticker=ticker, context=context, chunks=chunks)


# -------------------------------
# UI
# -------------------------------
def render():
    st.title("🧪 Patch 6 — Teste (Ingest + LLM)")

    tickers_raw = st.text_input(
        "Tickers (vírgula)",
        value="BBAS3, ABEV3",
    )

    tickers = _parse_tickers(tickers_raw)

    st.markdown("### A) Ingest")
    ingest_runner = _try_find_ingest_runner()

    if ingest_runner:
        if st.button("Rodar ingest"):
            out = _safe_call(
                ingest_runner,
                tickers=tickers,
                anos=2,
                max_docs_por_ticker=60,
            )
            st.json(out)
    else:
        st.warning("Runner de ingest não encontrado.")

    st.markdown("### B) Contagem Docs")
    if st.button("Contar docs"):
        total, by = count_docs_by_tickers(tickers)
        st.success(f"Total: {total}")
        st.json(by)

    st.markdown("### C) LLM (RAG)")
    ticker_llm = st.selectbox("Ticker", tickers)

    if st.button("Rodar LLM"):
        out = _run_llm(ticker_llm, "estrategico", 25)
        st.json(out)


if __name__ == "__main__":
    render()
