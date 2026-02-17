# dashboard/page/patch6_teste.py
# Patch 6 — Teste (Ingest + LLM) — CAMINHO 1 (SEM categoria)
#
# Versão estável:
# - Usa months_back (janela em meses)
# - Limite de tempo configurável pela UI
# - Compatível com runners antigos (anos/years convertidos automaticamente)

from __future__ import annotations

import importlib
import inspect
import json
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import text


# ---------------- Helpers ----------------

def _parse_tickers(raw: str) -> List[str]:
    if not raw:
        return []
    out = []
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

    # Conversão anos/years -> months_back
    if "months_back" in sig.parameters and "months_back" not in accepted:
        if "anos" in kwargs:
            accepted["months_back"] = int(kwargs["anos"]) * 12
        elif "years" in kwargs:
            accepted["months_back"] = int(kwargs["years"]) * 12

    return fn(**accepted)


def _norm_tk(t: str) -> str:
    return (t or "").strip().upper().replace(".SA", "").strip()


# ---------------- DB ----------------

def _get_engine():
    from core.db_loader import get_supabase_engine
    return get_supabase_engine()


def _read_sql_df(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    eng = _get_engine()
    with eng.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


def count_docs_by_tickers(tickers: List[str]) -> Tuple[int, Dict[str, int]]:
    tks = [_norm_tk(t) for t in tickers]
    if not tks:
        return 0, {}
    df = _read_sql_df(
        "select ticker, count(*)::int as cnt from public.docs_corporativos where ticker = any(:tks) group by ticker",
        {"tks": tks},
    )
    by = {t: 0 for t in tks}
    for _, r in df.iterrows():
        by[str(r["ticker"])] = int(r["cnt"])
    return sum(by.values()), by


# ---------------- UI ----------------

def render() -> None:
    st.title("🧪 Patch 6 — Teste (Ingest + LLM)")

    colA, colB, colC = st.columns([2, 1, 1])
    with colA:
        tickers_raw = st.text_input("Tickers", value="BBAS3")
    with colB:
        months_back = st.number_input("Meses (janela)", min_value=1, max_value=36, value=12)
    with colC:
        max_docs = st.number_input("Máx docs por ticker", min_value=5, max_value=300, value=60)

    tickers = _parse_tickers(tickers_raw)

    cvm_only = st.toggle("Somente CVM (Plano A)", value=True)
    max_runtime_s = st.number_input("Limite total de tempo (s)", min_value=5, max_value=180, value=25)

    if st.button("⬇️ Rodar ingest"):
        ingest_runner = None
        for mod_name in [
            "pickup.ingest_docs_fallback",
            "pickup.ingest_docs_cvm_ipe",
            "core.ingest_docs_cvm_ipe",
        ]:
            try:
                mod = importlib.import_module(mod_name)
                ingest_runner = getattr(mod, "ingest_ipe_for_tickers", None) or getattr(mod, "ingest_strategy_for_tickers", None)
                if callable(ingest_runner):
                    break
            except Exception:
                continue

        if ingest_runner is None:
            st.error("Runner de ingest não encontrado.")
        else:
            with st.spinner("Executando ingest..."):
                out = _safe_call(
                    ingest_runner,
                    tickers=tickers,
                    months_back=int(months_back),
                    anos=max(1, int(months_back // 12)),
                    max_docs_por_ticker=int(max_docs),
                    strategy=("A" if cvm_only else "A->B"),
                    max_runtime_s=float(max_runtime_s),
                )
            st.json(out)

    st.divider()

    if st.button("Contar docs"):
        total, by = count_docs_by_tickers(tickers)
        st.success(f"Total docs: {total}")
        st.json(by)


if __name__ == "__main__":
    render()
