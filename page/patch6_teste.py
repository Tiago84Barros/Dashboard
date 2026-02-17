# dashboard/page/patch6_teste.py
# Patch 6 — Teste (Ingest + Chunking + LLM) — CAMINHO 1 (SEM categoria)
from __future__ import annotations

import importlib
import inspect
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import text


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _parse_tickers(raw: str) -> List[str]:
    if not raw:
        return []
    out: List[str] = []
    for p in raw.replace(";", ",").split(","):
        t = (p or "").strip().upper()
        if t:
            out.append(t)
    seen = set()
    uniq: List[str] = []
    for t in out:
        if t not in seen:
            uniq.append(t)
            seen.add(t)
    return uniq


def _safe_call(fn: Callable[..., Any], **kwargs) -> Any:
    """
    Chama fn apenas com kwargs compatíveis com a assinatura.
    Evita quebrar quando nomes de parâmetros mudam (anos/years/window_months etc).
    """
    try:
        sig = inspect.signature(fn)
    except Exception:
        return fn(**kwargs)

    accepted: Dict[str, Any] = {}
    for k, v in kwargs.items():
        if k in sig.parameters:
            accepted[k] = v

    # aliases
    if "anos" in kwargs and "anos" not in accepted:
        if "years" in sig.parameters:
            accepted["years"] = kwargs["anos"]
        elif "window_years" in sig.parameters:
            accepted["window_years"] = kwargs["anos"]
        elif "window_months" in sig.parameters:
            accepted["window_months"] = int(kwargs["anos"]) * 12

    if "window_months" in kwargs and "window_months" not in accepted:
        if "months" in sig.parameters:
            accepted["months"] = kwargs["window_months"]

    if "max_docs_por_ticker" in kwargs and "max_docs_por_ticker" not in accepted:
        if "max_docs" in sig.parameters:
            accepted["max_docs"] = kwargs["max_docs_por_ticker"]
        elif "limit_per_ticker" in sig.parameters:
            accepted["limit_per_ticker"] = kwargs["max_docs_por_ticker"]
        elif "max_docs_per_ticker" in sig.parameters:
            accepted["max_docs_per_ticker"] = kwargs["max_docs_por_ticker"]

    if "tickers" in kwargs and "tickers" not in accepted:
        if "symbols" in sig.parameters:
            accepted["symbols"] = kwargs["tickers"]

    return fn(**accepted)


def _norm_tk(t: str) -> str:
    return (t or "").strip().upper().replace(".SA", "").strip()


# ---------------------------------------------------------------------
# Supabase via SQLAlchemy (core.db_loader.get_supabase_engine)
# ---------------------------------------------------------------------
def _get_engine():
    from core.db_loader import get_supabase_engine
    return get_supabase_engine()


def _read_sql_df(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    eng = _get_engine()
    with eng.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


def count_docs_by_tickers(tickers: List[str]) -> Tuple[int, Dict[str, int]]:
    tks = [_norm_tk(t) for t in (tickers or []) if str(t).strip()]
    if not tks:
        return 0, {}
    df = _read_sql_df(
        """
        select ticker, count(*)::int as cnt
        from public.docs_corporativos
        where ticker = any(:tks)
        group by ticker
        """,
        {"tks": tks},
    )
    by = {t: 0 for t in tks}
    for _, r in df.iterrows():
        by[str(r["ticker"])] = int(r["cnt"])
    return int(sum(by.values())), by


def count_chunks_by_tickers(tickers: List[str]) -> Tuple[int, Dict[str, int]]:
    tks = [_norm_tk(t) for t in (tickers or []) if str(t).strip()]
    if not tks:
        return 0, {}
    df = _read_sql_df(
        """
        select ticker, count(*)::int as cnt
        from public.docs_corporativos_chunks
        where ticker = any(:tks)
        group by ticker
        """,
        {"tks": tks},
    )
    by = {t: 0 for t in tks}
    for _, r in df.iterrows():
        by[str(r["ticker"])] = int(r["cnt"])
    return int(sum(by.values())), by


def get_recent_docs(ticker: str, limit: int = 20) -> pd.DataFrame:
    tk = _norm_tk(ticker)
    if not tk:
        return pd.DataFrame()
    return _read_sql_df(
        """
        select id, ticker, data, fonte, tipo, titulo, url, created_at,
               (case when coalesce(texto,'')<>'' then length(texto) else 0 end) as texto_len,
               (case when coalesce(raw_text,'')<>'' then length(raw_text) else 0 end) as raw_text_len
        from public.docs_corporativos
        where ticker = :tk
        order by id desc
        limit :lim
        """,
        {"tk": tk, "lim": int(limit)},
    )


def get_chunks_for_rag(ticker: str, top_k: int) -> List[Dict[str, Any]]:
    tk = _norm_tk(ticker)
    if not tk:
        return []
    df = _read_sql_df(
        """
        select id, doc_id, ticker, chunk_index, chunk_text, created_at
        from public.docs_corporativos_chunks
        where ticker = :tk
        order by id desc
        limit :lim
        """,
        {"tk": tk, "lim": int(top_k)},
    )
    return [] if df is None or df.empty else df.to_dict(orient="records")


# ---------------------------------------------------------------------
# Ingest runner
# ---------------------------------------------------------------------
def _try_find_ingest_runner() -> Optional[Callable[..., Any]]:
    candidates = [
        ("pickup.ingest_docs_cvm_ipe", ["ingest_ipe_for_tickers"]),
        ("core.ingest_docs_cvm_ipe", ["ingest_ipe_for_tickers"]),
        ("pickup.ingest_docs_fallback", ["ingest_strategy_for_tickers", "ingest_docs_for_tickers"]),
        ("core.ingest_docs_fallback", ["ingest_strategy_for_tickers", "ingest_docs_for_tickers"]),
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


# ---------------------------------------------------------------------
# Chunker (unificado ao ingest)
# ---------------------------------------------------------------------
def _import_chunker_or_error() -> Tuple[Optional[Callable[..., Any]], Optional[str]]:
    """
    Retorna (callable, error_message). Importa core.patch6_store e procura um chunker.
    Se falhar, devolve o erro real (não engole).
    """
    try:
        mod = importlib.import_module("core.patch6_store")
    except Exception as e:
        return None, f"core.patch6_store: {type(e).__name__}: {e}"

    # preferência: process_missing_chunks_for_ticker (mais seguro)
    for fn in ("process_missing_chunks_for_ticker", "process_document_chunks"):
        f = getattr(mod, fn, None)
        if callable(f):
            return f, None

    return None, "core.patch6_store importado, mas nenhuma função de chunking conhecida foi encontrada."


# ---------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------
def _build_prompt(ticker: str, context: str, manual_text: str) -> str:
    manual_block = f"\n\n[TEXTO MANUAL]\n{manual_text.strip()}\n" if manual_text and manual_text.strip() else ""
    return f"""
Você é um analista fundamentalista focado em direcionalidade estratégica (capex, expansão, guidance, investimentos futuros,
desalavancagem, alocação de capital e prioridades do management).

Seu trabalho é julgar a empresa **{ticker}** com base nos documentos coletados (CVM/IPE) e no texto manual (se houver).

ENTREGA (responda em JSON):
{{
  "ticker": "{ticker}",
  "perspectiva_compra": "forte|moderada|fraca",
  "resumo": "2-4 frases, direto",
  "pontos_chave": ["...","...","..."],
  "riscos_ou_alertas": ["...","..."],
  "sinais_de_investimento_futuro": ["capex","expansão","projetos","guidance","M&A","desalavancagem", "..."],
  "porque": "1 parágrafo objetivo (por que forte/moderada/fraca)",
  "evidencias": [
    {{"fonte":"CVM/IPE","trecho":"<=240 chars","observacao":"por que isso importa"}}
  ]
}}

REGRAS:
- Não invente números. Se não houver, diga explicitamente "não informado".
- Foque em intenção estratégica e direcionamento, não em DFP/ITR.
- Evidências devem vir do contexto fornecido (RAG + texto manual).

[CONTEXTO - RAG]
{context}
{manual_block}
""".strip()


def _run_llm_direct(ticker: str, top_k: int, manual_text: str) -> Dict[str, Any]:
    chunks = get_chunks_for_rag(ticker=ticker, top_k=int(top_k))
    if not chunks:
        return {"ok": False, "error": f"Sem chunks no Supabase para {ticker}. Rode o ingest+chunking antes."}

    parts: List[str] = []
    for c in chunks[::-1]:
        txt = str(c.get("chunk_text", "") or "").strip()
        if txt:
            parts.append(txt[:1800])
    context = "\n\n---\n\n".join(parts)

    prompt = _build_prompt(ticker=ticker, context=context, manual_text=manual_text)

    schema_hint = r"""
{
  "ticker": "STRING",
  "perspectiva_compra": "forte|moderada|fraca",
  "resumo": "STRING",
  "pontos_chave": ["STRING"],
  "riscos_ou_alertas": ["STRING"],
  "sinais_de_investimento_futuro": ["STRING"],
  "porque": "STRING",
  "evidencias": [{"fonte":"STRING","trecho":"STRING","observacao":"STRING"}]
}
""".strip()

    from core.ai_models.llm_client.factory import get_llm_client
    llm = get_llm_client()

    system = """
Você é um analista buy-side, cético e orientado a evidência.
- NÃO invente fatos, números, datas.
- Use APENAS o contexto fornecido.
- Responda OBRIGATORIAMENTE em JSON válido.
""".strip()

    out = llm.generate_json(system=system, user=prompt, schema_hint=schema_hint, context=None)
    return {"ok": True, "result": out, "meta": {"top_k": int(top_k), "chunks_used": len(chunks)}}


# ---------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------
def render() -> None:
    st.title("🧪 Patch 6 — Teste (Ingest + Chunking + LLM) — CVM/IPE")

    default_tickers = "BRAP3"

    colA, colB, colC = st.columns([2, 1, 1], gap="large")
    with colA:
        tickers_raw = st.text_input("Tickers (separados por vírgula)", value=default_tickers)
    with colB:
        window_months = st.number_input("Janela (meses)", min_value=1, max_value=24, value=12, step=1)
    with colC:
        max_docs_ingest = st.number_input("Máx docs por ticker (ingest)", min_value=5, max_value=200, value=60, step=5)

    tickers = _parse_tickers(tickers_raw)
    if not tickers:
        st.warning("Informe ao menos 1 ticker.")
        st.stop()

    col1, col2, col3 = st.columns([1, 1, 1], gap="large")
    with col1:
        strategic_only = st.toggle("Somente estratégicos (heurística)", value=True)
    with col2:
        download_pdfs = st.toggle("Baixar PDFs e extrair texto (sem OCR)", value=True)
    with col3:
        max_pdfs = st.number_input("Máx PDFs por ticker", min_value=0, max_value=50, value=12, step=1)

    st.markdown("### A) 📥 Ingest + 🧩 Chunking (CVM/IPE)")
    col4, col5 = st.columns([1, 1], gap="large")
    with col4:
        # aqui o limite precisa ser maior para incluir chunking + inserts
        max_runtime_s = st.number_input("Limite total de tempo (s)", min_value=10, max_value=300, value=90, step=10)
    with col5:
        only_with_text = st.toggle("Chunk apenas com texto", value=True)

    ingest_runner = _try_find_ingest_runner()
    if ingest_runner is None:
        st.error("Não encontrei pickup.ingest_docs_cvm_ipe.ingest_ipe_for_tickers (ou core.*).")
        st.stop()

    if st.button("⬇️ Rodar ingest + chunking agora", use_container_width=True):
        t0 = time.monotonic()
        out: Dict[str, Any] = {"ok": False}

        with st.spinner("Ingerindo documentos CVM/IPE..."):
            ingest_out = _safe_call(
                ingest_runner,
                tickers=tickers,
                window_months=int(window_months),
                max_docs_per_ticker=int(max_docs_ingest),
                strategic_only=bool(strategic_only),
                download_pdfs=bool(download_pdfs),
                max_pdfs_per_ticker=int(max_pdfs),
                max_runtime_s=float(max_runtime_s),
                verbose=False,
            )

        out["ingest"] = ingest_out

        # chunking imediatamente após ingest (unificado)
        chunk_fn, chunk_err = _import_chunker_or_error()
        if chunk_fn is None:
            out["chunking"] = {"ok": False, "error": chunk_err or "Chunker indisponível."}
            st.error(out["chunking"]["error"])
        else:
            with st.spinner("Gerando chunks para docs ainda sem chunks..."):
                res_all = {}
                # budget de tempo residual (se o chunker suportar)
                elapsed = time.monotonic() - t0
                remaining = max(0.0, float(max_runtime_s) - elapsed)

                for tk in tickers:
                    # preferimos process_missing_chunks_for_ticker(ticker, limit_docs=..., only_with_text=...)
                    try:
                        res_all[tk] = _safe_call(
                            chunk_fn,
                            ticker=_norm_tk(tk),
                            tk=_norm_tk(tk),
                            symbol=_norm_tk(tk),
                            limit_docs=50,
                            only_with_text=bool(only_with_text),
                            max_runtime_s=float(remaining) if remaining > 0 else None,
                        )
                    except Exception as e:
                        res_all[tk] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
                out["chunking"] = {"ok": True, "by_ticker": res_all}

        # contagens finais
        total_docs, by_docs = count_docs_by_tickers(tickers)
        total_chunks, by_chunks = count_chunks_by_tickers(tickers)
        out["counts"] = {"docs_total": total_docs, "chunks_total": total_chunks, "docs_by": by_docs, "chunks_by": by_chunks}
        out["ok"] = True

        st.subheader("Resultado (Ingest + Chunking)")
        st.json(out)
        st.info(f"Após ingest+chunking → docs: {total_docs} | chunks: {total_chunks}")

    st.divider()
    st.markdown("### B) 📚 Inspeção no Supabase")
    cA, cB = st.columns([1, 1], gap="large")
    with cA:
        if st.button("Contar docs", use_container_width=True):
            total_docs, by_docs = count_docs_by_tickers(tickers)
            st.success(f"Docs: {total_docs}")
            st.json(by_docs)
    with cB:
        if st.button("Contar chunks", use_container_width=True):
            total_chunks, by_chunks = count_chunks_by_tickers(tickers)
            st.success(f"Chunks: {total_chunks}")
            st.json(by_chunks)

    with st.expander("Ver docs recentes (amostra)", expanded=False):
        tk_sel = st.selectbox("Ticker", options=tickers, index=0)
        lim = st.number_input("Limite", min_value=5, max_value=100, value=20, step=5)
        df_docs = get_recent_docs(tk_sel, limit=int(lim))
        st.dataframe(df_docs, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### C) 🧠 Teste da LLM (RAG + texto manual opcional)")
    ticker_llm = st.selectbox("Ticker (LLM)", options=tickers, index=0)
    top_k = st.number_input("Top-K chunks", min_value=5, max_value=120, value=25, step=5)
    manual_text = st.text_area("Texto manual (opcional)", value="", height=160)

    if st.button("🚀 Rodar LLM agora", use_container_width=True):
        with st.spinner("Rodando LLM com RAG..."):
            out = _run_llm_direct(_norm_tk(ticker_llm), int(top_k), manual_text)
        if out.get("ok"):
            st.success("LLM executada.")
            st.json(out)
        else:
            st.error(out.get("error") or "Falha ao rodar LLM.")


if __name__ == "__main__":
    render()
