# dashboard/page/patch6_teste.py
# Patch 6 — Teste (Ingest + LLM) — CAMINHO 1 (SEM categoria)
#
# Ajuste principal desta versão:
# - Quando "Somente CVM (Plano A)" estiver ligado, chamamos DIRETO o runner do IPE (ingest_ipe_for_tickers)
#   e passamos apenas months_back / limites de runtime (evita erro de assinatura com years).
# - Mantém UI configurável (months_back, max_docs, max_runtime_s).
# - Mantém LLM/RAG (caminho 1) buscando chunks por ticker.

from __future__ import annotations

import importlib
import inspect
import json
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
    # remove duplicados preservando ordem
    seen = set()
    uniq = []
    for t in out:
        if t not in seen:
            uniq.append(t)
            seen.add(t)
    return uniq


def _safe_call(fn: Callable[..., Any], **kwargs) -> Any:
    """
    Chama fn apenas com kwargs compatíveis com a assinatura.
    Evita quebrar quando nomes de parâmetros mudam.
    """
    try:
        sig = inspect.signature(fn)
    except Exception:
        return fn(**kwargs)

    accepted: Dict[str, Any] = {}
    for k, v in kwargs.items():
        if k in sig.parameters:
            accepted[k] = v

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


def get_recent_docs(ticker: str, limit: int = 20) -> pd.DataFrame:
    tk = _norm_tk(ticker)
    if not tk:
        return pd.DataFrame()

    return _read_sql_df(
        """
        select id, ticker, data, fonte, tipo, titulo, url, created_at
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
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


# ---------------------------------------------------------------------
# Ingest runners
# ---------------------------------------------------------------------
def _import_first(mod_names: List[str], fn_name: str) -> Optional[Callable[..., Any]]:
    for mod_name in mod_names:
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            continue
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            return fn
    return None


def _get_runner_cvm_ipe() -> Optional[Callable[..., Any]]:
    # prefer pickup, then core
    return _import_first(
        ["pickup.ingest_docs_cvm_ipe", "core.ingest_docs_cvm_ipe"],
        "ingest_ipe_for_tickers",
    )


def _get_runner_strategy() -> Optional[Callable[..., Any]]:
    return _import_first(
        ["pickup.ingest_docs_fallback", "core.ingest_docs_fallback"],
        "ingest_strategy_for_tickers",
    )


# ---------------------------------------------------------------------
# LLM (direto no seu client core.ai_models.llm_client)
# ---------------------------------------------------------------------
def _build_prompt(ticker: str, context: str, manual_text: str) -> str:
    manual_block = ""
    if manual_text and manual_text.strip():
        manual_block = f"\n\n[TEXTO MANUAL]\n{manual_text.strip()}\n"

    return f"""
Você é um analista fundamentalista focado em direcionalidade estratégica (capex, expansão, guidance, investimentos futuros,
desalavancagem, alocação de capital e prioridades do management).

Seu trabalho é julgar a empresa **{ticker}** com base nos documentos coletados (principalmente RI/CVM/release) e no texto manual (se houver).

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
    {{"fonte":"RI/CVM/outro","trecho":"<=240 chars","observacao":"por que isso importa"}}
  ]
}}

REGRAS:
- Não invente números. Se não houver, diga explicitamente "não informado".
- Foque em intenção estratégica e direcionamento do lucro/dívida/patrimônio, não em DFP/ITR.
- Evidências devem vir do contexto fornecido (RAG + texto manual).

[CONTEXTO - RAG]
{context}
{manual_block}
""".strip()


def _run_llm_direct(ticker: str, top_k: int, manual_text: str) -> Dict[str, Any]:
    chunks = get_chunks_for_rag(ticker=ticker, top_k=int(top_k))
    if not chunks:
        return {"ok": False, "error": f"Sem chunks no Supabase para {ticker}. Rode o ingest/chunking antes."}

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
  "evidencias": [
    {"fonte":"STRING","trecho":"STRING","observacao":"STRING"}
  ]
}
""".strip()

    try:
        from core.ai_models.llm_client.factory import get_llm_client
        llm = get_llm_client()
    except Exception as e:
        return {"ok": False, "error": f"Falha ao inicializar LLM client: {type(e).__name__}: {e}"}

    system = """
Você é um analista buy-side, cético e orientado a evidência.
Regras:
- NÃO invente fatos, números, datas.
- Use APENAS o contexto fornecido.
- Se a evidência for fraca, classifique como moderada/fraca e explique.
Responda OBRIGATORIAMENTE em JSON válido.
""".strip()

    try:
        out = llm.generate_json(system=system, user=prompt, schema_hint=schema_hint, context=None)
        return {"ok": True, "result": out, "meta": {"top_k": int(top_k), "chunks_used": len(chunks)}}
    except Exception as e:
        return {"ok": False, "error": f"Erro ao rodar LLM: {type(e).__name__}: {e}", "debug": {"prompt_preview": prompt[:1400]}}


# ---------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------
def render() -> None:
    st.title("🧪 Patch 6 — Teste (Ingest + LLM) — Caminho 1 (sem categoria)")
    st.caption("Objetivo: capturar documentos estratégicos (CVM/IPE) → Supabase → RAG → LLM.")

    colA, colB, colC = st.columns([2, 1, 1], gap="large")
    with colA:
        tickers_raw = st.text_input("Tickers (separados por vírgula)", value="BBAS3")
    with colB:
        months_back = st.number_input("Meses (janela CVM/IPE)", min_value=1, max_value=36, value=12, step=1)
    with colC:
        max_docs_ingest = st.number_input("Máx docs por ticker (ingest)", min_value=5, max_value=300, value=60, step=5)

    tickers = _parse_tickers(tickers_raw)
    if not tickers:
        st.warning("Informe ao menos 1 ticker.")
        st.stop()

    col4, col5 = st.columns([1, 1], gap="large")
    with col4:
        cvm_only = st.toggle("Somente CVM (Plano A)", value=True)
    with col5:
        max_runtime_s = st.number_input("Limite total de tempo (s)", min_value=5, max_value=180, value=25, step=5)

    st.markdown("### A) 📥 Ingest (capturar/atualizar docs no Supabase)")
    st.caption("CVM/IPE (Plano A). Se você desligar CVM-only, usa fallback A->B (RI) se existir.")

    if st.button("⬇️ Rodar ingest agora", use_container_width=True):
        with st.spinner("Ingerindo documentos..."):
            if cvm_only:
                runner = _get_runner_cvm_ipe()
                if runner is None:
                    st.error("Não encontrei ingest_ipe_for_tickers em pickup/core.")
                    st.stop()
                out = _safe_call(
                    runner,
                    tickers=tickers,
                    months_back=int(months_back),
                    max_docs_por_ticker=int(max_docs_ingest),
                    max_runtime_s=float(max_runtime_s),
                    sleep_s=0.0,
                )
            else:
                runner = _get_runner_strategy()
                if runner is None:
                    st.error("Não encontrei ingest_strategy_for_tickers em pickup/core.")
                    st.stop()
                # fallback ainda usa anos internamente; convertemos meses -> anos mínimo 1
                anos = max(1, int((int(months_back) + 11) // 12))
                out = _safe_call(
                    runner,
                    tickers=tickers,
                    anos=int(anos),
                    max_docs_por_ticker=int(max_docs_ingest),
                    sleep_s=0.0,
                    strategy="A->B",
                )

        st.subheader("Resultado do ingest")
        st.json(out)

        # Reconta automaticamente
        try:
            total2, by2 = count_docs_by_tickers(tickers)
            st.session_state["patch6_docs_total"] = total2
            st.session_state["patch6_docs_by"] = by2
            st.info(f"Após ingestão → total docs: {total2}")
        except Exception as e:
            st.warning(f"Não consegui recontar docs após ingestão: {e}")

    st.divider()

    st.markdown("### B) 📚 Documentos (carregar do Supabase)")
    if st.button("Contar docs no Supabase", use_container_width=True):
        total, by = count_docs_by_tickers(tickers)
        st.session_state["patch6_docs_total"] = total
        st.session_state["patch6_docs_by"] = by

    total = st.session_state.get("patch6_docs_total", None)
    by = st.session_state.get("patch6_docs_by", None)
    if isinstance(total, int) and isinstance(by, dict):
        st.success(f"Docs no Supabase: {total}")
        with st.expander("Ver contagem por ticker", expanded=True):
            for tk in tickers:
                st.write(f"**{tk}**: {int(by.get(_norm_tk(tk), 0))} docs")

    with st.expander("Ver docs recentes (amostra)", expanded=False):
        tk_sel = st.selectbox("Ticker para ver docs recentes", options=tickers, index=0, key="patch6_recent_ticker")
        lim = st.number_input("Limite de docs (preview)", min_value=5, max_value=100, value=20, step=5)
        df_docs = get_recent_docs(tk_sel, limit=int(lim))
        if df_docs.empty:
            st.info("Sem docs para este ticker.")
        else:
            st.dataframe(df_docs, use_container_width=True, hide_index=True)

    st.divider()

    st.markdown("### C) 🧠 Teste da LLM (RAG + texto manual opcional)")
    c1, c2 = st.columns([1, 1], gap="large")
    with c1:
        ticker_llm = st.selectbox("Ticker (LLM)", options=tickers, index=0, key="patch6_llm_ticker")
    with c2:
        top_k = st.number_input("Top-K chunks", min_value=5, max_value=120, value=25, step=5)

    manual_text = st.text_area(
        "Texto manual (opcional) — cole trechos de call/release/RI/CVM",
        value="",
        height=160,
        placeholder="Cole aqui trechos relevantes (guidance, capex, projetos, expansão, desalavancagem...)",
    )

    if st.button("🚀 Rodar LLM agora", use_container_width=True):
        with st.spinner("Rodando LLM com RAG..."):
            out = _run_llm_direct(ticker=_norm_tk(ticker_llm), top_k=int(top_k), manual_text=manual_text)

        if out.get("ok") is True:
            st.success("LLM executada.")
            st.json(out)
        else:
            st.error(out.get("error") or "Falha ao rodar LLM.")
            if out.get("debug"):
                st.code(json.dumps(out["debug"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    render()
