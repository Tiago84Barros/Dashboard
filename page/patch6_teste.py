# dashboard/page/patch6_teste.py
# Patch 6 — Teste (Ingest + LLM)
#
# Objetivo:
#  - Permitir testar a ingestão de documentos "estratégicos" (CVM/RI/Fontes seguras) para 1+ tickers
#  - Validar que os docs ficam no Supabase (docs_corporativos / docs_corporativos_chunks)
#  - Rodar um teste de LLM (RAG + texto manual opcional) para gerar "perspectiva de compra" (forte/fraca) + motivos
#
# Observação: este arquivo foi escrito para ser resiliente a mudanças do projeto.
# Ele tenta localizar automaticamente funções/rotas existentes (ingest e LLM) via imports e introspecção.

from __future__ import annotations

import importlib
import inspect
import json
import pkgutil
from typing import Any, Callable, Dict, List, Optional, Tuple

import streamlit as st

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
    Evita quebrar quando nomes de parâmetros mudam (ex: anos vs years).
    """
    sig = None
    try:
        sig = inspect.signature(fn)
    except Exception:
        # se não der para inspecionar, tenta chamar direto
        return fn(**kwargs)

    accepted = {}
    for k, v in kwargs.items():
        if k in sig.parameters:
            accepted[k] = v

    # alguns aliases comuns
    if "anos" not in accepted and "anos" in kwargs:
        if sig and "years" in sig.parameters:
            accepted["years"] = kwargs["anos"]
        elif sig and "window_years" in sig.parameters:
            accepted["window_years"] = kwargs["anos"]

    if "max_docs_por_ticker" not in accepted and "max_docs_por_ticker" in kwargs:
        if sig and "max_docs" in sig.parameters:
            accepted["max_docs"] = kwargs["max_docs_por_ticker"]
        elif sig and "limit_per_ticker" in sig.parameters:
            accepted["limit_per_ticker"] = kwargs["max_docs_por_ticker"]

    if "tickers" not in accepted and "tickers" in kwargs:
        if sig and "symbols" in sig.parameters:
            accepted["symbols"] = kwargs["tickers"]

    return fn(**accepted)


# ---------------------------------------------------------------------
# Supabase reads (tolerante a diferentes implementações)
# ---------------------------------------------------------------------
def _get_supabase_client() -> Any:
    """
    Tenta obter client Supabase do projeto (múltiplos nomes comuns).
    Retorna o client ou levanta Exception.
    """
    candidates = [
        ("core.db_loader", ["get_supabase", "get_supabase_client", "supabase_client"]),
        ("core.supabase_client", ["get_client", "get_supabase", "supabase"]),
        ("pickup.db_loader", ["get_supabase", "get_supabase_client", "supabase_client"]),
    ]
    last_err = None
    for mod_name, attrs in candidates:
        try:
            mod = importlib.import_module(mod_name)
        except Exception as e:
            last_err = e
            continue

        for a in attrs:
            obj = getattr(mod, a, None)
            if obj is None:
                continue
            try:
                if callable(obj):
                    return obj()
                return obj
            except Exception as e:
                last_err = e
                continue

    raise RuntimeError(f"Não consegui obter supabase client (último erro: {last_err})")


def count_docs_by_tickers(tickers: List[str]) -> Tuple[int, Dict[str, int]]:
    sb = _get_supabase_client()
    # tabela alvo
    table = "docs_corporativos"
    total = 0
    by: Dict[str, int] = {}
    for tk in tickers:
        q = sb.table(table).select("id", count="exact").eq("ticker", tk)
        res = q.execute()
        cnt = int(getattr(res, "count", None) or 0)
        by[tk] = cnt
        total += cnt
    return total, by


def get_recent_docs(ticker: str, limit: int = 20, categoria: Optional[str] = None) -> List[Dict[str, Any]]:
    sb = _get_supabase_client()
    q = sb.table("docs_corporativos").select("id,ticker,titulo,fonte,tipo,categoria,created_at").eq("ticker", ticker)
    if categoria:
        q = q.eq("categoria", categoria)
    q = q.order("id", desc=True).limit(int(limit))
    res = q.execute()
    data = getattr(res, "data", None) or []
    return list(data)


def get_chunks_for_rag(
    ticker: str,
    categoria: Optional[str],
    top_k: int,
) -> List[Dict[str, Any]]:
    sb = _get_supabase_client()
    q = sb.table("docs_corporativos_chunks").select("id,doc_id,ticker,categoria,chunk_text").eq("ticker", ticker)
    if categoria:
        q = q.eq("categoria", categoria)
    # chunk_id DESC costuma pegar os mais recentes; para RAG simples serve
    q = q.order("id", desc=True).limit(int(top_k))
    res = q.execute()
    data = getattr(res, "data", None) or []
    return list(data)


# ---------------------------------------------------------------------
# Ingest (A/B/C)
# ---------------------------------------------------------------------
def _try_find_ingest_runner() -> Optional[Callable[..., Any]]:
    """
    Procura função de ingestão com fallback (A/B/C).
    Prioridade:
      1) pickup.ingest_docs_fallback.ingest_strategy_for_tickers
      2) pickup.ingest_docs_fallback.ingest_docs_for_tickers
      3) pickup.ingest_docs_cvm_ipe.ingest_ipe_for_tickers
      4) pickup.ingest_docs_enet.ingest_enet_for_tickers
    """
    candidates = [
        ("pickup.ingest_docs_fallback", ["ingest_strategy_for_tickers", "ingest_docs_for_tickers"]),
        ("core.ingest_docs_fallback", ["ingest_strategy_for_tickers", "ingest_docs_for_tickers"]),
        ("pickup.ingest_docs_cvm_ipe", ["ingest_ipe_for_tickers"]),
        ("pickup.ingest_docs_enet", ["ingest_enet_for_tickers"]),
        ("core.ingest_docs_cvm_ipe", ["ingest_ipe_for_tickers"]),
        ("core.ingest_docs_enet", ["ingest_enet_for_tickers"]),
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
# LLM (RAG)
# ---------------------------------------------------------------------
def _try_find_llm_runner() -> Optional[Callable[..., Any]]:
    """
    Varre core.ai_models.pipelines procurando uma função que pareça 'rodar LLM/RAG' no Patch6.
    Aceita diferentes nomes para manter compatibilidade.
    """
    # nomes de funções candidatas
    fn_candidates = {
        "run_llm",
        "run_rag",
        "run_patch6_llm",
        "patch6_llm",
        "judge_company",
        "judge_company_from_docs",
        "analyze_company_docs",
        "avaliar_empresa",
        "gerar_perspectiva_compra",
    }

    # 1) tenta imports diretos (rápido)
    direct = [
        ("core.ai_models.pipelines.patch6_llm", list(fn_candidates)),
        ("core.ai_models.pipelines.llm_patch6", list(fn_candidates)),
        ("core.ai_models.pipelines.rag_patch6", list(fn_candidates)),
        ("pickup.ai_models.pipelines.patch6_llm", list(fn_candidates)),
    ]
    for mod_name, fns in direct:
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            continue
        for fn in fns:
            f = getattr(mod, fn, None)
            if callable(f):
                return f

    # 2) varre o pacote pipelines
    try:
        pkg = importlib.import_module("core.ai_models.pipelines")
    except Exception:
        return None

    try:
        for mi in pkgutil.iter_modules(getattr(pkg, "__path__", []), pkg.__name__ + "."):
            try:
                m = importlib.import_module(mi.name)
            except Exception:
                continue
            for fn in fn_candidates:
                f = getattr(m, fn, None)
                if callable(f):
                    return f
    except Exception:
        pass

    return None


def _build_prompt(
    ticker: str,
    context: str,
    manual_text: str,
) -> str:
    manual_block = ""
    if manual_text and manual_text.strip():
        manual_block = f"\n\n[TEXTO MANUAL]\n{manual_text.strip()}\n"

    return f"""
Você é um analista fundamentalista focado em direcionalidade estratégica (capex, expansão, guidance, investimentos futuros, desalavancagem, alocação de capital e prioridades do management).
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
- Não invente números. Se não houver, fale explicitamente "não informado".
- Foque em intenção estratégica e direcionamento do lucro/dívida/patrimônio, não em DFP/ITR.
- Evidências devem vir do contexto fornecido.

[CONTEXTO - RAG]
{context}
{manual_block}
""".strip()


def _run_llm(
    ticker: str,
    categoria: Optional[str],
    top_k: int,
    manual_text: str,
) -> Dict[str, Any]:
    # monta contexto via chunks
    chunks = get_chunks_for_rag(ticker=ticker, categoria=categoria, top_k=top_k)
    if not chunks:
        return {
            "ok": False,
            "error": f"Sem chunks no Supabase para {ticker} (categoria={categoria}). Rode o ingest antes.",
        }

    # junta contexto
    context_parts = []
    for c in chunks[::-1]:  # do mais antigo -> mais novo dentro do top_k
        txt = (c.get("chunk_text") or "").strip()
        if not txt:
            continue
        context_parts.append(txt[:2000])
    context = "\n\n---\n\n".join(context_parts)

    prompt = _build_prompt(ticker=ticker, context=context, manual_text=manual_text)

    runner = _try_find_llm_runner()
    if runner is None:
        # fallback: sem runner encontrado
        return {
            "ok": False,
            "error": "Nenhum runner de LLM/RAG encontrado em core.ai_models.pipelines. "
                     "Verifique se os módulos existem em core/ai_models/pipelines.",
            "debug": {"prompt_preview": prompt[:1200]},
        }

    # tenta chamar runner de forma flexível
    # parâmetros comuns que podem existir:
    # - prompt / question
    # - ticker
    # - context
    # - chunks
    # - categoria
    kwargs = {
        "ticker": ticker,
        "categoria": categoria,
        "top_k": top_k,
        "chunks": chunks,
        "context": context,
        "prompt": prompt,
        "question": prompt,
        "manual_text": manual_text,
    }

    out = _safe_call(runner, **kwargs)

    # normaliza output
    if isinstance(out, str):
        # pode vir JSON string
        try:
            return {"ok": True, "result": json.loads(out)}
        except Exception:
            return {"ok": True, "result_raw": out}

    if isinstance(out, dict):
        return {"ok": True, **out}

    return {"ok": True, "result": out}


# ---------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------
def render() -> None:
    st.title("🧪 Patch 6 — Teste (Ingest + LLM)")

    st.caption(
        "Objetivo: capturar documentos estratégicos (CVM/RI/Fontes seguras) → Supabase → "
        "usar LLM (RAG) para gerar perspectiva de compra forte/fraca e motivos."
    )

    default_tickers = "BBAS3, ABEV3"

    colA, colB, colC = st.columns([2, 1, 1], gap="large")
    with colA:
        tickers_raw = st.text_input("Tickers (separados por vírgula)", value=default_tickers)
    with colB:
        anos = st.number_input("Anos (janela)", min_value=0, max_value=15, value=2, step=1)
    with colC:
        max_docs_ingest = st.number_input("Máx docs por ticker (ingest)", min_value=1, max_value=400, value=60, step=10)

    tickers = _parse_tickers(tickers_raw)

    col1, col2, col3 = st.columns([1, 1, 1], gap="large")
    with col1:
        max_pages_ri = st.number_input("Máx páginas RI (plano B)", min_value=1, max_value=200, value=30, step=5)
    with col2:
        allow_external = st.toggle("Permitir plano C (fontes seguras externas)", value=True)
    with col3:
        verbose_debug = st.toggle("Debug detalhado", value=True)

    if not tickers:
        st.warning("Informe ao menos 1 ticker.")
        st.stop()

    st.markdown("### A) 📥 Ingest (capturar/atualizar docs no Supabase)")
    st.caption("Prioridade A/B/C: CVM (se disponível) → RI → Fontes seguras (opcional).")

    ingest_runner = _try_find_ingest_runner()
    if ingest_runner is None:
        st.error(
            "Não encontrei um runner de ingest. "
            "Verifique se existe pickup/ingest_docs_fallback.py (ingest_strategy_for_tickers) "
            "ou pickup/ingest_docs_cvm_ipe.py / pickup/ingest_docs_enet.py."
        )
    else:
        if st.button("⬇️ Rodar ingest agora", use_container_width=True):
            with st.spinner("Ingerindo documentos..."):
                out = _safe_call(
                    ingest_runner,
                    tickers=tickers,
                    anos=int(anos),
                    max_docs_por_ticker=int(max_docs_ingest),
                    max_pages=int(max_pages_ri),
                    allow_external=bool(allow_external),
                    verbose=bool(verbose_debug),
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
    colx, coly = st.columns([1, 2], gap="large")
    with colx:
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
                st.write(f"**{tk}**: {int(by.get(tk, 0))} docs")

    # preview dos docs recentes
    with st.expander("Ver docs recentes (amostra)", expanded=False):
        tk_sel = st.selectbox("Ticker para ver docs recentes", options=tickers, index=0, key="patch6_recent_ticker")
        cat_sel = st.selectbox("Categoria", options=["(todas)", "estrategico", "institucional"], index=0, key="patch6_recent_cat")
        cat = None if cat_sel == "(todas)" else cat_sel
        docs = get_recent_docs(tk_sel, limit=20, categoria=cat)
        if not docs:
            st.info("Sem docs para este filtro.")
        else:
            st.dataframe(docs, use_container_width=True, hide_index=True)

    st.divider()

    st.markdown("### C) 🧠 Teste da LLM (RAG + texto manual opcional)")
    st.caption("Se o texto manual estiver vazio, a LLM usa somente os chunks do Supabase.")

    c1, c2, c3 = st.columns([1, 1, 1], gap="large")
    with c1:
        ticker_llm = st.selectbox("Ticker (LLM)", options=tickers, index=0, key="patch6_llm_ticker")
    with c2:
        categoria_llm = st.selectbox("Categoria (RAG)", options=["estrategico", "institucional", "(todas)"], index=0, key="patch6_llm_cat")
    with c3:
        top_k = st.number_input("Top-K chunks", min_value=5, max_value=120, value=25, step=5)

    manual_text = st.text_area(
        "Texto manual (opcional) — cole trechos de call/release/RI/CVM",
        value="",
        height=160,
        placeholder="Cole aqui trechos relevantes (guidance, capex, projetos, expansão, desalavancagem...)",
    )

    if st.button("🚀 Rodar LLM agora", use_container_width=True):
        with st.spinner("Rodando LLM com RAG..."):
            cat = None if categoria_llm == "(todas)" else categoria_llm
            out = _run_llm(
                ticker=ticker_llm,
                categoria=cat,
                top_k=int(top_k),
                manual_text=manual_text,
            )

        if out.get("ok") is True:
            st.success("LLM executada.")
            st.json(out)
        else:
            st.error(out.get("error") or "Falha ao rodar LLM.")
            if out.get("debug"):
                st.code(json.dumps(out["debug"], ensure_ascii=False, indent=2))

    st.divider()
    st.caption("Dica: se o RAG estiver vazio, rode o ingest (A) primeiro e confirme a tabela docs_corporativos_chunks.")


# Streamlit multipage entrypoint: algumas versões usam render() como callback
if __name__ == "__main__":
    render()
