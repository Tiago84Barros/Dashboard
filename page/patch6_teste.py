from __future__ import annotations

import os
import inspect
from typing import Any, Dict, List, Optional, Sequence

import streamlit as st

# Store / RAG (Supabase)
from core.patch6_store import prepare_patch6_docs_and_keys

# LLM
from core.ai_models.llm_client.factory import get_llm_client

# Ingest (pickup)
from pickup.ingest_docs_cvm_ipe import ingest_ipe_for_tickers


# -------------------------
# Helpers
# -------------------------
def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()


def _dedup(seq: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in seq:
        x = _norm_ticker(x)
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def _clip(s: str, n: int = 900) -> str:
    s = (s or "").strip()
    return s[:n] + ("…" if len(s) > n else "")


def _call_ingest_flex(
    tickers: List[str],
    *,
    anos: int,
    max_docs_por_ticker: int,
    sleep_s: float,
) -> Dict[str, Any]:
    """
    Chama ingest_ipe_for_tickers com parâmetros compatíveis com diferentes versões do arquivo.
    Resolve automaticamente nomes diferentes (ex.: anos vs years) e ignora params inexistentes.
    """
    fn = ingest_ipe_for_tickers
    sig = inspect.signature(fn)
    params = sig.parameters

    kwargs: Dict[str, Any] = {}

    # tenta mapear "anos"
    if "anos" in params:
        kwargs["anos"] = int(anos)
    elif "years" in params:
        kwargs["years"] = int(anos)
    elif "year_window" in params:
        kwargs["year_window"] = int(anos)
    # se não existir nada, não passa nada (usa default interno)

    # max docs
    if "max_docs_por_ticker" in params:
        kwargs["max_docs_por_ticker"] = int(max_docs_por_ticker)
    elif "max_docs" in params:
        kwargs["max_docs"] = int(max_docs_por_ticker)
    elif "limit" in params:
        kwargs["limit"] = int(max_docs_por_ticker)

    # sleep
    if "sleep_s" in params:
        kwargs["sleep_s"] = float(sleep_s)
    elif "sleep" in params:
        kwargs["sleep"] = float(sleep_s)
    elif "delay_s" in params:
        kwargs["delay_s"] = float(sleep_s)

    # importante: o primeiro argumento deve ser tickers
    return fn(tickers, **kwargs)


def _run_llm(
    *,
    ticker: str,
    empresa: str,
    docs: List[Dict[str, Any]],
    manual_text: str,
) -> Dict[str, Any]:
    schema_hint = """
{
  "iniciativas": [
    {
      "tipo": "expansao|capex|m&a|guidance|reestruturacao|eficiencia|regulatorio|outros",
      "descricao_curta": "STRING",
      "horizonte": "curto|medio|longo|nao_informado",
      "dependencias": ["STRING"],
      "impacto_esperado": "receita|margem|eficiencia|divida|caixa|ambivalente|nao_informado",
      "sinal": "positivo|negativo|ambivalente",
      "evidencia": {"fonte": "STRING", "data": "STRING", "trecho": "STRING"}
    }
  ],
  "avaliacao_execucao": {
    "risco_execucao": "baixo|medio|alto|nao_informado",
    "pontos_a_favor": ["STRING"],
    "pontos_contra": ["STRING"],
    "perguntas_criticas": ["STRING"]
  },
  "rating_compra": {
    "rating": "forte|moderada|fraca|nao_informado",
    "motivo": "STRING"
  },
  "resumo_1_paragrafo": "STRING"
}
""".strip()

    system = """
Você é um analista buy-side, cético e orientado a evidência.
Regras obrigatórias:
- NÃO invente fatos, números, datas, operações.
- Use APENAS o conteúdo fornecido em 'docs' e 'texto manual'.
- Se não houver evidência, deixe 'iniciativas' vazio e explique no resumo.
- Saída SEMPRE em JSON válido no schema solicitado.
""".strip()

    ctx_docs = []
    for d in (docs or [])[:10]:
        ctx_docs.append(
            {
                "fonte": str(d.get("fonte", "NA")),
                "tipo": str(d.get("tipo", "NA")),
                "data": str(d.get("data", "NA")),
                "titulo": str(d.get("titulo", "")),
                "text": str(d.get("raw_text", ""))[:3200],
            }
        )

    user = f"""
Empresa: {empresa} ({ticker})

docs (documentos oficiais/estratégicos):
{ctx_docs}

texto_manual (opcional):
{manual_text if manual_text and manual_text.strip() else "(vazio)"}

Tarefa:
1) Extraia iniciativas estratégicas futuras (projeções, planos, investimentos, reestruturações, guidance etc.)
2) Para cada iniciativa, inclua evidência (fonte/data/trecho) do material fornecido.
3) Avalie risco de execução e classifique compra: forte/moderada/fraca, justificando pelo texto.
""".strip()

    llm = get_llm_client()
    return llm.generate_json(
        system=system,
        user=user,
        schema_hint=schema_hint,
        context=None,
    )


# -------------------------
# Page
# -------------------------
def render() -> None:
    st.markdown("# 🧪 Patch 6 — Teste (Ingest + LLM)")

    with st.sidebar:
        st.markdown("## Entrada")
        ticker = _norm_ticker(st.text_input("Ticker", value="BBAS3"))
        empresa = st.text_input("Nome empresa (opcional)", value=ticker)

        st.markdown("## Ingest (captura)")
        anos = st.number_input("Janela (anos)", 0, 10, 2, 1)
        max_docs = st.number_input("Máx docs por ticker", 1, 80, 25, 1)
        sleep_s = st.number_input("Sleep (s) entre docs", 0.0, 3.0, 0.15, 0.05)

        st.markdown("## Leitura Supabase")
        limit_docs = st.number_input("Docs max p/ LLM (Supabase)", 1, 30, 10, 1)

        provider = (os.getenv("AI_PROVIDER") or "openai").lower()
        model_env = os.getenv("AI_MODEL") or "gpt-4.1-mini"
        st.caption(f"AI_PROVIDER: {provider}")
        st.caption(f"AI_MODEL: {model_env}")

    if not ticker:
        st.warning("Informe um ticker.")
        return

    # =========================
    # (A) INGEST
    # =========================
    st.markdown("## A) 📥 Ingest (capturar/atualizar docs no Supabase)")

    colA1, colA2 = st.columns([1.0, 2.2])
    with colA1:
        do_ingest = st.button("📥 Rodar ingest deste ticker", use_container_width=True)
    with colA2:
        st.caption(
            "Executa o coletor e salva em docs_corporativos / docs_corporativos_chunks. "
            "Depois disso, a LLM consegue ler do Supabase."
        )

    if do_ingest:
        with st.spinner("Ingest em execução..."):
            try:
                resp = _call_ingest_flex(
                    [ticker],
                    anos=int(anos),
                    max_docs_por_ticker=int(max_docs),
                    sleep_s=float(sleep_s),
                )
                st.success("Ingest finalizado.")
                st.json(resp)
            except Exception as e:
                st.error(f"Ingest falhou: {type(e).__name__}: {e}")
                st.stop()

    st.divider()

    # =========================
    # (B) CARREGAR DOCS DO SUPABASE
    # =========================
    st.markdown("## B) 📚 Documentos (carregar do Supabase)")
    try:
        pkg = prepare_patch6_docs_and_keys(
            ticker,
            limit_docs=int(limit_docs),
            provider=provider,
            model=model_env,
            patch_version="patch6_test_v1",
        )
    except Exception as e:
        st.error(f"Falha ao carregar docs do Supabase: {type(e).__name__}: {e}")
        return

    docs = pkg.get("docs") or []
    st.caption(f"Docs encontrados: {len(docs)} | last_doc_date: {pkg.get('last_doc_date')}")

    if not docs:
        st.warning("Nenhum documento encontrado no Supabase para este ticker.")
    else:
        with st.expander("Ver documentos carregados", expanded=False):
            for d in docs:
                st.markdown(f"**{d.get('titulo') or '(sem título)'}**")
                st.caption(f"{d.get('fonte')} | {d.get('tipo')} | {d.get('data')}")
                st.write(_clip(d.get("raw_text", "")))
                st.divider()

    st.divider()

    # =========================
    # (C) TESTE LLM
    # =========================
    st.markdown("## C) 🧠 Teste da LLM (RAG + texto manual opcional)")

    manual_text = st.text_area(
        "Texto manual (opcional) — cole trechos de CVM/RI/call/release. Se vazio, a LLM usa somente o Supabase.",
        height=140,
    )

    if st.button("🚀 Rodar LLM agora"):
        if not docs and not manual_text.strip():
            st.error("Sem docs no Supabase e sem texto manual — não há o que analisar.")
            return

        with st.spinner("Chamando LLM..."):
            try:
                out = _run_llm(
                    ticker=ticker,
                    empresa=empresa or ticker,
                    docs=docs,
                    manual_text=manual_text,
                )
            except Exception as e:
                st.error(f"LLM falhou: {type(e).__name__}: {e}")
                return

        st.success("LLM respondeu.")
        st.json(out)
