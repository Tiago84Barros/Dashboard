from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

# Store (cache + persistência)
from core.patch6_store import (
    prepare_patch6_docs_and_keys,
    get_assessment_by_run_key,
    upsert_assessment_and_initiatives,
)

# LLM
from core.ai_models.llm_client.factory import get_llm_client

# INGEST
from pickup.ingest_docs_cvm_ipe import ingest_ipe_for_tickers


# ============================================================
# Helpers
# ============================================================

def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()

def _clip(s: str, n: int = 800) -> str:
    s = (s or "").strip()
    return s[:n] + ("…" if len(s) > n else "")

def _as_list(x: Any) -> List[str]:
    if isinstance(x, list):
        return [str(i) for i in x if str(i).strip()]
    if isinstance(x, str) and x.strip():
        return [x.strip()]
    return []


# ============================================================
# LLM
# ============================================================

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
Você é um analista buy-side focado em evidência textual.
Use apenas o conteúdo fornecido.
Não invente dados.
Sempre retorne JSON válido conforme schema.
""".strip()

    llm = get_llm_client()

    user = f"""
Empresa: {empresa} ({ticker})

Documentos:
{docs}

Texto manual:
{manual_text if manual_text else "(vazio)"}

Extraia iniciativas estratégicas futuras, avalie risco de execução
e classifique compra como forte/moderada/fraca.
"""

    return llm.generate_json(
        system=system,
        user=user,
        schema_hint=schema_hint,
        context=None,
    )


# ============================================================
# Página
# ============================================================

def render() -> None:

    st.markdown("# 🧪 Patch 6 — Modo Teste Completo")

    # ------------------------------------------------------------
    # Sidebar
    # ------------------------------------------------------------
    with st.sidebar:

        ticker = _norm_ticker(
            st.text_input("Ticker", value="BBAS3")
        )

        empresa = st.text_input("Nome empresa", value=ticker)

        limit_docs = st.number_input("Docs max", 1, 30, 10)
        anos = st.number_input("Janela anos (ingest)", 1, 10, 2)
        max_docs = st.number_input("Max docs por ticker (ingest)", 5, 200, 30)

        provider = (os.getenv("AI_PROVIDER") or "openai").lower()
        model_env = os.getenv("AI_MODEL") or "gpt-4.1-mini"

        st.write(f"Provider: {provider}")
        st.write(f"Model: {model_env}")

    if not ticker:
        st.warning("Informe ticker.")
        return

    # ------------------------------------------------------------
    # INGEST
    # ------------------------------------------------------------
    st.markdown("## ⬇️ Atualizar documentos no Supabase")

    col1, col2 = st.columns(2)

    if col1.button("Atualizar este ticker"):
        with st.spinner("Ingerindo..."):
            resp = ingest_ipe_for_tickers(
                [ticker],
                anos=int(anos),
                max_docs_por_ticker=int(max_docs),
            )
        st.success("Finalizado.")
        st.json(resp)

    tickers_lote = st.text_area(
        "Atualizar lote (1 ticker por linha)",
        height=100
    )

    if col2.button("Atualizar lote"):
        lista = [
            _norm_ticker(x)
            for x in tickers_lote.splitlines()
            if x.strip()
        ]
        if lista:
            with st.spinner("Ingerindo lote..."):
                resp = ingest_ipe_for_tickers(
                    lista,
                    anos=int(anos),
                    max_docs_por_ticker=int(max_docs),
                )
            st.success("Finalizado.")
            st.json(resp)

    st.divider()

    # ------------------------------------------------------------
    # BUSCA DOCS
    # ------------------------------------------------------------
    pkg = prepare_patch6_docs_and_keys(
        ticker,
        limit_docs=int(limit_docs),
        provider=provider,
        model=model_env,
        patch_version="patch6_v1",
    )

    st.markdown("## 📚 Documentos encontrados")

    docs = pkg["docs"]

    if not docs:
        st.warning("Nenhum documento encontrado.")
    else:
        for d in docs:
            st.markdown(f"**{d.get('titulo')}**")
            st.caption(f"{d.get('fonte')} | {d.get('tipo')} | {d.get('data')}")
            st.write(_clip(d.get("raw_text", "")))
            st.divider()

    # ------------------------------------------------------------
    # CACHE
    # ------------------------------------------------------------
    st.markdown("## 🧠 Cache")

    cached = get_assessment_by_run_key(pkg["run_key"])

    if cached and cached.get("assessment"):
        st.success("Cache HIT")
        st.json(cached["assessment"])
    else:
        st.info("Cache MISS")

    st.divider()

    # ------------------------------------------------------------
    # LLM
    # ------------------------------------------------------------
    manual_text = st.text_area(
        "Texto manual opcional",
        height=120
    )

    if st.button("🚀 Rodar LLM"):

        if not docs and not manual_text.strip():
            st.error("Sem dados para enviar à LLM.")
            return

        with st.spinner("Chamando LLM..."):
            out = _run_llm(
                ticker=ticker,
                empresa=empresa,
                docs=docs,
                manual_text=manual_text,
            )

        st.markdown("## Resultado LLM")
        st.json(out)

        iniciativas = out.get("iniciativas", [])
        aval = out.get("avaliacao_execucao", {})
        rating = out.get("rating_compra", {})

        assessment_id = upsert_assessment_and_initiatives(
            run_key=pkg["run_key"],
            ticker=ticker,
            provider=provider,
            model=model_env,
            docs_hash=pkg["docs_hash"],
            docs_count=pkg["docs_count"],
            last_doc_date_iso=pkg["last_doc_date"],
            score_regua_0_100=None,
            ajuste_ia_pp=None,
            score_final_0_100=None,
            risco_execucao=aval.get("risco_execucao"),
            rating_compra=rating.get("rating"),
            motivo_compra=rating.get("motivo"),
            fator_peso_aporte=None,
            resumo_1_paragrafo=out.get("resumo_1_paragrafo"),
            pontos_a_favor=_as_list(aval.get("pontos_a_favor")),
            pontos_contra=_as_list(aval.get("pontos_contra")),
            perguntas_criticas=_as_list(aval.get("perguntas_criticas")),
            llm_json=out,
            iniciativas=iniciativas,
        )

        st.success(f"Salvo no Supabase (assessment_id={assessment_id})")
