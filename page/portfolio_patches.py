# page/portfolio_patches.py

from __future__ import annotations

import streamlit as st
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime
import traceback

# ---------------------------------------------------------------------
# IMPORTS DO PROJETO
# ---------------------------------------------------------------------

try:
    from core.ai_models.llm_client.factory import get_llm_client
except Exception:
    get_llm_client = None

try:
    from pickup.docs_rag import get_docs_for_ticker
except Exception:
    get_docs_for_ticker = None

try:
    from pickup.ingest_docs_cvm_ipe import run_ingest_docs_cvm
except Exception:
    run_ingest_docs_cvm = None


# =====================================================================
# PATCH 6 — Perspectivas & Factibilidade
# =====================================================================

def render_patch6_perspectivas_factibilidade(
    empresas_lideres_finais: List[Dict[str, Any]]
) -> None:

    st.markdown("## 🧩 Patch 6 — Perspectivas & Factibilidade")
    st.caption(
        "Analisa planos futuros (CVM/IPE/notícias) + capacidade financeira de execução."
    )

    # -----------------------------------------------------------------
    # PROTEÇÃO TOTAL CONTRA VARIÁVEIS NÃO INICIALIZADAS
    # -----------------------------------------------------------------
    tickers: List[str] = []

    if not empresas_lideres_finais:
        st.info("Nenhuma empresa líder disponível.")
        return

    # Montagem segura dos tickers
    try:
        tickers = sorted({
            (e.get("ticker") or "").replace(".SA", "").upper()
            for e in empresas_lideres_finais
            if e.get("ticker")
        })
    except Exception:
        tickers = []

    if not tickers:
        st.warning("Não foi possível extrair tickers das empresas líderes.")
        return

    # -----------------------------------------------------------------
    # BLOCO DE ATUALIZAÇÃO DE DOCUMENTOS
    # -----------------------------------------------------------------
    with st.expander("⚙️ Atualizar base de documentos (CVM/IPE) — somente vencedoras"):

        if run_ingest_docs_cvm is None:
            st.warning("Módulo de ingestão não encontrado.")
        else:
            if st.button("🚀 Atualizar documentos agora"):
                try:
                    run_ingest_docs_cvm(tickers)
                    st.success("Base atualizada com sucesso.")
                except Exception as e:
                    st.error(f"Erro na atualização: {e}")
                    st.code(traceback.format_exc())

    st.divider()

    # -----------------------------------------------------------------
    # EXECUÇÃO DO PATCH 6
    # -----------------------------------------------------------------

    if st.button("🧠 Executar Patch 6"):

        if get_llm_client is None:
            st.error("Cliente LLM não encontrado.")
            return

        llm = get_llm_client()

        resultados = []

        for tk in tickers:

            st.write(f"Analisando {tk}...")

            # ----------------------------------------------------------
            # COLETA DE DOCUMENTOS (RAG)
            # ----------------------------------------------------------
            textos = []

            if get_docs_for_ticker:
                try:
                    textos = get_docs_for_ticker(tk)
                except Exception:
                    textos = []

            if not textos:
                st.warning(f"Sem documentos para {tk}.")
                continue

            contexto_textual = "\n\n".join(textos[:5])  # limite de contexto

            # ----------------------------------------------------------
            # PROMPT ESTRUTURADO
            # ----------------------------------------------------------

            system_prompt = """
Você é um analista financeiro especializado em avaliação de capacidade
de execução de planos estratégicos corporativos.
Responda apenas em JSON válido.
"""

            user_prompt = f"""
Analise os planos futuros descritos abaixo e avalie:

1. Principais iniciativas
2. Grau de ambição (baixo/médio/alto)
3. Riscos principais
4. Probabilidade de execução (0 a 100)
5. Comentário objetivo

TEXTOS:
{contexto_textual}
"""

            schema_hint = """
{
  "iniciativas": [],
  "ambicao": "",
  "riscos": [],
  "prob_execucao": 0,
  "comentario": ""
}
"""

            try:
                resposta = llm.generate_json(
                    system=system_prompt,
                    user=user_prompt,
                    schema_hint=schema_hint,
                )

                resposta["ticker"] = tk
                resultados.append(resposta)

            except Exception as e:
                st.error(f"Erro IA {tk}: {e}")
                continue

        # --------------------------------------------------------------
        # RESULTADOS
        # --------------------------------------------------------------

        if resultados:
            df_result = pd.DataFrame(resultados)
            st.success("Análise concluída.")
            st.dataframe(df_result, use_container_width=True)
        else:
            st.warning("Nenhuma análise gerada.")


# =====================================================================
# EXPORT
# =====================================================================

__all__ = [
    "render_patch6_perspectivas_factibilidade",
]
