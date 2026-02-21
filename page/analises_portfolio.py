
# page/analises_portfolio.py

import json
import streamlit as st
from core.rag_retriever import get_topk_chunks_inteligente
from core.patch6_runs_store import save_patch6_run, list_patch6_history
import core.ai_models.llm_client.factory as llm_factory

st.set_page_config(page_title="Análises de Portfólio", layout="wide")

st.title("📊 Análises de Portfólio – Radar Estratégico (Top-K Inteligente + LLM)")

st.markdown("""
Esta seção avalia a **intenção futura e alocação de capital** da companhia:
- Capex / expansão
- Dívida / desalavancagem
- Dividendos / recompra
- M&A / desinvestimentos
- Guidance estratégico

A LLM gera uma conclusão estruturada e auditável com base em evidências reais.
""")

st.markdown("---")

col1, col2, col3 = st.columns(3)

with col1:
    ticker_escolhido = st.text_input("Ticker", value="PETR3").upper()

with col2:
    top_k = st.number_input("Top-K", min_value=3, max_value=20, value=8)

with col3:
    months_window = st.number_input("Janela (meses)", min_value=3, max_value=36, value=18)

debug = st.checkbox("🔎 Debug Top-K (mostrar score detalhado)", value=False)

st.markdown("---")

if st.button("Rodar análise estratégica completa"):

    if not ticker_escolhido:
        st.error("Informe um ticker.")
        st.stop()

    with st.spinner("Buscando contexto estratégico..."):
        resultado = get_topk_chunks_inteligente(
            ticker=ticker_escolhido,
            top_k=top_k,
            months_window=months_window,
            debug=debug
        )

    if debug:
        st.subheader("🔎 Debug – Score detalhado")
        st.dataframe(
            [{
                "chunk_id": h.chunk_id,
                "doc_id": h.doc_id,
                "tipo_doc": h.tipo_doc,
                "data_doc": h.data_doc,
                "score_final": round(h.score_final, 4),
                "intent": round(h.score_intent, 4),
                "recency": round(h.score_recency, 4),
                "peso_tipo": round(h.weight_tipo, 4),
            } for h in resultado],
            use_container_width=True
        )
        chunks = [h.chunk_text for h in resultado]
    else:
        chunks = resultado

    if not chunks:
        st.warning("Nenhum contexto estratégico encontrado.")
        st.stop()

    contexto = "\n\n".join(chunks)

    prompt = f"""
Você é um analista fundamentalista focado em criação de valor para o acionista minoritário.

Use exclusivamente o CONTEXTO abaixo.

Retorne APENAS JSON válido na seguinte estrutura:

{{
  "perspectiva": "forte|moderada|fraca",
  "tese_minoritaria": ["..."],
  "alocacao_capital": {{
    "capex_expansao": "...",
    "divida_desalavancagem": "...",
    "dividendos_recompra": "...",
    "ma_desinvest": "..."
  }},
  "sinais_positivos": ["..."],
  "sinais_alerta": ["..."],
  "catalisadores": ["..."],
  "evidencias": ["trechos literais do contexto"]
}}

CONTEXTO:
{contexto}
"""

    with st.spinner("Chamando LLM..."):
        client = llm_factory.get_llm_client()
        raw = client.complete(prompt)

    try:
        resultado_json = json.loads(raw)
    except Exception:
        st.error("A LLM não retornou JSON válido.")
        st.code(raw)
        st.stop()

    st.success("Análise gerada com sucesso.")
    st.json(resultado_json)

    save_patch6_run(
        snapshot_id="manual_run",
        ticker=ticker_escolhido,
        period_ref="current",
        result=resultado_json,
    )

    st.markdown("---")
    st.subheader("📜 Histórico de análises")
    try:
        historico = list_patch6_history(ticker_escolhido, limit=5)
        st.dataframe(historico, use_container_width=True)
    except Exception as e:
        st.caption(f"Erro ao carregar histórico: {e}")
