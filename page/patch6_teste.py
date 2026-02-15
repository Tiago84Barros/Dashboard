from __future__ import annotations

"""
page/patch6_teste.py
--------------------
Página de TESTE do Patch 6:
- não roda score/backtest/criação de portfólio
- permite:
  (1) informar tickers
  (2) contar docs no Supabase
  (3) ingerir IPE (CVM) (se disponível)
  (4) rodar Patch6 usando RAG do Supabase (docs_rag)

Requer:
- pickup/docs_rag.py com:
    - count_docs_by_tickers(tickers)
    - get_docs_by_tickers(tickers, limit_per_ticker=..., use_chunks=...)
- pickup/ingest_docs_cvm_ipe.py com:
    - ingest_ipe_for_tickers(tickers, ...)
- core/ai_models/llm_client/factory.py e openai_client.py
"""

from typing import Any, Dict, List
import streamlit as st

# Imports do seu projeto
from core.ai_models.llm_client.factory import get_llm_client
from pickup.docs_rag import count_docs_by_tickers, get_docs_by_tickers
#from pickup.ingest_docs_cvm_ipe import ingest_ipe_for_tickers
from pickup.ingest_docs_cvm_enet import ingest_enet_for_tickers

def _parse_tickers(s: str) -> List[str]:
    raw = (s or "").replace(";", ",").replace("\n", ",")
    out = []
    for t in raw.split(","):
        t = (t or "").strip().upper().replace(".SA", "")
        if t:
            out.append(t)
    # unique mantendo ordem
    seen = set()
    uniq = []
    for t in out:
        if t not in seen:
            uniq.append(t)
            seen.add(t)
    return uniq


def render() -> None:
    st.title("🧪 Patch 6 — Modo Teste (rápido)")

    st.caption("Roda o Patch 6 sem executar criação de portfólio, score ou backtest.")

    tickers_txt = st.text_input(
        "Tickers (separados por vírgula)",
        value="ROMI3, KEPL3, MYPK3, BBAS3",
        help="Ex: PETR4, VALE3, ITUB4",
    )
    tickers = _parse_tickers(tickers_txt)

    st.markdown("## Parâmetros do teste")
    ativar_ajuste_peso = st.checkbox("Ativar ajuste de peso", value=True)

    st.divider()

    # ---------------------------------------------------------------------
    # 2) Contar docs no Supabase
    # ---------------------------------------------------------------------
    st.markdown("## 2) Verificar docs já existentes no Supabase")

    if st.button("Contar docs no Supabase", use_container_width=True):
        try:
            counts = count_docs_by_tickers(tickers)
            total = sum(int(v) for v in counts.values()) if isinstance(counts, dict) else 0
            st.success(f"Docs carregados do Supabase: {total}")

            with st.expander("Ver contagem por ticker", expanded=True):
                for tk in tickers:
                    st.write(f"**{tk}**: {int(counts.get(tk, 0))} docs")
        except Exception as e:
            st.error(f"Falha ao contar docs no Supabase: {e}")

    st.divider()

    # ---------------------------------------------------------------------
    # 3) Ingerir IPE (CVM)
    # ---------------------------------------------------------------------
    st.markdown("## 3) Ingerir docs IPE (CVM) para os tickers")
    st.caption("Se a CVM/endpoint/CSV mudar, esta etapa pode falhar. Ainda assim dá para testar o Patch6 com texto manual.")

    col1, col2, col3 = st.columns(3)
    with col1:
        anos = st.number_input("Anos (janela)", min_value=1, max_value=8, value=2, step=1)
    with col2:
        max_docs = st.number_input("Máx docs por ticker", min_value=1, max_value=200, value=25, step=5)
    with col3:
        sleep_s = st.number_input("Sleep entre docs (s)", min_value=0.0, max_value=2.0, value=0.2, step=0.1)

    if st.button("⬇️ Ingerir IPE (CVM) agora", use_container_width=True):
        with st.spinner("Ingerindo IPE..."):
            try:
                res = ingest_ipe_for_tickers(
                    tickers,
                    anos=int(anos),
                    max_docs_por_ticker=int(max_docs),
                    sleep_s=float(sleep_s),
                )
                st.json(res)
            except Exception as e:
                st.error(f"Falha ao ingerir IPE: {e}")

    st.divider()

    # ---------------------------------------------------------------------
    # 4) Rodar Patch 6 usando RAG do Supabase
    # ---------------------------------------------------------------------
    st.markdown("## 4) Rodar Patch 6 com RAG do Supabase")
    st.caption("Aqui o Patch6 puxa automaticamente docs_by_ticker do Supabase.")

    # Schema de resposta do LLM (JSON obrigatório)
    schema_hint = """{
  "tese": "string curta (1-3 frases)",
  "iniciativas": [
    {
      "titulo": "string",
      "tipo": "expansao|eficiencia|capex|m&a|divida|governanca|outros",
      "horizonte_meses": 0,
      "impacto": "baixo|medio|alto",
      "confianca": 0.0,
      "evidencias": ["trechos curtos"]
    }
  ],
  "risco_execucao": "baixo|medio|alto",
  "comentarios": "string"
}"""

    limit_docs = st.number_input("Limite docs por ticker (RAG)", min_value=1, max_value=50, value=10, step=1)
    use_chunks = st.checkbox("Usar chunks (docs_corporativos_chunks)", value=True)

    if st.button("🧠 Rodar Patch 6 (usar RAG Supabase)", use_container_width=True):
        try:
            # 1) carrega docs do Supabase
            docs_by_ticker = get_docs_by_tickers(
                tickers,
                limit_per_ticker=int(limit_docs),
                use_chunks=bool(use_chunks),
            )

            # 2) monta contexto por ticker
            llm = get_llm_client()

            for tk in tickers:
                st.subheader(f"📌 {tk}")

                docs = docs_by_ticker.get(tk) or []
                if not docs:
                    st.warning("Nenhum doc encontrado no Supabase para este ticker.")
                    continue

                # junta textos (com limite defensivo)
                ctx_parts = []
                for d in docs:
                    txt = (d.get("chunk_text") or d.get("raw_text") or "").strip()
                    if txt:
                        ctx_parts.append(txt)
                contexto = "\n\n---\n\n".join(ctx_parts)[:18000]

                system = (
                    "Você é um analista financeiro. Extraia iniciativas, horizonte, risco e evidências. "
                    "Responda SOMENTE em JSON válido no schema solicitado."
                )

                user = (
                    f"Empresa: {tk}\n"
                    f"A seguir há textos (CVM/RI/notícias) para a empresa. "
                    f"Extraia iniciativas e avalie factibilidade com evidências.\n\n"
                    f"TEXTOS:\n{contexto}\n"
                )

                with st.spinner("Executando análise estruturada..."):
                    out = ingest_enet_for_tickers(
                        tickers=tickers,
                        anos=anos,
                        max_docs_por_ticker=max_docs,
                        baixar_e_extrair=True,
                   )

                   st.json(out)

        except Exception as e:
            st.error(f"Patch 6 falhou: {e}")
