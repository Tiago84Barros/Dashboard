from __future__ import annotations

import streamlit as st

from pickup.docs_rag import count_docs_by_tickers, get_docs_by_tickers
from pickup.ingest_docs_cvm_ipe import ingest_ipe_for_tickers
from core.ai_models.llm_client.factory import get_llm_client


def _parse_tickers(raw: str) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    parts = [p.strip().upper().replace(".SA", "") for p in raw.split(",")]
    return [p for p in parts if p]


def render() -> None:
    st.markdown("# 🧪 Patch 6 — Modo Teste (Rápido)")
    st.caption("Roda o Patch 6 sem executar criação de portfólio, score ou backtest. "
               "Use para validar ingestão + RAG + chamada da IA.")

    raw = st.text_input("Tickers (separados por vírgula)", value="BBAS3")
    tickers = _parse_tickers(raw)

    c1, c2 = st.columns(2)
    with c1:
        anos = st.number_input("Anos (CSV IPE)", min_value=1, max_value=5, value=1, step=1)
    with c2:
        max_docs = st.number_input("Máx docs por ticker", min_value=1, max_value=50, value=12, step=1)

    st.markdown("## 1) Verificar docs já existentes")
    if st.button("Contar docs no Supabase", use_container_width=True):
        with st.spinner("Contando..."):
            counts = count_docs_by_tickers(tickers)
        total = sum(counts.values()) if counts else 0
        st.success(f"Docs carregados do Supabase: {total}")
        with st.expander("Ver contagem por ticker", expanded=True):
            for tk in tickers:
                st.write(f"**{tk}**: {counts.get(tk, 0)} docs")

    st.markdown("## 2) Ingerir docs IPE (CVM) via CSV + PDF")
    st.caption("Requer `IPE_CSV_URL_TEMPLATE` nas Secrets/Env. Ex.: https://.../ipe_{ano}.csv")
    if st.button("⬇️ Ingerir IPE (CVM) agora", use_container_width=True):
        with st.spinner("Ingerindo IPE..."):
            result = ingest_ipe_for_tickers(
                tickers,
                anos=int(anos),
                max_docs_por_ticker=int(max_docs),
                sleep_s=0.0,
            )
        st.json(result)

    st.markdown("## 3) Rodar Patch 6 com RAG do Supabase")
    if st.button("🧠 Rodar Patch 6 (usar RAG Supabase)", type="primary", use_container_width=True):
        if not tickers:
            st.warning("Informe ao menos 1 ticker.")
            return

        llm = get_llm_client()
        docs_map = get_docs_by_tickers(tickers, limit_per_ticker=20)

        outputs = {}
        for tk in tickers:
            docs = docs_map.get(tk, []) or []
            corpus = "\\n\\n".join(
                f"[{d.get('fonte','')}/{d.get('tipo','')}] {d.get('titulo','')}\\n{d.get('raw_text','')}"
                for d in docs
            ).strip()

            if not corpus:
                outputs[tk] = {"skip": True, "reason": "Sem docs no Supabase para este ticker."}
                continue

            system = (
                "Você é um analista financeiro. Extraia iniciativas e fatos relevantes a partir de textos corporativos "
                "(CVM/IPE/RI/Notícias) e responda em JSON estrito."
            )

            schema_hint = \"\"\"{
  "ticker": "string",
  "resumo": "string",
  "iniciativas": [
    {
      "titulo": "string",
      "categoria": "string",
      "horizonte": "curto|medio|longo",
      "impacto_esperado": "baixo|medio|alto",
      "evidencias": ["string"]
    }
  ],
  "alertas": ["string"]
}\"\"\"

            user = (
                f"Ticker: {tk}\\n\\n"
                f"Base de textos:\\n{corpus}\\n\\n"
                f"Regras:\\n"
                f"- Seja conservador: só afirme o que estiver no texto.\\n"
                f"- Se algo estiver vago, coloque em 'alertas'.\\n"
            )

            with st.spinner(f"Executando IA para {tk}..."):
                try:
                    outputs[tk] = llm.generate_json(system=system, user=user, schema_hint=schema_hint, context=None)
                except Exception as e:
                    outputs[tk] = {"error": f"{type(e).__name__}: {e}"}

        st.success("Patch 6 finalizado.")
        st.json(outputs)
