# dashboard/page/patch6_teste.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
import streamlit as st


def render() -> None:
    st.title("🧪 Teste Rápido — Patch 6 (isolado)")
    st.caption("Roda o Patch 6 sem executar criação de portfólio, score ou backtest.")

    # Import local (evita quebrar o app caso algo ainda esteja faltando)
    try:
        from page.portfolio_patches import render_patch6_perspectivas_factibilidade
    except Exception as e:
        st.error(f"Falha ao importar Patch 6: {type(e).__name__}: {e}")
        return

    # ---- Entrada de tickers
    default = "ROMI3, KEPL3, MYPK3"
    tickers_txt = st.text_input("Tickers (separados por vírgula)", value=default)

    tickers = [t.strip().upper().replace(".SA", "") for t in tickers_txt.split(",") if t.strip()]
    tickers = list(dict.fromkeys(tickers))

    if not tickers:
        st.warning("Informe pelo menos 1 ticker.")
        return

    empresas_mock: List[Dict[str, Any]] = [{"ticker": tk, "nome": tk, "peso": 0.10} for tk in tickers]

    st.markdown("### Parâmetros do teste")
    col1, col2, col3 = st.columns(3)
    with col1:
        ativar_ajuste_peso = st.checkbox("Ativar ajuste de peso", value=True)
    with col2:
        cache_horas = st.number_input("Cache (horas)", min_value=1, max_value=168, value=24, step=1)
    with col3:
        usar_docs_supabase = st.checkbox("Carregar docs do Supabase (RAG)", value=True)

    docs_by_ticker: Optional[Dict[str, List[Dict[str, Any]]]] = None

    # ---- (Opcional) Carregar docs via Supabase
    if usar_docs_supabase:
        try:
            # ajuste este import se seu arquivo estiver em outro path
            from pickup.docs_rag import get_docs_by_ticker  # type: ignore

            with st.spinner("Lendo docs do Supabase..."):
                docs_by_ticker = get_docs_by_ticker(
                    tickers=tickers,
                    limit_docs=8,
                    limit_chars_per_doc=4000,
                )

            total_docs = sum(len(v or []) for v in (docs_by_ticker or {}).values())
            st.success(f"Docs carregados do Supabase: {total_docs}")

            with st.expander("Ver contagem por ticker", expanded=False):
                for tk in tickers:
                    st.write(f"{tk}: {len((docs_by_ticker or {}).get(tk, []) or [])} docs")

        except Exception as e:
            st.error(f"Falha ao carregar docs do Supabase (docs_rag): {type(e).__name__}: {e}")
            docs_by_ticker = None

    st.markdown("---")

    if st.button("🧠 Rodar Patch 6 agora", type="primary"):
        try:
            render_patch6_perspectivas_factibilidade(
                empresas_mock,
                indicadores_por_ticker=None,       # depois plugamos indicadores aqui
                docs_by_ticker=docs_by_ticker,     # RAG vindo do Supabase
                ativar_ajuste_peso=ativar_ajuste_peso,
                cache_horas_default=int(cache_horas),
            )
        except Exception as e:
            st.error(f"Patch 6 quebrou: {type(e).__name__}: {e}")
