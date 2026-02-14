# page/patch6_teste.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
import streamlit as st

# RAG / Supabase docs
from pickup.docs_rag import count_docs_by_tickers, get_docs_by_tickers
from pickup.ingest_docs_cvm_ipe import ingest_ipe_for_tickers


def _parse_tickers(txt: str) -> List[str]:
    out: List[str] = []
    for p in (txt or "").replace(";", ",").split(","):
        tk = (p or "").strip().upper().replace(".SA", "").strip()
        if tk:
            out.append(tk)
    return list(dict.fromkeys(out))


def _parse_years(txt: str) -> List[int]:
    years: List[int] = []
    for p in (txt or "").replace(";", ",").split(","):
        p = p.strip()
        if p.isdigit():
            years.append(int(p))
    return list(dict.fromkeys(years))


def _import_patch6_renderer():
    """
    Import robusto: tenta caminhos diferentes onde o projeto costuma colocar portfolio_patches.
    Ajuste mínimo sem quebrar o app inteiro.
    """
    # 1) mais comum no seu projeto atual
    try:
        from core.portfolio_patches import render_patch6_perspectivas_factibilidade
        return render_patch6_perspectivas_factibilidade
    except Exception:
        pass

    # 2) fallback (se tiver movido o arquivo)
    try:
        from portfolio_patches import render_patch6_perspectivas_factibilidade
        return render_patch6_perspectivas_factibilidade
    except Exception as e:
        raise ImportError(
            "Não consegui importar render_patch6_perspectivas_factibilidade "
            "de core.portfolio_patches nem de portfolio_patches."
        ) from e


def render() -> None:
    # ✅ ESSA FUNÇÃO PRECISA EXISTIR
    st.markdown("# 🧪 Teste do Patch 6 (modo rápido)")
    st.caption("Roda o Patch 6 sem executar criação de portfólio, score ou backtest.")

    tickers_txt = st.text_input(
        "Tickers (separados por vírgula)",
        value="ROMI3,KEPL3,MYPK3,BBAS3",
        help="Ex: ROMI3, KEPL3, MYPK3, BBAS3",
    )
    tickers = _parse_tickers(tickers_txt)

    st.markdown("## Parâmetros do teste")
    c1, c2, c3 = st.columns([1, 1, 1.2])
    with c1:
        ativar_ajuste_peso = st.checkbox("Ativar ajuste de peso", value=True)
    with c2:
        anos_txt = st.text_input("Ano(s) IPE (ex: 2025 ou 2024,2025)", value="2025")
    with c3:
        max_docs = st.number_input("Máx docs por ticker (ingest)", 1, 40, 8, 1)

    years = _parse_years(anos_txt)

    st.markdown("## Base de documentos (Supabase)")
    if not tickers:
        st.info("Informe pelo menos 1 ticker.")
        return

    # Contagem atual
    try:
        counts = count_docs_by_tickers(tickers)
        total = sum(counts.values()) if counts else 0
        st.success(f"Docs carregados do Supabase: {total}")

        with st.expander("Ver contagem por ticker", expanded=False):
            for tk in tickers:
                st.write(f"**{tk}**: {counts.get(tk, 0)} docs")
    except Exception as e:
        st.error(f"Falha ao carregar contagem de docs do Supabase: {type(e).__name__}: {e}")
        return

    st.markdown("---")
    st.markdown("## 1) Atualizar base (CVM/IPE) — Opção 1")
    st.caption("Isso alimenta as tabelas docs_corporativos e docs_corporativos_chunks.")

    if st.button("⚙️ Atualizar base agora (CVM/IPE)"):
        with st.spinner("Baixando e ingerindo IPE..."):
            try:
                res = ingest_ipe_for_tickers(
                    tickers,
                    years=years if years else None,
                    max_docs_per_ticker=int(max_docs),
                    chunk_size=1200,
                    overlap=180,
                    fetch_full_text=True,
                )
                if res.get("ok"):
                    st.success(
                        f"Ingest concluído. Selecionados={res.get('rows_selected')} | Inseridos={res.get('inserted')}"
                    )
                    st.json(res.get("by_ticker", {}))
                    # importante para atualizar contagens/consultas
                    st.cache_data.clear()
                else:
                    st.error(f"Ingest falhou: {res.get('error')}")
            except Exception as e:
                st.error(f"Falha no ingest: {type(e).__name__}: {e}")

    st.markdown("---")
    st.markdown("## 2) Rodar Patch 6 agora")
    st.caption("Se ainda estiver 0 docs, o Patch 6 vai cair no fluxo de “sem textos fornecidos”.")

    # Monta estrutura mínima (mock) que o Patch6 espera
    empresas_lideres_finais = [
        {"ticker": tk, "nome": tk, "segmento": "", "peso": 0.0} for tk in tickers
    ]

    try:
        docs_by_ticker = get_docs_by_tickers(
            tickers,
            limit_docs=10,
            prefer_chunks=True,
            limit_chunks=18,
        )
    except Exception as e:
        st.error(f"Falha ao carregar docs do Supabase (docs_rag): {type(e).__name__}: {e}")
        docs_by_ticker = {}

    # Importa Patch6 somente quando necessário
    render_patch6 = None
    try:
        render_patch6 = _import_patch6_renderer()
    except Exception as e:
        st.error(str(e))
        st.stop()

    if st.button("🧠 Rodar Patch 6 agora"):
        with st.spinner("Executando Patch 6..."):
            try:
                render_patch6(
                    empresas_lideres_finais,
                    docs_by_ticker=docs_by_ticker,
                    indicadores_por_ticker=None,
                    ativar_ajuste_peso=ativar_ajuste_peso,
                    cache_horas_default=24,
                )
            except Exception as e:
                st.error(f"Patch 6 falhou: {type(e).__name__}: {e}")
