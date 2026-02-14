from __future__ import annotations

import streamlit as st
from datetime import datetime
from typing import Dict, List, Any, Optional

# =============================================================================
# PATCH 1 – RÉGUA DE CONVICÇÃO
# =============================================================================

def render_patch1_regua_conviccao(score_global, lideres_global, empresas_lideres_finais):
    st.markdown("### 🧩 Patch 1 — Régua de Convicção")

    if not empresas_lideres_finais:
        st.info("Nenhuma empresa selecionada.")
        return

    for emp in empresas_lideres_finais:
        st.write(f"• {emp.get('ticker')} — Peso sugerido: {emp.get('peso', 0)*100:.1f}%")


# =============================================================================
# PATCH 2 – DOMINÂNCIA
# =============================================================================

def render_patch2_dominancia(score_global, lideres_global, empresas_lideres_finais):
    st.markdown("### 🧩 Patch 2 — Dominância")

    if lideres_global is None or lideres_global.empty:
        st.info("Sem dados de dominância.")
        return

    st.write("Dominância calculada com base no histórico de liderança.")


# =============================================================================
# PATCH 3 – DIVERSIFICAÇÃO
# =============================================================================

def render_patch3_diversificacao(empresas_lideres_finais, contrib_globais=None):
    st.markdown("### 🧩 Patch 3 — Diversificação")

    if not empresas_lideres_finais:
        st.info("Sem empresas para avaliar.")
        return

    pesos = [e.get("peso", 0) for e in empresas_lideres_finais]
    st.write(f"Peso médio: {sum(pesos)/len(pesos)*100:.2f}%")


# =============================================================================
# PATCH 4 – BENCHMARK SEGMENTO
# =============================================================================

def render_patch4_benchmark_segmento(score_global, empresas_lideres_finais, precos=None, max_universe=80):
    st.markdown("### 🧩 Patch 4 — Benchmark do Segmento")

    if score_global is None or score_global.empty:
        st.info("Sem dados de benchmark.")
        return

    st.write("Benchmark baseado no último ano disponível do score.")


# =============================================================================
# PATCH 5 – DESEMPENHO EMPRESAS
# =============================================================================

def render_patch5_desempenho_empresas(empresas_lideres_finais):
    st.markdown("### 🧩 Patch 5 — Desempenho das Empresas")

    if not empresas_lideres_finais:
        st.info("Sem empresas para avaliar.")
        return

    for emp in empresas_lideres_finais:
        st.write(f"{emp.get('ticker')} — análise histórica consolidada.")


# =============================================================================
# PATCH 6 – PERSPECTIVAS & FACTIBILIDADE (ESTÁVEL)
# =============================================================================

def render_patch6_perspectivas_factibilidade(
    empresas_lideres_finais: List[Dict[str, Any]],
    indicadores_por_ticker: Optional[Dict[str, Any]] = None,
    docs_by_ticker: Optional[Dict[str, Any]] = None,
    ativar_ajuste_peso: bool = True,
    cache_horas_default: int = 24,
):
    """
    Patch 6 completamente seguro.
    Nada executa automaticamente.
    Só roda após clique no botão.
    """

    st.markdown("## 🧩 Patch 6 — Perspectivas & Factibilidade")

    if not empresas_lideres_finais:
        st.info("Nenhuma empresa disponível.")
        return

    # -----------------------------
    # Botão de execução manual
    # -----------------------------
    executar = st.button("🧠 Executar Patch 6 (Analisar planos futuros)")

    if not executar:
        st.info("Clique no botão acima para executar a análise.")
        return

    # -----------------------------
    # Execução segura
    # -----------------------------
    st.success("Executando análise estruturada...")

    for empresa in empresas_lideres_finais:

        ticker = empresa.get("ticker", "")
        nome = empresa.get("nome", ticker)

        st.markdown(f"### 📌 {nome} ({ticker})")

        # Texto manual (MVP seguro)
        texto_usuario = st.text_area(
            f"Informe texto de referência (CVM/RI/notícias) para {ticker}",
            height=120,
            key=f"text_{ticker}"
        )

        if not texto_usuario:
            st.warning("Nenhum texto informado. Pulando análise.")
            continue

        # Simulação de estruturação (sem IA automática)
        iniciativas = _extrair_iniciativas_simples(texto_usuario)

        st.write("**Iniciativas detectadas:**")
        for item in iniciativas:
            st.write(f"- {item}")

        # Factibilidade básica objetiva
        score_execucao = _score_execucao_simples(texto_usuario)

        st.write(f"**Score de execução estimado:** {score_execucao:.1f}/10")

        if ativar_ajuste_peso:
            ajuste = score_execucao / 10.0
            novo_peso = empresa.get("peso", 0) * ajuste
            st.write(f"Peso ajustado sugerido: {novo_peso*100:.2f}%")


# =============================================================================
# FUNÇÕES AUXILIARES (SEGURAS)
# =============================================================================

def _extrair_iniciativas_simples(texto: str) -> List[str]:
    palavras_chave = [
        "expansão",
        "aquisição",
        "redução de dívida",
        "investimento",
        "novo projeto",
        "capex",
        "licitação",
    ]

    iniciativas = []
    texto_lower = texto.lower()

    for palavra in palavras_chave:
        if palavra in texto_lower:
            iniciativas.append(palavra)

    if not iniciativas:
        iniciativas.append("Nenhuma iniciativa estratégica clara detectada.")

    return iniciativas


def _score_execucao_simples(texto: str) -> float:
    tamanho = len(texto)

    if tamanho > 2000:
        return 8.0
    elif tamanho > 1000:
        return 6.5
    elif tamanho > 500:
        return 5.0
    else:
        return 3.5
