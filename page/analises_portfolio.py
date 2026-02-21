# page/analises_portfolio.py
# Compatível com loader que exige função render() e com estrutura /Modulos/*

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import streamlit as st


# ---------------------------
# Bootstrapping de PATH
# ---------------------------
def _ensure_project_paths() -> None:
    """
    Garante que imports 'core.*' e 'pickup.*' funcionem tanto quando:
    - arquivo está em Dashboard/page/
    - arquivo está em Dashboard/Modulos/page/
    Sem depender de alterar o dashboard.py.
    """
    here = Path(__file__).resolve()
    page_dir = here.parent  # .../page
    base_dir = page_dir.parent  # .../(Modulos ou Dashboard)

    # Se estamos em .../Modulos/page, base_dir = .../Modulos
    # Se estamos em .../Dashboard/page, base_dir = .../Dashboard

    # 1) adiciona base_dir ao sys.path (para 'core', 'pickup', 'page' dentro dele)
    if str(base_dir) not in sys.path:
        sys.path.insert(0, str(base_dir))

    # 2) se existir uma pasta Modulos na raiz do Dashboard, adiciona também
    root_candidate = base_dir.parent  # possivelmente .../Dashboard
    modulos_dir = root_candidate / "Modulos"
    if modulos_dir.exists() and str(modulos_dir) not in sys.path:
        sys.path.insert(0, str(modulos_dir))


_ensure_project_paths()


# ---------------------------
# Imports do projeto (com fallback)
# ---------------------------
def _import_or_warn(import_fn: Callable[[], Any], friendly_name: str) -> Any:
    try:
        return import_fn()
    except Exception as e:
        st.error(f"Falha ao importar {friendly_name}: {e}")
        raise


# RAG retriever
get_topk_chunks_inteligente = _import_or_warn(
    lambda: __import__("core.rag_retriever", fromlist=["get_topk_chunks_inteligente"]).get_topk_chunks_inteligente,
    "core.rag_retriever.get_topk_chunks_inteligente"
)

# Patch6 runs store (opcional)
try:
    _runs_mod = __import__("core.patch6_runs_store", fromlist=["save_patch6_run", "list_patch6_history"])  # type: ignore
    save_patch6_run = getattr(_runs_mod, "save_patch6_run")
    list_patch6_history = getattr(_runs_mod, "list_patch6_history")
except Exception:
    # fallback: não quebra a página se o store ainda não estiver no deploy
    def save_patch6_run(*args, **kwargs):  # type: ignore
        return None

    def list_patch6_history(*args, **kwargs):  # type: ignore
        return []

# LLM factory (opcional)
llm_factory = None
try:
    llm_factory = __import__("core.ai_models.llm_client.factory", fromlist=["get_llm_client"])  # type: ignore
except Exception:
    llm_factory = None


# ---------------------------
# Helpers
# ---------------------------
def _safe_llm_complete(prompt: str) -> str:
    """Tenta completar via cliente LLM do projeto. Falha com mensagem clara."""
    if llm_factory is None or not hasattr(llm_factory, "get_llm_client"):
        raise RuntimeError("LLM client factory não disponível (core.ai_models.llm_client.factory).")

    client = llm_factory.get_llm_client()  # type: ignore

    # compat: complete(prompt) ou chat(prompt) ou generate(prompt)
    for fn_name in ("complete", "chat", "generate", "invoke"):
        fn = getattr(client, fn_name, None)
        if callable(fn):
            out = fn(prompt)
            # normaliza retorno
            if isinstance(out, str):
                return out
            if isinstance(out, dict) and "text" in out:
                return str(out["text"])
            return json.dumps(out, ensure_ascii=False)

    raise RuntimeError("Cliente LLM não expõe métodos complete/chat/generate/invoke.")


def _build_prompt(ticker: str, contexto: str) -> str:
    return f"""
Você é um analista fundamentalista focado em criação de valor para o acionista minoritário.
Use exclusivamente o CONTEXTO abaixo. Não invente fatos.

Retorne APENAS JSON válido na seguinte estrutura:

{{
  "perspectiva": "forte|moderada|fraca",
  "tese_minoritaria": ["..."],
  "alocacao_capital": {{
    "capex_expansao": "alta|media|baixa|nao_mencionado",
    "divida_desalavancagem": "melhorando|estavel|piorando|nao_mencionado",
    "dividendos_recompra": "pro_acionista|neutro|incerto|nao_mencionado",
    "ma_desinvest": "acretivo|neutro|arriscado|nao_mencionado"
  }},
  "sinais_positivos": ["..."],
  "sinais_alerta": ["..."],
  "catalisadores": [{{"evento":"...","horizonte":"curto|medio|longo"}}],
  "evidencias": ["trechos literais do contexto"]
}}

Ticker: {ticker}

CONTEXTO:
{contexto}
""".strip()


# ---------------------------
# Página (loader exige render)
# ---------------------------
def render() -> None:
    st.set_page_config(page_title="Análises de Portfólio", layout="wide")
    st.title("📊 Análises de Portfólio – Radar Estratégico (Top-K Inteligente + LLM)")

    st.markdown(
        """
        Esta seção avalia **intenção futura e alocação de capital** (não é DFP/ITR):
        - capex / expansão
        - dívida / desalavancagem
        - dividendos / recompra
        - M&A / desinvestimentos
        - guidance / prioridades do management
        """
    )

    st.markdown("---")

    col1, col2, col3 = st.columns(3)
    with col1:
        ticker = st.text_input("Ticker", value="PETR3").upper().strip()
    with col2:
        top_k = st.number_input("Top-K", min_value=3, max_value=20, value=8)
    with col3:
        months_window = st.number_input("Janela (meses)", min_value=3, max_value=36, value=18)

    debug = st.checkbox("🔎 Debug Top-K (mostrar score detalhado)", value=False)
    st.caption("Obs: para rodar LLM, é preciso que o módulo core.ai_models.llm_client.factory esteja no deploy e configurado.")

    st.markdown("---")

    if st.button("Rodar Top-K inteligente + LLM"):
        if not ticker:
            st.error("Informe um ticker.")
            st.stop()

        with st.spinner("Buscando contexto estratégico (Top-K inteligente)..."):
            result = get_topk_chunks_inteligente(
                ticker=ticker,
                top_k=int(top_k),
                months_window=int(months_window),
                debug=bool(debug),
            )

        # result pode ser lista de hits (debug=True) ou lista de textos (debug=False)
        if debug:
            st.subheader("🔎 Debug – Score detalhado (Top-K)")
            st.dataframe(
                [
                    {
                        "chunk_id": h.chunk_id,
                        "doc_id": h.doc_id,
                        "tipo_doc": h.tipo_doc,
                        "data_doc": h.data_doc,
                        "score_final": round(h.score_final, 4),
                        "intent": round(h.score_intent, 4),
                        "recency": round(h.score_recency, 4),
                        "peso_tipo": round(h.weight_tipo, 4),
                    }
                    for h in result
                ],
                use_container_width=True,
            )
            chunks = [h.chunk_text for h in result]
        else:
            chunks = result

        if not chunks:
            st.warning("Nenhum chunk relevante encontrado na janela selecionada.")
            st.stop()

        st.subheader("📄 Contexto selecionado (Top-K)")
        for i, c in enumerate(chunks, 1):
            with st.expander(f"Chunk {i}", expanded=False):
                st.write(c)

        contexto = "\n\n".join(chunks)
        prompt = _build_prompt(ticker, contexto)

        st.markdown("---")
        st.subheader("🧠 Resultado da LLM (JSON)")

        try:
            with st.spinner("Chamando LLM..."):
                raw = _safe_llm_complete(prompt)
        except Exception as e:
            st.error(f"Falha ao chamar LLM: {e}")
            st.stop()

        try:
            parsed = json.loads(raw)
        except Exception:
            st.error("A LLM não retornou JSON válido. Veja abaixo o retorno bruto:")
            st.code(raw)
            st.stop()

        st.json(parsed)

        # Persistência (se store existir)
        try:
            save_patch6_run(
                snapshot_id="manual_run",
                ticker=ticker,
                period_ref="current",
                result=parsed,
            )
        except Exception as e:
            st.caption(f"(Aviso) Não foi possível salvar patch6_run: {e}")

        # Histórico (se store existir)
        st.markdown("---")
        st.subheader("📜 Histórico (últimos 5)")
        try:
            hist = list_patch6_history(ticker, limit=5)
            if hist:
                st.dataframe(hist, use_container_width=True)
            else:
                st.caption("Sem histórico (ou store não configurado).")
        except Exception as e:
            st.caption(f"(Aviso) Não foi possível carregar histórico: {e}")
