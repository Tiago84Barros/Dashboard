from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

# Patch6 store (cache/persistência)
from core.patch6_store import (
    prepare_patch6_docs_and_keys,
    get_assessment_by_run_key,
    upsert_assessment_and_initiatives,
)

# LLM client do seu projeto
from core.ai_models.llm_client.factory import get_llm_client


# ============================================================
# Helpers locais (test page)
# ============================================================

def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()

def _clip(s: str, n: int = 600) -> str:
    s = (s or "").strip()
    return s[:n] + ("…" if len(s) > n else "")

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if pd.isna(v):
            return None
        return v
    except Exception:
        return None

def _as_list_str(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(i) for i in x if str(i).strip()]
    if isinstance(x, str) and x.strip():
        return [x.strip()]
    return []

def _llm_extract_assess(
    *,
    llm: Any,
    ticker: str,
    empresa: str,
    docs: List[Dict[str, Any]],
    manual_text: str,
) -> Dict[str, Any]:
    """
    Versão TESTE:
      - extrai iniciativas com evidência
      - classifica risco de execução
      - dá rating compra forte/moderada/fraca
    """

    schema_hint = """
{
  "iniciativas": [
    {
      "tipo": "expansao|capex|m&a|desinvestimento|guidance|reestruturacao|eficiencia|regulatorio|outros",
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
Você é um analista buy-side cético e orientado a evidência.
Regras obrigatórias:
- NÃO invente fatos, números, datas, nomes, operações ou guidance não escrito.
- Use APENAS o conteúdo fornecido em 'Documentos' e no 'Texto manual' para extrair iniciativas.
- Toda iniciativa DEVE conter um trecho de evidência.
- Se não houver evidência, retorne iniciativas vazias e explique no resumo.
- A saída DEVE ser JSON válido e seguir exatamente o schema.
""".strip()

    # Contexto “limpo” (evita payload gigante)
    ctx_docs: List[Dict[str, Any]] = []
    for d in (docs or [])[:12]:
        ctx_docs.append(
            {
                "source": f"{d.get('fonte','NA')}|{d.get('tipo','NA')}",
                "date": str(d.get("data") or "NA"),
                "title": str(d.get("titulo") or "NA"),
                "url": str(d.get("url") or ""),
                "text": (str(d.get("raw_text") or "")[:3500]),
            }
        )

    user = f"""
Empresa: {empresa} ({ticker})

Documentos (oficiais) já coletados:
{ctx_docs}

Texto manual (opcional do usuário):
{manual_text.strip() if manual_text else "(vazio)"}

Tarefa:
1) Extraia as iniciativas futuras e planejam. Seja específico.
2) Para cada iniciativa, preencha tipo/horizonte/dependências/impacto/sinal e evidência.
3) Avalie risco de execução (baixo/médio/alto) com pontos a favor/contra/perguntas críticas.
4) Dê rating de compra (forte/moderada/fraca) e um motivo curto.
""".strip()

    out = llm.generate_json(system=system, user=user, schema_hint=schema_hint, context=None)
    return out if isinstance(out, dict) else {}


# ============================================================
# Página Streamlit
# ============================================================

def render() -> None:
    st.markdown("# 🧪 Patch 6 — Modo Teste (RAG + Cache + Persistência)")
    st.caption(
        "Esta página serve para testar rapidamente: "
        "buscar documentos no Supabase, calcular run_key, verificar cache e chamar a LLM sob demanda."
    )

    # ------------------------------------------------------------
    # Inputs
    # ------------------------------------------------------------
    with st.sidebar:
        st.markdown("## Configuração de Teste")

        ticker = st.text_input("Ticker (ex.: BBAS3)", value="BBAS3")
        ticker = _norm_ticker(ticker)

        empresa = st.text_input("Nome (opcional)", value=ticker or "EMPRESA")

        limit_docs = st.number_input("Qtd docs (max)", min_value=1, max_value=30, value=12, step=1)

        # prioridades (só ordena, não exclui)
        tipos_prioritarios = st.text_input(
            "Tipos prioritários (csv)",
            value="fato_relevante,comunicado,release,apresentacao,guidance",
        )
        fontes_prioritarias = st.text_input(
            "Fontes prioritárias (csv)",
            value="CVM,RI",
        )

        # knobs entram no run_key
        max_textos_llm = st.number_input("Máx textos usados pela LLM", min_value=1, max_value=12, value=8, step=1)

        # modelo
        provider = (os.getenv("AI_PROVIDER") or "openai").lower()
        model_env = os.getenv("AI_MODEL") or "gpt-4.1-mini"
        st.write(f"Provider: `{provider}`")
        st.write(f"Modelo: `{model_env}`")

    if not ticker:
        st.warning("Informe um ticker válido.")
        return

    # ------------------------------------------------------------
    # Buscar docs + calcular chaves
    # ------------------------------------------------------------
    tipos_list = [x.strip() for x in (tipos_prioritarios or "").split(",") if x.strip()]
    fontes_list = [x.strip() for x in (fontes_prioritarias or "").split(",") if x.strip()]

    pkg = prepare_patch6_docs_and_keys(
        ticker,
        limit_docs=int(limit_docs),
        tipos_prioritarios=tipos_list,
        fontes_prioritarias=fontes_list,
        provider=provider,
        model=model_env,
        patch_version="patch6_v1",
        knobs={"max_textos_llm": int(max_textos_llm)},
    )

    st.markdown("## 🔑 Identidade do processamento")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Ticker", pkg["ticker"])
    with c2:
        st.metric("Docs usados", pkg["docs_count"])
    with c3:
        st.metric("Última data doc", pkg["last_doc_date"] or "—")
    with c4:
        st.metric("Cache key (run_key)", pkg["run_key"][:10] + "…")

    with st.expander("Ver docs_hash / run_key completos", expanded=False):
        st.code(f"docs_hash: {pkg['docs_hash']}\nrun_key:  {pkg['run_key']}")

    # ------------------------------------------------------------
    # Preview docs
    # ------------------------------------------------------------
    st.markdown("## 📚 Documentos encontrados")
    docs = pkg["docs"] or []
    if not docs:
        st.warning(
            "Nenhum documento encontrado em `public.docs_corporativos` para esse ticker.\n\n"
            "Verifique se você já inseriu docs (via ingest/RI/manual) e se o ticker está normalizado (sem .SA)."
        )
    else:
        rows = []
        for d in docs:
            rows.append(
                {
                    "data": d.get("data"),
                    "fonte": d.get("fonte"),
                    "tipo": d.get("tipo"),
                    "titulo": d.get("titulo"),
                    "url": d.get("url"),
                    "doc_hash": (d.get("doc_hash") or "")[:10] + "…",
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

        with st.expander("Preview (texto)", expanded=False):
            for i, d in enumerate(docs[:8], start=1):
                st.markdown(
                    f"**{i}. {d.get('titulo') or 'Sem título'}**  \n"
                    f"`{d.get('fonte','NA')} | {d.get('tipo','NA')} | {d.get('data','NA')}`"
                )
                if d.get("url"):
                    st.caption(d.get("url"))
                st.write(_clip(str(d.get("raw_text") or ""), 900))
                st.divider()

    # ------------------------------------------------------------
    # Cache hit?
    # ------------------------------------------------------------
    st.markdown("## 🧠 Cache (Supabase)")
    cached = None
    try:
        cached = get_assessment_by_run_key(pkg["run_key"])
    except Exception as e:
        st.error(f"Falha ao consultar cache (patch6_assessments): {type(e).__name__}: {e}")

    if cached and isinstance(cached, dict) and cached.get("assessment"):
        a = cached["assessment"]
        st.success("✅ Cache HIT: já existe avaliação salva para esse run_key.")

        cc1, cc2, cc3, cc4 = st.columns(4)
        with cc1:
            st.metric("rating", str(a.get("rating_compra") or "—"))
        with cc2:
            st.metric("risco", str(a.get("risco_execucao") or "—"))
        with cc3:
            st.metric("docs_count", int(a.get("docs_count") or 0))
        with cc4:
            st.metric("updated_at", str(a.get("updated_at") or a.get("created_at") or "—"))

        st.markdown("**Motivo:**")
        st.write(str(a.get("motivo_compra") or ""))

        with st.expander("Iniciativas salvas", expanded=False):
            inits = cached.get("initiatives") or []
            if not inits:
                st.info("Sem iniciativas salvas.")
            else:
                st.dataframe(pd.DataFrame(inits), use_container_width=True)

        with st.expander("JSON completo salvo (llm_json)", expanded=False):
            st.json(a.get("llm_json") or {})
    else:
        st.info("Cache MISS: não existe avaliação salva para esse run_key ainda.")

    # ------------------------------------------------------------
    # Texto manual + botões
    # ------------------------------------------------------------
    st.markdown("## ✍️ Texto manual (opcional)")
    manual_text = st.text_area(
        "Cole aqui qualquer trecho do RI/CVM que você quer forçar no teste (opcional).",
        value="",
        height=140,
        placeholder="Ex.: trecho de guidance, plano de investimento, expansão, desinvestimento, capex, etc.",
    )

    colA, colB, colC = st.columns([1.2, 1.2, 2.0])
    with colA:
        run_llm = st.button("🚀 Rodar LLM agora", use_container_width=True)
    with colB:
        force = st.checkbox("Forçar reprocessamento (ignorar cache)", value=False)
    with colC:
        st.caption("A LLM só roda quando você clicar no botão. Sem clique, nada é executado além das queries do Supabase.")

    # ------------------------------------------------------------
    # Execução sob demanda
    # ------------------------------------------------------------
    if run_llm:
        if (cached and not force):
            st.warning("Cache já existe. Marque **Forçar reprocessamento** se quiser rodar novamente.")
            return

        if not docs and not manual_text.strip():
            st.error("Sem docs e sem texto manual. Insira ao menos um deles para testar a LLM.")
            return

        # inicializa LLM
        try:
            llm = get_llm_client()
        except Exception as e:
            st.error(f"Não consegui inicializar o cliente LLM: {type(e).__name__}: {e}")
            return

        # limita docs usados
        docs_for_llm = docs[: int(max_textos_llm)] if docs else []

        with st.spinner("Chamando LLM e gerando avaliação..."):
            try:
                out = _llm_extract_assess(
                    llm=llm,
                    ticker=ticker,
                    empresa=empresa,
                    docs=docs_for_llm,
                    manual_text=manual_text or "",
                )
            except Exception as e:
                st.error(f"Falha ao chamar LLM: {type(e).__name__}: {e}")
                return

        st.markdown("## ✅ Saída da LLM (JSON)")
        st.json(out)

        # --------------------------------------------------------
        # Normaliza campos e grava
        # --------------------------------------------------------
        iniciativas = out.get("iniciativas", []) if isinstance(out, dict) else []
        aval = out.get("avaliacao_execucao", {}) if isinstance(out, dict) else {}
        rating = out.get("rating_compra", {}) if isinstance(out, dict) else {}

        risco_exec = str(aval.get("risco_execucao", "nao_informado") or "nao_informado").strip()
        rating_compra = str(rating.get("rating", "nao_informado") or "nao_informado").strip()
        motivo_compra = str(rating.get("motivo", "") or "").strip()
        resumo_1 = str(out.get("resumo_1_paragrafo", "") or "").strip()

        pontos_a_favor = _as_list_str(aval.get("pontos_a_favor"))
        pontos_contra = _as_list_str(aval.get("pontos_contra"))
        perguntas_criticas = _as_list_str(aval.get("perguntas_criticas"))

        # nesta fase de teste, você não está integrando score -> então salva score_nulos
        # (quando integrar no Patch6 real, você preenche score_regua/ajuste/score_final)
        score_regua = None
        ajuste_pp = None
        score_final = None
        fator_peso = None  # opcional; pode ser calculado depois

        try:
            aid = upsert_assessment_and_initiatives(
                run_key=pkg["run_key"],
                ticker=ticker,
                provider=provider,
                model=model_env,
                docs_hash=pkg["docs_hash"],
                docs_count=int(pkg["docs_count"]),
                last_doc_date_iso=pkg["last_doc_date"],

                score_regua_0_100=score_regua,
                ajuste_ia_pp=ajuste_pp,
                score_final_0_100=score_final,
                risco_execucao=risco_exec,

                rating_compra=rating_compra,
                motivo_compra=motivo_compra,
                fator_peso_aporte=fator_peso,

                resumo_1_paragrafo=resumo_1,
                pontos_a_favor=pontos_a_favor,
                pontos_contra=pontos_contra,
                perguntas_criticas=perguntas_criticas,

                llm_json=out,
                iniciativas=iniciativas if isinstance(iniciativas, list) else [],
            )
            st.success(f"Gravado no Supabase com sucesso. assessment_id={aid}")
            st.info("Agora recarregue a página: deve dar Cache HIT para o mesmo run_key.")
        except Exception as e:
            st.error(f"Falha ao gravar no Supabase: {type(e).__name__}: {e}")
            return
