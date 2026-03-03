"""core/patch6_report.py

Renderização profissional do Patch6 (relatório estilo casa de análise) usando dados persistidos em public.patch6_runs.

- Não mexe no pipeline (ingest/chunk/RAG/LLM). Apenas consolida e apresenta.
- Funciona mesmo sem LLM: usa templates + agregações.
- Se um cliente LLM estiver disponível (via llm_factory.get_llm_client()), cria Resumo Executivo e Conclusão com linguagem institucional.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional

import os
import re
import html
import pandas as pd
import streamlit as st
from sqlalchemy import text

from typing import Optional, Any

from core.db_loader import get_supabase_engine


@dataclass
class PortfolioStats:
    fortes: int = 0
    moderadas: int = 0
    fracas: int = 0
    desconhecidas: int = 0

    @property
    def total(self) -> int:
        return self.fortes + self.moderadas + self.fracas + self.desconhecidas

    def label_qualidade(self) -> str:
        # Heurística simples: converte perspectiva em "qualidade" agregada
        if self.total == 0:
            return "—"
        if self.fortes >= max(1, int(0.4 * self.total)):
            return "Alta"
        if self.fracas >= max(1, int(0.4 * self.total)):
            return "Baixa"
        return "Moderada"

    def label_perspectiva(self) -> str:
        if self.total == 0:
            return "—"
        if self.fortes > self.fracas and self.fortes >= self.moderadas:
            return "Construtiva"
        if self.fracas > self.fortes and self.fracas >= self.moderadas:
            return "Cautelosa"
        return "Neutra"
   
    def _safe_call_llm(llm_client: Any, prompt: str) -> Optional[str]:
        try:
            if llm_client is None:
                return None
    
            model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    
            # OpenAI SDK novo
            if hasattr(llm_client, "responses") and hasattr(llm_client.responses, "create") and callable(llm_client.responses.create):
                resp = llm_client.responses.create(model=model, input=prompt)
                txt = getattr(resp, "output_text", None)
                if txt:
                    return txt
                try:
                    return resp.output[0].content[0].text
                except Exception:
                    return str(resp)
    
            # OpenAI SDK legado
            if hasattr(llm_client, "chat") and hasattr(llm_client.chat, "completions") and hasattr(llm_client.chat.completions, "create"):
                resp = llm_client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                )
                return resp.choices[0].message.content
    
            # métodos antigos
            if hasattr(llm_client, "complete") and callable(getattr(llm_client, "complete")):
                return llm_client.complete(prompt)
            if hasattr(llm_client, "chat") and callable(getattr(llm_client, "chat")):
                return llm_client.chat(prompt)
            if hasattr(llm_client, "invoke") and callable(getattr(llm_client, "invoke")):
                return llm_client.invoke(prompt)
            if callable(llm_client):
                return llm_client(prompt)
    
            return None
        except Exception:
            return None


def _load_latest_runs(tickers: List[str], period_ref: str) -> pd.DataFrame:
    tickers = [str(t).strip().upper() for t in (tickers or []) if str(t).strip()]
    if not tickers:
        return pd.DataFrame()

    engine = get_supabase_engine()

    q = text("""
        with ranked as (
            select
                ticker,
                period_ref,
                created_at,
                perspectiva_compra,
                resumo,
                row_number() over (partition by ticker, period_ref order by created_at desc) as rn
            from public.patch6_runs
            where period_ref = :pr and ticker = any(:tks)
        )
        select ticker, period_ref, created_at, perspectiva_compra, resumo
        from ranked
        where rn = 1
        order by ticker asc
    """)

    with engine.connect() as conn:
        df = pd.read_sql_query(q, conn, params={"pr": str(period_ref).strip(), "tks": tickers})
    return df


def _compute_stats(df_latest: pd.DataFrame) -> PortfolioStats:
    stats = PortfolioStats()
    if df_latest is None or df_latest.empty:
        return stats

    for p in df_latest["perspectiva_compra"].fillna("").astype(str).str.strip().str.lower().tolist():
        if p == "forte":
            stats.fortes += 1
        elif p == "moderada":
            stats.moderadas += 1
        elif p == "fraca":
            stats.fracas += 1
        else:
            stats.desconhecidas += 1
    return stats


def _badge(texto: str, tone: str = "neutral") -> str:
    tone_map = {
        "good": "#0ea5e9",
        "warn": "#f59e0b",
        "bad": "#ef4444",
        "neutral": "#94a3b8",
    }
    color = tone_map.get(tone, "#94a3b8")
    return (
        f"<span style='display:inline-block;padding:2px 10px;border-radius:999px;"
        f"border:1px solid {color};color:{color};font-weight:600;font-size:12px'>{texto}</span>"
    )


def _tone_from_perspectiva(p: str) -> str:
    p = (p or "").strip().lower()
    if p == "forte":
        return "good"
    if p == "moderada":
        return "warn"
    if p == "fraca":
        return "bad"
    return "neutral"


def _safe_call_llm(llm_client: Any, prompt: str) -> Optional[str]:
    """
    Wrapper compatível com:
    - OpenAI SDK novo: client.responses.create(...)
    - OpenAI SDK legado: client.chat.completions.create(...)
    - Clientes custom: .complete/.chat/.invoke ou callable
    """
    try:
        if llm_client is None:
            return None

        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

        # 1) OpenAI SDK novo (Responses API)
        if hasattr(llm_client, "responses") and hasattr(llm_client.responses, "create") and callable(llm_client.responses.create):
            resp = llm_client.responses.create(model=model, input=prompt)
            txt = getattr(resp, "output_text", None)
            if txt:
                return txt
            try:
                return resp.output[0].content[0].text
            except Exception:
                return str(resp)

        # 2) OpenAI SDK legado (Chat Completions)
        if hasattr(llm_client, "chat") and hasattr(llm_client.chat, "completions") and hasattr(llm_client.chat.completions, "create"):
            resp = llm_client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            return resp.choices[0].message.content

        # 3) Clientes custom
        if hasattr(llm_client, "complete") and callable(getattr(llm_client, "complete")):
            return llm_client.complete(prompt)
        if hasattr(llm_client, "chat") and callable(getattr(llm_client, "chat")):
            return llm_client.chat(prompt)
        if hasattr(llm_client, "invoke") and callable(getattr(llm_client, "invoke")):
            return llm_client.invoke(prompt)
        if callable(llm_client):
            return llm_client(prompt)

        return None
    except Exception:
        return None
_TAG_RE = re.compile(r"<[^>]+>")

def _strip_html(s: Any) -> str:
    """Remove tags HTML e normaliza espaços (protege layout contra HTML salvo no banco)."""
    if s is None:
        return ""
    txt = str(s)
    txt = _TAG_RE.sub("", txt)          # remove <div>, <span>, etc.
    txt = txt.replace("&nbsp;", " ")
    txt = re.sub(r"\s+\n", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip()

def _esc(s: Any) -> str:
    """Escapa HTML para usar dentro de st.markdown(..., unsafe_allow_html=True)."""
    return html.escape(_strip_html(s))

def render_patch6_report(
    tickers: List[str],
    period_ref: str,
    llm_factory: Optional[Any] = None,
    show_company_details: bool = True,
) -> None:
    """Renderiza relatório profissional do portfólio (Patch6) usando dados já salvos em patch6_runs."""

    st.markdown("# 📘 Relatório de Análise de Portfólio (Patch6)")
    st.caption("Consolidação qualitativa com base em evidências do RAG. Formato institucional (research).")
    st.markdown("""
    <style>
    .p6-cards { margin-top: 12px; }
    
    .p6-card{
      border:1px solid rgba(255,255,255,0.08);
      background:rgba(255,255,255,0.03);
      border-radius:16px;
      padding:16px 18px;
      box-shadow:0 10px 24px rgba(0,0,0,0.25);
      min-height:110px;
    }
    
    .p6-card-label{
      font-size:12px;
      opacity:0.7;
      margin-bottom:6px;
      letter-spacing:0.3px;
    }
    
    .p6-card-value{
      font-size:28px;
      font-weight:900;
      margin-bottom:6px;
    }
    
    .p6-card-extra{
      font-size:12px;
      opacity:0.65;
    }
    </style>
    """, unsafe_allow_html=True)

    df_latest = _load_latest_runs(tickers=tickers, period_ref=period_ref)
    if df_latest.empty:
        st.warning(
            "Não há execuções salvas em patch6_runs para este period_ref e tickers do portfólio. "
            "Rode a LLM e salve os resultados primeiro."
        )
        return

    stats = _compute_stats(df_latest)

    # Scorecard
    qualidade = stats.label_qualidade()
    perspectiva = stats.label_perspectiva()
    cobertura = f"{stats.total}/{len(tickers)}"
    distrib = f"Fortes {stats.fortes} • Moderadas {stats.moderadas} • Fracas {stats.fracas}"
    
    col1, col2, col3, col4 = st.columns(4)
    
    col1.markdown(f"""
    <div class="p6-card">
      <div class="p6-card-label">Qualidade (heurística)</div>
      <div class="p6-card-value">{qualidade}</div>
      <div class="p6-card-extra">Heurística agregada a partir dos sinais do RAG.</div>
    </div>
    """, unsafe_allow_html=True)
    
    col2.markdown(f"""
    <div class="p6-card">
      <div class="p6-card-label">Perspectiva 12m</div>
      <div class="p6-card-value">{perspectiva}</div>
      <div class="p6-card-extra">Direcionalidade consolidada para os próximos 12 meses.</div>
    </div>
    """, unsafe_allow_html=True)
    
    col3.markdown(f"""
    <div class="p6-card">
      <div class="p6-card-label">Cobertura</div>
      <div class="p6-card-value">{cobertura}</div>
      <div class="p6-card-extra">Ativos com evidências suficientes no período analisado.</div>
    </div>
    """, unsafe_allow_html=True)
    
    col4.markdown(f"""
    <div class="p6-card">
      <div class="p6-card-label">Distribuição</div>
      <div class="p6-card-value">{distrib}</div>
      <div class="p6-card-extra">Distribuição de sinais qualitativos no portfólio.</div>
    </div>
    """, unsafe_allow_html=True)

    st.caption("🛈 Como a qualidade é estimada: combinação de (i) cobertura do portfólio, (ii) perspectiva 12m agregada e (iii) distribuição de sinais (forte/moderada/fraca). É uma heurística para orientar leitura — não é recomendação.")

    # Contexto agregado para LLM (opcional)
    contexto_portfolio = "\n".join(
        [
            f"- {row.ticker}: perspectiva={row.perspectiva_compra}; resumo={row.resumo}"
            for row in df_latest.itertuples(index=False)
        ]
    )

    st.markdown("## 🧠 Resumo Executivo")

    llm_client = None
    if llm_factory is not None:
        try:
            llm_client = llm_factory.get_llm_client()
        except Exception:
            llm_client = None

    prompt_exec = f"""
Você é um analista sell-side (research). Escreva um resumo executivo profissional do portfólio com base SOMENTE nos bullets abaixo.
Estruture em 4 blocos curtos:
1) Leitura geral do portfólio
2) Principais oportunidades (2-4 itens)
3) Principais riscos/alertas (2-4 itens)
4) Perspectiva base para 12 meses

Regras:
- linguagem institucional, objetiva
- não invente fatos não citados
- evite jargão excessivo
- não use tabelas

BULLETS:
{contexto_portfolio}
"""

    llm_text = _safe_call_llm(llm_client, prompt_exec)
    if llm_text:
        st.write(llm_text)
    else:
        st.write(
            f"O portfólio apresenta leitura **{stats.label_perspectiva().lower()}** para 12 meses, com distribuição de perspectivas: "
            f"**{stats.fortes}** forte, **{stats.moderadas}** moderada e **{stats.fracas}** fraca (cobertura {stats.total} ativos). "
            "Abaixo, os destaques por empresa no formato de research."
        )

    if show_company_details:
        st.markdown("## 🏢 Relatórios por Empresa")
        for row in df_latest.itertuples(index=False):
            tk = row.ticker
            p = str(row.perspectiva_compra or "").strip().lower()
            badge = _badge((p or "—").upper(), _tone_from_perspectiva(p))

            with st.expander(f"{tk}", expanded=False):
                st.markdown(f"### {tk}  {badge}", unsafe_allow_html=True)
                st.caption(f"Período: {row.period_ref} • Atualizado em: {row.created_at}")

                st.markdown("**Tese (síntese)**")
                tese = _strip_html(row.resumo) or "—"
                
                st.markdown(
                    f"""
                    <div style="
                        border:1px solid rgba(255,255,255,0.08);
                        background:rgba(255,255,255,0.03);
                        border-radius:14px;
                        padding:14px 16px;
                        box-shadow:0 10px 24px rgba(0,0,0,0.18);
                        margin-top:8px;
                        line-height:1.45;
                        ">
                        {_esc(tese).replace("\\n", "<br/>")}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                st.markdown("**Leitura / Direcionalidade**")
                if p == "forte":
                    st.write(
                        "Viés construtivo, com sinais qualitativos favoráveis no recorte analisado. "
                        "Mantém assimetria positiva, com monitoramento de riscos."
                    )
                elif p == "moderada":
                    st.write(
                        "Leitura equilibrada, com pontos positivos e ressalvas. "
                        "Indica acompanhamento de gatilhos (execução, guidance, alocação de capital)."
                    )
                elif p == "fraca":
                    st.write(
                        "Leitura cautelosa, com sinais qualitativos desfavoráveis no recorte analisado. "
                        "Recomenda postura defensiva e foco em mitigação de risco."
                    )
                else:
                    st.write("Leitura inconclusiva por ausência/insuficiência de evidências no recorte analisado.")

    st.markdown("## 🔎 Conclusão Estratégica")
    prompt_conc = f"""
Escreva uma conclusão estratégica (research) para o portfólio, em até 8 linhas, com foco em:
- coerência do conjunto para liquidez e dividendos
- principais alavancas para melhora/deterioração
- recomendação de acompanhamento (o que monitorar nos próximos trimestres)

Use SOMENTE os bullets abaixo (não invente dados).

BULLETS:
{contexto_portfolio}
"""

    llm_conc = _safe_call_llm(llm_client, prompt_conc)
    if llm_conc:
        st.write(llm_conc)
    else:
        st.write(
            "A carteira deve ser acompanhada por gatilhos de execução e sinais de alocação de capital. "
            "Reforce monitoramento de resultados trimestrais, dívida/custo financeiro, e consistência de distribuição de caixa ao acionista, "
            "priorizando ativos com leitura construtiva e reduzindo exposição a teses com viés cauteloso quando houver deterioração recorrente."
        )
