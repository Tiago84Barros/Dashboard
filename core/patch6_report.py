"""core/patch6_report.py

Renderização profissional do Patch6 (relatório estilo casa de análise) usando dados persistidos em public.patch6_runs.

- Não mexe no pipeline (ingest/chunk/RAG/LLM). Apenas consolida e apresenta.
- Funciona mesmo sem LLM: usa templates + agregações.
- Se um cliente LLM estiver disponível (via llm_factory.get_llm_client()), cria Resumo Executivo e Conclusão com linguagem institucional.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional

import pandas as pd
import streamlit as st
from sqlalchemy import text

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
    """Tenta chamar um cliente LLM sem acoplar ao SDK específico."""
    try:
        if llm_client is None:
            return None
        if hasattr(llm_client, "complete") and callable(getattr(llm_client, "complete")):
            return llm_client.complete(prompt)
        if hasattr(llm_client, "chat") and callable(getattr(llm_client, "chat")):
            return llm_client.chat(prompt)
        if hasattr(llm_client, "invoke") and callable(getattr(llm_client, "invoke")):
            return llm_client.invoke(prompt)
        if callable(llm_client):
            return llm_client(prompt)
    except Exception:
        return None
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


def render_patch6_report(
    tickers: List[str],
    period_ref: str,
    llm_factory: Optional[Any] = None,
    show_company_details: bool = True,
) -> None:
    """Renderiza relatório profissional do portfólio (Patch6) usando dados já salvos em patch6_runs."""

    st.markdown("# 📘 Relatório de Análise de Portfólio (Patch6)")
    st.caption("Consolidação qualitativa com base em evidências do RAG. Formato institucional (research).")

    df_latest = _load_latest_runs(tickers=tickers, period_ref=period_ref)
    if df_latest.empty:
        st.warning(
            "Não há execuções salvas em patch6_runs para este period_ref e tickers do portfólio. "
            "Rode a LLM e salve os resultados primeiro."
        )
        return

    stats = _compute_stats(df_latest)

    # Scorecard
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Qualidade (heurística)", stats.label_qualidade())
    with c2:
        st.metric("Perspectiva 12m", stats.label_perspectiva())
    with c3:
        st.metric("Cobertura", f"{stats.total}/{len(tickers)}")
    with c4:
        st.metric("Distribuição", f"F {stats.fortes} | M {stats.moderadas} | Fr {stats.fracas}")

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

    st.markdown("## 🧾 Sumário do Portfólio")
    view = df_latest.copy()
    view["sinal"] = view["perspectiva_compra"].fillna("").apply(
        lambda x: _badge(str(x).upper() if x else "—", _tone_from_perspectiva(str(x)))
    )
    view = view[["ticker", "period_ref", "created_at", "sinal", "resumo"]].rename(
        columns={"created_at": "atualizado_em", "resumo": "resumo"}
    )
    st.markdown(view.to_html(escape=False, index=False), unsafe_allow_html=True)

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
                st.write(row.resumo or "—")

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
