# core/patch6_report.py
from __future__ import annotations

import json
import html
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

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
        # Heurística agregada: usa a distribuição de perspectivas
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


def _load_latest_runs(
    tickers: Sequence[str],
    period_ref: Optional[str],
) -> pd.DataFrame:
    """
    Carrega a última execução salva em public.patch6_runs.

    - Se period_ref informado: traz somente aquele período.
    - Se period_ref vazio/None: traz o "último disponível" por ticker (independente do trimestre).
    """
    tks = [str(t).strip().upper() for t in (tickers or []) if str(t).strip()]
    if not tks:
        return pd.DataFrame()

    engine = get_supabase_engine()

    if period_ref and str(period_ref).strip():
        q = text(
            """
            select snapshot_id, ticker, period_ref, created_at, perspectiva_compra, resumo, result_json
            from public.patch6_runs
            where ticker = any(:tks)
              and period_ref = :pr
            order by created_at desc
            """
        )
        params = {"tks": tks, "pr": str(period_ref).strip()}
    else:
        # PostgreSQL: pega o mais recente por ticker
        q = text(
            """
            select distinct on (ticker)
                snapshot_id, ticker, period_ref, created_at, perspectiva_compra, resumo, result_json
            from public.patch6_runs
            where ticker = any(:tks)
            order by ticker, created_at desc
            """
        )
        params = {"tks": tks}

    with engine.connect() as conn:
        return pd.read_sql_query(q, conn, params=params)


def _safe_json_loads(s: Any) -> Dict[str, Any]:
    if s is None:
        return {}
    if isinstance(s, dict):
        return s
    try:
        return json.loads(s)
    except Exception:
        return {}


def _inject_css() -> None:
    st.markdown(
        """
        <style>
        .p6-wrap { margin-top: 0.25rem; }
        .p6-header { display:flex; justify-content:space-between; align-items:flex-end; gap: 12px; }
        .p6-title { font-size: 1.2rem; font-weight: 800; margin: 0; }
        .p6-sub { color: rgba(255,255,255,0.7); margin: 0.15rem 0 0 0; font-size: 0.9rem; }
        .p6-grid { display:grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 10px; margin: 12px 0 6px 0; }
        .p6-card { background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,0.10);
                   border-radius: 16px; padding: 12px 12px; }
        .p6-k { font-size: 0.78rem; color: rgba(255,255,255,0.65); margin-bottom: 4px; }
        .p6-v { font-size: 1.05rem; font-weight: 800; }
        .p6-company { background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.09);
                      border-radius: 18px; padding: 14px; margin: 10px 0; }
        .p6-row { display:flex; justify-content:space-between; align-items:center; gap: 10px; flex-wrap: wrap; }
        .p6-tk { font-size: 1.05rem; font-weight: 900; letter-spacing: 0.2px; }
        .p6-pill { display:inline-block; padding: 4px 10px; border-radius: 999px;
                   border:1px solid rgba(255,255,255,0.18); background: rgba(255,255,255,0.06);
                   font-size: 0.78rem; color: rgba(255,255,255,0.85); }
        .p6-md { margin-top: 10px; }
        @media (max-width: 900px) { .p6-grid { grid-template-columns: repeat(2, minmax(0,1fr)); } }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_patch6_report(
    tickers: Sequence[str],
    period_ref: Optional[str],
    llm_factory: Any = None,
    show_company_details: bool = True,
) -> None:
    """
    Renderiza o relatório Patch6 com base no que está salvo em public.patch6_runs.

    Observação: period_ref é opcional. Se vazio, mostra o último disponível por ticker.
    """
    _inject_css()

    df = _load_latest_runs(tickers=tickers, period_ref=period_ref)
    if df is None or df.empty:
        st.info("Nenhum relatório salvo encontrado para os tickers selecionados.")
        return

    stats = _compute_stats(df)

    # Header
    pr_label = (str(period_ref).strip() if period_ref else "Último disponível")
    st.markdown(
        f"""
        <div class="p6-wrap">
          <div class="p6-header">
            <div>
              <div class="p6-title">Relatório institucional (Patch6)</div>
              <div class="p6-sub">Período: <b>{pr_label}</b> • Fonte: patch6_runs</div>
            </div>
            <div class="p6-pill">Empresas: {len(df)}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Cards
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(f"<div class='p6-card'><div class='p6-k'>Qualidade (agregada)</div><div class='p6-v'>{stats.label_qualidade()}</div></div>", unsafe_allow_html=True)
    c2.markdown(f"<div class='p6-card'><div class='p6-k'>Perspectiva (agregada)</div><div class='p6-v'>{stats.label_perspectiva()}</div></div>", unsafe_allow_html=True)
    c3.markdown(f"<div class='p6-card'><div class='p6-k'>Fortes</div><div class='p6-v'>{stats.fortes}</div></div>", unsafe_allow_html=True)
    c4.markdown(f"<div class='p6-card'><div class='p6-k'>Moderadas / Fracas</div><div class='p6-v'>{stats.moderadas} / {stats.fracas}</div></div>", unsafe_allow_html=True)

    if not show_company_details:
        return

    st.markdown("### Empresas")
    # Ordena por ticker
    df = df.sort_values(["ticker", "created_at"], ascending=[True, False])

    for _, row in df.iterrows():
        tk = str(row.get("ticker") or "").strip().upper()
        perspectiva = str(row.get("perspectiva_compra") or "—").strip()
        created_at = row.get("created_at")
        resumo = str(row.get("resumo") or "").strip()
        result = _safe_json_loads(row.get("result_json"))

        evidencias = result.get("evidencias_total") or result.get("n_evidencias") or result.get("evidencias") or ""
        if isinstance(evidencias, list):
            evidencias = len(evidencias)

        st.markdown(
            f"""
            <div class="p6-company">
              <div class="p6-row">
                <div class="p6-tk">{tk}</div>
                <div class="p6-row">
                  <span class="p6-pill">Perspectiva: <b>{perspectiva}</b></span>
                  <span class="p6-pill">Evidências: <b>{evidencias if evidencias != '' else '—'}</b></span>
                </div>
              </div>
              <div class="p6-md">
                {("<div style='color:rgba(255,255,255,0.9); font-size:0.95rem; line-height:1.35'>" + html.escape(resumo) + "</div>") if resumo else "<div style='color:rgba(255,255,255,0.65)'>Sem resumo salvo.</div>"}
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Detalhes estruturados (se existirem)
        detalhes = result.get("topicos") or result.get("blocos") or None
        if isinstance(detalhes, dict):
            with st.expander(f"Detalhes (tópicos) — {tk}", expanded=False):
                for k, v in detalhes.items():
                    st.markdown(f"**{k}**")
                    if isinstance(v, list):
                        for item in v[:20]:
                            st.write(f"• {item}")
                    else:
                        st.write(str(v))
