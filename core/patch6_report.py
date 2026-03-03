
# core/patch6_report.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

import streamlit as st

from core.portfolio_snapshot_store import PortfolioSnapshot
from core.patch6_runs_store import load_patch6_runs


def _pill(label: str, value: str) -> str:
    return f"""<span class="p6-pill"><b>{label}:</b> {value}</span>"""


def _css() -> None:
    st.markdown(
        """
<style>
.p6-wrap {border:1px solid rgba(255,255,255,0.08); border-radius:18px; padding:18px; background:rgba(255,255,255,0.03);}
.p6-title {font-size:32px; font-weight:800; margin:0 0 6px 0;}
.p6-sub {opacity:.8; margin:0 0 14px 0;}
.p6-grid {display:grid; grid-template-columns: repeat(12, 1fr); gap:12px; margin-top:10px;}
.p6-card {grid-column: span 12; border:1px solid rgba(255,255,255,.08); border-radius:16px; padding:14px 14px; background:rgba(0,0,0,.25);}
@media(min-width: 980px){ .p6-card {grid-column: span 6;} }
.p6-h {display:flex; align-items:center; justify-content:space-between; gap:10px;}
.p6-ticker {font-size:18px; font-weight:800; letter-spacing:.3px;}
.p6-meta {opacity:.8; font-size:12px;}
.p6-pills {display:flex; flex-wrap:wrap; gap:8px; margin-top:10px;}
.p6-pill {display:inline-block; padding:6px 10px; border-radius:999px; border:1px solid rgba(255,255,255,.10); background:rgba(255,255,255,.03); font-size:12px;}
.p6-body {margin-top:10px; line-height:1.45; opacity:.95;}
.p6-evi {margin-top:10px; font-size:12px; opacity:.75;}
</style>
""",
        unsafe_allow_html=True,
    )


def render_saved_report(snapshot: PortfolioSnapshot, period_ref: Optional[str]) -> None:
    """Renderiza o relatório usando o que já está salvo em patch6_runs."""
    _css()

    tickers = snapshot.tickers or []
    if not tickers:
        st.warning("Snapshot não possui tickers. Refaça a criação do portfólio.")
        return

    runs = load_patch6_runs(snapshot_id=snapshot.id, tickers=tickers, period_ref=period_ref)

    st.markdown(
        f"""
<div class="p6-wrap">
  <div class="p6-title">📘 Relatório de Análise de Portfólio (Patch6)</div>
  <div class="p6-sub">Consolidação qualitativa baseada em evidências do RAG • Snapshot <b>{snapshot.id}</b></div>
</div>
""",
        unsafe_allow_html=True,
    )

    if not runs:
        st.info("Não há execuções salvas em patch6_runs para este snapshot/tickers. Rode a LLM e salve os resultados.")
        return

    st.markdown('<div class="p6-grid">', unsafe_allow_html=True)

    for r in sorted(runs, key=lambda x: x.get("ticker", "")):
        ticker = r.get("ticker", "")
        pref = r.get("period_ref", "")
        perspectiva = r.get("perspectiva_compra") or (r.get("result") or {}).get("perspectiva_compra") or "-"
        resumo = r.get("resumo") or (r.get("result") or {}).get("resumo") or ""

        # optional metrics from result_json
        res = r.get("result") or {}
        cobertura = res.get("cobertura") or res.get("coverage") or None
        distribuicao = res.get("distribuicao") or res.get("distribution") or None
        qualidade = res.get("qualidade") or res.get("quality") or None

        pills = []
        if qualidade is not None:
            pills.append(_pill("Qualidade", str(qualidade)))
        if cobertura is not None:
            pills.append(_pill("Cobertura", str(cobertura)))
        if distribuicao is not None:
            pills.append(_pill("Distribuição", str(distribuicao)))
        pills.append(_pill("Perspectiva", str(perspectiva)))
        pills.append(_pill("period_ref", str(pref)))

        st.markdown(
            f"""
<div class="p6-card">
  <div class="p6-h">
    <div class="p6-ticker">🧠 {ticker}</div>
    <div class="p6-meta">Atualizado em {str(r.get("created_at") or "")[:19]}</div>
  </div>
  <div class="p6-pills">{''.join(pills)}</div>
  <div class="p6-body">{(resumo or '').replace('\n','<br>')}</div>
</div>
""",
            unsafe_allow_html=True,
        )

    st.markdown("</div>", unsafe_allow_html=True)
