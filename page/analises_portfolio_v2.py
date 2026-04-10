from __future__ import annotations

from typing import Any, List

import streamlit as st

from core.portfolio_snapshot_store import get_latest_snapshot
import core.ai_models.llm_client.factory as llm_factory
from core.patch6_report_v2 import render_patch6_report_v2


def _safe_upper(x: Any) -> str:
    return str(x or "").strip().upper()


def render() -> None:
    st.title("📊 Análise de Portfólio V2")
    st.caption("Leitura orientada à decisão, usando o snapshot ativo do sistema.")

    with st.sidebar:
        st.markdown("## Análises de Portfólio V2")
        st.caption("Configurações desta seção")
        analysis_mode_label = st.radio(
            "Modo de análise",
            options=["Rígida", "Flexível"],
            index=0,
            key="portfolio_analysis_mode_sidebar_v2",
            help=(
                "Rígida: usa apenas os dados presentes no sistema. "
                "Flexível: combina os dados do sistema com inferência contextual ampliada da IA."
            ),
        )

    analysis_mode = "rigid" if analysis_mode_label == "Rígida" else "flexible"

    snapshot = get_latest_snapshot()
    if not snapshot:
        st.warning("Nenhum snapshot ativo encontrado. Execute primeiro a Criação de Portfólio.")
        return

    raw_list = snapshot.get("items") or snapshot.get("tickers") or []

    if raw_list and isinstance(raw_list, list) and raw_list and isinstance(raw_list[0], str):
        raw_list = [{"ticker": t} for t in raw_list]

    items = raw_list if isinstance(raw_list, list) else []
    tickers: List[str] = [_safe_upper(it.get("ticker")) for it in items if _safe_upper(it.get("ticker"))]
    tickers = sorted(list(dict.fromkeys(tickers)))

    if not tickers:
        st.warning("O snapshot ativo não possui tickers válidos.")
        return

    analysis_window_months = 36
    analysis_period_ref = "36M"

    st.markdown(
        f"""
        **Carteira detectada:** {len(tickers)} ativo(s)  
        **Janela de análise:** {analysis_window_months} meses  
        **Modo de análise:** {"Rígida" if analysis_mode == "rigid" else "Flexível"}
        """
    )

    render_patch6_report_v2(
        tickers=tickers,
        period_ref=analysis_period_ref,
        llm_factory=llm_factory,
        show_company_details=True,
        analysis_mode=analysis_mode,
    )
