from __future__ import annotations

import streamlit as st
from sqlalchemy.engine import Engine

from core.db_supabase import get_engine
from core.cvm_sync import apply_update
from cvm.multiplos_sync_universe import rebuild_multiplos_universe


# =============================================================================
# Página de Configurações
# =============================================================================

def render() -> None:
    st.header("Configurações do Sistema")

    st.markdown(
        """
        Esta seção executa **toda a cadeia de atualização do banco**:
        
        1. Demonstrações financeiras (CVM – DFP / ITR)  
        2. Preços de ações (mensal + último pregão do ano)  
        3. Recalculo de múltiplos para todo o universo  

        Use o **Modo Seguro** para evitar sobrecarga.
        """
    )

    # ──────────────────────────────────────────────────────────
    # Controles
    # ──────────────────────────────────────────────────────────

    modo_seguro = st.checkbox(
        "Modo seguro (limitar tickers por execução)",
        value=True,
        help="Recomendado em produção para evitar timeout e rate limit.",
    )

    max_tickers = st.number_input(
        "Máximo de tickers por execução (modo seguro)",
        min_value=10,
        max_value=500,
        value=150,
        step=10,
        disabled=not modo_seguro,
    )

    st.divider()

    # ──────────────────────────────────────────────────────────
    # Botão principal
    # ──────────────────────────────────────────────────────────

    if st.button("Atualizar banco (tudo)", use_container_width=True):
        engine: Engine = get_engine()

        log_box = st.container()
        progress = st.progress(0)

        try:
            # =====================================================
            # 1) CVM – DFP / ITR
            # =====================================================
            with log_box:
                st.info("Iniciando sincronismo CVM (DFP / ITR)...")

            progress.progress(10)

            apply_update(
                engine,
                update_cvm=True,
                update_prices=False,
                update_multiplos=False,
            )

            with log_box:
                st.success("CVM sincronizado com sucesso.")

            progress.progress(35)

            # =====================================================
            # 2) Preços – mensal + anual (2010 → hoje)
            # =====================================================
            with log_box:
                st.info("Atualizando preços (mensal + último pregão do ano)...")

            progress.progress(50)

            apply_update(
                engine,
                update_cvm=False,
                update_prices=True,
                update_multiplos=False,
                modo_seguro=modo_seguro,
                max_tickers=max_tickers if modo_seguro else None,
            )

            with log_box:
                st.success("Preços atualizados com sucesso.")

            progress.progress(75)

            # =====================================================
            # 3) Múltiplos – universo completo
            # =====================================================
            with log_box:
                st.info("Recalculando múltiplos do universo...")

            progress.progress(85)

            result = rebuild_multiplos_universe(engine)

            if not result.get("ok"):
                raise RuntimeError(result.get("error", "Erro desconhecido ao recalcular múltiplos."))

            with log_box:
                st.success(f"Múltiplos recalculados com sucesso ({result['rows']} registros).")

            progress.progress(100)

            st.success("Atualização completa finalizada com sucesso.")

        except Exception as e:
            st.error(f"Falha ao atualizar banco: {e}")
