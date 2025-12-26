from __future__ import annotations

import importlib
from typing import Callable, Optional

import pandas as pd
import streamlit as st
from sqlalchemy import text

from core.db_supabase import get_engine

# mantém o seu CVM como já está no projeto (não altero assinatura aqui)
from core.cvm_sync import apply_update

# NOVO: auditoria macro centralizada
from core.macro_audit import run_macro_audit  # você disse que este arquivo existe


def render() -> None:
    st.title("Configurações")

    engine = get_engine()

    # =========================
    # CVM (mantém como estava)
    # =========================
    st.subheader("Atualizar CVM (DFP/ITR)")

    col1, col2 = st.columns(2)
    with col1:
        ano_inicial = st.number_input("Ano inicial", min_value=2000, max_value=2100, value=2010, step=1)
    with col2:
        ano_final = st.number_input("Ano final", min_value=2000, max_value=2100, value=2025, step=1)

    dfp_batch_years = st.number_input("DFP por clique (anos)", min_value=1, max_value=10, value=1, step=1)
    itr_batch_qtrs = st.number_input("ITR por clique (trimestres)", min_value=1, max_value=20, value=1, step=1)

    # =========================
    # MACRO (auditável)
    # =========================
    st.subheader("Macro (BCB) — Auditoria passo a passo")

    audit_tab, preview_tab = st.tabs(["Auditoria (logs)", "Diagnóstico (SQL)"])

    with audit_tab:
        audit_log = st.empty()
        summary = st.empty()

        def log(msg: str) -> None:
            # render incremental
            cur = st.session_state.get("_macro_audit_logs", [])
            cur.append(msg)
            st.session_state["_macro_audit_logs"] = cur
            audit_log.write("\n".join(cur))

        # Botão único (sem criar 2 botões separados)
        if st.button("Atualizar banco (CVM normal + Macro auditável)", use_container_width=True):
            st.session_state["_macro_audit_logs"] = []

            cvm_ok = True
            macro_ok = True

            # ---- 1) CVM roda normal (sem auditoria detalhada aqui)
            try:
                log("CVM: iniciando apply_update(...)")
                # mantém parâmetros já estabelecidos
                apply_update(
                    engine,
                    start_year=int(ano_inicial),
                    end_year=int(ano_final),
                    dfp_batch_years=int(dfp_batch_years),
                    itr_batch_quarters=int(itr_batch_qtrs),
                    # se o seu apply_update aceitar callback, ok; se não aceitar, remova
                )
                log("CVM: OK")
            except Exception as e:
                cvm_ok = False
                log(f"CVM: FALHOU — {e}")

            # ---- 2) Macro: auditoria completa
            try:
                log("MACRO: iniciando run_macro_audit(...)")
                run_macro_audit(engine, progress_cb=log)
                log("MACRO: OK")
            except Exception as e:
                macro_ok = False
                log(f"MACRO: FALHOU — {e}")

            # ---- resumo
            with summary:
                st.markdown("## Resumo")
                st.success("CVM: OK") if cvm_ok else st.error("CVM: FALHOU (ver logs)")
                st.success("MACRO: OK") if macro_ok else st.error("MACRO: FALHOU (ver logs)")

    # =========================
    # Diagnóstico rápido (SQL)
    # =========================
    with preview_tab:
        st.caption("Diagnóstico direto no Supabase. Ajuda a localizar gargalo (ingest vs wide).")

        if st.button("Rodar diagnóstico Macro agora", use_container_width=True):
            # 1) contagem por série no RAW
            st.markdown("### RAW — contagem por série (cvm.macro_bcb)")
            q1 = """
                select series_name, count(*) as n, count(valor) as n_valor
                from cvm.macro_bcb
                group by series_name
                order by n desc;
            """
            st.dataframe(pd.read_sql(text(q1), engine), use_container_width=True)

            # 2) preenchimento das colunas do wide mensal
            st.markdown("### WIDE — preenchimento por coluna (cvm.info_economica_mensal)")
            q2 = """
                select
                  count(*) as linhas,
                  count(selic) as selic_ok,
                  count(cambio) as cambio_ok,
                  count(ipca) as ipca_ok,
                  count(icc) as icc_ok,
                  count(pib) as pib_ok,
                  count(balanca_comercial) as balanca_ok
                from cvm.info_economica_mensal;
            """
            st.dataframe(pd.read_sql(text(q2), engine), use_container_width=True)

            # 3) amostra do wide mensal
            st.markdown("### WIDE — amostra (últimos 24 meses)")
            q3 = """
                select *
                from cvm.info_economica_mensal
                order by data desc
                limit 24;
            """
            st.dataframe(pd.read_sql(text(q3), engine), use_container_width=True)
