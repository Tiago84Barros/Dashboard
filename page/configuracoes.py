# page/configuracoes.py
from __future__ import annotations

import streamlit as st

from core.db.engine import get_engine
from core.db.admin import healthcheck, row_count, last_update_date

# pipelines
from cvm.cvm_dfp_ingest import run as run_dfp
from cvm.cvm_itr_ingest import run as run_itr
from cvm.macro_bcb_ingest import run as run_macro


def render():
    st.header("Configurações e Banco de Dados (Supabase)")

    # Engine (recurso)
    @st.cache_resource
    def _engine():
        return get_engine()

    try:
        engine = _engine()
    except Exception as e:
        st.error(f"Não foi possível criar engine do Supabase: {e}")
        st.stop()

    # Healthcheck
    h = healthcheck(engine)
    if h.ok:
        st.success(h.message)
        st.caption(h.server_version or "")
    else:
        st.error(h.message)
        st.stop()

    st.subheader("Status das Tabelas")
    cols = st.columns(3)
    with cols[0]:
        st.metric("DFP (linhas)", row_count(engine, "cvm.demonstracoes_financeiras"))
    with cols[1]:
        st.metric("ITR (linhas)", row_count(engine, "cvm.multiplos"))
    with cols[2]:
        st.metric("Macro (linhas)", row_count(engine, "cvm.info_economica"))

    st.write("Última data (quando aplicável):")
    st.write({
        "DFP.max(data)": last_update_date(engine, "cvm.demonstracoes_financeiras", "data"),
        "Macro.max(data)": last_update_date(engine, "cvm.info_economica", "data"),
    })

    st.divider()
    st.subheader("Atualizações (ETL)")

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Atualizar DFP"):
            run_dfp(engine=engine)
            st.success("DFP atualizado.")
    with c2:
        if st.button("Atualizar ITR"):
            run_itr(engine=engine)
            st.success("ITR atualizado.")
    with c3:
        if st.button("Atualizar Macro"):
            run_macro(engine=engine)
            st.success("Macro atualizado.")
