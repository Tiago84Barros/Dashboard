# configuracoes.py
from __future__ import annotations

import streamlit as st

from core.db_supabase import get_engine
from core.macro_audit import AuditLogger
from core.macro_bcb_ingest import run as macro_run


def render_configuracoes():
    st.header("Configurações do Sistema")

    st.subheader("Atualização de Dados Macro Econômicos (BCB)")

    st.markdown(
        """
        Este processo executa **todo o pipeline macroeconômico**:
        1. Download direto das séries do Banco Central (SGS)
        2. Gravação no RAW (`cvm.macro_bcb`)
        3. Consolidação mensal (`cvm.info_economica_mensal`)
        
        Todas as etapas são **auditadas em tempo real** abaixo.
        """
    )

    engine = get_engine()

    # Área visual de auditoria
    audit_box = st.empty()
    audit = AuditLogger(title="Auditoria Macro (BCB)").bind(audit_box)

    status_box = st.empty()
    progress_bar = st.progress(0)

    def progress_cb(msg: str):
        status_box.write(msg)

    st.divider()

    if st.button("Atualizar Macro (BCB)", use_container_width=True):
        try:
            audit.log("UI: botão 'Atualizar Macro (BCB)' acionado.")
            progress_bar.progress(5)

            macro_run(
                engine,
                progress_cb=progress_cb,
                audit_cb=audit.log,  # 🔥 auditoria linha a linha
            )

            progress_bar.progress(100)
            audit.log("UI: processo concluído com sucesso.")
            st.success("Atualização macroeconômica finalizada. Verifique a auditoria acima.")

        except Exception as e:
            audit.log(f"UI: ERRO FATAL → {repr(e)}")
            st.error("Falha na atualização macroeconômica. Veja os detalhes no log acima.")

    st.divider()

    st.caption(
        "Observação: este botão executa **todas** as atualizações macroeconômicas. "
        "Não há múltiplos gatilhos ou fluxos paralelos."
    )
