from __future__ import annotations

import io
import os
import traceback
from contextlib import redirect_stdout, redirect_stderr

import streamlit as st


def render() -> None:
    st.header("Configurações")
    st.caption(
        "Use esta seção para executar rotinas de atualização das tabelas no Supabase. "
        "Nesta primeira etapa, validaremos apenas a carga de demonstrações anuais (DFP)."
    )

    st.subheader("Atualização de Base")
    st.write("**Ordem recomendada (parcial):**\n1) Demonstrações completas (DFP/anual)")

    st.markdown("### Diagnóstico rápido")
    st.write("SUPABASE_DB_URL definida?", bool(os.getenv("SUPABASE_DB_URL")))

    st.divider()

    # trava simples para evitar duplo clique
    if "job_dfp_running" not in st.session_state:
        st.session_state["job_dfp_running"] = False

    col1, col2 = st.columns([1, 2], gap="large")

    with col1:
        run_dfp = st.button(
            "Atualizar Demonstrações Completas (DFP)",
            use_container_width=True,
            disabled=st.session_state["job_dfp_running"],
        )

    with col2:
        st.info(
            "Este botão executa o script **pickup/dados_cvm_dfp.py** para baixar os DFP da CVM, "
            "consolidar e gravar em **public.Demonstracoes_Financeiras** no Supabase.\n\n"
            "Requisitos: configurar **SUPABASE_DB_URL** em Secrets/Env Vars."
        )

    with st.expander("Ações de manutenção", expanded=False):
        if st.button("Resetar trava do botão"):
            st.session_state["job_dfp_running"] = False
            st.success("Trava resetada.")
            st.rerun()

    st.markdown("### Logs da execução")
    log_out = st.empty()
    log_err = st.empty()

    if run_dfp:
        st.session_state["job_dfp_running"] = True

        # Pré-check obrigatório
        if not os.getenv("SUPABASE_DB_URL"):
            st.error("SUPABASE_DB_URL não está definida. Configure em Secrets/Env Vars e tente novamente.")
            st.session_state["job_dfp_running"] = False
            st.stop()

        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()

        try:
            with st.status("Executando carga DFP (pode demorar alguns minutos)...", expanded=True) as status:
                status.write("Importando módulo `pickup.dados_cvm_dfp` …")

                with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
                    from pickup import dados_cvm_dfp

                    status.write("Executando `dados_cvm_dfp.main()` …")
                    dados_cvm_dfp.main()

                status.update(label="Execução finalizada.", state="complete")

            out = stdout_buf.getvalue().strip()
            err = stderr_buf.getvalue().strip()

            if out:
                log_out.code(out, language="text")
            else:
                log_out.info("Nenhum log foi produzido em stdout.")

            if err:
                log_err.warning("Saída em stderr (avisos/erros):")
                log_err.code(err, language="text")
            else:
                log_err.empty()

            st.success("Carga DFP concluída (sem exceções Python).")

            # Se você usa cache_data em queries, limpar ajuda a refletir novos dados
            try:
                st.cache_data.clear()
            except Exception:
                pass

        except Exception as e:
            st.error("Falha ao executar a carga DFP. Traceback completo:")
            st.code("".join(traceback.format_exception(type(e), e, e.__traceback__)), language="text")

            out = stdout_buf.getvalue().strip()
            err = stderr_buf.getvalue().strip()
            if out:
                st.markdown("#### stdout (até o erro)")
                st.code(out, language="text")
            if err:
                st.markdown("#### stderr (até o erro)")
                st.code(err, language="text")

        finally:
            st.session_state["job_dfp_running"] = False


# Compatibilidade com loaders que chamam `configuracoes()`
def configuracoes() -> None:
    render()
