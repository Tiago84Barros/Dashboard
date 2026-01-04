from __future__ import annotations

import io
import os
import traceback
from contextlib import redirect_stdout, redirect_stderr

import streamlit as st


def _run_job(
    *,
    job_key: str,
    button_label: str,
    info_text: str,
    status_label: str,
    module_import_path: str,   # ex: "pickup.dados_cvm_dfp"
    module_attr_name: str,     # ex: "dados_cvm_dfp"
    main_func_name: str = "main",
) -> None:
    # trava simples para evitar duplo clique
    if job_key not in st.session_state:
        st.session_state[job_key] = False

    col1, col2 = st.columns([1, 2], gap="large")

    with col1:
        run = st.button(
            button_label,
            use_container_width=True,
            disabled=st.session_state[job_key],
        )

    with col2:
        st.info(info_text)

    with st.expander("Ações de manutenção", expanded=False):
        if st.button(f"Resetar trava do botão ({button_label})"):
            st.session_state[job_key] = False
            st.success("Trava resetada.")
            st.rerun()

    st.markdown("### Logs da execução")
    log_err = st.empty()

    if run:
        st.session_state[job_key] = True

        # Pré-check obrigatório
        if not os.getenv("SUPABASE_DB_URL"):
            st.error("SUPABASE_DB_URL não está definida. Configure em Secrets/Env Vars e tente novamente.")
            st.session_state[job_key] = False
            st.stop()

        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()

        try:
            with st.status(status_label, expanded=True) as status:
                status.write(f"Importando módulo `{module_import_path}` …")

                with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
                    mod = __import__(module_import_path, fromlist=[module_attr_name])
                    status.write(f"Executando `{module_attr_name}.{main_func_name}()` …")
                    getattr(mod, main_func_name)()

                status.update(label="Execução finalizada.", state="complete")

            out = stdout_buf.getvalue().strip()
            err = stderr_buf.getvalue().strip()

            if out:
                st.text_area("Saída completa do script (stdout)", out, height=400)
            else:
                st.info("Nenhum log foi produzido em stdout.")

            if err:
                log_err.warning("Saída em stderr (avisos/erros):")
                log_err.code(err, language="text")
            else:
                log_err.empty()

            st.success("Rotina concluída (sem exceções Python).")

            # Se você usa cache_data em queries, limpar ajuda a refletir novos dados
            try:
                st.cache_data.clear()
            except Exception:
                pass

        except Exception as e:
            st.error("Falha ao executar a rotina. Traceback completo:")
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
            st.session_state[job_key] = False


def render() -> None:
    st.header("Configurações")
    st.caption(
        "Use esta seção para executar rotinas de atualização das tabelas no Supabase. "
        "A execução ocorre no servidor do Streamlit e grava nas tabelas do schema public."
    )

    st.subheader("Atualização de Base")
    st.write(
        "**Ordem recomendada (parcial):**\n"
        "1) Demonstrações completas (DFP/anual)\n"
        "2) Demonstrações trimestrais (ITR/TRI)\n"
        "3) Informações econômicas (macro Brasil)"
    )

    st.markdown("### Diagnóstico rápido")
    st.write("SUPABASE_DB_URL definida?", bool(os.getenv("SUPABASE_DB_URL")))

    st.divider()

    # =========================
    # BOTÃO 1: DFP (ANUAL)
    # =========================
    st.markdown("## 1. Demonstrações completas (DFP/anual)")
    _run_job(
        job_key="job_dfp_running",
        button_label="Atualizar Demonstrações Completas (DFP)",
        info_text=(
            "Este botão executa o script **pickup/dados_cvm_dfp.py** para baixar os DFP da CVM, "
            "consolidar e gravar em **public.Demonstracoes_Financeiras** no Supabase.\n\n"
            "Requisitos: configurar **SUPABASE_DB_URL** em Secrets/Env Vars."
        ),
        status_label="Executando carga DFP (pode demorar alguns minutos)...",
        module_import_path="pickup.dados_cvm_dfp",
        module_attr_name="dados_cvm_dfp",
    )

    st.divider()

    # =========================
    # BOTÃO 2: ITR/TRI (TRIMESTRAL)
    # =========================
    st.markdown("## 2. Demonstrações trimestrais (ITR/TRI)")
    _run_job(
        job_key="job_tri_running",
        button_label="Atualizar Demonstrações Trimestrais (TRI/ITR)",
        info_text=(
            "Este botão executa o script **pickup/dados_cvm_itr.py** para baixar os ITR consolidados da CVM, "
            "consolidar e gravar em **public.Demonstracoes_Financeiras_TRI** no Supabase.\n\n"
            "Requisitos: configurar **SUPABASE_DB_URL** em Secrets/Env Vars e garantir unique key em (Ticker, Data)."
        ),
        status_label="Executando carga ITR (pode demorar alguns minutos)...",
        module_import_path="pickup.dados_cvm_itr",
        module_attr_name="dados_cvm_itr",
    )

    st.divider()

    # =========================
    # BOTÃO 3: MACRO (INFO ECONÔMICA)
    # =========================
    st.markdown("## 3. Informações Econômicas (Macro Brasil)")

    with st.expander("Detalhes / Variáveis de ambiente (macro)", expanded=False):
        st.write("ICC_MODE (final|mean):", os.getenv("ICC_MODE", "final"))
        st.write("MACRO_START_DATE (YYYY-MM-DD):", os.getenv("MACRO_START_DATE", "2010-01-01"))
        st.write("MACRO_MAX_YEARS_CHUNK:", os.getenv("MACRO_MAX_YEARS_CHUNK", "10"))
        st.write("MACRO_WRITE_MONTHLY (1 para gravar mensal):", os.getenv("MACRO_WRITE_MONTHLY", "0"))
        st.caption(
            "Observação: anual grava em public.info_economica. "
            "Se MACRO_WRITE_MONTHLY=1, tenta gravar também em public.info_economica_mensal."
        )

    _run_job(
        job_key="job_macro_running",
        button_label="Atualizar Informações Econômicas (BCB/SGS)",
        info_text=(
            "Executa **pickup/dados_macro_brasil.py** para coletar séries do BCB/SGS, gerar base **anual** "
            "para contexto/regime (tabela **public.info_economica**) e, opcionalmente, base **mensal** "
            "(tabela **public.info_economica_mensal**) quando `MACRO_WRITE_MONTHLY=1`.\n\n"
            "Requisitos: `SUPABASE_DB_URL` e dependência `python-bcb` no requirements."
        ),
        status_label="Executando carga Macro Brasil (BCB/SGS)...",
        module_import_path="pickup.dados_macro_brasil",
        module_attr_name="dados_macro_brasil",
    )


# Compatibilidade com loaders que chamam `configuracoes()`
def configuracoes() -> None:
    render()
