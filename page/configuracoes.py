# configuracoes_v2_jobs.py
# Versão adaptada com suporte aos pipelines CVM V2

from contextlib import contextmanager, redirect_stderr, redirect_stdout
import os
import streamlit as st

# --- helper para variáveis de ambiente temporárias ---
@contextmanager
def _temporary_environ(overrides=None):
    overrides = overrides or {}
    old_env = {}
    try:
        for k, v in overrides.items():
            old_env[k] = os.environ.get(k)
            os.environ[k] = str(v)
        yield
    finally:
        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

# --- executor simplificado ---
def _run_job(label, module_path, env=None):
    if st.button(label):
        with st.spinner(f"Executando {label}..."):
            with _temporary_environ(env):
                mod = __import__(module_path, fromlist=["main"])
                mod.main()
        st.success("Concluído.")

# --- UI ---
def render():
    st.title("Configurações - CVM V2")

    st.header("Pipeline CVM V2")

    _run_job(
        "1. Extract Raw (DFP)",
        "pickup.cvm_extract_v2",
        {"CVM_DOC_TYPE": "DFP"}
    )

    _run_job(
        "2. Extract Raw (ITR)",
        "pickup.cvm_extract_v2",
        {"CVM_DOC_TYPE": "ITR"}
    )

    _run_job(
        "3. Map Normalized",
        "pickup.cvm_map_v2"
    )

    _run_job(
        "4. Publish Financials",
        "pickup.cvm_publish_financials_v2"
    )

def configuracoes():
    render()
