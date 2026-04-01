# core/secrets.py
# Resolução de segredos sem acoplamento direto ao Streamlit.
#
# Prioridade:
#   1) st.secrets  (Streamlit Cloud / sessão ativa)
#   2) variável de ambiente
#   3) RuntimeError
#
# Importar st é feito de forma lazy e opcional — este módulo
# pode ser usado fora de qualquer contexto Streamlit.

from __future__ import annotations

import os


def get_secret(key: str) -> str:
    """Retorna o valor do segredo identificado por `key`.

    Tenta, em ordem:
    1. ``st.secrets[key]`` — disponível quando rodando no Streamlit Cloud
       ou com ``.streamlit/secrets.toml`` configurado localmente.
    2. Variável de ambiente ``os.getenv(key)``.

    Levanta ``RuntimeError`` se nenhuma fonte tiver o valor.
    """
    # Tenta Streamlit Secrets sem exigir que `st` esteja no topo do módulo.
    # Se o Streamlit não estiver disponível ou não estiver em execução,
    # o bloco except captura silenciosamente e cai no fallback.
    try:
        import streamlit as st  # import lazy — não polui módulos que não usam st
        if hasattr(st, "secrets") and key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass

    value = os.getenv(key)
    if value:
        return value

    raise RuntimeError(
        f"Segredo '{key}' não encontrado. "
        f"Configure em .streamlit/secrets.toml, Streamlit Cloud Secrets "
        f"ou como variável de ambiente."
    )
