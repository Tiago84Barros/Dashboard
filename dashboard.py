"""
dashboard.py
~~~~~~~~~~~~
Script principal da aplicação Streamlit.

Execute com:
    streamlit run dashboard.py
"""

from __future__ import annotations

import pathlib
import sys
import streamlit as st

# ───────────────────────── Ajuste de path ──────────────────────────
ROOT_DIR = pathlib.Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

# ───────────────────────── Imports internos ────────────────────────
from core.db_loader import load_setores_from_db  # noqa: E402
from page import basic as pb  # noqa: E402
from page import advanced as pa  # noqa: E402
from page import criacao_portfolio as pc  # noqa: E402
from design.layout import configurar_pagina, aplicar_estilos_css  # layout global

# ───────────────────────── Layout Global ───────────────────────────
configurar_pagina()
aplicar_estilos_css()

# ───────────────────────── Cache inicial ───────────────────────────
if "setores_df" not in st.session_state:
    st.session_state["setores_df"] = load_setores_from_db()

# ───────────────────────── Sidebar navegação ───────────────────────
with st.sidebar:
    st.markdown("# Análises")
    pagina_escolhida = st.radio(
        "Escolha a seção:",
        ["Básica", "Avançada", "Criação de Portfólio"],
    )

# ───────────────────────── Roteamento ──────────────────────────────
if pagina_escolhida == "Básica":
    pb.render()
elif pagina_escolhida == "Avançada":
    pa.render()
elif pagina_escolhida == "Criação de Portfólio":
    pc.render()
else:
    st.error("Página não encontrada.")
