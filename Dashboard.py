"""dashboard.py
~~~~~~~~~~~~~~~~
Script principal que instancia a UI e delega às páginas modularizadas
(`page_basic`, `page_advanced`, …).

*Execute com* `streamlit run dashboard.py`.
"""

from __future__ import annotations

import streamlit as st
import pandas as pd

# ---------------------------------------------------------------------------
# Módulos internos -----------------------------------------------------------
# ---------------------------------------------------------------------------
from db_loader import load_setores_from_db
import page_basic as pb
import page_advanced as pa

# ---------------------------------------------------------------------------
# Helper local (fallback) ----------------------------------------------------
# ---------------------------------------------------------------------------

def get_logo_url(ticker: str) -> str:
    """Retorna URL PNG do logotipo a partir do repositório public *icones-b3*."""
    tk = ticker.replace('.SA', '').upper()
    return f"https://raw.githubusercontent.com/thefintz/icones-b3/main/icones/{tk}.png"

# Register helper globally so that page modules can import if needed
import sys, types
aux = types.ModuleType('utils')
aux.get_logo_url = get_logo_url
sys.modules['utils'] = aux  # para page_advanced agora funcionar mesmo se utils não existir

# ---------------------------------------------------------------------------
# Configuração inicial da página --------------------------------------------
# ---------------------------------------------------------------------------
st.set_page_config(page_title='Dashboard Financeiro', layout='wide')

# CSS global (mantive seus placeholders de variáveis de tema)
st.markdown(
    """
    <style>
    .main {background-color: var(--background-color);color: var(--text-color);}
    .stApp {background-color: var(--background-color);color: var(--text-color);} 
    /* Métricas */
    div[data-testid="metric-container"] {background: var(--block-background-color);border:1px solid var(--block-border-color);padding:5% 5% 5% 10%;border-radius:10px;box-shadow: 2px 2px 5px rgba(0,0,0,.1);}  
    div[data-testid="metric-container"] > label {color: var(--metric-text-color);font-size:18px;}
    div[data-testid="metric-container"] > div > p {color: var(--positive-color);font-size:18px;}
    button {background: var(--button-background-color);color: var(--button-text-color);border-radius:5px;padding:5px 10px;border:none;}
    button:hover {background: var(--button-hover-background-color);color: var(--button-hover-text-color);}
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Carrega dados compartilhados ----------------------------------------------
# ---------------------------------------------------------------------------
setores_df: pd.DataFrame | None = load_setores_from_db()
if setores_df is None or setores_df.empty:
    st.error('Erro ao carregar tabela de setores do banco de dados.')
    st.stop()

# Armazena no session_state para que os módulos possam acessar
st.session_state['setores_df'] = setores_df

# ---------------------------------------------------------------------------
# Barra lateral – Navegação --------------------------------------------------
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown('# Análises')
    pagina = st.radio('Escolha a seção:', ['Básica', 'Avançada', 'Trading'])

# ---------------------------------------------------------------------------
# Delega renderização --------------------------------------------------------
# ---------------------------------------------------------------------------
st.session_state['pagina'] = pagina  # página atual visível para os módulos

# Chama render de cada página (elas próprias checam se devem desenhar)
pb.render()
pa.render()

# (Futuro) page_trading.render()
