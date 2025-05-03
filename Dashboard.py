"""dashboard.py
~~~~~~~~~~~~~~~~
Script principal que instancia a UI e delega às páginas modularizadas
(`page_basic`, `page_advanced`, …).

*Execute com* `streamlit run dashboard.py`.
"""

from __future__ import annotations

import streamlit asst
import pandas as pd

# ---------------------------------------------------------------------------
# Módulos internos -----------------------------------------------------------
# ---------------------------------------------------------------------------
from db_loader import load_setores_from_db
import page_basic as pb
import page_advanced as pa
from helpers import get_logo_url

# ---------------------------------------------------------------------------
# Configuração inicial da página --------------------------------------------
# ---------------------------------------------------------------------------
st.set_page_config(page_title='Dashboard Financeiro', layout='wide')

# CSS global ----------------------------------------------------------------
st.markdown(
    """
    <style>
    .main {background-color: var(--background-color); color: var(--text-color);}  
    .stApp {background-color: var(--background-color); color: var(--text-color);}  
    div[data-testid="metric-container"] {
        background: var(--block-background-color);
        border:1px solid var(--block-border-color);
        padding:5% 5% 5% 10%;
        border-radius:10px;
        box-shadow:2px 2px 5px rgba(0,0,0,.1);
    }
    div[data-testid="metric-container"] > label {
        color: var(--metric-text-color);
        font-size:18px;
    }
    div[data-testid="metric-container"] > div > p {
        color: var(--positive-color);
        font-size:18px;
    }
    button {
        background: var(--button-background-color);
        color: var(--button-text-color);
        border-radius:5px;
        padding:5px 10px;
        border:none;
    }
    button:hover {
        background: var(--button-hover-background-color);
        color: var(--button-hover-text-color);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Carrega dados compartilhados ----------------------------------------------
# ---------------------------------------------------------------------------
setores_df = load_setores_from_db()
if setores_df is None or setores_df.empty:
    st.error('Erro ao carregar tabela de setores do banco de dados.')
    st.stop()

st.session_state['setores_df'] = setores_df

# ---------------------------------------------------------------------------
# Barra lateral – Navegação --------------------------------------------------
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown('# Análises')
    pagina = st.radio('Escolha a seção:', ['Básica', 'Avançada', 'Trading'])

st.session_state['pagina'] = pagina

# ---------------------------------------------------------------------------
# Delega renderização --------------------------------------------------------
# ---------------------------------------------------------------------------
pb.render()
pa.render()
