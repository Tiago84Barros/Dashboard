import streamlit as st
import pandas as pd

from core.db_supabase import get_engine
from core.db.loader import (
    load_demonstracoes_financeiras,
    load_multiplos,
)

st.set_page_config(page_title="Análise Básica de Ações", layout="wide")

st.title("Análise Básica de Ações")
st.caption("Análise fundamentalista básica da empresa selecionada.")

# ─────────────────────────────────────────────
# Ticker vindo do dashboard (única fonte)
# ─────────────────────────────────────────────
ticker = (
    st.session_state.get("ticker")
    or st.session_state.get("ticker_selecionado")
    or st.session_state.get("ticker_filtrado")
)

if not ticker:
    st.warning("Selecione uma ação no menu lateral.")
    st.stop()

engine = get_engine()

# ─────────────────────────────────────────────
# Dados da empresa (via setores_df)
# ─────────────────────────────────────────────
setores_df = st.session_state.get("setores_df")

if setores_df is None or setores_df.empty:
    st.error("Dados de setores não carregados.")
    st.stop()

empresa = setores_df[setores_df["ticker"] == ticker]

if empresa.empty:
    st.error(f"Empresa {ticker} não encontrada.")
    st.stop()

nome_empresa = empresa.iloc[0]["nome_empresa"]

st.subheader(f"{nome_empresa} ({ticker})")

# ─────────────────────────────────────────────
# Demonstrações Financeiras
# ─────────────────────────────────────────────
df_fin = load_demonstracoes_financeiras(
    engine=engine,
    ticker=ticker,
)

if df_fin is None or df_fin.empty:
    st.warning("Não há demonstrações financeiras disponíveis.")
else:
    st.markdown("### Demonstrações Financeiras")
    st.dataframe(df_fin, use_container_width=True)

# ─────────────────────────────────────────────
# Múltiplos
# ─────────────────────────────────────────────
df_mult = load_multiplos(
    engine=engine,
    ticker=ticker,
)

if df_mult is None or df_mult.empty:
    st.warning("Não há múltiplos disponíveis.")
else:
    st.markdown("### Múltiplos")
    st.dataframe(df_mult, use_container_width=True)
