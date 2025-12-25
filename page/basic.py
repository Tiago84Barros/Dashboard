from __future__ import annotations

import streamlit as st
import pandas as pd

from core.db_supabase import get_engine
from core.db.loader import load_demonstracoes_financeiras, load_multiplos


def _get_selected_ticker() -> str | None:
    """
    Fonte única do ticker: dashboard (sidebar global).
    Compatível com chaves antigas.
    """
    t = (
        st.session_state.get("ticker")
        or st.session_state.get("ticker_selecionado")
        or st.session_state.get("ticker_filtrado")
        or st.session_state.get("ticker_busca")
    )
    if not t:
        return None
    return str(t).strip().upper()


def _get_empresa_info_from_setores_df(ticker: str) -> dict:
    """
    Busca nome_empresa/setor/subsetor/segmento no df já carregado pelo dashboard.
    Retorna dict com chaves possivelmente ausentes.
    """
    out = {"nome_empresa": None, "setor": None, "subsetor": None, "segmento": None}
    df = st.session_state.get("setores_df")
    if df is None or getattr(df, "empty", True):
        return out

    try:
        row = df[df["ticker"] == ticker]
        if row.empty:
            return out
        r0 = row.iloc[0]
        out["nome_empresa"] = r0.get("nome_empresa")
        # Pode haver colunas diferentes dependendo do loader (caixa alta/baixa)
        out["setor"] = r0.get("setor") if "setor" in row.columns else r0.get("SETOR")
        out["subsetor"] = r0.get("subsetor") if "subsetor" in row.columns else r0.get("SUBSETOR")
        out["segmento"] = r0.get("segmento") if "segmento" in row.columns else r0.get("SEGMENTO")
        return out
    except Exception:
        return out


def _safe_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    return df


# ──────────────────────────────────────────────────────────────────
# Página
# ──────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Análise Básica de Ações", layout="wide")

st.title("Análise Básica de Ações")

ticker = _get_selected_ticker()
if not ticker:
    st.warning("Selecione uma ação no menu lateral (Busca de ações).")
    st.stop()

engine = get_engine()

empresa_info = _get_empresa_info_from_setores_df(ticker)
nome_empresa = empresa_info.get("nome_empresa") or ticker

# Cabeçalho (layout simples e limpo)
left, right = st.columns([2, 3], vertical_alignment="top")

with left:
    st.subheader(f"{nome_empresa} ({ticker})")

with right:
    chips = []
    if empresa_info.get("setor"):
        chips.append(f"Setor: {empresa_info['setor']}")
    if empresa_info.get("subsetor"):
        chips.append(f"Subsetor: {empresa_info['subsetor']}")
    if empresa_info.get("segmento"):
        chips.append(f"Segmento: {empresa_info['segmento']}")
    if chips:
        st.caption(" • ".join(chips))

st.divider()

# ──────────────────────────────────────────────────────────────────
# Carregamento de dados (sem alterar layout)
# ──────────────────────────────────────────────────────────────────
# Demonstrações
try:
    df_fin = load_demonstracoes_financeiras(engine=engine, ticker=ticker)
except TypeError:
    # fallback caso a assinatura seja diferente em algum ambiente
    df_fin = load_demonstracoes_financeiras(engine, ticker)
except Exception as e:
    st.error("Falha ao carregar demonstrações financeiras.")
    st.exception(e)
    df_fin = pd.DataFrame()

df_fin = _safe_dataframe(df_fin)

# Múltiplos
try:
    df_mult = load_multiplos(engine=engine, ticker=ticker)
except TypeError:
    df_mult = load_multiplos(engine, ticker)
except Exception as e:
    st.error("Falha ao carregar múltiplos.")
    st.exception(e)
    df_mult = pd.DataFrame()

df_mult = _safe_dataframe(df_mult)

# ──────────────────────────────────────────────────────────────────
# Layout das seções (básico e estável)
# ──────────────────────────────────────────────────────────────────
tab1, tab2 = st.tabs(["Demonstrações", "Múltiplos"])

with tab1:
    st.markdown("### Demonstrações Financeiras")
    if df_fin.empty:
        st.info("Não há demonstrações financeiras disponíveis para este ticker.")
    else:
        st.dataframe(df_fin, use_container_width=True)

with tab2:
    st.markdown("### Múltiplos")
    if df_mult.empty:
        st.info("Não há múltiplos disponíveis para este ticker.")
    else:
        st.dataframe(df_mult, use_container_width=True)
