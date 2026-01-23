from __future__ import annotations

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from core.db_loader import (
    load_data_from_supabase,
    load_multiplos_from_supabase,
    load_macro_summary,
    load_setores_from_db,
)
from core.helpers import get_logo_url
from core.scoring import (
    calcular_score_acumulado,
    penalizar_crowding,
    penalizar_decay_lideranca,
    penalizar_plato,
    detectar_anomalias_mercado,
)
from core.portfolio import (
    gerir_carteira,
    calcular_patrimonio_selic_macro,
)
from core.weights import get_pesos
from core.yf_data import baixar_precos, coletar_dividendos


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
def _norm_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    return t if t.endswith(".SA") else f"{t}.SA"


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "").strip()


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.astype(str).str.strip().str.replace("\ufeff", "", regex=False)
    return out


def _safe_year_count_from_dre(dre: pd.DataFrame) -> int:
    if dre is None or dre.empty:
        return 0
    if "Data" not in dre.columns:
        return 0
    years = pd.to_datetime(dre["Data"], errors="coerce").dt.year
    return int(years.dropna().nunique())


@st.cache_data(show_spinner=False, ttl=6 * 60 * 60)  # 6h
def _get_setores_cached() -> pd.DataFrame:
    df = load_setores_from_db()
    if df is None:
        return pd.DataFrame()
    return _clean_columns(df)


@st.cache_data(show_spinner=False, ttl=6 * 60 * 60)  # 6h
def _get_macro_cached() -> pd.DataFrame:
    df = load_macro_summary()
    if df is None:
        return pd.DataFrame()
    df = _clean_columns(df)
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
        df = df.dropna(subset=["Data"]).sort_values("Data").reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────
# Render
# ─────────────────────────────────────────────────────────────
def render() -> None:
    st.markdown(
