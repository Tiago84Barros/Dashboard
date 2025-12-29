from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import text

from core.db_supabase import get_engine


# =============================================================================
# Utilidades
# =============================================================================

def _norm_ticker(t: str) -> str:
    return t.replace(".SA", "").upper()


def _fmt_num(x):
    if x is None or pd.isna(x):
        return "—"
    return f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_pct(x):
    if x is None or pd.isna(x):
        return "—"
    return f"{x * 100:.1f}%"


def _trend(series: pd.Series) -> str:
    s = series.dropna()
    if len(s) < 3:
        return "inconclusiva"
    coef = np.polyfit(range(len(s)), s.values, 1)[0]
    if coef > 0:
        return "crescente"
    if coef < 0:
        return "decrescente"
    return "estável"


def _last_valid(series: pd.Series):
    """Retorna o último valor não-nulo de uma série (ou None)."""
    if series is None:
        return None
    s = series.dropna()
    if s.empty:
        return None
    return s.iloc[-1]


# =============================================================================
# LOADERS (cacheados corretamente)
# =============================================================================

@st.cache_data(ttl=3600, show_spinner=False)
def _load_dfp(_engine, ticker: str) -> pd.DataFrame:
    return pd.read_sql(
        text(
            """
            select
                data,
                receita_liquida,
                ebit,
                lucro_liquido,
                patrimonio_liquido
            from cvm.demonstracoes_financeiras_dfp
            where ticker = :t
            order by data
            """
        ),
        _engine,
        params={"t": ticker},
    )


@st.cache_data(ttl=3600, show_spinner=False)
def _load_multiplos(_engine, ticker: str) -> pd.DataFrame:
    return pd.read_sql(
        text(
            """
            select
                ano,
                pl,
                roe,
                margem_liquida,
                margem_ebit,
                divida_liquida_ebit,
                divida_total_patrimonio
            from cvm.multiplos
            where ticker = :t
            order by ano
            """
        ),
        _engine,
        params={"t": ticker},
    )


@st.cache_data(ttl=3600, show_spinner=False)
def _load_prices_monthly(_engine, ticker: str) -> pd.DataFrame:
    return pd.read_sql(
        text(
            """
            select
                month_end,
                close
            from cvm.prices_b3_monthly
            where ticker = :t
            order by month_end
            """
        ),
        _engine,
        params={"t": ticker},
    )


# =============================================================================
# Relatório Executivo Determinístico
# =============================================================================

def _executive_summary(dfp: pd.DataFrame, mult: pd.DataFrame) -> list[str]:
    insights: list[str] = []

    if not dfp.empty:
        insights.append(f"Receita apresenta tendência **{_trend(dfp['receita_liquida'])}**.")
        insights.append(f"Lucro líquido apresenta tendência **{_trend(dfp['lucro_liquido'])}**.")

    if not mult.empty:
        roe_med = mult["roe"].dropna().tail(5).mean()
        pl_med = mult["pl"].dropna().tail(5).mean()
        alav = mult["divida_liquida_ebit"].dropna().tail(5).mean()

        if not pd.isna(roe_med):
            if roe_med > 0.15:
                insights.append("ROE médio recente **elevado**, indicando boa eficiência.")
            elif roe_med > 0:
                insights.append("ROE médio recente **moderado**.")
            else:
                insights.append("ROE médio recente **baixo ou negativo**.")

        if not pd.isna(pl_med):
            if pl_med < 10:
                insights.append("P/L médio **baixo**, possível desconto relativo.")
            elif pl_med < 18:
                insights.append("P/L médio **em faixa razoável**.")
            else:
                insights.append("P/L médio **elevado**, crescimento já precificado.")

        if not pd.isna(alav):
            if alav > 4:
                insights.append("Alavancagem **alta**, exige cautela.")
            else:
                insights.append("Alavancagem **sob controle**.")

    return insights


# =============================================================================
