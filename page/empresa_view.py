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
        insights.append(
            f"Receita apresenta tendência **{_trend(dfp['receita_liquida'])}**."
        )
        insights.append(
            f"Lucro líquido apresenta tendência **{_trend(dfp['lucro_liquido'])}**."
        )

    if not mult.empty:
        roe_med = mult["roe"].dropna().tail(5).mean()
        pl_med = mult["pl"].dropna().tail(5).mean()
        alav = mult["divida_liquida_ebit"].dropna().tail(5).mean()

        if roe_med is not None:
            if roe_med > 0.15:
                insights.append("ROE médio recente **elevado**, indicando boa eficiência.")
            elif roe_med > 0:
                insights.append("ROE médio recente **moderado**.")
            else:
                insights.append("ROE médio recente **baixo ou negativo**.")

        if pl_med is not None:
            if pl_med < 10:
                insights.append("P/L médio **baixo**, possível desconto relativo.")
            elif pl_med < 18:
                insights.append("P/L médio **em faixa razoável**.")
            else:
                insights.append("P/L médio **elevado**, crescimento já precificado.")

        if alav is not None:
            if alav > 4:
                insights.append("Alavancagem **alta**, exige cautela.")
            else:
                insights.append("Alavancagem **sob controle**.")

    return insights


# =============================================================================
# Render principal
# =============================================================================

def render_empresa_view(ticker: str) -> None:
    engine = get_engine()
    t = _norm_ticker(ticker)

    st.markdown(f"## 📊 {t}")

    # ------------------------------
    # Carregamento de dados
    # ------------------------------
    dfp = _load_dfp(engine, t)
    mult = _load_multiplos(engine, t)
    prices_m = _load_prices_monthly(engine, t)

    if dfp.empty:
        st.warning("Não há dados fundamentais disponíveis para esta empresa.")
        return

    # ------------------------------
    # KPIs principais
    # ------------------------------
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Receita (último ano)", _fmt_num(dfp.iloc[-1]["receita_liquida"]))

    with col2:
        st.metric("Lucro Líquido (último ano)", _fmt_num(dfp.iloc[-1]["lucro_liquido"]))

    with col3:
        roe_last = mult["roe"].dropna().iloc[-1] if not mult.empty else None
        st.metric("ROE", _fmt_pct(roe_last))

    with col4:
        pl_last = mult["pl"].dropna().iloc[-1] if not mult.empty else None
        st.metric("P/L", _fmt_num(pl_last))

    st.divider()

    # ------------------------------
    # Gráficos fundamentais
    # ------------------------------
    dfp_plot = dfp.copy()
    dfp_plot["ano"] = pd.to_datetime(dfp_plot["data"]).dt.year

    fig1 = px.line(
        dfp_plot,
        x="ano",
        y=["receita_liquida", "lucro_liquido"],
        title="Receita e Lucro ao longo do tempo",
        markers=True,
    )
    st.plotly_chart(fig1, use_container_width=True)

    fig2 = px.line(
        dfp_plot,
        x="ano",
        y="patrimonio_liquido",
        title="Evolução do Patrimônio Líquido",
        markers=True,
    )
    st.plotly_chart(fig2, use_container_width=True)

    # ------------------------------
    # Preço mensal (benchmark)
    # ------------------------------
    if not prices_m.empty:
        fig3 = px.line(
            prices_m,
            x="month_end",
            y="close",
            title="Preço da ação (último pregão do mês)",
        )
        st.plotly_chart(fig3, use_container_width=True)

    # ------------------------------
    # Indicadores ao longo do tempo
    # ------------------------------
    if not mult.empty:
        fig4 = px.line(
            mult,
            x="ano",
            y=["roe", "margem_liquida", "margem_ebit"],
            title="Indicadores de rentabilidade",
            markers=True,
        )
        st.plotly_chart(fig4, use_container_width=True)

    # ------------------------------
    # Relatório Executivo
    # ------------------------------
    st.subheader("🧠 Relatório Executivo")

    insights = _executive_summary(dfp, mult)
    if insights:
        for i in insights:
            st.markdown(f"- {i}")
    else:
        st.write("Dados insuficientes para análise conclusiva.")
