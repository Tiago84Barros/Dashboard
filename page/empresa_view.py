from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

from core.helpers import get_company_info, get_logo_url
from core.db_loader import (
    load_data_from_db,
    load_multiplos_from_db,
    load_multiplos_limitado_from_db,
)
from core.yf_data import get_price, get_fundamentals_yf, baixar_precos


# ─────────────────────────────────────────────────────────────
# Crescimento (médio anual) com regressão em log
# ─────────────────────────────────────────────────────────────


def calculate_growth_rate(df: pd.DataFrame, column: str) -> float:
    try:
        if df is None or df.empty or "Data" not in df.columns or column not in df.columns:
            return np.nan

        tmp = df[["Data", column]].copy()
        tmp["Data"] = pd.to_datetime(tmp["Data"], errors="coerce")
        tmp[column] = pd.to_numeric(tmp[column], errors="coerce")
        tmp = tmp.dropna(subset=["Data", column])
        tmp = tmp[tmp[column] > 0].sort_values("Data")

        if tmp.shape[0] < 2:
            return np.nan

        X = (tmp["Data"] - tmp["Data"].iloc[0]).dt.days / 365.25
        y_log = np.log(tmp[column].values.astype(float))

        slope, _ = np.polyfit(X.values.astype(float), y_log, deg=1)
        g = float(np.exp(slope) - 1.0)
        return g if np.isfinite(g) else np.nan
    except Exception:
        return np.nan


def format_growth_rate(value: float) -> str:
    if isinstance(value, (int, float)) and not pd.isna(value) and not np.isinf(value):
        return f"{value:.2%}"
    return "-"


# ─────────────────────────────────────────────────────────────
# Preço (yfinance) — histórico + retornos anuais
# ─────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=60 * 60)  # 1h
def _get_price_history_cached(ticker: str, start: str) -> pd.Series:
    """Baixa histórico de preço ajustado (Close) via yfinance e devolve Series com índice datetime."""
    dfp = baixar_precos(ticker, start=start)
    if dfp is None or dfp.empty:
        return pd.Series(dtype="float64")
    # `baixar_precos` retorna colunas sem .SA (ex.: PETR4)
    col = (ticker or "").upper().replace(".SA", "").strip()
    if col not in dfp.columns:
        # fallback: primeira coluna
        col = dfp.columns[0]
    s = pd.to_numeric(dfp[col], errors="coerce").dropna()
    s.index = pd.to_datetime(s.index, errors="coerce")
    s = s.dropna()
    s.name = "Preço (ajust.)"
    return s


def _infer_price_start_from_financials(df_fin: pd.DataFrame) -> str:
    """Define o start do yfinance baseado no histórico financeiro disponível no Supabase."""
    if df_fin is None or df_fin.empty or "Data" not in df_fin.columns:
        return "2010-01-01"
    d = pd.to_datetime(df_fin["Data"], errors="coerce").dropna()
    if d.empty:
        return "2010-01-01"
    # Um ano a mais para pegar 'início do ano' com mais chance de ter pregões
    y = int(d.min().year) - 1
    y = max(y, 1990)
    return f"{y}-01-01"


def _annual_price_performance(price: pd.Series) -> pd.DataFrame:
    """Tabela de desempenho anual (1º e último pregão do ano)."""
    if price is None or price.empty:
        return pd.DataFrame(columns=["Ano", "Preço inicial", "Preço final", "Variação %"])

    s = price.sort_index().dropna()
    df = s.to_frame("close")
    df["Ano"] = df.index.year

    grp = df.groupby("Ano")["close"]
    ini = grp.first()
    fim = grp.last()

    out = pd.DataFrame(
        {
            "Ano": ini.index.astype(int),
            "Preço inicial": ini.values,
            "Preço final": fim.values,
        }
    )
    out["Variação %"] = (out["Preço final"] / out["Preço inicial"] - 1.0) * 100.0
    return out.sort_values("Ano").reset_index(drop=True)


def _cagr_from_series(price: pd.Series) -> float:
    if price is None or price.empty:
        return float("nan")
    s = price.sort_index().dropna()
    if s.shape[0] < 2:
        return float("nan")
    start_val = float(s.iloc[0])
    end_val = float(s.iloc[-1])
    if start_val <= 0 or end_val <= 0:
        return float("nan")
    years = (s.index[-1] - s.index[0]).days / 365.25
    if years <= 0:
        return float("nan")
    return (end_val / start_val) ** (1.0 / years) - 1.0


# ─────────────────────────────────────────────────────────────
# Helpers de múltiplos (display)
# ─────────────────────────────────────────────────────────────

def _latest_row_by_date(df: pd.DataFrame, date_col: str = "Data") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    if date_col in out.columns:
        out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
        out = out.dropna(subset=[date_col]).sort_values(date_col)
        return out.iloc[[-1]].copy()
    return out.iloc[[-1]].copy()


def _fmt_metric(label: str, value) -> str:
    if value is None or (isinstance(value, float) and (pd.isna(value) or np.isinf(value))):
        return "-"
    try:
        if "P/" in label or label in {"P/L", "P/VP", "P/EBIT", "EV/EBITDA"}:
            return f"{float(value):.2f}"
        if "DY" in label or "Yield" in label:
            return f"{float(value):.2%}"
        if "Margem" in label:
            return f"{float(value):.2%}"
        if label in {"ROE", "ROIC"}:
            return f"{float(value):.2%}"
        return f"{float(value):,.2f}"
    except Exception:
        return str(value)


def render_empresa_view(ticker: str) -> None:
    st.subheader(f"Visão Geral — {ticker}")

    df = load_data_from_db(ticker)
    if df is None or df.empty:
        st.warning("Dados financeiros não encontrados para este ticker.")
        return

    # garante Data em datetime
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")

    # infos yfinance
    nome, website = get_company_info(ticker)
    price = get_price(ticker)
    fundamentals = get_fundamentals_yf(ticker)

    # layout topo (logo + infos)
    col1, col2 = st.columns([1, 4])
    with col1:
        st.image(get_logo_url(ticker), width=80)
    with col2:
        st.markdown(f"**Empresa:** {nome or '-'}")
        st.markdown(f"**Site:** {website or '-'}")
        st.markdown(f"**Preço atual (yfinance):** {('R$ ' + f'{price:,.2f}') if price else '-'}")

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # Indicadores financeiros (crescimento)
    # ─────────────────────────────────────────────────────────
    st.markdown("### Crescimento (médio anual) — baseado no histórico do Supabase")

    cols = st.columns(4)
    metrics = [
        ("Receita Líquida", calculate_growth_rate(df, "Receita_Liquida")),
        ("EBIT", calculate_growth_rate(df, "EBIT")),
        ("Lucro Líquido", calculate_growth_rate(df, "Lucro_Liquido")),
        ("Dividendos", calculate_growth_rate(df, "Dividendos")),
    ]
    for i, (label, v) in enumerate(metrics):
        with cols[i]:
            st.metric(label, format_growth_rate(v))

    # ─────────────────────────────────────────────────────────
    # Múltiplos (yfinance atual)
    # ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Múltiplos (yfinance — atual)")

    mult = fundamentals or {}
    multiplos_show = [
        ("P/L", mult.get("pe_ratio")),
        ("P/VP", mult.get("pb_ratio")),
        ("DY", mult.get("dividend_yield")),
        ("ROE", mult.get("roe")),
        ("ROIC", mult.get("roic")),
        ("Margem Líquida", mult.get("profit_margin")),
        ("Margem Operacional", mult.get("operating_margin")),
        ("EV/EBITDA", mult.get("ev_ebitda")),
    ]

    cols = st.columns(4)
    for i, (label, v) in enumerate(multiplos_show):
        with cols[i % 4]:
            st.markdown(
                f"""
                <div style='padding:12px;border-radius:10px;border:1px solid #eee'>
                    <div class='metric-value'>{_fmt_metric(label, v)}</div>
                    <div class='metric-label'><strong>{label}</strong></div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # ─────────────────────────────────────────────────────────
    # Gráfico de múltiplos (histórico do DB)
    # ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Gráfico de Múltiplos (Histórico do Banco)")

    mult_hist = load_multiplos_from_db(ticker)
    if mult_hist is None or mult_hist.empty:
        st.info("Histórico de múltiplos não encontrado no banco.")
        return

    mult_hist = mult_hist.copy()
    mult_hist["Data"] = pd.to_datetime(mult_hist["Data"], errors="coerce")
    mult_hist = mult_hist.dropna(subset=["Data"]).sort_values("Data")

    exclude_columns = {"Data", "Ticker", "N Acoes", "N_Acoes"}
    cols_num = [c for c in mult_hist.columns if c not in exclude_columns]

    col_name_mapping = {col: col.replace("_", " ").title() for col in cols_num}
    display_name_to_col = {v: k for k, v in col_name_mapping.items()}
    display_names = list(col_name_mapping.values())

    default_display = [n for n in ["Margem Liquida", "Margem Operacional"] if n in display_names]
    variaveis_display = st.multiselect("Escolha os Indicadores:", display_names, default=default_display)

    if variaveis_display:
        variaveis = [display_name_to_col[n] for n in variaveis_display if n in display_name_to_col]
        if variaveis:
            dfm_mult = mult_hist.melt(id_vars=["Data"], value_vars=variaveis, var_name="Indicador", value_name="Valor")
            dfm_mult["Indicador"] = dfm_mult["Indicador"].map(col_name_mapping)
            st.plotly_chart(px.bar(dfm_mult, x="Data", y="Valor", color="Indicador", barmode="group"), use_container_width=True)
        else:
            st.info("Nenhuma variável válida selecionada.")

    # ─────────────────────────────────────────────────────────
    # Gráfico de Preço (yfinance) + Desempenho anual
    # ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Preço da Ação (Histórico via yfinance)")

    # define start com base no histórico financeiro do Supabase
    start_price = _infer_price_start_from_financials(df)

    with st.expander("Configurações do gráfico de preço", expanded=False):
        modo = st.radio(
            "Visualização",
            options=["Total", "Anual", "Mensal"],
            horizontal=True,
            index=0,
            key=f"price_view_mode_{ticker}",
        )
        # opcional: permitir ajustar janelas (sem obrigar)
        lookback_anos = st.slider("Anual: últimos N meses", min_value=3, max_value=24, value=12, step=1, key=f"price_lookback_y_{ticker}")
        lookback_dias = st.slider("Mensal: últimos N dias", min_value=7, max_value=120, value=30, step=1, key=f"price_lookback_m_{ticker}")

    price_hist = _get_price_history_cached(ticker, start=start_price)
    if price_hist.empty:
        st.info("Não foi possível obter histórico de preços via yfinance para este ticker.")
        return

    s_plot = price_hist.copy()

    if modo == "Anual":
        cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=int(lookback_anos * 30.44))
        s_plot = s_plot[s_plot.index >= cutoff]
    elif modo == "Mensal":
        cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=int(lookback_dias))
        s_plot = s_plot[s_plot.index >= cutoff]

    df_price_plot = s_plot.reset_index()
    df_price_plot.columns = ["Data", "Preço"]

    st.plotly_chart(
        px.line(df_price_plot, x="Data", y="Preço", title=f"{ticker} — {modo}"),
        use_container_width=True,
    )

    st.markdown("#### Desempenho anual do preço (1º x último pregão do ano)")
    perf = _annual_price_performance(price_hist)

    if perf.empty:
        st.info("Não foi possível calcular o desempenho anual com o histórico disponível.")
        return

    # restringe a anos compatíveis com o histórico financeiro do Supabase, quando existir
    if df is not None and not df.empty and "Data" in df.columns:
        dmin = pd.to_datetime(df["Data"], errors="coerce").dropna()
        if not dmin.empty:
            y_min = int(dmin.min().year)
            y_max = int(pd.to_datetime(df["Data"], errors="coerce").dropna().max().year)
            perf = perf[(perf["Ano"] >= y_min) & (perf["Ano"] <= y_max)]

    perf_show = perf.copy()
    perf_show["Preço inicial"] = perf_show["Preço inicial"].map(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    perf_show["Preço final"] = perf_show["Preço final"].map(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    perf_show["Variação %"] = perf_show["Variação %"].map(lambda x: f"{x:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

    st.dataframe(perf_show, use_container_width=True, hide_index=True)

    # métricas resumo
    perf_num = _annual_price_performance(price_hist)
    if df is not None and not df.empty and "Data" in df.columns:
        dmin = pd.to_datetime(df["Data"], errors="coerce").dropna()
        if not dmin.empty:
            y_min = int(dmin.min().year)
            y_max = int(pd.to_datetime(df["Data"], errors="coerce").dropna().max().year)
            perf_num = perf_num[(perf_num["Ano"] >= y_min) & (perf_num["Ano"] <= y_max)]

    avg_yoy = float(np.nanmean(perf_num["Variação %"].values)) / 100.0 if not perf_num.empty else float("nan")
    cagr = _cagr_from_series(price_hist)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Média de variação anual", format_growth_rate(avg_yoy))
    with c2:
        st.metric("CAGR (crescimento composto)", format_growth_rate(cagr))
