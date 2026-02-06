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
from core.yf_data import get_price, get_fundamentals_yf

# Histórico de preços (yfinance)
try:
    from core.yf_data import baixar_precos
except Exception:
    baixar_precos = None  # type: ignore


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
# Demonstrações Financeiras (NOVO) — gráficos do histórico do Supabase
# ─────────────────────────────────────────────────────────────

def _fmt_brl(x) -> str:
    try:
        if x is None or (isinstance(x, float) and (pd.isna(x) or np.isinf(x))):
            return "-"
        return f"R$ {float(x):,.0f}"
    except Exception:
        return "-"


def render_graficos_demonstracoes_financeiras(df: pd.DataFrame, ticker: str) -> None:
    st.markdown("---")
    st.markdown("### Demonstrações Financeiras (Histórico do Banco)")

    if df is None or df.empty or "Data" not in df.columns:
        st.info("Sem dados de Demonstrações Financeiras para exibir.")
        return

    dff = df.copy()
    dff["Data"] = pd.to_datetime(dff["Data"], errors="coerce")
    dff = dff.dropna(subset=["Data"]).sort_values("Data")

    candidatos = [
        ("Receita_Liquida", "Receita Líquida"),
        ("EBIT", "EBIT"),
        ("Lucro_Liquido", "Lucro Líquido"),
        ("Dividendos", "Dividendos"),
        ("Ativo_Total", "Ativo Total"),
        ("Patrimonio_Liquido", "Patrimônio Líquido"),
        ("Divida_Total", "Dívida Total"),
        ("Divida_Liquida", "Dívida Líquida"),
        ("Caixa_Liquido", "Caixa Líquido"),
    ]
    existentes = [(c, lbl) for (c, lbl) in candidatos if c in dff.columns]

    if not existentes:
        st.info("Não encontrei colunas financeiras esperadas para plotar no DataFrame.")
        return

    col_a, col_b = st.columns([3, 2])
    with col_a:
        opcoes = [lbl for _, lbl in existentes]
        default = [x for x in ["Receita Líquida", "Lucro Líquido", "Dividendos"] if x in opcoes]
        selecionados_lbl = st.multiselect(
            "Escolha as linhas para visualizar",
            options=opcoes,
            default=default if default else opcoes[:2],
            key=f"df_demonstracoes_sel_{ticker}",
        )
    with col_b:
        escala = st.radio(
            "Escala",
            options=["Normal", "Log (visual)"],
            horizontal=True,
            index=0,
            key=f"df_demonstracoes_scale_{ticker}",
        )

    if not selecionados_lbl:
        st.info("Selecione pelo menos um indicador.")
        return

    lbl_to_col = {lbl: col for col, lbl in existentes}
    cols_sel = [lbl_to_col[lbl] for lbl in selecionados_lbl if lbl in lbl_to_col]

    plot = dff[["Data"] + cols_sel].copy()
    for c in cols_sel:
        plot[c] = pd.to_numeric(plot[c], errors="coerce")

    plot = plot.dropna(subset=["Data"], how="any")
    if plot.empty:
        st.info("Sem dados suficientes para plotar após limpeza.")
        return

    melt = plot.melt(id_vars=["Data"], value_vars=cols_sel, var_name="Indicador", value_name="Valor")
    melt["Indicador"] = melt["Indicador"].map({col: lbl for col, lbl in existentes})

    fig = px.line(melt, x="Data", y="Valor", color="Indicador", markers=True)
    if escala.startswith("Log"):
        fig.update_yaxes(type="log")

    st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Últimos valores disponíveis (mais recente no banco)")
    last = dff.sort_values("Data").iloc[-1]
    cols = st.columns(min(4, len(cols_sel)))
    for i, c in enumerate(cols_sel[:4]):
        lbl = {col: lbl for col, lbl in existentes}.get(c, c)
        with cols[i % len(cols)]:
            st.metric(lbl, _fmt_brl(last.get(c)))


# ─────────────────────────────────────────────────────────────
# Preço (yfinance) — histórico + retornos anuais
# ─────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=60 * 60)  # 1h
def _get_price_history_cached(ticker: str, start: str) -> pd.Series:
    """Baixa histórico de preço (Close) via yfinance e devolve Series com índice datetime."""
    if baixar_precos is None:
        return pd.Series(dtype="float64")

    dfp = baixar_precos(ticker, start=start)
    if dfp is None or dfp.empty:
        return pd.Series(dtype="float64")

    col = (ticker or "").upper().replace(".SA", "").strip()
    if col not in dfp.columns:
        col = dfp.columns[0]

    s = pd.to_numeric(dfp[col], errors="coerce").dropna()
    s.index = pd.to_datetime(s.index, errors="coerce")
    s = s.dropna().sort_index()
    s.name = "Preço"
    return s


def _infer_price_start_from_financials(df_fin: pd.DataFrame) -> str:
    """Define o start do yfinance com base no histórico financeiro do Supabase (Demonstracoes_Financeiras)."""
    if df_fin is None or df_fin.empty or "Data" not in df_fin.columns:
        return "2010-01-01"
    d = pd.to_datetime(df_fin["Data"], errors="coerce").dropna()
    if d.empty:
        return "2010-01-01"
    y = max(int(d.min().year) - 1, 1990)
    return f"{y}-01-01"


def _annual_price_performance(price: pd.Series) -> pd.DataFrame:
    """Tabela anual: preço inicial/final do ano e variação % (1º x último pregão do ano)."""
    if price is None or price.empty:
        return pd.DataFrame(columns=["Ano", "Preço inicial", "Preço final", "Variação %"])

    s = price.dropna().sort_index()
    dfp = s.to_frame("close")
    dfp["Ano"] = dfp.index.year

    grp = dfp.groupby("Ano")["close"]
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
    """CAGR do período (crescimento composto)."""
    if price is None or price.empty:
        return float("nan")
    s = price.dropna().sort_index()
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
# Helpers internos (multiplos / display)
# ─────────────────────────────────────────────────────────────

def _latest_row_by_date(df: pd.DataFrame, date_col: str = "Data") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if date_col in out.columns:
        out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
        out = out.dropna(subset=[date_col]).sort_values(date_col)
        if out.empty:
            return pd.DataFrame()
        return out.iloc[[-1]].copy()
    return out.iloc[[-1]].copy()


def _is_missing(x) -> bool:
    try:
        if x is None:
            return True
        if isinstance(x, (float, int)):
            if pd.isna(x) or np.isinf(x) or float(x) == 0.0:
                return True
        if isinstance(x, str) and not x.strip():
            return True
    except Exception:
        return True
    return False


def _merge_display_multiplos_db_primary(
    db_latest: pd.DataFrame,
    yf_latest: pd.DataFrame | None,
) -> pd.DataFrame:
    """
    DF final (1 linha) para exibição:
      - Base: DB
      - Complemento: YF somente nos campos faltantes
    """
    db1 = _latest_row_by_date(db_latest) if db_latest is not None else pd.DataFrame()
    if db1 is None or db1.empty:
        if isinstance(yf_latest, pd.DataFrame) and not yf_latest.empty:
            return yf_latest.head(1).copy()
        return pd.DataFrame([{}])

    df_disp = db1.copy()

    if isinstance(yf_latest, pd.DataFrame) and not yf_latest.empty:
        yf1 = yf_latest.head(1).copy()
        for c in yf1.columns:
            yv = yf1.at[0, c]
            if c not in df_disp.columns:
                df_disp[c] = yv
            else:
                dbv = df_disp.at[0, c]
                if _is_missing(dbv) and not _is_missing(yv):
                    df_disp.at[0, c] = yv

    return df_disp


def _fmt_metric(label: str, value) -> str:
    if value is None or (isinstance(value, float) and (pd.isna(value) or np.isinf(value))) or value == 0:
        return "-"
    try:
        v = float(value)
    except Exception:
        return "-"

    if "Margem" in label or label in ["ROE", "ROIC", "Payout", "Dividend Yield", "Endividamento Total"]:
        return f"{v:.2f}%"
    return f"{v:.2f}"


def _needs_yf_fundamentals(mult_db_latest: pd.DataFrame) -> bool:
    if mult_db_latest is None or mult_db_latest.empty:
        return True

    row = mult_db_latest.iloc[0]
    needed = ["DY", "P/VP", "P/L", "Payout"]
    for c in needed:
        if c not in mult_db_latest.columns or _is_missing(row.get(c)):
            return True
    return False


def render_empresa_view(ticker: str) -> None:
    st.subheader(f"Visão Geral — {ticker}")

    df = load_data_from_db(ticker)
    if df is None or df.empty:
        st.warning("Dados financeiros não encontrados para este ticker.")
        return

    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")

    nome, website = get_company_info(ticker)
    price = get_price(ticker)

    st.markdown(
        """
        <style>
        .logo-box {
            background-color: #ffffff;
            border-radius: 10px;
            margin-bottom: 10px;
            display: flex;
            justify-content: center;
            justify-items: center;
            align-items: center;
            height: 100px;
            width: 100%;
            text-align: center;
            font-size: 20px;
            font-weight: bold;
            color: #333;
            background-color: #f9f9f9;
        }
        .metric-box {
            background-color: #f9f9f9;
            border-radius: 10px;
            padding: 10px;
            text-align: center;
            margin-bottom: 15px;
            border: 1px solid #e0e0e0;
        }
        .metric-value { font-size: 24px; font-weight: bold; color: #222; }
        .metric-label { font-size: 14px; color: #ff6600; font-weight: bold; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns([1, 4])
    with col1:
        st.image(get_logo_url(ticker), width=80)
    with col2:
        st.markdown(f"**Empresa:** {nome or '-'}")
        st.markdown(f"**Site:** {website or '-'}")
        st.markdown(f"**Preço atual (yfinance):** {('R$ ' + f'{price:,.2f}') if price else '-'}")

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # Crescimento (médio anual) — baseado no histórico do Supabase
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
    # Demonstrações Financeiras (NOVO) — gráficos do histórico
    # ─────────────────────────────────────────────────────────
    render_graficos_demonstracoes_financeiras(df, ticker)

    # ─────────────────────────────────────────────────────────
    # Indicadores Financeiros (cards) — DB + fallback silencioso
    # ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Indicadores Financeiros")

    mult_db_recent = load_multiplos_limitado_from_db(ticker, limite=12)
    mult_db_latest = _latest_row_by_date(mult_db_recent) if mult_db_recent is not None else pd.DataFrame()

    mult_yf_latest = None
    if _needs_yf_fundamentals(mult_db_latest):
        mult_yf_latest = get_fundamentals_yf(ticker)
        if isinstance(mult_yf_latest, dict):
            mult_yf_latest = pd.DataFrame([mult_yf_latest])

    multiplos_display = _merge_display_multiplos_db_primary(mult_db_latest, mult_yf_latest)

    descricoes = {
        "Margem Líquida": "Lucro Líquido ÷ Receita Líquida — quanto sobra do faturamento como lucro final.",
        "Margem Operacional": "EBIT ÷ Receita Líquida — eficiência operacional antes de juros e impostos.",
        "ROE": "Lucro Líquido ÷ Patrimônio Líquido — rentabilidade ao acionista.",
        "ROIC": "NOPAT ÷ Capital Investido — eficiência do capital operacional.",
        "Dividend Yield": "Dividendos por ação ÷ Preço da ação — rentabilidade via proventos.",
        "P/VP": "Preço ÷ Valor patrimonial por ação — quanto se paga pelo patrimônio.",
        "Payout": "Dividendos ÷ Lucro Líquido — parcela do lucro distribuída.",
        "P/L": "Preço ÷ Lucro por ação — quantos anos o lucro ‘paga’ o preço.",
        "Endividamento Total": "Dívida Total ÷ Patrimônio — grau de alavancagem financeira.",
        "Alavancagem Financeira": "Indicador de endividamento (depende da fonte).",
        "Liquidez Corrente": "Ativo Circulante ÷ Passivo Circulante — fôlego de curto prazo.",
    }

    valores = [
        ("Margem_Liquida", "Margem Líquida"),
        ("Margem_Operacional", "Margem Operacional"),
        ("ROE", "ROE"),
        ("ROIC", "ROIC"),
        ("DY", "Dividend Yield"),
        ("P/VP", "P/VP"),
        ("Payout", "Payout"),
        ("P/L", "P/L"),
        ("Endividamento_Total", "Endividamento Total"),
        ("Alavancagem_Financeira", "Alavancagem Financeira"),
        ("Liquidez_Corrente", "Liquidez Corrente"),
    ]

    c1, c2, c3 = st.columns(3)
    cols_cards = [c1, c2, c3]
    for i, (col_key, label) in enumerate(valores):
        v = None
        if multiplos_display is not None and not multiplos_display.empty and col_key in multiplos_display.columns:
            v = multiplos_display.iloc[0][col_key]
        with cols_cards[i % 3]:
            with st.expander(label, expanded=False):
                st.markdown(
                    f"""
                    <div class='metric-box'>
                        <div class='metric-value'>{_fmt_metric(label, v)}</div>
                        <div class='metric-label'><strong>{label}</strong></div>
                    </div>
                    <div style='font-size: 13px; color: #555;'>
                        {descricoes.get(label, "-")}
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
    else:
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
                dfm_mult = mult_hist.melt(
                    id_vars=["Data"],
                    value_vars=variaveis,
                    var_name="Indicador",
                    value_name="Valor",
                )
                dfm_mult["Indicador"] = dfm_mult["Indicador"].map(col_name_mapping)
                st.plotly_chart(
                    px.bar(dfm_mult, x="Data", y="Valor", color="Indicador", barmode="group"),
                    use_container_width=True,
                )
            else:
                st.info("Nenhuma variável válida selecionada.")

    # ─────────────────────────────────────────────────────────
    # Preço da ação (yfinance) + desempenho anual
    # ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Preço da Ação (Histórico via yfinance)")

    start_price = _infer_price_start_from_financials(df)

    with st.expander("Configurações do gráfico de preço", expanded=False):
        modo = st.radio(
            "Visualização",
            options=["Total", "Anual", "Mensal"],
            index=0,
            horizontal=True,
            key=f"price_view_mode_{ticker}",
        )
        lookback_anos = st.slider(
            "Anual: últimos N meses",
            min_value=3,
            max_value=24,
            value=12,
            step=1,
            key=f"price_lookback_y_{ticker}",
        )
        lookback_dias = st.slider(
            "Mensal: últimos N dias",
            min_value=7,
            max_value=120,
            value=30,
            step=1,
            key=f"price_lookback_m_{ticker}",
        )

    price_hist = _get_price_history_cached(ticker, start=start_price)
    if price_hist.empty:
        st.info("Não foi possível obter histórico de preços via yfinance para este ticker.")
        return

    s_plot = price_hist.copy()
    today = pd.Timestamp.today().normalize()

    if modo == "Anual":
        cutoff = today - pd.Timedelta(days=int(lookback_anos * 30.44))
        s_plot = s_plot[s_plot.index >= cutoff]
    elif modo == "Mensal":
        cutoff = today - pd.Timedelta(days=int(lookback_dias))
        s_plot = s_plot[s_plot.index >= cutoff]

    df_price_plot = s_plot.reset_index()
    df_price_plot.columns = ["Data", "Preço"]

    st.plotly_chart(
        px.line(df_price_plot, x="Data", y="Preço", title=f"{ticker} — {modo}"),
        use_container_width=True,
    )

    # ─────────────────────────────────────────────────────────
    # Tabela anual (PROFISSIONAL) — sem mudar cálculo, só exibição
    # ─────────────────────────────────────────────────────────
    st.markdown("#### Desempenho anual do preço (1º x último pregão do ano)")
    perf = _annual_price_performance(price_hist)

    # restringe a anos compatíveis com o histórico financeiro do Supabase, quando existir
    if df is not None and not df.empty and "Data" in df.columns:
        dd = pd.to_datetime(df["Data"], errors="coerce").dropna()
        if not dd.empty:
            y_min = int(dd.min().year)
            y_max = int(dd.max().year)
            perf = perf[(perf["Ano"] >= y_min) & (perf["Ano"] <= y_max)]

    if perf.empty:
        st.info("Não foi possível calcular o desempenho anual com o histórico disponível.")
        return

    perf_num = perf.copy()
    perf_num["Preço inicial"] = pd.to_numeric(perf_num["Preço inicial"], errors="coerce")
    perf_num["Preço final"] = pd.to_numeric(perf_num["Preço final"], errors="coerce")
    perf_num["Variação %"] = pd.to_numeric(perf_num["Variação %"], errors="coerce")

    def _brl(x):
        if x is None or (isinstance(x, float) and (pd.isna(x) or np.isinf(x))):
            return "-"
        try:
            return f"R$ {float(x):,.2f}"
        except Exception:
            return "-"

    def _pct(x):
        if x is None or (isinstance(x, float) and (pd.isna(x) or np.isinf(x))):
            return "-"
        try:
            return f"{float(x):+.2f}%"
        except Exception:
            return "-"

    def _color_return(v):
        try:
            if pd.isna(v):
                return ""
            return "color: #16a34a; font-weight: 700;" if float(v) >= 0 else "color: #dc2626; font-weight: 700;"
        except Exception:
            return ""

    max_abs = float(np.nanmax(np.abs(perf_num["Variação %"].values))) if perf_num["Variação %"].notna().any() else 1.0
    max_abs = max(max_abs, 1.0)

    def _bar_css(v):
        try:
            if pd.isna(v):
                return ""
            v = float(v)
            w = min(abs(v) / max_abs, 1.0) * 100.0
            if v >= 0:
                return f"background: linear-gradient(90deg, rgba(22,163,74,0.22) {w}%, transparent {w}%);"
            return f"background: linear-gradient(90deg, rgba(220,38,38,0.18) {w}%, transparent {w}%);"
        except Exception:
            return ""

    styler = (
        perf_num.style
        .format({
            "Ano": "{:d}",
            "Preço inicial": _brl,
            "Preço final": _brl,
            "Variação %": _pct,
        })
        .set_properties(**{
            "text-align": "right",
            "white-space": "nowrap",
            "font-size": "0.95rem",
        })
        .set_table_styles([
            {"selector": "th", "props": [("text-align", "right"), ("font-weight", "700")]},
            {"selector": "td", "props": [("padding", "6px 10px")]},
        ])
        .applymap(_color_return, subset=["Variação %"])
        .applymap(_bar_css, subset=["Variação %"])
    )

    st.dataframe(styler, use_container_width=True, hide_index=True)

    avg_yoy = float(np.nanmean(perf["Variação %"].values)) / 100.0
    cagr = _cagr_from_series(price_hist)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Média de variação anual", format_growth_rate(avg_yoy))
    with c2:
        st.metric("CAGR (crescimento composto)", format_growth_rate(cagr))
