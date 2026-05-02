from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

from core.helpers import get_company_info, get_logo_url
from core.ticker_utils import normalize_ticker
from core.ui_bridge import (
    load_data_from_db,
    load_multiplos_from_db,
    load_multiplos_limitado_from_db,
)
from core.yf_data import get_price, get_fundamentals_yf, coletar_dividendos

# Histórico de preços (yfinance)
try:
    from core.yf_data import baixar_precos
except Exception:
    baixar_precos = None  # type: ignore


# ─────────────────────────────────────────────────────────────
# Format helpers
# ─────────────────────────────────────────────────────────────
def format_brl(v) -> str:
    try:
        if v is None or (isinstance(v, float) and (pd.isna(v) or np.isinf(v))):
            return "-"
        return f"R$ {float(v):,.2f}"
    except Exception:
        return "-"


def format_brl_compacto(v) -> str:
    try:
        if v is None or (isinstance(v, float) and (pd.isna(v) or np.isinf(v))):
            return "-"
        x = float(v)
        ax = abs(x)
        if ax >= 1e9:
            return f"R$ {x/1e9:,.2f}B"
        if ax >= 1e6:
            return f"R$ {x/1e6:,.2f}M"
        if ax >= 1e3:
            return f"R$ {x/1e3:,.2f}K"
        return f"R$ {x:,.2f}"
    except Exception:
        return "-"


def format_percent(v, signed: bool = False) -> str:
    try:
        if v is None or (isinstance(v, float) and (pd.isna(v) or np.isinf(v))):
            return "-"
        x = float(v)
        if signed:
            return f"{x:+.2f}%"
        return f"{x:.2f}%"
    except Exception:
        return "-"




def _normalize_date_col(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    if "data" in out.columns and "Data" not in out.columns:
        out = out.rename(columns={"data": "Data"})
    return out


def _get_yf_dividend_series(ticker: str) -> pd.Series:
    try:
        div_map = coletar_dividendos([ticker])
        if isinstance(div_map, dict):
            keys = [str(ticker).strip().upper().replace('.SA', ''), str(ticker).strip().upper(), f"{str(ticker).strip().upper().replace('.SA', '')}.SA"]
            for key in keys:
                s = div_map.get(key)
                if isinstance(s, pd.Series) and not s.empty:
                    s = pd.to_numeric(s, errors="coerce").dropna()
                    s.index = pd.to_datetime(s.index, errors="coerce")
                    s = s[~s.index.isna()].sort_index()
                    return s
    except Exception:
        pass
    return pd.Series(dtype="float64")


def _enrich_dividendos_from_yf(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Preenche somente lacunas relevantes de dividendos sem alterar o layout existente."""
    df = _normalize_date_col(df)
    if df is None or df.empty or "Data" not in df.columns:
        return df

    out = df.copy()
    current = pd.to_numeric(out["Dividendos"], errors="coerce") if "Dividendos" in out.columns else pd.Series(dtype="float64")
    needs_fill = ("Dividendos" not in out.columns) or current.dropna().empty or (current.fillna(0) <= 0).all()
    if not needs_fill:
        return out

    s_div = _get_yf_dividend_series(ticker)
    if s_div.empty:
        if "Dividendos" not in out.columns:
            out["Dividendos"] = np.nan
        return out

    annual = pd.to_numeric(s_div, errors="coerce").dropna().groupby(s_div.index.year).sum()
    annual = annual[annual > 0]
    if annual.empty:
        if "Dividendos" not in out.columns:
            out["Dividendos"] = np.nan
        return out

    out["Data"] = pd.to_datetime(out["Data"], errors="coerce")
    mapped = out["Data"].dt.year.map(annual.to_dict())
    if "Dividendos" in out.columns:
        out["Dividendos"] = current.where(current.notna() & (current > 0), mapped)
    else:
        out["Dividendos"] = mapped
    return out

# ─────────────────────────────────────────────────────────────
# Crescimento (médio anual) com regressão em log
# ─────────────────────────────────────────────────────────────
def calculate_growth_rate(df: pd.DataFrame, column: str) -> float:
    try:
        df = _normalize_date_col(df)
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
# Demonstrações Financeiras — gráficos do histórico do Supabase
# (sem bloco "Últimos valores disponíveis")
# ─────────────────────────────────────────────────────────────
def render_graficos_demonstracoes_financeiras(df: pd.DataFrame, ticker: str) -> None:
    st.markdown("---")
    st.markdown("### Demonstrações Financeiras (Histórico do Banco)")

    df = _normalize_date_col(df)
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
        ("Lucro_Antes_IR", "Lucro Antes IR"),
        ("Resultado_Financeiro", "Resultado Financeiro"),
        ("Dividendos", "Dividendos"),
        ("Ativo_Total", "Ativo Total"),
        ("Patrimonio_Liquido", "Patrimônio Líquido"),
        ("Divida_Total", "Dívida Total"),
        ("Divida_Liquida", "Dívida Líquida"),
        ("Caixa_Liquido", "FCO (Caixa Operacional)"),
        ("FCI", "FCI (Investimento)"),
        ("FCF", "FCF (Fluxo Livre)"),
        ("Caixa", "Caixa"),
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


# ─────────────────────────────────────────────────────────────
# CSS / Header / Cards no padrão "Controle Financeiro"
# ─────────────────────────────────────────────────────────────
def _inject_cf_css() -> None:
    st.markdown(
        """
        <style>
          .cf-header{
            display:flex; justify-content:space-between; align-items:flex-start;
            padding: 6px 0 6px 0;
          }
          .cf-title{ margin:0; font-size: 34px; line-height: 1.1; }
          .cf-subtitle{ margin:8px 0 0 0; opacity:.85; }

          .cf-pill{
            display:inline-block;
            padding: 8px 12px;
            border-radius: 999px;
            border: 1px solid rgba(255,255,255,.14);
            background: rgba(255,255,255,.06);
            font-size: 12px;
            opacity: .95;
          }

          .cf-card{
            border-radius: 18px;
            padding: 14px 16px;
            border: 1px solid rgba(255,255,255,0.14);
            background: rgba(255,255,255,0.05);
            box-shadow: 0 0 0 1px rgba(255,255,255,0.06) inset;
            min-height: 112px;
          }
          .cf-card-label{
            font-size: 12px;
            letter-spacing: .10em;
            text-transform: uppercase;
            opacity: .85;
            margin-bottom: 6px;
          }
          .cf-card-value{
            font-size: 30px;
            font-weight: 850;
            line-height: 1.05;
            margin-bottom: 6px;
          }
          .cf-card-extra{
            font-size: 12px;
            opacity: .85;
            line-height: 1.25;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
          }

          .cf-card-income{ background: rgba(59,130,246,0.12); border-color: rgba(59,130,246,0.30); }
          .cf-card-expense{ background: rgba(245,158,11,0.12); border-color: rgba(245,158,11,0.30); }
          .cf-card-ratio{ background: rgba(148,163,184,0.10); border-color: rgba(148,163,184,0.24); }
          .cf-card-balance-positive{ background: rgba(34,197,94,0.12); border-color: rgba(34,197,94,0.30); }
          .cf-card-balance-negative{ background: rgba(239,68,68,0.12); border-color: rgba(239,68,68,0.30); }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _latest_financial_row(df: pd.DataFrame) -> pd.Series | None:
    df = _normalize_date_col(df)
    if df is None or df.empty or "Data" not in df.columns:
        return None
    tmp = df.copy()
    tmp["Data"] = pd.to_datetime(tmp["Data"], errors="coerce")
    tmp = tmp.dropna(subset=["Data"]).sort_values("Data")
    if tmp.empty:
        return None
    return tmp.iloc[-1]


def render_header_empresa(nome: str | None, website: str | None, price: float | None, ticker: str) -> None:
    st.markdown(
        f"""
        <div class="cf-header">
            <div>
                <h1 class="cf-title">📊 Empresa • <span style="opacity:.92">{ticker}</span></h1>
                <p class="cf-subtitle">
                    <strong>{(nome or "-")}</strong> • {(website or "-")} • Preço atual: <strong>{("R$ " + f"{price:,.2f}") if price else "-"}</strong>
                </p>
            </div>
            <div>
                <span class="cf-pill">Fonte de preço: yfinance • Dados financeiros: Supabase</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────
# Crescimento (médio anual) em blocos
# ─────────────────────────────────────────────────────────────
def render_cards_crescimento_supabase(df_fin: pd.DataFrame) -> None:
    st.markdown("---")
    st.markdown("### Crescimento (médio anual)")

    g_receita = calculate_growth_rate(df_fin, "Receita_Liquida")
    g_ebit = calculate_growth_rate(df_fin, "EBIT")
    g_lucro = calculate_growth_rate(df_fin, "Lucro_Liquido")
    g_divs = calculate_growth_rate(df_fin, "Dividendos")

    def _cls(v: float) -> str:
        if v is None or pd.isna(v) or np.isinf(v):
            return "cf-card-ratio"
        return "cf-card-balance-positive" if float(v) >= 0 else "cf-card-balance-negative"

    c1, c2, c3, c4 = st.columns(4)

    c1.markdown(
        f"""
        <div class="cf-card {_cls(g_receita)}">
            <div class="cf-card-label">Cresc. Receita (médio a.a.)</div>
            <div class="cf-card-value">{format_growth_rate(g_receita)}</div>
            <div class="cf-card-extra">Regressão em log no histórico do Supabase.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    c2.markdown(
        f"""
        <div class="cf-card {_cls(g_ebit)}">
            <div class="cf-card-label">Cresc. EBIT (médio a.a.)</div>
            <div class="cf-card-value">{format_growth_rate(g_ebit)}</div>
            <div class="cf-card-extra">Regressão em log no histórico do Supabase.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    c3.markdown(
        f"""
        <div class="cf-card {_cls(g_lucro)}">
            <div class="cf-card-label">Cresc. Lucro (médio a.a.)</div>
            <div class="cf-card-value">{format_growth_rate(g_lucro)}</div>
            <div class="cf-card-extra">Regressão em log no histórico do Supabase.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    c4.markdown(
        f"""
        <div class="cf-card {_cls(g_divs)}">
            <div class="cf-card-label">Cresc. Dividendos (médio a.a.)</div>
            <div class="cf-card-value">{format_growth_rate(g_divs)}</div>
            <div class="cf-card-extra">Regressão em log no histórico do Supabase.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_cards_resumo(df_fin: pd.DataFrame, perf_price: pd.DataFrame, avg_yoy: float, cagr: float) -> None:
    last = _latest_financial_row(df_fin)

    receita = float(last.get("Receita_Liquida")) if last is not None and pd.notna(last.get("Receita_Liquida")) else np.nan
    ebit = float(last.get("EBIT")) if last is not None and pd.notna(last.get("EBIT")) else np.nan
    lucro = float(last.get("Lucro_Liquido")) if last is not None and pd.notna(last.get("Lucro_Liquido")) else np.nan
    divs = float(last.get("Dividendos")) if last is not None and pd.notna(last.get("Dividendos")) else np.nan

    g_receita = calculate_growth_rate(df_fin, "Receita_Liquida")
    g_ebit = calculate_growth_rate(df_fin, "EBIT")
    g_lucro = calculate_growth_rate(df_fin, "Lucro_Liquido")
    g_divs = calculate_growth_rate(df_fin, "Dividendos")

    col1, col2, col3, col4 = st.columns(4)

    col1.markdown(
        f"""
        <div class="cf-card cf-card-income">
            <div class="cf-card-label">Receita (último)</div>
            <div class="cf-card-value">{format_brl_compacto(receita)}</div>
            <div class="cf-card-extra">Último valor disponível no banco.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    lucro_class = "cf-card-balance-positive" if (not pd.isna(lucro) and lucro >= 0) else "cf-card-balance-negative"
    col2.markdown(
        f"""
        <div class="cf-card {lucro_class}">
            <div class="cf-card-label">Lucro (último)</div>
            <div class="cf-card-value">{format_brl_compacto(lucro)}</div>
            <div class="cf-card-extra">Último valor disponível no banco.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col3.markdown(
        f"""
        <div class="cf-card cf-card-expense">
            <div class="cf-card-label">Dividendos (último)</div>
            <div class="cf-card-value">{format_brl_compacto(divs)}</div>
            <div class="cf-card-extra">Último valor disponível no banco.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    cagr_class = "cf-card-balance-positive" if (not pd.isna(cagr) and cagr >= 0) else "cf-card-balance-negative"
    col4.markdown(
        f"""
        <div class="cf-card {cagr_class}">
            <div class="cf-card-label">CAGR (preço)</div>
            <div class="cf-card-value">{format_growth_rate(cagr)}</div>
            <div class="cf-card-extra">Crescimento composto do preço no período.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col5, col6, col7, col8 = st.columns(4)

    r1_class = "cf-card-balance-positive" if (not pd.isna(g_receita) and g_receita >= 0) else "cf-card-balance-negative"
    col5.markdown(
        f"""
        <div class="cf-card {r1_class}">
            <div class="cf-card-label">Cresc. Receita (médio a.a.)</div>
            <div class="cf-card-value">{format_growth_rate(g_receita)}</div>
            <div class="cf-card-extra">Base: histórico no Supabase.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    r2_class = "cf-card-balance-positive" if (not pd.isna(g_ebit) and g_ebit >= 0) else "cf-card-balance-negative"
    col6.markdown(
        f"""
        <div class="cf-card {r2_class}">
            <div class="cf-card-label">Cresc. EBIT (médio a.a.)</div>
            <div class="cf-card-value">{format_growth_rate(g_ebit)}</div>
            <div class="cf-card-extra">Base: histórico no Supabase.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    r3_class = "cf-card-balance-positive" if (not pd.isna(g_lucro) and g_lucro >= 0) else "cf-card-balance-negative"
    col7.markdown(
        f"""
        <div class="cf-card {r3_class}">
            <div class="cf-card-label">Cresc. Lucro (médio a.a.)</div>
            <div class="cf-card-value">{format_growth_rate(g_lucro)}</div>
            <div class="cf-card-extra">Base: histórico no Supabase.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    yoy_class = "cf-card-balance-positive" if (not pd.isna(avg_yoy) and avg_yoy >= 0) else "cf-card-balance-negative"
    col8.markdown(
        f"""
        <div class="cf-card {yoy_class}">
            <div class="cf-card-label">Média variação anual (preço)</div>
            <div class="cf-card-value">{format_growth_rate(avg_yoy)}</div>
            <div class="cf-card-extra">1º × último pregão por ano.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col9, col10, col11, col12 = st.columns(4)

    col9.markdown(
        f"""
        <div class="cf-card cf-card-ratio">
            <div class="cf-card-label">EBIT (último)</div>
            <div class="cf-card-value">{format_brl_compacto(ebit)}</div>
            <div class="cf-card-extra">Último valor disponível no banco.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    div_class = "cf-card-balance-positive" if (not pd.isna(g_divs) and g_divs >= 0) else "cf-card-balance-negative"
    col10.markdown(
        f"""
        <div class="cf-card {div_class}">
            <div class="cf-card-label">Cresc. Dividendos (médio a.a.)</div>
            <div class="cf-card-value">{format_growth_rate(g_divs)}</div>
            <div class="cf-card-extra">Base: histórico no Supabase.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col11.markdown(
        f"""
        <div class="cf-card cf-card-ratio">
            <div class="cf-card-label">Janela de preço (anos)</div>
            <div class="cf-card-value">{int(perf_price["Ano"].nunique()) if perf_price is not None and not perf_price.empty else "-"}</div>
            <div class="cf-card-extra">Anos com cálculo anual disponível.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col12.markdown(
        f"""
        <div class="cf-card cf-card-ratio">
            <div class="cf-card-label">Base</div>
            <div class="cf-card-value">Supabase + YF</div>
            <div class="cf-card-extra">Financeiro do banco + preços via yfinance.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────
# Preço (yfinance) — histórico + retornos anuais
# ─────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=60 * 60)  # 1h
def _get_price_history_cached(ticker: str, start: str) -> pd.Series:
    if baixar_precos is None:
        return pd.Series(dtype="float64")

    dfp = baixar_precos(ticker, start=start)
    if dfp is None or dfp.empty:
        return pd.Series(dtype="float64")

    col = normalize_ticker(ticker)
    if col not in dfp.columns:
        col = dfp.columns[0]

    s = pd.to_numeric(dfp[col], errors="coerce").dropna()
    s.index = pd.to_datetime(s.index, errors="coerce")
    s = s.dropna().sort_index()
    s.name = "Preço"
    return s


def _infer_price_start_from_financials(df_fin: pd.DataFrame) -> str:
    df_fin = _normalize_date_col(df_fin)
    if df_fin is None or df_fin.empty or "Data" not in df_fin.columns:
        return "2010-01-01"
    d = pd.to_datetime(df_fin["Data"], errors="coerce").dropna()
    if d.empty:
        return "2010-01-01"
    y = max(int(d.min().year) - 1, 1990)
    return f"{y}-01-01"


def _annual_price_performance(price: pd.Series) -> pd.DataFrame:
    if price is None or price.empty:
        return pd.DataFrame(columns=["Ano", "Preço inicial", "Preço final", "Variação %"])

    s = price.dropna().sort_index()
    dfp = s.to_frame("close")
    dfp["Ano"] = dfp.index.year

    grp = dfp.groupby("Ano")["close"]
    ini = grp.first()
    fim = grp.last()

    out = pd.DataFrame({"Ano": ini.index.astype(int), "Preço inicial": ini.values, "Preço final": fim.values})
    out["Variação %"] = (out["Preço final"] / out["Preço inicial"] - 1.0) * 100.0
    return out.sort_values("Ano").reset_index(drop=True)


def _cagr_from_series(price: pd.Series) -> float:
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
    out = _normalize_date_col(df)
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


def _merge_display_multiplos_db_primary(db_latest: pd.DataFrame, yf_latest: pd.DataFrame | None) -> pd.DataFrame:
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


# ─────────────────────────────────────────────────────────────
# NOVO: gráfico de barras divergente (retorno anual)
# ─────────────────────────────────────────────────────────────
def render_grafico_retorno_anual_barras(perf: pd.DataFrame, ticker: str) -> None:
    st.markdown("#### Retorno anual do preço (ganho à direita / perda à esquerda)")

    if perf is None or perf.empty or "Ano" not in perf.columns or "Variação %" not in perf.columns:
        st.info("Sem dados suficientes para exibir o retorno anual em barras.")
        return

    d = perf.copy()
    d["Variação %"] = pd.to_numeric(d["Variação %"], errors="coerce")
    d = d.dropna(subset=["Variação %", "Ano"])
    if d.empty:
        st.info("Sem dados suficientes para exibir o retorno anual em barras.")
        return

    d["Ano"] = d["Ano"].astype(int)
    d = d.sort_values("Ano")
    d["Sinal"] = np.where(d["Variação %"] >= 0, "Ganho", "Perda")

    # Para ficar “bonito” e legível
    max_abs = float(np.nanmax(np.abs(d["Variação %"].values))) if d["Variação %"].notna().any() else 10.0
    max_abs = max(max_abs, 10.0)

    fig = px.bar(
        d,
        y="Ano",
        x="Variação %",
        orientation="h",
        color="Sinal",
        color_discrete_map={"Ganho": "#22c55e", "Perda": "#ef4444"},
        text=d["Variação %"].map(lambda v: f"{v:+.2f}%"),
    )

    fig.update_traces(textposition="outside", cliponaxis=False)
    fig.update_layout(
        height=min(520, 260 + 18 * d.shape[0]),
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_title="Retorno anual (%)",
        yaxis_title="Ano",
        legend_title_text="",
    )
    fig.update_xaxes(range=[-max_abs * 1.15, max_abs * 1.15], zeroline=True, zerolinewidth=2)
    fig.update_yaxes(categoryorder="category ascending")

    st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────────────────
# Fluxo de Caixa (FCO / FCI / FCF)
# ─────────────────────────────────────────────────────────────
def render_fluxo_de_caixa(df: pd.DataFrame, ticker: str) -> None:
    st.markdown("---")
    st.markdown("### 💰 Fluxo de Caixa")

    df = _normalize_date_col(df)
    if df is None or df.empty or "Data" not in df.columns:
        st.info("Sem dados de fluxo de caixa disponíveis.")
        return

    dff = df.copy()
    dff["Data"] = pd.to_datetime(dff["Data"], errors="coerce")
    dff = dff.dropna(subset=["Data"]).sort_values("Data")

    candidatos = [
        ("Caixa_Liquido", "FCO (Operacional)"),
        ("FCI", "FCI (Investimento)"),
        ("FCF", "FCF (Livre)"),
    ]
    existentes = [(c, lbl) for c, lbl in candidatos if c in dff.columns]
    if not existentes:
        st.info("Colunas FCO/FCI/FCF não disponíveis neste ticker.")
        return

    last = _latest_financial_row(dff)
    card_cols = st.columns(len(existentes))
    for i, (col, lbl) in enumerate(existentes):
        v = float(last[col]) if last is not None and col in last and pd.notna(last.get(col)) else np.nan
        v_fin = v if np.isfinite(v) else np.nan
        if np.isnan(v_fin):
            cls = "cf-card-ratio"
        elif v_fin >= 0:
            cls = "cf-card-balance-positive"
        else:
            cls = "cf-card-balance-negative"
        card_cols[i].markdown(
            f"""<div class="cf-card {cls}">
                <div class="cf-card-label">{lbl} (último)</div>
                <div class="cf-card-value">{format_brl_compacto(v_fin)}</div>
                <div class="cf-card-extra">Fonte: Demonstrações Financeiras (Supabase)</div>
            </div>""",
            unsafe_allow_html=True,
        )

    cols_plot = [c for c, _ in existentes]
    plot = dff[["Data"] + cols_plot].copy()
    for c in cols_plot:
        plot[c] = pd.to_numeric(plot[c], errors="coerce")
    lbl_map = {c: lbl for c, lbl in existentes}
    melt = plot.melt(id_vars=["Data"], value_vars=cols_plot, var_name="Fluxo", value_name="Valor (R$)")
    melt["Fluxo"] = melt["Fluxo"].map(lbl_map)
    melt = melt.dropna(subset=["Valor (R$)"])

    if not melt.empty:
        fig = px.bar(
            melt, x="Data", y="Valor (R$)", color="Fluxo", barmode="group",
            color_discrete_map={
                "FCO (Operacional)": "#22c55e",
                "FCI (Investimento)": "#f97316",
                "FCF (Livre)": "#3b82f6",
            },
        )
        fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.25)")
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
    st.caption("FCO = Caixa das operações · FCI = Caixa dos investimentos · FCF = FCO + FCI (fluxo livre de caixa)")


# ─────────────────────────────────────────────────────────────
# Estrutura de Capital (Caixa vs Dívida CP vs LP)
# ─────────────────────────────────────────────────────────────
def render_estrutura_capital(df: pd.DataFrame, ticker: str) -> None:
    st.markdown("---")
    st.markdown("### 🏛️ Estrutura de Capital e Dívida")

    df = _normalize_date_col(df)
    if df is None or df.empty or "Data" not in df.columns:
        st.info("Sem dados de estrutura de capital disponíveis.")
        return

    dff = df.copy()
    dff["Data"] = pd.to_datetime(dff["Data"], errors="coerce")
    dff = dff.dropna(subset=["Data"]).sort_values("Data")
    last = _latest_financial_row(dff)

    cards_cfg = [
        ("Caixa",              "Caixa",              "cf-card-balance-positive"),
        ("Divida_CP",          "Dívida CP",           "cf-card-expense"),
        ("Divida_LP",          "Dívida LP",           "cf-card-balance-negative"),
        ("Divida_Total",       "Dívida Total",        "cf-card-balance-negative"),
        ("Divida_Liquida",     "Dívida Líquida",      "cf-card-ratio"),
        ("Patrimonio_Liquido", "Patrimônio Líquido",  "cf-card-income"),
    ]
    existentes_cards = [(c, lbl, cls) for c, lbl, cls in cards_cfg if c in dff.columns]

    if existentes_cards:
        card_cols = st.columns(min(len(existentes_cards), 6))
        for i, (col, lbl, cls) in enumerate(existentes_cards):
            v = float(last[col]) if last is not None and col in last and pd.notna(last.get(col)) else np.nan
            card_cols[i % len(card_cols)].markdown(
                f"""<div class="cf-card {cls}">
                    <div class="cf-card-label">{lbl}</div>
                    <div class="cf-card-value">{format_brl_compacto(v)}</div>
                    <div class="cf-card-extra">Último período disponível</div>
                </div>""",
                unsafe_allow_html=True,
            )

    plot_candidatos = [
        ("Caixa",    "Caixa"),
        ("Divida_CP","Dívida CP"),
        ("Divida_LP","Dívida LP"),
    ]
    existentes_plot = [(c, lbl) for c, lbl in plot_candidatos if c in dff.columns]
    if len(existentes_plot) >= 2:
        cols_p = [c for c, _ in existentes_plot]
        plot = dff[["Data"] + cols_p].copy()
        for c in cols_p:
            plot[c] = pd.to_numeric(plot[c], errors="coerce")
        lbl_map = {c: lbl for c, lbl in existentes_plot}
        melt = plot.melt(id_vars=["Data"], value_vars=cols_p, var_name="Item", value_name="Valor (R$)")
        melt["Item"] = melt["Item"].map(lbl_map)
        melt = melt.dropna(subset=["Valor (R$)"])
        if not melt.empty:
            fig = px.bar(
                melt, x="Data", y="Valor (R$)", color="Item", barmode="group",
                color_discrete_map={"Caixa": "#22c55e", "Dívida CP": "#f97316", "Dívida LP": "#ef4444"},
            )
            fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)
    st.caption("CP = Curto Prazo (vence em 12 meses) · LP = Longo Prazo")


# ─────────────────────────────────────────────────────────────
# Liquidez & Eficiência Operacional (novos múltiplos)
# ─────────────────────────────────────────────────────────────
def render_liquidez_eficiencia(multiplos_display: pd.DataFrame) -> None:
    st.markdown("---")
    st.markdown("### 🔍 Liquidez & Eficiência Operacional")

    descricoes = {
        "Liquidez_Seca":             "Ativo Circulante sem Estoques ÷ Passivo Circulante — liquidez real sem depender de estoques.",
        "Liquidez_Imediata":         "Caixa ÷ Passivo Circulante — capacidade de quitar dívidas CP só com caixa disponível.",
        "Giro_Ativo":                "Receita Líquida ÷ Ativo Total — eficiência no uso de todos os ativos para gerar receita.",
        "Prazo_Medio_Recebimento":   "Contas a Receber ÷ Receita × 365 — dias médios que a empresa demora a receber das vendas.",
        "NCG":                       "Contas a Receber + Estoques − Fornecedores — capital preso no ciclo operacional.",
    }

    valores = [
        ("Liquidez_Seca",           "Liquidez Seca",              "x"),
        ("Liquidez_Imediata",       "Liquidez Imediata",          "x"),
        ("Giro_Ativo",              "Giro do Ativo",              "x"),
        ("Prazo_Medio_Recebimento", "Prazo Médio Recebimento",    "dias"),
        ("NCG",                     "Necessidade Capital de Giro", "R$"),
    ]

    if multiplos_display is None or multiplos_display.empty:
        st.info("Indicadores de liquidez e eficiência não disponíveis.")
        return

    existentes = [(c, lbl, fmt) for c, lbl, fmt in valores if c in multiplos_display.columns]
    if not existentes:
        st.info("Indicadores de liquidez e eficiência não disponíveis neste ticker.")
        return

    cols = st.columns(min(len(existentes), 5))
    for i, (col_key, label, fmt) in enumerate(existentes):
        v = multiplos_display.iloc[0].get(col_key)
        try:
            fv_num = float(v) if v is not None and pd.notna(v) else None
        except Exception:
            fv_num = None
        if fv_num is None or not np.isfinite(fv_num):
            fv = "-"
        elif fmt == "dias":
            fv = f"{fv_num:.1f} dias"
        elif fmt == "R$":
            fv = format_brl_compacto(fv_num)
        else:
            fv = f"{fv_num:.2f}x"

        with cols[i % len(cols)]:
            with st.expander(label, expanded=False):
                st.markdown(
                    f"""<div style="border-radius:14px;padding:10px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.12)">
                        <div style="font-size:22px;font-weight:800">{fv}</div>
                        <div style="font-size:12px;opacity:.9;margin-top:6px">{descricoes.get(col_key, "")}</div>
                    </div>""",
                    unsafe_allow_html=True,
                )


# ─────────────────────────────────────────────────────────────
# Qualidade e Geração de Caixa + Valuation Avançado
# ─────────────────────────────────────────────────────────────
def render_geracao_caixa_e_valuation(multiplos_display: pd.DataFrame, mult_hist: pd.DataFrame, ticker: str) -> None:
    st.markdown("---")
    st.markdown("### 🧾 Qualidade de Caixa & Valuation Avançado")

    if multiplos_display is None or multiplos_display.empty:
        st.info("Indicadores de geração de caixa não disponíveis.")
        return

    descricoes = {
        "Margem_FCO":            "FCO ÷ Receita Líquida — quanto da receita vira caixa operacional real (mais difícil de manipular que o lucro).",
        "FCO_sobre_Divida":      "FCO ÷ Dívida Bruta — em quantos anos o FCO quita toda a dívida. >0,3 é saudável.",
        "Cobertura_Investimento":"FCO ÷ |FCI| — FCO cobre quantas vezes os investimentos realizados. >1 é positivo.",
        "P_FCO":                 "Preço ÷ FCO por ação — versão 'cash' do P/L, mais robusta a accruals contábeis.",
        "EV_EBIT":               "Valor da Firma ÷ EBIT — valuation operacional, neutro à estrutura de capital.",
        "P_Receita":             "Preço ÷ Receita por ação — útil para empresas em crescimento sem lucro ainda.",
    }

    valores = [
        ("Margem_FCO",            "Margem FCO",           True,  "%"),
        ("FCO_sobre_Divida",      "FCO / Dívida",         True,  "x"),
        ("Cobertura_Investimento","Cobertura Investimento",True,  "x"),
        ("P_FCO",                 "P/FCO",                False, "x"),
        ("EV_EBIT",               "EV/EBIT",              False, "x"),
        ("P_Receita",             "P/Receita",            False, "x"),
    ]

    existentes = [(c, lbl, bom_alto, fmt) for c, lbl, bom_alto, fmt in valores if c in multiplos_display.columns]

    if not existentes:
        st.info("Indicadores de geração de caixa não disponíveis neste ticker.")
    else:
        cols = st.columns(min(len(existentes), 3))
        for i, (col_key, label, bom_alto, fmt) in enumerate(existentes):
            v = multiplos_display.iloc[0].get(col_key)
            try:
                fv_num = float(v) if v is not None and pd.notna(v) else None
            except Exception:
                fv_num = None
            if fv_num is None or not np.isfinite(fv_num):
                fv = "-"
            elif fmt == "%":
                fv = f"{fv_num:.2f}%"
            else:
                fv = f"{fv_num:.2f}x"

            with cols[i % len(cols)]:
                with st.expander(label, expanded=False):
                    st.markdown(
                        f"""<div style="border-radius:14px;padding:10px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.12)">
                            <div style="font-size:22px;font-weight:800">{fv}</div>
                            <div style="font-size:12px;opacity:.9;margin-top:6px">{descricoes.get(col_key, "")}</div>
                        </div>""",
                        unsafe_allow_html=True,
                    )

    # Comparação histórica: Margem FCO vs Margem Líquida
    if mult_hist is not None and not mult_hist.empty:
        cols_cmp = [c for c in ["Margem_FCO", "Margem_Liquida", "Data"] if c in mult_hist.columns]
        if "Data" in cols_cmp and ("Margem_FCO" in cols_cmp or "Margem_Liquida" in cols_cmp):
            dfc = mult_hist[cols_cmp].copy()
            dfc["Data"] = pd.to_datetime(dfc["Data"], errors="coerce")
            dfc = dfc.dropna(subset=["Data"]).sort_values("Data")
            melt_cols = [c for c in ["Margem_FCO", "Margem_Liquida"] if c in dfc.columns]
            if melt_cols:
                melt = dfc.melt(id_vars=["Data"], value_vars=melt_cols, var_name="Indicador", value_name="Margem (%)")
                lbl_map = {"Margem_FCO": "Margem FCO (caixa)", "Margem_Liquida": "Margem Líquida (contábil)"}
                melt["Indicador"] = melt["Indicador"].map(lbl_map)
                melt = melt.dropna(subset=["Margem (%)"])
                if not melt.empty:
                    st.markdown("#### Margem FCO vs Margem Líquida — Caixa vs Contábil")
                    fig = px.line(melt, x="Data", y="Margem (%)", color="Indicador", markers=True,
                                  color_discrete_map={
                                      "Margem FCO (caixa)": "#22c55e",
                                      "Margem Líquida (contábil)": "#3b82f6",
                                  })
                    fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.25)")
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                    st.caption("💡 Quando a Margem FCO acompanha (ou supera) a Margem Líquida, os lucros estão se convertendo em caixa real — sinal de alta qualidade dos resultados.")


# ─────────────────────────────────────────────────────────────
# Valuation Histórico — Bandas dos múltiplos-chave
# ─────────────────────────────────────────────────────────────
def _fmt_band_val(v: float | None, fmt: str) -> str:
    """Formata valor de múltiplo para exibição em bandas."""
    if v is None or not np.isfinite(v):
        return "-"
    if fmt == "%":
        return f"{v * 100:.1f}%"
    return f"{v:.1f}x"


def render_valuation_historico(ticker: str, mult_hist: pd.DataFrame) -> None:
    """Exibe P/L, EV/EBIT, ROIC e DY atuais vs. bandas históricas de 5 anos."""
    import plotly.graph_objects as go

    st.markdown("---")
    st.markdown("### 🎯 Valuation Histórico — Onde o múltiplo está agora vs. o passado")

    if mult_hist is None or mult_hist.empty:
        st.info("Histórico de múltiplos insuficiente para análise de bandas.")
        return

    mult_hist = _normalize_date_col(mult_hist.copy())
    if "Data" not in mult_hist.columns:
        st.info("Coluna de data ausente no histórico de múltiplos.")
        return

    mult_hist["Data"] = pd.to_datetime(mult_hist["Data"], errors="coerce")
    mult_hist = mult_hist.dropna(subset=["Data"]).sort_values("Data")

    cutoff_5y = pd.Timestamp.today() - pd.DateOffset(years=5)
    hist_5y = mult_hist[mult_hist["Data"] >= cutoff_5y].copy()

    if hist_5y.empty:
        st.info("Histórico de 5 anos ainda não disponível para este ticker.")
        return

    latest = mult_hist.iloc[-1]

    # (coluna_db, label_exibição, bom_baixo?, fmt)
    metricas_cfg = [
        ("P/L",    "P/L",       True,  "x"),
        ("EV_EBIT","EV/EBIT",   True,  "x"),
        ("ROIC",   "ROIC",      False, "%"),
        ("DY",     "DY",        False, "%"),
        ("P/VP",   "P/VP",      True,  "x"),
        ("Margem_Liquida", "Margem Líq.", False, "%"),
    ]
    metricas_disp = [
        (c, lbl, bl, fmt)
        for c, lbl, bl, fmt in metricas_cfg
        if c in hist_5y.columns
    ]

    if not metricas_disp:
        st.info("Colunas de múltiplos (P/L, EV/EBIT, ROIC, DY…) não encontradas no histórico.")
        return

    st.caption(
        "💡 **Bandas históricas (5 anos):** cinza claro = mín-máx · cinza médio = P25-P75 (intervalo interquartil) · "
        "tracejado branco = mediana · ◆ colorido = valor atual. "
        "🟢 Atrativo indica múltiplo abaixo da mediana para indicadores de preço (P/L, EV/EBIT…) "
        "ou acima dela para rentabilidade (ROIC, DY, Margem)."
    )

    n_cols = min(len(metricas_disp), 3)
    cols = st.columns(n_cols)

    for idx, (col_key, label, bom_baixo, fmt) in enumerate(metricas_disp):
        serie = pd.to_numeric(hist_5y[col_key], errors="coerce").dropna()
        if serie.empty:
            continue

        # Normalizar percentuais armazenados como > 1 (ex: 6.5 → 0.065)
        if fmt == "%" and serie.abs().median() > 1.0:
            serie = serie / 100.0

        p_min  = float(serie.min())
        p25    = float(serie.quantile(0.25))
        median = float(serie.median())
        p75    = float(serie.quantile(0.75))
        p_max  = float(serie.max())

        v_raw = latest.get(col_key)
        try:
            v_atual = float(v_raw) if v_raw is not None and pd.notna(v_raw) else None
            if v_atual is not None and fmt == "%" and abs(v_atual) > 1.0:
                v_atual = v_atual / 100.0
        except Exception:
            v_atual = None

        if v_atual is not None and np.isfinite(v_atual):
            atrativo = (v_atual < median) if bom_baixo else (v_atual > median)
            bar_color = "#22c55e" if atrativo else "#f97316"
            signal    = "🟢 Atrativo" if atrativo else "🔴 Esticado"
        else:
            atrativo  = None
            bar_color = "#94a3b8"
            signal    = "⚪ Sem dado"

        atual_str = _fmt_band_val(v_atual, fmt)
        span = max(p_max - p_min, 1e-9)
        pad  = span * 0.12

        fig = go.Figure()

        # Banda total (mín–máx)
        fig.add_shape(type="rect", x0=p_min, x1=p_max, y0=0.1, y1=0.9,
                      fillcolor="rgba(148,163,184,0.15)", line_width=0)
        # IQR (P25–P75)
        fig.add_shape(type="rect", x0=p25, x1=p75, y0=0.1, y1=0.9,
                      fillcolor="rgba(148,163,184,0.35)", line_width=0)
        # Mediana
        fig.add_shape(type="line", x0=median, x1=median, y0=0.05, y1=0.95,
                      line=dict(color="rgba(255,255,255,0.55)", dash="dot", width=1.5))

        # Valor atual
        if v_atual is not None and np.isfinite(v_atual):
            fig.add_shape(type="line", x0=v_atual, x1=v_atual, y0=0.0, y1=1.0,
                          line=dict(color=bar_color, width=2.5))
            fig.add_trace(go.Scatter(
                x=[v_atual], y=[0.5],
                mode="markers",
                marker=dict(color=bar_color, size=13, symbol="diamond",
                            line=dict(color="white", width=1.5)),
                showlegend=False,
                hovertemplate=f"Atual: {atual_str}<extra></extra>",
            ))

        # Anotações mín / med / máx
        for val, anchor, txt in [
            (p_min,  "left",   f"Mín<br>{_fmt_band_val(p_min,  fmt)}"),
            (median, "center", f"Med<br>{_fmt_band_val(median, fmt)}"),
            (p_max,  "right",  f"Máx<br>{_fmt_band_val(p_max,  fmt)}"),
        ]:
            fig.add_annotation(x=val, y=1.25, text=txt, showarrow=False,
                                font=dict(size=8, color="rgba(255,255,255,0.55)"),
                                xanchor=anchor, align=anchor)

        fig.update_layout(
            height=120,
            margin=dict(l=5, r=5, t=35, b=5),
            xaxis=dict(visible=False, range=[p_min - pad, p_max + pad]),
            yaxis=dict(visible=False, range=[-0.2, 1.6]),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )

        with cols[idx % n_cols]:
            st.markdown(
                f"""<div style="background:rgba(255,255,255,.04);border-radius:12px;
                    padding:10px 14px 4px;border:1px solid rgba(255,255,255,.1);margin-bottom:4px">
                    <div style="font-size:12px;opacity:.65;font-weight:600;letter-spacing:.5px">{label}</div>
                    <div style="font-size:26px;font-weight:800;color:{bar_color};line-height:1.1">{atual_str}</div>
                    <div style="font-size:11px;opacity:.55;margin-top:2px">{signal}</div>
                </div>""",
                unsafe_allow_html=True,
            )
            st.plotly_chart(fig, use_container_width=True, key=f"vh_{ticker}_{col_key}_{idx}")

    # ── Série temporal dos múltiplos selecionados ──────────────
    st.markdown("#### 📈 Evolução histórica dos múltiplos")
    col_opts = [c for c, _, _, _ in metricas_disp if c in hist_5y.columns]
    default_ts = col_opts[:2]
    sel_ts = st.multiselect(
        "Indicadores para o gráfico de série temporal:",
        options=col_opts,
        default=default_ts,
        key=f"vh_ts_{ticker}",
    )
    if sel_ts:
        df_ts = hist_5y[["Data"] + sel_ts].copy()
        # Normalizar percentuais
        for c, _, _, fmt_c in metricas_disp:
            if c in sel_ts and fmt_c == "%":
                col_vals = pd.to_numeric(df_ts[c], errors="coerce")
                if col_vals.abs().median() > 1.0:
                    df_ts[c] = col_vals / 100.0
        df_melt = df_ts.melt(id_vars=["Data"], value_vars=sel_ts,
                              var_name="Múltiplo", value_name="Valor")
        df_melt = df_melt.dropna(subset=["Valor"])
        if not df_melt.empty:
            fig_ts = px.line(df_melt, x="Data", y="Valor", color="Múltiplo",
                             markers=True,
                             color_discrete_sequence=px.colors.qualitative.Pastel)
            fig_ts.update_layout(
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            fig_ts.update_xaxes(showgrid=False)
            fig_ts.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.07)")
            st.plotly_chart(fig_ts, use_container_width=True)
        else:
            st.info("Dados insuficientes para o gráfico temporal.")


# ─────────────────────────────────────────────────────────────
# View principal
# ─────────────────────────────────────────────────────────────
def render_empresa_view(ticker: str) -> None:
    _inject_cf_css()

    df = load_data_from_db(ticker)
    if df is None or df.empty:
        st.warning("Dados financeiros não encontrados para este ticker.")
        return

    df = _normalize_date_col(df)
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")

    df = _enrich_dividendos_from_yf(df, ticker)

    nome, website = get_company_info(ticker)
    price_now = get_price(ticker)

    render_header_empresa(nome, website, price_now, ticker)

    colL, colR = st.columns([1, 5])
    with colL:
        st.image(get_logo_url(ticker), width=80)
    with colR:
        st.caption(" ")

    # Crescimento (médio anual) em blocos
    render_cards_crescimento_supabase(df)

    # Demonstrações Financeiras (gráfico)
    render_graficos_demonstracoes_financeiras(df, ticker)

    st.markdown("---")
    st.markdown("### Indicadores Financeiros")

    mult_db_recent = load_multiplos_limitado_from_db(ticker, limite=12)
    mult_db_recent = _normalize_date_col(mult_db_recent) if mult_db_recent is not None else pd.DataFrame()
    mult_db_latest = _latest_row_by_date(mult_db_recent) if mult_db_recent is not None else pd.DataFrame()

    mult_yf_latest = None
    if _needs_yf_fundamentals(mult_db_latest):
        mult_yf_latest = get_fundamentals_yf(ticker)
        if isinstance(mult_yf_latest, dict):
            mult_yf_latest = pd.DataFrame([mult_yf_latest])

    multiplos_display = _merge_display_multiplos_db_primary(mult_db_latest, mult_yf_latest)

    descricoes = {
        "Margem Líquida":        "Lucro Líquido ÷ Receita Líquida — quanto sobra do faturamento como lucro final.",
        "Margem Operacional":    "EBIT ÷ Receita Líquida — eficiência operacional antes de juros e impostos.",
        "ROE":                   "Lucro Líquido ÷ Patrimônio Líquido — rentabilidade ao acionista.",
        "ROA":                   "Lucro Líquido ÷ Ativo Total — eficiência no uso de todos os ativos.",
        "ROIC":                  "NOPAT ÷ Capital Investido — eficiência do capital operacional.",
        "Dividend Yield":        "Dividendos por ação ÷ Preço da ação — rentabilidade via proventos.",
        "P/VP":                  "Preço ÷ Valor patrimonial por ação — quanto se paga pelo patrimônio.",
        "Payout":                "Dividendos ÷ Lucro Líquido — parcela do lucro distribuída.",
        "P/L":                   "Preço ÷ Lucro por ação — quantos anos o lucro ‘paga’ o preço.",
        "Endividamento Total":   "Dívida Total ÷ Patrimônio — grau de alavancagem financeira.",
        "Alavancagem Financeira":"Dívida Líquida ÷ EBITDA — anos de geração operacional para quitar dívida.",
        "Liquidez Corrente":     "Ativo Circulante ÷ Passivo Circulante — fôlego de curto prazo.",
    }

    valores = [
        ("Margem_Liquida",       "Margem Líquida"),
        ("Margem_Operacional",   "Margem Operacional"),
        ("ROE",                  "ROE"),
        ("ROA",                  "ROA"),
        ("ROIC",                 "ROIC"),
        ("DY",                   "Dividend Yield"),
        ("P/VP",                 "P/VP"),
        ("Payout",               "Payout"),
        ("P/L",                  "P/L"),
        ("Endividamento_Total",  "Endividamento Total"),
        ("Alavancagem_Financeira","Alavancagem Financeira"),
        ("Liquidez_Corrente",    "Liquidez Corrente"),
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
                    <div style="border-radius:14px;padding:10px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.12)">
                        <div style="font-size:22px;font-weight:800">{_fmt_metric(label, v)}</div>
                        <div style="font-size:12px;opacity:.9;margin-top:6px">{descricoes.get(label, "-")}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    # ── Novas secções com dados já no banco ──────────────────────
    render_liquidez_eficiencia(multiplos_display)

    mult_hist = load_multiplos_from_db(ticker)
    render_geracao_caixa_e_valuation(multiplos_display, mult_hist, ticker)
    render_fluxo_de_caixa(df, ticker)
    render_estrutura_capital(df, ticker)

    # ── Valuation Histórico (bandas de 5 anos) ────────────────
    render_valuation_historico(ticker, mult_hist)

    st.markdown("---")
    st.markdown("### Gráfico de Múltiplos (Histórico do Banco)")
    if mult_hist is None or mult_hist.empty:
        st.info("Histórico de múltiplos não encontrado no banco.")
    else:
        mult_hist = _normalize_date_col(mult_hist)
        if "Data" not in mult_hist.columns:
            st.info("Histórico de múltiplos sem coluna de data.")
            return
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

    st.markdown("---")
    st.markdown("### Preço da Ação")

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

    # ✅ NOVO: gráfico de barras divergente imediatamente abaixo do gráfico de preços
    perf = _annual_price_performance(price_hist)

    # Filtra anos conforme janela do Supabase (para casar "tempo de histórico")
    df = _normalize_date_col(df)
    if df is not None and not df.empty and "Data" in df.columns:
        dd = pd.to_datetime(df["Data"], errors="coerce").dropna()
        if not dd.empty:
            y_min = int(dd.min().year)
            y_max = int(dd.max().year)
            perf = perf[(perf["Ano"] >= y_min) & (perf["Ano"] <= y_max)]

    if perf.empty:
        st.info("Não foi possível calcular o desempenho anual com o histórico disponível.")
        return

    render_grafico_retorno_anual_barras(perf, ticker)

    # ✅ Agora empurra os blocos de resumo para a parte mais baixa do dashboard (depois do gráfico anual)
    avg_yoy = float(np.nanmean(pd.to_numeric(perf["Variação %"], errors="coerce").values)) / 100.0
    cagr = _cagr_from_series(price_hist)

    st.markdown("---")
    st.markdown("### Resumo")
    render_cards_resumo(df, perf, avg_yoy=avg_yoy, cagr=cagr)
