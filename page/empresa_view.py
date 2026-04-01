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
                    <div style="border-radius:14px;padding:10px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.12)">
                        <div style="font-size:22px;font-weight:800">{_fmt_metric(label, v)}</div>
                        <div style="font-size:12px;opacity:.9;margin-top:6px">{descricoes.get(label, "-")}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    st.markdown("---")
    st.markdown("### Gráfico de Múltiplos (Histórico do Banco)")

    mult_hist = load_multiplos_from_db(ticker)
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
