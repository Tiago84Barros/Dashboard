from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

from core.helpers import get_company_info, get_logo_url
from core.db.engine import get_engine
from core.db.loader import (
    load_demonstracoes_financeiras,
    load_multiplos,
)
from core.yf_data import get_price, get_fundamentals_yf


# ─────────────────────────────────────────────────────────────
# Engine (Supabase) cacheado
# ─────────────────────────────────────────────────────────────
@st.cache_resource
def _engine():
    return get_engine()


def _norm_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    if not t:
        return t
    return t if t.endswith(".SA") else f"{t}.SA"


def _pick_date_col(df: pd.DataFrame) -> str | None:
    for c in ("Data", "data"):
        if c in df.columns:
            return c
    return None


# ─────────────────────────────────────────────────────────────
# Crescimento (médio anual) com regressão em log
# ─────────────────────────────────────────────────────────────

def calculate_growth_rate(df: pd.DataFrame, column: str) -> float:
    """
    Estima crescimento médio anual aproximado usando regressão em log.

    - Filtra valores <= 0 (log inválido)
    - Retorna np.nan se dados insuficientes
    """
    try:
        if df is None or df.empty or column not in df.columns:
            return np.nan

        date_col = _pick_date_col(df)
        if not date_col:
            return np.nan

        tmp = df[[date_col, column]].copy()
        tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
        tmp[column] = pd.to_numeric(tmp[column], errors="coerce")
        tmp = tmp.dropna(subset=[date_col, column])
        tmp = tmp[tmp[column] > 0].sort_values(date_col)

        if tmp.shape[0] < 2:
            return np.nan

        X = (tmp[date_col] - tmp[date_col].iloc[0]).dt.days / 365.25
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
# Helpers de múltiplos (display)
# ─────────────────────────────────────────────────────────────

def _latest_row_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna um DF com 1 linha: o último registro por Data/data.
    Se não houver Data, retorna primeira linha.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    date_col = _pick_date_col(out)
    if date_col:
        out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
        out = out.dropna(subset=[date_col]).sort_values(date_col)
        if out.empty:
            return pd.DataFrame()
        return out.tail(1).reset_index(drop=True)

    return out.head(1).reset_index(drop=True)


def _merge_display_multiplos(
    db_latest: pd.DataFrame,
    yf_latest: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    """
    Constrói um DF (1 linha) para exibição:
    - Base: yfinance
    - Completa campos faltantes com DB (se houver)
    Retorna:
      (df_display, fonte_por_coluna)
    """
    fonte: dict = {}

    db1 = _latest_row_by_date(db_latest) if db_latest is not None else pd.DataFrame()
    yf1 = yf_latest.copy() if isinstance(yf_latest, pd.DataFrame) else pd.DataFrame()

    if yf1 is None or yf1.empty:
        df_disp = db1.copy()
        for c in df_disp.columns:
            fonte[c] = "DB"
        return df_disp, fonte

    df_disp = yf1.copy()
    for c in df_disp.columns:
        fonte[c] = "YF"

    # completa com DB quando YF está vazio/zero
    if db1 is not None and not db1.empty:
        for c in db1.columns:
            if c not in df_disp.columns:
                df_disp[c] = db1.iloc[0].get(c)
                fonte[c] = "DB"
                continue

            yf_val = df_disp.at[0, c]
            db_val = db1.at[0, c]

            def _missing(x):
                try:
                    if x is None:
                        return True
                    if isinstance(x, (float, int)) and (pd.isna(x) or np.isinf(x)):
                        return True
                    if isinstance(x, (float, int)) and float(x) == 0.0:
                        return True
                except Exception:
                    return True
                return False

            if _missing(yf_val) and not _missing(db_val):
                df_disp.at[0, c] = db_val
                fonte[c] = "DB"

    return df_disp, fonte


def _fmt_metric(label: str, value) -> str:
    if value is None or (isinstance(value, float) and (pd.isna(value) or np.isinf(value))) or value == 0:
        return "-"
    try:
        v = float(value)
    except Exception:
        return "-"

    # campos percentuais
    if "Margem" in label or label in ["ROE", "ROIC", "Payout", "Dividend Yield", "Endividamento Total"]:
        return f"{v:.2f}%"
    return f"{v:.2f}"


# ─────────────────────────────────────────────────────────────
# Loads (DB) com cache
# ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def _load_dre(ticker: str) -> pd.DataFrame:
    return load_demonstracoes_financeiras(_norm_sa(ticker), engine=_engine())


@st.cache_data(ttl=3600)
def _load_multiplos(ticker: str) -> pd.DataFrame:
    return load_multiplos(_norm_sa(ticker), engine=_engine())


def _multiplos_limitado(ticker: str, limite: int = 12, anos: int | None = None) -> pd.DataFrame:
    """
    Substitui load_multiplos_limitado_from_db:
    - se 'anos' informado: filtra por janela de tempo
    - senão: retorna os últimos 'limite' registros ordenados por Data/data
    """
    df = _load_multiplos(ticker)
    if df is None or df.empty:
        return df

    out = df.copy()
    date_col = _pick_date_col(out)
    if not date_col:
        return out

    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out = out.dropna(subset=[date_col]).sort_values(date_col)

    if out.empty:
        return out

    if anos is not None:
        cutoff = pd.Timestamp.today().normalize() - pd.DateOffset(years=int(anos))
        return out[out[date_col] >= cutoff].sort_values(date_col)

    # por limite
    return out.tail(int(limite)).reset_index(drop=True)


# ─────────────────────────────────────────────────────────────
# Página principal
# ─────────────────────────────────────────────────────────────

def render_empresa_view(ticker: str):
    if not ticker or not str(ticker).strip():
        st.warning("Informe um ticker válido.")
        return

    ticker = _norm_sa(ticker)

    # ── DRE / indicadores do DB (fonte primária)
    indicadores = _load_dre(ticker)
    if indicadores is None or indicadores.empty:
        st.error("Indicadores financeiros (DRE) não encontrados no banco.")
        return

    indicadores = indicadores.drop(columns=["Ticker"], errors="ignore")
    date_col = _pick_date_col(indicadores)
    if date_col:
        indicadores[date_col] = pd.to_datetime(indicadores[date_col], errors="coerce")
        indicadores = indicadores.dropna(subset=[date_col]).sort_values(date_col)
        if date_col != "Data":
            indicadores = indicadores.rename(columns={date_col: "Data"})
    else:
        st.error("DRE sem coluna de data (Data/data).")
        return

    # ── Cabeçalho
    company_name, company_website = get_company_info(ticker)
    company_name = company_name or ticker.replace(".SA", "").upper()
    current_price = get_price(ticker)
    logo_url = get_logo_url(ticker)

    col1, col2 = st.columns([4, 1])
    with col1:
        if current_price is None:
            st.subheader(f"{company_name} — Preço atual indisponível")
        else:
            st.subheader(
                f"{company_name} — Preço Atual: R$ {current_price:,.2f}"
                .replace(",", "X").replace(".", ",").replace("X", ".")
            )
        if company_website:
            st.caption(company_website)
        st.caption(f"Ticker: {ticker}")
    with col2:
        st.image(logo_url, width=80)

    # ── Crescimentos
    growth_rates = {
        col: calculate_growth_rate(indicadores, col)
        for col in indicadores.columns
        if col != "Data"
    }

    st.markdown("## Visão Geral (Taxa de Crescimento Médio Anual)")
    st.markdown(
        """
        <style>
        .growth-box {
            border: 2px solid #ddd;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 10px;
            display: flex;
            justify-content: center;
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
        .metric-value {
            font-size: 24px;
            font-weight: bold;
            color: #222;
        }
        .metric-label {
            font-size: 14px;
            color: #ff6600;
            font-weight: bold;
        }
        .metric-source {
            font-size: 11px;
            color: #999;
            margin-top: 2px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            f"<div class='growth-box'>Receita Líquida: {format_growth_rate(growth_rates.get('Receita_Liquida'))}</div>",
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f"<div class='growth-box'>Lucro Líquido: {format_growth_rate(growth_rates.get('Lucro_Liquido'))}</div>",
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            f"<div class='growth-box'>Patrimônio Líquido: {format_growth_rate(growth_rates.get('Patrimonio_Liquido'))}</div>",
            unsafe_allow_html=True,
        )

    # ─────────────────────────────────────────────────────────
    # Demonstrações Financeiras (DRE)
    # ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Demonstrações Financeiras")

    friendly = {
        "Receita_Liquida": "Receita Líquida",
        "Lucro_Liquido": "Lucro Líquido",
        "EBIT": "EBIT",
        "LPA": "LPA",
        "Divida_Liquida": "Dívida Líquida",
        "Patrimonio_Liquido": "Patrimônio Líquido",
        "Caixa_Liquido": "Caixa Líquido",
    }

    opcoes = [friendly.get(c, c.replace("_", " ")) for c in indicadores.columns if c != "Data"]
    default_sel = [x for x in ["Receita Líquida", "Lucro Líquido", "Dívida Líquida"] if x in opcoes]

    sel = st.multiselect("Escolha os Indicadores:", opcoes, default=default_sel)

    if sel:
        rev = {v: k for k, v in friendly.items()}
        cols_sel = [rev.get(x, x.replace(" ", "_")) for x in sel if x]
        cols_sel = [c for c in cols_sel if c in indicadores.columns]

        if cols_sel:
            dfm = indicadores.melt(id_vars=["Data"], value_vars=cols_sel, var_name="Indicador", value_name="Valor")
            dfm["Indicador"] = dfm["Indicador"].map(lambda x: friendly.get(x, x.replace("_", " ")))
            st.plotly_chart(
                px.bar(dfm, x="Data", y="Valor", color="Indicador", barmode="group"),
                use_container_width=True,
            )
        else:
            st.info("Sem colunas válidas para o gráfico (dados insuficientes no banco).")

    # ─────────────────────────────────────────────────────────
    # Indicadores Financeiros (cards) — DB + fallback YF
    # ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Indicadores Financeiros")

    # DB “recente”: últimos registros (equivalente ao antigo limitado)
    mult_db_recent = _multiplos_limitado(ticker, limite=12)
    mult_db_latest = _latest_row_by_date(mult_db_recent) if mult_db_recent is not None else pd.DataFrame()

    # YF (para exibição): 1 linha
    mult_yf_latest = get_fundamentals_yf(ticker)

    multiplos_display, fontes = _merge_display_multiplos(mult_db_latest, mult_yf_latest)

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

    if multiplos_display is None or multiplos_display.empty:
        st.info("Indicadores financeiros indisponíveis (DB/YF).")
    else:
        rows = (len(valores) + 3) // 4
        for i in range(rows):
            cols = st.columns(4)
            for j, (col_key, label) in enumerate(valores[i * 4 : (i + 1) * 4]):
                with cols[j]:
                    v = multiplos_display.at[0, col_key] if col_key in multiplos_display.columns else None
                    tooltip = descricoes.get(label, "")
                    src = fontes.get(col_key, "-")
                    st.markdown(
                        f"""
                        <div class='metric-box' title="{tooltip}">
                            <div class='metric-value'>{_fmt_metric(label, v)}</div>
                            <div class='metric-label'><strong>{label}</strong></div>
                            <div class='metric-source'>Fonte: {src}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

    # ─────────────────────────────────────────────────────────
    # Gráfico de múltiplos (histórico do DB)
    # ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Gráfico de Múltiplos (Histórico do Banco)")

    mult_hist = _load_multiplos(ticker)
    if mult_hist is None or mult_hist.empty:
        st.info("Histórico de múltiplos não encontrado no banco.")
        return

    mult_hist = mult_hist.copy()
    date_col = _pick_date_col(mult_hist)
    if not date_col:
        st.info("Histórico de múltiplos sem coluna de data (Data/data).")
        return

    mult_hist[date_col] = pd.to_datetime(mult_hist[date_col], errors="coerce")
    mult_hist = mult_hist.dropna(subset=[date_col]).sort_values(date_col)
    if date_col != "Data":
        mult_hist = mult_hist.rename(columns={date_col: "Data"})

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
            st.plotly_chart(
                px.bar(dfm_mult, x="Data", y="Valor", color="Indicador", barmode="group"),
                use_container_width=True,
            )
        else:
            st.info("Nenhuma variável válida selecionada.")
