from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

from core.helpers import get_logo_url
from core.db_loader import (
    load_data_from_db,
    load_multiplos_from_db,
    load_multiplos_limitado_from_db,
)
from core.yf_data import (
    get_company_info,     # (nome, website) cacheado + guard
    get_price,            # history() cacheado + guard
    get_fundamentals_yf,  # info() cacheado + guard
)


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
# Helpers de múltiplos (display)
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
        return out.tail(1).reset_index(drop=True)
    return out.head(1).reset_index(drop=True)


def _safe_float(x):
    try:
        v = float(x)
        return v if np.isfinite(v) else None
    except Exception:
        return None


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


def _pick_first(row: pd.Series, *cols: str):
    for c in cols:
        if c in row.index:
            return row.get(c)
    return None


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


def _compute_pl_pvp_from_db_and_price(
    indicadores: pd.DataFrame,
    mult_db_latest: pd.DataFrame,
    price: float | None,
) -> dict:
    """
    Calcula P/L e P/VP quando possível:
      P/L = Preço / LPA
      P/VP = Preço / VPA  (ou PL / N_Acoes)
    """
    out = {"P/L": None, "P/VP": None}

    if price is None or not np.isfinite(price) or price <= 0:
        return out

    ind = indicadores.copy()
    if "Data" in ind.columns:
        ind["Data"] = pd.to_datetime(ind["Data"], errors="coerce")
        ind = ind.dropna(subset=["Data"]).sort_values("Data")
    if ind.empty:
        return out

    last_ind = ind.iloc[-1]
    lpa = _safe_float(_pick_first(last_ind, "LPA", "Lucro_Por_Acao", "LucroPorAcao"))
    vpa = _safe_float(_pick_first(last_ind, "VPA", "Valor_Patrimonial_Por_Acao", "ValorPatrimonialPorAcao"))
    pliq = _safe_float(_pick_first(last_ind, "Patrimonio_Liquido", "PatrimonioLiquido"))

    n_acoes = None
    if mult_db_latest is not None and not mult_db_latest.empty:
        rowm = mult_db_latest.iloc[0]
        n_acoes = _safe_float(_pick_first(rowm, "N_Acoes", "N Acoes", "Num_Acoes", "N_Acoes_Total"))

    if vpa is None and pliq is not None and n_acoes is not None and n_acoes > 0:
        vpa = pliq / n_acoes

    if lpa is not None and lpa > 0:
        out["P/L"] = price / lpa

    if vpa is not None and vpa > 0:
        out["P/VP"] = price / vpa

    return out


def _merge_display_multiplos(
    db_latest: pd.DataFrame,
    yf_latest: pd.DataFrame | None,
    computed: dict,
) -> tuple[pd.DataFrame, dict]:
    """
    Monta DF final (1 linha) e fontes:
      - Base: DB
      - Complemento: YF (só nos campos faltantes)
      - Complemento: Calc (P/L, P/VP se computados)
    """
    fonte: dict = {}

    db1 = _latest_row_by_date(db_latest) if db_latest is not None else pd.DataFrame()
    df_disp = db1.copy() if db1 is not None else pd.DataFrame()

    if df_disp is not None and not df_disp.empty:
        for c in df_disp.columns:
            fonte[c] = "DB"

    # Injeta Yahoo apenas se existir (1 linha)
    if isinstance(yf_latest, pd.DataFrame) and not yf_latest.empty:
        yf1 = yf_latest.head(1).copy()
        if df_disp is None or df_disp.empty:
            df_disp = pd.DataFrame([{}])

        for c in yf1.columns:
            yv = yf1.at[0, c]
            if c not in df_disp.columns:
                df_disp[c] = yv
                fonte[c] = "YF"
            else:
                dbv = df_disp.at[0, c]
                if _is_missing(dbv) and not _is_missing(yv):
                    df_disp.at[0, c] = yv
                    fonte[c] = "YF"

    if df_disp is None or df_disp.empty:
        df_disp = pd.DataFrame([{}])

    # Computados (sobrescrevem apenas se existirem)
    if computed.get("P/L") is not None:
        df_disp["P/L"] = computed["P/L"]
        fonte["P/L"] = "Calc"
    if computed.get("P/VP") is not None:
        df_disp["P/VP"] = computed["P/VP"]
        fonte["P/VP"] = "Calc"

    return df_disp, fonte


@st.cache_data(show_spinner=False, ttl=6 * 60 * 60)
def _yf_snapshot_if_needed(ticker: str, need: tuple[bool, bool, bool]) -> dict:
    """
    Snapshot silencioso (cacheado por 6h) — evita martelar Yahoo.

    need:
      (need_info, need_price, need_fund)
    """
    need_info, need_price, need_fund = need

    out = {"name": None, "website": None, "price": None, "fund": None}

    # Faz somente o mínimo necessário
    if need_info:
        name, website = get_company_info(ticker)
        out["name"] = name
        out["website"] = website

    if need_price:
        out["price"] = get_price(ticker)

    if need_fund:
        out["fund"] = get_fundamentals_yf(ticker)

    return out


# ─────────────────────────────────────────────────────────────
# Página principal (assinatura compatível com page/basic.py)
# ─────────────────────────────────────────────────────────────

def render_empresa_view(ticker: str):
    if not ticker or not str(ticker).strip():
        st.warning("Informe um ticker válido.")
        return

    ticker = str(ticker).strip().upper()

    # ── DB: DRE / indicadores
    indicadores = load_data_from_db(ticker)
    if indicadores is None or indicadores.empty:
        st.error("Indicadores financeiros (DRE) não encontrados no banco.")
        return

    indicadores = indicadores.drop(columns=["Ticker"], errors="ignore")
    indicadores["Data"] = pd.to_datetime(indicadores["Data"], errors="coerce")
    indicadores = indicadores.dropna(subset=["Data"]).sort_values("Data")

    # ── DB: múltiplos
    mult_db_recent = load_multiplos_limitado_from_db(ticker, limite=12)
    mult_db_latest = _latest_row_by_date(mult_db_recent) if mult_db_recent is not None else pd.DataFrame()

    # ── Determina quais campos estão faltando no DB e só então consulta Yahoo (silencioso)
    need_fund = False
    if mult_db_latest is None or mult_db_latest.empty:
        need_fund = True
    else:
        row = mult_db_latest.iloc[0]
        # queremos preencher apenas o que estiver faltando
        for col in ["DY", "P/VP", "P/L", "Payout"]:
            if col not in mult_db_latest.columns or _is_missing(row.get(col)):
                need_fund = True
                break

    # preço ajuda em P/L e P/VP (calc), e também para mostrar no topo
    need_price = True  # sempre tentamos (cacheado) — se falhar, só fica indisponível

    # info (nome/site) é opcional; tentamos sempre, mas cacheado
    need_info = True

    snap = _yf_snapshot_if_needed(ticker, (need_info, need_price, need_fund))
    company_name = snap.get("name") or ticker.replace(".SA", "").upper()
    company_website = snap.get("website")
    current_price = _safe_float(snap.get("price"))
    fund_yf = snap.get("fund") if isinstance(snap.get("fund"), pd.DataFrame) else None

    # ── Header (sem mencionar fonte)
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
    growth_rates = {col: calculate_growth_rate(indicadores, col) for col in indicadores.columns if col != "Data"}

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
        .metric-value { font-size: 24px; font-weight: bold; color: #222; }
        .metric-label { font-size: 14px; color: #ff6600; font-weight: bold; }
        .metric-source { font-size: 11px; color: #999; margin-top: 2px; }
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
            st.plotly_chart(px.bar(dfm, x="Data", y="Valor", color="Indicador", barmode="group"), use_container_width=True)

    # ─────────────────────────────────────────────────────────
    # Indicadores Financeiros (cards)
    # ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Indicadores Financeiros")

    computed = _compute_pl_pvp_from_db_and_price(
        indicadores=indicadores,
        mult_db_latest=mult_db_latest,
        price=current_price,
    )

    multiplos_display, fontes = _merge_display_multiplos(mult_db_latest, fund_yf, computed)

    descricoes = {
        "Margem Líquida": "Lucro Líquido ÷ Receita Líquida — quanto sobra do faturamento como lucro final.",
        "Margem Operacional": "EBIT ÷ Receita Líquida — eficiência operacional antes de juros e impostos.",
        "ROE": "Lucro Líquido ÷ Patrimônio Líquido — rentabilidade ao acionista.",
        "ROIC": "NOPAT ÷ Capital Investido — eficiência do capital operacional.",
        "Dividend Yield": "Dividendos por ação ÷ Preço da ação — rentabilidade via proventos.",
        "P/VP": "Preço ÷ Valor patrimonial por ação.",
        "Payout": "Dividendos ÷ Lucro Líquido — parcela do lucro distribuída.",
        "P/L": "Preço ÷ Lucro por ação (LPA).",
        "Endividamento Total": "Dívida Total ÷ Patrimônio.",
        "Alavancagem Financeira": "Indicador de alavancagem (depende da fonte).",
        "Liquidez Corrente": "Ativo Circulante ÷ Passivo Circulante.",
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
        st.info("Indicadores financeiros indisponíveis.")
    else:
        rows = (len(valores) + 3) // 4
        for i in range(rows):
            cols = st.columns(4)
            for j, (col_key, label) in enumerate(valores[i * 4 : (i + 1) * 4]):
                with cols[j]:
                    v = multiplos_display.at[0, col_key] if col_key in multiplos_display.columns else None
                    tooltip = descricoes.get(label, "")
                    st.markdown(
                        f"""
                        <div class='metric-box' title="{tooltip}">
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
