"""
page/dividendos.py
~~~~~~~~~~~~~~~~~~
Página dedicada à análise de dividendos do portfólio.

Fonte de dados (em ordem de prioridade):
  1. yfinance  — dividendos históricos + preço (sempre disponível para B3)
  2. Supabase  — tabela multiplos.DY como referência secundária
  3. yfinance ticker.info — trailing_annual_dividend_yield como última alternativa

Seções:
  1. Histórico de pagamentos (timeline)
  2. Dividend Yield anual ao longo do tempo
  3. Comparativo DY entre empresas
  4. Calendário de proventos (últimos 12 meses)
  5. Simulador de renda passiva
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf
from typing import Dict, List, Optional, Tuple

# ── Importações internas ─────────────────────────────────────
try:
    from core.ui_bridge import load_multiplos_from_db
except Exception:
    load_multiplos_from_db = None  # type: ignore

try:
    from core.yf_data import coletar_dividendos, get_price, baixar_precos
except Exception:
    coletar_dividendos = None  # type: ignore
    get_price = None           # type: ignore
    baixar_precos = None       # type: ignore

try:
    from core.db_loader import load_setores_from_db
except Exception:
    load_setores_from_db = None  # type: ignore

try:
    from core.portfolio_snapshot_store import get_latest_snapshot
except Exception:
    get_latest_snapshot = None  # type: ignore


# ── CSS local ────────────────────────────────────────────────
_DIV_CSS = """
<style>
.div-card {
    background: rgba(255,255,255,.04);
    border: 1px solid rgba(255,255,255,.1);
    border-radius: 14px;
    padding: 14px 18px;
    margin-bottom: 10px;
}
.div-card-title {
    font-size: 12px;
    font-weight: 700;
    opacity: .6;
    text-transform: uppercase;
    letter-spacing: .6px;
    margin-bottom: 4px;
}
.div-card-value {
    font-size: 28px;
    font-weight: 900;
    line-height: 1.1;
}
.div-card-sub {
    font-size: 11px;
    opacity: .5;
    margin-top: 3px;
}
.div-ticker-badge {
    display: inline-block;
    background: rgba(99,102,241,.25);
    border: 1px solid rgba(99,102,241,.5);
    color: #a5b4fc;
    border-radius: 20px;
    padding: 2px 10px;
    font-size: 12px;
    font-weight: 700;
    margin: 2px;
}
</style>
"""


# ═══════════════════════════════════════════════════════════════
# CAMADA DE DADOS — yfinance primeiro, DB como suporte
# ═══════════════════════════════════════════════════════════════

def _norm_ticker(tk: str) -> str:
    """Garante sufixo .SA para B3."""
    tk = tk.strip().upper()
    return tk if tk.endswith(".SA") else f"{tk}.SA"


def _strip_sa(tk: str) -> str:
    return tk.replace(".SA", "").strip().upper()


def _tz_strip(s: pd.Series) -> pd.Series:
    """Remove timezone de uma Series de datas."""
    try:
        if hasattr(s.dtype, "tz") and s.dtype.tz is not None:
            return s.dt.tz_localize(None)
    except Exception:
        pass
    return pd.to_datetime(s, errors="coerce", utc=True).dt.tz_localize(None)


# ── 1. Dividendos brutos (yfinance) ──────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_dividends_yf(tickers: tuple) -> Dict[str, pd.DataFrame]:
    """
    Retorna {TICKER: DataFrame(Data, Dividendo, Ticker)} via yfinance.
    Tenta com .SA e sem .SA para máxima cobertura.
    """
    result: Dict[str, pd.DataFrame] = {}
    for tk in tickers:
        base = _strip_sa(tk)
        div_serie = pd.Series(dtype="float64")
        for variant in [_norm_ticker(base), base]:
            try:
                s = yf.Ticker(variant).dividends
                if s is not None and not s.empty:
                    div_serie = s
                    break
            except Exception:
                continue

        if div_serie.empty:
            continue

        idx = _tz_strip(pd.Series(div_serie.index))
        df = pd.DataFrame({"Data": idx.values, "Dividendo": div_serie.values})
        df = df.dropna(subset=["Data"]).sort_values("Data")
        df["Ticker"] = base
        result[base] = df

    return result


# ── 2. Preço histórico mensal (yfinance) ─────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_prices_monthly(tickers: tuple, start: str = "2018-01-01") -> Dict[str, pd.Series]:
    """
    Retorna {TICKER: Series(preço mensal, index=datetime)} via yfinance.
    Reamostrado para fim-de-mês.
    """
    result: Dict[str, pd.Series] = {}
    for tk in tickers:
        base = _strip_sa(tk)
        prices = pd.Series(dtype="float64")
        for variant in [_norm_ticker(base), base]:
            try:
                raw = yf.Ticker(variant).history(start=start, auto_adjust=True)
                if raw is not None and not raw.empty and "Close" in raw.columns:
                    prices = raw["Close"].copy()
                    prices.index = _tz_strip(pd.Series(prices.index)).values
                    prices = prices.resample("ME").last().dropna()
                    break
            except Exception:
                continue
        if not prices.empty:
            result[base] = prices
    return result


# ── 3. Preço atual (yfinance) ─────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def _fetch_price_now(ticker: str) -> Optional[float]:
    """Preço atual: tenta yfinance info, fast_info e histórico recente."""
    base = _strip_sa(ticker)
    for variant in [_norm_ticker(base), base]:
        try:
            tk_obj = yf.Ticker(variant)
            # fast_info é mais rápido
            p = getattr(tk_obj.fast_info, "last_price", None)
            if p and p > 0:
                return float(p)
            # fallback: último fechamento
            hist = tk_obj.history(period="5d", auto_adjust=True)
            if hist is not None and not hist.empty:
                return float(hist["Close"].iloc[-1])
        except Exception:
            continue
    # Último recurso: get_price interno
    if get_price is not None:
        try:
            v = get_price(base)
            if v and float(v) > 0:
                return float(v)
        except Exception:
            pass
    return None


# ── 4. DY trailing 12M calculado via yfinance ────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _calc_trailing_dy(tickers: tuple) -> Dict[str, Optional[float]]:
    """
    Calcula DY trailing 12M para cada ticker:
      DY = soma_dividendos_12m / preço_atual

    Fallback: yfinance trailing_annual_dividend_yield do .info
    """
    divs_all  = _fetch_dividends_yf(tickers)
    result: Dict[str, Optional[float]] = {}
    cutoff_12m = pd.Timestamp.today() - pd.DateOffset(months=12)

    for tk in tickers:
        base = _strip_sa(tk)
        dy: Optional[float] = None

        # Tenta calcular: sum(dividendos 12M) / preço
        if base in divs_all:
            df_div = divs_all[base]
            div_12m = df_div[df_div["Data"] >= cutoff_12m]["Dividendo"].sum()
            if div_12m > 0:
                price = _fetch_price_now(base)
                if price and price > 0:
                    dy = div_12m / price

        # Fallback: trailing_annual_dividend_yield do yfinance .info
        if dy is None:
            for variant in [_norm_ticker(base), base]:
                try:
                    info = yf.Ticker(variant).info
                    v = info.get("trailingAnnualDividendYield") or info.get("dividendYield")
                    if v and float(v) > 0:
                        dy = float(v)
                        break
                except Exception:
                    continue

        # Sanity check: DY entre 0 e 200%
        if dy is not None and (dy <= 0 or dy > 2.0):
            dy = None

        result[base] = dy

    return result


# ── 5. Série anual de DY (yfinance) ──────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _calc_annual_dy_series(tickers: tuple) -> Dict[str, pd.DataFrame]:
    """
    Para cada ticker retorna DataFrame(Ano, DY%) com DY anual:
      DY_ano = soma_divs_ano / preço_médio_ano
    """
    divs_all   = _fetch_dividends_yf(tickers)
    prices_all = _fetch_prices_monthly(tickers)
    result: Dict[str, pd.DataFrame] = {}

    for tk in tickers:
        base = _strip_sa(tk)
        if base not in divs_all or base not in prices_all:
            continue

        df_div = divs_all[base].copy()
        prices  = prices_all[base]

        df_div["Ano"] = pd.to_datetime(df_div["Data"]).dt.year
        anual_divs = df_div.groupby("Ano")["Dividendo"].sum()

        # Preço médio por ano (dos fechamentos mensais)
        prices_df = prices.reset_index()
        prices_df.columns = ["Data", "Preco"]
        prices_df["Ano"] = pd.to_datetime(prices_df["Data"]).dt.year
        anual_price = prices_df.groupby("Ano")["Preco"].mean()

        dy_anual = (anual_divs / anual_price).dropna()
        dy_anual = dy_anual[dy_anual > 0]

        if dy_anual.empty:
            continue

        df_out = dy_anual.reset_index()
        df_out.columns = ["Ano", "DY"]
        df_out["DY_pct"] = df_out["DY"] * 100
        df_out["Ticker"] = base
        result[base] = df_out

    return result


# ── 6. Portfólio e tickers ────────────────────────────────────
def _load_portfolio_tickers() -> List[str]:
    if get_latest_snapshot is None:
        return []
    try:
        snapshot = get_latest_snapshot()
        if not snapshot:
            return []
        raw = snapshot.get("items") or snapshot.get("tickers") or []
        if raw and isinstance(raw, list):
            if isinstance(raw[0], str):
                return sorted(set(t.strip().upper() for t in raw if t))
            if isinstance(raw[0], dict):
                return sorted(set(
                    str(it.get("ticker", "")).strip().upper()
                    for it in raw if it.get("ticker")
                ))
    except Exception:
        pass
    return []


def _all_tickers_from_db() -> List[str]:
    if "setores_df" in st.session_state and st.session_state["setores_df"] is not None:
        df = st.session_state["setores_df"]
        for col in ("Ticker", "ticker", "TICKER"):
            if col in df.columns:
                return sorted(df[col].dropna().unique().tolist())
    return []


# ═══════════════════════════════════════════════════════════════
# SEÇÕES DA PÁGINA
# ═══════════════════════════════════════════════════════════════

# ── Seção 1 — Histórico de pagamentos ────────────────────────
def _render_historico(divs: Dict[str, pd.DataFrame]) -> None:
    st.markdown("### 📅 Histórico de Pagamentos")

    if not divs:
        st.info("Nenhum dividendo encontrado no yfinance para os tickers selecionados.")
        return

    all_div = pd.concat(list(divs.values()), ignore_index=True).sort_values("Data")
    all_div["Ano"] = all_div["Data"].dt.year

    min_ano = int(all_div["Ano"].min()) if not all_div.empty else 2015
    max_ano = int(all_div["Ano"].max()) if not all_div.empty else 2025

    col_a, col_b = st.columns(2)
    with col_a:
        ano_ini = st.slider("Ano inicial", min_ano, max_ano,
                            max(min_ano, max_ano - 5), key="div_ano_ini")
    with col_b:
        ano_fim = st.slider("Ano final", min_ano, max_ano, max_ano, key="div_ano_fim")

    filtered = all_div[(all_div["Ano"] >= ano_ini) & (all_div["Ano"] <= ano_fim)]
    if filtered.empty:
        st.info("Nenhum pagamento no período selecionado.")
        return

    fig = px.scatter(
        filtered, x="Data", y="Dividendo", color="Ticker",
        size="Dividendo", size_max=22,
        hover_data={"Ticker": True, "Dividendo": ":.4f", "Data": "|%d/%m/%Y"},
        color_discrete_sequence=px.colors.qualitative.Pastel,
        labels={"Dividendo": "R$/ação"},
    )
    fig.update_layout(height=350, margin=dict(l=10, r=10, t=10, b=10),
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      legend=dict(orientation="h", yanchor="bottom", y=1.02))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.07)")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Dividendos totais por ano e empresa")
    pivot = (
        filtered.groupby(["Ticker", "Ano"])["Dividendo"]
        .sum().unstack(level=0).fillna(0).sort_index(ascending=False)
    )
    st.dataframe(
        pivot.style.format("{:.4f}").background_gradient(cmap="Greens", axis=None),
        use_container_width=True,
    )


# ── Seção 2 — DY anual ao longo do tempo ─────────────────────
def _render_dy_historico(tickers: List[str], dy_annual: Dict[str, pd.DataFrame]) -> None:
    st.markdown("### 📉 Dividend Yield Anual Histórico")
    st.caption(
        "DY calculado diretamente do yfinance: "
        "**soma dos dividendos de cada ano ÷ preço médio daquele ano**."
    )

    frames = [df for df in dy_annual.values() if not df.empty]
    if not frames:
        st.info("Dados de DY insuficientes no yfinance para os tickers selecionados.")
        return

    all_dy = pd.concat(frames, ignore_index=True)
    all_dy = all_dy[all_dy["Ano"] >= (pd.Timestamp.today().year - 7)]

    fig = px.bar(
        all_dy, x="Ano", y="DY_pct", color="Ticker",
        barmode="group",
        text=all_dy["DY_pct"].map(lambda v: f"{v:.1f}%"),
        labels={"DY_pct": "DY (%)"},
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        height=380, margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        xaxis=dict(dtick=1),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.07)")
    st.plotly_chart(fig, use_container_width=True)

    # Tabela resumo
    resumo = (
        all_dy.groupby("Ticker")["DY_pct"]
        .agg(["mean", "max", "min"])
        .rename(columns={"mean": "Média (%)", "max": "Máximo (%)", "min": "Mínimo (%)"})
        .round(2)
        .sort_values("Média (%)", ascending=False)
    )
    st.dataframe(resumo.style.background_gradient(cmap="Greens", subset=["Média (%)"]),
                 use_container_width=True)


# ── Seção 3 — Comparativo DY atual ───────────────────────────
def _render_comparativo_dy(
    tickers: List[str],
    trailing_dy: Dict[str, Optional[float]],
    dy_annual: Dict[str, pd.DataFrame],
) -> None:
    st.markdown("### 🏆 Comparativo de DY entre Empresas")
    st.caption(
        "DY trailing 12M = soma dos dividendos dos últimos 12 meses ÷ preço atual. "
        "Barra de média histórica mostra a média dos últimos anos."
    )

    rows = []
    for tk in tickers:
        base = _strip_sa(tk)
        dy_trail = trailing_dy.get(base)

        # DY médio histórico (últimos 5 anos) via série anual
        dy_hist_med = None
        if base in dy_annual and not dy_annual[base].empty:
            hist = dy_annual[base]
            hist5 = hist[hist["Ano"] >= (pd.Timestamp.today().year - 5)]
            if not hist5.empty:
                dy_hist_med = float(hist5["DY_pct"].mean())

        rows.append({
            "Ticker": base,
            "DY Trailing 12M (%)": round(dy_trail * 100, 2) if dy_trail else None,
            "DY Média 5a (%)": round(dy_hist_med, 2) if dy_hist_med else None,
        })

    df_comp = pd.DataFrame(rows).dropna(subset=["DY Trailing 12M (%)"]) \
                                .sort_values("DY Trailing 12M (%)", ascending=False)

    if df_comp.empty:
        st.warning(
            "DY trailing não disponível. Possíveis causas: ticker sem dividendos nos últimos "
            "12 meses ou yfinance sem dados de preço para cálculo."
        )
        # Tenta mostrar só o histórico médio
        rows_hist = [
            {"Ticker": _strip_sa(tk),
             "DY Média 5a (%)": round(dy_annual[_strip_sa(tk)]["DY_pct"].mean(), 2)
                                if _strip_sa(tk) in dy_annual and not dy_annual[_strip_sa(tk)].empty else None}
            for tk in tickers
        ]
        df_hist = pd.DataFrame(rows_hist).dropna().sort_values("DY Média 5a (%)", ascending=False)
        if not df_hist.empty:
            st.markdown("#### DY médio histórico (últimos 5 anos)")
            fig = px.bar(df_hist, x="DY Média 5a (%)", y="Ticker", orientation="h",
                         text="DY Média 5a (%)", color="DY Média 5a (%)",
                         color_continuous_scale="greens")
            fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
            fig.update_layout(height=max(250, len(df_hist) * 40),
                              margin=dict(l=10, r=30, t=10, b=10),
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)
        return

    # Gráfico principal
    fig = go.Figure()

    # Barra trailing
    fig.add_trace(go.Bar(
        y=df_comp["Ticker"], x=df_comp["DY Trailing 12M (%)"],
        orientation="h", name="Trailing 12M",
        text=df_comp["DY Trailing 12M (%)"].map(lambda v: f"{v:.2f}%" if pd.notna(v) else ""),
        textposition="outside",
        marker_color="rgba(99,102,241,0.75)",
    ))

    # Ponto de média histórica
    df_hist_ok = df_comp.dropna(subset=["DY Média 5a (%)"])
    if not df_hist_ok.empty:
        fig.add_trace(go.Scatter(
            y=df_hist_ok["Ticker"], x=df_hist_ok["DY Média 5a (%)"],
            mode="markers", name="Média 5a",
            marker=dict(color="#f97316", size=10, symbol="diamond",
                        line=dict(color="white", width=1)),
        ))

    fig.update_layout(
        height=max(300, len(df_comp) * 42),
        margin=dict(l=10, r=50, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        barmode="overlay",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        xaxis=dict(title="DY (%)", showgrid=True, gridcolor="rgba(255,255,255,0.07)"),
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Cards resumo
    c1, c2, c3 = st.columns(3)
    with c1:
        best = df_comp.iloc[0]
        st.markdown(
            f'<div class="div-card"><div class="div-card-title">Maior DY (12M)</div>'
            f'<div class="div-card-value" style="color:#22c55e">{best["DY Trailing 12M (%)"]:.2f}%</div>'
            f'<div class="div-card-sub">{best["Ticker"]}</div></div>',
            unsafe_allow_html=True,
        )
    with c2:
        med = df_comp["DY Trailing 12M (%)"].median()
        st.markdown(
            f'<div class="div-card"><div class="div-card-title">Mediana do grupo</div>'
            f'<div class="div-card-value">{med:.2f}%</div>'
            f'<div class="div-card-sub">{len(df_comp)} empresas com DY</div></div>',
            unsafe_allow_html=True,
        )
    with c3:
        worst = df_comp.iloc[-1]
        st.markdown(
            f'<div class="div-card"><div class="div-card-title">Menor DY (12M)</div>'
            f'<div class="div-card-value" style="color:#f97316">{worst["DY Trailing 12M (%)"]:.2f}%</div>'
            f'<div class="div-card-sub">{worst["Ticker"]}</div></div>',
            unsafe_allow_html=True,
        )


# ── Seção 4 — Calendário últimos 12 meses ─────────────────────
def _render_calendario(divs: Dict[str, pd.DataFrame]) -> None:
    st.markdown("### 🗓️ Proventos — Últimos 12 Meses")

    cutoff = pd.Timestamp.today() - pd.DateOffset(months=12)
    frames = [df[df["Data"] >= cutoff].copy() for df in divs.values() if not df.empty]
    frames = [f for f in frames if not f.empty]

    if not frames:
        st.info("Nenhum provento registrado nos últimos 12 meses para os tickers selecionados.")
        return

    cal_orig = pd.concat(frames, ignore_index=True).sort_values("Data")
    cal_orig["Mês"] = cal_orig["Data"].dt.to_period("M").astype(str)

    mensal = (
        cal_orig.groupby(["Mês", "Ticker"])["Dividendo"]
        .sum().unstack(fill_value=0).round(4)
    )

    if not mensal.empty:
        fig_heat = px.imshow(
            mensal.T,
            labels=dict(x="Mês", y="Empresa", color="Dividendo (R$)"),
            color_continuous_scale="greens",
            aspect="auto",
            text_auto=".3f",
        )
        fig_heat.update_layout(
            height=max(200, len(mensal.columns) * 40),
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_heat, use_container_width=True)

    cal_disp = cal_orig.copy()
    cal_disp["Data"] = cal_disp["Data"].dt.strftime("%d/%m/%Y")
    cal_disp = cal_disp.sort_values("Data", ascending=False).rename(
        columns={"Dividendo": "Dividendo (R$/ação)"}
    )
    st.dataframe(
        cal_disp[["Data", "Ticker", "Dividendo (R$/ação)"]].reset_index(drop=True),
        use_container_width=True, hide_index=True,
    )


# ── Seção 5 — Simulador de renda passiva ─────────────────────
def _render_simulador(
    tickers: List[str],
    trailing_dy: Dict[str, Optional[float]],
) -> None:
    st.markdown("### 💰 Simulador de Renda Passiva")
    st.caption(
        "Renda estimada = quantidade de ações × preço atual × DY trailing 12M. "
        "O DY é calculado diretamente do yfinance (dividendos pagos ÷ preço)."
    )

    rows = []
    for tk in tickers:
        base = _strip_sa(tk)
        price = _fetch_price_now(base)
        dy    = trailing_dy.get(base)
        rows.append({"Ticker": base, "Preço (R$)": price, "DY Trailing": dy})

    df_sim = pd.DataFrame(rows)

    # Diagnóstico
    with st.expander("🔍 Dados carregados (preço + DY)", expanded=False):
        diag = df_sim.copy()
        diag["Preço"] = diag["Preço (R$)"].apply(
            lambda v: f"R$ {v:.2f}" if v is not None and v > 0 else "⚠️ não encontrado"
        )
        diag["DY Trailing 12M"] = diag["DY Trailing"].apply(
            lambda v: f"{v*100:.2f}%" if v is not None else "⚠️ não encontrado"
        )
        st.dataframe(diag[["Ticker", "Preço", "DY Trailing 12M"]],
                     use_container_width=True, hide_index=True)

    st.markdown("#### Defina suas posições")
    n_cols = min(len(tickers), 4)
    cols_sim = st.columns(n_cols)
    qtd_dict: Dict[str, int] = {}
    for i, tk in enumerate(tickers):
        base = _strip_sa(tk)
        with cols_sim[i % n_cols]:
            qtd = st.number_input(
                f"{base}", min_value=0, step=100, value=0, key=f"sim_qtd_{base}"
            )
            qtd_dict[base] = int(qtd)

    # Calcular
    renda_total_anual = 0.0
    detail_rows = []
    for _, row in df_sim.iterrows():
        base  = row["Ticker"]
        price = row["Preço (R$)"]
        dy    = row["DY Trailing"]
        qtd   = qtd_dict.get(base, 0)
        if qtd > 0 and price is not None and price > 0 and dy is not None and dy > 0:
            valor_pos  = qtd * price
            renda_anual = valor_pos * dy
            detail_rows.append({
                "Ticker":           base,
                "Qtd. ações":       qtd,
                "Preço (R$)":       round(price, 2),
                "DY (%)":           round(dy * 100, 2),
                "Posição (R$)":     round(valor_pos, 2),
                "Renda anual (R$)": round(renda_anual, 2),
                "Renda mensal (R$)":round(renda_anual / 12, 2),
            })
            renda_total_anual += renda_anual

    renda_total_mensal = renda_total_anual / 12

    # Cards de totais (sempre visíveis)
    st.markdown("---")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            f'<div class="div-card"><div class="div-card-title">Renda Anual Estimada</div>'
            f'<div class="div-card-value" style="color:#22c55e">R$ {renda_total_anual:,.2f}</div>'
            f'<div class="div-card-sub">baseado no DY trailing 12M</div></div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f'<div class="div-card"><div class="div-card-title">Renda Mensal Estimada</div>'
            f'<div class="div-card-value" style="color:#22c55e">R$ {renda_total_mensal:,.2f}</div>'
            f'<div class="div-card-sub">renda anual ÷ 12</div></div>',
            unsafe_allow_html=True,
        )
    with c3:
        pos_total = sum(
            qtd_dict.get(r["Ticker"], 0) * r["Preço (R$)"]
            for r in detail_rows
        )
        dy_med_pond = (renda_total_anual / pos_total * 100) if pos_total > 0 else 0
        st.markdown(
            f'<div class="div-card"><div class="div-card-title">DY Médio Ponderado</div>'
            f'<div class="div-card-value">{dy_med_pond:.2f}%</div>'
            f'<div class="div-card-sub">renda total ÷ posição total</div></div>',
            unsafe_allow_html=True,
        )

    if not detail_rows:
        sem_price = [r["Ticker"] for _, r in df_sim.iterrows() if r["Preço (R$)"] is None]
        sem_dy    = [r["Ticker"] for _, r in df_sim.iterrows() if r["DY Trailing"] is None]
        msgs = []
        if sem_price:
            msgs.append(f"Preço não encontrado: **{', '.join(sem_price)}**")
        if sem_dy:
            msgs.append(f"Sem dividendos nos últimos 12M: **{', '.join(sem_dy)}**")
        if not msgs:
            msgs = ["Informe a quantidade de ações acima para calcular."]
        for m in msgs:
            st.info(m)
        return

    st.markdown("#### Detalhamento por empresa")
    df_det = pd.DataFrame(detail_rows)
    st.dataframe(df_det, use_container_width=True, hide_index=True)

    # Gráfico pizza
    if len(detail_rows) > 1:
        fig_pie = px.pie(
            df_det, names="Ticker", values="Renda anual (R$)",
            color_discrete_sequence=px.colors.qualitative.Pastel,
            hole=0.45,
        )
        fig_pie.update_traces(textinfo="percent+label")
        fig_pie.update_layout(
            height=300, margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=-0.2),
        )
        st.plotly_chart(fig_pie, use_container_width=True)


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════
def render() -> None:
    st.markdown("## 💸 Dividendos & Proventos")
    st.markdown("Análise de histórico de pagamentos, Dividend Yield e simulação de renda passiva.")
    st.markdown(_DIV_CSS, unsafe_allow_html=True)

    # ── Seleção de tickers ────────────────────────────────────
    all_tickers     = _all_tickers_from_db()
    portfolio_default = _load_portfolio_tickers()

    with st.sidebar:
        st.markdown("---")
        st.markdown("### 💸 Dividendos")

        input_mode = st.radio(
            "Fonte dos tickers:",
            ["Portfólio salvo", "Seleção manual"],
            index=0,
            key="div_input_mode",
        )

        if input_mode == "Portfólio salvo":
            tickers_sel = portfolio_default
            if tickers_sel:
                st.markdown(
                    "**Tickers do portfólio:**<br>"
                    + " ".join(f'<span class="div-ticker-badge">{t}</span>'
                               for t in tickers_sel),
                    unsafe_allow_html=True,
                )
            else:
                st.info("Nenhum portfólio encontrado. Execute a **Criação de Portfólio** primeiro.")
        else:
            tickers_sel = st.multiselect(
                "Selecione os tickers:",
                options=all_tickers if all_tickers else [],
                default=[],
                key="div_manual_sel",
                placeholder="Ex: PETR4, VALE3, ITUB4…",
            )

        txt_extra = st.text_input(
            "Adicionar tickers (separados por vírgula):",
            value="", key="div_extra_tickers",
        )
        if txt_extra.strip():
            extras = [t.strip().upper() for t in txt_extra.split(",") if t.strip()]
            tickers_sel = list(dict.fromkeys((tickers_sel or []) + extras))

    if not tickers_sel:
        st.info(
            "Nenhum ticker selecionado. Escolha empresas na barra lateral "
            "ou carregue um portfólio na página **Criação de Portfólio**."
        )
        return

    st.markdown(
        "**Analisando:** "
        + " ".join(f'<span class="div-ticker-badge">{t}</span>' for t in tickers_sel),
        unsafe_allow_html=True,
    )
    st.markdown("")

    # ── Pré-carregar todos os dados uma vez ───────────────────
    tickers_tuple = tuple(sorted(_strip_sa(t) for t in tickers_sel))

    with st.spinner("Carregando dividendos e preços via yfinance…"):
        divs        = _fetch_dividends_yf(tickers_tuple)
        trailing_dy = _calc_trailing_dy(tickers_tuple)
        dy_annual   = _calc_annual_dy_series(tickers_tuple)

    # ── Abas ─────────────────────────────────────────────────
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📅 Histórico",
        "📉 DY Anual",
        "🏆 Comparativo",
        "🗓️ Calendário",
        "💰 Simulador",
    ])

    with tab1:
        _render_historico(divs)

    with tab2:
        _render_dy_historico(list(tickers_tuple), dy_annual)

    with tab3:
        _render_comparativo_dy(list(tickers_tuple), trailing_dy, dy_annual)

    with tab4:
        _render_calendario(divs)

    with tab5:
        _render_simulador(list(tickers_tuple), trailing_dy)
