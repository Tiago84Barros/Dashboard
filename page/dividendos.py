"""
page/dividendos.py
~~~~~~~~~~~~~~~~~~
Página dedicada à análise de dividendos do portfólio.

Seções:
  1. Histórico de pagamentos (timeline)
  2. Dividend Yield ao longo do tempo
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
from typing import Dict, List

# ── Importações internas ─────────────────────────────────────
try:
    from core.ui_bridge import load_multiplos_from_db
except Exception:
    load_multiplos_from_db = None  # type: ignore

try:
    from core.yf_data import coletar_dividendos, get_price
except Exception:
    coletar_dividendos = None  # type: ignore
    get_price = None  # type: ignore

try:
    from core.db_loader import load_setores_from_db
except Exception:
    load_setores_from_db = None  # type: ignore


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


# ── Helpers ──────────────────────────────────────────────────
def _normalize_date(s: pd.Series) -> pd.Series:
    if s.dtype == "object":
        s = pd.to_datetime(s, errors="coerce")
    elif hasattr(s.dtype, "tz") and s.dtype.tz is not None:
        s = s.dt.tz_localize(None)
    return pd.to_datetime(s, errors="coerce")


@st.cache_data(ttl=3600)
def _get_dividendos_yf(tickers: tuple) -> Dict[str, pd.DataFrame]:
    """Retorna histórico de dividendos do yfinance para cada ticker."""
    if coletar_dividendos is None:
        return {}
    raw = coletar_dividendos(list(tickers))
    result: Dict[str, pd.DataFrame] = {}
    for tk, serie in raw.items():
        if serie is None or serie.empty:
            continue
        df = serie.reset_index()
        df.columns = ["Data", "Dividendo"]
        df["Data"] = _normalize_date(df["Data"])
        df = df.dropna(subset=["Data"]).sort_values("Data")
        df["Ticker"] = tk.replace(".SA", "")
        result[tk.replace(".SA", "")] = df
    return result


@st.cache_data(ttl=3600)
def _get_mult_hist(ticker: str) -> pd.DataFrame:
    if load_multiplos_from_db is None:
        return pd.DataFrame()
    df = load_multiplos_from_db(ticker)
    if df is None or df.empty:
        return pd.DataFrame()
    # Normaliza coluna de data
    for col in ("Data", "data", "DATE", "date"):
        if col in df.columns:
            df = df.rename(columns={col: "Data"})
            break
    if "Data" not in df.columns:
        return pd.DataFrame()
    df["Data"] = _normalize_date(df["Data"])
    df = df.dropna(subset=["Data"]).sort_values("Data")
    return df


@st.cache_data(ttl=300)
def _get_price(ticker: str) -> float | None:
    if get_price is None:
        return None
    try:
        return float(get_price(ticker))
    except Exception:
        return None


def _all_tickers_from_db() -> List[str]:
    """Lista todos os tickers disponíveis no banco (via setores_df)."""
    if "setores_df" in st.session_state and st.session_state["setores_df"] is not None:
        df = st.session_state["setores_df"]
        for col in ("Ticker", "ticker", "TICKER"):
            if col in df.columns:
                return sorted(df[col].dropna().unique().tolist())
    return []


# ── Seção 1 — Histórico de pagamentos ────────────────────────
def _render_historico(divs: Dict[str, pd.DataFrame]) -> None:
    st.markdown("### 📅 Histórico de Pagamentos")

    if not divs:
        st.info("Nenhum dado de dividendos encontrado no yfinance para os tickers selecionados.")
        return

    frames = []
    for tk, df in divs.items():
        frames.append(df[["Data", "Dividendo", "Ticker"]])
    all_div = pd.concat(frames, ignore_index=True).sort_values("Data")
    all_div["Ano"] = all_div["Data"].dt.year

    # Filtro de período
    min_ano = int(all_div["Ano"].min()) if not all_div.empty else 2015
    max_ano = int(all_div["Ano"].max()) if not all_div.empty else 2025
    col_a, col_b = st.columns(2)
    with col_a:
        ano_ini = st.slider("Ano inicial", min_ano, max_ano, max(min_ano, max_ano - 5),
                            key="div_ano_ini")
    with col_b:
        ano_fim = st.slider("Ano final", min_ano, max_ano, max_ano, key="div_ano_fim")

    filtered = all_div[(all_div["Ano"] >= ano_ini) & (all_div["Ano"] <= ano_fim)]

    if filtered.empty:
        st.info("Nenhum pagamento no período selecionado.")
        return

    # Gráfico timeline
    fig = px.scatter(
        filtered, x="Data", y="Dividendo",
        color="Ticker",
        size="Dividendo",
        size_max=20,
        hover_data={"Ticker": True, "Dividendo": ":.4f", "Data": "|%d/%m/%Y"},
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    fig.update_layout(
        height=350,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.07)",
                     title="R$/ação")
    st.plotly_chart(fig, use_container_width=True)

    # Tabela anual acumulada
    st.markdown("#### Dividendos totais por ano e empresa")
    pivot = (
        filtered.groupby(["Ticker", "Ano"])["Dividendo"]
        .sum()
        .unstack(level=0)
        .fillna(0)
        .sort_index(ascending=False)
    )
    st.dataframe(
        pivot.style.format("{:.4f}").background_gradient(cmap="Greens", axis=None),
        use_container_width=True,
    )


# ── Seção 2 — DY ao longo do tempo ──────────────────────────
def _render_dy_historico(tickers: List[str]) -> None:
    st.markdown("### 📉 Dividend Yield Histórico")
    frames = []
    for tk in tickers:
        df = _get_mult_hist(tk)
        if df.empty or "DY" not in df.columns:
            continue
        df = df[["Data", "DY"]].copy()
        dy_vals = pd.to_numeric(df["DY"], errors="coerce")
        # Normalizar se em percentual
        if dy_vals.abs().median() > 1.0:
            dy_vals = dy_vals / 100.0
        df["DY"] = dy_vals * 100  # exibir como %
        df = df.dropna(subset=["DY"])
        df["Ticker"] = tk
        frames.append(df)

    if not frames:
        st.info("DY histórico não encontrado no banco de dados para os tickers selecionados.")
        return

    all_dy = pd.concat(frames, ignore_index=True).sort_values("Data")
    # Filtro 5 anos
    cutoff = pd.Timestamp.today() - pd.DateOffset(years=5)
    all_dy = all_dy[all_dy["Data"] >= cutoff]

    if all_dy.empty:
        st.info("Dados insuficientes para o gráfico de DY histórico.")
        return

    fig = px.line(
        all_dy, x="Data", y="DY", color="Ticker",
        markers=False,
        labels={"DY": "Dividend Yield (%)"},
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    fig.update_layout(
        height=320,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.07)")
    st.plotly_chart(fig, use_container_width=True)


# ── Seção 3 — Comparativo DY atual ───────────────────────────
def _render_comparativo_dy(tickers: List[str]) -> None:
    st.markdown("### 🏆 Comparativo de DY Atual entre Empresas")

    rows = []
    for tk in tickers:
        df = _get_mult_hist(tk)
        if df.empty or "DY" not in df.columns:
            rows.append({"Ticker": tk, "DY (%)": None})
            continue
        dy_raw = pd.to_numeric(df["DY"], errors="coerce").dropna()
        if dy_raw.empty:
            rows.append({"Ticker": tk, "DY (%)": None})
            continue
        dy_last = float(dy_raw.iloc[-1])
        if abs(dy_last) > 1.0:
            dy_last /= 100.0
        rows.append({"Ticker": tk, "DY (%)": round(dy_last * 100, 2)})

    df_comp = pd.DataFrame(rows).dropna(subset=["DY (%)"]).sort_values("DY (%)", ascending=False)

    if df_comp.empty:
        st.info("Dados de DY atual não disponíveis.")
        return

    # Gráfico de barras horizontal
    fig = px.bar(
        df_comp, x="DY (%)", y="Ticker",
        orientation="h",
        text="DY (%)",
        color="DY (%)",
        color_continuous_scale="greens",
        labels={"DY (%)": "Dividend Yield (%)"},
    )
    fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig.update_layout(
        height=max(250, len(df_comp) * 40),
        margin=dict(l=10, r=30, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        coloraxis_showscale=False,
        yaxis=dict(autorange="reversed"),
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.07)")
    st.plotly_chart(fig, use_container_width=True)

    # Métricas resumo
    cols = st.columns(3)
    with cols[0]:
        best = df_comp.iloc[0]
        st.markdown(
            f'<div class="div-card"><div class="div-card-title">Maior DY</div>'
            f'<div class="div-card-value" style="color:#22c55e">{best["DY (%)"]:.2f}%</div>'
            f'<div class="div-card-sub">{best["Ticker"]}</div></div>',
            unsafe_allow_html=True,
        )
    with cols[1]:
        med = df_comp["DY (%)"].median()
        st.markdown(
            f'<div class="div-card"><div class="div-card-title">Mediana do grupo</div>'
            f'<div class="div-card-value">{med:.2f}%</div>'
            f'<div class="div-card-sub">{len(df_comp)} empresas</div></div>',
            unsafe_allow_html=True,
        )
    with cols[2]:
        worst = df_comp.iloc[-1]
        st.markdown(
            f'<div class="div-card"><div class="div-card-title">Menor DY</div>'
            f'<div class="div-card-value" style="color:#f97316">{worst["DY (%)"]:.2f}%</div>'
            f'<div class="div-card-sub">{worst["Ticker"]}</div></div>',
            unsafe_allow_html=True,
        )


# ── Seção 4 — Calendário últimos 12 meses ─────────────────────
def _render_calendario(divs: Dict[str, pd.DataFrame]) -> None:
    st.markdown("### 🗓️ Proventos — Últimos 12 Meses")

    cutoff = pd.Timestamp.today() - pd.DateOffset(months=12)
    frames = []
    for tk, df in divs.items():
        recentes = df[df["Data"] >= cutoff].copy()
        if not recentes.empty:
            frames.append(recentes)

    if not frames:
        st.info("Nenhum provento registrado nos últimos 12 meses para os tickers selecionados.")
        return

    cal = pd.concat(frames, ignore_index=True).sort_values("Data", ascending=False)
    cal["Data"] = cal["Data"].dt.strftime("%d/%m/%Y")
    cal["Dividendo (R$/ação)"] = cal["Dividendo"].map("{:.4f}".format)

    # Agrupamento mensal
    cal_orig = pd.concat(frames, ignore_index=True).copy()
    cal_orig["Mês"] = cal_orig["Data"].dt.to_period("M").astype(str)
    mensal = cal_orig.groupby(["Mês", "Ticker"])["Dividendo"].sum().unstack(fill_value=0).round(4)

    # Heatmap de intensidade de proventos
    if not mensal.empty:
        fig_heat = px.imshow(
            mensal.T,
            labels=dict(x="Mês", y="Empresa", color="Dividendo (R$)"),
            color_continuous_scale="greens",
            aspect="auto",
        )
        fig_heat.update_layout(
            height=max(180, len(mensal.columns) * 38),
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_heat, use_container_width=True)

    st.dataframe(
        cal[["Data", "Ticker", "Dividendo (R$/ação)"]].reset_index(drop=True),
        use_container_width=True,
        hide_index=True,
    )


# ── Seção 5 — Simulador de renda passiva ─────────────────────
def _render_simulador(tickers: List[str]) -> None:
    st.markdown("### 💰 Simulador de Renda Passiva")
    st.caption(
        "Estime a renda anual em dividendos com base no preço atual e no DY histórico. "
        "Informe a quantidade de ações que possui (ou pretende ter) em cada empresa."
    )

    rows = []
    for tk in tickers:
        price = _get_price(tk)
        df_m = _get_mult_hist(tk)
        dy_pct = None
        if not df_m.empty and "DY" in df_m.columns:
            dy_raw = pd.to_numeric(df_m["DY"], errors="coerce").dropna()
            if not dy_raw.empty:
                v = float(dy_raw.iloc[-1])
                if abs(v) > 1.0:
                    v /= 100.0
                dy_pct = v if 0 < v <= 0.5 else None
        rows.append({"Ticker": tk, "Preço (R$)": price, "DY atual": dy_pct})

    if not rows:
        st.info("Sem dados suficientes para o simulador.")
        return

    df_sim = pd.DataFrame(rows)

    st.markdown("#### Defina suas posições")
    qtd_dict: Dict[str, int] = {}
    cols_sim = st.columns(min(len(tickers), 4))
    for i, tk in enumerate(tickers):
        with cols_sim[i % len(cols_sim)]:
            qtd = st.number_input(
                f"{tk}", min_value=0, step=100, value=0, key=f"sim_qtd_{tk}"
            )
            qtd_dict[tk] = int(qtd)

    # Calcular
    renda_total_anual = 0.0
    renda_total_mensal = 0.0
    detail_rows = []
    for _, row in df_sim.iterrows():
        tk = row["Ticker"]
        price = row["Preço (R$)"]
        dy = row["DY atual"]
        qtd = qtd_dict.get(tk, 0)
        if qtd > 0 and price and dy:
            valor_pos = qtd * price
            renda_anual = valor_pos * dy
            detail_rows.append({
                "Ticker": tk,
                "Qtd. ações": qtd,
                "Preço atual": f"R$ {price:.2f}",
                "DY (%)": f"{dy*100:.2f}%",
                "Posição (R$)": f"R$ {valor_pos:,.2f}",
                "Renda anual (R$)": f"R$ {renda_anual:,.2f}",
                "Renda mensal (R$)": f"R$ {renda_anual/12:,.2f}",
            })
            renda_total_anual += renda_anual
            renda_total_mensal += renda_anual / 12

    # Totais
    st.markdown("---")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            f'<div class="div-card"><div class="div-card-title">Renda Anual Estimada</div>'
            f'<div class="div-card-value" style="color:#22c55e">R$ {renda_total_anual:,.2f}</div>'
            f'<div class="div-card-sub">baseado no DY mais recente</div></div>',
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
        posicao_total = sum(
            qtd_dict.get(row["Ticker"], 0) * (row["Preço (R$)"] or 0)
            for _, row in df_sim.iterrows()
            if row["Preço (R$)"] is not None
        )
        dy_medio = (renda_total_anual / posicao_total * 100) if posicao_total > 0 else 0
        st.markdown(
            f'<div class="div-card"><div class="div-card-title">DY Médio Ponderado</div>'
            f'<div class="div-card-value">{dy_medio:.2f}%</div>'
            f'<div class="div-card-sub">renda total ÷ posição total</div></div>',
            unsafe_allow_html=True,
        )

    if detail_rows:
        st.markdown("#### Detalhamento por empresa")
        st.dataframe(pd.DataFrame(detail_rows), use_container_width=True, hide_index=True)

        # Gráfico pizza de renda por empresa
        df_pie = pd.DataFrame([
            {"Ticker": r["Ticker"],
             "Renda": float(r["Renda anual (R$)"].replace("R$ ", "").replace(",", ""))}
            for r in detail_rows
        ])
        if not df_pie.empty:
            fig_pie = px.pie(
                df_pie, names="Ticker", values="Renda",
                color_discrete_sequence=px.colors.qualitative.Pastel,
                hole=0.45,
            )
            fig_pie.update_layout(
                height=300,
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", yanchor="bottom", y=-0.2),
            )
            st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("Informe a quantidade de ações acima para calcular a renda estimada.")


# ── Entry point ───────────────────────────────────────────────
def render() -> None:
    st.markdown("## 💸 Dividendos & Proventos")
    st.markdown("Análise de histórico de pagamentos, Dividend Yield e simulação de renda passiva.")

    st.markdown(_DIV_CSS, unsafe_allow_html=True)

    # ── Seleção de tickers ────────────────────────────────────
    all_tickers = _all_tickers_from_db()

    # Tenta pre-popular com o portfólio da sessão
    portfolio_default: List[str] = []
    for key in ("portfolio_tickers", "tickers_selecionados", "portfolio"):
        if key in st.session_state:
            v = st.session_state[key]
            if isinstance(v, (list, tuple)):
                portfolio_default = [str(t) for t in v]
                break

    with st.sidebar:
        st.markdown("---")
        st.markdown("### 💸 Dividendos")

        input_mode = st.radio(
            "Fonte dos tickers:",
            ["Portfólio da sessão", "Seleção manual"],
            index=0,
            key="div_input_mode",
        )

        if input_mode == "Portfólio da sessão":
            tickers_sel = portfolio_default
            if tickers_sel:
                st.markdown(
                    "**Tickers do portfólio:**<br>"
                    + " ".join(f'<span class="div-ticker-badge">{t}</span>' for t in tickers_sel),
                    unsafe_allow_html=True,
                )
            else:
                st.info("Nenhum portfólio carregado na sessão. Use a página Criação de Portfólio primeiro.")
        else:
            tickers_sel = st.multiselect(
                "Selecione os tickers:",
                options=all_tickers if all_tickers else [],
                default=[],
                key="div_manual_sel",
                placeholder="Ex: PETR4, VALE3, ITUB4…",
            )

        # Campo de texto livre como alternativa
        txt_extra = st.text_input(
            "Adicionar tickers manualmente (separados por vírgula):",
            value="",
            key="div_extra_tickers",
        )
        if txt_extra.strip():
            extras = [t.strip().upper() for t in txt_extra.split(",") if t.strip()]
            tickers_sel = list(dict.fromkeys(tickers_sel + extras))

    if not tickers_sel:
        st.info(
            "Nenhum ticker selecionado. Escolha empresas na barra lateral ou carregue um portfólio "
            "na página **Criação de Portfólio**."
        )
        return

    st.markdown(
        "**Analisando:** "
        + " ".join(f'<span class="div-ticker-badge">{t}</span>' for t in tickers_sel),
        unsafe_allow_html=True,
    )
    st.markdown("")

    # ── Carregar dados ────────────────────────────────────────
    with st.spinner("Carregando dados de dividendos…"):
        divs = _get_dividendos_yf(tuple(sorted(tickers_sel)))

    # ── Abas ─────────────────────────────────────────────────
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📅 Histórico",
        "📉 DY ao Tempo",
        "🏆 Comparativo",
        "🗓️ Calendário",
        "💰 Simulador",
    ])

    with tab1:
        _render_historico(divs)

    with tab2:
        _render_dy_historico(tickers_sel)

    with tab3:
        _render_comparativo_dy(tickers_sel)

    with tab4:
        _render_calendario(divs)

    with tab5:
        _render_simulador(tickers_sel)
