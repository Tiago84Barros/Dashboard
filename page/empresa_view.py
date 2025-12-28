from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.db_supabase import get_engine


# =========================
# Config
# =========================

DFP_TABLE = "cvm.demonstracoes_financeiras_dfp"
METRICS_TABLE = "cvm.financial_metrics"
MULTIPLOS_TABLE = "cvm.multiplos"


@st.cache_resource(show_spinner=False)
def _engine() -> Engine:
    return get_engine()


def _norm_no_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    if t.endswith(".SA"):
        t = t[:-3]
    return t


@dataclass(frozen=True)
class EmpresaViewConfig:
    dfp_table: str = DFP_TABLE
    metrics_table: str = METRICS_TABLE
    multiplos_table: str = MULTIPLOS_TABLE
    years_default: int = 10


# =========================
# Loaders
# =========================

@st.cache_data(show_spinner=False, ttl=60 * 60)
def load_dfp(ticker: str, table: str = DFP_TABLE) -> pd.DataFrame:
    t = _norm_no_sa(ticker)
    sql = f"""
      select
        ticker, data,
        extract(year from data)::int as ano,
        receita_liquida, ebit, lucro_liquido, lpa,
        ativo_total, ativo_circulante,
        passivo_circulante, passivo_total,
        patrimonio_liquido,
        dividendos,
        caixa_e_equivalentes,
        divida_total,
        divida_liquida
      from {table}
      where ticker = :ticker
      order by data;
    """
    df = pd.read_sql(text(sql), con=_engine(), params={"ticker": t})
    if df is None or df.empty:
        return pd.DataFrame()
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    for c in df.columns:
        if c not in ("ticker", "data"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(show_spinner=False, ttl=60 * 60)
def load_financial_metrics(ticker: str, table: str = METRICS_TABLE) -> pd.DataFrame:
    t = _norm_no_sa(ticker)
    sql = f"""
      select *
      from {table}
      where ticker = :ticker
      order by ano;
    """
    df = pd.read_sql(text(sql), con=_engine(), params={"ticker": t})
    if df is None or df.empty:
        return pd.DataFrame()
    # força numéricos
    for c in df.columns:
        if c not in ("ticker",):
            df[c] = pd.to_numeric(df[c], errors="ignore")
    return df


@st.cache_data(show_spinner=False, ttl=60 * 60)
def load_multiplos(ticker: str, table: str = MULTIPLOS_TABLE) -> pd.DataFrame:
    t = _norm_no_sa(ticker)
    sql = f"""
      select ticker, ano, ref_date, price_close, pl, pvp, dy, shares_est
      from {table}
      where ticker = :ticker
      order by ano;
    """
    try:
        df = pd.read_sql(text(sql), con=_engine(), params={"ticker": t})
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df["ref_date"] = pd.to_datetime(df["ref_date"], errors="coerce")
    for c in ["price_close", "pl", "pvp", "dy", "shares_est"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# =========================
# Helpers: métricas e diagnóstico
# =========================

def _safe_cagr(series: pd.Series) -> float | np.nan:
    """CAGR simples com base no primeiro e último valor > 0."""
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < 2:
        return np.nan
    # pega primeiro e último positivos
    s_pos = s[s > 0]
    if len(s_pos) < 2:
        return np.nan
    first = s_pos.iloc[0]
    last = s_pos.iloc[-1]
    n = max(len(s_pos) - 1, 1)
    return (last / first) ** (1 / n) - 1


def _trend_last_vs_median(series: pd.Series) -> tuple[float | np.nan, float | np.nan]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return (np.nan, np.nan)
    return (s.iloc[-1], float(np.nanmedian(s.values)))


def _bucket(value: float, good: float, ok: float) -> str:
    """Retorna 'good' / 'ok' / 'bad' conforme limiares."""
    if np.isnan(value):
        return "na"
    if value >= good:
        return "good"
    if value >= ok:
        return "ok"
    return "bad"


def _fmt_pct(x: float | np.nan) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{x*100:.1f}%"


def _fmt_num(x: float | np.nan) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{x:,.0f}".replace(",", ".")


def _fmt_mult(x: float | np.nan) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{x:.1f}"


def _build_executive_report(
    dfp: pd.DataFrame,
    metrics: pd.DataFrame,
    mult: pd.DataFrame,
) -> dict:
    """
    Relatório determinístico (regras fixas).
    Retorna: {status, headline, bullets_pos, bullets_neg, details}
    """

    # Crescimento via DFP (robusto)
    cagr_receita = _safe_cagr(dfp["receita_liquida"]) if "receita_liquida" in dfp else np.nan
    cagr_lucro = _safe_cagr(dfp["lucro_liquido"]) if "lucro_liquido" in dfp else np.nan
    cagr_pl = _safe_cagr(dfp["patrimonio_liquido"]) if "patrimonio_liquido" in dfp else np.nan

    # Rentabilidade e margens (preferir financial_metrics)
    roe_last = roe_med = np.nan
    roic_last = roic_med = np.nan
    mliq_last = mliq_med = np.nan
    mebit_last = mebit_med = np.nan

    if not metrics.empty:
        if "roe" in metrics:
            roe_last, roe_med = _trend_last_vs_median(metrics["roe"])
        if "roic" in metrics:
            roic_last, roic_med = _trend_last_vs_median(metrics["roic"])
        if "margem_liquida" in metrics:
            mliq_last, mliq_med = _trend_last_vs_median(metrics["margem_liquida"])
        if "margem_ebit" in metrics:
            mebit_last, mebit_med = _trend_last_vs_median(metrics["margem_ebit"])

    # Endividamento (DFP)
    dl_pl_last = np.nan
    if "divida_liquida" in dfp.columns and "patrimonio_liquido" in dfp.columns:
        dl = pd.to_numeric(dfp["divida_liquida"], errors="coerce")
        pl = pd.to_numeric(dfp["patrimonio_liquido"], errors="coerce")
        if len(dl) and len(pl) and pl.iloc[-1] not in (0, np.nan):
            dl_pl_last = float(dl.iloc[-1] / pl.iloc[-1]) if pd.notna(dl.iloc[-1]) and pd.notna(pl.iloc[-1]) and pl.iloc[-1] != 0 else np.nan

    # Liquidez corrente (DFP)
    liq_corr_last = np.nan
    if "ativo_circulante" in dfp.columns and "passivo_circulante" in dfp.columns:
        ac = pd.to_numeric(dfp["ativo_circulante"], errors="coerce")
        pc = pd.to_numeric(dfp["passivo_circulante"], errors="coerce")
        if len(ac) and len(pc) and pc.iloc[-1] not in (0, np.nan):
            liq_corr_last = float(ac.iloc[-1] / pc.iloc[-1]) if pd.notna(ac.iloc[-1]) and pd.notna(pc.iloc[-1]) and pc.iloc[-1] != 0 else np.nan

    # Valuation (multiplos)
    pl_last = pvp_last = dy_last = np.nan
    if not mult.empty:
        pl_last = float(mult["pl"].iloc[-1]) if "pl" in mult.columns else np.nan
        pvp_last = float(mult["pvp"].iloc[-1]) if "pvp" in mult.columns else np.nan
        dy_last = float(mult["dy"].iloc[-1]) if "dy" in mult.columns else np.nan

    bullets_pos: list[str] = []
    bullets_neg: list[str] = []

    # Regras (simples, mas efetivas)
    # Crescimento
    if pd.notna(cagr_receita) and cagr_receita >= 0.08:
        bullets_pos.append(f"Crescimento de receita consistente (CAGR ~ {_fmt_pct(cagr_receita)}).")
    elif pd.notna(cagr_receita) and cagr_receita < 0.02:
        bullets_neg.append(f"Crescimento de receita baixo (CAGR ~ {_fmt_pct(cagr_receita)}).")

    if pd.notna(cagr_lucro) and cagr_lucro >= 0.08:
        bullets_pos.append(f"Crescimento do lucro em bom ritmo (CAGR ~ {_fmt_pct(cagr_lucro)}).")
    elif pd.notna(cagr_lucro) and cagr_lucro < 0:
        bullets_neg.append(f"Lucro com tendência negativa no período (CAGR ~ {_fmt_pct(cagr_lucro)}).")

    # Rentabilidade
    if pd.notna(roe_med) and roe_med >= 0.15:
        bullets_pos.append(f"Rentabilidade elevada (ROE mediano ~ {_fmt_pct(roe_med)}).")
    elif pd.notna(roe_med) and roe_med < 0.08:
        bullets_neg.append(f"Rentabilidade fraca (ROE mediano ~ {_fmt_pct(roe_med)}).")

    if pd.notna(mliq_med) and mliq_med >= 0.10:
        bullets_pos.append(f"Boa margem líquida (mediana ~ {_fmt_pct(mliq_med)}).")
    elif pd.notna(mliq_med) and mliq_med < 0.04:
        bullets_neg.append(f"Margem líquida baixa (mediana ~ {_fmt_pct(mliq_med)}).")

    # Endividamento
    if pd.notna(dl_pl_last) and dl_pl_last <= 0.6:
        bullets_pos.append(f"Alavancagem controlada (Dívida Líq/PL ~ {dl_pl_last:.2f}).")
    elif pd.notna(dl_pl_last) and dl_pl_last >= 1.5:
        bullets_neg.append(f"Alavancagem elevada (Dívida Líq/PL ~ {dl_pl_last:.2f}).")

    # Liquidez
    if pd.notna(liq_corr_last) and liq_corr_last >= 1.2:
        bullets_pos.append(f"Liquidez corrente confortável (~ {liq_corr_last:.2f}).")
    elif pd.notna(liq_corr_last) and liq_corr_last < 1.0:
        bullets_neg.append(f"Liquidez corrente pressionada (~ {liq_corr_last:.2f}).")

    # Valuation (somente se existir)
    if pd.notna(pl_last) and pl_last > 0:
        bullets_pos.append(f"Múltiplos calculados (P/L atual ~ {_fmt_mult(pl_last)}).")
    if mult.empty:
        bullets_neg.append("Valuation indisponível: tabela `cvm.multiplos` ainda não foi populada.")

    # Status
    bad_count = len(bullets_neg)
    if bad_count >= 3:
        status = "Atenção"
    elif bad_count == 2:
        status = "Neutra"
    else:
        status = "Saudável"

    headline = (
        "Resumo: "
        "crescimento, rentabilidade e risco (endividamento/liquidez) foram avaliados com base em DFP/metrics; "
        "valuation depende de `multiplos`."
    )

    details = {
        "cagr_receita": cagr_receita,
        "cagr_lucro": cagr_lucro,
        "cagr_pl": cagr_pl,
        "roe_last": roe_last,
        "roe_med": roe_med,
        "roic_last": roic_last,
        "roic_med": roic_med,
        "mliq_last": mliq_last,
        "mliq_med": mliq_med,
        "mebit_last": mebit_last,
        "mebit_med": mebit_med,
        "dl_pl_last": dl_pl_last,
        "liq_corr_last": liq_corr_last,
        "pl_last": pl_last,
        "pvp_last": pvp_last,
        "dy_last": dy_last,
    }

    return {
        "status": status,
        "headline": headline,
        "bullets_pos": bullets_pos[:6],
        "bullets_neg": bullets_neg[:6],
        "details": details,
    }


# =========================
# Render V2
# =========================

def render_empresa_view(ticker: str, *, config: Optional[EmpresaViewConfig] = None) -> None:
    cfg = config or EmpresaViewConfig()
    t = _norm_no_sa(ticker)

    st.subheader(f"Empresa: {t}")

    # Carregar bases
    dfp = load_dfp(t, table=cfg.dfp_table)
    metrics = load_financial_metrics(t, table=cfg.metrics_table)
    mult = load_multiplos(t, table=cfg.multiplos_table)

    if dfp.empty:
        st.warning("Sem dados DFP para este ticker em `cvm.demonstracoes_financeiras_dfp`.")
        return

    # Filtro de janela
    anos = dfp["ano"].dropna().astype(int).tolist()
    if anos:
        min_ano = max(min(anos), max(anos) - cfg.years_default + 1)
    else:
        min_ano = None

    with st.expander("Configurações de visualização", expanded=False):
        if anos:
            min_sel = st.slider(
                "Ano inicial",
                min_value=int(min(anos)),
                max_value=int(max(anos)),
                value=int(min_ano) if min_ano else int(min(anos)),
                step=1,
            )
            dfp_v = dfp[dfp["ano"] >= min_sel].copy()
        else:
            dfp_v = dfp.copy()

    # Relatório executivo determinístico
    rep = _build_executive_report(dfp, metrics, mult)

    # Header / status
    status = rep["status"]
    if status == "Saudável":
        st.success(f"Status: {status}")
    elif status == "Neutra":
        st.info(f"Status: {status}")
    else:
        st.warning(f"Status: {status}")

    st.caption(rep["headline"])

    # Cards principais
    d = rep["details"]
    c1, c2, c3, c4, c5 = st.columns(5)

    with c1:
        st.metric("CAGR Receita", _fmt_pct(d["cagr_receita"]))
    with c2:
        st.metric("CAGR Lucro", _fmt_pct(d["cagr_lucro"]))
    with c3:
        st.metric("ROE (med)", _fmt_pct(d["roe_med"]))
    with c4:
        st.metric("Margem Líq (med)", _fmt_pct(d["mliq_med"]))
    with c5:
        st.metric("Dívida Líq / PL", "—" if np.isnan(d["dl_pl_last"]) else f"{d['dl_pl_last']:.2f}")

    # Relatório em bullets
    st.markdown("### Relatório Executivo")
    colA, colB = st.columns(2)
    with colA:
        st.markdown("**Pontos fortes**")
        if rep["bullets_pos"]:
            for b in rep["bullets_pos"]:
                st.write(f"- {b}")
        else:
            st.write("- —")
    with colB:
        st.markdown("**Pontos de atenção**")
        if rep["bullets_neg"]:
            for b in rep["bullets_neg"]:
                st.write(f"- {b}")
        else:
            st.write("- —")

    # Gráficos principais (DFP)
    st.markdown("### Evolução histórica (DFP)")
    base_cols = ["receita_liquida", "ebit", "lucro_liquido", "patrimonio_liquido", "divida_liquida"]
    avail = [c for c in base_cols if c in dfp_v.columns]

    if avail:
        df_long = dfp_v.melt(id_vars=["ano"], value_vars=avail, var_name="metric", value_name="value")
        fig = px.line(df_long, x="ano", y="value", color="metric", markers=True)
        st.plotly_chart(fig, use_container_width=True)

    # Margens e ROE (metrics)
    if not metrics.empty:
        st.markdown("### Rentabilidade e margens (financial_metrics)")
        m = metrics.copy()
        if "ano" in m.columns:
            m["ano"] = pd.to_numeric(m["ano"], errors="coerce")

        charts = []
        for col in ["roe", "roic", "margem_liquida", "margem_ebit"]:
            if col in m.columns:
                charts.append(col)

        if charts:
            dfm = m[["ano"] + charts].dropna(subset=["ano"])
            dfm_long = dfm.melt(id_vars=["ano"], var_name="metric", value_name="value")
            fig2 = px.line(dfm_long, x="ano", y="value", color="metric", markers=True)
            st.plotly_chart(fig2, use_container_width=True)

    # Valuation (multiplos)
    st.markdown("### Valuation (múltiplos)")
    if mult.empty:
        st.info("Tabela `cvm.multiplos` está vazia. Popule-a via pipeline de preços + cálculo de múltiplos.")
    else:
        mv = mult.copy()
        mv["ano"] = pd.to_numeric(mv["ano"], errors="coerce")
        val_cols = [c for c in ["pl", "pvp", "dy"] if c in mv.columns]
        if val_cols:
            mv_long = mv.melt(id_vars=["ano"], value_vars=val_cols, var_name="multiple", value_name="value")
            fig3 = px.line(mv_long, x="ano", y="value", color="multiple", markers=True)
            st.plotly_chart(fig3, use_container_width=True)

        # preço de referência anual
        if "price_close" in mv.columns:
            fig4 = px.line(mv, x="ano", y="price_close", markers=True)
            st.plotly_chart(fig4, use_container_width=True)

    # Transparência
    with st.expander("Como este diagnóstico foi calculado (transparência)"):
        st.json(rep["details"])

    # Tabela (opcional)
    with st.expander("Tabela DFP (dados base)"):
        st.dataframe(dfp_v, use_container_width=True)
