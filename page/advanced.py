from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from core.data_access import (
    load_data_from_db,
    load_multiplos_limitado_from_db,
)
from core.helpers import get_logo_url

pd.set_option("display.float_format", "{:.2f}".format)

DEFAULT_DESCONTO_DCF = 0.12
DEFAULT_CRESC_LUCRO = 0.06
DEFAULT_MARGEM_SEGURANCA = 0.20


def _fmt_money(v) -> str:
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return "—"
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "—"


def _fmt_pct(v) -> str:
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return "—"
        return f"{float(v) * 100:.1f}%".replace(".", ",")
    except Exception:
        return "—"


def _safe_selectbox(label: str, options: list, key: str):
    if key in st.session_state and st.session_state[key] not in options:
        st.session_state.pop(key, None)
    if not options:
        return None
    return st.selectbox(label, options, key=key)


def _safe_radio(label: str, options: list, key: str, index: int = 0):
    if key in st.session_state and st.session_state[key] not in options:
        st.session_state.pop(key, None)
    return st.radio(label, options, key=key, index=index, horizontal=True)


def render() -> None:
    st.markdown("<h1 style='text-align:center'>Análise Avançada de Ações</h1>", unsafe_allow_html=True)

    setores_df = st.session_state.get("setores_df", None)
    if setores_df is None or setores_df.empty:
        st.warning("Base de setores não carregada. Vá em Configurações e execute a atualização/ingest.")
        return

    setores = setores_df.copy()

    # ───────────────────────── Filtros (na página) ─────────────────────────
    with st.expander("Filtros", expanded=True):
        col1, col2, col3 = st.columns([1, 1, 1])

        setor_opts = sorted(setores["SETOR"].dropna().unique().tolist()) if "SETOR" in setores.columns else []
        if not setor_opts:
            st.error("Coluna SETOR não encontrada ou sem dados.")
            return

        with col1:
            setor = _safe_selectbox("Setor", setor_opts, key="adv_setor")

        subsetor_opts = []
        if setor and "SUBSETOR" in setores.columns:
            subsetor_opts = sorted(setores.loc[setores["SETOR"] == setor, "SUBSETOR"].dropna().unique().tolist())

        with col2:
            subsetor = _safe_selectbox("Subsetor", subsetor_opts, key="adv_subsetor")

        segmento_opts = []
        if setor and subsetor and "SEGMENTO" in setores.columns:
            segmento_opts = sorted(
                setores.loc[
                    (setores["SETOR"] == setor) & (setores["SUBSETOR"] == subsetor),
                    "SEGMENTO",
                ].dropna().unique().tolist()
            )

        with col3:
            segmento = _safe_selectbox("Segmento", segmento_opts, key="adv_segmento")

        st.markdown("---")
        col4, col5, col6 = st.columns([1, 1, 1])

        with col4:
            criterio_ordenacao = _safe_radio(
                "Ordenar por",
                ["Score total", "Score crescimento", "Score valor", "ROE", "Margem líquida"],
                key="adv_order",
                index=0,
            )

        with col5:
            desconto_dcf = st.number_input(
                "Taxa de desconto (DCF a.a.)",
                min_value=0.01,
                max_value=0.50,
                value=float(st.session_state.get("adv_desconto_dcf", DEFAULT_DESCONTO_DCF)),
                step=0.01,
                format="%.2f",
                key="adv_desconto_dcf",
            )

        with col6:
            margem_seg = st.number_input(
                "Margem de segurança",
                min_value=0.00,
                max_value=0.80,
                value=float(st.session_state.get("adv_margem_seg", DEFAULT_MARGEM_SEGURANCA)),
                step=0.05,
                format="%.2f",
                key="adv_margem_seg",
            )

        cresc_lucro = st.slider(
            "Crescimento anual do lucro (projeção)",
            min_value=0.0,
            max_value=0.30,
            value=float(st.session_state.get("adv_cresc_lucro", DEFAULT_CRESC_LUCRO)),
            step=0.01,
            key="adv_cresc_lucro",
        )

    if not (setor and subsetor and segmento):
        st.info("Selecione Setor, Subsetor e Segmento para continuar.")
        return

    universo = setores.loc[
        (setores["SETOR"] == setor) & (setores["SUBSETOR"] == subsetor) & (setores["SEGMENTO"] == segmento)
    ].copy()

    if universo.empty:
        st.warning("Nenhuma empresa encontrada para o filtro selecionado.")
        return

    tickers = sorted(universo["ticker"].dropna().unique().tolist())
    if not tickers:
        st.warning("Nenhum ticker encontrado para o filtro selecionado.")
        return

    # Carrega dados do DB
    try:
        dfp = load_data_from_db(tickers=tickers, tipo="DFP")
    except Exception as e:
        st.error(f"Falha ao carregar DFP do banco: {e}")
        return

    try:
        itr = load_data_from_db(tickers=tickers, tipo="ITR")
    except Exception as e:
        st.error(f"Falha ao carregar ITR do banco: {e}")
        return

    if (dfp is None or dfp.empty) and (itr is None or itr.empty):
        st.warning("Sem dados financeiros (DFP/ITR) para o universo selecionado.")
        return

    try:
        multiplos = load_multiplos_limitado_from_db(tickers=tickers)
    except Exception as e:
        multiplos = pd.DataFrame()
        st.info(f"Não foi possível carregar múltiplos limitados: {e}")

    # Normalização leve
    def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        df = df.copy()
        df.columns = [c.strip() for c in df.columns]
        return df

    dfp = _norm_cols(dfp)
    itr = _norm_cols(itr)
    multiplos = _norm_cols(multiplos)

    # Último ponto por ticker
    dfp_last = None
    if dfp is not None and not dfp.empty:
        dfp_last = dfp.sort_values(["ticker", "data"]).groupby("ticker").tail(1).reset_index(drop=True)

    # Crescimento simples por ticker (se houver colunas padrão do seu schema)
    cresc_receita = None
    cresc_lucro_hist = None
    if dfp is not None and not dfp.empty and "Receita_Liquida" in dfp.columns and "Lucro_Liquido" in dfp.columns:
        dfp_sorted = dfp.sort_values(["ticker", "data"])
        grp = dfp_sorted.groupby("ticker")
        last2 = grp.tail(2).copy()

        def _growth(s):
            if len(s) < 2:
                return np.nan
            v0, v1 = s.iloc[0], s.iloc[1]
            if v0 in (0, None) or pd.isna(v0) or pd.isna(v1):
                return np.nan
            return (v1 / v0) - 1

        cresc_receita = last2.groupby("ticker")["Receita_Liquida"].apply(_growth)
        cresc_lucro_hist = last2.groupby("ticker")["Lucro_Liquido"].apply(_growth)

    # Base de score
    score_df = pd.DataFrame({"ticker": tickers}).set_index("ticker")

    def _z(x: pd.Series, invert=False):
        x = x.astype(float)
        z = (x - x.mean()) / (x.std(ddof=0) + 1e-9)
        return -z if invert else z

    if multiplos is not None and not multiplos.empty and "ticker" in multiplos.columns:
        m = multiplos.set_index("ticker")
        for c in ["roe", "margem_liquida", "div_liq_ebitda", "pl", "pvp", "dividend_yield"]:
            if c in m.columns:
                score_df[c] = m[c]

    if cresc_receita is not None:
        score_df["cresc_receita"] = cresc_receita
    if cresc_lucro_hist is not None:
        score_df["cresc_lucro_hist"] = cresc_lucro_hist

    # Qualidade
    quality_parts = []
    if "roe" in score_df.columns:
        quality_parts.append(_z(score_df["roe"]))
    if "margem_liquida" in score_df.columns:
        quality_parts.append(_z(score_df["margem_liquida"]))
    if "div_liq_ebitda" in score_df.columns:
        quality_parts.append(_z(score_df["div_liq_ebitda"], invert=True))

    score_df["score_qualidade"] = np.nan
    if quality_parts:
        score_df["score_qualidade"] = np.nanmean(np.vstack(quality_parts), axis=0)

    # Valor
    value_parts = []
    if "pl" in score_df.columns:
        value_parts.append(_z(score_df["pl"], invert=True))
    if "pvp" in score_df.columns:
        value_parts.append(_z(score_df["pvp"], invert=True))
    if "dividend_yield" in score_df.columns:
        value_parts.append(_z(score_df["dividend_yield"]))

    score_df["score_valor"] = np.nan
    if value_parts:
        score_df["score_valor"] = np.nanmean(np.vstack(value_parts), axis=0)

    # Crescimento
    growth_parts = []
    if "cresc_receita" in score_df.columns:
        growth_parts.append(_z(score_df["cresc_receita"]))
    if "cresc_lucro_hist" in score_df.columns:
        growth_parts.append(_z(score_df["cresc_lucro_hist"]))

    score_df["score_crescimento"] = np.nan
    if growth_parts:
        score_df["score_crescimento"] = np.nanmean(np.vstack(growth_parts), axis=0)

    # Total
    score_df["score_total"] = np.nanmean(
        np.vstack(
            [
                score_df["score_valor"].fillna(0).values,
                score_df["score_qualidade"].fillna(0).values,
                score_df["score_crescimento"].fillna(0).values,
            ]
        ),
        axis=0,
    )

    score_df = score_df.reset_index()

    sort_col_map = {
        "Score total": "score_total",
        "Score crescimento": "score_crescimento",
        "Score valor": "score_valor",
        "ROE": "roe",
        "Margem líquida": "margem_liquida",
    }
    sort_col = sort_col_map.get(criterio_ordenacao, "score_total")
    if sort_col in score_df.columns:
        score_df = score_df.sort_values(sort_col, ascending=False)

    # DCF simplificado (referência)
    fair_value = {}
    if dfp_last is not None and not dfp_last.empty and "Lucro_Liquido" in dfp_last.columns:
        base = dfp_last.set_index("ticker")["Lucro_Liquido"].astype(float)
        anos = 5
        for t in tickers:
            if t not in base.index:
                continue
            f0 = base.loc[t]
            if pd.isna(f0) or f0 <= 0:
                continue
            pv = 0.0
            for k in range(1, anos + 1):
                fk = f0 * ((1 + float(cresc_lucro)) ** k)
                pv += fk / ((1 + float(desconto_dcf)) ** k)
            pv *= (1 - float(margem_seg))
            fair_value[t] = pv

    score_df["valor_justo_dcf_ref"] = score_df["ticker"].map(fair_value)

    # ───────────────────────── UI ─────────────────────────
    st.subheader("Ranking do universo (heurístico)")
    st.caption("Ordenação conforme critério selecionado. O 'valor justo DCF' é uma referência simplificada.")

    top = score_df.head(20).copy()
    cols = [
        "ticker", "score_total", "score_valor", "score_qualidade", "score_crescimento",
        "roe", "margem_liquida", "pl", "pvp", "dividend_yield", "valor_justo_dcf_ref",
    ]
    cols = [c for c in cols if c in top.columns]

    st.dataframe(
        top[cols].rename(
            columns={
                "ticker": "Ticker",
                "score_total": "Score total",
                "score_valor": "Score valor",
                "score_qualidade": "Score qualidade",
                "score_crescimento": "Score crescimento",
                "roe": "ROE",
                "margem_liquida": "Margem líquida",
                "pl": "P/L",
                "pvp": "P/VP",
                "dividend_yield": "DY",
                "valor_justo_dcf_ref": "Valor justo (DCF ref.)",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Distribuições (universo)")
    dist_cols = st.columns(3)

    if "score_total" in score_df.columns:
        dist_cols[0].plotly_chart(px.histogram(score_df, x="score_total", nbins=25, title="Score total"), use_container_width=True)
    if "pl" in score_df.columns:
        dist_cols[1].plotly_chart(px.histogram(score_df, x="pl", nbins=25, title="P/L"), use_container_width=True)
    if "dividend_yield" in score_df.columns:
        dist_cols[2].plotly_chart(px.histogram(score_df, x="dividend_yield", nbins=25, title="Dividend Yield"), use_container_width=True)

    st.subheader("Detalhe por ticker")
    pick = st.selectbox("Selecione um ticker para detalhar", score_df["ticker"].tolist(), key="adv_pick")

    c1, c2, c3 = st.columns([1, 2, 2])
    with c1:
        st.image(get_logo_url(pick), width=96)
        st.markdown(f"### {pick}")

    row = score_df.loc[score_df["ticker"] == pick].iloc[0].to_dict()
    with c2:
        st.markdown("**Scores**")
        st.write(f"Score total: {row.get('score_total', np.nan):.2f}")
        st.write(f"Score valor: {row.get('score_valor', np.nan):.2f}")
        st.write(f"Score qualidade: {row.get('score_qualidade', np.nan):.2f}")
        st.write(f"Score crescimento: {row.get('score_crescimento', np.nan):.2f}")

    with c3:
        st.markdown("**Múltiplos / Indicadores**")
        st.write(f"ROE: {_fmt_pct(row.get('roe'))}")
        st.write(f"Margem líquida: {_fmt_pct(row.get('margem_liquida'))}")
        st.write(f"Dívida líquida / EBITDA: {row.get('div_liq_ebitda', '—')}")
        st.write(f"P/L: {row.get('pl', '—')}")
        st.write(f"P/VP: {row.get('pvp', '—')}")
        st.write(f"DY: {_fmt_pct(row.get('dividend_yield'))}")
        st.write(f"Valor justo (DCF ref.): {_fmt_money(row.get('valor_justo_dcf_ref'))}")
