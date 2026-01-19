# pages/advanced.py
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Optional, Set

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px

# ─────────────────────────────────────────────────────────────
# IMPORTS ORIGINAIS DO PROJETO (MANTIDOS)
# ─────────────────────────────────────────────────────────────
from core.helpers import (
    get_logo_url,
    obter_setor_da_empresa,
    determinar_lideres,
    formatar_real,
)
from core.db_loader import (
    load_setores_from_db,
    load_data_from_db,
    load_multiplos_from_db,
    load_macro_summary,
)
from core.yf_data import baixar_precos, coletar_dividendos
from core.scoring import calcular_score_acumulado, penalizar_plato
from core.portfolio import (
    gerir_carteira,
    gerir_carteira_todas_empresas,
    calcular_patrimonio_selic_macro,
)
from core.weights import get_pesos

try:
    from core.scoring_v2 import calcular_score_acumulado_v2
except Exception:
    calcular_score_acumulado_v2 = None

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONSTANTES (SOMENTE PARA PAINEL DECISÓRIO)
# ─────────────────────────────────────────────────────────────
LOWER_IS_BETTER = {
    "P/L", "P/VP", "Endividamento_Total", "Alavancagem_Financeira",
    "Divida_Liquida", "Divida_Liquida_EBITDA",
}
RESERVED_COLS = {"Ano", "Data", "Ticker", "ticker", "Empresa", "Nome"}

# ─────────────────────────────────────────────────────────────
# FUNÇÕES AUXILIARES (MINIMAMENTE INTRUSIVAS)
# ─────────────────────────────────────────────────────────────
def ensure_year(df: pd.DataFrame) -> pd.DataFrame:
    if df is not None and not df.empty and "Ano" not in df.columns and "Data" in df.columns:
        df = df.copy()
        df["Ano"] = pd.to_datetime(df["Data"], errors="coerce").dt.year
    return df

def numeric_indicators(df: pd.DataFrame) -> List[str]:
    cols = []
    for c in df.columns:
        if c in RESERVED_COLS:
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        if s.notna().sum() >= 3:
            cols.append(c)
    return cols

def build_long(empresas, source: str, col: str) -> pd.DataFrame:
    rows = []
    for e in empresas:
        df = getattr(e, source, None)
        if df is None or df.empty:
            continue
        df = ensure_year(df)
        if "Ano" not in df.columns or col not in df.columns:
            continue

        tmp = df[["Ano", col]].copy()
        tmp["Ano"] = pd.to_numeric(tmp["Ano"], errors="coerce")
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce")
        tmp = tmp.dropna()

        if source == "mult":
            tmp = tmp.groupby("Ano", as_index=False)[col].mean()
        else:
            tmp = tmp.groupby("Ano", as_index=False)[col].sum()

        for _, r in tmp.iterrows():
            rows.append({"Ano": int(r["Ano"]), "Ticker": e.ticker, "Valor": float(r[col])})
    return pd.DataFrame(rows)

def render_segment_vs_leaders(df_long, top_tickers, year, indicador, use_median=True):
    df = df_long[df_long["Ano"] == year].copy()
    if df.empty:
        st.warning("Sem dados para o indicador no ano base.")
        return

    ref = df["Valor"].median() if use_median else df["Valor"].mean()
    leaders = df[df["Ticker"].isin(top_tickers)].groupby("Ticker", as_index=False)["Valor"].mean()

    leaders["Ref_Segmento"] = ref
    leaders["Gap_%"] = (leaders["Valor"] / ref - 1) * 100 if ref else np.nan

    if indicador in LOWER_IS_BETTER:
        leaders["Status"] = np.where(leaders["Gap_%"] < 0, "Melhor", "Pior")
        leaders = leaders.sort_values("Gap_%")
    else:
        leaders["Status"] = np.where(leaders["Gap_%"] > 0, "Melhor", "Pior")
        leaders = leaders.sort_values("Gap_%", ascending=False)

    líder = leaders.iloc[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Referência do Segmento", f"{ref:,.2f}")
    c2.metric("Líder", f"{líder['Ticker']} | {líder['Valor']:,.2f}")
    c3.metric("Gap vs Segmento", f"{líder['Gap_%']:+.1f}%")

    fig = px.bar(
        leaders.head(5),
        x="Gap_%", y="Ticker", orientation="h",
        title="Líderes vs Segmento (Gap %)"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        leaders[["Ticker", "Valor", "Ref_Segmento", "Gap_%", "Status"]].round(2),
        use_container_width=True
    )

# ─────────────────────────────────────────────────────────────
# ESTRUTURA DE DADOS POR EMPRESA (ORIGINAL)
# ─────────────────────────────────────────────────────────────
@dataclass
class EmpresaDados:
    ticker: str
    nome: str
    dre: pd.DataFrame
    mult: pd.DataFrame

def load_empresa(ticker: str, nome: str) -> Optional[EmpresaDados]:
    dre = load_data_from_db(f"{ticker}.SA")
    mult = load_multiplos_from_db(f"{ticker}.SA")
    if dre is None or mult is None or dre.empty or mult.empty:
        return None
    return EmpresaDados(ticker=ticker, nome=nome, dre=dre, mult=mult)

# ─────────────────────────────────────────────────────────────
# RENDER (ORIGINAL + AJUSTE PONTUAL)
# ─────────────────────────────────────────────────────────────
def render():

    st.title("Análise Avançada")

    setores = load_setores_from_db()
    setor = st.sidebar.selectbox("Setor", sorted(setores["SETOR"].unique()))
    subsetor = st.sidebar.selectbox(
        "Subsetor",
        sorted(setores[setores["SETOR"] == setor]["SUBSETOR"].unique())
    )
    segmento = st.sidebar.selectbox(
        "Segmento",
        sorted(setores[
            (setores["SETOR"] == setor) &
            (setores["SUBSETOR"] == subsetor)
        ]["SEGMENTO"].unique())
    )

    seg_df = setores[
        (setores["SETOR"] == setor) &
        (setores["SUBSETOR"] == subsetor) &
        (setores["SEGMENTO"] == segmento)
    ].copy()

    tickers = seg_df["ticker"].astype(str).str.replace(".SA", "").tolist()

    empresas: List[EmpresaDados] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(load_empresa, t, t) for t in tickers]
        for f in as_completed(futs):
            e = f.result()
            if e:
                empresas.append(e)

    if len(empresas) < 2:
        st.warning("Dados insuficientes.")
        return

    pesos = get_pesos(setor)
    payload = [{"ticker": e.ticker, "nome": e.nome, "multiplos": e.mult, "dre": e.dre} for e in empresas]

    dados_macro = load_macro_summary()

    score = calcular_score_acumulado(payload, {}, pesos, dados_macro)
    if score.empty:
        st.warning("Score vazio.")
        return

    ano_base = int(score["Ano"].max())
    top_tickers = (
        score[score["Ano"] == ano_base]
        .sort_values("Score_Ajustado", ascending=False)["ticker"]
        .head(5).tolist()
    )

    st.markdown("## Segmento vs Líderes")

    fonte = st.selectbox("Fonte", ["Múltiplos", "DRE"])
    source = "mult" if fonte == "Múltiplos" else "dre"

    indicadores: Set[str] = set()
    for e in empresas:
        df = ensure_year(getattr(e, source))
        indicadores.update(numeric_indicators(df))

    indicador = st.selectbox("Indicador", sorted(indicadores))

    df_long = build_long(empresas, source, indicador)
    render_segment_vs_leaders(df_long, top_tickers, ano_base, indicador)
