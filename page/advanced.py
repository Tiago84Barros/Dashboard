from __future__ import annotations

import logging
import textwrap
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px

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

logger = logging.getLogger(__name__)


# ============================================================
# Utilidades
# ============================================================

def _md(html: str) -> None:
    """Renderiza HTML/CSS de forma segura (evita code block do Markdown)."""
    st.markdown(textwrap.dedent(html).strip(), unsafe_allow_html=True)


def _norm_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    return t if t.endswith(".SA") else f"{t}.SA"


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "")


def _clean_df_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.astype(str).str.strip().str.replace("\ufeff", "", regex=False)
    return df


def _count_years_from_dre(dre: Optional[pd.DataFrame]) -> int:
    if dre is None or dre.empty or "Data" not in dre.columns:
        return 0
    return int(pd.to_datetime(dre["Data"], errors="coerce").dt.year.dropna().nunique())


@dataclass(frozen=True)
class EmpresaDados:
    ticker: str
    nome: str
    dre: pd.DataFrame
    mult: pd.DataFrame


def _load_empresa_dados(ticker: str, nome: str) -> Optional[EmpresaDados]:
    tk = _strip_sa(ticker)
    if not tk:
        return None

    dre = load_data_from_db(_norm_sa(tk))
    mult = load_multiplos_from_db(_norm_sa(tk))

    if dre is None or mult is None or dre.empty or mult.empty:
        return None

    dre = _clean_df_cols(dre)
    mult = _clean_df_cols(mult)

    if "Data" in dre.columns and "Ano" not in dre.columns:
        dre["Ano"] = pd.to_datetime(dre["Data"], errors="coerce").dt.year
    if "Data" in mult.columns and "Ano" not in mult.columns:
        mult["Ano"] = pd.to_datetime(mult["Data"], errors="coerce").dt.year

    return EmpresaDados(ticker=tk, nome=nome, dre=dre, mult=mult)


def _safe_macro() -> Optional[pd.DataFrame]:
    dm = load_macro_summary()
    if dm is None or dm.empty:
        return None
    dm = _clean_df_cols(dm)
    if "Data" in dm.columns:
        dm["Data"] = pd.to_datetime(dm["Data"], errors="coerce")
        dm = dm.dropna(subset=["Data"]).sort_values("Data")
    return dm


# ============================================================
# Render
# ============================================================

def render() -> None:
    _md("<h1 style='text-align:center'>Análise Avançada de Ações</h1>")

    # ---------- Base setores ----------
    setores = st.session_state.get("setores_df")
    if setores is None or setores.empty:
        setores = load_setores_from_db()
        setores = _clean_df_cols(setores)
        st.session_state["setores_df"] = setores

    dados_macro = _safe_macro()
    if dados_macro is None:
        st.error("Falha ao carregar dados macroeconômicos.")
        return

    # ---------- Sidebar ----------
    with st.sidebar:
        setor = st.selectbox("Setor:", sorted(setores["SETOR"].dropna().unique()))
        subsetor = st.selectbox(
            "Subsetor:",
            sorted(setores[setores["SETOR"] == setor]["SUBSETOR"].dropna().unique()),
        )
        segmento = st.selectbox(
            "Segmento:",
            sorted(
                setores[
                    (setores["SETOR"] == setor)
                    & (setores["SUBSETOR"] == subsetor)
                ]["SEGMENTO"].dropna().unique()
            ),
        )
        tipo = st.radio(
            "Perfil da empresa:",
            ["Crescimento (<10 anos)", "Estabelecida (≥10 anos)", "Todas"],
            index=2,
        )

    # ---------- Filtra empresas ----------
    seg_df = setores[
        (setores["SETOR"] == setor)
        & (setores["SUBSETOR"] == subsetor)
        & (setores["SEGMENTO"] == segmento)
    ].copy()

    seg_df["ticker"] = seg_df["ticker"].astype(str).apply(_strip_sa)
    if "nome_empresa" not in seg_df.columns:
        seg_df["nome_empresa"] = seg_df["ticker"]

    seg_df = seg_df.dropna(subset=["ticker"])
    if seg_df.empty:
        st.warning("Nenhuma empresa encontrada.")
        return

    # ---------- Histórico ----------
    years_map: Dict[str, int] = {}

    def _year_check(tk: str):
        dre = load_data_from_db(_norm_sa(tk))
        return tk, _count_years_from_dre(dre)

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = [ex.submit(_year_check, tk) for tk in seg_df["ticker"].unique()]
        for f in as_completed(futs):
            tk, n = f.result()
            years_map[tk] = n

    def _pass_tipo(tk: str) -> bool:
        n = years_map.get(tk, 0)
        if tipo == "Crescimento (<10 anos)":
            return 0 < n < 10
        if tipo == "Estabelecida (≥10 anos)":
            return n >= 10
        return n > 0

    seg_df = seg_df[seg_df["ticker"].apply(_pass_tipo)]
    if seg_df.empty:
        st.warning("Filtro eliminou todas as empresas.")
        return

    # ---------- Carrega dados ----------
    empresas: List[EmpresaDados] = []
    for r in seg_df.itertuples():
        item = _load_empresa_dados(r.ticker, r.nome_empresa)
        if item:
            empresas.append(item)

    if len(empresas) < 2:
        st.warning("Dados insuficientes.")
        return

    setores_empresa = {e.ticker: obter_setor_da_empresa(e.ticker, setores) for e in empresas}
    pesos = get_pesos(setor)

    payload = [
        {"ticker": e.ticker, "nome": e.nome, "dre": e.dre, "multiplos": e.mult}
        for e in empresas
    ]

    score = calcular_score_acumulado(payload, setores_empresa, pesos, dados_macro, anos_minimos=4)
    if score.empty:
        st.warning("Score vazio.")
        return

    # ---------- Preços ----------
    tickers = [_norm_sa(t) for t in score["ticker"].unique()]
    precos = baixar_precos(tickers)
    precos.index = pd.to_datetime(precos.index, errors="coerce")
    precos = precos.dropna(how="all")

    precos_m = precos.resample("M").last()
    score = penalizar_plato(score, precos_m, meses=12, penal=0.30)

    dividendos = coletar_dividendos(tickers)
    lideres = determinar_lideres(score)

    # ---------- Simulações ----------
    patrimonio_estrategia, datas = gerir_carteira(precos, score, lideres, dividendos)
    patrimonio_empresas = gerir_carteira_todas_empresas(precos, score["ticker"].unique(), datas, dividendos)
    patrimonio_selic = calcular_patrimonio_selic_macro(dados_macro, datas)

    patrimonio_final = pd.concat(
        [patrimonio_estrategia, patrimonio_empresas, patrimonio_selic],
        axis=1,
    ).ffill()

    # ---------- Gráfico ----------
    fig, ax = plt.subplots(figsize=(12, 6))
    patrimonio_final.plot(ax=ax)
    ax.set_ylabel("R$")
    ax.grid(True, alpha=0.3)
    st.pyplot(fig)

    # ---------- CSS ----------
    _md("""
    <style>
      .pf-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:18px; }
      .pf-card { background:#0f172a; border-radius:16px; padding:18px; }
      .pf-head { display:grid; grid-template-columns:48px 1fr; gap:12px; align-items:center; }
      .pf-logo img { width:38px; height:38px; }
      .pf-title { font-weight:800; font-size:18px; }
      .pf-value { margin-top:14px; font-size:22px; font-weight:900; color:#22c55e; }
      .pf-foot { margin-top:6px; font-size:12px; opacity:.7; }
      .pf-badge { display:inline-flex; gap:6px; padding:4px 10px; border-radius:999px;
                  background:rgba(255,255,255,.08); font-size:12px; }
    </style>
    """)

    # ---------- Cards ----------
    st.markdown("## Patrimônio final por ativo")
    last = patrimonio_final.iloc[-1].dropna().sort_values(ascending=False)
    lider_counts = score.groupby("ticker")["Ano"].nunique().to_dict()

    _md("<div class='pf-grid'>")

    for tk, val in last.items():
        logo = (
            "https://raw.githubusercontent.com/twitter/twemoji/master/assets/72x72/1f4b0.png"
            if tk.lower() == "tesouro selic"
            else get_logo_url(tk)
        )

        badge = ""
        if lider_counts.get(tk, 0) > 0:
            badge = (
                f"<span class='pf-badge'>"
                f"🏆 {lider_counts[tk]}x Líder"
                f"</span>"
            )

        _md(f"""
        <div class="pf-card">
          <div class="pf-head">
            <img src="{logo}" />
            <div>
              <div class="pf-title">{tk}</div>
              {badge}
            </div>
          </div>
          <div class="pf-value">{formatar_real(val)}</div>
          <div class="pf-foot">Valor final</div>
        </div>
        """)

    _md("</div>")
