from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from core.db.engine import get_engine
from core.db.loader import (
    load_setores,
    load_demonstracoes_financeiras,
    load_multiplos,
    load_macro_summary,
    load_macro_mensal,
)
from analytics.helpers import (
    obter_setor_da_empresa,
    determinar_lideres,
    get_logo_url,
)
from analytics.scoring import (
    calcular_score_acumulado,
    penalizar_plato,
)
from analytics.portfolio import (
    calcular_patrimonio_selic_macro,
    gerir_carteira,
    encontrar_proxima_data_valida,
    gerir_carteira_simples,
)
from data_sources.yf_data import (
    baixar_precos,
    coletar_dividendos,
    baixar_precos_ano_corrente,
)
from analytics.weights import get_pesos

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Engine (Supabase)
# ─────────────────────────────────────────────────────────────
@st.cache_resource
def _engine():
    return get_engine()


# ─────────────────────────────────────────────────────────────
# Utilitários internos
# ─────────────────────────────────────────────────────────────

def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.astype(str).str.strip().str.replace("\ufeff", "", regex=False)
    return out


def _norm_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    return t if t.endswith(".SA") else f"{t}.SA"


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "")


def _pick_date_col(df: pd.DataFrame) -> Optional[str]:
    for c in ("Data", "data"):
        if c in df.columns:
            return c
    return None


def _safe_years_from_dre(dre: pd.DataFrame) -> int:
    col = _pick_date_col(dre)
    if not col:
        return 0
    y = pd.to_datetime(dre[col], errors="coerce").dt.year
    return int(y.dropna().nunique())


# ─────────────────────────────────────────────────────────────
# MACRO — NORMALIZAÇÃO CRÍTICA (igual ao advanced.py)
# ─────────────────────────────────────────────────────────────

def _safe_macro(engine) -> Optional[pd.DataFrame]:
    dm = load_macro_mensal(engine)

    if isinstance(dm, dict):
        dm = pd.DataFrame([dm])

    if dm is None or dm.empty:
        return None

    dm = _clean_columns(dm)

    # Data
    col = _pick_date_col(dm)
    if col:
        dm[col] = pd.to_datetime(dm[col], errors="coerce")
        dm = dm.dropna(subset=[col]).sort_values(col)
        if col != "Data":
            dm = dm.rename(columns={col: "Data"})
    elif dm.index.name in ("Data", "data"):
        dm = dm.reset_index()
        dm["Data"] = pd.to_datetime(dm["Data"], errors="coerce")
        dm = dm.dropna(subset=["Data"]).sort_values("Data")
    else:
        return None

    # Selic (padroniza case)
    if "selic" in dm.columns and "Selic" not in dm.columns:
        dm = dm.rename(columns={"selic": "Selic"})
    if "Selic" not in dm.columns:
        return None

    dm["Selic"] = pd.to_numeric(dm["Selic"], errors="coerce")

    return dm


# ─────────────────────────────────────────────────────────────
# Estruturas de dados
# ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class EmpresaCarregada:
    ticker: str
    nome: str
    multiplos: pd.DataFrame
    dre: pd.DataFrame


def _carregar_empresa(row: dict, engine) -> Optional[EmpresaCarregada]:
    try:
        tk = _strip_sa(row.get("ticker", ""))
        if not tk:
            return None

        nome = row.get("nome_empresa", tk)
        tk_sa = _norm_sa(tk)

        mult = load_multiplos(tk_sa, engine=engine)
        dre = load_demonstracoes_financeiras(tk_sa, engine=engine)

        if mult is None or dre is None or mult.empty or dre.empty:
            return None

        mult = _clean_columns(mult)
        dre = _clean_columns(dre)

        col_m = _pick_date_col(mult)
        if col_m:
            mult["Ano"] = pd.to_datetime(mult[col_m], errors="coerce").dt.year

        col_d = _pick_date_col(dre)
        if col_d:
            dre["Ano"] = pd.to_datetime(dre[col_d], errors="coerce").dt.year

        return EmpresaCarregada(ticker=tk, nome=nome, multiplos=mult, dre=dre)
    except Exception:
        return None


def _filtrar_tickers_min_anos(
    tickers: Sequence[str],
    engine,
    min_anos: int = 10,
) -> List[str]:

    def _check(tk: str):
        dre = load_demonstracoes_financeiras(_norm_sa(tk), engine=engine)
        return tk, (_safe_years_from_dre(dre) >= min_anos)

    ok = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = [ex.submit(_check, t) for t in set(tickers)]
        for fut in as_completed(futs):
            tk, good = fut.result()
            if good:
                ok.append(tk)

    return sorted(ok)


# ─────────────────────────────────────────────────────────────
# Render principal
# ─────────────────────────────────────────────────────────────

def render():
    st.markdown("<h1 style='text-align:center'>Criação de Portfólio</h1>", unsafe_allow_html=True)

    with st.sidebar:
        margem = st.text_input("% acima da Selic para destacar:", "")
        gerar = st.button("Gerar Portfólio")

    if not margem.strip():
        st.warning("Informe a margem percentual.")
        return

    try:
        margem = float(margem)
    except ValueError:
        st.error("Valor inválido.")
        return

    if not gerar:
        st.stop()

    engine = _engine()

    setores = load_setores(engine=engine)
    setores = _clean_columns(setores)

    dados_macro = _safe_macro(engine)
    if dados_macro is None:
        st.error("Falha ao carregar dados macroeconômicos (SELIC).")
        return

    setores_unicos = setores[["SETOR", "SUBSETOR", "SEGMENTO"]].drop_duplicates()

    empresas_lideres_finais = []

    for _, seg in setores_unicos.iterrows():
        setor, subsetor, segmento = seg

        empresas_seg = setores[
            (setores["SETOR"] == setor)
            & (setores["SUBSETOR"] == subsetor)
            & (setores["SEGMENTO"] == segmento)
        ]

        tickers = [_strip_sa(t) for t in empresas_seg["ticker"].astype(str)]
        tickers = [t for t in tickers if t]

        if len(set(tickers)) <= 1:
            continue

        tickers_ok = _filtrar_tickers_min_anos(tickers, engine, min_anos=10)
        if len(tickers_ok) <= 1:
            continue

        empresas_validas = empresas_seg[
            empresas_seg["ticker"].apply(lambda x: _strip_sa(str(x)) in tickers_ok)
        ]

        rows = empresas_validas.to_dict("records")
        empresas = []

        with ThreadPoolExecutor(max_workers=12) as ex:
            futs = [ex.submit(_carregar_empresa, r, engine) for r in rows]
            for fut in as_completed(futs):
                e = fut.result()
                if e:
                    empresas.append(e)

        if len(empresas) <= 1:
            continue

        setores_empresa = {e.ticker: obter_setor_da_empresa(e.ticker, setores) for e in empresas}
        pesos = get_pesos(setor)

        payload = [
            {"ticker": e.ticker, "nome": e.nome, "multiplos": e.multiplos, "dre": e.dre}
            for e in empresas
        ]

        score = calcular_score_acumulado(payload, setores_empresa, pesos, dados_macro, anos_minimos=4)
        if score is None or score.empty:
            continue

        precos = baixar_precos([_norm_sa(e.ticker) for e in empresas])
        if precos is None or precos.empty:
            continue

        precos.index = pd.to_datetime(precos.index)
        precos_m = precos.resample("M").last()
        score = penalizar_plato(score, precos_m, meses=12, penal=0.30)

        lideres = determinar_lideres(score)
        if lideres is None or lideres.empty:
            continue

        dividendos = coletar_dividendos([_norm_sa(t) for t in score["ticker"].unique()])
        patrimonio, datas_aportes = gerir_carteira(precos, score, lideres, dividendos)

        patrimonio_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aportes)
        if patrimonio_selic is None or patrimonio_selic.empty:
            continue

        final_empresas = patrimonio.iloc[-1].drop("Patrimônio", errors="ignore").sum()
        final_selic = patrimonio_selic.iloc[-1]["Tesouro Selic"]

        diff = ((final_empresas / final_selic) - 1) * 100
        if diff < margem:
            continue

        st.markdown(f"### {setor} > {subsetor} > {segmento}")
        st.markdown(f"**Resultado:** {diff:.1f}% acima do Tesouro Selic")

        empresas_lideres_finais.extend(
            lideres.assign(
                setor=setor,
                nome=lideres["ticker"].map(
                    lambda x: next((e.nome for e in empresas if e.ticker == x), x)
                ),
            ).to_dict("records")
        )

    if empresas_lideres_finais:
        st.markdown("## Empresas líderes sugeridas")
        for e in empresas_lideres_finais:
            st.markdown(f"- **{e['nome']} ({e['ticker']})** — setor {e['setor']}")

