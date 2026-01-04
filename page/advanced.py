from __future__ import annotations

import logging
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import streamlit as st
import pandas as pd
import numpy as np
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
from core.scoring import (
    calcular_score_acumulado,
    penalizar_plato,
)

# Score v2 (robusto) — import opcional para não quebrar o app se o módulo ainda não existir
try:
    from core.scoring_v2 import calcular_score_acumulado_v2  # type: ignore
except Exception:
    calcular_score_acumulado_v2 = None  # type: ignore

from core.portfolio import (
    gerir_carteira,
    gerir_carteira_todas_empresas,
    calcular_patrimonio_selic_macro,
)
from core.weights import get_pesos

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Utilitários internos
# ─────────────────────────────────────────────────────────────

def _norm_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    if not t:
        return t
    return t if t.endswith(".SA") else f"{t}.SA"


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "")


def _clean_df_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.astype(str).str.strip().str.replace("\ufeff", "", regex=False)
    return out


def _count_years_from_dre(dre: Optional[pd.DataFrame]) -> int:
    if dre is None or dre.empty or "Data" not in dre.columns:
        return 0
    y = pd.to_datetime(dre["Data"], errors="coerce").dt.year
    return int(y.dropna().nunique())


@dataclass(frozen=True)
class EmpresaDados:
    ticker: str  # sem .SA
    nome: str
    dre: pd.DataFrame
    mult: pd.DataFrame


def _load_empresa_dados(ticker: str, nome: str) -> Optional[EmpresaDados]:
    """Carrega DRE + múltiplos para um ticker (B3), retornando estrutura padronizada."""
    tk = _strip_sa(ticker)
    if not tk:
        return None
    tk_sa = _norm_sa(tk)

    dre = load_data_from_db(tk_sa)
    mult = load_multiplos_from_db(tk_sa)

    if dre is None or mult is None or dre.empty or mult.empty:
        return None

    dre = _clean_df_cols(dre)
    mult = _clean_df_cols(mult)

    # adiciona Ano quando existir Data
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
    # portfolio.calcular_patrimonio_selic_macro aceita Data em coluna ou índice com nome Data
    if "Data" in dm.columns:
        dm["Data"] = pd.to_datetime(dm["Data"], errors="coerce")
        dm = dm.dropna(subset=["Data"]).sort_values("Data")
    return dm


# ─────────────────────────────────────────────────────────────
# Render
# ─────────────────────────────────────────────────────────────

def render() -> None:
    st.markdown("<h1 style='text-align:center'>Análise Avançada de Ações</h1>", unsafe_allow_html=True)

    # ── setores em sessão
    setores = st.session_state.get("setores_df")
    if setores is None or getattr(setores, "empty", True):
        setores = load_setores_from_db()
        if setores is None or setores.empty:
            st.error("Não foi possível carregar a base de setores do banco.")
            return
        setores = _clean_df_cols(setores)
        st.session_state["setores_df"] = setores
    # ─────────────────────────────────────────────────────────────
    # Mapas ticker -> (SEGMENTO / SUBSETOR / SETOR) para Score v2 fallback
    # ─────────────────────────────────────────────────────────────
    _tmp = setores[["ticker", "SEGMENTO", "SUBSETOR", "SETOR"]].copy()
    
    # garante ticker sem ".SA" e uppercase
    _tmp["ticker"] = _tmp["ticker"].astype(str).str.upper().str.replace(".SA", "", regex=False).str.strip()
    
    # preenche nulos
    _tmp["SEGMENTO"] = _tmp["SEGMENTO"].fillna("OUTROS").astype(str)
    _tmp["SUBSETOR"] = _tmp["SUBSETOR"].fillna("OUTROS").astype(str)
    _tmp["SETOR"] = _tmp["SETOR"].fillna("OUTROS").astype(str)
    
    group_map = dict(zip(_tmp["ticker"], _tmp["SEGMENTO"]))
    subsetor_map = dict(zip(_tmp["ticker"], _tmp["SUBSETOR"]))
    setor_map = dict(zip(_tmp["ticker"], _tmp["SETOR"]))

    # validação mínima de schema (case-sensitive conforme Postgres com colunas entre aspas)
    needed = {"SETOR", "SUBSETOR", "SEGMENTO", "ticker"}
    if not needed.issubset(setores.columns):
        st.error(f"A tabela de setores não contém colunas esperadas: {sorted(needed)}")
        return

    dados_macro = _safe_macro()
    if dados_macro is None or dados_macro.empty:
        st.error("Não foi possível carregar os dados macroeconômicos (info_economica).")
        return

    # ── Sidebar filtros
    with st.sidebar:
        setor = st.selectbox("Setor:", sorted(setores["SETOR"].dropna().unique().tolist()))
        subsetores = setores.loc[setores["SETOR"] == setor, "SUBSETOR"].dropna().unique().tolist()
        subsetor = st.selectbox("Subsetor:", sorted(subsetores))
        segmentos = setores.loc[
            (setores["SETOR"] == setor) & (setores["SUBSETOR"] == subsetor),
            "SEGMENTO"
        ].dropna().unique().tolist()
        segmento = st.selectbox("Segmento:", sorted(segmentos))
        tipo = st.radio("Perfil de empresa:", ["Crescimento (<10 anos)", "Estabelecida (≥10 anos)", "Todas"], index=2)

        # Scoring (v2 robusto) — não altera layout principal
        with st.expander("Scoring (opções)", expanded=False):
            if calcular_score_acumulado_v2 is None:
                st.caption("Score v2 indisponível (core/scoring_v2.py não encontrado). Usando v1.")
                use_score_v2 = False
            else:
                use_score_v2 = st.checkbox(
                    "Usar Score v2 (robusto: winsorização + percentil)",
                    value=True,
                )
                st.caption("v2 mantém a estratégia, mas torna o ranking menos sensível a outliers e mais estável.")

    # ── filtra tickers do segmento
    seg_df = setores[
        (setores["SETOR"] == setor) &
        (setores["SUBSETOR"] == subsetor) &
        (setores["SEGMENTO"] == segmento)
    ].copy()

    if seg_df.empty:
        st.warning("Nenhuma empresa encontrada para os filtros escolhidos.")
        return

    # normaliza tickers e nomes
    seg_df["ticker"] = seg_df["ticker"].astype(str).apply(_strip_sa)
    if "nome_empresa" not in seg_df.columns:
        seg_df["nome_empresa"] = seg_df["ticker"]

    seg_df = seg_df.dropna(subset=["ticker"])
    seg_df = seg_df[seg_df["ticker"].astype(str).str.len() > 0]

    if seg_df.empty:
        st.warning("Nenhuma empresa válida encontrada para os filtros escolhidos.")
        return

    # ─────────────────────────────────────────────────────────
    # (Opcional, não disruptivo) Diagnóstico colapsável
    # ─────────────────────────────────────────────────────────
    with st.expander("Diagnóstico (dados do Supabase)", expanded=False):
        st.caption("Seção apenas informativa. Não altera resultados nem layout principal.")
        st.write(
            {
                "Setor": setor,
                "Subsetor": subsetor,
                "Segmento": segmento,
                "Empresas no segmento (bruto)": int(len(seg_df)),
                "Linhas setores_df": int(len(setores)),
                "Macro (linhas)": int(len(dados_macro)),
                "Macro (data mínima)": str(pd.to_datetime(dados_macro["Data"]).min()) if "Data" in dados_macro.columns else "n/a",
                "Macro (data máxima)": str(pd.to_datetime(dados_macro["Data"]).max()) if "Data" in dados_macro.columns else "n/a",
            }
        )

    # ─────────────────────────────────────────────────────────
    # 1) Filtrar por histórico (<10 / ≥10) com paralelismo (DRE)
    # ─────────────────────────────────────────────────────────
    tickers = seg_df["ticker"].drop_duplicates().tolist()

    def _year_check(tk: str) -> Tuple[str, int]:
        try:
            dre = load_data_from_db(_norm_sa(tk))
            return tk, _count_years_from_dre(dre)
        except Exception:
            return tk, 0

    years_map: Dict[str, int] = {}
    max_workers = min(12, max(2, len(tickers)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_year_check, tk) for tk in tickers]
        for fut in as_completed(futs):
            tk, y = fut.result()
            years_map[tk] = y

    if tipo == "Crescimento (<10 anos)":
        tickers = [t for t in tickers if years_map.get(t, 0) < 10]
    elif tipo == "Estabelecida (≥10 anos)":
        tickers = [t for t in tickers if years_map.get(t, 0) >= 10]

    seg_df = seg_df[seg_df["ticker"].isin(tickers)].copy()
    if seg_df.empty:
        st.warning("Nenhuma empresa permaneceu após o filtro de histórico.")
        return

    st.markdown("## Empresas do segmento")

    cols = st.columns(3)
    for i, r in enumerate(seg_df[["ticker", "nome_empresa"]].drop_duplicates().to_dict("records")):
        tk = r["ticker"]
        nm = r["nome_empresa"]
        anos_hist = years_map.get(tk, 0)
        logo_url = get_logo_url(tk)

        with cols[i % 3]:
            st.markdown(
                f"""
            <div style="border:2px solid #ddd;border-radius:10px;padding:12px;margin:8px;background:#f9f9f9;text-align:center;box-sizing:border-box;width:100%;">
                <img src="{logo_url}" style="width:45px;height:45px;margin-bottom:8px;">
                <div style="font-weight:700;color:#333;">{nm} ({tk})</div>
                <div style="font-size:12px;color:#666;">Histórico DRE: {anos_hist} ano(s)</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 2) Carregar dados completos (múltiplos + DRE) em paralelo
    # ─────────────────────────────────────────────────────────
    rows = seg_df[["ticker", "nome_empresa"]].drop_duplicates().to_dict("records")
    empresas: List[EmpresaDados] = []

    max_workers = min(12, max(2, len(rows)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_load_empresa_dados, r["ticker"], r["nome_empresa"]) for r in rows]
        for fut in as_completed(futs):
            try:
                item = fut.result()
            except Exception as e:
                logger.exception("Falha ao carregar dados de empresa: %s", e)
                item = None
            if item is not None:
                empresas.append(item)

    if len(empresas) < 2:
        st.warning("Não há dados suficientes (DRE/Múltiplos) para pelo menos 2 empresas do segmento.")
        return

    # setores_empresa (por ticker sem .SA)
    setores_empresa = {e.ticker: obter_setor_da_empresa(e.ticker, setores) for e in empresas}

    # pesos por setor (regra existente)
    pesos = get_pesos(setor)

    # payload scoring (compatível com scoring.py)
    payload = [{"ticker": e.ticker, "nome": e.nome, "multiplos": e.mult, "dre": e.dre} for e in empresas]

    # score (v1/v2)
    if 'use_score_v2' in locals() and use_score_v2 and calcular_score_acumulado_v2 is not None:
        # group_map ticker->SEGMENTO (a página já está filtrada por segmento, mas isso preserva consistência do v2)
        try:
            _tmp = setores[['ticker', 'SEGMENTO']].copy()
            _tmp['ticker'] = _tmp['ticker'].astype(str).apply(_strip_sa)
            _tmp['SEGMENTO'] = _tmp['SEGMENTO'].fillna('OUTROS').astype(str)
            group_map = dict(zip(_tmp['ticker'], _tmp['SEGMENTO']))
        except Exception:
            group_map = {e.ticker: segmento for e in empresas}

        score = calcular_score_acumulado_v2(
            lista_empresas=payload,
            group_map=group_map,
            subsetor_map=subsetor_map,
            setor_map=setor_map,
            pesos_utilizados=pesos,
            anos_minimos=4,
            prefer_group_col="SEGMENTO",
            min_n_group=7,
        )

    else:
        score = calcular_score_acumulado(payload, setores_empresa, pesos, dados_macro, anos_minimos=4)

    if score is None or score.empty:
        st.warning("Score vazio: não há dados suficientes após os filtros e janela mínima.")
        return

    # ─────────────────────────────────────────────────────────
    # 3) Preços + penalização de platô + dividendos
    # ─────────────────────────────────────────────────────────
    tickers_scores = sorted(score["ticker"].dropna().astype(str).unique().tolist())
    tickers_yf = [_norm_sa(tk) for tk in tickers_scores]

    precos = baixar_precos(tickers_yf)
    if precos is None or precos.empty:
        st.warning("Não foi possível baixar preços para o segmento selecionado.")
        return

    precos.index = pd.to_datetime(precos.index, errors="coerce")
    precos = precos.dropna(how="all")
    if precos.empty:
        st.warning("Preços vieram vazios após normalização.")
        return

    # mensal para penalização de platô
    precos_mensal = precos.resample("M").last()
    score = penalizar_plato(score, precos_mensal, meses=12, penal=0.30)

    dividendos = coletar_dividendos(tickers_yf)

    # ─────────────────────────────────────────────────────────
    # 4) Liderança + backtest estratégia + backtest todas
    # ─────────────────────────────────────────────────────────
    lideres = determinar_lideres(score)
    if lideres is None or lideres.empty:
        st.warning("Não foi possível determinar líderes com o score calculado.")
        return

    patrimonio_estrategia, datas_aportes = gerir_carteira(precos, score, lideres, dividendos)
    if patrimonio_estrategia is None or patrimonio_estrategia.empty:
        st.warning("Falha ao simular a carteira da estratégia.")
        return
    patrimonio_estrategia = patrimonio_estrategia[["Patrimônio"]]

    patrimonio_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aportes)
    if patrimonio_selic is None or patrimonio_selic.empty:
        st.warning("Falha ao calcular o benchmark Tesouro Selic.")
        return

    patrimonio_empresas = gerir_carteira_todas_empresas(precos, tickers_scores, datas_aportes, dividendos)
    if patrimonio_empresas is None or patrimonio_empresas.empty:
        st.warning("Falha ao simular a carteira (todas as empresas).")
        return

    patrimonio_final = pd.concat([patrimonio_estrategia, patrimonio_empresas, patrimonio_selic], axis=1).sort_index()
    patrimonio_final = patrimonio_final.apply(pd.to_numeric, errors="coerce").ffill()

    st.markdown("## Evolução do patrimônio (Estratégia vs Empresas vs Selic)")

    fig, ax = plt.subplots(figsize=(12, 6))
    if "Patrimônio" in patrimonio_final.columns:
        ax.plot(patrimonio_final.index, patrimonio_final["Patrimônio"], label="Estratégia (Líderes)")

    if "Tesouro Selic" in patrimonio_final.columns:
        ax.plot(patrimonio_final.index, patrimonio_final["Tesouro Selic"], label="Tesouro Selic")

    cols_emp = [c for c in patrimonio_empresas.columns if c in patrimonio_final.columns]
    if cols_emp:
        media_emp = patrimonio_final[cols_emp].mean(axis=1, skipna=True)
        ax.plot(patrimonio_final.index, media_emp, label="Média (Empresas do segmento)")

    ax.set_xlabel("Data")
    ax.set_ylabel("Patrimônio (R$)")
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.4)
    st.pyplot(fig)

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 5) Cards de patrimônio final por ativo (inclui Estratégia e Selic)
    # ─────────────────────────────────────────────────────────
    st.markdown("## Patrimônio final por ativo (todas as empresas)")

    ultimo = patrimonio_final.iloc[-1:].T.reset_index()
    ultimo.columns = ["Ativo", "Patrimônio_Final"]
    ultimo = ultimo.sort_values("Patrimônio_Final", ascending=False)

    cols = st.columns(3)
    for i, row in enumerate(ultimo.to_dict("records")):
        ativo = str(row["Ativo"])
        val = float(row["Patrimônio_Final"]) if pd.notna(row["Patrimônio_Final"]) else 0.0

        with cols[i % 3]:
            st.markdown(
                f"""
                <div style="border:2px solid #ddd;border-radius:10px;padding:12px;margin:8px;background:#ffffff;text-align:center;box-sizing:border-box;width:100%;">
                    <div style="font-weight:700;color:#333;">{ativo}</div>
                    <div style="font-size:18px;color:#111;margin-top:6px;">{formatar_real(val)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 6) Tabela: líderes por ano
    # ─────────────────────────────────────────────────────────
    st.markdown("## Líderes por ano (Score_Ajustado)")

    if "Ano" in lideres.columns:
        lideres_view = lideres.copy()
        lideres_view["ticker"] = lideres_view["ticker"].astype(str)
        lideres_view = lideres_view.sort_values("Ano")
        st.dataframe(lideres_view, use_container_width=True)
    else:
        st.dataframe(lideres, use_container_width=True)

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 7) Histórico do score (tabela)
    # ─────────────────────────────────────────────────────────
    st.markdown("## Histórico do score")

    score_view = score.copy()
    if "Score_Ajustado" in score_view.columns:
        score_view["Score_Ajustado"] = pd.to_numeric(score_view["Score_Ajustado"], errors="coerce")
    score_view = score_view.sort_values(["Ano", "Score_Ajustado"], ascending=[True, False])

    st.dataframe(score_view, use_container_width=True)

    # ─────────────────────────────────────────────────────────
    # 8) Gráfico: Score ao longo do tempo
    # ─────────────────────────────────────────────────────────
    if {"Ano", "ticker", "Score_Ajustado"}.issubset(score.columns):
        st.markdown("## Evolução do Score_Ajustado (top 10 tickers por ano)")

        tmp = score[["Ano", "ticker", "Score_Ajustado"]].copy()
        tmp["Score_Ajustado"] = pd.to_numeric(tmp["Score_Ajustado"], errors="coerce")

        # top 10 por ano
        tmp = tmp.sort_values(["Ano", "Score_Ajustado"], ascending=[True, False])
        tmp = tmp.groupby("Ano", as_index=False).head(10)

        fig2 = px.line(tmp, x="Ano", y="Score_Ajustado", color="ticker", markers=True)
        st.plotly_chart(fig2, use_container_width=True)
