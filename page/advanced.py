from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from core.db_loader import (
    load_data_from_db,
    load_macro_summary,
    load_multiplos_from_db,
    load_setores_from_db,
)
from core.helpers import (
    determinar_lideres,
    get_logo_url,
    obter_setor_da_empresa,
)
from core.portfolio import (
    calcular_patrimonio_selic_macro,
    encontrar_proxima_data_valida,
    gerir_carteira,
    gerir_carteira_simples,
)
from core.scoring import (
    calcular_score_acumulado,
    penalizar_plato,
)
from core.weights import get_pesos
from core.yf_data import (
    baixar_precos,
    baixar_precos_ano_corrente,
    coletar_dividendos,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Utilitários internos (preservando o comportamento do layout)
# ─────────────────────────────────────────────────────────────
def _clean_df_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.astype(str).str.strip().str.replace("\ufeff", "", regex=False)
    return out


def _norm_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    return t if t.endswith(".SA") else f"{t}.SA"


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "")


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

    # validação mínima de schema (case-sensitive conforme Postgres com colunas entre aspas)
    needed = {"SETOR", "SUBSETOR", "SEGMENTO", "ticker"}
    if not needed.issubset(setores.columns):
        st.error(f"A tabela de setores não contém colunas esperadas: {sorted(needed)}")
        return

    dados_macro = _safe_macro()
    if dados_macro is None or dados_macro.empty:
        st.error("Não foi possível carregar os dados macroeconômicos (info_economica).")
        return

    # ── Sidebar filtros (layout preservado)
    with st.sidebar:
        setor = st.selectbox("Setor:", sorted(setores["SETOR"].dropna().unique().tolist()))
        subsetores = setores.loc[setores["SETOR"] == setor, "SUBSETOR"].dropna().unique().tolist()
        subsetor = st.selectbox("Subsetor:", sorted(subsetores))
        segmentos = setores.loc[
            (setores["SETOR"] == setor) & (setores["SUBSETOR"] == subsetor),
            "SEGMENTO",
        ].dropna().unique().tolist()
        segmento = st.selectbox("Segmento:", sorted(segmentos))
        tipo = st.radio(
            "Perfil de empresa:",
            ["Crescimento (<10 anos)", "Estabelecida (≥10 anos)", "Todas"],
            index=2,
        )

    # ── filtra tickers do segmento
    seg_df = setores[
        (setores["SETOR"] == setor)
        & (setores["SUBSETOR"] == subsetor)
        & (setores["SEGMENTO"] == segmento)
    ].copy()

    if seg_df.empty:
        st.warning("Nenhuma empresa encontrada para os filtros escolhidos.")
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
        except Exception as e:
            logger.exception("Falha ao checar anos de DRE para %s: %s", tk, e)
            return tk, 0

    years_map: Dict[str, int] = {}
    max_workers = min(12, max(2, len(tickers)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_year_check, tk) for tk in tickers]
        for fut in as_completed(futs):
            tk, n = fut.result()
            years_map[tk] = int(n) if n is not None else 0

    def _pass_tipo(tk: str) -> bool:
        n = years_map.get(tk, 0)
        if tipo == "Crescimento (<10 anos)":
            return n < 10 and n > 0
        if tipo == "Estabelecida (≥10 anos)":
            return n >= 10
        return n > 0

    seg_df = seg_df[seg_df["ticker"].apply(_pass_tipo)]
    if seg_df.empty:
        st.warning("Nenhuma empresa atende ao filtro de histórico escolhido.")
        return

    # ── exibição cards empresas (layout preservado)
    st.markdown("### Empresas no filtro")
    cols = st.columns(3, gap="large")
    for i, row in enumerate(seg_df.itertuples(index=False)):
        c = cols[i % 3]
        tk = str(row.ticker)
        nm = str(getattr(row, "nome_empresa", tk))
        logo_url = get_logo_url(tk)
        anos_hist = years_map.get(tk, 0)
        c.markdown(
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

    precos_mensal = precos.resample("M").last()

    # penalidade de platô (mantém regra existente)
    score = penalizar_plato(score, precos_mensal)

    # dividendos (mantém regra existente)
    dividendos = coletar_dividendos(tickers_yf)

    # ─────────────────────────────────────────────────────────
    # 4) Líderes e visualizações
    # ─────────────────────────────────────────────────────────
    st.markdown("### Score por empresa (após penalizações)")
    st.dataframe(score, use_container_width=True)

    st.markdown("### Empresas líderes por ano (Score_Ajustado)")
    lideres = determinar_lideres(score, ["Score_Ajustado"])
    st.dataframe(lideres, use_container_width=True)

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 5) Carteiras / Backtests (layout preservado)
    # ─────────────────────────────────────────────────────────
    st.markdown("## Simulação de Carteira (Aportes Mensais)")

    # datas para simulação
    ano_ini = int(score["Ano"].min()) if "Ano" in score.columns and not score["Ano"].isna().all() else None
    ano_fim = int(score["Ano"].max()) if "Ano" in score.columns and not score["Ano"].isna().all() else None

    if ano_ini is None or ano_fim is None:
        st.warning("Não foi possível inferir intervalo de anos para simulação.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        aporte = st.number_input("Aporte mensal (R$):", min_value=0.0, value=500.0, step=50.0)
    with col2:
        ano_inicio = st.number_input("Ano início:", min_value=ano_ini, max_value=ano_fim, value=ano_ini, step=1)
    with col3:
        ano_fim_sel = st.number_input("Ano fim:", min_value=ano_ini, max_value=ano_fim, value=ano_fim, step=1)

    st.markdown("")

    # carteira simples (mantém)
    if st.button("Rodar simulação (carteira simples)"):
        try:
            df_perf, df_pos = gerir_carteira_simples(
                score_df=score,
                precos_diarios=precos,
                dividendos=dividendos,
                aporte_mensal=float(aporte),
                ano_inicio=int(ano_inicio),
                ano_fim=int(ano_fim_sel),
            )
            st.success("Simulação concluída.")
            st.markdown("### Evolução do patrimônio (carteira simples)")
            st.dataframe(df_perf, use_container_width=True)

            fig = plt.figure()
            plt.plot(pd.to_datetime(df_perf["Data"]), df_perf["Patrimonio"])
            plt.xticks(rotation=45)
            st.pyplot(fig)

            st.markdown("### Posições finais")
            st.dataframe(df_pos, use_container_width=True)
        except Exception as e:
            st.error(f"Erro ao simular carteira simples: {e}")

    st.markdown("---")

    # carteira com regras (mantém)
    if st.button("Rodar simulação (carteira com regras)"):
        try:
            df_perf, df_pos = gerir_carteira(
                score_df=score,
                precos_diarios=precos,
                dividendos=dividendos,
                aporte_mensal=float(aporte),
                ano_inicio=int(ano_inicio),
                ano_fim=int(ano_fim_sel),
            )
            st.success("Simulação concluída.")
            st.markdown("### Evolução do patrimônio (carteira com regras)")
            st.dataframe(df_perf, use_container_width=True)

            fig = plt.figure()
            plt.plot(pd.to_datetime(df_perf["Data"]), df_perf["Patrimonio"])
            plt.xticks(rotation=45)
            st.pyplot(fig)

            st.markdown("### Posições finais")
            st.dataframe(df_pos, use_container_width=True)
        except Exception as e:
            st.error(f"Erro ao simular carteira com regras: {e}")

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 6) Benchmark Selic (mantém)
    # ─────────────────────────────────────────────────────────
    st.markdown("## Benchmark: Tesouro Selic (macro)")

    if st.button("Calcular patrimônio no Tesouro Selic"):
        try:
            df_selic = calcular_patrimonio_selic_macro(
                dados_macro=dados_macro,
                aporte_mensal=float(aporte),
                ano_inicio=int(ano_inicio),
                ano_fim=int(ano_fim_sel),
            )
            st.success("Benchmark calculado.")
            st.dataframe(df_selic, use_container_width=True)

            fig = plt.figure()
            plt.plot(pd.to_datetime(df_selic["Data"]), df_selic["Patrimonio"])
            plt.xticks(rotation=45)
            st.pyplot(fig)
        except Exception as e:
            st.error(f"Erro ao calcular benchmark Selic: {e}")

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 7) Ano corrente (mantém)
    # ─────────────────────────────────────────────────────────
    st.markdown("## Ano Corrente (Preços)")

    if st.button("Baixar preços do ano corrente"):
        try:
            df_ano_corrente = baixar_precos_ano_corrente(tickers_yf)
            if df_ano_corrente is None or df_ano_corrente.empty:
                st.warning("Não foi possível obter preços do ano corrente.")
            else:
                st.dataframe(df_ano_corrente, use_container_width=True)
        except Exception as e:
            st.error(f"Erro ao baixar preços do ano corrente: {e}")
