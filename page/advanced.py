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
from core.ticker_utils import normalize_ticker, add_sa_suffix
from core.ui_bridge import (
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
# >>> PATCH SCORE V2 (import opcional)
try:
    from core.scoring_v2 import calcular_score_acumulado_v2
except Exception:
    calcular_score_acumulado_v2 = None
# <<< PATCH SCORE V2
from core.portfolio import (
    gerir_carteira,
    gerir_carteira_modulada,
    gerir_carteira_todas_empresas,
    gerir_carteira_equal_weight_segmento,
    calcular_patrimonio_selic_macro,
)
from core.weights import get_pesos

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Utilitários internos
# ─────────────────────────────────────────────────────────────

def _norm_sa(ticker: str) -> str:
    return add_sa_suffix(ticker)


def _strip_sa(ticker: str) -> str:
    return normalize_ticker(ticker)


def _clean_df_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.astype(str).str.strip().str.replace("\ufeff", "", regex=False)
    return out


def _count_years_from_dre(dre: Optional[pd.DataFrame]) -> int:
    if dre is None or dre.empty:
        return 0

    dre = _clean_df_cols(dre)

    col_data = None
    if "Data" in dre.columns:
        col_data = "Data"
    elif "data" in dre.columns:
        col_data = "data"

    if col_data is None:
        return 0

    y = pd.to_datetime(dre[col_data], errors="coerce").dt.year
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
    if "data" in dre.columns and "Data" not in dre.columns:
        dre = dre.rename(columns={"data": "Data"})

    if "data" in mult.columns and "Data" not in mult.columns:
        mult = mult.rename(columns={"data": "Data"})

    # normaliza nome da coluna de data sem alterar layout geral da página
    if "data" in dre.columns and "Data" not in dre.columns:
        dre = dre.rename(columns={"data": "Data"})
    if "data" in mult.columns and "Data" not in mult.columns:
        mult = mult.rename(columns={"data": "Data"})

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
    if "data" in dm.columns and "Data" not in dm.columns:
        dm = dm.rename(columns={"data": "Data"})
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

    # validação mínima de schema (case-sensitive conforme Postgres com colunas entre aspas)
    needed = {"SETOR", "SUBSETOR", "SEGMENTO", "ticker"}
    if not needed.issubset(setores.columns):
        st.error(f"A tabela de setores não contém colunas esperadas: {sorted(needed)}")
        return

    # >>> PATCH SCORE V2 (mapas p/ fallback SEGMENTO -> SUBSETOR -> SETOR)
    _tmp = setores[["ticker", "SEGMENTO", "SUBSETOR", "SETOR"]].copy()
    _tmp["ticker"] = (
        _tmp["ticker"].astype(str)
        .str.upper()
        .str.replace(".SA", "", regex=False)
        .str.strip()
    )
    _tmp["SEGMENTO"] = _tmp["SEGMENTO"].fillna("OUTROS").astype(str)
    _tmp["SUBSETOR"] = _tmp["SUBSETOR"].fillna("OUTROS").astype(str)
    _tmp["SETOR"] = _tmp["SETOR"].fillna("OUTROS").astype(str)

    group_map = dict(zip(_tmp["ticker"], _tmp["SEGMENTO"]))
    subsetor_map = dict(zip(_tmp["ticker"], _tmp["SUBSETOR"]))
    setor_map = dict(zip(_tmp["ticker"], _tmp["SETOR"]))
    # <<< PATCH SCORE V2

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

        # >>> PATCH SCORE V2 (controle na sidebar, sem alterar layout existente)
        with st.expander("Scoring (opções)", expanded=False):
            if calcular_score_acumulado_v2 is None:
                st.caption("Score v2 indisponível (core/scoring_v2.py não encontrado).")
                use_score_v2 = False
            else:
                use_score_v2 = st.checkbox("Usar Score v2 (robusto)", value=True)
        # <<< PATCH SCORE V2
        # ── Carteira (modo)
        with st.expander("Carteira (modo)", expanded=False):
            st.markdown("**Modo automático (binário)**:")
            st.write("• Se **nº de empresas elegíveis** no segmento ≤ **4** → **Modelo Padrão (aportes iguais)**")
            st.write("• Se **nº de empresas elegíveis** no segmento ≥ **5** → **Ajuste Calibrado (auto-tuning)**")
            st.caption("Gate binário: usa o total de empresas do segmento após o filtro de histórico (tipo). Se n≤4 → padrão; se n≥5 → calibrado.")
            policy_calibrada: Dict = {"mode": "heuristica_calibrada", "eps": 0.35}

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
    n_total_segmento_raw = int(seg_df["ticker"].nunique())  # tamanho bruto do segmento (antes do filtro de histórico)
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
    n_total_segmento = int(seg_df["ticker"].nunique())  # OPÇÃO 2: tamanho do segmento condicionado ao filtro de histórico (tipo)
    if seg_df.empty:
        st.warning("Nenhuma empresa atende ao filtro de histórico escolhido.")
        return

    # ── exibição cards empresas
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

    # >>> PATCH SCORE V2 (switch v1/v2 sem alterar layout)
    if ("use_score_v2" in locals()) and use_score_v2 and (calcular_score_acumulado_v2 is not None):
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
    # <<< PATCH SCORE V2

    if score is None or score.empty:
        st.warning("Score vazio: não há dados suficientes após os filtros e janela mínima.")
        return

    # ─────────────────────────────────────────────────────────
    # 2.1) Decisão automática do modo (binário) por tamanho estrutural do segmento (OPÇÃO 2)
    # ─────────────────────────────────────────────────────────
    # Regra:
    #   - se n_total_segmento <= 4  -> Modelo Padrão (aportes iguais)
    #   - se n_total_segmento >= 5  -> Ajuste Calibrado
    # Observação: n_total_segmento é calculado ANTES dos filtros de elegibilidade do ano-ref.
    score = score.dropna(axis=1, how="all")
    n_empresas_elegiveis = int(score.shape[1])
    usar_calibrado = n_total_segmento >= 5

    if usar_calibrado:
        st.sidebar.success(
            f"Modo aplicado: **Ajuste Calibrado** (segmento com n={n_total_segmento}; elegíveis no ano-ref: {n_empresas_elegiveis})"
        )
    else:
        st.sidebar.info(
            f"Modo aplicado: **Modelo Padrão** (segmento com n={n_total_segmento}; elegíveis no ano-ref: {n_empresas_elegiveis})"
        )

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
    precos_mensal = precos.resample("ME").last()
    score = penalizar_plato(score, precos_mensal, meses=12, penal=0.30)

    dividendos = coletar_dividendos(tickers_yf)

    # ─────────────────────────────────────────────────────────
    # 4) Liderança + backtest estratégia + backtest todas
    # ─────────────────────────────────────────────────────────
    lideres = determinar_lideres(score)
    if lideres is None or lideres.empty:
        st.warning("Não foi possível determinar líderes com o score calculado.")
        return

    if usar_calibrado:
        patrimonio_estrategia, datas_aportes = gerir_carteira_modulada(
            precos, score, lideres, dividendos, policy=policy_calibrada
        )
    else:
        patrimonio_estrategia, datas_aportes = gerir_carteira(precos, score, lideres, dividendos)
    if patrimonio_estrategia is None or patrimonio_estrategia.empty:
        st.warning("Falha ao simular a carteira da estratégia.")
        return
    patrimonio_estrategia = patrimonio_estrategia[["Patrimônio"]]

    patrimonio_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aportes)
    if patrimonio_selic is None or patrimonio_selic.empty:
        st.warning("Falha ao calcular o benchmark Tesouro Selic.")
        return

    # Mantém a simulação individual por empresa para os cards comparativos abaixo.
    patrimonio_empresas = gerir_carteira_todas_empresas(precos, tickers_scores, datas_aportes, dividendos)
    if patrimonio_empresas is None or patrimonio_empresas.empty:
        st.warning("Falha ao simular a carteira (todas as empresas).")
        return

    # Benchmark correto: R$ 1.000/mês TOTAL para o segmento,
    # dividido igualmente entre todas as empresas elegíveis, sem usar ranking.
    benchmark_segmento = gerir_carteira_equal_weight_segmento(
        precos=precos,
        tickers=tickers_scores,
        datas_aportes=datas_aportes,
        dividendos_dict=dividendos,
    )
    if benchmark_segmento is None or benchmark_segmento.empty:
        st.warning("Falha ao simular o benchmark equal-weight do segmento.")
        return

    patrimonio_benchmark_segmento = benchmark_segmento.to_frame("Benchmark Equal-Weight Segmento")

    patrimonio_final = pd.concat(
        [patrimonio_estrategia, patrimonio_empresas, patrimonio_selic, patrimonio_benchmark_segmento],
        axis=1,
    ).sort_index()
    patrimonio_final = patrimonio_final.apply(pd.to_numeric, errors="coerce").ffill()

    st.markdown("## Evolução do patrimônio (Estratégia vs Benchmark do Segmento vs Selic)")

    fig, ax = plt.subplots(figsize=(12, 6))
    if "Patrimônio" in patrimonio_final.columns:
        ax.plot(patrimonio_final.index, patrimonio_final["Patrimônio"], label="Estratégia (Líderes)")

    if "Tesouro Selic" in patrimonio_final.columns:
        ax.plot(patrimonio_final.index, patrimonio_final["Tesouro Selic"], label="Tesouro Selic")

    if "Benchmark Equal-Weight Segmento" in patrimonio_final.columns:
        ax.plot(
            patrimonio_final.index,
            patrimonio_final["Benchmark Equal-Weight Segmento"],
            label="Carteira Equal-Weight (Segmento)",
        )

    ax.set_xlabel("Data")
    ax.set_ylabel("Patrimônio (R$)")
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.4)
    st.pyplot(fig)

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 5) Cards de patrimônio final por ativo (inclui Estratégia e Selic)
    # ─────────────────────────────────────────────────────────
    st.markdown("## Patrimônio final por ativo")

    last = patrimonio_final.iloc[-1].dropna()
    if last.empty:
        st.warning("Dados insuficientes para exibir patrimônio final.")
        return

    df_final = last.reset_index()
    df_final.columns = ["Ticker", "Valor Final"]
    df_final["Ticker"] = df_final["Ticker"].astype(str)
    df_final["Valor Final"] = pd.to_numeric(df_final["Valor Final"], errors="coerce")
    df_final = df_final.dropna(subset=["Valor Final"]).sort_values("Valor Final", ascending=False)

    # contagem de lideranças (mais coerente com “quantas vezes liderou”)
    contagem_lideres = lideres["ticker"].value_counts().to_dict()

    num_columns = 3
    cols_cards = st.columns(num_columns, gap="large")

    for i, (tk, val) in enumerate(df_final.itertuples(index=False, name=None)):
        tk = str(tk)
        try:
            val = float(val)
        except Exception:
            continue

        if tk == "Patrimônio":
            icone_url = "https://cdn-icons-png.flaticon.com/512/1019/1019709.png"
            border_color = "#DAA520"
            nome_exibicao = "Estratégia de Aporte"
            lider_texto = ""
        elif tk == "Tesouro Selic":
            icone_url = "https://cdn-icons-png.flaticon.com/512/2331/2331949.png"
            border_color = "#007bff"
            nome_exibicao = "Tesouro Selic"
            lider_texto = ""
        elif tk == "Benchmark Equal-Weight Segmento":
            icone_url = "https://cdn-icons-png.flaticon.com/512/3135/3135706.png"
            border_color = "#2ecc71"
            nome_exibicao = "Benchmark Equal-Weight Segmento"
            lider_texto = "R$ 1.000/mês dividido entre todas as empresas elegíveis"
        else:
            icone_url = get_logo_url(tk)
            border_color = "#d3d3d3"
            nome_exibicao = tk
            vezes_lider = int(contagem_lideres.get(tk, 0))
            lider_texto = f"🏆 {vezes_lider}x Líder" if vezes_lider > 0 else ""

        patrimonio_formatado = formatar_real(val)

        col = cols_cards[i % num_columns]
        with col:
            st.markdown(
                f"""
                <div style="
                    background-color:#ffffff;
                    border:3px solid {border_color};
                    border-radius:10px;
                    padding:15px;
                    margin:10px 0;
                    text-align:center;
                    box-shadow:2px 2px 5px rgba(0,0,0,0.10);
                    box-sizing:border-box;
                    width:100%;
                ">
                    <img src="{icone_url}" alt="{nome_exibicao}" style="width:50px;height:auto;margin-bottom:6px;">
                    <div style="margin:0;color:#4a4a4a;font-weight:800;font-size:18px;">{nome_exibicao}</div>
                    <div style="font-size:18px;margin:6px 0;font-weight:900;color:#2ecc71;">
                        {patrimonio_formatado}
                    </div>
                    <div style="font-size:14px;color:#FFA500;">{lider_texto}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 6) Comparação de múltiplos
    # ─────────────────────────────────────────────────────────
    st.markdown("### Comparação de Indicadores (Múltiplos) entre Empresas")

    indicadores_disponiveis = [
        # Rentabilidade
        "Margem Líquida", "Margem Operacional", "ROE", "ROA", "ROIC",
        # Valuation
        "P/L", "P/VP", "DY", "P/FCO", "EV/EBIT", "P/Receita",
        # Liquidez
        "Liquidez Corrente", "Liquidez Seca", "Liquidez Imediata",
        # Eficiência
        "Giro do Ativo", "Prazo Médio Recebimento",
        # Geração de caixa
        "Margem FCO", "FCO sobre Dívida", "Cobertura Investimento",
        # Endividamento
        "Alavancagem Financeira", "Endividamento Total",
    ]

    nomes_to_col = {
        "Margem Líquida":        "Margem_Liquida",
        "Margem Operacional":    "Margem_Operacional",
        "ROE":                   "ROE",
        "ROA":                   "ROA",
        "ROIC":                  "ROIC",
        "P/L":                   "P/L",
        "P/VP":                  "P/VP",
        "DY":                    "DY",
        "P/FCO":                 "P_FCO",
        "EV/EBIT":               "EV_EBIT",
        "P/Receita":             "P_Receita",
        "Liquidez Corrente":     "Liquidez_Corrente",
        "Liquidez Seca":         "Liquidez_Seca",
        "Liquidez Imediata":     "Liquidez_Imediata",
        "Giro do Ativo":         "Giro_Ativo",
        "Prazo Médio Recebimento":"Prazo_Medio_Recebimento",
        "Margem FCO":            "Margem_FCO",
        "FCO sobre Dívida":      "FCO_sobre_Divida",
        "Cobertura Investimento":"Cobertura_Investimento",
        "Alavancagem Financeira":"Alavancagem_Financeira",
        "Endividamento Total":   "Endividamento_Total",
    }

    # direção: True = maior é melhor (para colorir quadro comparativo)
    _indicador_melhor_alto = {
        "Margem Líquida": True, "Margem Operacional": True,
        "ROE": True, "ROA": True, "ROIC": True,
        "P/L": False, "P/VP": False, "DY": True,
        "P/FCO": False, "EV/EBIT": False, "P/Receita": False,
        "Liquidez Corrente": True, "Liquidez Seca": True, "Liquidez Imediata": True,
        "Giro do Ativo": True, "Prazo Médio Recebimento": False,
        "Margem FCO": True, "FCO sobre Dívida": True, "Cobertura Investimento": True,
        "Alavancagem Financeira": False, "Endividamento Total": False,
    }

    lista_nomes = [e.nome for e in empresas]
    empresas_selecionadas = st.multiselect(
        "Selecione as empresas a exibir:",
        lista_nomes,
        default=lista_nomes,
    )

    indicador = st.selectbox("Selecione o indicador:", indicadores_disponiveis, index=0)
    col_db = nomes_to_col[indicador]

    long_rows: List[dict] = []
    for e in empresas:
        if e.nome not in empresas_selecionadas:
            continue
        dfm = e.mult.copy()
        if dfm is None or dfm.empty:
            continue
        if "Ano" not in dfm.columns and "Data" in dfm.columns:
            dfm["Ano"] = pd.to_datetime(dfm["Data"], errors="coerce").dt.year
        if "Ano" not in dfm.columns or col_db not in dfm.columns:
            continue

        tmp = dfm[["Ano", col_db]].copy()
        tmp["Ano"] = pd.to_numeric(tmp["Ano"], errors="coerce")
        tmp[col_db] = pd.to_numeric(tmp[col_db], errors="coerce")
        tmp = tmp.dropna(subset=["Ano", col_db])
        if tmp.empty:
            continue

        tmp = tmp.groupby("Ano", as_index=False)[col_db].mean()
        for _, rr in tmp.iterrows():
            long_rows.append({"Ano": int(rr["Ano"]), "Empresa": e.nome, "Valor": float(rr[col_db])})

    if long_rows:
        df_long = pd.DataFrame(long_rows).sort_values(["Ano", "Empresa"])
        fig = px.line(
            df_long,
            x="Ano",
            y="Valor",
            color="Empresa",
            markers=True,
            title=f"{indicador} — comparação por ano (média anual)",
        )
        fig.update_layout(xaxis=dict(type="category"))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Não há dados suficientes para o indicador selecionado nas empresas escolhidas.")

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 7) Comparação de Demonstrações Financeiras (DRE)
    # ─────────────────────────────────────────────────────────
    st.markdown("### Comparação de Demonstrações Financeiras entre Empresas")

    indicadores_dre = {
        "Receita Líquida":      "Receita_Liquida",
        "EBIT":                 "EBIT",
        "Lucro Líquido":        "Lucro_Liquido",
        "Lucro Antes IR":       "Lucro_Antes_IR",
        "Resultado Financeiro": "Resultado_Financeiro",
        "Patrimônio Líquido":   "Patrimonio_Liquido",
        "Ativo Total":          "Ativo_Total",
        "Dívida Total":         "Divida_Total",
        "Dívida Líquida":       "Divida_Liquida",
        "Dívida CP":            "Divida_CP",
        "Dívida LP":            "Divida_LP",
        "FCO (Caixa Operacional)": "Caixa_Liquido",
        "FCI (Investimento)":   "FCI",
        "FCF (Fluxo Livre)":    "FCF",
        "Caixa":                "Caixa",
        "Contas a Receber":     "Contas_Receber",
        "Estoques":             "Estoques",
    }

    indicador_display = st.selectbox("Selecione o item da DRE:", list(indicadores_dre.keys()), index=0)
    col_dre = indicadores_dre[indicador_display]

    long_dre: List[dict] = []
    for e in empresas:
        if e.nome not in empresas_selecionadas:
            continue
        dfd = e.dre.copy()
        if dfd is None or dfd.empty:
            continue
        if "Ano" not in dfd.columns and "Data" in dfd.columns:
            dfd["Ano"] = pd.to_datetime(dfd["Data"], errors="coerce").dt.year
        if "Ano" not in dfd.columns or col_dre not in dfd.columns:
            continue

        tmp = dfd[["Ano", col_dre]].copy()
        tmp["Ano"] = pd.to_numeric(tmp["Ano"], errors="coerce")
        tmp[col_dre] = pd.to_numeric(tmp[col_dre], errors="coerce")
        tmp = tmp.dropna(subset=["Ano", col_dre])
        if tmp.empty:
            continue

        tmp = tmp.groupby("Ano", as_index=False)[col_dre].sum()
        for _, rr in tmp.iterrows():
            long_dre.append({"Ano": int(rr["Ano"]), "Empresa": e.nome, "Valor": float(rr[col_dre])})

    if long_dre:
        df_dre_long = pd.DataFrame(long_dre).sort_values(["Ano", "Empresa"])
        fig = px.bar(
            df_dre_long,
            x="Ano",
            y="Valor",
            color="Empresa",
            barmode="group",
            title=f"{indicador_display} — comparação por ano",
        )
        fig.update_layout(xaxis=dict(type="category"))
        fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.25)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Não há dados suficientes para o indicador selecionado entre as empresas escolhidas.")

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 8) Quadro Comparativo — tabela colorida por quartil
    # ─────────────────────────────────────────────────────────
    st.markdown("### 📊 Quadro Comparativo (último ano disponível)")
    st.caption("Verde = melhor quartil no segmento · Vermelho = pior quartil · Cinza = sem dado")

    _cols_quadro = [
        ("Margem Líquida",     "Margem_Liquida",     True),
        ("Margem Operacional", "Margem_Operacional",  True),
        ("ROE",                "ROE",                 True),
        ("ROIC",               "ROIC",                True),
        ("Margem FCO",         "Margem_FCO",          True),
        ("FCO/Dívida",         "FCO_sobre_Divida",    True),
        ("DY",                 "DY",                  True),
        ("P/VP",               "P/VP",                False),
        ("P/L",                "P/L",                 False),
        ("Liq. Corrente",      "Liquidez_Corrente",   True),
        ("Liq. Seca",          "Liquidez_Seca",       True),
        ("Giro Ativo",         "Giro_Ativo",          True),
        ("Endividamento",      "Endividamento_Total",  False),
    ]

    quadro_rows: List[dict] = []
    for e in empresas:
        if e.nome not in empresas_selecionadas:
            continue
        dfm = e.mult.copy()
        if dfm is None or dfm.empty:
            continue
        if "Data" in dfm.columns:
            dfm["Data"] = pd.to_datetime(dfm["Data"], errors="coerce")
            dfm = dfm.dropna(subset=["Data"]).sort_values("Data")
            last_row = dfm.iloc[-1]
        else:
            last_row = dfm.iloc[-1]

        row = {"Empresa": e.nome}
        for lbl, col, _ in _cols_quadro:
            v = last_row.get(col) if col in dfm.columns else None
            try:
                row[lbl] = float(v) if v is not None and pd.notna(v) else np.nan
            except Exception:
                row[lbl] = np.nan
        quadro_rows.append(row)

    if quadro_rows:
        df_quadro = pd.DataFrame(quadro_rows).set_index("Empresa")

        def _color_quartile(col_series: pd.Series, melhor_alto: bool) -> List[str]:
            vals = pd.to_numeric(col_series, errors="coerce")
            colors = []
            for v in vals:
                if pd.isna(v):
                    colors.append("background-color: rgba(128,128,128,0.15); color: #888")
                    continue
                finite = vals.dropna()
                if finite.empty or finite.nunique() < 2:
                    colors.append("")
                    continue
                pct = float((finite < v).sum()) / len(finite)
                if melhor_alto:
                    if pct >= 0.75:
                        colors.append("background-color: rgba(34,197,94,0.25); color: #16a34a; font-weight:700")
                    elif pct <= 0.25:
                        colors.append("background-color: rgba(239,68,68,0.20); color: #dc2626; font-weight:700")
                    else:
                        colors.append("")
                else:
                    if pct <= 0.25:
                        colors.append("background-color: rgba(34,197,94,0.25); color: #16a34a; font-weight:700")
                    elif pct >= 0.75:
                        colors.append("background-color: rgba(239,68,68,0.20); color: #dc2626; font-weight:700")
                    else:
                        colors.append("")
            return colors

        melhor_alto_map = {lbl: ma for lbl, _, ma in _cols_quadro}

        def _style_df(df: pd.DataFrame):
            styled = pd.DataFrame("", index=df.index, columns=df.columns)
            for col in df.columns:
                ma = melhor_alto_map.get(col, True)
                styled[col] = _color_quartile(df[col], ma)
            return styled

        df_fmt = df_quadro.copy()
        for lbl, _, _ in _cols_quadro:
            if lbl in df_fmt.columns:
                df_fmt[lbl] = df_fmt[lbl].apply(
                    lambda v: f"{v:.1f}" if pd.notna(v) and np.isfinite(v) else "-"
                )

        st.dataframe(
            df_quadro.style.apply(_style_df, axis=None).format(
                {lbl: lambda v: f"{v:.1f}" if pd.notna(v) and np.isfinite(v) else "-"
                 for lbl, _, _ in _cols_quadro if lbl in df_quadro.columns}
            ),
            use_container_width=True,
        )
    else:
        st.info("Sem dados para o quadro comparativo.")

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 9) Scatter Plot — cruzamento de 2 indicadores
    # ─────────────────────────────────────────────────────────
    st.markdown("### 🔀 Cruzamento de Indicadores (Scatter)")
    st.caption("Identifica empresas com combinações raras: ex. ROE alto + P/VP baixo (valor com qualidade).")

    _ind_scatter = [n for n in indicadores_disponiveis if n not in ("Prazo Médio Recebimento",)]
    col_x_lbl, col_y_lbl = st.columns(2)
    with col_x_lbl:
        eixo_x = st.selectbox("Eixo X:", _ind_scatter, index=_ind_scatter.index("P/VP") if "P/VP" in _ind_scatter else 0, key="scatter_x")
    with col_y_lbl:
        eixo_y = st.selectbox("Eixo Y:", _ind_scatter, index=_ind_scatter.index("ROE") if "ROE" in _ind_scatter else 1, key="scatter_y")

    scatter_rows: List[dict] = []
    for e in empresas:
        if e.nome not in empresas_selecionadas:
            continue
        dfm = e.mult.copy()
        if dfm is None or dfm.empty:
            continue
        if "Data" in dfm.columns:
            dfm = dfm.dropna(subset=["Data"]).sort_values("Data")
            last_row = dfm.iloc[-1]
        else:
            last_row = dfm.iloc[-1]

        col_x = nomes_to_col.get(eixo_x)
        col_y = nomes_to_col.get(eixo_y)
        vx = float(last_row[col_x]) if col_x and col_x in dfm.columns and pd.notna(last_row.get(col_x)) else np.nan
        vy = float(last_row[col_y]) if col_y and col_y in dfm.columns and pd.notna(last_row.get(col_y)) else np.nan
        if np.isfinite(vx) and np.isfinite(vy):
            scatter_rows.append({"Empresa": e.nome, eixo_x: vx, eixo_y: vy})

    if len(scatter_rows) >= 2:
        df_sc = pd.DataFrame(scatter_rows)
        # winsorize para não deixar outliers extremos distorcer o gráfico
        for col in [eixo_x, eixo_y]:
            s = pd.to_numeric(df_sc[col], errors="coerce")
            lo, hi = s.quantile(0.05), s.quantile(0.95)
            if lo < hi:
                df_sc[col] = s.clip(lower=lo, upper=hi)

        fig = px.scatter(
            df_sc, x=eixo_x, y=eixo_y, text="Empresa",
            title=f"{eixo_x} × {eixo_y} — último valor disponível",
            color="Empresa",
        )
        fig.update_traces(textposition="top center", marker_size=12)
        fig.add_vline(x=float(df_sc[eixo_x].median()), line_dash="dash", line_color="rgba(255,255,255,0.3)", annotation_text="mediana X")
        fig.add_hline(y=float(df_sc[eixo_y].median()), line_dash="dash", line_color="rgba(255,255,255,0.3)", annotation_text="mediana Y")
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Linhas tracejadas = mediana do grupo. Empresas no quadrante superior/esquerdo (eixo Y alto + eixo X baixo) tendem a ser as mais atrativas quando Y = qualidade e X = preço.")
    else:
        st.info("Dados insuficientes para o scatter plot (mínimo 2 empresas com ambos os indicadores).")

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 10) Qualidade dos Resultados — FCO vs Lucro Líquido
    # ─────────────────────────────────────────────────────────
    st.markdown("### 🧾 Qualidade dos Resultados — FCO vs Lucro Líquido")
    st.caption("Razão FCO/Lucro > 1 indica que o caixa supera o lucro contábil — sinal de alta qualidade. Valores persistentemente < 1 podem indicar accruals elevados.")

    fco_rows: List[dict] = []
    for e in empresas:
        if e.nome not in empresas_selecionadas:
            continue
        dfd = e.dre.copy()
        if dfd is None or dfd.empty:
            continue
        if "Ano" not in dfd.columns and "Data" in dfd.columns:
            dfd["Ano"] = pd.to_datetime(dfd["Data"], errors="coerce").dt.year
        if "Ano" not in dfd.columns:
            continue
        cols_ok = [c for c in ["Caixa_Liquido", "Lucro_Liquido"] if c in dfd.columns]
        if len(cols_ok) < 2:
            continue

        tmp = dfd[["Ano", "Caixa_Liquido", "Lucro_Liquido"]].copy()
        tmp["Ano"] = pd.to_numeric(tmp["Ano"], errors="coerce")
        tmp["Caixa_Liquido"] = pd.to_numeric(tmp["Caixa_Liquido"], errors="coerce")
        tmp["Lucro_Liquido"] = pd.to_numeric(tmp["Lucro_Liquido"], errors="coerce")
        tmp = tmp.dropna(subset=["Ano", "Caixa_Liquido", "Lucro_Liquido"])
        tmp = tmp[tmp["Lucro_Liquido"].abs() > 1e3]  # evita divisão por quase-zero
        if tmp.empty:
            continue

        tmp = tmp.groupby("Ano", as_index=False)[["Caixa_Liquido", "Lucro_Liquido"]].sum()
        tmp["FCO/Lucro"] = tmp["Caixa_Liquido"] / tmp["Lucro_Liquido"]
        # clip extremos para legibilidade
        tmp["FCO/Lucro"] = tmp["FCO/Lucro"].clip(-5, 10)
        for _, rr in tmp.iterrows():
            if pd.notna(rr["FCO/Lucro"]) and np.isfinite(rr["FCO/Lucro"]):
                fco_rows.append({"Ano": int(rr["Ano"]), "Empresa": e.nome, "FCO/Lucro": float(rr["FCO/Lucro"])})

    if fco_rows:
        df_fco = pd.DataFrame(fco_rows).sort_values(["Ano", "Empresa"])
        fig = px.line(
            df_fco, x="Ano", y="FCO/Lucro", color="Empresa", markers=True,
            title="Razão FCO / Lucro Líquido por ano",
        )
        fig.add_hline(y=1.0, line_dash="dash", line_color="#22c55e", annotation_text="FCO = Lucro (ideal)")
        fig.add_hline(y=0.0, line_dash="dot",  line_color="rgba(255,255,255,0.3)")
        fig.update_layout(xaxis=dict(type="category"), margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Dados de FCO e Lucro insuficientes para as empresas selecionadas.")
