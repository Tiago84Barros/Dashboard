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

# >>> PATCH SCORE V2 (import opcional)
try:
    from core.scoring_v2 import calcular_score_acumulado_v2
except Exception:
    calcular_score_acumulado_v2 = None
# <<< PATCH SCORE V2

# >>> PATCH SCORE V3 (import opcional)
try:
    from core.scoring_v3 import calcular_score_acumulado_v3, ScoreV3Config
except Exception:
    calcular_score_acumulado_v3 = None
    ScoreV3Config = None  # type: ignore
# <<< PATCH SCORE V3

from core.portfolio import (
    gerir_carteira,
    gerir_carteira_modulada,
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

    # validação mínima de schema (case-sensitive conforme Postgres com colunas entre aspas)
    needed = {"SETOR", "SUBSETOR", "SEGMENTO", "ticker"}
    if not needed.issubset(setores.columns):
        st.error(f"A tabela de setores não contém colunas esperadas: {sorted(needed)}")
        return

    # >>> PATCH MAPAS (fallback SEGMENTO -> SUBSETOR -> SETOR)
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
    # <<< PATCH MAPAS

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

        # >>> PATCH SCORE V1/V2/V3 (controle comparativo)
        with st.expander("Scoring (opções)", expanded=False):
            opcoes = ["v1"]
            labels = {"v1": "Score v1 (legado)"}

            if calcular_score_acumulado_v2 is not None:
                opcoes.append("v2")
                labels["v2"] = "Score v2 (robusto)"

            if calcular_score_acumulado_v3 is not None:
                opcoes.append("v3")
                labels["v3"] = "Score v3 (robusto + tanh)"

            # padrão: v2 se existir; senão v1; se existir v3 e você quiser default v3, troque para "v3"
            default_mode = "v2" if "v2" in opcoes else "v1"
            scoring_mode = st.radio(
                "Versão do Score:",
                opcoes,
                index=opcoes.index(default_mode),
                format_func=lambda x: labels.get(x, x),
            )

            if scoring_mode == "v3" and ScoreV3Config is not None:
                st.caption("Ajustes v3 (opcionais):")
                tanh_c = st.slider("tanh_c (saturação)", min_value=0.8, max_value=6.0, value=2.0, step=0.1)
            else:
                tanh_c = 2.0
        # <<< PATCH SCORE V1/V2/V3

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
    n_total_segmento_raw = int(seg_df["ticker"].nunique())  # antes do filtro de histórico
    if "nome_empresa" not in seg_df.columns:
        seg_df["nome_empresa"] = seg_df["ticker"]

    seg_df = seg_df.dropna(subset=["ticker"])
    seg_df = seg_df[seg_df["ticker"].astype(str).str.len() > 0]

    if seg_df.empty:
        st.warning("Nenhuma empresa válida encontrada para os filtros escolhidos.")
        return

    # ─────────────────────────────────────────────────────────
    # (Opcional) Diagnóstico colapsável
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
    n_total_segmento = int(seg_df["ticker"].nunique())  # tamanho condicionado ao filtro de histórico (tipo)
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

    # payload scoring (compatível com scoring.py e v2/v3)
    payload = [{"ticker": e.ticker, "nome": e.nome, "multiplos": e.mult, "dre": e.dre} for e in empresas]

    # ─────────────────────────────────────────────────────────
    # 2.0) Calcular score (v1 / v2 / v3)
    # ─────────────────────────────────────────────────────────
    if ("scoring_mode" in locals()) and scoring_mode == "v3" and (calcular_score_acumulado_v3 is not None):
        cfg_v3 = ScoreV3Config(tanh_c=float(tanh_c)) if ScoreV3Config is not None else None
        score = calcular_score_acumulado_v3(
            lista_empresas=payload,
            group_map=group_map,
            subsetor_map=subsetor_map,
            setor_map=setor_map,
            pesos_utilizados=pesos,
            anos_minimos=4,
            prefer_group_col="SEGMENTO",
            min_n_group=7,
            config=cfg_v3,
        )
    elif ("scoring_mode" in locals()) and scoring_mode == "v2" and (calcular_score_acumulado_v2 is not None):
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
    # 2.1) Decisão automática do modo (binário) por tamanho estrutural do segmento
    # ─────────────────────────────────────────────────────────
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
        "Margem Líquida",
        "Margem Operacional",
        "ROE",
        "ROIC",
        "P/L",
        "P/VP",
        "DY",
        "Liquidez Corrente",
        "Alavancagem Financeira",
        "Endividamento Total",
    ]

    nomes_to_col = {
        "Margem Líquida": "Margem_Liquida",
        "Margem Operacional": "Margem_Operacional",
        "ROE": "ROE",
        "ROIC": "ROIC",
        "P/L": "P/L",
        "P/VP": "P/VP",
        "DY": "DY",
        "Liquidez Corrente": "Liquidez_Corrente",
        "Alavancagem Financeira": "Alavancagem_Financeira",
        "Endividamento Total": "Endividamento_Total",
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
        "Receita Líquida": "Receita_Liquida",
        "EBIT": "EBIT",
        "Lucro Líquido": "Lucro_Liquido",
        "Patrimônio Líquido": "Patrimonio_Liquido",
        "Dívida Líquida": "Divida_Liquida",
        "Caixa Líquido": "Caixa_Liquido",
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
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Não há dados suficientes para o indicador selecionado entre as empresas escolhidas.")
