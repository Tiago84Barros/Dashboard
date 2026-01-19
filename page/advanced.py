from __future__ import annotations

import logging
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Set

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

# Score v2 (opcional)
try:
    from core.scoring_v2 import calcular_score_acumulado_v2
except Exception:
    calcular_score_acumulado_v2 = None

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
# Painel decisório: Segmento vs Líderes (sem z-score na UI)
# ─────────────────────────────────────────────────────────────

# Indicadores onde "menor é melhor" (heurística econômica)
_LOWER_IS_BETTER = {
    "P/L", "P/VP", "P_EBIT", "P_EBITDA",
    "Endividamento_Total", "Alavancagem_Financeira",
    "Divida_Liquida", "Divida_Bruta", "Divida_Liquida_EBITDA",
    "DL_EBITDA", "DividaLiquidaEBITDA",
}

# Colunas que não são indicadores
_RESERVED_COLS = {
    "Ano", "Data", "Ticker", "ticker", "Empresa", "nome", "Nome",
    "CNPJ", "SETOR", "SUBSETOR", "SEGMENTO", "LISTAGEM",
}


def _ensure_ano(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if "Ano" not in out.columns and "Data" in out.columns:
        out["Ano"] = pd.to_datetime(out["Data"], errors="coerce").dt.year
    return out


def _numeric_indicators_from_df(df: pd.DataFrame) -> List[str]:
    """Retorna todas as colunas numéricas com dados suficientes (>=3 valores)."""
    if df is None or df.empty:
        return []
    cols: List[str] = []
    for c in df.columns:
        if c in _RESERVED_COLS:
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        if int(s.notna().sum()) >= 3:
            cols.append(c)
    return cols


def _collect_available_indicators(empresas: List["EmpresaDados"], source: str) -> List[str]:
    """source: 'mult' ou 'dre'"""
    found: Set[str] = set()
    for e in empresas:
        df = getattr(e, source, None)
        if df is None or df.empty:
            continue
        df = _ensure_ano(df)
        for col in _numeric_indicators_from_df(df):
            found.add(col)
    return sorted(found)


def _build_long_df(empresas: List["EmpresaDados"], source: str, col: str) -> pd.DataFrame:
    """
    Constrói DF longo: Ano, Ticker, Valor.
    mult => média anual; dre => soma anual
    """
    rows: List[dict] = []
    for e in empresas:
        df = getattr(e, source, None)
        if df is None or df.empty:
            continue
        df = _ensure_ano(df)
        if "Ano" not in df.columns or col not in df.columns:
            continue

        tmp = df[["Ano", col]].copy()
        tmp["Ano"] = pd.to_numeric(tmp["Ano"], errors="coerce")
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce")
        tmp = tmp.dropna(subset=["Ano", col])
        if tmp.empty:
            continue

        if source == "mult":
            tmp = tmp.groupby("Ano", as_index=False)[col].mean()
        else:
            tmp = tmp.groupby("Ano", as_index=False)[col].sum()

        for _, r in tmp.iterrows():
            rows.append({"Ano": int(r["Ano"]), "Ticker": e.ticker, "Valor": float(r[col])})

    if not rows:
        return pd.DataFrame(columns=["Ano", "Ticker", "Valor"])
    return pd.DataFrame(rows)


def _get_ano_base(score: pd.DataFrame) -> Optional[int]:
    if score is None or score.empty:
        return None
    if "Ano" in score.columns:
        mx = pd.to_numeric(score["Ano"], errors="coerce").max()
        if pd.notna(mx):
            return int(mx)
    return None


def _top_tickers_from_score(score: pd.DataFrame, top_n: int, ano_base: Optional[int]) -> List[str]:
    df = score.copy()
    if "Score_Ajustado" not in df.columns:
        return sorted(df["ticker"].dropna().astype(str).unique().tolist())[:top_n]

    df["Score_Ajustado"] = pd.to_numeric(df["Score_Ajustado"], errors="coerce")
    if ano_base is not None and "Ano" in df.columns:
        df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce")
        df = df[df["Ano"] == ano_base]

    df = df.dropna(subset=["ticker", "Score_Ajustado"]).sort_values("Score_Ajustado", ascending=False)
    return df["ticker"].astype(str).head(int(top_n)).tolist()


def _segment_vs_leaders(df_long: pd.DataFrame, year: int, top_tickers: List[str], indicator: str) -> Tuple[float, pd.DataFrame]:
    """
    Retorna (mediana_segmento, leaders_df com gap% e status).
    """
    x = df_long.copy()
    x["Ano"] = pd.to_numeric(x["Ano"], errors="coerce")
    x["Valor"] = pd.to_numeric(x["Valor"], errors="coerce")
    x = x.dropna(subset=["Ano", "Ticker", "Valor"])
    x = x[x["Ano"] == year]

    if x.empty:
        return np.nan, pd.DataFrame(columns=["Ticker", "Valor", "Mediana_Segmento", "Gap_%", "Status"])

    med = float(x["Valor"].median())

    leaders = x[x["Ticker"].isin(top_tickers)].copy()
    if leaders.empty:
        return med, pd.DataFrame(columns=["Ticker", "Valor", "Mediana_Segmento", "Gap_%", "Status"])

    leaders = leaders.groupby("Ticker", as_index=False)["Valor"].mean()
    leaders["Mediana_Segmento"] = med

    if np.isfinite(med) and med != 0:
        leaders["Gap_%"] = (leaders["Valor"] / med - 1.0) * 100.0
    else:
        leaders["Gap_%"] = np.nan

    if indicator in _LOWER_IS_BETTER:
        leaders["Status"] = np.where(leaders["Gap_%"] < 0, "Melhor", "Pior")
        leaders = leaders.sort_values("Gap_%", ascending=True)  # mais negativo = melhor
    else:
        leaders["Status"] = np.where(leaders["Gap_%"] > 0, "Melhor", "Pior")
        leaders = leaders.sort_values("Gap_%", ascending=False)

    return med, leaders


def _render_segment_leaders_panel(
    empresas: List["EmpresaDados"],
    score: pd.DataFrame,
    default_source: str = "mult",
) -> None:
    st.markdown("## Segmento vs Líderes (painel decisório)")
    st.caption("Selecione qualquer indicador para comparar a mediana do segmento contra as empresas líderes (Top N por Score_Ajustado).")

    colA, colB, colC, colD = st.columns(4)
    with colA:
        top_n = st.slider("Top N líderes", 3, 25, 5)
    with colB:
        max_bars = st.slider("Máx. líderes no gráfico", 3, 12, 5)
    with colC:
        ref = st.selectbox("Referência do segmento", ["Mediana", "Média"], index=0)
    with colD:
        fonte = st.selectbox("Fonte", ["Múltiplos", "DRE"], index=(0 if default_source == "mult" else 1))

    source = "mult" if fonte == "Múltiplos" else "dre"

    ano_base = _get_ano_base(score)
    if ano_base is None:
        st.warning("Não foi possível determinar o ano-base do score. (Coluna 'Ano' ausente ou vazia.)")
        return

    top_tickers = _top_tickers_from_score(score, top_n=top_n, ano_base=ano_base)

    indicators = _collect_available_indicators(empresas, source)
    if not indicators:
        st.warning("Não foram encontrados indicadores numéricos nessa fonte para as empresas do segmento.")
        return

    indicador = st.selectbox("Indicador (todos disponíveis)", indicators, index=0)

    df_long = _build_long_df(empresas, source, indicador)
    if df_long.empty:
        st.warning("Sem dados suficientes para este indicador no segmento.")
        return

    # Segmento reference (median/mean)
    x_year = df_long[pd.to_numeric(df_long["Ano"], errors="coerce") == ano_base].copy()
    x_year["Valor"] = pd.to_numeric(x_year["Valor"], errors="coerce")
    x_year = x_year.dropna(subset=["Valor"])

    if x_year.empty:
        st.warning("Sem dados no ano-base para este indicador.")
        return

    if ref == "Média":
        seg_ref_value = float(x_year["Valor"].mean())
    else:
        seg_ref_value = float(x_year["Valor"].median())

    # líderes
    leaders = x_year[x_year["Ticker"].isin(top_tickers)].copy()
    if leaders.empty:
        st.warning("Não há dados do indicador no ano-base para os líderes selecionados.")
        return

    leaders = leaders.groupby("Ticker", as_index=False)["Valor"].mean()
    leaders["Ref_Segmento"] = seg_ref_value
    if np.isfinite(seg_ref_value) and seg_ref_value != 0:
        leaders["Gap_%"] = (leaders["Valor"] / seg_ref_value - 1.0) * 100.0
    else:
        leaders["Gap_%"] = np.nan

    if indicador in _LOWER_IS_BETTER:
        leaders["Status"] = np.where(leaders["Gap_%"] < 0, "Melhor", "Pior")
        leaders = leaders.sort_values("Gap_%", ascending=True)
    else:
        leaders["Status"] = np.where(leaders["Gap_%"] > 0, "Melhor", "Pior")
        leaders = leaders.sort_values("Gap_%", ascending=False)

    # Cards econômicos
    leader1 = leaders.iloc[0]
    c1, c2, c3 = st.columns(3)
    c1.metric(f"{ref} do segmento ({ano_base})", f"{seg_ref_value:,.2f}")
    c2.metric("Líder", f"{leader1['Ticker']} | {float(leader1['Valor']):,.2f}")
    gap = leader1["Gap_%"]
    c3.metric("Gap vs segmento", f"{float(gap):+.1f}%" if pd.notna(gap) else "—")

    # Gráfico simples: gap %
    plot_df = leaders.head(int(max_bars)).copy()
    fig = px.bar(plot_df, x="Gap_%", y="Ticker", orientation="h", title="Líderes vs referência do segmento (Gap %)")
    fig.update_layout(height=360, xaxis_title="Gap (%)", yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)

    # Tabela limpa
    st.dataframe(
        leaders[["Ticker", "Valor", "Ref_Segmento", "Gap_%", "Status"]].round({"Valor": 2, "Ref_Segmento": 2, "Gap_%": 1}),
        use_container_width=True
    )


# ─────────────────────────────────────────────────────────────
# Estrutura de dados por empresa
# ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class EmpresaDados:
    ticker: str  # sem .SA
    nome: str
    dre: pd.DataFrame
    mult: pd.DataFrame


def _load_empresa_dados(ticker: str, nome: str) -> Optional[EmpresaDados]:
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

    if "Data" in dre.columns and "Ano" not in dre.columns:
        dre["Ano"] = pd.to_datetime(dre["Data"], errors="coerce").dt.year
    if "Data" in mult.columns and "Ano" not in mult.columns:
        mult["Ano"] = pd.to_datetime(mult["Data"], errors="coerce").dt.year

    return EmpresaDados(ticker=tk, nome=nome, dre=dre, mult=mult)


# ─────────────────────────────────────────────────────────────
# Render
# ─────────────────────────────────────────────────────────────

def render() -> None:
    st.markdown("<h1 style='text-align:center'>Análise Avançada de Ações</h1>", unsafe_allow_html=True)

    # setores em sessão
    setores = st.session_state.get("setores_df")
    if setores is None or getattr(setores, "empty", True):
        setores = load_setores_from_db()
        if setores is None or setores.empty:
            st.error("Não foi possível carregar a base de setores do banco.")
            return
        setores = _clean_df_cols(setores)
        st.session_state["setores_df"] = setores

    needed = {"SETOR", "SUBSETOR", "SEGMENTO", "ticker"}
    if not needed.issubset(setores.columns):
        st.error(f"A tabela de setores não contém colunas esperadas: {sorted(needed)}")
        return

    # mapas p/ score v2 fallback SEGMENTO -> SUBSETOR -> SETOR
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

    dados_macro = _safe_macro()
    if dados_macro is None or dados_macro.empty:
        st.error("Não foi possível carregar os dados macroeconômicos (info_economica).")
        return

    # Sidebar filtros
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

        with st.expander("Scoring (opções)", expanded=False):
            if calcular_score_acumulado_v2 is None:
                st.caption("Score v2 indisponível (core/scoring_v2.py não encontrado).")
                use_score_v2 = False
            else:
                use_score_v2 = st.checkbox("Usar Score v2 (robusto)", value=True)

    # filtra tickers do segmento
    seg_df = setores[
        (setores["SETOR"] == setor) &
        (setores["SUBSETOR"] == subsetor) &
        (setores["SEGMENTO"] == segmento)
    ].copy()

    if seg_df.empty:
        st.warning("Nenhuma empresa encontrada para os filtros escolhidos.")
        return

    seg_df["ticker"] = seg_df["ticker"].astype(str).apply(_strip_sa)
    if "nome_empresa" not in seg_df.columns:
        seg_df["nome_empresa"] = seg_df["ticker"]

    seg_df = seg_df.dropna(subset=["ticker"])
    seg_df = seg_df[seg_df["ticker"].astype(str).str.len() > 0]

    if seg_df.empty:
        st.warning("Nenhuma empresa válida encontrada para os filtros escolhidos.")
        return

    # Diagnóstico colapsável
    with st.expander("Diagnóstico (dados do Supabase)", expanded=False):
        st.caption("Seção apenas informativa. Não altera resultados nem layout principal.")
        st.write(
            {
                "Setor": setor,
                "Subsetor": subsetor,
                "Segmento": segmento,
                "Empresas no segmento (bruto)": int(len(seg_df)),
                "Linhas setores_df": int(len(setores)),
            }
        )

    # (Pré) anos de histórico por ticker (rápido)
    years_map: Dict[str, int] = {}
    rows = seg_df[["ticker", "nome_empresa"]].drop_duplicates().to_dict("records")
    max_workers = min(12, max(2, len(rows)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(load_data_from_db, _norm_sa(r["ticker"])): r["ticker"] for r in rows}
        for fut in as_completed(futs):
            tk = futs[fut]
            try:
                dre = fut.result()
            except Exception:
                dre = None
            years_map[str(tk)] = _count_years_from_dre(dre)

    # Cards das empresas no filtro
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

    # 2) Carregar dados completos (múltiplos + DRE) em paralelo
    empresas: List[EmpresaDados] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs2 = [ex.submit(_load_empresa_dados, r["ticker"], r["nome_empresa"]) for r in rows]
        for fut in as_completed(futs2):
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

    pesos = get_pesos(setor)
    payload = [{"ticker": e.ticker, "nome": e.nome, "multiplos": e.mult, "dre": e.dre} for e in empresas]

    # Score (v1/v2)
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

    if score is None or score.empty:
        st.warning("Score vazio: não há dados suficientes após os filtros e janela mínima.")
        return

    # 3) Preços + penalização de platô + dividendos
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
    score = penalizar_plato(score, precos_mensal, meses=12, penal=0.30)

    dividendos = coletar_dividendos(tickers_yf)

    # 4) Liderança + backtest estratégia + backtest todas
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

    # 5) Cards de patrimônio final por ativo
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

    contagem_lideres = lideres["ticker"].value_counts().to_dict() if "ticker" in lideres.columns else {}

    num_columns = 3
    cols_cards = st.columns(num_columns)
    for i, row in enumerate(df_final.itertuples(index=False)):
        tk = str(row.Ticker)
        val = float(row._2)

        if tk == "Patrimônio":
            icone_url = "https://cdn-icons-png.flaticon.com/512/2331/2331949.png"
            border_color = "#2ecc71"
            nome_exibicao = "Estratégia (Líderes)"
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
    # 6) NOVO: Painel decisório — Segmento vs Líderes
    # ─────────────────────────────────────────────────────────
    _render_segment_leaders_panel(empresas=empresas, score=score, default_source="mult")


# Se seu router chama render() explicitamente, ok.
# Se ele espera variável render_page, não altere aqui.
