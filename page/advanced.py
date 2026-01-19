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

from core.portfolio import (
    gerir_carteira,
    gerir_carteira_todas_empresas,
    calcular_patrimonio_selic_macro,
)
from core.weights import get_pesos

# >>> DIAGNÓSTICO (módulo já criado por você)
from core.diagnostico_anomalias_simulacao import diagnosticar_anomalias_simulacao
# <<< DIAGNÓSTICO

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

# ─────────────────────────────────────────────────────────────
# Modo Decisório — utilitários (reduz carga visual)
# ─────────────────────────────────────────────────────────────

def _last_n_years_df(df: pd.DataFrame, n: int, year_col: str = "Ano") -> pd.DataFrame:
    if df is None or df.empty or year_col not in df.columns:
        return df
    mx = pd.to_numeric(df[year_col], errors="coerce").max()
    if not np.isfinite(mx):
        return df
    mx = int(mx)
    return df[pd.to_numeric(df[year_col], errors="coerce") >= (mx - int(n) + 1)].copy()


def _zscore_by_year(df: pd.DataFrame, value_col: str = "Valor", year_col: str = "Ano") -> pd.DataFrame:
    """
    Z-score por ano: compara empresa contra o "contexto" do mesmo ano.
    """
    out = df.copy()
    out[year_col] = pd.to_numeric(out[year_col], errors="coerce")
    out[value_col] = pd.to_numeric(out[value_col], errors="coerce")
    out = out.dropna(subset=[year_col, value_col])

    g = out.groupby(year_col)[value_col]
    mu = g.transform("mean")
    sd = g.transform("std").replace(0, np.nan)
    out["Z"] = ((out[value_col] - mu) / sd).fillna(0.0).clip(-4.0, 4.0)
    return out


def _summary_level_vs_vol(df: pd.DataFrame, entity_col: str = "Ticker", value_col: str = "Valor") -> pd.DataFrame:
    """
    Retorna Nível (média) e Consistência (volatilidade) no recorte já filtrado.
    """
    x = df.copy()
    x[value_col] = pd.to_numeric(x[value_col], errors="coerce")
    x = x.dropna(subset=[entity_col, value_col])
    agg = (x.groupby(entity_col)[value_col]
             .agg(media="mean", volatilidade="std", minimo="min", maximo="max", n="count")
             .reset_index())
    agg["cv"] = agg["volatilidade"] / agg["media"].replace(0, np.nan)
    return agg


def _diagnostics_simple(df: pd.DataFrame, indicator_name: str, entity_col="Ticker", year_col="Ano", value_col="Valor") -> List[Tuple[str, str]]:
    """
    Alertas simples e úteis para decisão (sem ML).
    """
    msgs: List[Tuple[str, str]] = []
    if df is None or df.empty:
        return msgs

    x = df.copy()
    x[year_col] = pd.to_numeric(x[year_col], errors="coerce")
    x[value_col] = pd.to_numeric(x[value_col], errors="coerce")
    x = x.dropna(subset=[entity_col, year_col, value_col]).sort_values([entity_col, year_col])

    if x.empty:
        return msgs

    med = x.groupby(year_col)[value_col].median().rename("mediana_ano")
    x = x.join(med, on=year_col)
    x["acima_mediana"] = x[value_col] >= x["mediana_ano"]

    # Volatilidade de referência do segmento (no recorte)
    seg_std = float(x[value_col].std(ddof=0)) if np.isfinite(x[value_col].std(ddof=0)) else 0.0

    for tk, g in x.groupby(entity_col):
        g = g.dropna(subset=[value_col])
        if g.empty:
            continue

        # (1) Abaixo da mediana por 3 anos seguidos
        streak = (~g["acima_mediana"]).astype(int).values
        run = 0
        max_run = 0
        for v in streak:
            run = run + 1 if v == 1 else 0
            max_run = max(max_run, run)
        if max_run >= 3:
            msgs.append((tk, f"⚠ {indicator_name}: abaixo da mediana por {max_run} anos seguidos."))

        # (2) Tendência recente (slope linear)
        if len(g) >= 5:
            y = g[value_col].values.astype(float)
            xx = np.arange(len(y))
            slope = float(np.polyfit(xx, y, 1)[0])
            if slope > 0:
                msgs.append((tk, f"✅ {indicator_name}: tendência de melhora no período recente."))
            elif slope < 0:
                msgs.append((tk, f"⚠ {indicator_name}: tendência de piora no período recente."))

        # (3) Volatilidade alta (outlier simples)
        if seg_std > 0 and float(g[value_col].std(ddof=0)) > (seg_std * 1.5):
            msgs.append((tk, f"⚠ {indicator_name}: volatilidade acima do padrão do segmento."))

    return msgs


def _plot_ranking_bar(df_rank: pd.DataFrame, x_col: str, y_col: str, title: str):
    fig = px.bar(df_rank, x=x_col, y=y_col, orientation="h", title=title)
    fig.update_layout(height=350, yaxis_title="", xaxis_title="")
    fig.update_traces(texttemplate="%{x:.2f}", textposition="outside", cliponaxis=False)
    return fig


def _plot_zscore_diverging(df_z_last: pd.DataFrame, title: str):
    df_plot = df_z_last.sort_values("Z")
    fig = px.bar(df_plot, x="Z", y="Ticker", orientation="h", title=title)
    fig.update_layout(height=450, yaxis_title="", xaxis_title="Z-score (vs segmento no ano)")
    return fig


def _plot_level_vs_consistency(df_lv: pd.DataFrame, title: str, use_cv: bool = False):
    y = "cv" if use_cv else "volatilidade"
    fig = px.scatter(
        df_lv, x="media", y=y, text="Ticker",
        hover_data=["media", "volatilidade", "cv", "minimo", "maximo", "n"],
        title=title
    )
    fig.update_traces(textposition="top center")
    fig.update_layout(height=520, xaxis_title="Nível (média)", yaxis_title="Risco (volatilidade)")
    return fig


def _plot_heatmap(df_z: pd.DataFrame, title: str):
    piv = df_z.pivot_table(index="Ticker", columns="Ano", values="Z", aggfunc="mean")
    fig = px.imshow(piv, aspect="auto", title=title)
    fig.update_layout(height=600, xaxis_title="Ano", yaxis_title="")
    return fig



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

        # >>> PATCH SCORE V2
        with st.expander("Scoring (opções)", expanded=False):
            if calcular_score_acumulado_v2 is None:
                st.caption("Score v2 indisponível (core/scoring_v2.py não encontrado).")
                use_score_v2 = False
            else:
                use_score_v2 = st.checkbox("Usar Score v2 (robusto)", value=True)
        # <<< PATCH SCORE V2

        # >>> DIAGNÓSTICO (parâmetros)
        with st.expander("Diagnóstico (anomalias da simulação)", expanded=False):
            diag_aporte_mensal = st.number_input(
                "Aporte mensal por ação (R$)",
                min_value=0.0,
                value=1000.0,
                step=100.0,
                format="%.2f",
            )
            diag_fee_bps = st.number_input(
                "Fee (bps)",
                min_value=0.0,
                value=0.0,
                step=1.0,
                format="%.2f",
            )
            diag_slip_bps = st.number_input(
                "Slippage (bps)",
                min_value=0.0,
                value=0.0,
                step=1.0,
                format="%.2f",
            )
            diag_min_preco = st.number_input(
                "Preço mínimo aceitável (R$)",
                min_value=0.0,
                value=0.10,
                step=0.05,
                format="%.2f",
            )
            diag_max_div = st.number_input(
                "Dividendo máximo por ação no mês (R$)",
                min_value=0.0,
                value=50.0,
                step=1.0,
                format="%.2f",
            )
            diag_max_mult = st.number_input(
                "Multiplicador máximo Patrimônio/Aportado",
                min_value=1.0,
                value=300.0,
                step=10.0,
                format="%.2f",
            )
        # <<< DIAGNÓSTICO

    # ── filtra tickers do segmento
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

    setores_empresa = {e.ticker: obter_setor_da_empresa(e.ticker, setores) for e in empresas}
    pesos = get_pesos(setor)
    payload = [{"ticker": e.ticker, "nome": e.nome, "multiplos": e.mult, "dre": e.dre} for e in empresas]

    # >>> SCORE v1/v2
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
    # <<< SCORE

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

    # ─────────────────────────────────────────────────────────
    # DIAGNÓSTICO (BOTÃO) — colocado aqui, com indentação correta
    # ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Diagnóstico de anomalias da simulação")

    st.caption(
        "Executa verificações para identificar distorções que normalmente geram números irreais "
        "(ex.: preço fora de escala, dividendo em unidade errada e explosões do multiplicador)."
    )

    # Normaliza colunas de preços e chaves de dividendos para o diagnóstico (sem alterar seus backtests)
    precos_diag = precos.copy()
    precos_diag.columns = [_strip_sa(c) for c in precos_diag.columns]

    dividendos_diag = {}
    if isinstance(dividendos, dict):
        for k, v in dividendos.items():
            dividendos_diag[_strip_sa(str(k))] = v

    if st.button("Executar diagnóstico de anomalias", key="btn_diag_anomalias"):
        with st.spinner("Executando diagnóstico..."):
            df_anomalias = diagnosticar_anomalias_simulacao(
                precos=precos_diag,
                tickers=tickers_scores,            # tickers sem .SA
                datas_aportes=datas_aportes,
                dividendos_dict=dividendos_diag,   # chaves sem .SA
                aporte_mensal=float(diag_aporte_mensal),
                fee_bps=float(diag_fee_bps),
                slippage_bps=float(diag_slip_bps),
                min_preco_aceitavel=float(diag_min_preco),
                max_div_por_acao_mes=float(diag_max_div),
                max_multiplicador_patrimonio=float(diag_max_mult),
            )

        if df_anomalias is None or df_anomalias.empty:
            st.success("Nenhuma anomalia detectada com os parâmetros atuais.")
        else:
            st.error(f"Foram detectadas {len(df_anomalias)} anomalias.")

            tk_opts = ["Todos"] + sorted(df_anomalias["ticker"].astype(str).unique().tolist())
            tk_sel = st.selectbox("Filtrar por ticker", tk_opts, index=0, key="diag_ticker_sel")

            df_view = df_anomalias.copy()
            if tk_sel != "Todos":
                df_view = df_view[df_view["ticker"].astype(str) == tk_sel]

            st.dataframe(df_view, use_container_width=True)

            st.caption(
                "Interpretação: "
                "flag_preco=True sugere preço fora de escala (split/ajuste/coluna errada); "
                "flag_div=True sugere unidade errada de dividendos (total vs por ação); "
                "flag_mult=True indica explosão do multiplicador e merece inspeção do mês exato."
            )

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 5) Cards de patrimônio final por ativo
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
    # 6) Modo Decisório — Comparação útil para decisão
    # ─────────────────────────────────────────────────────────
    st.markdown("## Comparação decisória (menos ruído, mais decisão)")

    col1, col2, col3 = st.columns(3)
    with col1:
        top_n = st.slider("Top N (líderes para foco)", 3, 10, 5)
    with col2:
        janela_anos = st.slider("Janela (anos)", 3, 10, 5)
    with col3:
        use_cv = st.checkbox("Volatilidade relativa (CV)", value=False)

    # Mapeia nome->ticker (evita legenda gigante e reduz confusão)
    nome_to_ticker = {e.nome: e.ticker for e in empresas}
    ticker_to_nome = {e.ticker: e.nome for e in empresas}

    # Controle de seleção: por padrão, focar no Top N do score (último ano)
    try:
        ano_score_max = int(pd.to_numeric(score["Ano"], errors="coerce").max())
        score_last = score[pd.to_numeric(score["Ano"], errors="coerce") == ano_score_max].copy()
        score_last["Score_Ajustado"] = pd.to_numeric(score_last["Score_Ajustado"], errors="coerce")
        score_last = score_last.dropna(subset=["ticker", "Score_Ajustado"]).sort_values("Score_Ajustado", ascending=False)
        tickers_focus_default = score_last["ticker"].head(top_n).astype(str).tolist()
    except Exception:
        tickers_focus_default = [e.ticker for e in empresas[:top_n]]

    # Seleção manual opcional (mas incentiva foco)
    nomes_default = [ticker_to_nome.get(tk, tk) for tk in tickers_focus_default]
    empresas_foco_nomes = st.multiselect(
        "Empresas em foco (recomendado: Top N do score)",
        options=[e.nome for e in empresas],
        default=nomes_default,
    )
    tickers_foco = [nome_to_ticker[nm] for nm in empresas_foco_nomes if nm in nome_to_ticker]

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 6A) Indicadores (Múltiplos) — decisório
    # ─────────────────────────────────────────────────────────
    st.markdown("### Indicadores (Múltiplos) — visão decisória")

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

    indicador = st.selectbox("Selecione o indicador (Múltiplos):", indicadores_disponiveis, index=0)
    col_db = nomes_to_col[indicador]

    # Monta DF longo apenas com empresas em foco (reduz ruído)
    long_rows: List[dict] = []
    for e in empresas:
        if tickers_foco and e.ticker not in tickers_foco:
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
            long_rows.append({"Ano": int(rr["Ano"]), "Ticker": e.ticker, "Valor": float(rr[col_db])})

    if long_rows:
        df_long = pd.DataFrame(long_rows).sort_values(["Ano", "Ticker"])
        df_long = _last_n_years_df(df_long, n=janela_anos, year_col="Ano")

        # (1) Ranking no recorte (média do Z-score)
        df_z = _zscore_by_year(df_long, value_col="Valor", year_col="Ano")
        rank = df_z.groupby("Ticker")["Z"].mean().rename("Z_medio").reset_index()
        top_rank = rank.sort_values("Z_medio", ascending=False).head(top_n)
        st.plotly_chart(
            _plot_ranking_bar(top_rank.sort_values("Z_medio"), "Z_medio", "Ticker",
                              f"Ranking (média do Z-score) — {indicador} — últimos {janela_anos} anos"),
            use_container_width=True
        )

        # (2) Distância da mediana (Z) no último ano
        last_year = int(df_z["Ano"].max())
        df_last = df_z[df_z["Ano"] == last_year][["Ticker", "Z"]].dropna()
        df_last = df_last.sort_values("Z", ascending=False).head(max(top_n * 2, 10))
        st.plotly_chart(
            _plot_zscore_diverging(df_last, f"Distância da mediana — {indicador} — {last_year} (Z-score)"),
            use_container_width=True
        )

        # (3) Nível x Consistência
        lv = _summary_level_vs_vol(df_long, entity_col="Ticker", value_col="Valor")
        lv_show = lv.sort_values("media", ascending=False).head(max(top_n * 3, 15))
        st.plotly_chart(
            _plot_level_vs_consistency(lv_show, f"Nível x Consistência — {indicador} — últimos {janela_anos} anos", use_cv=use_cv),
            use_container_width=True
        )

        # (4) Heatmap Top N
        tickers_heat = top_rank["Ticker"].astype(str).tolist()
        df_heat = df_z[df_z["Ticker"].isin(tickers_heat)]
        st.plotly_chart(
            _plot_heatmap(df_heat, f"Heatmap (Z-score) — Top {len(tickers_heat)} — {indicador}"),
            use_container_width=True
        )

        # (5) Diagnóstico
        st.markdown("#### Diagnóstico automático (indicador selecionado)")
        msgs = _diagnostics_simple(df_long, indicator_name=indicador, entity_col="Ticker", year_col="Ano", value_col="Valor")
        if not msgs:
            st.info("Nenhum alerta relevante no recorte selecionado.")
        else:
            # Prioriza empresas em foco
            foco_order = tickers_foco if tickers_foco else tickers_heat
            seen = set()
            for tk in foco_order + [t for t, _ in msgs if t not in foco_order]:
                if tk in seen:
                    continue
                seen.add(tk)
                t_msgs = [m for tt, m in msgs if tt == tk]
                if t_msgs:
                    with st.expander(f"{tk} — diagnósticos", expanded=(tk in foco_order[:max(1, top_n)])):
                        for m in t_msgs[:8]:
                            st.write(m)

    else:
        st.warning("Sem dados suficientes do indicador para as empresas em foco.")

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 6B) DRE — decisório (mesma lógica, sem gráfico denso)
    # ─────────────────────────────────────────────────────────
    st.markdown("### Demonstrações (DRE) — visão decisória")

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
        if tickers_foco and e.ticker not in tickers_foco:
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

        # DRE: soma anual
        tmp = tmp.groupby("Ano", as_index=False)[col_dre].sum()
        for _, rr in tmp.iterrows():
            long_dre.append({"Ano": int(rr["Ano"]), "Ticker": e.ticker, "Valor": float(rr[col_dre])})

    if long_dre:
        df_dre_long = pd.DataFrame(long_dre).sort_values(["Ano", "Ticker"])
        df_dre_long = _last_n_years_df(df_dre_long, n=janela_anos, year_col="Ano")

        dfz = _zscore_by_year(df_dre_long, value_col="Valor", year_col="Ano")
        rank = dfz.groupby("Ticker")["Z"].mean().rename("Z_medio").reset_index()
        top_rank = rank.sort_values("Z_medio", ascending=False).head(top_n)

        st.plotly_chart(
            _plot_ranking_bar(top_rank.sort_values("Z_medio"), "Z_medio", "Ticker",
                              f"Ranking (média do Z-score) — {indicador_display} — últimos {janela_anos} anos"),
            use_container_width=True
        )

        last_year = int(dfz["Ano"].max())
        df_last = dfz[dfz["Ano"] == last_year][["Ticker", "Z"]].dropna()
        df_last = df_last.sort_values("Z", ascending=False).head(max(top_n * 2, 10))
        st.plotly_chart(
            _plot_zscore_diverging(df_last, f"Distância da mediana — {indicador_display} — {last_year} (Z-score)"),
            use_container_width=True
        )

        lv = _summary_level_vs_vol(df_dre_long, entity_col="Ticker", value_col="Valor")
        lv_show = lv.sort_values("media", ascending=False).head(max(top_n * 3, 15))
        st.plotly_chart(
            _plot_level_vs_consistency(lv_show, f"Nível x Consistência — {indicador_display} — últimos {janela_anos} anos", use_cv=use_cv),
            use_container_width=True
        )

        tickers_heat = top_rank["Ticker"].astype(str).tolist()
        df_heat = dfz[dfz["Ticker"].isin(tickers_heat)]
        st.plotly_chart(
            _plot_heatmap(df_heat, f"Heatmap (Z-score) — Top {len(tickers_heat)} — {indicador_display}"),
            use_container_width=True
        )

        st.markdown("#### Diagnóstico automático (DRE selecionada)")
        msgs = _diagnostics_simple(df_dre_long, indicator_name=indicador_display, entity_col="Ticker", year_col="Ano", value_col="Valor")
        if not msgs:
            st.info("Nenhum alerta relevante no recorte selecionado.")
        else:
            foco_order = tickers_foco if tickers_foco else tickers_heat
            seen = set()
            for tk in foco_order + [t for t, _ in msgs if t not in foco_order]:
                if tk in seen:
                    continue
                seen.add(tk)
                t_msgs = [m for tt, m in msgs if tt == tk]
                if t_msgs:
                    with st.expander(f"{tk} — diagnósticos", expanded=(tk in foco_order[:max(1, top_n)])):
                        for m in t_msgs[:8]:
                            st.write(m)

    else:
        st.warning("Não há dados suficientes para o item da DRE selecionado nas empresas em foco.")

    st.markdown("---")

    # ─────────────────────────────────────────────────────────
    # 6C) Modo Exploratório (antigos gráficos densos) — opcional
    # ─────────────────────────────────────────────────────────
    with st.expander("Exploração (comparativos densos – opcional)", expanded=False):
        st.caption("Use apenas para auditoria/estudo. Para decisão, prefira os gráficos acima.")

        # Reabilita seleção ampla (como antes)
        lista_nomes = [e.nome for e in empresas]
        empresas_selecionadas = st.multiselect(
            "Selecione as empresas a exibir (exploração):",
            lista_nomes,
            default=lista_nomes,
            key="exploracao_empresas_multi",
        )

        # Gráfico antigo de múltiplos (linha)
        long_rows_exp: List[dict] = []
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
                long_rows_exp.append({"Ano": int(rr["Ano"]), "Empresa": e.nome, "Valor": float(rr[col_db])})

        if long_rows_exp:
            df_long_exp = pd.DataFrame(long_rows_exp).sort_values(["Ano", "Empresa"])
            fig = px.line(
                df_long_exp,
                x="Ano", y="Valor", color="Empresa", markers=True,
                title=f"[Exploração] {indicador} — comparação por ano (média anual)",
            )
            fig.update_layout(xaxis=dict(type="category"))
            st.plotly_chart(fig, use_container_width=True)

        # Gráfico antigo de DRE (barras)
        long_dre_exp: List[dict] = []
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
                long_dre_exp.append({"Ano": int(rr["Ano"]), "Empresa": e.nome, "Valor": float(rr[col_dre])})

        if long_dre_exp:
            df_dre_exp = pd.DataFrame(long_dre_exp).sort_values(["Ano", "Empresa"])
            fig = px.bar(
                df_dre_exp,
                x="Ano", y="Valor", color="Empresa", barmode="group",
                title=f"[Exploração] {indicador_display} — comparação por ano",
            )
            fig.update_layout(xaxis=dict(type="category"))
            st.plotly_chart(fig, use_container_width=True)
