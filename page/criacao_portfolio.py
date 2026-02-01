# =========================
# page/criacao_portfolio.py  (COM persistência em sessão + Patch7 neutralizado)
# =========================
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore

from core.db_loader import (
    load_setores_from_db,
    load_data_from_db,
    load_multiplos_from_db,
    load_macro_summary,
)
from core.helpers import (
    obter_setor_da_empresa,
    determinar_lideres,
    get_logo_url,
)
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
    calcular_patrimonio_selic_macro,
    gerir_carteira,
    encontrar_proxima_data_valida,
    gerir_carteira_simples,
)
from core.yf_data import (
    baixar_precos,
    coletar_dividendos,
    baixar_precos_ano_corrente,
)

from page.portfolio_patches import (
    render_patch1_regua_conviccao,
    render_patch2_dominancia,
    render_patch3_stress_test,
    render_patch4_diversificacao,
    render_patch5_benchmark_segmento,
    render_patch6_ia_selecao_lideres,
    # render_patch7_validacao_evidencias,  # ❌ Patch 7 neutralizado
)

from core.weights import get_pesos

# >>> Persistência em sessão
from core.session_store import (
    RunStoreConfig,
    make_run_key,
    save_run,
    load_run,
    list_runs,
    last_run_key,
    last_run_label,
    clear_runs,
    set_force_render_saved,
    consume_force_render_saved,
)
# <<< Persistência em sessão

logger = logging.getLogger(__name__)

# Limite simples para evitar salvar preço gigante na sessão
MAX_PRECOS_COLS_SALVAR = 300


# ─────────────────────────────────────────────────────────────
# Helpers de tempo / label
# ─────────────────────────────────────────────────────────────
def _now_sp() -> datetime:
    if ZoneInfo is None:
        return datetime.now()
    try:
        return datetime.now(ZoneInfo("America/Sao_Paulo"))
    except Exception:
        return datetime.now()


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.astype(str).str.strip().str.replace("\ufeff", "", regex=False)
    return out


def _norm_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    return t if t.endswith(".SA") else f"{t}.SA"


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "")


def _safe_year_count_from_dre(dre: pd.DataFrame) -> int:
    if dre is None or dre.empty:
        return 0
    if "Data" not in dre.columns:
        return 0
    years = pd.to_datetime(dre["Data"], errors="coerce").dt.year
    return int(years.dropna().nunique())


def _build_macro() -> Optional[pd.DataFrame]:
    dados_macro = load_macro_summary()
    if dados_macro is None or dados_macro.empty:
        return None
    dados_macro = _clean_columns(dados_macro)
    if "Data" not in dados_macro.columns:
        return None
    dados_macro["Data"] = pd.to_datetime(dados_macro["Data"], errors="coerce")
    dados_macro = dados_macro.dropna(subset=["Data"]).sort_values("Data").reset_index(drop=True)
    return dados_macro


def _maybe_shrink_precos(precos_global: pd.DataFrame, tickers_finais: List[str]) -> pd.DataFrame:
    if not isinstance(precos_global, pd.DataFrame) or precos_global.empty:
        return pd.DataFrame()

    cols = [str(c) for c in precos_global.columns]
    if len(cols) <= MAX_PRECOS_COLS_SALVAR:
        return precos_global

    tset = set([_strip_sa(t) for t in (tickers_finais or []) if str(t).strip()])
    keep = [c for c in cols if _strip_sa(c) in tset]
    keep = list(dict.fromkeys(keep))
    return precos_global[keep].copy() if keep else pd.DataFrame()


def _minify_score_global(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    base_cols = ["ticker", "Ano", "SETOR", "SUBSETOR", "SEGMENTO"]
    candidates = base_cols + [
        "Score_Final", "score_final", "Score", "score", "score_total",
        "Alpha_vs_Segmento_pp", "alpha_vs_segmento_pp",
        "DY", "PVP", "ROE", "ROIC",
        "Margem_Liquida", "Margem_EBITDA", "Margem_Bruta",
    ]
    keep = [c for c in candidates if c in df.columns]
    if not keep:
        keep = [c for c in base_cols if c in df.columns]
    return df[keep].copy() if keep else pd.DataFrame()


def _minify_lideres_global(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    cols = ["ticker", "Ano", "SETOR", "SUBSETOR", "SEGMENTO"]
    keep = [c for c in cols if c in df.columns]
    return df[keep].copy() if keep else pd.DataFrame()


@dataclass(frozen=True)
class EmpresaCarregada:
    ticker: str     # sem .SA
    nome: str
    multiplos: pd.DataFrame
    dre: pd.DataFrame


def _carregar_empresa(row: dict) -> Optional[EmpresaCarregada]:
    try:
        tk = _strip_sa(str(row.get("ticker", "")))
        nome = str(row.get("nome_empresa", tk))
        if not tk:
            return None

        tk_sa = _norm_sa(tk)
        mult = load_multiplos_from_db(tk_sa)
        dre = load_data_from_db(tk_sa)

        if mult is None or dre is None or mult.empty or dre.empty:
            return None

        mult = _clean_columns(mult)
        dre = _clean_columns(dre)

        if "Data" in mult.columns and "Ano" not in mult.columns:
            mult["Ano"] = pd.to_datetime(mult["Data"], errors="coerce").dt.year
        if "Data" in dre.columns and "Ano" not in dre.columns:
            dre["Ano"] = pd.to_datetime(dre["Data"], errors="coerce").dt.year

        return EmpresaCarregada(ticker=tk, nome=nome, multiplos=mult, dre=dre)
    except Exception as e:
        logger.debug("Falha ao carregar empresa %s: %s", row.get("ticker"), e)
        return None


def _filtrar_tickers_com_min_anos(tickers: Sequence[str], min_anos: int = 10, max_workers: int = 12) -> List[str]:
    tickers = [_strip_sa(t) for t in tickers if (t or "").strip()]
    if not tickers:
        return []

    def _check(tk: str) -> Tuple[str, bool]:
        dre = load_data_from_db(_norm_sa(tk))
        return tk, (_safe_year_count_from_dre(dre) >= min_anos)

    ok: List[str] = []
    max_workers = min(max_workers, max(2, len(tickers)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_check, tk): tk for tk in tickers}
        for fut in as_completed(futs):
            tk, good = fut.result()
            if good:
                ok.append(tk)

    return sorted(set(ok))


def _render_bloco_final_portfolio(empresas_lideres_finais: List[dict]) -> None:
    if not empresas_lideres_finais:
        return

    st.markdown("## 📑 Empresas líderes para o próximo ano")
    colunas_lideres = st.columns(3)
    for idx, emp in enumerate(empresas_lideres_finais):
        col = colunas_lideres[idx % 3]
        col.markdown(
            f"""
            <div style='border: 2px solid #28a745; border-radius: 10px; padding: 12px; margin-bottom: 10px; background-color: #f0fff4; text-align: center;'>
                <img src="{emp.get('logo_url','')}" width="45" />
                <h5 style="margin: 5px 0 0;">{emp.get('nome','')}</h5>
                <p style="margin: 0; color: #666; font-size: 13px;">({emp.get('ticker','')})</p>
                <p style="font-size: 12px; color: #333;">Líder em {emp.get('ano_lider','')}<br>Para compra em {emp.get('ano_compra','')}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("## 📊 Distribuição setorial do portfólio sugerido")
    setores_portfolio = pd.Series([e.get("setor", "OUTROS") for e in empresas_lideres_finais]).value_counts()

    fig, ax = plt.subplots()
    ax.pie(
        setores_portfolio.values,
        labels=setores_portfolio.index,
        autopct="%1.1f%%",
        startangle=90,
        textprops={"fontsize": 10},
    )
    ax.axis("equal")
    st.pyplot(fig)


def _render_all_patches(
    *,
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[dict],
    precos_global: pd.DataFrame,
    contrib_globais,
    show_patch6: bool = True,
) -> Tuple[Optional[dict], Optional[dict]]:
    """
    Renderiza patches 1..6. Patch 7 neutralizado para diagnóstico de reinício.
    Retorna (resp_patch6, None).
    """
    if not empresas_lideres_finais:
        st.info("Sem líderes finais para análise de patches nesta execução.")
        return None, None

    render_patch1_regua_conviccao(score_global, lideres_global, empresas_lideres_finais)
    render_patch2_dominancia(score_global, lideres_global, empresas_lideres_finais)
    render_patch3_stress_test(score_global, lideres_global, empresas_lideres_finais)
    render_patch4_diversificacao(empresas_lideres_finais, contrib_globais=contrib_globais)

    precos_para_patch5 = _maybe_shrink_precos(precos_global, [e.get("ticker", "") for e in empresas_lideres_finais])
    render_patch5_benchmark_segmento(
        score_global,
        empresas_lideres_finais,
        precos=precos_para_patch5,
        max_universe=80,
    )

    patch6_resp = None
    if show_patch6:
        st.markdown("<hr>", unsafe_allow_html=True)
        patch6_resp = render_patch6_ia_selecao_lideres(
            score_global=score_global,
            lideres_global=lideres_global,
            empresas_lideres_finais=empresas_lideres_finais,
            max_recs_default=10,
        )

    st.markdown("<hr>", unsafe_allow_html=True)
    st.info("Patch 7 neutralizado temporariamente para diagnóstico do reinício (criação_portfolio.py).")

    return patch6_resp, None


def render():
    st.markdown("<h1 style='text-align: center;'>Criação de Portfólio</h1>", unsafe_allow_html=True)

    store_cfg = RunStoreConfig(namespace="portfolio")
    default_margem = st.session_state.get("portfolio_last_margem_input", "")

    with st.sidebar:
        margem_input = st.text_input(
            "% acima do Tesouro Selic para destacar (obrigatório):",
            value=default_margem,
            key="portfolio_margem_input",
        )

        with st.expander("Scoring (opções)", expanded=False):
            if calcular_score_acumulado_v2 is None:
                st.caption("Score v2 indisponível (core/scoring_v2.py não encontrado).")
                use_score_v2 = False
            else:
                use_score_v2 = st.checkbox("Usar Score v2 (robusto)", value=True, key="portfolio_use_score_v2")

        gerar = st.button("Gerar Portfólio", key="portfolio_btn_gerar")

    with st.expander("Resultados salvos nesta sessão", expanded=True):
        runs = list_runs(store_cfg)
        lk_label = last_run_label(store_cfg)

        if runs:
            st.caption(f"Execuções salvas: {len(runs)}")
            if lk_label:
                st.write(f"Última execução: {lk_label}")

            c1, c2 = st.columns(2)
            with c1:
                if st.button("Reexibir último resultado (sem recalcular)", key="portfolio_show_saved"):
                    set_force_render_saved(store_cfg, True)
                    st.rerun()
            with c2:
                if st.button("Limpar resultados salvos", key="portfolio_clear_saved"):
                    clear_runs(store_cfg)
                    st.success("Resultados salvos removidos desta sessão.")
        else:
            st.caption("Nenhum resultado salvo ainda. Gere um portfólio para salvar.")

    if not margem_input.strip():
        st.warning("Digite uma porcentagem no campo lateral e clique em 'Gerar Portfólio'.")
        return

    st.session_state["portfolio_last_margem_input"] = margem_input.strip()

    try:
        margem_superior = float(margem_input.strip())
    except ValueError:
        st.error("Porcentagem inválida. Digite apenas números.")
        return

    setores_df = st.session_state.get("setores_df")
    if setores_df is None or getattr(setores_df, "empty", True):
        setores_df = load_setores_from_db()
        if setores_df is None or setores_df.empty:
            st.error("Não foi possível carregar a base de setores do banco.")
            st.stop()
        setores_df = _clean_columns(setores_df)
        st.session_state["setores_df"] = setores_df

    required_cols = {"SETOR", "SUBSETOR", "SEGMENTO", "ticker"}
    if not required_cols.issubset(setores_df.columns):
        st.error(f"Base de setores não contém as colunas esperadas: {sorted(required_cols)}")
        st.stop()

    dados_macro = _build_macro()
    if dados_macro is None or dados_macro.empty:
        st.error("Não foi possível carregar/normalizar os dados macroeconômicos.")
        st.stop()

    # MODO SALVO (sem recalcular)
    if consume_force_render_saved(store_cfg):
        rk = make_run_key(
            store_cfg,
            params={"margem_superior": float(margem_superior), "use_score_v2": bool(use_score_v2)},
            setores_df=setores_df,
            macro_df=dados_macro,
        )

        saved = load_run(store_cfg, rk)
        if saved is None:
            saved = load_run(store_cfg, last_run_key(store_cfg))

        if saved:
            st.info("Reexibindo resultados salvos (sem recalcular).")

            empresas_lideres_finais = saved.get("empresas_lideres_finais", [])
            score_global = saved.get("score_global", pd.DataFrame())
            lideres_global = saved.get("lideres_global", pd.DataFrame())
            precos_global = saved.get("precos_global", pd.DataFrame())
            contrib_globais = saved.get("contrib_globais", None)

            patch6_saved = saved.get("ia_recomendacoes", None)

            _render_bloco_final_portfolio(empresas_lideres_finais)

            st.markdown("<hr>", unsafe_allow_html=True)
            if empresas_lideres_finais:
                _render_all_patches(
                    score_global=score_global,
                    lideres_global=lideres_global,
                    empresas_lideres_finais=empresas_lideres_finais,
                    precos_global=precos_global,
                    contrib_globais=contrib_globais,
                    show_patch6=False,
                )

                st.markdown("<hr>", unsafe_allow_html=True)
                st.markdown("## 🤖 Patch 6 — IA (salvo)")
                if patch6_saved:
                    st.json(patch6_saved)
                else:
                    st.caption("Nenhuma recomendação de IA foi salva nesta execução.")
            else:
                st.info("Resultado salvo não contém líderes finais.")

            st.stop()

        st.warning("Não encontrei um resultado salvo compatível para reexibir.")
        st.stop()

    # MODO INTERATIVO (padrão)
    if not gerar:
        rk = st.session_state.get("portfolio_last_run_key") or last_run_key(store_cfg)
        saved = load_run(store_cfg, rk) if rk else None

        if saved:
            st.info("Reexibindo último resultado salvo (modo interativo).")

            empresas_lideres_finais = saved.get("empresas_lideres_finais", [])
            score_global = saved.get("score_global", pd.DataFrame())
            lideres_global = saved.get("lideres_global", pd.DataFrame())
            precos_global = saved.get("precos_global", pd.DataFrame())
            contrib_globais = saved.get("contrib_globais", None)

            _render_bloco_final_portfolio(empresas_lideres_finais)

            st.markdown("<hr>", unsafe_allow_html=True)
            if empresas_lideres_finais:
                _render_all_patches(
                    score_global=score_global,
                    lideres_global=lideres_global,
                    empresas_lideres_finais=empresas_lideres_finais,
                    precos_global=precos_global,
                    contrib_globais=contrib_globais,
                    show_patch6=True,
                )
            else:
                st.info("Resultado salvo não contém líderes finais.")
        else:
            st.info("Nenhum resultado salvo ainda. Clique em **Gerar Portfólio** para criar um.")
        st.stop()

    # EXECUÇÃO NOVA
    _tmp = setores_df[["ticker", "SEGMENTO", "SUBSETOR", "SETOR"]].copy()
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

    setores_unicos = (
        setores_df[["SETOR", "SUBSETOR", "SEGMENTO"]]
        .drop_duplicates()
        .sort_values(["SETOR", "SUBSETOR", "SEGMENTO"])
    )

    empresas_lideres_finais: List[dict] = []
    score_global_parts: List[pd.DataFrame] = []
    lideres_global_parts: List[pd.DataFrame] = []
    precos_global: pd.DataFrame = pd.DataFrame()
    contrib_globais = None

    for _, seg in setores_unicos.iterrows():
        setor = str(seg["SETOR"])
        subsetor = str(seg["SUBSETOR"])
        segmento = str(seg["SEGMENTO"])

        empresas_segmento = setores_df[
            (setores_df["SETOR"] == setor)
            & (setores_df["SUBSETOR"] == subsetor)
            & (setores_df["SEGMENTO"] == segmento)
        ].copy()

        tickers_segmento = [_strip_sa(t) for t in empresas_segmento["ticker"].astype(str).tolist()]
        tickers_segmento = [t for t in tickers_segmento if t]
        if len(set(tickers_segmento)) <= 1:
            continue

        tickers_validos = _filtrar_tickers_com_min_anos(tickers_segmento, min_anos=10, max_workers=12)
        if len(tickers_validos) <= 1:
            continue

        tickers_validos_set = set(tickers_validos)
        empresas_validas = empresas_segmento[
            empresas_segmento["ticker"].astype(str).apply(lambda x: _strip_sa(x) in tickers_validos_set)
        ]
        if empresas_validas.empty or len(empresas_validas) <= 1:
            continue

        lista_empresas: List[EmpresaCarregada] = []
        rows = empresas_validas.to_dict("records")

        with ThreadPoolExecutor(max_workers=12) as ex:
            futs = [ex.submit(_carregar_empresa, r) for r in rows]
            for fut in as_completed(futs):
                item = fut.result()
                if item is not None:
                    lista_empresas.append(item)

        if len(lista_empresas) <= 1:
            continue

        setores_empresa = {e.ticker: obter_setor_da_empresa(e.ticker, setores_df) for e in lista_empresas}
        pesos = get_pesos(setor)

        payload_empresas = [{"ticker": e.ticker, "nome": e.nome, "multiplos": e.multiplos, "dre": e.dre} for e in lista_empresas]

        if ("use_score_v2" in locals()) and use_score_v2 and (calcular_score_acumulado_v2 is not None):
            score = calcular_score_acumulado_v2(
                lista_empresas=payload_empresas,
                group_map=group_map,
                subsetor_map=subsetor_map,
                setor_map=setor_map,
                pesos_utilizados=pesos,
                anos_minimos=4,
                prefer_group_col="SEGMENTO",
                min_n_group=7,
            )
        else:
            score = calcular_score_acumulado(payload_empresas, setores_empresa, pesos, dados_macro, anos_minimos=4)

        if score is None or score.empty:
            continue

        if "ticker" in score.columns:
            score["ticker"] = score["ticker"].astype(str).apply(_strip_sa)
        score["SETOR"] = setor
        score["SUBSETOR"] = subsetor
        score["SEGMENTO"] = segmento

        try:
            precos = baixar_precos([_norm_sa(e.ticker) for e in lista_empresas])
            if precos is None or precos.empty:
                continue

            precos.index = pd.to_datetime(precos.index, errors="coerce")
            precos = precos.dropna(how="all")
            if precos.empty:
                continue

            precos_seg = precos.copy()
            precos_seg.index = pd.to_datetime(precos_seg.index, errors="coerce")
            precos_seg = precos_seg.dropna(how="all")
            precos_seg.columns = [_strip_sa(str(c)) for c in precos_seg.columns.astype(str).tolist()]

            if precos_global is None or precos_global.empty:
                precos_global = precos_seg
            else:
                precos_global = precos_global.join(precos_seg, how="outer", rsuffix="_dup")
                dup_cols = [c for c in precos_global.columns if str(c).endswith("_dup")]
                if dup_cols:
                    precos_global = precos_global.drop(columns=dup_cols, errors="ignore")

            precos_mensal = precos.resample("M").last()
            score = penalizar_plato(score, precos_mensal, meses=12, penal=0.30)
        except Exception:
            continue

        if score.empty:
            continue

        tickers_score = [str(t) for t in score["ticker"].dropna().unique().tolist()]
        tickers_score_yf = [_norm_sa(t) for t in tickers_score]
        dividendos = coletar_dividendos(tickers_score_yf)

        lideres = determinar_lideres(score)
        if lideres is None or lideres.empty:
            continue

        lideres2 = lideres.copy()
        if "ticker" in lideres2.columns:
            lideres2["ticker"] = lideres2["ticker"].astype(str).apply(_strip_sa)
        lideres2["SETOR"] = setor
        lideres2["SUBSETOR"] = subsetor
        lideres2["SEGMENTO"] = segmento

        score_global_parts.append(score.copy())
        lideres_global_parts.append(lideres2)

        patrimonio_empresas, datas_aportes = gerir_carteira(precos, score, lideres, dividendos)
        if patrimonio_empresas is None or patrimonio_empresas.empty:
            continue

        patrimonio_empresas = patrimonio_empresas.apply(pd.to_numeric, errors="coerce")
        final_empresas = float(patrimonio_empresas.iloc[-1].drop("Patrimônio", errors="ignore").sum())

        patrimonio_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aportes)
        if patrimonio_selic is None or patrimonio_selic.empty:
            continue

        final_selic = float(patrimonio_selic.iloc[-1]["Tesouro Selic"])
        if final_selic <= 0:
            continue

        diff = ((final_empresas / final_selic) - 1) * 100.0
        if diff < margem_superior:
            continue

        st.markdown(f"### {setor} > {subsetor} > {segmento}")
        st.markdown(f"**Valor final da estratégia:** R$ {final_empresas:,.2f} ({diff:.1f}% acima do Tesouro Selic)")

        ultimo_ano = int(pd.to_numeric(score["Ano"], errors="coerce").max())
        lideres_ano_anterior = lideres[lideres["Ano"] == ultimo_ano]
        for _, row in lideres_ano_anterior.iterrows():
            tk = _strip_sa(str(row["ticker"]))
            empresas_lideres_finais.append(
                {
                    "ticker": tk,
                    "nome": tk,
                    "logo_url": get_logo_url(tk),
                    "ano_lider": int(row["Ano"]),
                    "ano_compra": int(row["Ano"]) + 1,
                    "setor": setor,
                }
            )

    # Consolida globais
    score_global = pd.concat(score_global_parts, ignore_index=True) if score_global_parts else pd.DataFrame()
    lideres_global = pd.concat(lideres_global_parts, ignore_index=True) if lideres_global_parts else pd.DataFrame()

    if isinstance(precos_global, pd.DataFrame) and not precos_global.empty:
        precos_global.index = pd.to_datetime(precos_global.index, errors="coerce")
        precos_global = precos_global.dropna(how="all").sort_index()
    else:
        precos_global = pd.DataFrame()

    _render_bloco_final_portfolio(empresas_lideres_finais)

    st.markdown("<hr>", unsafe_allow_html=True)
    patch6_resp, patch7_resp = (None, None)

    if empresas_lideres_finais:
        patch6_resp, patch7_resp = _render_all_patches(
            score_global=score_global,
            lideres_global=lideres_global,
            empresas_lideres_finais=empresas_lideres_finais,
            precos_global=precos_global,
            contrib_globais=contrib_globais,
            show_patch6=True,
        )

    # Salva execução (Patch7 ficará None)
    run_key = make_run_key(
        store_cfg,
        params={"margem_superior": float(margem_superior), "use_score_v2": bool(use_score_v2)},
        setores_df=setores_df,
        macro_df=dados_macro,
    )

    precos_salvar = _maybe_shrink_precos(precos_global, [e.get("ticker", "") for e in (empresas_lideres_finais or [])])
    score_salvar = _minify_score_global(score_global)
    lideres_salvar = _minify_lideres_global(lideres_global)

    try:
        save_run(
            store_cfg,
            run_key,
            payload={
                "_meta": {
                    "created_at": _now_sp().isoformat(timespec="seconds"),
                    "margem_superior": margem_superior,
                    "use_score_v2": bool(use_score_v2),
                    "precos_cols_salvos": int(precos_salvar.shape[1]) if isinstance(precos_salvar, pd.DataFrame) else 0,
                },
                "margem_superior": margem_superior,
                "use_score_v2": bool(use_score_v2),
                "empresas_lideres_finais": empresas_lideres_finais,
                "score_global": score_salvar,
                "lideres_global": lideres_salvar,
                "precos_global": precos_salvar,
                "contrib_globais": contrib_globais,
                "ia_recomendacoes": patch6_resp,
                "patch7_evidencias": None,
            },
        )
    except Exception as e:
        st.error(f"Falha ao salvar execução em sessão: {e}")
        st.stop()

    st.session_state["portfolio_last_run_key"] = run_key
    st.success("Portfólio gerado e salvo (Patch 7 neutralizado). Reexibindo em modo interativo…")
    st.rerun()
