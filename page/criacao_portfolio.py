# =========================
# page/criacao_portfolio.py  (SEM persistência) - versão para diagnóstico
# =========================
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Sequence, Tuple

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
    render_patch7_validacao_evidencias,
)

from core.weights import get_pesos

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Helpers de tempo / normalização
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
    return (ticker or "").strip().upper().replace(".SA", "").strip()


def _safe_year_count_from_dre(dre: pd.DataFrame) -> int:
    if dre is None or dre.empty:
        return 0
    if "Data" not in dre.columns:
        return 0
    years = pd.to_datetime(dre["Data"], errors="coerce").dt.year
    return int(years.dropna().nunique())


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


def _filtrar_tickers_com_min_anos(
    tickers: Sequence[str],
    min_anos: int = 10,
    max_workers: int = 12
) -> List[str]:
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


# ─────────────────────────────────────────────────────────────
# Render UI: cards + pizza
# ─────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────
# Render patches (sem persistência)
# ─────────────────────────────────────────────────────────────

def _render_all_patches(
    *,
    score_global: pd.DataFrame,
    lideres_global: pd.DataFrame,
    empresas_lideres_finais: List[dict],
    precos_patch5: pd.DataFrame,
    contrib_globais,
    show_patch6: bool = True,
    show_patch7: bool = True,
):
    if not empresas_lideres_finais:
        st.info("Sem líderes finais para análise de patches nesta execução.")
        return None, None

    render_patch1_regua_conviccao(score_global, lideres_global, empresas_lideres_finais)
    render_patch2_dominancia(score_global, lideres_global, empresas_lideres_finais)
    render_patch3_stress_test(score_global, lideres_global, empresas_lideres_finais)
    render_patch4_diversificacao(empresas_lideres_finais, contrib_globais=contrib_globais)

    render_patch5_benchmark_segmento(
        score_global,
        empresas_lideres_finais,
        precos=precos_patch5,
        max_universe=80,
    )

    patch6_resp = None
    patch7_resp = None

    if show_patch6:
        st.markdown("<hr>", unsafe_allow_html=True)
        patch6_resp = render_patch6_ia_selecao_lideres(
            score_global=score_global,
            lideres_global=lideres_global,
            empresas_lideres_finais=empresas_lideres_finais,
            max_recs_default=10,
        )

    if show_patch7:
        st.markdown("<hr>", unsafe_allow_html=True)
        patch7_resp = render_patch7_validacao_evidencias(
            score_global=score_global,
            lideres_global=lideres_global,
            empresas_lideres_finais=empresas_lideres_finais,
            days_default=60,
            max_items_per_ticker_default=10,
            cache_ttl_hours_default=12,
        )

    return patch6_resp, patch7_resp


# ─────────────────────────────────────────────────────────────
# Render Streamlit
# ─────────────────────────────────────────────────────────────

def render():
    st.markdown("<h1 style='text-align: center;'>Criação de Portfólio</h1>", unsafe_allow_html=True)

    # debug leve (se reiniciar, esse id muda)
    if "boot_id" not in st.session_state:
        st.session_state["boot_id"] = _now_sp().isoformat(timespec="seconds")
    st.caption(f"boot_id={st.session_state['boot_id']}")

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

        with st.expander("Diagnóstico", expanded=False):
            show_patch6 = st.checkbox("Renderizar Patch 6 (IA)", value=True, key="diag_show_patch6")
            show_patch7 = st.checkbox("Renderizar Patch 7 (Evidências)", value=True, key="diag_show_patch7")
            st.caption("Use para isolar se o reset vem do Patch 6/7.")

    if not margem_input.strip():
        st.warning("Digite uma porcentagem no campo lateral e clique em 'Gerar Portfólio'.")
        return

    st.session_state["portfolio_last_margem_input"] = margem_input.strip()

    try:
        margem_superior = float(margem_input.strip())
    except ValueError:
        st.error("Porcentagem inválida. Digite apenas números.")
        return

    # se não clicou, não roda nada (sem persistência)
    if not gerar:
        st.info("Clique em **Gerar Portfólio** para executar.")
        return

    # ── Carrega setores
    setores_df = load_setores_from_db()
    if setores_df is None or setores_df.empty:
        st.error("Não foi possível carregar a base de setores do banco.")
        st.stop()
    setores_df = _clean_columns(setores_df)

    required_cols = {"SETOR", "SUBSETOR", "SEGMENTO", "ticker"}
    if not required_cols.issubset(setores_df.columns):
        st.error(f"Base de setores não contém as colunas esperadas: {sorted(required_cols)}")
        st.stop()

    dados_macro = _build_macro()
    if dados_macro is None or dados_macro.empty:
        st.error("Não foi possível carregar/normalizar os dados macroeconômicos.")
        st.stop()

    # >>> PATCH SCORE V2 (mapas ticker -> SEGMENTO/SUBSETOR/SETOR)
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
    # <<< PATCH SCORE V2

    setores_unicos = (
        setores_df[["SETOR", "SUBSETOR", "SEGMENTO"]]
        .drop_duplicates()
        .sort_values(["SETOR", "SUBSETOR", "SEGMENTO"])
    )

    empresas_lideres_finais: List[dict] = []
    score_global_parts: List[pd.DataFrame] = []
    lideres_global_parts: List[pd.DataFrame] = []
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

        if use_score_v2 and (calcular_score_acumulado_v2 is not None):
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

        empresas_estrategia = patrimonio_empresas.columns.drop("Patrimônio", errors="ignore")
        colunas_empresas = st.columns(min(3, len(empresas_estrategia)))

        for idx, ticker_col in enumerate(empresas_estrategia):
            col = colunas_empresas[idx % len(colunas_empresas)]
            tk_clean = _strip_sa(str(ticker_col))
            logo_url = get_logo_url(tk_clean)

            nome = next((e.nome for e in lista_empresas if _strip_sa(e.ticker) == tk_clean), tk_clean)
            valor_final = float(patrimonio_empresas[ticker_col].iloc[-1])
            perc_part = (valor_final / final_empresas) * 100.0 if final_empresas != 0 else 0.0

            anos_lider = lideres[lideres["ticker"].astype(str).apply(_strip_sa) == tk_clean]["Ano"].tolist()
            anos_lider_str = f"{len(anos_lider)}x Líder: {', '.join(map(str, anos_lider))}" if anos_lider else ""

            col.markdown(
                f"""
                <div style='border: 1px solid #ccc; border-radius: 8px; padding: 10px; margin-bottom: 10px; text-align: center;'>
                    <img src='{logo_url}' width='40' />
                    <p style='margin: 5px 0 0; font-weight: bold;'>{nome}</p>
                    <p style='margin: 0; color: #666; font-size: 12px;'>({tk_clean})</p>
                    <p style='font-size: 12px; color: #999;'>{anos_lider_str}</p>
                    <p style='font-size: 12px; color: #2c3e50;'>Participação: {perc_part:.1f}%</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

        ultimo_ano = int(pd.to_numeric(score["Ano"], errors="coerce").max())
        lideres_ano_anterior = lideres[lideres["Ano"] == ultimo_ano]

        for _, row in lideres_ano_anterior.iterrows():
            tk = _strip_sa(str(row["ticker"]))
            empresas_lideres_finais.append(
                {
                    "ticker": tk,
                    "nome": next((e.nome for e in lista_empresas if _strip_sa(e.ticker) == tk), tk),
                    "logo_url": get_logo_url(tk),
                    "ano_lider": int(row["Ano"]),
                    "ano_compra": int(row["Ano"]) + 1,
                    "setor": setor,
                    "subsetor": subsetor,
                    "segmento": segmento,
                }
            )

    # ---- Ano corrente
    if empresas_lideres_finais:
        st.markdown("## 📊 Desempenho parcial das líderes (ano atual)")

        ano_corrente = _now_sp().year
        tickers_corrente = [e["ticker"] for e in empresas_lideres_finais if int(e["ano_compra"]) == ano_corrente]

        if tickers_corrente:
            tickers_corrente_yf = [_norm_sa(tk) for tk in tickers_corrente]
            precos_corrente = baixar_precos_ano_corrente(tickers_corrente_yf)
            if precos_corrente is None or precos_corrente.empty:
                st.warning("⚠️ Dados de preço indisponíveis para as ações escolhidas no ano atual.")
                st.stop()

            precos_corrente.index = pd.to_datetime(precos_corrente.index, errors="coerce")
            precos_corrente = precos_corrente.dropna(how="all")
            precos_corrente = precos_corrente.resample("B").last().ffill()
            if precos_corrente.empty:
                st.warning("⚠️ Dados de preço indisponíveis para as ações escolhidas no ano atual.")
                st.stop()

            tickers_limpos = [_strip_sa(tk) for tk in tickers_corrente_yf]
            dividendos_dict = coletar_dividendos(tickers_corrente_yf)

            datas_potenciais = pd.date_range(start=f"{ano_corrente}-01-01", end=f"{ano_corrente}-12-31", freq="MS")
            datas_aporte: List[pd.Timestamp] = []
            for data in datas_potenciais:
                data_valida = encontrar_proxima_data_valida(data, precos_corrente)
                if data_valida is not None and data_valida in precos_corrente.index:
                    datas_aporte.append(data_valida)

            patrimonio_aporte = gerir_carteira_simples(
                precos_corrente,
                tickers_limpos,
                datas_aporte,
                dividendos_dict=dividendos_dict,
            )

            df_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aporte)
            if df_selic is None or df_selic.empty:
                st.warning("⚠️ Não foi possível calcular o benchmark Selic para o período.")
                st.stop()

            df_selic = df_selic.reindex(patrimonio_aporte.index).ffill()
            df_final = pd.concat([patrimonio_aporte.rename("Estratégia de Aporte"), df_selic], axis=1).dropna()
            if df_final.empty or df_final["Tesouro Selic"].isna().all():
                st.warning("⚠️ Não foi possível construir gráfico com os dados disponíveis.")
                st.stop()

            st.markdown(f"### Comparativo de desempenho parcial em {ano_corrente}")
            fig, ax = plt.subplots(figsize=(10, 5))
            df_final["Estratégia de Aporte"].plot(ax=ax, label="Estratégia de Aporte")
            df_final["Tesouro Selic"].plot(ax=ax, label="Tesouro Selic")
            ax.set_ylabel("Valor acumulado (R$)")
            ax.set_xlabel("Data")
            ax.legend()
            ax.grid(True, linestyle="--", alpha=0.5)
            st.pyplot(fig)

    # Consolida globais para patches (nesta execução)
    score_global = pd.concat(score_global_parts, ignore_index=True) if score_global_parts else pd.DataFrame()
    lideres_global = pd.concat(lideres_global_parts, ignore_index=True) if lideres_global_parts else pd.DataFrame()

    _render_bloco_final_portfolio(empresas_lideres_finais)
    st.markdown("<hr>", unsafe_allow_html=True)

    # Para o Patch 5: por enquanto, usa apenas preços dos tickers finais (mínimo necessário)
    # (isso evita um universo grande e ajuda a testar estabilidade)
    tickers_finais = sorted({_strip_sa(str(e.get("ticker", ""))) for e in empresas_lideres_finais if str(e.get("ticker", "")).strip()})
    precos_patch5 = pd.DataFrame()
    if tickers_finais:
        precos_patch5 = baixar_precos([_norm_sa(t) for t in tickers_finais])
        if isinstance(precos_patch5, pd.DataFrame) and not precos_patch5.empty:
            precos_patch5.index = pd.to_datetime(precos_patch5.index, errors="coerce")
            precos_patch5 = precos_patch5.dropna(how="all")
            precos_patch5.columns = [_strip_sa(str(c)) for c in precos_patch5.columns.astype(str).tolist()]
            precos_patch5 = precos_patch5.loc[~precos_patch5.index.isna()].sort_index()

    if empresas_lideres_finais:
        _render_all_patches(
            score_global=score_global,
            lideres_global=lideres_global,
            empresas_lideres_finais=empresas_lideres_finais,
            precos_patch5=precos_patch5,
            contrib_globais=None,
            show_patch6=bool(show_patch6),
            show_patch7=bool(show_patch7),
        )

    st.success("Execução finalizada (sem persistência).")
