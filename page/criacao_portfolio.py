from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Sequence, Tuple, Dict, Any

import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

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
from core.weights import get_pesos

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# PATCHES: importa portfolio_patches.py
# ─────────────────────────────────────────────────────────────
_PATCHES_OK = False
_PATCHES_IMPORT_ERR = None
try:
    # Ajuste se o seu arquivo estiver em outro caminho.
    from page.portfolio_patches import (
        render_patch1_regua_conviccao,
        render_patch2_dominancia,
        render_patch3_stress_test,
        render_patch4_diversificacao,
        render_patch5_benchmark_segmento,
        render_patch6_ia_selecao_lideres,
        render_patch7_validacao_evidencias,
    )
    _PATCHES_OK = True
except Exception as e1:
    try:
        from portfolio_patches import (
            render_patch1_regua_conviccao,
            render_patch2_dominancia,
            render_patch3_stress_test,
            render_patch4_diversificacao,
            render_patch5_benchmark_segmento,
            render_patch6_ia_selecao_lideres,
            render_patch7_validacao_evidencias,
        )
        _PATCHES_OK = True
    except Exception as e2:
        _PATCHES_OK = False
        _PATCHES_IMPORT_ERR = e2


# ─────────────────────────────────────────────────────────────
# Utilitários internos
# ─────────────────────────────────────────────────────────────

def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.astype(str).str.strip().str.replace("\ufeff", "", regex=False)
    return out


def _norm_sa(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    if not t:
        return ""
    return t if t.endswith(".SA") else f"{t}.SA"


def _strip_sa(ticker: str) -> str:
    return (ticker or "").strip().upper().replace(".SA", "").strip()


def _ensure_prices_df(precos: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Normaliza índice datetime, ordena e limpa colunas vazias; strip .SA nas colunas."""
    if precos is None or not isinstance(precos, pd.DataFrame) or precos.empty:
        return pd.DataFrame()

    df = precos.copy()
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()].sort_index()
    df.columns = [_strip_sa(str(c)) for c in df.columns.astype(str).tolist()]
    df = df.dropna(how="all", axis=0)
    df = df.dropna(how="all", axis=1)
    return df


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


def _maybe_merge_segment_cols(score_df: pd.DataFrame, *, setor: str, subsetor: str, segmento: str) -> pd.DataFrame:
    if score_df is None or score_df.empty:
        return pd.DataFrame()

    out = score_df.copy()
    if "ticker" in out.columns:
        out["ticker"] = out["ticker"].astype(str).map(_strip_sa)
    if "SETOR" not in out.columns:
        out["SETOR"] = str(setor)
    if "SUBSETOR" not in out.columns:
        out["SUBSETOR"] = str(subsetor)
    if "SEGMENTO" not in out.columns:
        out["SEGMENTO"] = str(segmento)

    out["SETOR"] = out["SETOR"].fillna("OUTROS").astype(str)
    out["SUBSETOR"] = out["SUBSETOR"].fillna("OUTROS").astype(str)
    out["SEGMENTO"] = out["SEGMENTO"].fillna("OUTROS").astype(str)
    return out


def _normalize_lideres(lideres_df: pd.DataFrame) -> pd.DataFrame:
    if lideres_df is None or lideres_df.empty:
        return pd.DataFrame()
    out = lideres_df.copy()
    if "ticker" in out.columns:
        out["ticker"] = out["ticker"].astype(str).map(_strip_sa)
    if "Ano" in out.columns:
        out["Ano"] = pd.to_numeric(out["Ano"], errors="coerce")
    return out.dropna(subset=["ticker", "Ano"])


def _state_key() -> str:
    return "criacao_portfolio_last_run"


def _save_run_state(payload: Dict[str, Any]) -> None:
    st.session_state[_state_key()] = payload


def _load_run_state() -> Optional[Dict[str, Any]]:
    obj = st.session_state.get(_state_key())
    if isinstance(obj, dict) and obj.get("ok") is True:
        return obj
    return None


def _clear_run_state() -> None:
    if _state_key() in st.session_state:
        del st.session_state[_state_key()]


# ─────────────────────────────────────────────────────────────
# Render Streamlit
# ─────────────────────────────────────────────────────────────

def render():
    st.markdown("<h1 style='text-align: center;'>Criação de Portfólio</h1>", unsafe_allow_html=True)

    # Sidebar controls
    with st.sidebar:
        margem_input = st.text_input("% acima do Tesouro Selic para destacar (obrigatório):", value=st.session_state.get("cp_margem_input", ""))

        # >>> PATCH SCORE V2
        with st.expander("Scoring (opções)", expanded=False):
            if calcular_score_acumulado_v2 is None:
                st.caption("Score v2 indisponível (core/scoring_v2.py não encontrado).")
                use_score_v2 = False
            else:
                use_score_v2 = st.checkbox("Usar Score v2 (robusto)", value=st.session_state.get("cp_use_score_v2", True))
        # <<< PATCH SCORE V2

        # Botões
        colb1, colb2 = st.columns(2)
        with colb1:
            gerar = st.button("Gerar Portfólio", use_container_width=True)
        with colb2:
            limpar = st.button("Limpar", use_container_width=True)

        st.caption("⚠️ Dica: ao clicar nos Patch 6/7, o Streamlit faz rerun. Este módulo mantém o último resultado em sessão.")

    if limpar:
        _clear_run_state()
        st.session_state["cp_margem_input"] = ""
        st.session_state["cp_use_score_v2"] = True
        st.rerun()

    # Persist inputs
    st.session_state["cp_margem_input"] = margem_input
    st.session_state["cp_use_score_v2"] = bool(use_score_v2)

    # Se NÃO clicou em gerar, mas já existe resultado salvo, reexibe (isso resolve o “reiniciar” do Patch 6)
    last = _load_run_state()
    if (not gerar) and last is not None:
        _render_from_state(last)
        return

    # Se não tem estado e não clicou em gerar: instrução
    if not gerar:
        st.info("Preencha a margem e clique em **Gerar Portfólio**. Depois você pode clicar nos patches (6/7) sem recalcular.")
        return

    # Valida margem
    if not margem_input.strip():
        st.warning("Digite uma porcentagem no campo lateral.")
        return

    try:
        margem_superior = float(margem_input.strip())
    except ValueError:
        st.error("Porcentagem inválida. Digite apenas números.")
        return

    # ─────────────────────────────────────────────────────────
    # Execução principal (gera e salva em sessão)
    # ─────────────────────────────────────────────────────────

    setores_df = load_setores_from_db()

    # defensivo: pode vir DF/Series/list
    if setores_df is None:
        st.error("Não foi possível carregar a base de setores do banco.")
        return
    if isinstance(setores_df, pd.Series):
        setores_df = setores_df.to_frame()
    if not isinstance(setores_df, pd.DataFrame):
        try:
            setores_df = pd.DataFrame(setores_df)
        except Exception:
            st.error("Base de setores retornou um formato inesperado (não é DataFrame).")
            return
    if setores_df.empty:
        st.error("Não foi possível carregar a base de setores do banco (vazia).")
        return

    setores_df = _clean_columns(setores_df)

    required_cols = {"SETOR", "SUBSETOR", "SEGMENTO", "ticker"}
    if not required_cols.issubset(setores_df.columns):
        st.error(f"Base de setores não contém as colunas esperadas: {sorted(required_cols)}")
        return

    # map ticker -> grupo
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

    dados_macro = _build_macro()
    if dados_macro is None or dados_macro.empty:
        st.error("Não foi possível carregar/normalizar os dados macroeconômicos.")
        return

    setores_unicos = (
        setores_df[["SETOR", "SUBSETOR", "SEGMENTO"]]
        .drop_duplicates()
        .sort_values(["SETOR", "SUBSETOR", "SEGMENTO"])
    )

    empresas_lideres_finais: List[dict] = []
    score_global_parts: List[pd.DataFrame] = []
    lideres_global_parts: List[pd.DataFrame] = []
    contrib_globais: List[Dict[str, Any]] = []

    # Loop por segmento
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

        # carrega dados completos em paralelo
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
        payload_empresas = [
            {"ticker": e.ticker, "nome": e.nome, "multiplos": e.multiplos, "dre": e.dre}
            for e in lista_empresas
        ]

        # score v2/v1
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

        score = _maybe_merge_segment_cols(score, setor=setor, subsetor=subsetor, segmento=segmento)

        # preços + penalização platô (mantém como seu principal)
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

        # Exibe segmento destacado
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

            contrib_globais.append({"ticker": tk_clean, "valor_final": valor_final})

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

        score_global_parts.append(score.copy())
        lideres_global_parts.append(_normalize_lideres(lideres.copy()))

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

    # Bloco final: líderes + gráfico setorial
    if empresas_lideres_finais:
        st.markdown("## 📑 Empresas líderes para o próximo ano")
        colunas_lideres = st.columns(3)
        for idx, emp in enumerate(empresas_lideres_finais):
            col = colunas_lideres[idx % 3]
            col.markdown(
                f"""
                <div style='border: 2px solid #28a745; border-radius: 10px; padding: 12px; margin-bottom: 10px; background-color: #f0fff4; text-align: center;'>
                    <img src="{emp['logo_url']}" width="45" />
                    <h5 style="margin: 5px 0 0;">{emp['nome']}</h5>
                    <p style="margin: 0; color: #666; font-size: 13px;">({emp['ticker']})</p>
                    <p style="font-size: 12px; color: #333;">Líder em {emp['ano_lider']}<br>Para compra em {emp['ano_compra']}</p>
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

    # Desempenho parcial ano corrente
    if empresas_lideres_finais:
        st.markdown("## 📊 Desempenho parcial das líderes (ano atual)")

        ano_corrente = datetime.now().year
        tickers_corrente = [e["ticker"] for e in empresas_lideres_finais if int(e["ano_compra"]) == ano_corrente]

        if tickers_corrente:
            tickers_corrente_yf = [_norm_sa(tk) for tk in tickers_corrente]

            precos = baixar_precos_ano_corrente(tickers_corrente_yf)
            if precos is None or precos.empty:
                st.warning("⚠️ Dados de preço indisponíveis para as ações escolhidas no ano atual.")
            else:
                precos.index = pd.to_datetime(precos.index, errors="coerce")
                precos = precos.dropna(how="all")
                precos = precos.resample("B").last().ffill()
                if precos.empty:
                    st.warning("⚠️ Dados de preço indisponíveis para as ações escolhidas no ano atual.")
                else:
                    tickers_limpos = [_strip_sa(tk) for tk in tickers_corrente_yf]
                    dividendos_dict = coletar_dividendos(tickers_corrente_yf)

                    datas_potenciais = pd.date_range(start=f"{ano_corrente}-01-01", end=f"{ano_corrente}-12-31", freq="MS")
                    datas_aporte: List[pd.Timestamp] = []
                    for data in datas_potenciais:
                        data_valida = encontrar_proxima_data_valida(data, precos)
                        if data_valida is not None and data_valida in precos.index:
                            datas_aporte.append(data_valida)

                    patrimonio_aporte = gerir_carteira_simples(
                        precos,
                        tickers_limpos,
                        datas_aporte,
                        dividendos_dict=dividendos_dict,
                    )

                    df_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aporte)
                    if df_selic is None or df_selic.empty:
                        st.warning("⚠️ Não foi possível calcular o benchmark Selic para o período.")
                    else:
                        df_selic = df_selic.reindex(patrimonio_aporte.index).ffill()
                        df_final = pd.concat(
                            [
                                patrimonio_aporte.rename("Estratégia de Aporte"),
                                df_selic,
                            ],
                            axis=1,
                        ).dropna()

                        if not df_final.empty and not df_final["Tesouro Selic"].isna().all():
                            st.markdown(f"### Comparativo de desempenho parcial em {ano_corrente}")
                            fig, ax = plt.subplots(figsize=(10, 5))
                            df_final["Estratégia de Aporte"].plot(ax=ax, label="Estratégia de Aporte")
                            df_final["Tesouro Selic"].plot(ax=ax, label="Tesouro Selic")
                            ax.set_ylabel("Valor acumulado (R$)")
                            ax.set_xlabel("Data")
                            ax.legend()
                            ax.grid(True, linestyle="--", alpha=0.5)
                            st.pyplot(fig)

    # ─────────────────────────────────────────────────────────
    # Monta globais + precos_global (para patches)
    # ─────────────────────────────────────────────────────────
    score_global = pd.concat(score_global_parts, ignore_index=True) if score_global_parts else pd.DataFrame()
    lideres_global = pd.concat(lideres_global_parts, ignore_index=True) if lideres_global_parts else pd.DataFrame()

    if not score_global.empty:
        score_global = _clean_columns(score_global)
        if "ticker" in score_global.columns:
            score_global["ticker"] = score_global["ticker"].astype(str).map(_strip_sa)

    if not lideres_global.empty:
        lideres_global = _clean_columns(lideres_global)
        if "ticker" in lideres_global.columns:
            lideres_global["ticker"] = lideres_global["ticker"].astype(str).map(_strip_sa)

    tickers_final = sorted({str(e.get("ticker", "")).strip() for e in empresas_lideres_finais if str(e.get("ticker", "")).strip()})
    precos_global = pd.DataFrame()
    try:
        if tickers_final:
            precos_tmp = baixar_precos([_norm_sa(tk) for tk in tickers_final])
            precos_global = _ensure_prices_df(precos_tmp)
    except Exception as e:
        st.warning(f"Falha ao carregar preços globais para patches: {type(e).__name__}: {e}")
        precos_global = pd.DataFrame()

    # Salva estado (ESSENCIAL para Patch 6 não “reiniciar”)
    _save_run_state(
        {
            "ok": True,
            "generated_at": datetime.now().isoformat(),
            "margem_superior": float(margem_superior),
            "use_score_v2": bool(use_score_v2),
            "empresas_lideres_finais": empresas_lideres_finais,
            "score_global": score_global,
            "lideres_global": lideres_global,
            "precos_global": precos_global,
            "contrib_globais": contrib_globais,
            "dados_macro": dados_macro,
        }
    )

    # Render patches a partir do estado recém-gerado
    _render_from_state(_load_run_state() or {})


def _render_from_state(state: Dict[str, Any]) -> None:
    """
    Renderiza a parte final (patches) SEM recalcular o portfólio.
    Isso evita “reset” quando você clica no Patch 6 (button => rerun).
    """
    st.markdown("<hr>", unsafe_allow_html=True)

    if not state or state.get("ok") is not True:
        st.info("Sem execução salva. Clique em **Gerar Portfólio**.")
        return

    empresas_lideres_finais = state.get("empresas_lideres_finais") or []
    score_global = state.get("score_global") if isinstance(state.get("score_global"), pd.DataFrame) else pd.DataFrame()
    lideres_global = state.get("lideres_global") if isinstance(state.get("lideres_global"), pd.DataFrame) else pd.DataFrame()
    precos_global = state.get("precos_global") if isinstance(state.get("precos_global"), pd.DataFrame) else pd.DataFrame()
    contrib_globais = state.get("contrib_globais") or []

    st.caption(f"Última execução salva: {state.get('generated_at','—')}")

    if not empresas_lideres_finais:
        st.info("Sem líderes finais — painel de patches não será exibido.")
        return

    if not _PATCHES_OK:
        st.warning(
            "Patches não puderam ser importados. Verifique o caminho do arquivo `portfolio_patches.py`.\n\n"
            f"Erro: {type(_PATCHES_IMPORT_ERR).__name__}: {_PATCHES_IMPORT_ERR}"
        )
        return

    # Renderiza patches (Patch 6 agora funciona sem “reiniciar” porque o estado persiste)
    try:
        render_patch1_regua_conviccao(score_global, lideres_global, empresas_lideres_finais)
        render_patch2_dominancia(score_global, lideres_global, empresas_lideres_finais)
        render_patch3_stress_test(score_global, lideres_global, empresas_lideres_finais)
        render_patch4_diversificacao(empresas_lideres_finais, contrib_globais=contrib_globais)

        render_patch5_benchmark_segmento(
            score_global=score_global,
            empresas_lideres_finais=empresas_lideres_finais,
            precos=precos_global,
            max_universe=80,
        )

        render_patch6_ia_selecao_lideres(
            score_global=score_global,
            lideres_global=lideres_global,
            empresas_lideres_finais=empresas_lideres_finais,
            max_recs_default=10,
        )

        render_patch7_validacao_evidencias(
            score_global=score_global,
            lideres_global=lideres_global,
            empresas_lideres_finais=empresas_lideres_finais,
            days_default=60,
            max_items_per_ticker_default=10,
            cache_ttl_hours_default=12,
        )

    except Exception as e:
        st.error(f"Falha ao renderizar patches: {type(e).__name__}: {e}")
        return
