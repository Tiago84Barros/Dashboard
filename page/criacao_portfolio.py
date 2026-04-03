from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from core.ui_bridge import (
    load_data_from_db,
    load_macro_summary,
    load_multiplos_from_db,
    load_setores_from_db,
)
from core.ticker_utils import normalize_ticker, add_sa_suffix
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

# Snapshot (Supabase) — precisa estar no seu projeto
# (compatível com suas tabelas portfolio_snapshots / portfolio_snapshot_items)
from core.portfolio_snapshot_store import compute_plan_hash, save_snapshot

logger = logging.getLogger(__name__)

# >>> PATCH SCORE V2 (import opcional; uso AUTOMÁTICO se existir)
try:
    from core.scoring_v2 import calcular_score_acumulado_v2  # type: ignore
except Exception:
    calcular_score_acumulado_v2 = None
# <<< PATCH SCORE V2


# >>> PATCHES (portfolio_patches) — import opcional
try:
    from page.portfolio_patches import (
        render_patch1_regua_conviccao,
        render_patch2_dominancia,
    )

    try:
        from page.portfolio_patches import render_patch3_diversificacao
    except Exception:
        from page.portfolio_patches import (  # type: ignore
            render_patch4_diversificacao as render_patch3_diversificacao,
        )

    try:
        from page.portfolio_patches import render_patch4_benchmark_segmento
    except Exception:
        from page.portfolio_patches import (  # type: ignore
            render_patch5_benchmark_segmento as render_patch4_benchmark_segmento,
        )

except Exception:
    render_patch1_regua_conviccao = None  # type: ignore
    render_patch2_dominancia = None  # type: ignore
    render_patch3_diversificacao = None  # type: ignore
    render_patch4_benchmark_segmento = None  # type: ignore

try:
    from page.portfolio_patches import render_patch5_desempenho_empresas
except Exception:
    render_patch5_desempenho_empresas = None  # type: ignore
# <<< PATCHES


# ─────────────────────────────────────────────────────────────
# Utilitários internos
# ─────────────────────────────────────────────────────────────

def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.astype(str).str.strip().str.replace("\ufeff", "", regex=False)
    return out


def _norm_sa(ticker: str) -> str:
    return add_sa_suffix(ticker)


def _strip_sa(ticker: str) -> str:
    return normalize_ticker(ticker)


def _safe_year_count_from_dre(dre: pd.DataFrame) -> int:
    if dre is None or dre.empty:
        return 0

    dre = _clean_columns(dre)

    col_data = None
    if "Data" in dre.columns:
        col_data = "Data"
    elif "data" in dre.columns:
        col_data = "data"

    if col_data is None:
        return 0

    years = pd.to_datetime(dre[col_data], errors="coerce").dt.year
    return int(years.dropna().nunique())


@dataclass(frozen=True)
class EmpresaCarregada:
    ticker: str  # sem .SA
    nome: str
    multiplos: pd.DataFrame
    dre: pd.DataFrame


def _carregar_empresa(row: dict) -> Optional[EmpresaCarregada]:
    """Carrega múltiplos + DRE para uma empresa, retornando estrutura padronizada."""
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

        if "data" in mult.columns and "Data" not in mult.columns:
            mult = mult.rename(columns={"data": "Data"})
        if "data" in dre.columns and "Data" not in dre.columns:
            dre = dre.rename(columns={"data": "Data"})

        # adiciona Ano quando possível (compatível com scoring)
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
    min_anos: int = 10,  # AUTOMÁTICO: estabelecidas >=10 anos
    max_workers: int = 12,
) -> List[str]:
    """Filtra tickers com pelo menos `min_anos` anos de DRE (paralelo)."""
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

    if "data" in dados_macro.columns and "Data" not in dados_macro.columns:
        dados_macro = dados_macro.rename(columns={"data": "Data"})

    if "Data" not in dados_macro.columns:
        return None

    dados_macro["Data"] = pd.to_datetime(dados_macro["Data"], errors="coerce")
    dados_macro = dados_macro.dropna(subset=["Data"]).sort_values("Data").reset_index(drop=True)
    return dados_macro


def _merge_empresa_final(
    acc: Dict[str, dict],
    item: dict,
) -> None:
    """Merge por ticker: mantém motivos combinados e flags."""
    tk = str(item.get("ticker", "")).strip().upper()
    if not tk:
        return

    if tk not in acc:
        acc[tk] = item
        return

    cur = acc[tk]
    # Motivos como lista
    motivos_cur = list(cur.get("motivos") or [])
    motivos_new = list(item.get("motivos") or [])
    motivos = []
    for m in (motivos_cur + motivos_new):
        if m and m not in motivos:
            motivos.append(m)
    cur["motivos"] = motivos

    # Flags
    cur["is_lider_ultimo"] = bool(cur.get("is_lider_ultimo")) or bool(item.get("is_lider_ultimo"))
    cur["is_maior_part"] = bool(cur.get("is_maior_part")) or bool(item.get("is_maior_part"))

    # ano_lider: se qualquer lado tiver ano_lider, mantém o maior
    a1 = cur.get("ano_lider")
    a2 = item.get("ano_lider")
    try:
        cur["ano_lider"] = int(max([x for x in [a1, a2] if x is not None]))
    except Exception:
        cur["ano_lider"] = a1 if a1 is not None else a2

    # setor/subsetor/segmento: mantém o primeiro (ou atualiza se vazio)
    for k in ("setor", "subsetor", "segmento"):
        if not cur.get(k) and item.get(k):
            cur[k] = item.get(k)

    # nome/logo: mantém primeiro, completa se faltando
    if not cur.get("nome") and item.get("nome"):
        cur["nome"] = item.get("nome")
    if not cur.get("logo_url") and item.get("logo_url"):
        cur["logo_url"] = item.get("logo_url")


# ─────────────────────────────────────────────────────────────
# Render Streamlit
# ─────────────────────────────────────────────────────────────

def render():
    st.markdown("<h1 style='text-align: center;'>Criação de Portfólio</h1>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────────
    # CONTROLES DE EXECUÇÃO (não roda sozinho)
    # ─────────────────────────────────────────────────────────────
    if "cp_last_params" not in st.session_state:
        st.session_state["cp_last_params"] = {"margem_superior": 10.0}
    if "cp_should_run" not in st.session_state:
        st.session_state["cp_should_run"] = False

    with st.sidebar:
        st.markdown("### ▶️ Execução")

        with st.form("cp_run_form", clear_on_submit=False):
            margem_superior = st.number_input(
                "Margem mínima vs Tesouro Selic (%)",
                min_value=-1000.0,
                max_value=10000.0,
                value=float(st.session_state["cp_last_params"].get("margem_superior", 10.0)),
                step=0.1,
                format="%.2f",
                help="Digite a % e clique em RODAR. Ex.: 7.5, 12, 33.33",
            )
            gerar = st.form_submit_button("🚀 Rodar Criação de Portfólio")

        if gerar:
            st.session_state["cp_last_params"] = {"margem_superior": float(margem_superior)}
            st.session_state["cp_should_run"] = True

    if not st.session_state["cp_should_run"]:
        st.info("Defina a margem (%) na barra lateral e clique em **🚀 Rodar Criação de Portfólio**.")
        return

    margem_superior = float(st.session_state["cp_last_params"]["margem_superior"])

    # Score V2: AUTOMÁTICO (se disponível)
    use_score_v2 = calcular_score_acumulado_v2 is not None

    # ── Carrega setores (cache em sessão)
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

    # Mapas para Score V2 (se usar)
    _tmp = setores_df[["ticker", "SEGMENTO", "SUBSETOR", "SETOR"]].copy()
    _tmp["ticker"] = _tmp["ticker"].astype(str).str.upper().str.replace(".SA", "", regex=False).str.strip()
    _tmp["SEGMENTO"] = _tmp["SEGMENTO"].fillna("OUTROS").astype(str)
    _tmp["SUBSETOR"] = _tmp["SUBSETOR"].fillna("OUTROS").astype(str)
    _tmp["SETOR"] = _tmp["SETOR"].fillna("OUTROS").astype(str)
    group_map = dict(zip(_tmp["ticker"], _tmp["SEGMENTO"]))
    subsetor_map = dict(zip(_tmp["ticker"], _tmp["SUBSETOR"]))
    setor_map = dict(zip(_tmp["ticker"], _tmp["SETOR"]))

    dados_macro = _build_macro()
    if dados_macro is None or dados_macro.empty:
        st.error("Não foi possível carregar/normalizar os dados macroeconômicos.")
        st.stop()

    # grupos únicos por segmento
    setores_unicos = (
        setores_df[["SETOR", "SUBSETOR", "SEGMENTO"]]
        .drop_duplicates()
        .sort_values(["SETOR", "SUBSETOR", "SEGMENTO"])
    )

    empresas_final_map: Dict[str, dict] = {}

    score_global_parts: List[pd.DataFrame] = []
    lideres_global_parts: List[pd.DataFrame] = []
    precos_global_parts: List[pd.DataFrame] = []

    # ─────────────────────────────────────────────────────────
    # Loop por segmento
    # ─────────────────────────────────────────────────────────
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

        # Filtro mínimo de existência do segmento
        if len(set(tickers_segmento)) <= 1:
            continue

        # Histórico automático (>=10 anos)
        tickers_validos = _filtrar_tickers_com_min_anos(tickers_segmento, min_anos=10, max_workers=12)
        if len(tickers_validos) <= 1:
            continue

        tickers_validos_set = set(tickers_validos)
        empresas_validas = empresas_segmento[
            empresas_segmento["ticker"].astype(str).apply(lambda x: _strip_sa(x) in tickers_validos_set)
        ]
        if empresas_validas.empty or len(empresas_validas) <= 1:
            continue

        # carrega dados completos (multiplos + dre) em paralelo
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

        # Score (V2 automático se disponível)
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

        # preços + penalização de platô (mensal)
        try:
            precos = baixar_precos([_norm_sa(e.ticker) for e in lista_empresas])
            if precos is None or precos.empty:
                continue
            precos.index = pd.to_datetime(precos.index, errors="coerce")
            precos = precos.dropna(how="all")
            if precos.empty:
                continue
            try:
                precos_global_parts.append(precos.copy())
            except Exception:
                pass

            precos_mensal = precos.resample("ME").last()
            score = penalizar_plato(score, precos_mensal, meses=12, penal=0.30)
        except Exception:
            continue

        if score is None or score.empty:
            continue

        tickers_score = [str(t) for t in score["ticker"].dropna().unique().tolist()]
        tickers_score_yf = [_norm_sa(t) for t in tickers_score]
        dividendos = coletar_dividendos(tickers_score_yf)

        lideres = determinar_lideres(score)
        if lideres is None or lideres.empty:
            continue

        # Acumuladores para patches
        try:
            score_seg = score.copy()
            score_seg["SETOR"] = setor
            score_seg["SUBSETOR"] = subsetor
            score_seg["SEGMENTO"] = segmento
            score_global_parts.append(score_seg)

            lideres_seg = lideres.copy()
            lideres_seg["SETOR"] = setor
            lideres_seg["SUBSETOR"] = subsetor
            lideres_seg["SEGMENTO"] = segmento
            lideres_global_parts.append(lideres_seg)
        except Exception:
            pass

        patrimonio_empresas, datas_aportes = gerir_carteira(precos, score, lideres, dividendos)
        if patrimonio_empresas is None or patrimonio_empresas.empty:
            continue

        patrimonio_empresas = patrimonio_empresas.apply(pd.to_numeric, errors="coerce")

        cols_empresas = patrimonio_empresas.columns.drop("Patrimônio", errors="ignore")
        if len(cols_empresas) <= 0:
            continue

        final_empresas = float(patrimonio_empresas.iloc[-1][cols_empresas].dropna().sum())

        # Selic: igual ao antigo (macro DB)
        patrimonio_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aportes)
        if patrimonio_selic is None or patrimonio_selic.empty:
            continue

        final_selic = float(patrimonio_selic.iloc[-1]["Tesouro Selic"])
        if final_selic <= 0:
            continue

        diff = ((final_empresas / final_selic) - 1) * 100.0
        if diff < margem_superior:
            continue

        # Exibição do segmento destacado
        st.markdown(f"### {setor} > {subsetor} > {segmento}")
        st.markdown(
            f"**Valor final da estratégia:** R$ {final_empresas:,.2f} "
            f"({diff:.1f}% acima do Tesouro Selic)"
        )

        empresas_estrategia = cols_empresas
        colunas_empresas = st.columns(min(3, len(empresas_estrategia)))

        # Ticker de maior participação no segmento (último ponto)
        ult_vals = patrimonio_empresas.iloc[-1][cols_empresas].astype(float)
        ticker_maior_part = _strip_sa(str(ult_vals.idxmax()))

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

        # ─────────────────────────────────────────────
        # Seleção final do segmento para o "próximo ano"
        # Regra:
        # - ticker_lider_ultimo (score último ano)
        # - ticker_maior_part (último ponto do backtest)
        # - Se iguais → entra 1 empresa
        # - Se diferentes → entram 2
        # Rotulagem:
        # - "LÍDER NO SCORE (AAAA)"
        # - "MAIOR PARTICIPAÇÃO NO SEGMENTO"
        # - ambos quando aplicável
        # ─────────────────────────────────────────────
        ultimo_ano = int(pd.to_numeric(score["Ano"], errors="coerce").max())
        lideres_ano = lideres[lideres["Ano"] == ultimo_ano].copy()
        if lideres_ano is None or lideres_ano.empty:
            continue

        # Se houver mais de um líder no ano, escolhe o 1º por padrão (mantém determinismo)
        ticker_lider_ultimo = _strip_sa(str(lideres_ano.iloc[0]["ticker"]))

        # Selecionados do segmento
        selecionados = [ticker_lider_ultimo]
        if ticker_maior_part and (ticker_maior_part != ticker_lider_ultimo):
            selecionados.append(ticker_maior_part)

        for tk in selecionados:
            tk = _strip_sa(tk)
            if not tk:
                continue

            is_lider_ultimo = (tk == ticker_lider_ultimo)
            is_maior_part = (tk == ticker_maior_part)

            motivos: List[str] = []
            if is_lider_ultimo:
                motivos.append(f"LÍDER NO SCORE ({ultimo_ano})")
            if is_maior_part:
                motivos.append("MAIOR PARTICIPAÇÃO NO SEGMENTO")

            _merge_empresa_final(
                empresas_final_map,
                {
                    "ticker": tk,
                    "nome": next((e.nome for e in lista_empresas if _strip_sa(e.ticker) == tk), tk),
                    "logo_url": get_logo_url(tk),
                    "ano_lider": ultimo_ano if is_lider_ultimo else None,
                    "ano_compra": ultimo_ano + 1,
                    "setor": setor,
                    "subsetor": subsetor,
                    "segmento": segmento,
                    "is_lider_ultimo": is_lider_ultimo,
                    "is_maior_part": is_maior_part,
                    "motivos": motivos,
                },
            )

    # transforma map em lista final
    empresas_lideres_finais: List[dict] = list(empresas_final_map.values())

    # ─────────────────────────────────────────────────────────
    # Bloco final: empresas para o próximo ano + distribuição
    # ─────────────────────────────────────────────────────────
    if empresas_lideres_finais:
        st.markdown("## 📑 Empresas líderes para o próximo ano")

        colunas_lideres = st.columns(3)
        for idx, emp in enumerate(empresas_lideres_finais):
            col = colunas_lideres[idx % 3]

            motivos = emp.get("motivos") or []
            if isinstance(motivos, str):
                motivos = [motivos]
            motivos_html = "<br>".join([f"• {m}" for m in motivos if m])

            # fallback (não deveria precisar)
            if not motivos_html:
                if emp.get("ano_lider"):
                    motivos_html = f"• LÍDER NO SCORE ({emp['ano_lider']})"
                else:
                    motivos_html = "• Selecionada"

            col.markdown(
                f"""
                <div style='border: 2px solid #28a745; border-radius: 10px; padding: 12px; margin-bottom: 10px; background-color: #f0fff4; text-align: center;'>
                    <img src="{emp.get('logo_url','')}" width="45" />
                    <h5 style="margin: 5px 0 0;">{emp.get('nome','')}</h5>
                    <p style="margin: 0; color: #666; font-size: 13px;">({emp.get('ticker','')})</p>
                    <p style="font-size: 12px; color: #333; margin-top: 8px;">
                        {motivos_html}<br>
                        <span style="color:#666;">Para compra em {emp.get('ano_compra','')}</span>
                    </p>
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

    # ─────────────────────────────────────────────────────────
    # Etapa: Desempenho parcial no ano corrente (líderes do ano)
    # ─────────────────────────────────────────────────────────
    if empresas_lideres_finais:
        st.markdown("## 📊 Desempenho parcial das selecionadas (ano atual)")

        ano_corrente = datetime.now().year
        tickers_corrente = [e["ticker"] for e in empresas_lideres_finais if int(e.get("ano_compra", 0)) == ano_corrente]

        if tickers_corrente:
            tickers_corrente_yf = [_norm_sa(tk) for tk in tickers_corrente]

            precos = baixar_precos_ano_corrente(tickers_corrente_yf)
            if precos is None or precos.empty:
                st.warning("⚠️ Dados de preço indisponíveis para as ações escolhidas no ano atual.")
                st.stop()

            precos.index = pd.to_datetime(precos.index, errors="coerce")
            precos = precos.dropna(how="all")
            precos = precos.resample("B").last().ffill()

            if precos.empty:
                st.warning("⚠️ Dados de preço indisponíveis para as ações escolhidas no ano atual.")
                st.stop()

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
                st.stop()

            df_selic = df_selic.reindex(patrimonio_aporte.index).ffill()

            df_final = pd.concat(
                [patrimonio_aporte.rename("Estratégia de Aporte"), df_selic],
                axis=1,
            ).dropna()

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

    # ─────────────────────────────────────────────────────────
    # PATCHES (1..5)
    # ─────────────────────────────────────────────────────────
    try:
        score_global = pd.concat(score_global_parts, ignore_index=True) if score_global_parts else pd.DataFrame()
        lideres_global = pd.concat(lideres_global_parts, ignore_index=True) if lideres_global_parts else pd.DataFrame()
    except Exception:
        score_global = pd.DataFrame()
        lideres_global = pd.DataFrame()

    try:
        df_prices_global = pd.concat(precos_global_parts, axis=1) if precos_global_parts else pd.DataFrame()
        if not df_prices_global.empty:
            df_prices_global = df_prices_global.loc[:, ~df_prices_global.columns.duplicated()].copy()
            df_prices_global.index = pd.to_datetime(df_prices_global.index, errors="coerce")
            df_prices_global = df_prices_global.dropna(how="all")
            df_prices_global = df_prices_global.dropna(how="all", axis=1)
    except Exception:
        df_prices_global = pd.DataFrame()

    if empresas_lideres_finais:
        st.markdown("---")
        st.caption("🧪 Teste incremental: habilite os patches um a um para detectar reinícios (reruns) anormais.")

        if render_patch1_regua_conviccao is not None:
            with st.expander("🧩 Patch 1 — Régua de Convicção", expanded=False):
                try:
                    render_patch1_regua_conviccao(score_global, lideres_global, empresas_lideres_finais)
                except Exception as e:
                    st.error(f"Patch 1 falhou: {type(e).__name__}: {e}")

        if render_patch2_dominancia is not None:
            with st.expander("🧩 Patch 2 — Dominância", expanded=False):
                try:
                    render_patch2_dominancia(score_global, lideres_global, empresas_lideres_finais)
                except Exception as e:
                    st.error(f"Patch 2 falhou: {type(e).__name__}: {e}")

        if render_patch3_diversificacao is not None:
            with st.expander("🧩 Patch 3 — Diversificação e Concentração de Risco", expanded=False):
                try:
                    render_patch3_diversificacao(empresas_lideres_finais, contrib_globais=None)
                except Exception as e:
                    st.error(f"Patch 3 falhou: {type(e).__name__}: {e}")

        if render_patch4_benchmark_segmento is not None:
            with st.expander("🧩 Patch 4 — Benchmark do Segmento (último ano do score)", expanded=False):
                try:
                    render_patch4_benchmark_segmento(
                        score_global=score_global,
                        empresas_lideres_finais=empresas_lideres_finais,
                        precos=df_prices_global,
                        max_universe=80,
                    )
                except Exception as e:
                    st.error(f"Patch 4 falhou: {type(e).__name__}: {e}")

        if render_patch5_desempenho_empresas is not None:
            with st.expander("🧩 Patch 5 — Desempenho das Empresas (Preço/DY + Lucros)", expanded=False):
                try:
                    render_patch5_desempenho_empresas(
                        score_global=score_global,
                        lideres_global=lideres_global,
                        empresas_lideres_finais=empresas_lideres_finais,
                        precos=df_prices_global,
                        max_empresas=20,
                    )
                except Exception as e:
                    st.error(f"Patch 5 falhou: {type(e).__name__}: {e}")

    # ─────────────────────────────────────────────────────────
    # SNAPSHOT NO SUPABASE (obrigatório para habilitar Patch 6)
    # ─────────────────────────────────────────────────────────
    if empresas_lideres_finais:
        # itens (ticker/peso) — se não houver peso, igualitário
        items: List[dict] = []
        for e in empresas_lideres_finais:
            tk = str(e.get("ticker", "")).strip().upper()
            if not tk:
                continue
            peso = e.get("peso", e.get("peso_sugerido", None))
            items.append({"ticker": tk, "peso": float(peso) if peso is not None else None})

        if items:
            if any(it["peso"] is None for it in items):
                w = 1.0 / len(items)
                for it in items:
                    it["peso"] = w
            else:
                s = sum(float(it["peso"]) for it in items)
                if s > 0:
                    for it in items:
                        it["peso"] = float(it["peso"]) / s

            # Segmentos cobertos (salvo no snapshot para uso no Patch 6)
            # Observação: os itens salvos em portfolio_snapshot_items são apenas ticker/peso.
            # Para não alterar o schema agora, persistimos a lista de segmentos no header (filters_json).
            segmentos_cobertos = sorted(
                {
                    str(e.get("segmento")).strip()
                    for e in empresas_lideres_finais
                    if str(e.get("segmento") or "").strip()
                }
            )

            filters_json = {
                "tipo_empresa": "ESTABELECIDA_10A",
                "min_anos_dre": 10,
                "margem_superior_percent": float(margem_superior),
                "segmentos": segmentos_cobertos,
                "selic_source": "macro_db_load_macro_summary",
                "macro_last_date": str(dados_macro["Data"].max().date()) if "Data" in dados_macro.columns else None,
                "score_mode": "v2_auto_if_available" if use_score_v2 else "v1",
            }

            payload_hash = {
                "items": items,
                "tipo_empresa": "ESTABELECIDA_10A",
                "margem_superior_percent": float(margem_superior),
                "filters_json": filters_json,
                "status": "active",
            }
            plan_hash = compute_plan_hash(payload_hash)

            try:
                                # ─────────────────────────────────────────
                # Captura Selic atual (último valor macro)
                # ─────────────────────────────────────────
                selic_ref = None
                try:
                    if dados_macro is not None and not dados_macro.empty:
                        dm = dados_macro.copy()
                        dm.columns = [str(c).strip() for c in dm.columns]
                
                        col_selic = None
                        if "selic" in [c.lower() for c in dm.columns]:
                            for c in dm.columns:
                                if c.lower() == "selic":
                                    col_selic = c
                                    break
                
                        if col_selic is not None:
                            s = pd.to_numeric(dm[col_selic], errors="coerce").dropna()
                            if not s.empty:
                                selic_ref = float(s.iloc[-1])
                except Exception:
                    selic_ref = None

                snapshot_id = save_snapshot(
                    items=items,
                    selic_ref=selic_ref,
                    margem_superior=float(margem_superior),
                    tipo_empresa="ESTABELECIDA_10A",
                    filters_json=filters_json,
                    notes="criado via criacao_portfolio",
                    status="active",
                    plan_hash=plan_hash,
                )
                st.success(f"Snapshot salvo no Supabase ✅ id={snapshot_id}")
            except Exception as e:
                st.error(f"Falha ao salvar snapshot no Supabase: {type(e).__name__}: {e}")

    st.session_state["cp_should_run"] = False
    st.markdown("<hr>", unsafe_allow_html=True)
