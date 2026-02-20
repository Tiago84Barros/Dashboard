from __future__ import annotations

import logging
import json
import hashlib
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Sequence, Tuple

import pandas as pd
import numpy as np
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


# >>> PATCHES (portfolio_patches) — import opcional (teste incremental)
try:
    # Patch 1 e 2 (estáveis)
    from page.portfolio_patches import (
        render_patch1_regua_conviccao,
        render_patch2_dominancia,
    )

    # Patch 3 (novo): diversificação (antigo Patch 4)
    try:
        from page.portfolio_patches import render_patch3_diversificacao
    except Exception:
        # compat: se você ainda não renomeou no portfolio_patches.py
        from page.portfolio_patches import render_patch4_diversificacao as render_patch3_diversificacao  # type: ignore

    # Patch 4 (novo): benchmark do segmento (antigo Patch 5)
    try:
        from page.portfolio_patches import render_patch4_benchmark_segmento
    except Exception:
        from page.portfolio_patches import render_patch5_benchmark_segmento as render_patch4_benchmark_segmento  # type: ignore

except Exception:
    render_patch1_regua_conviccao = None  # type: ignore
    render_patch2_dominancia = None  # type: ignore
    render_patch3_diversificacao = None  # type: ignore
    render_patch4_benchmark_segmento = None  # type: ignore


# Patch 5 (novo): desempenho por empresa (cards)
try:
    from page.portfolio_patches import render_patch5_desempenho_empresas
except Exception:
    render_patch5_desempenho_empresas = None  # type: ignore
# <<< PATCHES (portfolio_patches)

from core.portfolio import (
    calcular_patrimonio_selic_macro,
    gerir_carteira,
    gerir_carteira_modulada,
    encontrar_proxima_data_valida,
    gerir_carteira_simples,
)
from core.yf_data import (
    baixar_precos,
    coletar_dividendos,
    baixar_precos_ano_corrente,
)
from core.weights import get_pesos

# Snapshot persistente (Supabase)
try:
    from core.portfolio_snapshot_store import save_snapshot
except Exception:
    save_snapshot = None  # type: ignore

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Utilitários internos (sem mexer em outros módulos)
# ─────────────────────────────────────────────────────────────

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


@dataclass(frozen=True)
class EmpresaCarregada:
    ticker: str     # sem .SA
    nome: str
    multiplos: pd.DataFrame
    dre: pd.DataFrame


def _carregar_empresa(row: dict) -> Optional[EmpresaCarregada]:
    """
    Carrega multiplos + dre para uma empresa, retornando estrutura padronizada.
    """
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

        # adiciona Ano quando possível (compatível com scoring)
        if "Data" in mult.columns and "Ano" not in mult.columns:
            mult["Ano"] = pd.to_datetime(mult["Data"], errors="coerce").dt.year
        if "Data" in dre.columns and "Ano" not in dre.columns:
            dre["Ano"] = pd.to_datetime(dre["Data"], errors="coerce").dt.year

        return EmpresaCarregada(ticker=tk, nome=nome, multiplos=mult, dre=dre)
    except Exception as e:
        logger.debug("Falha ao carregar empresa %s: %s", row.get("ticker"), e)
        return None


def _filtrar_tickers_com_min_anos(tickers: Sequence[str], min_anos: int = 10, max_workers: int = 12) -> List[str]:
    """
    Filtra tickers que têm pelo menos `min_anos` anos de DRE.
    Usa paralelismo para acelerar.
    """
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



def _filtrar_tickers_por_tipo(
    tickers: Sequence[str],
    tipo: str,
    max_workers: int = 12,
) -> Tuple[List[str], dict]:
    """
    Filtro de histórico (tipo de empresa) — define o “n do gate”.

    Retorna:
      - tickers_validos (SEM '.SA')
      - years_map: {ticker: anos_de_historico}

    Regras:
      - "Crescimento (<10 anos)": 1..9 anos
      - "Estabelecida (≥10 anos)": >=10 anos
      - "Todas": >=1 ano
    """
    tickers = [_strip_sa(t) for t in tickers if (t or "").strip()]
    tickers = [t for t in tickers if t]
    if not tickers:
        return [], {}

    def _year_check(tk: str) -> Tuple[str, int]:
        try:
            dre = load_data_from_db(_norm_sa(tk))
            n = _safe_year_count_from_dre(dre)
            return tk, int(n)
        except Exception:
            return tk, 0

    years_map: dict = {}
    max_workers = min(max_workers, max(2, len(tickers)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_year_check, tk) for tk in tickers]
        for fut in as_completed(futs):
            tk, n = fut.result()
            years_map[tk] = int(n) if n is not None else 0

    tipo0 = (tipo or "Todas").strip()
    def _pass(tk: str) -> bool:
        n = int(years_map.get(tk, 0))
        if tipo0 == "Crescimento (<10 anos)":
            return (n < 10) and (n > 0)
        if tipo0 == "Estabelecida (≥10 anos)":
            return n >= 10
        return n > 0

    ok = [tk for tk in tickers if _pass(tk)]
    return sorted(set(ok)), years_map


def _build_macro() -> Optional[pd.DataFrame]:
    """
    Retorna macro com coluna 'Data' (não index), alinhado ao padrão das outras páginas.
    """
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
# Render Streamlit
# ─────────────────────────────────────────────────────────────

def render():
    st.markdown("<h1 style='text-align: center;'>Criação de Portfólio</h1>", unsafe_allow_html=True)

        # ─────────────────────────────────────────────────────────────
    # CONTROLES DE EXECUÇÃO (não roda sozinho)
    # ─────────────────────────────────────────────────────────────
    if "cp_last_params" not in st.session_state:
        st.session_state["cp_last_params"] = {"margem_superior": 10.0, "use_score_v2": False, "tipo_empresa_idx": 2, "selic_ref": 0.0}
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

            # mantém o toggle do Score V2 (se existir)
            use_score_v2 = st.checkbox(
                "Usar Score V2 (se disponível)",
                value=bool(st.session_state["cp_last_params"].get("use_score_v2", False)),
            )

            tipo_empresa = st.radio(
                "Perfil de empresa (histórico DRE):",
                ["Crescimento (<10 anos)", "Estabelecida (≥10 anos)", "Todas"],
                index=int(st.session_state["cp_last_params"].get("tipo_empresa_idx", 2)),
                help="Este filtro define o universo elegível por segmento e também o gate binário do modo de carteira (igual ao advanced.py).",
            )

            selic_ref = st.number_input(
                "Selic referência (% a.a.)",
                min_value=0.0,
                max_value=100.0,
                value=float(st.session_state["cp_last_params"].get("selic_ref", 0.0)),
                step=0.05,
                format="%.2f",
                help="Valor informativo para persistência no snapshot. O benchmark do backtest continua usando info_economica (macro).",
            )

            gerar = st.form_submit_button("🚀 Rodar Criação de Portfólio")

        if gerar:
            st.session_state["cp_last_params"] = {
                "margem_superior": float(margem_superior),
                "use_score_v2": bool(use_score_v2),
                "tipo_empresa_idx": int(["Crescimento (<10 anos)", "Estabelecida (≥10 anos)", "Todas"].index(tipo_empresa)),
                "tipo_empresa": str(tipo_empresa),
                "selic_ref": float(selic_ref),
            }
            st.session_state["cp_should_run"] = True

    if not st.session_state["cp_should_run"]:
        st.info("Defina a margem (%) na barra lateral e clique em **🚀 Rodar Criação de Portfólio**.")
        return

    # parâmetro efetivo usado na execução
    margem_superior = float(st.session_state["cp_last_params"]["margem_superior"])
    use_score_v2 = bool(st.session_state["cp_last_params"]["use_score_v2"])
    tipo_empresa = str(st.session_state["cp_last_params"].get("tipo_empresa") or "Todas")
    selic_ref = float(st.session_state["cp_last_params"].get("selic_ref") or 0.0)
    policy_calibrada = {"mode": "heuristica_calibrada", "eps": 0.35}

    # ── Carrega setores (cache em sessão)
    setores_df = st.session_state.get("setores_df")
    if setores_df is None or getattr(setores_df, "empty", True):
        setores_df = load_setores_from_db()
        if setores_df is None or setores_df.empty:
            st.error("Não foi possível carregar a base de setores do banco.")
            st.stop()
        setores_df = _clean_columns(setores_df)
        st.session_state["setores_df"] = setores_df

    # valida colunas esperadas
    required_cols = {"SETOR", "SUBSETOR", "SEGMENTO", "ticker"}
    if not required_cols.issubset(setores_df.columns):
        st.error(f"Base de setores não contém as colunas esperadas: {sorted(required_cols)}")
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

    empresas_lideres_finais: List[dict] = []
    # Acumuladores para Patches 1-3 (histórico global, leve)
    score_global_parts: List[pd.DataFrame] = []
    lideres_global_parts: List[pd.DataFrame] = []
    precos_global_parts: List[pd.DataFrame] = []  # para Patch 5 (sem baixar novamente)

    # ─────────────────────────────────────────────────────────
    # Loop por segmento (pipeline leve)
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

        # ignora segmentos com poucos tickers
        if len(set(tickers_segmento)) <= 1:
            continue

        # filtro de histórico (tipo de empresa) — define o “n do gate”
        tickers_validos, years_map = _filtrar_tickers_por_tipo(
            tickers_segmento,
            tipo=tipo_empresa,
            max_workers=12,
        )
        n_total_segmento = int(len(tickers_validos))
        if n_total_segmento <= 1:
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

        # mapeia setor por empresa
        setores_empresa = {e.ticker: obter_setor_da_empresa(e.ticker, setores_df) for e in lista_empresas}

        # pesos por SETOR (regra existente)
        pesos = get_pesos(setor)

        # score acumulado
        payload_empresas = [
            {"ticker": e.ticker, "nome": e.nome, "multiplos": e.multiplos, "dre": e.dre}
            for e in lista_empresas
        ]

        # >>> PATCH SCORE V2 (switch v1/v2)
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
        # <<< PATCH SCORE V2

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
            # acumula preços (evita novos downloads nos patches)
            try:
                precos_global_parts.append(precos.copy())
            except Exception:
                pass
            precos_mensal = precos.resample("M").last()
            score = penalizar_plato(score, precos_mensal, meses=12, penal=0.30)
        except Exception:
            continue

        if score.empty:
            continue

        # dividendos (normaliza para .SA) + líderes + backtest
        tickers_score = [str(t) for t in score["ticker"].dropna().unique().tolist()]
        tickers_score_yf = [_norm_sa(t) for t in tickers_score]
        dividendos = coletar_dividendos(tickers_score_yf)

        lideres = determinar_lideres(score)
        if lideres is None or lideres.empty:
            continue
        # ── Acumula score e líderes para os patches (sem rede)
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

        # Gate binário do modo (igual ao advanced.py)
        #   - n_total_segmento >= 5 -> Ajuste Calibrado (auto-tuning)
        #   - n_total_segmento <= 4 -> Modelo Padrão (aportes iguais)
        usar_calibrado = int(n_total_segmento) >= 5

        if usar_calibrado:
            patrimonio_empresas, datas_aportes = gerir_carteira_modulada(
                precos, score, lideres, dividendos, policy=policy_calibrada
            )
        else:
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

        # ── Exibição do segmento destacado
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

        # Seleção de empresas para “próximo ano” dentro do segmento aprovado
        ultimo_ano = int(pd.to_numeric(score["Ano"], errors="coerce").max())

        # líder do último ano (top1)
        lider_ultimo_df = lideres[lideres["Ano"] == ultimo_ano].head(1)
        if lider_ultimo_df is None or lider_ultimo_df.empty:
            continue
        ticker_lider_ultimo = _strip_sa(str(lider_ultimo_df.iloc[0]["ticker"]))

        # maior participação no backtest (última linha, exclui "Patrimônio")
        last_row = patrimonio_empresas.iloc[-1].drop(labels=["Patrimônio"], errors="ignore")
        last_row = pd.to_numeric(last_row, errors="coerce").dropna()
        last_row = last_row[last_row > 0]
        if last_row.empty:
            continue
        ticker_maior_part = _strip_sa(str(last_row.sort_values(ascending=False).index[0]))

        if ticker_lider_ultimo == ticker_maior_part:
            escolhidos = [(ticker_lider_ultimo, True, "LÍDER")]
        else:
            escolhidos = [
                (ticker_lider_ultimo, True, "LÍDER"),
                (ticker_maior_part, False, "GRANDE_PARTICIPACAO"),
            ]

        # peso inicial proporcional à participação (no próprio segmento)
        sel_vals = {}
        for tk, _, _ in escolhidos:
            v = last_row.get(tk)
            if v is None:
                v = last_row.get(tk + ".SA")
            sel_vals[tk] = float(v) if v is not None else 0.0
        s_vals = sum(sel_vals.values())
        if s_vals <= 0:
            s_vals = float(len(escolhidos))
            sel_vals = {tk: 1.0 for tk, _, _ in escolhidos}

        for tk, is_lider, motivo in escolhidos:
            empresas_lideres_finais.append(
                {
                    "ticker": tk,
                    "nome": next((e.nome for e in lista_empresas if _strip_sa(e.ticker) == tk), tk),
                    "logo_url": get_logo_url(tk),
                    "ano_lider": int(ultimo_ano),
                    "ano_compra": int(ultimo_ano) + 1,
                    "setor": setor,
                    "subsetor": subsetor,
                    "segmento": segmento,
                    "is_lider_ultimo": bool(is_lider),
                    "motivo_select": str(motivo),
                    "peso": float(sel_vals.get(tk, 0.0)) / float(s_vals),
                }
            )


    # ─────────────────────────────────────────────────────────
    # Recalibração de “peso sugerido” (não muda seleção)
    #   40% igualitário
    #   30% consolidação (proxy: mcap + estabilidade via volatilidade)
    #   30% qualidade via score do último ano disponível
    # Aplica cap por ativo (default 30%)
    # ─────────────────────────────────────────────────────────
    def _cap_redistribute(w: dict, cap: float = 0.30, iters: int = 12) -> dict:
        w = {k: float(v) for k, v in (w or {}).items() if k and v is not None}
        s = sum(w.values())
        if s <= 0:
            return w
        w = {k: v / s for k, v in w.items()}
        for _ in range(iters):
            over = {k: v for k, v in w.items() if v > cap}
            if not over:
                break
            excess = sum(v - cap for v in over.values())
            for k in over:
                w[k] = cap
            under = [k for k, v in w.items() if v < cap - 1e-12]
            if not under or excess <= 0:
                break
            under_sum = sum(w[k] for k in under)
            if under_sum <= 0:
                add = excess / len(under)
                for k in under:
                    w[k] += add
            else:
                for k in under:
                    w[k] += excess * (w[k] / under_sum)
            s2 = sum(w.values())
            if s2 > 0:
                w = {k: v / s2 for k, v in w.items()}
        s = sum(w.values())
        return {k: v / s for k, v in w.items()} if s > 0 else w

    if empresas_lideres_finais:
        # agrega duplicatas por ticker (se ocorrer)
        tickers_final = [str(e.get("ticker") or "").strip().upper().replace(".SA", "") for e in empresas_lideres_finais]
        tickers_final = [t for t in tickers_final if t]
        uniq = list(dict.fromkeys(tickers_final))

        # 40% igualitário
        n = len(uniq)
        w_equal = {t: 1.0 / n for t in uniq} if n else {}

        # 30% qualidade (score último ano)
        quality_raw = {}
        try:
            if score_global_parts:
                sg = pd.concat(score_global_parts, ignore_index=True)
                sg["ticker"] = sg["ticker"].astype(str).str.upper().str.replace(".SA", "", regex=False).str.strip()
                sg["Ano"] = pd.to_numeric(sg.get("Ano"), errors="coerce")
                ultimo_ano_global = int(sg["Ano"].max()) if not sg.empty else None
                if ultimo_ano_global is not None:
                    sg_last = sg[sg["Ano"] == ultimo_ano_global].copy()
                    col_score = None
                    for c in ["Score_Ajustado", "Score", "score", "score_total"]:
                        if c in sg_last.columns:
                            col_score = c
                            break
                    if col_score:
                        tmp = sg_last[["ticker", col_score]].copy()
                        tmp[col_score] = pd.to_numeric(tmp[col_score], errors="coerce")
                        tmp = tmp.dropna()
                        for t in uniq:
                            v = tmp.loc[tmp["ticker"] == t, col_score]
                            if not v.empty:
                                quality_raw[t] = float(v.iloc[0])
        except Exception:
            quality_raw = {}

        if not quality_raw:
            w_quality = w_equal.copy()
        else:
            vals = np.array([quality_raw.get(t, np.nan) for t in uniq], dtype=float)
            mmin = np.nanmin(vals) if np.isfinite(vals).any() else 0.0
            mmax = np.nanmax(vals) if np.isfinite(vals).any() else 1.0
            denom = (mmax - mmin) if (mmax - mmin) != 0 else 1.0
            w_quality = {t: max(0.0, (float(quality_raw.get(t, mmin)) - mmin) / denom) for t in uniq}
            if sum(w_quality.values()) <= 0:
                w_quality = w_equal.copy()
            else:
                s = sum(w_quality.values())
                w_quality = {t: v / s for t, v in w_quality.items()}

        # 30% consolidação (proxy: market cap + estabilidade via volatilidade)
        cons_raw = {t: 0.0 for t in uniq}
        try:
            # market cap proxy via multiplos
            mcap = {}
            for t in uniq:
                dfm = load_multiplos_from_db(_norm_sa(t))
                if dfm is None or dfm.empty:
                    continue
                dfm = _clean_columns(dfm)
                col_m = None
                for c in ["MarketCap", "Valor_de_Mercado", "Valor de Mercado", "market_cap", "marketcap"]:
                    if c in dfm.columns:
                        col_m = c
                        break
                if col_m:
                    v = pd.to_numeric(dfm[col_m], errors="coerce").dropna()
                    if not v.empty:
                        mcap[t] = float(v.iloc[-1])
            # volatilidade proxy via preços (se disponível no acumulador global)
            vol = {}
            try:
                if precos_global_parts:
                    pg = pd.concat(precos_global_parts, axis=1)
                    pg.index = pd.to_datetime(pg.index, errors="coerce")
                    pg = pg.dropna(how="all")
                    for t in uniq:
                        col = _norm_sa(t)
                        if col in pg.columns:
                            s = pd.to_numeric(pg[col], errors="coerce").dropna()
                            if len(s) >= 60:
                                r = np.log(s).diff().dropna()
                                vol[t] = float(r.tail(252).std()) if len(r) >= 10 else float(r.std())
            except Exception:
                vol = {}

            mcap_vals = np.array([mcap.get(t, 0.0) for t in uniq], dtype=float)
            vol_vals = np.array([vol.get(t, np.nan) for t in uniq], dtype=float)
            mcap_rank = pd.Series(mcap_vals).rank(pct=True).to_numpy()
            invvol = np.where(np.isfinite(vol_vals) & (vol_vals > 0), 1.0 / vol_vals, np.nan)
            invvol_rank = pd.Series(invvol).rank(pct=True).fillna(0.5).to_numpy()
            for i, t in enumerate(uniq):
                cons_raw[t] = float(0.7 * mcap_rank[i] + 0.3 * invvol_rank[i])
        except Exception:
            cons_raw = {t: 1.0 for t in uniq}

        if sum(cons_raw.values()) <= 0:
            w_cons = w_equal.copy()
        else:
            s = sum(cons_raw.values())
            w_cons = {t: float(cons_raw.get(t, 0.0)) / s for t in uniq}

        # mistura final e cap
        peso_sugerido = {}
        for t in uniq:
            peso_sugerido[t] = 0.40 * w_equal.get(t, 0.0) + 0.30 * w_cons.get(t, 0.0) + 0.30 * w_quality.get(t, 0.0)

        peso_sugerido = _cap_redistribute(peso_sugerido, cap=0.30)

        for e in empresas_lideres_finais:
            tk = str(e.get("ticker") or "").strip().upper().replace(".SA", "")
            if tk:
                e["peso_sugerido"] = float(peso_sugerido.get(tk, 0.0))


    # ─────────────────────────────────────────────────────────
    # Bloco final: líderes para o próximo ano + distribuição setorial
    # ─────────────────────────────────────────────────────────
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
        setores_portfolio = pd.Series([e["setor"] for e in empresas_lideres_finais]).value_counts()
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
        st.markdown("## 📊 Desempenho parcial das líderes (ano atual)")

        ano_corrente = datetime.now().year
        tickers_corrente = [e["ticker"] for e in empresas_lideres_finais if int(e["ano_compra"]) == ano_corrente]

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

            patrimonio_aporte = gerir_carteira_simples(precos, tickers_limpos, datas_aporte, dividendos_dict=dividendos_dict)

            # Selic benchmark (no mesmo índice do patrimônio da estratégia)
            df_selic = calcular_patrimonio_selic_macro(dados_macro, datas_aporte)
            if df_selic is None or df_selic.empty:
                st.warning("⚠️ Não foi possível calcular o benchmark Selic para o período.")
                st.stop()

            df_selic = df_selic.reindex(patrimonio_aporte.index).ffill()

            df_final = pd.concat(
                [
                    patrimonio_aporte.rename("Estratégia de Aporte"),
                    df_selic,
                ],
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

            valor_estrategia_final = float(df_final["Estratégia de Aporte"].iloc[-1])
            valor_selic_final = float(df_final["Tesouro Selic"].iloc[-1])
            desempenho = ((valor_estrategia_final / valor_selic_final) - 1) * 100.0 if valor_selic_final > 0 else 0.0

            patrimonio_total_aplicado = 1000.0 * len(datas_aporte)
            retorno_estrategia = ((valor_estrategia_final / patrimonio_total_aplicado) - 1) * 100.0 if patrimonio_total_aplicado > 0 else 0.0

            if desempenho > 0:
                cor = "green"
                mensagem = f"A estratégia de aportes nas empresas líderes superou o Tesouro Selic em {desempenho:.2f}% no ano de {ano_corrente}."
            else:
                cor = "red"
                mensagem = f"A estratégia de aportes nas empresas líderes ficou {abs(desempenho):.2f}% abaixo do Tesouro Selic no ano de {ano_corrente}."

            st.markdown(
                f"""
                <div style="margin-top: 20px; padding: 15px; border-radius: 8px; background-color: #f9f9f9; border-left: 5px solid {cor};">
                    <h4 style="margin: 0;">📊 Resultado Comparativo</h4>
                    <p style="font-size: 16px; color: #333;">{mensagem}</p>
                    <p style="font-size: 14px; color: #666;">Retorno total da estratégia sobre o capital aportado no ano: <strong>{retorno_estrategia:.2f}%</strong></p>
                    <p style="font-size: 14px; color: #999;">Baseado nas empresas líderes selecionadas com score fundamentalista ajustado.</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ─────────────────────────────────────────────────────────
    # 
    # ─────────────────────────────────────────────────────────
    # PATCHES — Teste incremental (Patch 1, 2, 3, 4, 5)
    # (Rodam APÓS a construção do portfólio; nunca rodam em import)
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
    # Persistência do portfólio (snapshot) — Supabase
    # ─────────────────────────────────────────────────────────
    # filters_json: auditabilidade e reprodutibilidade
    filters_json = {
        "tipo_empresa": str(tipo_empresa),
        "selic_ref": float(selic_ref),
        "margem_superior": float(margem_superior),
        "use_score_v2": bool(use_score_v2),
        "min_anos": int(min_anos) if "min_anos" in locals() else None,
        "gate_modo": {"regra": ">=5 modulado / <=4 padrão"},
        "peso_sugerido_mix": {"equal": 0.40, "consolidacao": 0.30, "qualidade": 0.30, "cap": 0.30},
        "seleciona_1_ou_2": "lider_ultimo vs maior_participacao",
    }

    snapshot = {
        "tickers": [
            {
                "ticker": str(e.get("ticker") or "").strip().upper().replace(".SA", ""),
                "peso": float(e.get("peso_sugerido") if e.get("peso_sugerido") is not None else e.get("peso") or 0.0),
            }
            for e in empresas_lideres_finais
            if e.get("ticker")
        ],
        "selic_ref": float(selic_ref),
        "margem_superior": float(margem_superior),
        "tipo_empresa": str(tipo_empresa),
        "filters_json": filters_json,
        "created_at": datetime.now().isoformat(),
    }

    # plan_hash determinístico (não depende do timestamp)
    try:
        plan_payload = {
            "tickers": snapshot["tickers"],
            "selic_ref": snapshot["selic_ref"],
            "margem_superior": snapshot["margem_superior"],
            "tipo_empresa": snapshot["tipo_empresa"],
            "filters_json": snapshot["filters_json"],
        }
        payload_det = json.dumps(plan_payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        snapshot["plan_hash"] = hashlib.md5(payload_det.encode("utf-8")).hexdigest()
    except Exception:
        snapshot["plan_hash"] = ""

    # Compatibilidade (não é gating): mantém em sessão para debug
    st.session_state["portfolio_salvo"] = snapshot

    snap_id = None
    if save_snapshot is not None:
        try:
            snap_id = save_snapshot(
                tickers=snapshot["tickers"],
                selic_ref=snapshot["selic_ref"],
                margem_superior=snapshot["margem_superior"],
                tipo_empresa=snapshot["tipo_empresa"],
                hash_value=str(snapshot.get("plan_hash") or ""),
                filters_json=snapshot.get("filters_json") or {},
                notes=f"n_empresas={int(len(snapshot['tickers']))}",
            )
        except Exception as e:
            st.warning(f"Não foi possível salvar snapshot no Supabase: {type(e).__name__}: {e}")

    if snap_id:
        st.success(f"Snapshot salvo no Supabase: {snap_id}")
    else:
        st.info("Snapshot não persistido (store indisponível). Patch 6 ficará bloqueado se não houver snapshot salvo.")


    st.session_state["cp_should_run"] = False
    st.markdown("<hr>", unsafe_allow_html=True)
