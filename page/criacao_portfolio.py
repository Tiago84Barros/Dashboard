from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Sequence, Tuple

import pandas as pd
import numpy as np
import streamlit as st
import matplotlib.pyplot as plt

# yfinance (opcional) — usado para pesos por consolidação
try:
    import yfinance as yf  # type: ignore
except Exception:
    yf = None  # type: ignore

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

    # Patch 5 (novo): desempenho das empresas (Preço/DY + Lucros)
    try:
        from page.portfolio_patches import render_patch5_desempenho_empresas
    except Exception:
        render_patch5_desempenho_empresas = None  # type: ignore

except Exception:
    render_patch1_regua_conviccao = None  # type: ignore
    render_patch2_dominancia = None  # type: ignore
    render_patch3_diversificacao = None  # type: ignore
    render_patch4_benchmark_segmento = None  # type: ignore
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


def _filtrar_tickers_por_tipo(
    tickers: Sequence[str],
    tipo: str = "Estabelecida (≥10 anos)",
    max_workers: int = 12,
) -> List[str]:
    """
    Filtra tickers com base no histórico de DRE (nº de anos distintos na coluna Data).

    Regras (compatível com o gate binário do advanced.py):
      - "Todas"                   -> anos >= 1
      - "Crescimento (<10 anos)"  -> 1 <= anos < 10
      - "Estabelecida (≥10 anos)" -> anos >= 10
    """
    tickers = [_strip_sa(t) for t in tickers if (t or "").strip()]
    if not tickers:
        return []

    tipo = (tipo or "").strip()
    if tipo == "Crescimento (<10 anos)":
        min_anos, max_anos = 1, 9
    elif tipo == "Todas":
        min_anos, max_anos = 1, None
    else:
        min_anos, max_anos = 10, None

    def _check(tk: str) -> Tuple[str, int]:
        dre = load_data_from_db(_norm_sa(tk))
        return tk, _safe_year_count_from_dre(dre)

    ok: List[str] = []
    max_workers2 = min(int(max_workers), max(2, len(tickers)))
    with ThreadPoolExecutor(max_workers=max_workers2) as ex:
        futs = {ex.submit(_check, tk): tk for tk in tickers}
        for fut in as_completed(futs):
            tk, years = fut.result()
            if years < int(min_anos):
                continue
            if max_anos is not None and years > int(max_anos):
                continue
            ok.append(tk)

    return sorted(set(ok))


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
# Pesos por Consolidação (tamanho + liquidez + estabilidade)
# ─────────────────────────────────────────────────────────────

def _rank01(values: List[float]) -> List[float]:
    """Rank percentil simples (0..1) preservando ordem original."""
    if not values:
        return []
    s = pd.Series(values, dtype="float64")
    ranks = s.rank(pct=True, method="average").fillna(0.0)
    return [float(x) for x in ranks.tolist()]

def _calc_consolidation_scores(tickers: List[str], precos_global: Optional[pd.DataFrame] = None) -> Tuple[dict, dict]:
    """Retorna scores (0..1) e meta por ticker."""
    tickers = [_strip_sa(t) for t in (tickers or []) if (t or "").strip()]
    tickers = list(dict.fromkeys(tickers))
    if not tickers:
        return {}, {}

    mcap_list, volm_list, invvol_list = [], [], []
    meta: dict = {}

    def _get_close_series(tk: str) -> pd.Series:
        if precos_global is not None and not precos_global.empty:
            cols = [c for c in precos_global.columns if _strip_sa(str(c)) == tk]
            if cols:
                return pd.to_numeric(precos_global[cols[0]], errors="coerce").dropna()
        if yf is None:
            return pd.Series(dtype=float)
        try:
            h = yf.Ticker(_norm_sa(tk)).history(period="5y", auto_adjust=False)
            if h is None or h.empty or "Close" not in h.columns:
                return pd.Series(dtype=float)
            return pd.to_numeric(h["Close"], errors="coerce").dropna()
        except Exception:
            return pd.Series(dtype=float)

    def _get_vol_medio(tk: str) -> float:
        if yf is None:
            return float("nan")
        try:
            h = yf.Ticker(_norm_sa(tk)).history(period="3mo", auto_adjust=False)
            if h is None or h.empty or "Volume" not in h.columns:
                return float("nan")
            v = pd.to_numeric(h["Volume"], errors="coerce").dropna()
            return float(v.tail(60).mean()) if not v.empty else float("nan")
        except Exception:
            return float("nan")

    def _get_mcap(tk: str) -> float:
        if yf is None:
            return float("nan")
        try:
            info = getattr(yf.Ticker(_norm_sa(tk)), "info", {}) or {}
            m = info.get("marketCap", None)
            return float(m) if m is not None else float("nan")
        except Exception:
            return float("nan")

    for tk in tickers:
        close = _get_close_series(tk)
        vol = float("nan")
        if close.shape[0] > 40:
            rets = close.pct_change().dropna()
            if not rets.empty:
                vol = float(rets.std() * np.sqrt(252))
        invvol = (1.0 / vol) if (vol == vol and vol > 0) else float("nan")

        volm = _get_vol_medio(tk)
        mcap = _get_mcap(tk)

        meta[tk] = {"market_cap": mcap, "vol": vol, "vol_med": volm}
        mcap_list.append(mcap); volm_list.append(volm); invvol_list.append(invvol)

    mcap_r = _rank01(mcap_list)
    volm_r = _rank01(volm_list)
    invvol_r = _rank01(invvol_list)

    scores = {}
    for i, tk in enumerate(tickers):
        score = 0.45 * mcap_r[i] + 0.35 * volm_r[i] + 0.20 * invvol_r[i]
        scores[tk] = float(score)

    svals = pd.Series(list(scores.values()), dtype="float64")
    q1 = float(svals.quantile(0.33)) if not svals.empty else 0.33
    q2 = float(svals.quantile(0.66)) if not svals.empty else 0.66

    for tk, sc in scores.items():
        lvl = "Alta" if sc >= q2 else ("Média" if sc >= q1 else "Baixa")
        meta[tk]["level"] = lvl

    return scores, meta

def _mix_weights_equal_and_score(scores: dict, mix_equal: float = 0.50) -> dict:
    keys = list(scores.keys())
    if not keys:
        return {}
    n = len(keys)
    equal = 1.0 / max(1, n)
    s = pd.Series([float(scores.get(k, 0.0) or 0.0) for k in keys], index=keys, dtype="float64")
    s = s.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    total = float(s.sum())
    if total <= 0:
        return {k: equal for k in keys}
    s_norm = s / total
    w = mix_equal * equal + (1.0 - mix_equal) * s_norm
    w = w / float(w.sum()) if float(w.sum()) > 0 else w
    return {k: float(w[k]) for k in keys}


def _apply_cap(weights: dict, cap: float = 0.30) -> dict:
    """Aplica teto por ativo e redistribui excedente proporcionalmente entre os demais."""

    w = {k: float(v or 0.0) for k, v in (weights or {}).items()}
    if not w:
        return w
    # normaliza
    s = sum(w.values())
    if s > 0:
        w = {k: v / s for k, v in w.items()}

    # redistribuição iterativa
    for _ in range(10):  # converge rápido
        over = {k: v for k, v in w.items() if v > cap}
        if not over:
            break
        excess = sum(v - cap for v in over.values())
        for k in over:
            w[k] = cap
        under_keys = [k for k, v in w.items() if v < cap - 1e-12]
        if not under_keys or excess <= 0:
            break
        under_sum = sum(w[k] for k in under_keys)
        if under_sum <= 0:
            # fallback: distribui igual entre under
            add = excess / len(under_keys)
            for k in under_keys:
                w[k] += add
        else:
            for k in under_keys:
                w[k] += excess * (w[k] / under_sum)

    # normaliza final
    s = sum(w.values())
    if s > 0:
        w = {k: v / s for k, v in w.items()}
    return w


def _mix_weights_structural(
    tickers: List[str],
    scores_cons: dict,
    scores_quality: dict,
    mix_equal: float = 0.40,
    mix_cons: float = 0.30,
    mix_score: float = 0.30,
    cap: float = 0.30,
) -> dict:
    """Peso = mix_equal*igual + mix_cons*consolidação + mix_score*score (com cap)."""

    tickers = [_strip_sa(t) for t in (tickers or []) if (t or "").strip()]
    tickers = list(dict.fromkeys(tickers))
    n = len(tickers)
    if n == 0:
        return {}

    equal = 1.0 / n

    s_cons = pd.Series({t: float(scores_cons.get(t, 0.0) or 0.0) for t in tickers}, dtype="float64")
    s_qual = pd.Series({t: float(scores_quality.get(t, 0.0) or 0.0) for t in tickers}, dtype="float64")

    s_cons = s_cons.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    s_qual = s_qual.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    s_cons = (s_cons / float(s_cons.sum())) if float(s_cons.sum()) > 0 else s_cons
    s_qual = (s_qual / float(s_qual.sum())) if float(s_qual.sum()) > 0 else s_qual

    w = pd.Series({t: mix_equal * equal for t in tickers}, dtype="float64")
    w = w + mix_cons * s_cons + mix_score * s_qual

    total = float(w.sum())
    if total > 0:
        w = w / total

    w_dict = {t: float(w[t]) for t in tickers}
    w_dict = _apply_cap(w_dict, cap=float(cap))
    return w_dict


# ─────────────────────────────────────────────────────────────
# Render Streamlit
# ─────────────────────────────────────────────────────────────

def render():
    st.markdown("<h1 style='text-align: center;'>Criação de Portfólio</h1>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────────
    # CONTROLES DE EXECUÇÃO (não roda sozinho)
    # ─────────────────────────────────────────────────────────────
    if "cp_last_params" not in st.session_state:
        st.session_state["cp_last_params"] = {"margem_superior": 10.0, "use_score_v2": False, "tipo_empresa": "Estabelecida (≥10 anos)", }
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

            tipo_empresa = st.selectbox(
                "Perfil de empresa (histórico DRE)",
                ["Estabelecida (≥10 anos)", "Crescimento (<10 anos)", "Todas"],
                index=["Estabelecida (≥10 anos)", "Crescimento (<10 anos)", "Todas"].index(
                    str(st.session_state["cp_last_params"].get("tipo_empresa", "Estabelecida (≥10 anos)"))
                ),
            )

            gerar = st.form_submit_button("🚀 Rodar Criação de Portfólio")

        if gerar:
            st.session_state["cp_last_params"] = {
                "margem_superior": float(margem_superior),
                "use_score_v2": bool(use_score_v2),
                "tipo_empresa": str(tipo_empresa),
            }
            st.session_state["cp_should_run"] = True

    if not st.session_state["cp_should_run"]:
        st.info("Defina a margem (%) na barra lateral e clique em **🚀 Rodar Criação de Portfólio**.")
        return

    # parâmetro efetivo usado na execução
    margem_superior = float(st.session_state["cp_last_params"]["margem_superior"])
    use_score_v2 = bool(st.session_state["cp_last_params"]["use_score_v2"])
    tipo_empresa = str(st.session_state["cp_last_params"].get("tipo_empresa", "Estabelecida (≥10 anos)"))
# policy calibrada do modo "Ajuste Calibrado" (auto-tuning)
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

        # filtro de histórico (tipo) — este filtro define o "n" do gate binário
        tickers_validos = _filtrar_tickers_por_tipo(tickers_segmento, tipo=tipo_empresa, max_workers=12)
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

        usar_calibrado = bool(n_total_segmento >= 5)
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
        modo_txt = "Ajuste Calibrado (auto-tuning)" if usar_calibrado else "Modelo Padrão (aportes iguais)"
        st.caption(f"Modo aplicado: {modo_txt} | n (pós-filtro) = {n_total_segmento} | tipo = {tipo_empresa}")
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
</div>
                """,
                unsafe_allow_html=True,
            )

        # ── Seleção automática (Líder do último ano + Maior participação)
        # Regra:
        # 1) Se líder do último ano == maior participação → entra apenas 1
        # 2) Se diferente → entram 2 (líder do último ano e maior participação)
        ultimo_ano = int(pd.to_numeric(score["Ano"], errors="coerce").max())

        # líder do último ano (do score)
        try:
            lideres_ano = lideres[pd.to_numeric(lideres["Ano"], errors="coerce") == ultimo_ano]
            ticker_lider_ultimo = _strip_sa(str(lideres_ano.iloc[0]["ticker"]))
        except Exception:
            ticker_lider_ultimo = _strip_sa(str(lideres.iloc[0]["ticker"]))

        # maior participação no último ponto do backtest do segmento
        last_row = patrimonio_empresas.iloc[-1].drop("Patrimônio", errors="ignore").dropna()
        ticker_maior_part = _strip_sa(str(last_row.sort_values(ascending=False).index[0]))

        if ticker_lider_ultimo == ticker_maior_part:
            tickers_sel = [ticker_lider_ultimo]
        else:
            tickers_sel = [ticker_lider_ultimo, ticker_maior_part]

        tickers_sel = list(dict.fromkeys([t for t in tickers_sel if t]))

        def _value_for(tk: str) -> float:
            if tk in last_row.index:
                return float(last_row[tk])
            if (tk + ".SA") in last_row.index:
                return float(last_row[tk + ".SA"])
            return float("nan")

        valores_sel = [_value_for(t) for t in tickers_sel]
        total_sel = float(np.nansum(valores_sel)) if valores_sel else 0.0

        for tk in tickers_sel:
            v = _value_for(tk)
            peso_part = (v / total_sel) if total_sel > 0 and pd.notna(v) else 0.0

            is_lider_ultimo = (tk == ticker_lider_ultimo)
            motivo_select = "LÍDER" if is_lider_ultimo else "GRANDE_PARTICIPACAO"

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
                    "participacao_backtest": float(peso_part),
                    "is_lider_ultimo": bool(is_lider_ultimo),
                    "motivo_select": str(motivo_select),
                    "peso": float(peso_part),
                }
            )


    # Bloco final: líderes para o próximo ano + distribuição setorial
    # ─────────────────────────────────────────────────────────
    # ─────────────────────────────────────────────────────────
    # Recalcula peso sugerido por consolidação + Score (com CAP 30%)
    # ─────────────────────────────────────────────────────────
    try:
        tickers_finais = [_strip_sa(e.get("ticker","")) for e in empresas_lideres_finais]
        tickers_finais = [t for t in tickers_finais if t]
        precos_global_df = pd.concat(precos_global_parts, axis=1) if precos_global_parts else None

        # 1) Consolidação (mercado)
        scores_cons, meta_cons = _calc_consolidation_scores(tickers_finais, precos_global=precos_global_df)

        # 2) Qualidade (Score) — extrai do score_global_parts (último ano disponível)
        scores_quality = {}
        try:
            if score_global_parts:
                dfq = pd.concat(score_global_parts, ignore_index=True)
                # garante ticker normalizado
                dfq["_TK"] = dfq["ticker"].astype(str).map(_strip_sa)
                # restringe ao conjunto final (peso não pode alterar seleção/liderança)
                dfq = dfq[dfq["_TK"].isin(tickers_finais)].copy()
                dfq["_ANO"] = pd.to_numeric(dfq["Ano"], errors="coerce")
                ultimo_ano_q = int(dfq["_ANO"].max())
                dfq = dfq[dfq["_ANO"] == ultimo_ano_q].copy()

                # escolhe melhor coluna de score disponível
                cand_cols = [c for c in dfq.columns if str(c).lower() in ("score", "score_total", "scorefinal", "score_final", "pontuacao", "pontuação")]
                score_col = cand_cols[0] if cand_cols else None

                if score_col is not None:
                    dfq["_SRAW"] = pd.to_numeric(dfq[score_col], errors="coerce")
                else:
                    # fallback: soma de colunas numéricas (exceto Ano)
                    num = dfq.select_dtypes(include="number").copy()
                    for dropc in ["Ano", "_ANO"]:
                        if dropc in num.columns:
                            num = num.drop(columns=[dropc], errors="ignore")
                    dfq["_SRAW"] = num.sum(axis=1) if not num.empty else np.nan

                # agrega por ticker
                g = dfq.groupby("_TK")["_SRAW"].mean()
                # normaliza para positivo
                g = g.replace([np.inf, -np.inf], np.nan).dropna()
                if not g.empty:
                    mn = float(g.min())
                    g = (g - mn) + 1e-9
                    scores_quality = {k: float(v) for k, v in g.to_dict().items()}
        except Exception:
            scores_quality = {}

        # 3) Mistura estrutural + cap
        pesos_final = _mix_weights_structural(
            tickers=tickers_finais,
            scores_cons=scores_cons or {},
            scores_quality=scores_quality or {},
            mix_equal=0.40,
            mix_cons=0.30,
            mix_score=0.30,
            cap=0.30,
        ) if tickers_finais else {}

        for e in empresas_lideres_finais:
            tk = _strip_sa(e.get("ticker",""))
            if tk in pesos_final:
                e["peso"] = float(pesos_final[tk])

            lvl = (meta_cons.get(tk, {}) or {}).get("level", None)
            if lvl:
                e["nivel_consolidacao"] = str(lvl)

    except Exception:
        pass


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
                    <p style="font-size: 12px; color: #333;"><b>Segmento:</b> {emp.get('segmento','')}</p>
                    <p style="font-size: 12px; color: #333;">
                        {("Líder em " + str(emp.get('ano_lider')) + "<br>Para compra em " + str(emp.get('ano_compra'))) if emp.get('is_lider_ultimo') else "Grande participação no segmento"}
                    </p>
                    <p style="font-size: 12px; color: #2c3e50;"><b>Peso sugerido para compra:</b> {emp.get('peso',0.0)*100:.1f}%</p>
                    <p style="font-size: 12px; color: #2c3e50;"><b>Nível de consolidação:</b> {emp.get('nivel_consolidacao','—')}</p>
                </div>
                """
,
                unsafe_allow_html=True,
            )

        st.markdown("## 📊 Distribuição setorial do portfólio sugerido")
        setores_portfolio = pd.Series([e.get('peso', 1.0) for e in empresas_lideres_finais], index=[e.get('setor','OUTROS') for e in empresas_lideres_finais]).groupby(level=0).sum().sort_values(ascending=False)
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
    # PATCHES — Teste incremental (Patch 1, 2 e 3)
    # (Rodam APÓS a construção do portfólio; nunca rodam em import)
    # ─────────────────────────────────────────────────────────
    try:
        score_global = pd.concat(score_global_parts, ignore_index=True) if score_global_parts else pd.DataFrame()
        lideres_global = pd.concat(lideres_global_parts, ignore_index=True) if lideres_global_parts else pd.DataFrame()
    except Exception:
        score_global = pd.DataFrame()
        lideres_global = pd.DataFrame()

    # Preços globais (para Patch 5) — sem baixar novamente
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

        if render_patch3_diversificacao is not None and empresas_lideres_finais:
            with st.expander("🧩 Patch 3 — Diversificação e Concentração de Risco", expanded=False):
                try:
                    # contrib_globais é opcional; aqui usamos apenas pesos iguais por padrão
                    render_patch3_diversificacao(empresas_lideres_finais, contrib_globais=None)
                except Exception as e:
                    st.error(f"Patch 3 falhou: {type(e).__name__}: {e}")

        if render_patch4_benchmark_segmento is not None and empresas_lideres_finais:
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



        if 'render_patch5_desempenho_empresas' in globals() and render_patch5_desempenho_empresas is not None:
            with st.expander("🧩 Patch 5 — Desempenho das Empresas (Preço/DY + Lucros)", expanded=False):
                try:
                    render_patch5_desempenho_empresas(empresas_lideres_finais, df_prices_global, score_global=score_global, dividendos=dividendos)
                except Exception as e:
                    st.error(f"Patch 5 falhou: {type(e).__name__}: {e}")

    # Desarma a execução após rodar (evita “auto-rerun armado”)
    st.session_state["cp_should_run"] = False

    st.markdown("<hr>", unsafe_allow_html=True)
