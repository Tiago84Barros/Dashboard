from __future__ import annotations

"""
core/scoring_v2.py

Score v2 (robusto) para seleção comparativa intra-setor/segmento.

Principais melhorias (validadas pelo Especialista em Finanças):
- Normalização por percentil dentro do grupo (cross-sectional).
- Fallback automático do grupo: SEGMENTO -> SUBSETOR -> SETOR se amostra pequena (mediana < min_n).
- Penalidade de instabilidade baseada em Coeficiente de Variação (sigma/|mu|) em janela histórica (5-8 anos, default 8),
  aplicada como percentil dentro do grupo, progressiva e com cap (nunca zera score).
- Crowding aplicado após score consolidado, com cap de penalidade máxima.
- Decay anual leve aplicado ao score final, com cap.

Compatibilidade:
- Mantém a filosofia do core/scoring.py (usa hooks existentes quando possível).
- Funções públicas:
  - calcular_score_ajustado_v2(...)
  - calcular_score_acumulado_v2(...)
"""

import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

# Reuso de helpers do scoring v1 (mantém consistência)
from core.scoring import (
    _ensure_year,
    _to_numeric_series,
    calc_crowding_penalty,
    calcular_metricas_historicas_simplificadas,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Config defaults
# ─────────────────────────────────────────────────────────────

DEFAULT_MIN_N_GROUP = 7

DEFAULT_INSTABILITY_WINDOW_YEARS = 8
DEFAULT_INSTABILITY_CAP = 0.25        # desconto máximo 25%
DEFAULT_INSTABILITY_STRENGTH = 0.60   # intensidade (0..1). 0.60 => moderado
DEFAULT_INSTABILITY_POWER = 1.5       # curva progressiva (>=1)

DEFAULT_CROWDING_MIN_FACTOR = 0.80    # no mínimo 0.80 => penalidade máxima 20%
DEFAULT_DECAY_PER_YEAR = 0.07         # 7% ao ano
DEFAULT_DECAY_CAP = 0.30              # máximo 30%


# ─────────────────────────────────────────────────────────────
# Utilitários
# ─────────────────────────────────────────────────────────────

def winsorize_series(s: pd.Series, p_low: float = 0.05, p_high: float = 0.95) -> pd.Series:
    """Corta extremos por percentis, preservando NaN."""
    x = _to_numeric_series(s)
    if x.dropna().empty:
        return x
    lo = float(x.quantile(p_low))
    hi = float(x.quantile(p_high))
    if not np.isfinite(lo) or not np.isfinite(hi) or lo >= hi:
        return x
    return x.clip(lower=lo, upper=hi)


def percentile_score(series: pd.Series, melhor_alto: bool) -> pd.Series:
    """
    Score ∈ [0,1] via rank percentil.
    NaN -> 0.5 (neutro).
    """
    x = _to_numeric_series(series)
    pct = x.rank(pct=True, method="average").fillna(0.5)
    return pct if melhor_alto else (1.0 - pct)


def _median_group_size(df: pd.DataFrame, col: str) -> float:
    try:
        sizes = df.groupby(col, dropna=False).size()
        if sizes.empty:
            return 0.0
        return float(sizes.median())
    except Exception:
        return 0.0


def resolve_group_col(df: pd.DataFrame, prefer: str = "SEGMENTO", min_n: int = DEFAULT_MIN_N_GROUP) -> str:
    """
    Usa o nível mais granular com amostra suficiente.
    Ordem: prefer (geralmente SEGMENTO) -> SUBSETOR -> SETOR.
    Critério: mediana do tamanho dos grupos >= min_n.
    """
    for col in [prefer, "SUBSETOR", "SETOR"]:
        if col in df.columns:
            if _median_group_size(df, col) >= float(min_n):
                return col
    # fallback final
    if "SETOR" in df.columns:
        return "SETOR"
    return prefer


def _pct_within_group(df: pd.DataFrame, by: str, col: str) -> pd.Series:
    """Percentil (rank pct=True) por grupo; NaN -> 0.5."""
    return df.groupby(by, dropna=False)[col].transform(
        lambda s: _to_numeric_series(s).rank(pct=True, method="average").fillna(0.5)
    )


# ─────────────────────────────────────────────────────────────
# Instabilidade (CV σ/|μ|) em janela histórica
# ─────────────────────────────────────────────────────────────

def series_cv_from_yearly(df: pd.DataFrame, col: str, year_col: str = "Ano", window: int = DEFAULT_INSTABILITY_WINDOW_YEARS) -> float:
    """
    CV (σ/|μ|) na janela dos últimos N anos.
    """
    if df is None or df.empty or col not in df.columns or year_col not in df.columns:
        return 0.0

    x = df[[year_col, col]].copy()
    x[col] = pd.to_numeric(x[col], errors="coerce")
    x[year_col] = pd.to_numeric(x[year_col], errors="coerce")

    x = x.dropna(subset=[year_col, col])
    if x.empty:
        return 0.0

    x = x.sort_values(year_col).tail(int(window))
    mu = float(x[col].mean())
    sig = float(x[col].std(ddof=0))
    cv = float(sig / (abs(mu) + 1e-9))

    if not np.isfinite(cv):
        return 0.0
    return max(cv, 0.0)


def compute_instability_cv(
    df_dre_hist: pd.DataFrame,
    candidate_cols: Sequence[str],
    year_col: str = "Ano",
    window: int = DEFAULT_INSTABILITY_WINDOW_YEARS,
) -> float:
    """
    Calcula instabilidade como média do CV em colunas candidatas.
    Se não houver colunas disponíveis, retorna 0.0 (neutro).
    """
    cvs: List[float] = []
    for col in candidate_cols:
        if col in df_dre_hist.columns:
            cvs.append(series_cv_from_yearly(df_dre_hist, col, year_col=year_col, window=window))
    if not cvs:
        return 0.0
    return float(np.mean(cvs))


def apply_instability_penalty(
    df_ano: pd.DataFrame,
    group_col: str,
    cap: float = DEFAULT_INSTABILITY_CAP,
    strength: float = DEFAULT_INSTABILITY_STRENGTH,
    power: float = DEFAULT_INSTABILITY_POWER,
) -> pd.DataFrame:
    """
    Cria InstabilityPenalty ∈ [0, cap] baseado no percentil do Instability_CV dentro do grupo.
    Penalidade progressiva (pct^power), com cap e escala via strength.
    """
    out = df_ano.copy()
    if "Instability_CV" not in out.columns:
        out["Instability_CV"] = 0.0

    out["Instability_CV"] = pd.to_numeric(out["Instability_CV"], errors="coerce").fillna(0.0)

    # percentil por grupo: instabilidade maior => percentil maior
    out["_inst_pct"] = _pct_within_group(out, group_col, "Instability_CV")

    # penalidade progressiva e limitada
    pen = (out["_inst_pct"] ** float(power)) * float(cap) * float(strength)
    out["InstabilityPenalty"] = pd.to_numeric(pen, errors="coerce").fillna(0.0).clip(0.0, float(cap))

    out.drop(columns=["_inst_pct"], inplace=True, errors="ignore")
    return out


# ─────────────────────────────────────────────────────────────
# Score v2: agregação por pesos usando percentil por grupo
# ─────────────────────────────────────────────────────────────

def calcular_score_ajustado_v2(
    df: pd.DataFrame,
    pesos_utilizados: Mapping[str, Mapping[str, Any]],
    prefer_group_col: str = "SEGMENTO",
    min_n_group: int = DEFAULT_MIN_N_GROUP,
    winsor_p_low: float = 0.05,
    winsor_p_high: float = 0.95,
) -> pd.DataFrame:
    """
    Calcula Score_Ajustado v2 (somente a parte fundamental cross-sectional):
    - winsoriza indicadores
    - percentil por grupo (com fallback automático se amostra pequena)
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    # seleciona coluna de grupo robustamente
    group_col = resolve_group_col(out, prefer=prefer_group_col, min_n=min_n_group)
    if group_col not in out.columns:
        out[group_col] = "OUTROS"

    out["Score_Ajustado"] = 0.0

    for col, cfg in pesos_utilizados.items():
        if col not in out.columns:
            continue

        peso = float(cfg.get("peso", 0.0))
        if peso <= 0:
            continue

        melhor_alto = bool(cfg.get("melhor_alto", True))

        # winsoriza antes de ranquear
        out[col] = winsorize_series(out[col], p_low=winsor_p_low, p_high=winsor_p_high)

        # percentil intra-grupo
        pct_col = f"{col}_pct"
        out[pct_col] = out.groupby(group_col, dropna=False)[col].transform(
            lambda s, m=melhor_alto: percentile_score(s, melhor_alto=m)
        )

        out["Score_Ajustado"] += out[pct_col] * peso

    return out


# ─────────────────────────────────────────────────────────────
# Score acumulado v2 (mesma filosofia do v1 + robustez validada)
# ─────────────────────────────────────────────────────────────

def calcular_score_acumulado_v2(
    lista_empresas: Sequence[Mapping[str, Any]],
    group_map: Mapping[str, str],
    pesos_utilizados: Mapping[str, Mapping[str, Any]],
    anos_minimos: int = 4,
    prefer_group_col: str = "SEGMENTO",
    min_n_group: int = DEFAULT_MIN_N_GROUP,
    winsor_p_low: float = 0.05,
    winsor_p_high: float = 0.95,
    # Instabilidade
    instability_window_years: int = DEFAULT_INSTABILITY_WINDOW_YEARS,
    instability_cap: float = DEFAULT_INSTABILITY_CAP,
    instability_strength: float = DEFAULT_INSTABILITY_STRENGTH,
    instability_power: float = DEFAULT_INSTABILITY_POWER,
    # Crowding + decay
    crowding_min_factor: float = DEFAULT_CROWDING_MIN_FACTOR,
    decay_per_year: float = DEFAULT_DECAY_PER_YEAR,
    decay_cap: float = DEFAULT_DECAY_CAP,
    # Hierarquia opcional (para fallback SEGMENTO->SUBSETOR->SETOR)
    subsetor_map: Optional[Mapping[str, str]] = None,
    setor_map: Optional[Mapping[str, str]] = None,
    # [LAG] Anti look-ahead: réplica exata do publication_lag_years do v1.
    # 0 = comportamento original sem lag (não recomendado para backtests).
    # 1 = default correto: score do ano N usa apenas dados até N-1.
    publication_lag_years: int = 1,
) -> pd.DataFrame:
    """
    Versão v2 do acumulado:
    - Score fundamental por percentil intra-grupo (com fallback quando n pequeno)
    - Penalidade de instabilidade por CV em janela histórica (8 anos default),
      aplicada como percentil intra-grupo, progressiva e com cap.
    - Crowding + Decay mantidos e aplicados no score final (com caps).
    - publication_lag_years (default=1): corte anti look-ahead — score rotulado
      como ano N é calculado com dados até ano N-lag, replicando o v1.

    Observação: para o fallback SEGMENTO->SUBSETOR->SETOR funcionar plenamente,
    passe subsetor_map e setor_map (ticker -> SUBSETOR/SETOR). Se não passar,
    o fallback opera somente com as colunas disponíveis (pode ficar no SEGMENTO).
    """
    if not lista_empresas:
        return pd.DataFrame(columns=["Ano", "ticker", "Score_Ajustado"])

    subsetor_map = subsetor_map or {}
    setor_map = setor_map or {}

    # [LAG] Garante lag >= 0; rejeita valores negativos silenciosamente.
    lag = int(max(publication_lag_years, 0))

    # Descobrir anos disponíveis a partir de múltiplos
    anos: List[int] = []
    for emp in lista_empresas:
        dfm = emp.get("multiplos")
        if isinstance(dfm, pd.DataFrame) and not dfm.empty:
            dfm2 = _ensure_year(dfm, date_col="Data", year_col="Ano")
            for a in dfm2["Ano"].dropna().unique():
                try:
                    ai = int(a)
                    if np.isfinite(ai):
                        anos.append(ai)
                except Exception:
                    continue

    anos_disponiveis = sorted(set(anos))
    if len(anos_disponiveis) <= anos_minimos:
        return pd.DataFrame(columns=["Ano", "ticker", "Score_Ajustado"])

    resultados: List[pd.DataFrame] = []
    anos_lider: Dict[str, int] = {}

    # Colunas candidatas para instabilidade (no histórico de múltiplos/DRE)
    instability_candidates = [
        "ROIC",              "ROIC_mean",
        "Margem_Operacional","Margem_Operacional_mean",
        "Margem_Liquida",    "Margem_Liquida_mean",
        # Novos — instabilidade de caixa e eficiência
        "Margem_FCO",        "Margem_FCO_mean",
        "FCO_sobre_Divida",  "FCO_sobre_Divida_mean",
        "Giro_Ativo",        "Giro_Ativo_mean",
    ]

    for idx in range(anos_minimos, len(anos_disponiveis)):
        ano = int(anos_disponiveis[idx])

        # [LAG] ano_cutoff: dados disponíveis para o investidor no momento da
        # decisão referente ao ano N. Com lag=1, usa apenas dados até N-1,
        # evitando que demonstrações publicadas em N entrem no score de N.
        # Réplica exata da lógica do v1 (scoring.py, linha ~261).
        ano_cutoff = ano - lag

        # Pré-coleta P/VP por grupo no ano (para crowding)
        grupo_to_pvp: Dict[str, List[float]] = {}

        for emp in lista_empresas:
            tk = str(emp.get("ticker", "")).strip()
            dfm = emp.get("multiplos")
            if not tk or not isinstance(dfm, pd.DataFrame) or dfm.empty:
                continue

            dfm2 = _ensure_year(dfm, date_col="Data", year_col="Ano")
            # [LAG] usa ano_cutoff para o crowding — consistente com o corte histórico abaixo
            dfm_ano = dfm2[dfm2["Ano"] == ano_cutoff] if lag > 0 else dfm2[dfm2["Ano"] == ano]
            if dfm_ano.empty:
                continue

            # tenta "P/VP" e "P_VP" por robustez
            pvp_col = "P/VP" if "P/VP" in dfm_ano.columns else ("P_VP" if "P_VP" in dfm_ano.columns else None)
            if not pvp_col:
                continue

            grp = group_map.get(tk, "OUTROS")
            vals = _to_numeric_series(dfm_ano[pvp_col]).dropna().tolist()
            grupo_to_pvp.setdefault(grp, []).extend([float(v) for v in vals if np.isfinite(v)])

        # Monta dataframe anual com métricas por empresa
        dados_ano: List[Dict[str, Any]] = []

        for emp in lista_empresas:
            ticker = str(emp.get("ticker", "")).strip()
            if not ticker:
                continue

            df_mult = emp.get("multiplos")
            df_dre = emp.get("dre")
            if not isinstance(df_mult, pd.DataFrame) or not isinstance(df_dre, pd.DataFrame):
                continue

            df_mult2 = _ensure_year(df_mult, date_col="Data", year_col="Ano")
            df_dre2 = _ensure_year(df_dre, date_col="Data", year_col="Ano")

            # [LAG] corte anti look-ahead: usa ano_cutoff em vez de ano.
            # Réplica da lógica do v1: `df_mult_hist = df_mult2[df_mult2["Ano"] <= cut]`
            df_mult_hist = df_mult2[df_mult2["Ano"] <= ano_cutoff].copy()
            df_dre_hist = df_dre2[df_dre2["Ano"] <= ano_cutoff].copy()

            if df_mult_hist.empty or df_dre_hist.empty:
                continue

            seg = group_map.get(ticker, "OUTROS")
            sub = subsetor_map.get(ticker, "OUTROS")
            setr = setor_map.get(ticker, "OUTROS")

            # crowding: baseado no P/VP do grupo (segmento preferencial)
            df_grp_ano = pd.DataFrame({"P/VP": grupo_to_pvp.get(seg, [])})
            crowd_pen = calc_crowding_penalty(df_grp_ano, coluna="P/VP")

            # métricas simplificadas (v1) para alimentar colunas usadas em pesos
            metricas = calcular_metricas_historicas_simplificadas(df_mult_hist, df_dre_hist)

            # instabilidade real (CV em janela)
            # Os candidatos (ROIC, Margens, FCO/Divida, Giro_Ativo…) estão em
            # df_mult_hist, não em df_dre_hist. Fazemos merge por Ano para cobrir
            # colunas de ambas as fontes sem perder séries históricas.
            _cand_set = set(instability_candidates)
            _mult_keep = ["Ano"] + [c for c in df_mult_hist.columns if c in _cand_set]
            _dre_keep  = ["Ano"] + [c for c in df_dre_hist.columns  if c in _cand_set]

            if len(_mult_keep) > 1 and len(_dre_keep) > 1:
                df_inst = pd.merge(
                    df_mult_hist[_mult_keep],
                    df_dre_hist[_dre_keep],
                    on="Ano", how="outer",
                )
            elif len(_mult_keep) > 1:
                df_inst = df_mult_hist[_mult_keep]
            elif len(_dre_keep) > 1:
                df_inst = df_dre_hist[_dre_keep]
            else:
                df_inst = df_mult_hist  # fallback: tenta tudo de multiplos

            instability_cv = compute_instability_cv(
                df_inst,
                candidate_cols=instability_candidates,
                year_col="Ano",
                window=instability_window_years,
            )

            dados_ano.append(
                {
                    "ticker": ticker,
                    "Ano": ano,
                    "SEGMENTO": seg,
                    "SUBSETOR": sub,
                    "SETOR": setr,
                    **metricas,
                    "Penalty_Crowd": float(crowd_pen),
                    "Instability_CV": float(instability_cv),
                }
            )

        df_ano = pd.DataFrame(dados_ano)
        if df_ano.empty:
            continue

        # (A) Score fundamental por percentil intra-grupo (com fallback automático)
        df_ano = calcular_score_ajustado_v2(
            df_ano,
            pesos_utilizados=pesos_utilizados,
            prefer_group_col=prefer_group_col,
            min_n_group=min_n_group,
            winsor_p_low=winsor_p_low,
            winsor_p_high=winsor_p_high,
        )

        # resolve group_col efetivo (mesma regra usada no ajustado)
        group_col_eff = resolve_group_col(df_ano, prefer=prefer_group_col, min_n=min_n_group)

        # (B) Penalidade de instabilidade: percentil por grupo, progressiva, cap
        df_ano = apply_instability_penalty(
            df_ano,
            group_col=group_col_eff,
            cap=instability_cap,
            strength=instability_strength,
            power=instability_power,
        )

        # (C) Seleciona líder e aplica crowding + decay no score final
        df_ano = df_ano.sort_values("Score_Ajustado", ascending=False).reset_index(drop=True)
        lider_ano = str(df_ano.loc[0, "ticker"])

        final_scores: List[float] = []
        decay_list: List[float] = []
        crowd_list: List[float] = []

        for _, row in df_ano.iterrows():
            tk = str(row["ticker"])

            # contagem de liderança consecutiva
            if tk == lider_ano:
                anos_lider[tk] = anos_lider.get(tk, 0) + 1
            else:
                anos_lider[tk] = 0

            # Decay anual leve (aplicado ao score final), com cap
            decay_factor = 1.0 - min(float(decay_per_year) * max(anos_lider[tk] - 1, 0), float(decay_cap))
            decay_factor = float(np.clip(decay_factor, 0.0, 1.0))

            # Crowding: aplicar após score consolidado, com cap mínimo
            crowd = float(row.get("Penalty_Crowd", 1.0))
            if not np.isfinite(crowd):
                crowd = 1.0
            crowd = max(crowd, float(crowding_min_factor))  # cap de penalidade máxima

            # Base score
            base = float(row.get("Score_Ajustado", 0.0))
            if not np.isfinite(base):
                base = 0.0

            # Penalidade de instabilidade (progressiva, cap) — aplicada ao score final
            inst_pen = float(row.get("InstabilityPenalty", 0.0))
            if not np.isfinite(inst_pen):
                inst_pen = 0.0
            inst_pen = float(np.clip(inst_pen, 0.0, float(instability_cap)))

            base = base * (1.0 - inst_pen)

            # Score final anual
            final = base * crowd * decay_factor

            decay_list.append(decay_factor)
            crowd_list.append(crowd)
            final_scores.append(final)

        df_ano["Penalty_Decay"] = decay_list
        df_ano["Penalty_Crowd_Capped"] = crowd_list
        df_ano["Score_Ajustado"] = final_scores

        resultados.append(df_ano[["Ano", "ticker", "Score_Ajustado"]])

    if resultados:
        return pd.concat(resultados, ignore_index=True)

    return pd.DataFrame(columns=["Ano", "ticker", "Score_Ajustado"])


__all__ = [
    "calcular_score_ajustado_v2",
    "calcular_score_acumulado_v2",
    "resolve_group_col",
]
