# core/scoring_v3.py
from __future__ import annotations

"""
core/scoring_v3.py

Score v3 (robusto + não-linear + opcional blocos) para seleção comparativa intra-setor/segmento,
mantendo compatibilidade com o pipeline do Score v2:

- resolve_group_col: fallback SEGMENTO -> SUBSETOR -> SETOR (amostra mínima).
- Instabilidade: CV em janela histórica + penalidade progressiva (mesmo do v2).
- Crowding: aplicado após score consolidado, com cap mínimo.
- Decay: aplicado ao score final, com cap.

Diferença principal v3 vs v2:
- v2: percentil por métrica + soma ponderada
- v3: winsor + robust z (mediana/MAD) + tanh (saturação) + soma ponderada (ou por blocos),
      e no final converte para 0..1 via percentil intra-grupo para manter a escala do core.

Funções públicas:
- calcular_score_ajustado_v3(...)
- calcular_score_acumulado_v3(...)
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# Reuso de helpers do scoring v1 (mantém consistência com v2)
from core.scoring import (
    _ensure_year,
    _to_numeric_series,
    calc_crowding_penalty,
    calcular_metricas_historicas_simplificadas,
)

# Reuso direto de utilitários do v2 (mesma semântica de fallback e instabilidade)
from core.scoring_v2 import (
    DEFAULT_CROWDING_MIN_FACTOR,
    DEFAULT_DECAY_CAP,
    DEFAULT_DECAY_PER_YEAR,
    DEFAULT_INSTABILITY_CAP,
    DEFAULT_INSTABILITY_POWER,
    DEFAULT_INSTABILITY_STRENGTH,
    DEFAULT_INSTABILITY_WINDOW_YEARS,
    DEFAULT_MIN_N_GROUP,
    apply_instability_penalty,
    compute_instability_cv,
    resolve_group_col,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Config v3
# ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ScoreV3Config:
    # robustez
    winsor_p_low: float = 0.05
    winsor_p_high: float = 0.95
    mad_eps: float = 1e-9

    # não-linearidade
    tanh_c: float = 2.0  # u -> tanh(u/c)

    # normalização final (mantém compatibilidade 0..1 do core)
    # Se True: converte score_base_v3 em percentil (0..1) por grupo (como no v2).
    # Se False: mantém score_base_v3 bruto (geralmente não recomendado para o core atual).
    normalize_to_percentile: bool = True


# ─────────────────────────────────────────────────────────────
# Utilitários robustos
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


def robust_zscore_within_group(
    df: pd.DataFrame,
    group_col: str,
    col: str,
    mad_eps: float = 1e-9,
) -> pd.Series:
    """
    Robust z-score por grupo usando mediana e MAD:
      z = (x - med) / (1.4826 * MAD)
    """
    def _rz(s: pd.Series) -> pd.Series:
        x = _to_numeric_series(s)
        if x.dropna().empty:
            return x * 0.0  # tudo NaN -> retorna NaN; mas preserva index
        med = float(x.median(skipna=True))
        mad = float((x - med).abs().median(skipna=True))
        denom = 1.4826 * max(mad, float(mad_eps))
        return (x - med) / denom

    return df.groupby(group_col, dropna=False)[col].transform(_rz)


def tanh_squash(u: pd.Series, c: float = 2.0) -> pd.Series:
    c = float(c) if c and c > 0 else 2.0
    x = _to_numeric_series(u)
    return np.tanh(x / c)


def pct_within_group(df: pd.DataFrame, by: str, col: str) -> pd.Series:
    """Percentil (rank pct=True) por grupo; NaN -> 0.5."""
    return df.groupby(by, dropna=False)[col].transform(
        lambda s: _to_numeric_series(s).rank(pct=True, method="average").fillna(0.5)
    )


# ─────────────────────────────────────────────────────────────
# Blocos (opcional)
# ─────────────────────────────────────────────────────────────

def _infer_blocks_from_pesos(
    pesos_utilizados: Mapping[str, Mapping[str, Any]]
) -> Tuple[Dict[str, List[str]], Dict[str, float]]:
    """
    Se o usuário não passar block_map/block_weights, criamos blocos "unitários":
      bloco = cada coluna; peso = peso da coluna.
    Isso garante drop-in sem exigir reconfiguração.
    """
    block_map: Dict[str, List[str]] = {}
    block_weights: Dict[str, float] = {}

    for col, cfg in pesos_utilizados.items():
        peso = float(cfg.get("peso", 0.0))
        if peso <= 0:
            continue
        b = col  # bloco unitário
        block_map[b] = [col]
        block_weights[b] = peso

    # normaliza pesos dos blocos (evita scale drift)
    wsum = sum(max(0.0, w) for w in block_weights.values())
    if wsum > 0:
        block_weights = {b: max(0.0, w) / wsum for b, w in block_weights.items()}

    return block_map, block_weights


def build_block_scores(
    out: pd.DataFrame,
    block_map: Mapping[str, Sequence[str]],
    block_weights: Mapping[str, float],
) -> pd.Series:
    """
    Score base por blocos:
      S = sum_b w_b * mean(y_k in bloco b)
    Onde y_k já está em [-1, +1] após tanh.
    """
    # normaliza pesos (defensivo)
    bw = {b: float(w) for b, w in block_weights.items()}
    wsum = sum(max(0.0, w) for w in bw.values())
    if wsum <= 0:
        # fallback: pesos iguais
        keys = list(block_map.keys())
        if not keys:
            return pd.Series(0.0, index=out.index)
        bw = {k: 1.0 / float(len(keys)) for k in keys}
    else:
        bw = {b: max(0.0, w) / wsum for b, w in bw.items()}

    S = pd.Series(0.0, index=out.index)
    for b, cols in block_map.items():
        cols_eff = [c for c in cols if c in out.columns]
        if not cols_eff:
            continue
        F = pd.concat([_to_numeric_series(out[c]).fillna(0.0) for c in cols_eff], axis=1).mean(axis=1)
        S = S + float(bw.get(b, 0.0)) * F

    return S


# ─────────────────────────────────────────────────────────────
# Score v3: parte cross-sectional (substitui calcular_score_ajustado_v2)
# ─────────────────────────────────────────────────────────────

def calcular_score_ajustado_v3(
    df: pd.DataFrame,
    pesos_utilizados: Mapping[str, Mapping[str, Any]],
    prefer_group_col: str = "SEGMENTO",
    min_n_group: int = DEFAULT_MIN_N_GROUP,
    config: Optional[ScoreV3Config] = None,
    # blocos opcionais (se None, inferido de pesos_utilizados)
    block_map: Optional[Mapping[str, Sequence[str]]] = None,
    block_weights: Optional[Mapping[str, float]] = None,
) -> pd.DataFrame:
    """
    Calcula Score_Ajustado v3 (somente a parte fundamental cross-sectional):
    - winsoriza indicador
    - robust z-score por grupo (mediana/MAD)
    - aplica direcionalidade (melhor_alto)
    - aplica saturação tanh (ganhos marginais decrescentes)
    - agrega por pesos (ou por blocos)
    - converte para percentil 0..1 por grupo (para manter compatibilidade do core)
    """
    if df is None or df.empty:
        return df

    cfg = config or ScoreV3Config()
    out = df.copy()

    # seleciona coluna de grupo robustamente (mesma regra do v2)
    group_col = resolve_group_col(out, prefer=prefer_group_col, min_n=min_n_group)
    if group_col not in out.columns:
        out[group_col] = "OUTROS"

    # Se blocos não forem fornecidos, cria blocos unitários com pesos do v2
    if block_map is None or block_weights is None:
        bmap, bweights = _infer_blocks_from_pesos(pesos_utilizados)
        block_map = block_map or bmap
        block_weights = block_weights or bweights

    # calcula y_{col} (tanh do robust z ajustado pela direção)
    y_cols: List[str] = []
    for col, cfg_col in pesos_utilizados.items():
        if col not in out.columns:
            continue

        peso = float(cfg_col.get("peso", 0.0))
        if peso <= 0:
            continue

        melhor_alto = bool(cfg_col.get("melhor_alto", True))

        # winsor (defensivo)
        out[col] = winsorize_series(out[col], p_low=cfg.winsor_p_low, p_high=cfg.winsor_p_high)

        # robust z por grupo
        z = robust_zscore_within_group(out, group_col=group_col, col=col, mad_eps=cfg.mad_eps)

        # direcionalidade
        u = z if melhor_alto else (-1.0 * z)

        # saturação
        y = tanh_squash(u, c=cfg.tanh_c)

        ycol = f"{col}_y"
        out[ycol] = _to_numeric_series(y)
        y_cols.append(ycol)

    # score base por blocos (se blocos unitários, isso equivale à soma ponderada dos y)
    score_base = build_block_scores(out, block_map=block_map, block_weights=block_weights)
    out["Score_Base_v3"] = _to_numeric_series(score_base).fillna(0.0)

    # normalização final: 0..1 por percentil no grupo (compatível com o core do v2)
    if cfg.normalize_to_percentile:
        out["Score_Ajustado"] = pct_within_group(out, by=group_col, col="Score_Base_v3")
    else:
        out["Score_Ajustado"] = out["Score_Base_v3"]

    return out


# ─────────────────────────────────────────────────────────────
# Score acumulado v3 (equivalente ao calcular_score_acumulado_v2)
# ─────────────────────────────────────────────────────────────

def calcular_score_acumulado_v3(
    lista_empresas: Sequence[Mapping[str, Any]],
    group_map: Mapping[str, str],
    pesos_utilizados: Mapping[str, Mapping[str, Any]],
    anos_minimos: int = 4,
    prefer_group_col: str = "SEGMENTO",
    min_n_group: int = DEFAULT_MIN_N_GROUP,
    # Config v3
    config: Optional[ScoreV3Config] = None,
    block_map: Optional[Mapping[str, Sequence[str]]] = None,
    block_weights: Optional[Mapping[str, float]] = None,
    # Instabilidade (mesmos defaults do v2)
    instability_window_years: int = DEFAULT_INSTABILITY_WINDOW_YEARS,
    instability_cap: float = DEFAULT_INSTABILITY_CAP,
    instability_strength: float = DEFAULT_INSTABILITY_STRENGTH,
    instability_power: float = DEFAULT_INSTABILITY_POWER,
    # Crowding + decay (mesmos defaults do v2)
    crowding_min_factor: float = DEFAULT_CROWDING_MIN_FACTOR,
    decay_per_year: float = DEFAULT_DECAY_PER_YEAR,
    decay_cap: float = DEFAULT_DECAY_CAP,
    # Hierarquia opcional (para fallback SEGMENTO->SUBSETOR->SETOR)
    subsetor_map: Optional[Mapping[str, str]] = None,
    setor_map: Optional[Mapping[str, str]] = None,
) -> pd.DataFrame:
    """
    Versão v3 do acumulado:
    - Score fundamental por robust z-score + tanh + agregação (blocos ou unitário) + percentil por grupo (fallback n pequeno)
    - Penalidade de instabilidade por CV em janela histórica (mesma do v2)
    - Crowding + Decay mantidos e aplicados no score final (com caps)

    Retorno: DF com colunas ["Ano", "ticker", "Score_Ajustado"] para compatibilidade.
    """
    cfg = config or ScoreV3Config()

    if not lista_empresas:
        return pd.DataFrame(columns=["Ano", "ticker", "Score_Ajustado"])

    subsetor_map = subsetor_map or {}
    setor_map = setor_map or {}

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

    # Colunas candidatas para instabilidade (no DRE histórico)
    instability_candidates = [
        "ROIC", "ROIC_mean",
        "Margem_Operacional", "Margem_Operacional_mean",
        "Margem_Liquida", "Margem_Liquida_mean",
    ]

    for idx in range(anos_minimos, len(anos_disponiveis)):
        ano = int(anos_disponiveis[idx])

        # Pré-coleta P/VP por grupo no ano (para crowding)
        grupo_to_pvp: Dict[str, List[float]] = {}

        for emp in lista_empresas:
            tk = str(emp.get("ticker", "")).strip()
            dfm = emp.get("multiplos")
            if not tk or not isinstance(dfm, pd.DataFrame) or dfm.empty:
                continue

            dfm2 = _ensure_year(dfm, date_col="Data", year_col="Ano")
            dfm_ano = dfm2[dfm2["Ano"] == ano]
            if dfm_ano.empty:
                continue

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

            df_mult_hist = df_mult2[df_mult2["Ano"] <= ano].copy()
            df_dre_hist = df_dre2[df_dre2["Ano"] <= ano].copy()

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

            # instabilidade (CV em janela)
            instability_cv = compute_instability_cv(
                df_dre_hist,
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

        # (A) Score fundamental v3 (robust z + tanh + agregação + percentil por grupo com fallback)
        df_ano = calcular_score_ajustado_v3(
            df_ano,
            pesos_utilizados=pesos_utilizados,
            prefer_group_col=prefer_group_col,
            min_n_group=min_n_group,
            config=cfg,
            block_map=block_map,
            block_weights=block_weights,
        )

        # resolve group_col efetivo (mesma regra usada no v2)
        group_col_eff = resolve_group_col(df_ano, prefer=prefer_group_col, min_n=min_n_group)

        # (B) Penalidade de instabilidade: percentil por grupo, progressiva, cap (mesma do v2)
        df_ano = apply_instability_penalty(
            df_ano,
            group_col=group_col_eff,
            cap=instability_cap,
            strength=instability_strength,
            power=instability_power,
        )

        # (C) Seleciona líder e aplica crowding + decay no score final (mesma lógica do v2)
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
            crowd = max(crowd, float(crowding_min_factor))

            # Base score (0..1)
            base = float(row.get("Score_Ajustado", 0.0))
            if not np.isfinite(base):
                base = 0.0

            # Penalidade de instabilidade
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
    "ScoreV3Config",
    "calcular_score_ajustado_v3",
    "calcular_score_acumulado_v3",
]
