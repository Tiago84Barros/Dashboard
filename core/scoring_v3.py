# core/scoring_v3.py
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd


# =========================
# Configs / Types
# =========================

Number = Union[int, float, np.number]


@dataclass(frozen=True)
class ScoreV3Config:
    """
    Configuração do Score v3 (robusto + não-linear + blocos + confiabilidade).

    Premissas:
      - O score é calculado INTRA-SEGMENTO (group_col), com robust z-score por métrica.
      - Direcionalidade por métrica: +1 (alto é melhor), -1 (baixo é melhor).
      - Saturação não-linear (tanh) para ganhos marginais decrescentes.
      - Agregação em blocos de fatores (evita double-counting por correlação).
      - Peso por confiabilidade (penaliza instabilidade temporal do bloco).
      - Opcional: calibração logística -> score cardinal (probabilidade).

    Observação:
      - Se você não tiver histórico labelado para calibrar logisticamente,
        use a saída "score_0_100" baseada no percentil do score dentro do segmento.
    """

    # Colunas
    group_col: str = "segmento"
    company_col: str = "ticker"

    # Robustez
    winsor_p_low: float = 0.05
    winsor_p_high: float = 0.95
    robust_mad_eps: float = 1e-9  # evita divisão por zero em MAD muito pequeno

    # Saturação
    tanh_c: float = 2.0  # quanto maior, mais "linear"; quanto menor, mais satura

    # Confiabilidade (estabilidade temporal)
    # r = 1 / (1 + lambda * std(F_bloco ao longo do tempo))
    reliability_lambda: float = 0.75

    # Normalização final
    # Se logistic_params for None, score final 0..100 vem de percentil intra-segmento
    logistic_params: Optional[Tuple[float, float]] = None  # (a, b)

    # Clip final
    clip_0_100: bool = True


# =========================
# Helpers: robust stats
# =========================

def _safe_quantile(s: pd.Series, q: float) -> float:
    s2 = pd.to_numeric(s, errors="coerce").dropna()
    if s2.empty:
        return np.nan
    return float(s2.quantile(q))


def winsorize_by_group(
    df: pd.DataFrame,
    group_col: str,
    cols: Sequence[str],
    p_low: float = 0.05,
    p_high: float = 0.95,
) -> pd.DataFrame:
    """
    Winsorização por grupo/segmento: clip em [Q(p_low), Q(p_high)].
    Retorna cópia com colunas winsorizadas.
    """
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            continue

        def _clip_grp(g: pd.DataFrame) -> pd.Series:
            lo = _safe_quantile(g[c], p_low)
            hi = _safe_quantile(g[c], p_high)
            s = pd.to_numeric(g[c], errors="coerce")
            if np.isnan(lo) or np.isnan(hi):
                return s
            return s.clip(lower=lo, upper=hi)

        out[c] = out.groupby(group_col, group_keys=False).apply(_clip_grp)
    return out


def robust_zscore_by_group(
    df: pd.DataFrame,
    group_col: str,
    cols: Sequence[str],
    mad_eps: float = 1e-9,
) -> pd.DataFrame:
    """
    Robust z-score por grupo usando mediana e MAD:
      z = (x - med) / (1.4826 * MAD)
    Retorna um DF com colunas z_{col}.
    """
    zdf = pd.DataFrame(index=df.index)

    for c in cols:
        if c not in df.columns:
            continue

        x = pd.to_numeric(df[c], errors="coerce")

        def _robust_z(g: pd.DataFrame) -> pd.Series:
            s = pd.to_numeric(g[c], errors="coerce")
            med = float(s.median(skipna=True)) if s.notna().any() else np.nan
            mad = float((s - med).abs().median(skipna=True)) if s.notna().any() else np.nan
            denom = 1.4826 * max(mad, mad_eps) if not np.isnan(mad) else np.nan
            return (s - med) / denom

        z = df.groupby(group_col, group_keys=False).apply(_robust_z)
        zdf[f"z_{c}"] = z

    return zdf


def apply_directionality(
    zdf: pd.DataFrame,
    z_cols: Sequence[str],
    direction_map: Mapping[str, int],
) -> pd.DataFrame:
    """
    Aplica direcionalidade ao robust z:
      u = d * z
    direction_map deve ser definido no nome da métrica original (sem prefixo z_),
    ou alternativamente no próprio nome da coluna z_.
    """
    out = pd.DataFrame(index=zdf.index)
    for zc in z_cols:
        if zc not in zdf.columns:
            continue

        # métrica original: "z_margem_liquida" -> "margem_liquida"
        base = zc[2:] if zc.startswith("z_") else zc
        d = direction_map.get(base, direction_map.get(zc, +1))
        out[f"u_{base}"] = pd.to_numeric(zdf[zc], errors="coerce") * float(d)
    return out


def tanh_squash(
    udf: pd.DataFrame,
    u_cols: Sequence[str],
    c: float = 2.0,
) -> pd.DataFrame:
    """
    Saturação não-linear:
      y = tanh(u/c)
    """
    out = pd.DataFrame(index=udf.index)
    c = float(c) if c and c > 0 else 2.0
    for uc in u_cols:
        if uc not in udf.columns:
            continue
        base = uc[2:] if uc.startswith("u_") else uc
        u = pd.to_numeric(udf[uc], errors="coerce")
        out[f"y_{base}"] = np.tanh(u / c)
    return out


# =========================
# Blocos / Confiabilidade
# =========================

def build_factor_blocks(
    ydf: pd.DataFrame,
    block_map: Mapping[str, Sequence[str]],
    alpha_weights: Optional[Mapping[str, Mapping[str, float]]] = None,
) -> pd.DataFrame:
    """
    Constrói fatores por bloco:
      F_{i,b} = sum_{k in bloco} alpha_{b,k} * y_{i,k}

    block_map: {"Q": ["roe", "roic", "margem_liquida"], "V": ["p_vp"], ...}
      - nomes devem ser as métricas base (sem prefixos).
    alpha_weights (opcional):
      {"Q": {"roe": 0.4, "roic": 0.4, "margem_liquida": 0.2}, ...}
      Se não informado, usa pesos uniformes dentro do bloco.
    """
    out = pd.DataFrame(index=ydf.index)

    for b, metrics in block_map.items():
        metrics = [m for m in metrics if f"y_{m}" in ydf.columns]
        if not metrics:
            out[f"F_{b}"] = np.nan
            continue

        if alpha_weights and b in alpha_weights:
            aw = dict(alpha_weights[b])
            w = np.array([float(aw.get(m, 0.0)) for m in metrics], dtype=float)
            if w.sum() <= 0:
                w = np.ones(len(metrics), dtype=float)
        else:
            w = np.ones(len(metrics), dtype=float)

        w = w / w.sum()

        mat = np.column_stack([pd.to_numeric(ydf[f"y_{m}"], errors="coerce").to_numpy() for m in metrics])
        out[f"F_{b}"] = np.nan_to_num(mat, nan=0.0).dot(w)

    return out


def reliability_from_time_series(
    df_time: pd.DataFrame,
    company_col: str,
    time_col: str,
    factor_cols: Sequence[str],
    lam: float = 0.75,
) -> pd.DataFrame:
    """
    Calcula confiabilidade por empresa e por fator/bloco a partir de série temporal:
      r = 1 / (1 + lam * std(F ao longo do tempo))

    Retorna um DF indexado por company_col com colunas r_{factor}.
    """
    if df_time.empty:
        return pd.DataFrame(columns=[f"r_{c}" for c in factor_cols]).set_index(company_col)

    d = df_time[[company_col, time_col, *factor_cols]].copy()
    d = d.dropna(subset=[company_col, time_col])
    if d.empty:
        return pd.DataFrame(columns=[f"r_{c}" for c in factor_cols]).set_index(company_col)

    # std por empresa (na prática você pode usar janela fixa; aqui é global no df_time fornecido)
    g = d.groupby(company_col, dropna=True)
    stds = g[factor_cols].std(ddof=0)

    lam = float(lam) if lam >= 0 else 0.0
    r = 1.0 / (1.0 + lam * stds.replace([np.inf, -np.inf], np.nan).fillna(0.0))
    r.columns = [f"r_{c}" for c in factor_cols]
    r.index.name = company_col
    return r


def combine_blocks(
    factors_df: pd.DataFrame,
    block_weights: Mapping[str, float],
    reliability_df: Optional[pd.DataFrame] = None,
    company_series: Optional[pd.Series] = None,
) -> pd.Series:
    """
    Combina blocos:
      S = sum_b w_b * r_b * F_b
    Se reliability_df fornecido, tenta casar por company_col (via company_series).
    """
    S = pd.Series(0.0, index=factors_df.index)

    # pesos normalizados
    bw = {b: float(w) for b, w in block_weights.items()}
    wsum = sum(max(0.0, w) for w in bw.values())
    if wsum <= 0:
        bw = {b: 1.0 for b in bw.keys()}
        wsum = float(len(bw))
    bw = {b: max(0.0, w) / wsum for b, w in bw.items()}

    for b, w in bw.items():
        col = f"F_{b}"
        if col not in factors_df.columns:
            continue

        F = pd.to_numeric(factors_df[col], errors="coerce").fillna(0.0)

        if reliability_df is not None and company_series is not None:
            rcol = f"r_{col}"
            if rcol in reliability_df.columns:
                # map empresa->r
                rmap = reliability_df[rcol]
                r = company_series.map(rmap).astype(float).fillna(1.0)
            else:
                r = 1.0
        else:
            r = 1.0

        S = S + w * (F * r)

    return S


# =========================
# Calibração / Normalização final
# =========================

def sigmoid(x: Union[pd.Series, np.ndarray, float], a: float, b: float) -> Union[pd.Series, np.ndarray, float]:
    z = a * x + b
    # evita overflow
    if isinstance(z, pd.Series):
        z = z.clip(-60, 60)
        return 1.0 / (1.0 + np.exp(-z))
    z = np.clip(z, -60, 60)
    return 1.0 / (1.0 + np.exp(-z))


def percentile_0_100_by_group(
    df: pd.DataFrame,
    group_col: str,
    score_col: str,
) -> pd.Series:
    """
    Converte score contínuo em 0..100 via rank percentil intra-grupo (segmento).
    Mantém comparabilidade dentro do segmento quando não há calibração logística.
    """
    s = pd.to_numeric(df[score_col], errors="coerce")

    def _pct(g: pd.DataFrame) -> pd.Series:
        x = pd.to_numeric(g[score_col], errors="coerce")
        if x.notna().sum() <= 1:
            return pd.Series(np.where(x.notna(), 50.0, np.nan), index=g.index)
        r = x.rank(method="average", pct=True)
        return 100.0 * r

    return df.assign(**{score_col: s}).groupby(group_col, group_keys=False).apply(_pct)


# =========================
# API principal
# =========================

def calcular_score_acumulado_v3(
    df_atual: pd.DataFrame,
    metrics: Sequence[str],
    direction_map: Mapping[str, int],
    block_map: Mapping[str, Sequence[str]],
    block_weights: Mapping[str, float],
    *,
    config: Optional[ScoreV3Config] = None,
    # Série temporal opcional para confiabilidade:
    df_time: Optional[pd.DataFrame] = None,
    time_col: Optional[str] = None,
    alpha_weights: Optional[Mapping[str, Mapping[str, float]]] = None,
    # Penalidades externas (compatível com seu pipeline atual)
    penalty_fn: Optional[Callable[[pd.DataFrame], pd.Series]] = None,
) -> pd.DataFrame:
    """
    Calcula Score v3 e devolve DF com colunas:
      - score_base (antes de penalidades)
      - score_0_100 (normalizado)
      - score_final (após penalidades)
      - adicionais intermediários (opcionalmente úteis para debug)

    Requisitos mínimos em df_atual:
      - config.group_col (default "segmento")
      - config.company_col (default "ticker")
      - colunas de metrics
    """
    cfg = config or ScoreV3Config()

    required = [cfg.group_col, cfg.company_col]
    for rc in required:
        if rc not in df_atual.columns:
            raise ValueError(f"df_atual precisa conter a coluna obrigatória: '{rc}'")

    df = df_atual.copy()

    # 1) winsor
    df_w = winsorize_by_group(
        df,
        group_col=cfg.group_col,
        cols=[c for c in metrics if c in df.columns],
        p_low=cfg.winsor_p_low,
        p_high=cfg.winsor_p_high,
    )

    # 2) robust z
    zdf = robust_zscore_by_group(
        df_w,
        group_col=cfg.group_col,
        cols=[c for c in metrics if c in df_w.columns],
        mad_eps=cfg.robust_mad_eps,
    )
    z_cols = [c for c in zdf.columns if c.startswith("z_")]

    # 3) direcionalidade
    udf = apply_directionality(zdf, z_cols=z_cols, direction_map=direction_map)
    u_cols = [c for c in udf.columns if c.startswith("u_")]

    # 4) saturação
    ydf = tanh_squash(udf, u_cols=u_cols, c=cfg.tanh_c)

    # 5) blocos
    factors = build_factor_blocks(ydf, block_map=block_map, alpha_weights=alpha_weights)

    # 6) confiabilidade (opcional)
    reliability_df = None
    if df_time is not None and time_col is not None and not df_time.empty:
        factor_cols = [c for c in factors.columns if c.startswith("F_")]

        # df_time precisa ter as mesmas colunas de fator; em geral você vai calcular fatores
        # por período e armazenar. Se ainda não tem isso, passe df_time já com F_*
        missing = [c for c in factor_cols if c not in df_time.columns]
        if missing:
            # Se não tiver fatores no df_time, não aplica confiabilidade (mantém r=1)
            reliability_df = None
        else:
            reliability_df = reliability_from_time_series(
                df_time=df_time,
                company_col=cfg.company_col,
                time_col=time_col,
                factor_cols=factor_cols,
                lam=cfg.reliability_lambda,
            )

    # 7) combina blocos
    score_base = combine_blocks(
        factors_df=factors,
        block_weights=block_weights,
        reliability_df=reliability_df,
        company_series=df[cfg.company_col],
    )
    df["score_base_v3"] = score_base.astype(float)

    # 8) normalização / calibração
    if cfg.logistic_params is not None:
        a, b = cfg.logistic_params
        p = sigmoid(df["score_base_v3"], float(a), float(b))
        df["score_0_100_v3"] = 100.0 * pd.to_numeric(p, errors="coerce")
    else:
        df["score_0_100_v3"] = percentile_0_100_by_group(df, cfg.group_col, "score_base_v3")

    if cfg.clip_0_100:
        df["score_0_100_v3"] = pd.to_numeric(df["score_0_100_v3"], errors="coerce").clip(0, 100)

    # 9) penalidades externas (se existirem)
    if penalty_fn is not None:
        pen = penalty_fn(df).reindex(df.index)
        pen = pd.to_numeric(pen, errors="coerce").fillna(0.0)
    else:
        pen = pd.Series(0.0, index=df.index)

    df["penalidade_v3"] = pen
    df["score_final_v3"] = df["score_0_100_v3"] - df["penalidade_v3"]
    if cfg.clip_0_100:
        df["score_final_v3"] = pd.to_numeric(df["score_final_v3"], errors="coerce").clip(0, 100)

    # (Debug útil) anexar intermediários
    df = pd.concat([df, zdf, udf, ydf, factors], axis=1)

    return df


# =========================
# Exemplo de uso (comentado)
# =========================
#
# metrics = ["margem_bruta", "margem_ebitda", "margem_liquida", "roe", "roic", "p_vp", "dy", "slope_dre"]
# direction_map = {
#   "margem_bruta": +1, "margem_ebitda": +1, "margem_liquida": +1,
#   "roe": +1, "roic": +1,
#   "p_vp": -1,
#   "dy": +1,
#   "slope_dre": +1,
# }
# block_map = {
#   "Q": ["margem_bruta", "margem_ebitda", "margem_liquida", "roe", "roic"],
#   "V": ["p_vp"],
#   "D": ["dy"],
#   "G": ["slope_dre"],
# }
# block_weights = {"Q": 0.45, "V": 0.20, "D": 0.20, "G": 0.15}
#
# cfg = ScoreV3Config(group_col="segmento", company_col="ticker", tanh_c=2.0, logistic_params=None)
#
# df_out = calcular_score_acumulado_v3(
#   df_atual=df_atual,
#   metrics=metrics,
#   direction_map=direction_map,
#   block_map=block_map,
#   block_weights=block_weights,
#   config=cfg,
#   penalty_fn=None,  # ou sua função que calcula crowding/decay/platô em pontos de 0..100
# )
#
# df_out.sort_values(["segmento", "score_final_v3"], ascending=[True, False]).head()
