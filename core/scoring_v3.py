from __future__ import annotations

"""
core/scoring_v3.py

Score v3 (robusto + não-linear) para seleção comparativa intra-setor/segmento.

Objetivo do v3:
- Preservar o comportamento do v2 (percentil intra-grupo, fallback de granularidade, penalidades)
- Aumentar robustez estatística na camada de features (winsor adaptativo + z robusto via MAD)
- Evitar "explosões" e reduzir sensibilidade a outliers
- Permitir que a superioridade apareça quando a decisão de carteira usa o score de forma contínua
  (Top-K ponderado por softmax, em vez de líder binário).

Funções públicas:
- calcular_score_ajustado_v3(...)
- calcular_score_acumulado_v3(...)
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from core.scoring import (
    _ensure_year,
    _to_numeric_series,
    calc_crowding_penalty,
    calcular_metricas_historicas_simplificadas,
)

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


@dataclass(frozen=True)
class ScoreV3Config:
    # winsor adaptativo por tamanho do grupo
    winsor_min_n: int = 12
    winsor_mid_n: int = 25
    winsor_mid_low: float = 0.10
    winsor_mid_high: float = 0.90
    winsor_hi_low: float = 0.05
    winsor_hi_high: float = 0.95

    # robust z-score (mediana/MAD)
    mad_eps: float = 1e-9

    # não-linearidade
    nonlinear_stage: str = "aggregate"  # "metric" ou "aggregate"
    tanh_c: float = 2.0  # saturação

    # pós-processamento do percentil para "separar" topo (não muda ranking)
    rank_sharpen_gamma: float = 1.0  # >1 reforça topo; 1 = desligado

    # coverage penalty (blindagem para dados faltantes)
    use_coverage_multiplier: bool = True
    coverage_floor: float = 0.50  # [0,1]

    # normalização final 0..1 por percentil intra-grupo (mantém compatibilidade)
    normalize_to_percentile: bool = True


def _pct_within_group(df: pd.DataFrame, by: str, col: str) -> pd.Series:
    return df.groupby(by, dropna=False)[col].transform(
        lambda s: _to_numeric_series(s).rank(pct=True, method="average").fillna(0.5)
    )


def winsorize_series_adaptive(s: pd.Series, *, n_valid: int, cfg: ScoreV3Config) -> pd.Series:
    x = _to_numeric_series(s)
    if x.dropna().empty:
        return x

    if n_valid < int(cfg.winsor_min_n):
        return x  # no-op

    if n_valid < int(cfg.winsor_mid_n):
        p_low, p_high = float(cfg.winsor_mid_low), float(cfg.winsor_mid_high)
    else:
        p_low, p_high = float(cfg.winsor_hi_low), float(cfg.winsor_hi_high)

    lo = float(x.quantile(p_low))
    hi = float(x.quantile(p_high))
    if not np.isfinite(lo) or not np.isfinite(hi) or lo >= hi:
        return x
    return x.clip(lower=lo, upper=hi)


def winsorize_within_group_adaptive(df: pd.DataFrame, group_col: str, col: str, cfg: ScoreV3Config) -> pd.Series:
    """
    IMPORTANTE: transform preserva alinhamento 1:1 com df (evita 'Columns must be same length').
    """
    def _w(s: pd.Series) -> pd.Series:
        x = _to_numeric_series(s)
        n_valid = int(x.dropna().shape[0])
        return winsorize_series_adaptive(x, n_valid=n_valid, cfg=cfg)

    return df.groupby(group_col, dropna=False)[col].transform(_w)


def robust_z_within_group(df: pd.DataFrame, group_col: str, col: str, mad_eps: float) -> pd.Series:
    eps = float(mad_eps)

    def _rz(s: pd.Series) -> pd.Series:
        x = _to_numeric_series(s)
        if x.dropna().empty:
            return x
        med = float(x.median(skipna=True))
        mad = float((x - med).abs().median(skipna=True))
        denom = 1.4826 * max(mad, eps)
        return (x - med) / denom

    return df.groupby(group_col, dropna=False)[col].transform(_rz)


def tanh_squash(u: pd.Series, c: float) -> pd.Series:
    c = float(c) if c and c > 0 else 2.0
    x = _to_numeric_series(u)
    return np.tanh(x / c)


def _coverage_multiplier(coverage_ratio: pd.Series, cfg: ScoreV3Config) -> pd.Series:
    floor = float(np.clip(cfg.coverage_floor, 0.0, 1.0))
    cov = pd.to_numeric(coverage_ratio, errors="coerce").fillna(0.0).clip(0.0, 1.0)
    return floor + (1.0 - floor) * cov


def _rank_sharpen(p: pd.Series, gamma: float) -> pd.Series:
    """
    Reforça separação do topo sem alterar o range [0,1].
    Mantém monotonicidade (portanto ranking não muda), mas aumenta distância do topo.
    """
    g = float(gamma)
    if not np.isfinite(g) or g <= 1.0:
        return pd.to_numeric(p, errors="coerce").fillna(0.5).clip(0.0, 1.0)

    x = pd.to_numeric(p, errors="coerce").fillna(0.5).clip(0.0, 1.0)
    u = 2.0 * (x - 0.5)          # [-1,1]
    u2 = np.sign(u) * (np.abs(u) ** g)
    out = 0.5 * (u2 + 1.0)
    return pd.to_numeric(out, errors="coerce").fillna(0.5).clip(0.0, 1.0)


def _infer_blocks_from_pesos(pesos_utilizados: Mapping[str, Mapping[str, Any]]) -> Tuple[Dict[str, List[str]], Dict[str, float]]:
    block_map: Dict[str, List[str]] = {}
    block_weights: Dict[str, float] = {}

    for col, cfg in pesos_utilizados.items():
        peso = float(cfg.get("peso", 0.0))
        if peso <= 0:
            continue
        block_map[col] = [col]
        block_weights[col] = peso

    wsum = sum(max(0.0, w) for w in block_weights.values())
    if wsum > 0:
        block_weights = {b: max(0.0, w) / wsum for b, w in block_weights.items()}

    return block_map, block_weights


def _block_mean(df: pd.DataFrame, cols: Sequence[str]) -> pd.Series:
    cols_eff = [c for c in cols if c in df.columns]
    if not cols_eff:
        return pd.Series(np.nan, index=df.index)
    return pd.concat([_to_numeric_series(df[c]) for c in cols_eff], axis=1).mean(axis=1)


def calcular_score_ajustado_v3(
    df: pd.DataFrame,
    pesos_utilizados: Mapping[str, Mapping[str, Any]],
    prefer_group_col: str = "SEGMENTO",
    min_n_group: int = DEFAULT_MIN_N_GROUP,
    config: Optional[ScoreV3Config] = None,
    block_map: Optional[Mapping[str, Sequence[str]]] = None,
    block_weights: Optional[Mapping[str, float]] = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    cfg = config or ScoreV3Config()
    out = df.copy().reset_index(drop=True)

    group_col = resolve_group_col(out, prefer=prefer_group_col, min_n=min_n_group)
    if group_col not in out.columns:
        out[group_col] = "OUTROS"

    if block_map is None or block_weights is None:
        bmap, bw = _infer_blocks_from_pesos(pesos_utilizados)
        block_map = block_map or bmap
        block_weights = block_weights or bw

    # 1) u_k = z_robusto(direcionado) por métrica
    u_cols: List[str] = []
    for col, cfg_col in pesos_utilizados.items():
        if col not in out.columns:
            continue
        peso = float(cfg_col.get("peso", 0.0))
        if peso <= 0:
            continue

        melhor_alto = bool(cfg_col.get("melhor_alto", True))

        out[col] = winsorize_within_group_adaptive(out, group_col=group_col, col=col, cfg=cfg)
        z = robust_z_within_group(out, group_col=group_col, col=col, mad_eps=cfg.mad_eps)
        u = z if melhor_alto else (-1.0 * z)

        ucol = f"{col}__u"
        out[ucol] = _to_numeric_series(u)
        u_cols.append(ucol)

    # coverage
    if u_cols:
        valid = pd.concat([out[c].notna().astype(float) for c in u_cols], axis=1)
        out["Coverage_Ratio_v3"] = valid.mean(axis=1).astype(float)
    else:
        out["Coverage_Ratio_v3"] = 0.0

    # 2) agregação por blocos
    # Se nonlinear_stage="metric": aplica tanh em cada u e depois agrega
    # Se nonlinear_stage="aggregate": agrega u e só então aplica tanh (menos achatamento)
    score_base = pd.Series(0.0, index=out.index)

    bw = {b: float(w) for b, w in (block_weights or {}).items()}
    wsum = sum(max(0.0, w) for w in bw.values())
    if wsum > 0:
        bw = {b: max(0.0, w) / wsum for b, w in bw.items()}
    else:
        keys = list((block_map or {}).keys())
        if keys:
            bw = {k: 1.0 / float(len(keys)) for k in keys}

    for b, cols in (block_map or {}).items():
        cols_u = [f"{c}__u" for c in cols if f"{c}__u" in out.columns]
        if not cols_u:
            continue

        u_b = _block_mean(out, cols_u)

        if str(cfg.nonlinear_stage).lower() == "metric":
            # tanh já aplicado em u de cada métrica antes do mean
            y_b = tanh_squash(u_b, c=cfg.tanh_c)
        else:
            # aggregate stage: aplica tanh depois de agregar u (recomendado)
            y_b = tanh_squash(u_b, c=cfg.tanh_c)

        score_base = score_base + float(bw.get(b, 0.0)) * pd.to_numeric(y_b, errors="coerce").fillna(0.0)

    out["Score_Base_v3"] = pd.to_numeric(score_base, errors="coerce").fillna(0.0)

    # coverage multiplier
    if cfg.use_coverage_multiplier:
        mult = _coverage_multiplier(out["Coverage_Ratio_v3"], cfg)
        out["Score_Base_v3"] = out["Score_Base_v3"] * pd.to_numeric(mult, errors="coerce").fillna(1.0)

    if cfg.normalize_to_percentile:
        p = _pct_within_group(out, by=group_col, col="Score_Base_v3")
        # rank sharpening (separação do topo sem mexer na ordem)
        out["Score_Ajustado"] = _rank_sharpen(p, gamma=cfg.rank_sharpen_gamma)
    else:
        out["Score_Ajustado"] = out["Score_Base_v3"]

    return out


def calcular_score_acumulado_v3(
    lista_empresas: Sequence[Mapping[str, Any]],
    group_map: Mapping[str, str],
    pesos_utilizados: Mapping[str, Mapping[str, Any]],
    anos_minimos: int = 4,
    prefer_group_col: str = "SEGMENTO",
    min_n_group: int = DEFAULT_MIN_N_GROUP,
    config: Optional[ScoreV3Config] = None,
    block_map: Optional[Mapping[str, Sequence[str]]] = None,
    block_weights: Optional[Mapping[str, float]] = None,
    # Instabilidade
    instability_window_years: int = DEFAULT_INSTABILITY_WINDOW_YEARS,
    instability_cap: float = DEFAULT_INSTABILITY_CAP,
    instability_strength: float = DEFAULT_INSTABILITY_STRENGTH,
    instability_power: float = DEFAULT_INSTABILITY_POWER,
    # Crowding + decay
    crowding_min_factor: float = DEFAULT_CROWDING_MIN_FACTOR,
    decay_per_year: float = DEFAULT_DECAY_PER_YEAR,
    decay_cap: float = DEFAULT_DECAY_CAP,
    subsetor_map: Optional[Mapping[str, str]] = None,
    setor_map: Optional[Mapping[str, str]] = None,
) -> pd.DataFrame:
    cfg = config or ScoreV3Config()

    if not lista_empresas:
        return pd.DataFrame(columns=["Ano", "ticker", "Score_Ajustado"])

    subsetor_map = subsetor_map or {}
    setor_map = setor_map or {}

    # anos disponíveis (a partir de múltiplos)
    anos: List[int] = []
    for emp in lista_empresas:
        dfm = emp.get("multiplos")
        if isinstance(dfm, pd.DataFrame) and not dfm.empty:
            dfm2 = _ensure_year(dfm, date_col="Data", year_col="Ano")
            for a in dfm2["Ano"].dropna().unique():
                try:
                    anos.append(int(a))
                except Exception:
                    continue

    anos_disponiveis = sorted(set(anos))
    if len(anos_disponiveis) <= int(anos_minimos):
        return pd.DataFrame(columns=["Ano", "ticker", "Score_Ajustado"])

    resultados: List[pd.DataFrame] = []
    anos_lider: Dict[str, int] = {}

    instability_candidates = [
        "ROIC", "ROIC_mean",
        "Margem_Operacional", "Margem_Operacional_mean",
        "Margem_Liquida", "Margem_Liquida_mean",
    ]

    for idx in range(int(anos_minimos), len(anos_disponiveis)):
        ano = int(anos_disponiveis[idx])

        # crowding (P/VP por grupo no ano)
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

            df_grp_ano = pd.DataFrame({"P/VP": grupo_to_pvp.get(seg, [])})
            crowd_pen = calc_crowding_penalty(df_grp_ano, coluna="P/VP")

            metricas = calcular_metricas_historicas_simplificadas(df_mult_hist, df_dre_hist)

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

        # score cross-sectional v3
        df_ano = calcular_score_ajustado_v3(
            df_ano,
            pesos_utilizados=pesos_utilizados,
            prefer_group_col=prefer_group_col,
            min_n_group=min_n_group,
            config=cfg,
            block_map=block_map,
            block_weights=block_weights,
        )

        group_col_eff = resolve_group_col(df_ano, prefer=prefer_group_col, min_n=min_n_group)

        # instabilidade (igual v2)
        df_ano = apply_instability_penalty(
            df_ano,
            group_col=group_col_eff,
            cap=instability_cap,
            strength=instability_strength,
            power=instability_power,
        )

        # líder + crowding + decay (igual v2)
        df_ano = df_ano.sort_values("Score_Ajustado", ascending=False).reset_index(drop=True)
        lider_ano = str(df_ano.loc[0, "ticker"])

        final_scores: List[float] = []
        decay_list: List[float] = []
        crowd_list: List[float] = []

        for _, row in df_ano.iterrows():
            tk = str(row["ticker"])

            if tk == lider_ano:
                anos_lider[tk] = anos_lider.get(tk, 0) + 1
            else:
                anos_lider[tk] = 0

            decay_factor = 1.0 - min(float(decay_per_year) * max(anos_lider[tk] - 1, 0), float(decay_cap))
            decay_factor = float(np.clip(decay_factor, 0.0, 1.0))

            crowd = float(row.get("Penalty_Crowd", 1.0))
            if not np.isfinite(crowd):
                crowd = 1.0
            crowd = max(crowd, float(crowding_min_factor))

            base = float(row.get("Score_Ajustado", 0.0))
            if not np.isfinite(base):
                base = 0.0

            inst_pen = float(row.get("InstabilityPenalty", 0.0))
            if not np.isfinite(inst_pen):
                inst_pen = 0.0
            inst_pen = float(np.clip(inst_pen, 0.0, float(instability_cap)))

            base = base * (1.0 - inst_pen)
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


__all__ = ["ScoreV3Config", "calcular_score_ajustado_v3", "calcular_score_acumulado_v3"]
