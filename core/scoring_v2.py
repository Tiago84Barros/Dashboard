from __future__ import annotations

"""
Scoring v2 (robusto): percentil por grupo (segmento), winsorização, robustez e
penalidade de instabilidade. Mantém o v1 intacto em core/scoring.py.

Uso recomendado:
- advanced.py: permitir alternar entre v1 e v2 sem mudar layout.
"""

import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from core.scoring import (
    _ensure_year,
    _to_numeric_series,
    calc_crowding_penalty,
    calcular_metricas_historicas_simplificadas,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Helpers estatísticos robustos
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
    Score ∈ [0,1] via rank percentil (método average).
    - Se melhor_alto=False, inverte (menor -> melhor).
    """
    x = _to_numeric_series(series)
    # rank(pct=True) já entrega 0..1 (com NaN -> NaN)
    pct = x.rank(pct=True, method="average")
    pct = pct.fillna(0.5)  # neutro
    return pct if melhor_alto else (1.0 - pct)


def robust_volatility_penalty(
    series: pd.Series,
    p_low: float = 0.05,
    p_high: float = 0.95,
) -> pd.Series:
    """
    Produz penalidade ∈ [0,1] (maior = mais instável).
    Implementação: winsoriza, mede dispersão robusta (IQR/MAD proxy) e reescala.
    """
    x = winsorize_series(series, p_low=p_low, p_high=p_high)
    v = _to_numeric_series(x)
    if v.dropna().empty:
        return pd.Series(0.0, index=series.index)

    q1 = v.quantile(0.25)
    q3 = v.quantile(0.75)
    iqr = float(q3 - q1) if np.isfinite(q1) and np.isfinite(q3) else 0.0

    # normaliza para [0,1] via tanh (robusto)
    pen = np.tanh(abs(iqr))
    if not np.isfinite(pen):
        pen = 0.0
    return pd.Series(float(pen), index=series.index)


# ─────────────────────────────────────────────────────────────
# Score v2: agregação por pesos usando percentil por grupo
# ─────────────────────────────────────────────────────────────

def calcular_score_ajustado_v2(
    df: pd.DataFrame,
    pesos_utilizados: Mapping[str, Mapping[str, Any]],
    group_col: str = "SEGMENTO",
    winsor_p_low: float = 0.05,
    winsor_p_high: float = 0.95,
    add_instability_penalty: bool = True,
    instability_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Calcula Score_Ajustado v2:
    - winsoriza indicadores
    - percentil por grupo (segmento) e ano (o df já deve ser recortado para o ano)
    - penalidade opcional de instabilidade (colunas *_volatility_penalty consumidas pelo engine)
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    if group_col not in out.columns:
        out[group_col] = "OUTROS"

    # (1) opcional: penalidade de instabilidade (alimentando hooks já existentes no v1)
    if add_instability_penalty:
        if instability_cols is None:
            # foco em “consistência operacional”
            instability_cols = ["ROIC_mean", "Margem_Operacional_mean", "Margem_Liquida_mean"]
        for col in instability_cols:
            if col in out.columns:
                pen_col = col.replace("_mean", "_volatility_penalty")
                out[pen_col] = robust_volatility_penalty(out[col], p_low=winsor_p_low, p_high=winsor_p_high)

    # (2) score por percentil dentro do grupo
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

        # rank/percentil por grupo
        score_col = f"{col}_pct"
        out[score_col] = (
            out.groupby(group_col, dropna=False)[col]
            .transform(lambda s: percentile_score(s, melhor_alto=melhor_alto))
        )

        # aplica penalidade hook (se existir)
        pen_col = col.replace("_mean", "_volatility_penalty")
        if pen_col in out.columns:
            out[score_col] = out[score_col] * (1.0 - _to_numeric_series(out[pen_col]).fillna(0.0).clip(0, 1))

        out["Score_Ajustado"] += out[score_col] * peso

    return out


# ─────────────────────────────────────────────────────────────
# Score acumulado v2 (mesma lógica do v1, trocando o agregador)
# ─────────────────────────────────────────────────────────────

def calcular_score_acumulado_v2(
    lista_empresas: Sequence[Mapping[str, Any]],
    group_map: Mapping[str, str],  # ticker -> SEGMENTO (ou outra granularidade)
    pesos_utilizados: Mapping[str, Mapping[str, Any]],
    anos_minimos: int = 4,
    group_col: str = "SEGMENTO",
    winsor_p_low: float = 0.05,
    winsor_p_high: float = 0.95,
) -> pd.DataFrame:
    """
    Versão v2 do acumulado:
    - usa percentil por group_col (default: SEGMENTO)
    - mantém crowding + decay de liderança (v1), porque são bons e auditáveis
    """
    if not lista_empresas:
        return pd.DataFrame(columns=["Ano", "ticker", "Score_Ajustado"])

    # anos disponíveis a partir de multiplos
    anos: List[int] = []
    for emp in lista_empresas:
        dfm = emp.get("multiplos")
        if isinstance(dfm, pd.DataFrame) and not dfm.empty:
            dfm2 = _ensure_year(dfm, date_col="Data", year_col="Ano")
            anos.extend([int(a) for a in dfm2["Ano"].dropna().unique() if np.isfinite(a)])

    anos_disponiveis = sorted(set(anos))
    if len(anos_disponiveis) <= anos_minimos:
        return pd.DataFrame(columns=["Ano", "ticker", "Score_Ajustado"])

    resultados: List[pd.DataFrame] = []
    anos_lider: Dict[str, int] = {}

    for idx in range(anos_minimos, len(anos_disponiveis)):
        ano = int(anos_disponiveis[idx])

        # Pré-coleta P/VP por grupo no ano (crowding)
        grupo_to_pvp: Dict[str, List[float]] = {}
        for emp in lista_empresas:
            tk = str(emp.get("ticker", "")).strip()
            dfm = emp.get("multiplos")
            if not tk or not isinstance(dfm, pd.DataFrame) or dfm.empty:
                continue
            dfm2 = _ensure_year(dfm, date_col="Data", year_col="Ano")
            dfm_ano = dfm2[dfm2["Ano"] == ano]
            if dfm_ano.empty or "P/VP" not in dfm_ano.columns:
                continue
            grp = group_map.get(tk, "OUTROS")
            vals = _to_numeric_series(dfm_ano["P/VP"]).dropna().tolist()
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

            grp = group_map.get(ticker, "OUTROS")
            df_grp_ano = pd.DataFrame({"P/VP": grupo_to_pvp.get(grp, [])})
            crowd_pen = calc_crowding_penalty(df_grp_ano, coluna="P/VP")

            metricas = calcular_metricas_historicas_simplificadas(df_mult_hist, df_dre_hist)
            dados_ano.append(
                {"ticker": ticker, "Ano": ano, group_col: grp, **metricas, "Penalty_Crowd": crowd_pen}
            )

        df_ano = pd.DataFrame(dados_ano)
        if df_ano.empty:
            continue

        # v2: percentil por grupo
        df_ano = calcular_score_ajustado_v2(
            df_ano,
            pesos_utilizados=pesos_utilizados,
            group_col=group_col,
            winsor_p_low=winsor_p_low,
            winsor_p_high=winsor_p_high,
            add_instability_penalty=True,
        )

        # mantém crowding + decay
        df_ano = df_ano.sort_values("Score_Ajustado", ascending=False).reset_index(drop=True)
        lider_ano = str(df_ano.loc[0, "ticker"])

        final_scores: List[float] = []
        decay_list: List[float] = []

        for _, row in df_ano.iterrows():
            tk = str(row["ticker"])

            if tk == lider_ano:
                anos_lider[tk] = anos_lider.get(tk, 0) + 1
            else:
                anos_lider[tk] = 0

            decay_factor = 1.0 - min(0.03 * max(anos_lider[tk] - 1, 0), 0.25)
            crowd = float(row.get("Penalty_Crowd", 1.0))
            base = float(row.get("Score_Ajustado", 0.0))

            decay_list.append(decay_factor)
            final_scores.append(base * crowd * decay_factor)

        df_ano["Penalty_Decay"] = decay_list
        df_ano["Score_Ajustado"] = final_scores

        resultados.append(df_ano[["Ano", "ticker", "Score_Ajustado"]])

    if resultados:
        return pd.concat(resultados, ignore_index=True)

    return pd.DataFrame(columns=["Ano", "ticker", "Score_Ajustado"])
