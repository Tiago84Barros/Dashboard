from __future__ import annotations

"""Core de scoring fundamentalista (engine).

- Sem dependência de Streamlit.
- Tratamento robusto de NaN/Inf e colunas ausentes.
- Funções puras e testáveis.

Compatibilidade: mantém `calcular_score_acumulado(...)`.
"""

import collections
import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import TheilSenRegressor

logger = logging.getLogger(__name__)


def _to_numeric_series(s: pd.Series) -> pd.Series:
    out = pd.to_numeric(s, errors="coerce")
    return out.replace([np.inf, -np.inf], np.nan)


def z_score_normalize(series: pd.Series, melhor_alto: bool) -> pd.Series:
    s = _to_numeric_series(series)
    mean = s.mean(skipna=True)
    std = s.std(skipna=True)
    if not np.isfinite(std) or std == 0:
        return pd.Series(0.0, index=series.index)
    z = (s - mean) / std
    z = z.fillna(0.0)
    return z if melhor_alto else -z


def slope_regressao_log(df: pd.DataFrame, col: str, year_col: str = "Ano") -> float:
    if df is None or df.empty:
        return 0.0
    if year_col not in df.columns or col not in df.columns:
        return 0.0

    tmp = df[[year_col, col]].copy()
    tmp[year_col] = pd.to_numeric(tmp[year_col], errors="coerce")
    tmp[col] = pd.to_numeric(tmp[col], errors="coerce")
    tmp = tmp.dropna(subset=[year_col, col])
    tmp = tmp[tmp[col] > 0]
    if len(tmp) < 2:
        return 0.0

    X = tmp[[year_col]].astype(float).values
    y = np.log(tmp[col].astype(float).values)

    model = TheilSenRegressor(random_state=42)
    try:
        model.fit(X, y)
        slope = float(model.coef_[0])
        return slope if np.isfinite(slope) else 0.0
    except Exception as e:
        logger.exception("Falha no TheilSenRegressor para col=%s: %s", col, e)
        return 0.0


def slope_to_growth_percent(slope: float) -> float:
    if not np.isfinite(slope):
        return 0.0
    return float(np.exp(slope) - 1.0)


def calcular_media_e_std(df: pd.DataFrame, col: str) -> Tuple[float, float]:
    if df is None or df.empty or col not in df.columns:
        return 0.0, 0.0
    s = _to_numeric_series(df[col]).dropna()
    if s.empty:
        return 0.0, 0.0
    mean = float(s.mean())
    std = float(s.std())
    if not np.isfinite(mean):
        mean = 0.0
    if not np.isfinite(std):
        std = 0.0
    return mean, std


def _ensure_year(df: pd.DataFrame, date_col: str = "Data", year_col: str = "Ano") -> pd.DataFrame:
    out = df.copy()
    if year_col not in out.columns:
        if date_col in out.columns:
            out[year_col] = pd.to_datetime(out[date_col], errors="coerce").dt.year
        else:
            out[year_col] = np.nan
    return out


def calcular_metricas_historicas_simplificadas(df_mult: pd.DataFrame, df_dre: pd.DataFrame) -> Dict[str, float]:
    df_mult2 = _ensure_year(df_mult, date_col="Data", year_col="Ano")
    df_dre2 = _ensure_year(df_dre, date_col="Data", year_col="Ano")

    df_mult2 = df_mult2.dropna(subset=["Ano"]).sort_values("Ano")
    df_dre2 = df_dre2.dropna(subset=["Ano"]).sort_values("Ano")

    metrics: Dict[str, float] = {}

    mult_cols = [
        "Margem_Liquida",
        "Margem_Operacional",
        "ROE",
        "ROA",
        "ROIC",
        "P/VP",
        "Endividamento_Total",
        "Alavancagem_Financeira",
        "Liquidez_Corrente",
        "DY",
    ]
    for col in mult_cols:
        mean, std = calcular_media_e_std(df_mult2, col)
        metrics[f"{col}_mean"] = mean
        metrics[f"{col}_std"] = std

    dre_cols = ["Receita_Liquida", "Lucro_Liquido", "Patrimonio_Liquido", "Divida_Liquida", "Caixa_Liquido"]
    for col in dre_cols:
        slope = slope_regressao_log(df_dre2, col, year_col="Ano")
        metrics[f"{col}_slope_log"] = slope
        metrics[f"{col}_growth_approx"] = slope_to_growth_percent(slope)

    return metrics


def calc_crowding_penalty(
    df_setor: pd.DataFrame,
    coluna: str = "P/VP",
    floor: float = 0.85,
    ceil: float = 1.50,
) -> float:
    if df_setor is None or df_setor.empty or coluna not in df_setor.columns:
        return 1.0
    s = _to_numeric_series(df_setor[coluna]).dropna()
    if len(s) < 2:
        return 1.0
    dispersion = float(s.std())
    media = float(s.mean())
    if not np.isfinite(dispersion) or not np.isfinite(media) or media == 0:
        return 1.0
    crowd_score = 1.0 - float(np.tanh(abs(dispersion / media)))
    crowd_score = min(max(crowd_score, 0.0), 1.0)
    return float(floor + (ceil - floor) * crowd_score)


def penalizar_plato(
    df_scores: pd.DataFrame,
    precos_mensal: pd.DataFrame,
    meses: int = 18,
    penal: float = 0.25,
) -> pd.DataFrame:
    if df_scores is None or df_scores.empty:
        return df_scores
    if precos_mensal is None or precos_mensal.empty:
        return df_scores
    required = {"Ano", "ticker", "Score_Ajustado"}
    if not required.issubset(df_scores.columns):
        return df_scores

    penal = float(min(max(penal, 0.0), 0.95))
    ret_m = precos_mensal.pct_change(periods=int(meses))
    out = df_scores.copy()

    for ano in sorted(out["Ano"].dropna().unique()):
        ano_int = int(ano)
        mask_year = precos_mensal.index.year == ano_int
        if not mask_year.any():
            continue
        data_fim = precos_mensal.index[mask_year].max()
        if pd.isna(data_fim):
            continue

        linha = ret_m.loc[data_fim]
        ret_mediana = float(pd.to_numeric(linha, errors="coerce").median(skipna=True))
        if not np.isfinite(ret_mediana):
            continue

        mask = out["Ano"] == ano
        for idx, row in out.loc[mask, ["ticker", "Score_Ajustado"]].iterrows():
            tk = row["ticker"]
            if tk not in ret_m.columns:
                continue
            r = ret_m.loc[data_fim, tk]
            if pd.notna(r) and np.isfinite(r) and float(r) < ret_mediana:
                out.at[idx, "Score_Ajustado"] = float(out.at[idx, "Score_Ajustado"]) * (1.0 - penal)

    return out


def calcular_score_ajustado(df: pd.DataFrame, pesos_utilizados: Mapping[str, Mapping[str, Any]]) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    out["Score_Ajustado"] = 0.0

    for col, cfg in pesos_utilizados.items():
        if col not in out.columns:
            continue

        peso = float(cfg.get("peso", 0.0))
        melhor_alto = bool(cfg.get("melhor_alto", True))
        norm_col = f"{col}_norm"

        # Z-score normaliza o indicador bruto (ranking relativo entre empresas).
        out[norm_col] = z_score_normalize(out[col], melhor_alto=melhor_alto)

        # Penalidade de volatilidade aplicada APÓS normalização para não ser
        # cancelada pela relativização do z-score.
        vol_col = col.replace("_mean", "_volatility_penalty")
        if vol_col in out.columns:
            vol_pen = _to_numeric_series(out[vol_col]).fillna(0.0).clip(0.0, 1.0)
            out[norm_col] = out[norm_col] * (1.0 - vol_pen)

        out["Score_Ajustado"] += out[norm_col] * peso

    return out


def calcular_score_acumulado(
    lista_empresas: Sequence[Mapping[str, Any]],
    setores_empresa: Mapping[str, str],
    pesos_utilizados: Mapping[str, Mapping[str, Any]],
    dados_macro: Optional[pd.DataFrame] = None,
    anos_minimos: int = 4,
    publication_lag_years: int = 1,
) -> pd.DataFrame:
    """Calcula Score_Ajustado ao longo dos anos.

    publication_lag_years:
      - 0 mantém comportamento anterior (usa Ano<=ano).
      - 1 (default) usa Ano<=ano-1 para simular defasagem anti look-ahead.
    """
    if not lista_empresas:
        return pd.DataFrame(columns=["Ano", "ticker", "Score_Ajustado"])

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
    anos_lider = collections.defaultdict(int)
    lag = int(max(publication_lag_years, 0))

    for idx in range(anos_minimos, len(anos_disponiveis)):
        ano = int(anos_disponiveis[idx])
        ano_cutoff = ano - lag

        setor_to_pvp: Dict[str, List[float]] = collections.defaultdict(list)
        for emp in lista_empresas:
            tk = str(emp.get("ticker", "")).strip()
            dfm = emp.get("multiplos")
            if not tk or not isinstance(dfm, pd.DataFrame) or dfm.empty:
                continue
            dfm2 = _ensure_year(dfm, date_col="Data", year_col="Ano")
            dfm_ano = dfm2[dfm2["Ano"] == ano_cutoff] if lag > 0 else dfm2[dfm2["Ano"] == ano]
            if dfm_ano.empty or "P/VP" not in dfm_ano.columns:
                continue
            setor = setores_empresa.get(tk, "OUTROS")
            vals = _to_numeric_series(dfm_ano["P/VP"]).dropna().tolist()
            setor_to_pvp[setor].extend([float(v) for v in vals if np.isfinite(v)])

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

            # cutoff anti look-ahead
            cut = ano_cutoff if lag > 0 else ano
            df_mult_hist = df_mult2[df_mult2["Ano"] <= cut].copy()
            df_dre_hist = df_dre2[df_dre2["Ano"] <= cut].copy()
            if df_mult_hist.empty or df_dre_hist.empty:
                continue

            setor = setores_empresa.get(ticker, "OUTROS")
            df_setor_ano = pd.DataFrame({"P/VP": setor_to_pvp.get(setor, [])})
            crowd_pen = calc_crowding_penalty(df_setor_ano, coluna="P/VP")

            metricas = calcular_metricas_historicas_simplificadas(df_mult_hist, df_dre_hist)
            dados_ano.append({"ticker": ticker, "Ano": ano, **metricas, "Penalty_Crowd": crowd_pen})

        df_ano = pd.DataFrame(dados_ano)
        if df_ano.empty:
            continue

        df_ano = calcular_score_ajustado(df_ano, pesos_utilizados)

        df_ano = df_ano.sort_values("Score_Ajustado", ascending=False).reset_index(drop=True)
        lider_ano = str(df_ano.loc[0, "ticker"])

        final_scores: List[float] = []
        decay_list: List[float] = []
        for _, row in df_ano.iterrows():
            tk = str(row["ticker"])
            if tk == lider_ano:
                anos_lider[tk] += 1
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


__all__ = [
    "z_score_normalize",
    "slope_regressao_log",
    "slope_to_growth_percent",
    "calcular_media_e_std",
    "calcular_metricas_historicas_simplificadas",
    "calc_crowding_penalty",
    "penalizar_plato",
    "calcular_score_ajustado",
    "calcular_score_acumulado",
]
