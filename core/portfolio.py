from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _ensure_dt_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    out.index = pd.to_datetime(out.index, errors="coerce")
    out = out[~out.index.isna()].sort_index()
    return out


def _get_price(precos: pd.DataFrame, data: pd.Timestamp, ticker: str) -> Optional[float]:
    if precos is None or precos.empty:
        return None
    if ticker not in precos.columns:
        return None
    if data not in precos.index:
        return None
    v = precos.loc[data, ticker]
    try:
        v = float(v)
    except Exception:
        return None
    if not np.isfinite(v) or v <= 0:
        return None
    return v


def _as_div_series(div: Union[pd.Series, pd.DataFrame, None]) -> pd.Series:
    if div is None:
        return pd.Series(dtype="float64")
    if isinstance(div, pd.DataFrame):
        if div.shape[1] == 0:
            return pd.Series(dtype="float64")
        s = div.iloc[:, 0]
    else:
        s = div
    s = pd.to_numeric(s, errors="coerce")
    s.index = pd.to_datetime(s.index, errors="coerce")
    s = s.dropna()
    return s.astype(float)


def _cost_factor(fee_bps: float, slippage_bps: float) -> float:
    """Retorna fator multiplicativo (1 - custo_total)."""
    fb = float(fee_bps) / 10000.0
    sb = float(slippage_bps) / 10000.0
    total = max(0.0, fb) + max(0.0, sb)
    # limita custo total para evitar absurdo
    total = min(total, 0.20)
    return 1.0 - total


def encontrar_proxima_data_valida(data_aporte: pd.Timestamp, precos: pd.DataFrame) -> Optional[pd.Timestamp]:
    if precos is None or precos.empty:
        return None
    data_aporte = pd.Timestamp(data_aporte)
    idx = precos.index
    if data_aporte in idx:
        return data_aporte
    prox = idx[idx >= data_aporte]
    if len(prox) == 0:
        return None
    return prox[0]


def _build_monthly_schedule(precos: pd.DataFrame, anos: Sequence[int], start_year_offset: int = 1) -> List[pd.Timestamp]:
    if precos is None or precos.empty or not anos:
        return []
    datas: List[pd.Timestamp] = []
    for ano in sorted(set(int(a) for a in anos)):
        for mes in range(1, 13):
            data_nominal = pd.Timestamp(f"{ano + start_year_offset}-{mes:02d}-01")
            d = encontrar_proxima_data_valida(data_nominal, precos)
            if d is not None:
                datas.append(d)
    return sorted(set(datas))


def gerir_carteira_simples(
    precos: pd.DataFrame,
    tickers: Sequence[str],
    datas_aportes: Sequence[pd.Timestamp],
    dividendos_dict: Optional[Dict[str, Union[pd.Series, pd.DataFrame]]] = None,
    aporte_mensal: float = 1000.0,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
) -> pd.Series:
    precos = _ensure_dt_index(precos)
    tickers = [t for t in tickers if t in precos.columns]
    if precos.empty or not tickers or not datas_aportes:
        return pd.Series(dtype="float64")

    dividendos_dict = dividendos_dict or {}
    divs = {t: _as_div_series(dividendos_dict.get(t)) for t in tickers}
    cf = _cost_factor(fee_bps, slippage_bps)

    carteira = {t: 0.0 for t in tickers}
    carteira_hist: Dict[pd.Timestamp, Dict[str, float]] = {}

    for data0 in datas_aportes:
        data_aporte = encontrar_proxima_data_valida(pd.Timestamp(data0), precos)
        if data_aporte is None:
            continue

        aporte_por_ticker = float(aporte_mensal) / len(tickers)

        for t in tickers:
            px = _get_price(precos, data_aporte, t)
            if px is None:
                continue

            s = divs.get(t, pd.Series(dtype="float64"))
            div_mes = float(s[(s.index.year == data_aporte.year) & (s.index.month == data_aporte.month)].sum()) if not s.empty else 0.0
            reinvest = div_mes * carteira[t]  # reinvestimento “sem custo” (aproximação)

            aporte_total = (aporte_por_ticker * cf) + reinvest
            carteira[t] += aporte_total / px

        carteira_hist[data_aporte] = carteira.copy()

    patrimonio = pd.Series(index=precos.index, dtype="float64")
    datas_hist = sorted(carteira_hist.keys())

    if not datas_hist:
        return patrimonio

    last_snapshot: Optional[Dict[str, float]] = None
    snap_i = 0

    for d in precos.index:
        while snap_i < len(datas_hist) and datas_hist[snap_i] <= d:
            last_snapshot = carteira_hist[datas_hist[snap_i]]
            snap_i += 1

        if last_snapshot:
            total = 0.0
            for t in tickers:
                px = _get_price(precos, d, t)
                if px is not None:
                    total += last_snapshot.get(t, 0.0) * px
            patrimonio.loc[d] = total

    return patrimonio.ffill()


def gerir_carteira_todas_empresas(
    precos: pd.DataFrame,
    tickers: Sequence[str],
    datas_aportes: Sequence[pd.Timestamp],
    dividendos_dict: Dict[str, Union[pd.Series, pd.DataFrame]],
    aporte_mensal: float = 1000.0,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
) -> pd.DataFrame:
    precos = _ensure_dt_index(precos)
    tickers = [t for t in tickers if t in precos.columns]
    if precos.empty or not tickers or not datas_aportes:
        return pd.DataFrame()

    divs = {t: _as_div_series(dividendos_dict.get(t)) for t in tickers}
    cf = _cost_factor(fee_bps, slippage_bps)

    carteira = {t: 0.0 for t in tickers}
    patrimonio: Dict[str, Dict[pd.Timestamp, float]] = {t: {} for t in tickers}

    for data0 in datas_aportes:
        data_aporte = encontrar_proxima_data_valida(pd.Timestamp(data0), precos)
        if data_aporte is None:
            continue

        for t in tickers:
            px = _get_price(precos, data_aporte, t)
            if px is None:
                continue

            s = divs.get(t, pd.Series(dtype="float64"))
            div_mes = float(s[(s.index.year == data_aporte.year) & (s.index.month == data_aporte.month)].sum()) if not s.empty else 0.0
            reinvest = div_mes * carteira[t]

            aporte_total = (float(aporte_mensal) * cf) + reinvest
            carteira[t] += aporte_total / px
            patrimonio[t][data_aporte] = carteira[t] * px

    return pd.DataFrame.from_dict(patrimonio, orient="columns").sort_index()


def calcular_patrimonio_selic_macro(
    dados_macro: pd.DataFrame,
    datas_aportes: Sequence[pd.Timestamp],
    aporte_mensal: float = 1000.0,
) -> pd.DataFrame:
    if dados_macro is None or dados_macro.empty or not datas_aportes:
        return pd.DataFrame(columns=["Tesouro Selic"])

    dm = dados_macro.copy()
    if "Data" not in dm.columns and dm.index.name == "Data":
        dm = dm.reset_index()

    dm["Data"] = pd.to_datetime(dm["Data"], errors="coerce")
    dm = dm.dropna(subset=["Data"])
    dm = dm.set_index("Data").sort_index()

    if "Selic" not in dm.columns:
        return pd.DataFrame(columns=["Tesouro Selic"])

    datas = sorted(pd.to_datetime(list(datas_aportes)))
    df_patr = pd.DataFrame(index=datas, columns=["Tesouro Selic"], dtype="float64")

    saldo = 0.0
    for d in datas:
        ano_ref = int(d.year)
        taxa_ano = dm.loc[dm.index.year == ano_ref, "Selic"]
        if taxa_ano.empty:
            prev = dm.loc[dm.index <= d, "Selic"]
            if prev.empty:
                continue
            taxa = float(prev.iloc[-1]) / 100.0
        else:
            taxa = float(taxa_ano.iloc[0]) / 100.0

        taxa_mensal = (1.0 + taxa) ** (1.0 / 12.0) - 1.0
        saldo = (saldo + float(aporte_mensal)) * (1.0 + taxa_mensal)
        df_patr.loc[d, "Tesouro Selic"] = saldo

    return df_patr.sort_index()


def gerir_carteira(
    precos: pd.DataFrame,
    df_scores: pd.DataFrame,
    lideres_por_ano: pd.DataFrame,
    dividendos_dict: Dict[str, Union[pd.Series, pd.DataFrame]],
    aporte_mensal: float = 1000.0,
    deterioracao_limite: float = 0.0,
    registrar_eventos: bool = False,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
):
    precos = _ensure_dt_index(precos)

    if precos is None or precos.empty:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    if df_scores is None or df_scores.empty:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    if lideres_por_ano is None or lideres_por_ano.empty:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    divs = {t: _as_div_series(dividendos_dict.get(t)) for t in precos.columns}
    cf = _cost_factor(fee_bps, slippage_bps)

    anos_scores = sorted(int(a) for a in pd.to_numeric(df_scores.get("Ano"), errors="coerce").dropna().unique())
    if not anos_scores:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    datas_aportes = _build_monthly_schedule(precos, anos_scores, start_year_offset=1)
    if not datas_aportes:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    lider_map = {}
    for _, r in lideres_por_ano.dropna(subset=["Ano", "ticker"]).iterrows():
        try:
            lider_map[int(r["Ano"])] = str(r["ticker"])
        except Exception:
            continue

    df_scores2 = df_scores.copy()
    df_scores2["Ano"] = pd.to_numeric(df_scores2["Ano"], errors="coerce")
    df_scores2["ticker"] = df_scores2["ticker"].astype(str)
    df_scores2["Score_Ajustado"] = pd.to_numeric(df_scores2["Score_Ajustado"], errors="coerce")

    score_map: Dict[Tuple[int, str], float] = {}
    for _, r in df_scores2.dropna(subset=["Ano", "ticker", "Score_Ajustado"]).iterrows():
        score_map[(int(r["Ano"]), str(r["ticker"]))] = float(r["Score_Ajustado"])

    ano_base = anos_scores[0]

    carteira = defaultdict(float)
    aporte_acumulado = 0.0
    registros: List[dict] = []
    eventos = [] if registrar_eventos else None

    lideres_atuais: List[str] = []
    ano_ref_atual: Optional[int] = None

    for data_sinal in datas_aportes:
        ano_ref = int(data_sinal.year - 1)

        if ano_ref != ano_ref_atual:
            ano_ref_atual = ano_ref
            novo_lider = lider_map.get(ano_ref)
            if novo_lider and (novo_lider not in lideres_atuais):
                lideres_atuais.append(novo_lider)
                if registrar_eventos:
                    eventos.append({"data": data_sinal.strftime("%Y-%m"), "tipo": "entrada", "ticker": novo_lider})

        # dividendos (reinvest sem custo)
        for tk in list(carteira.keys()):
            if carteira[tk] <= 0:
                continue
            s = divs.get(tk, pd.Series(dtype="float64"))
            if s.empty:
                continue
            div_mes = float(s[(s.index.year == data_sinal.year) & (s.index.month == data_sinal.month)].sum())
            if div_mes <= 0:
                continue
            px = _get_price(precos, data_sinal, tk)
            if px is None:
                continue
            valor_reinvestido = div_mes * carteira[tk]
            carteira[tk] += valor_reinvestido / px

        # aporte com custo
        total_a_aportar = float(aporte_mensal) + float(aporte_acumulado)
        aporte_acumulado = 0.0

        if lideres_atuais:
            aporte_por_lider = total_a_aportar / len(lideres_atuais)
            for lider in lideres_atuais:
                px = _get_price(precos, data_sinal, lider)
                if px is None:
                    aporte_acumulado += aporte_por_lider
                    continue
                carteira[lider] += (aporte_por_lider * cf) / px
        else:
            aporte_acumulado += total_a_aportar

        # deterioração: venda/compra com custo
        lider_destino = lider_map.get(ano_ref)
        if lider_destino:
            for antiga in list(carteira.keys()):
                if antiga in lideres_atuais:
                    continue

                s_ini = score_map.get((ano_base, antiga))
                s_atual = score_map.get((ano_ref, antiga))
                if s_ini is None or s_atual is None or s_ini == 0:
                    continue

                razao = float(s_atual) / float(s_ini)
                if razao < float(deterioracao_limite):
                    px_venda = _get_price(precos, data_sinal, antiga)
                    px_dest = _get_price(precos, data_sinal, lider_destino)
                    if px_venda is None or px_dest is None:
                        continue

                    valor_bruto = carteira[antiga] * px_venda
                    valor_liquido = valor_bruto * cf  # custo de venda
                    qtd_nova = (valor_liquido * cf) / px_dest  # custo de compra

                    carteira.pop(antiga, None)
                    carteira[lider_destino] += qtd_nova

                    if registrar_eventos:
                        eventos.append({"data": data_sinal.strftime("%Y-%m"), "tipo": "saida", "ticker": antiga})

        registro = {"date": data_sinal}
        total = 0.0
        for tk, qtd in carteira.items():
            px = _get_price(precos, data_sinal, tk)
            if px is None:
                continue
            val = qtd * px
            registro[tk] = val
            total += val
        registro["Patrimônio"] = total
        registros.append(registro)

    if not registros:
        return (pd.DataFrame(), datas_aportes) if not registrar_eventos else (pd.DataFrame(), datas_aportes, eventos)

    df_patrimonio = pd.DataFrame(registros).set_index("date").sort_index().ffill()

    if "Patrimônio" in df_patrimonio.columns:
        df_patrimonio = df_patrimonio[df_patrimonio["Patrimônio"].fillna(0) != 0]

    if registrar_eventos:
        return df_patrimonio, datas_aportes, eventos

    return df_patrimonio, datas_aportes


__all__ = [
    "encontrar_proxima_data_valida",
    "gerir_carteira_simples",
    "gerir_carteira_todas_empresas",
    "calcular_patrimonio_selic_macro",
    "gerir_carteira",
]
