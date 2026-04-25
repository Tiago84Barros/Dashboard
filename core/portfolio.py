from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Limite defensivo para dividendos POR AÇÃO (R$/ação) em um mês.
# Se uma fonte retornar "Dividendos totais" (CVM/DFC) por engano,
# isso evita explosões irreais no backtest.
DIV_POR_ACAO_MAX = 20.0


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
    """
    Converte dividendos em Series indexada por datetime, com valores numéricos.
    IMPORTANTE: este motor assume que o valor é DIVIDENDO POR AÇÃO (R$/ação).
    """
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


def _div_mes_por_acao_sanitizado(
    s: pd.Series,
    ano: int,
    mes: int,
    ticker: str,
    px_ref: Optional[float] = None,
    hard_max: float = DIV_POR_ACAO_MAX,
) -> float:
    """
    Retorna o dividendo mensal POR AÇÃO (R$/ação) e rejeita valores com cara de "total contábil".

    Regras:
    - Trava absoluta: div_mes > hard_max => zera.
    - Regra auxiliar: se div_mes > 50% do preço do mês, zera (sinal forte de unidade errada).
    """
    if s is None or s.empty:
        return 0.0

    div_mes = float(s[(s.index.year == ano) & (s.index.month == mes)].sum())
    if not np.isfinite(div_mes) or div_mes <= 0:
        return 0.0

    # trava absoluta
    if div_mes > hard_max:
        logger.warning(
            "Dividendos suspeitos (provável TOTAL e não por ação) | ticker=%s ano=%s mes=%s div_mes=%.6f (zerado)",
            ticker, ano, mes, div_mes
        )
        return 0.0

    # regra auxiliar vs preço
    if px_ref is not None and np.isfinite(px_ref) and px_ref > 0:
        if div_mes > 0.5 * px_ref:
            logger.warning(
                "Dividendos fora de escala vs preço | ticker=%s ano=%s mes=%s div_mes=%.6f px=%.6f (zerado)",
                ticker, ano, mes, div_mes, px_ref
            )
            return 0.0

    return div_mes


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
    """
    Carteira simples: aporta mensalmente distribuindo igualmente entre tickers.
    Reinvestimento: dividendos POR AÇÃO do mês (R$/ação), com sanitização.
    """
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
            div_mes = _div_mes_por_acao_sanitizado(
                s=s,
                ano=int(data_aporte.year),
                mes=int(data_aporte.month),
                ticker=t,
                px_ref=px,
            )

            reinvest = div_mes * carteira[t]  # reinvest sem custo (aproximação)
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
    """
    Simula aporte mensal POR AÇÃO (cada ticker recebe aporte_mensal).
    Reinvestimento: dividendos POR AÇÃO do mês (R$/ação), com sanitização.
    """
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
            div_mes = _div_mes_por_acao_sanitizado(
                s=s,
                ano=int(data_aporte.year),
                mes=int(data_aporte.month),
                ticker=t,
                px_ref=px,
            )

            reinvest = div_mes * carteira[t]
            aporte_total = (float(aporte_mensal) * cf) + reinvest
            carteira[t] += aporte_total / px
            patrimonio[t][data_aporte] = carteira[t] * px

    return pd.DataFrame.from_dict(patrimonio, orient="columns").sort_index()



def gerir_carteira_equal_weight_segmento(
    precos: pd.DataFrame,
    tickers: Sequence[str],
    datas_aportes: Sequence[pd.Timestamp],
    dividendos_dict: Optional[Dict[str, Union[pd.Series, pd.DataFrame]]] = None,
    aporte_mensal: float = 1000.0,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
) -> pd.Series:
    """
    Benchmark investível equal-weight do segmento.

    O aporte_mensal é TOTAL para o segmento e é dividido igualmente
    entre todos os tickers elegíveis. Não usa ranking, liderança ou score.
    """
    precos = _ensure_dt_index(precos)
    if precos is None or precos.empty or not tickers or not datas_aportes:
        return pd.Series(dtype="float64", name="Benchmark Equal-Weight Segmento")

    dividendos_dict = dividendos_dict or {}

    tickers_exec: List[str] = []
    for tk in tickers:
        tk_str = str(tk).strip()
        col = _resolve_ticker_col(precos, tk_str)
        if col is None:
            col = _resolve_ticker_col(precos, f"{tk_str.replace('.SA', '')}.SA")
        if col is not None and col not in tickers_exec:
            tickers_exec.append(col)

    if not tickers_exec:
        return pd.Series(dtype="float64", name="Benchmark Equal-Weight Segmento")

    divs: Dict[str, pd.Series] = {}
    for col in tickers_exec:
        k = _resolve_div_key(dividendos_dict, col)
        divs[col] = _as_div_series(dividendos_dict.get(k)) if k is not None else pd.Series(dtype="float64")

    cf = _cost_factor(fee_bps, slippage_bps)
    carteira = {t: 0.0 for t in tickers_exec}
    patrimonio_hist: Dict[pd.Timestamp, float] = {}

    for data0 in datas_aportes:
        data_aporte = encontrar_proxima_data_valida(pd.Timestamp(data0), precos)
        if data_aporte is None:
            continue

        ativos_validos = [t for t in tickers_exec if _get_price(precos, data_aporte, t) is not None]
        if not ativos_validos:
            continue

        aporte_por_ticker = float(aporte_mensal) / len(ativos_validos)

        for t in ativos_validos:
            px = _get_price(precos, data_aporte, t)
            if px is None:
                continue

            s = divs.get(t, pd.Series(dtype="float64"))
            div_mes = _div_mes_por_acao_sanitizado(
                s=s,
                ano=int(data_aporte.year),
                mes=int(data_aporte.month),
                ticker=t,
                px_ref=px,
            )

            reinvest = div_mes * carteira[t]
            aporte_total = (aporte_por_ticker * cf) + reinvest
            carteira[t] += aporte_total / px

        total = 0.0
        for t, qtd in carteira.items():
            px = _get_price(precos, data_aporte, t)
            if px is not None:
                total += qtd * px

        patrimonio_hist[data_aporte] = total

    if not patrimonio_hist:
        return pd.Series(dtype="float64", name="Benchmark Equal-Weight Segmento")

    return pd.Series(patrimonio_hist, name="Benchmark Equal-Weight Segmento").sort_index().ffill()

def calcular_patrimonio_selic_macro(
    dados_macro: pd.DataFrame,
    datas_aportes: Sequence[pd.Timestamp],
    aporte_mensal: float = 1000.0,
) -> pd.DataFrame:
    if dados_macro is None or dados_macro.empty or not datas_aportes:
        return pd.DataFrame(columns=["Tesouro Selic"])

    dm = dados_macro.copy()
    dm.columns = [str(c).strip().lower() for c in dm.columns]

    index_name = "" if dm.index.name is None else str(dm.index.name).strip().lower()
    if "data" not in dm.columns and index_name == "data":
        dm = dm.reset_index()
        dm.columns = [str(c).strip().lower() for c in dm.columns]

    if "data" not in dm.columns:
        return pd.DataFrame(columns=["Tesouro Selic"])

    dm["data"] = pd.to_datetime(dm["data"], errors="coerce")
    dm = dm.dropna(subset=["data"])
    dm = dm.set_index("data").sort_index()

    if "selic" not in dm.columns:
        return pd.DataFrame(columns=["Tesouro Selic"])

    dm["selic"] = pd.to_numeric(dm["selic"], errors="coerce")

    datas = sorted(pd.to_datetime(list(datas_aportes)))
    df_patr = pd.DataFrame(index=datas, columns=["Tesouro Selic"], dtype="float64")

    saldo = 0.0
    for d in datas:
        ano_ref = int(d.year)
        taxa_ano = dm.loc[dm.index.year == ano_ref, "selic"]
        taxa_ano = taxa_ano.dropna()
        if taxa_ano.empty:
            prev = dm.loc[dm.index <= d, "selic"]
            prev = prev.dropna()
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
    """
    Estratégia de líderes: adiciona líder do ano (ano_ref = ano atual - 1),
    aporta mensalmente entre líderes atuais e reinveste dividendos POR AÇÃO (R$/ação) com sanitização.
    """
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

        # Reinvestimento de dividendos (POR AÇÃO), sanitizado
        for tk in list(carteira.keys()):
            if carteira[tk] <= 0:
                continue
            s = divs.get(tk, pd.Series(dtype="float64"))
            if s.empty:
                continue

            px = _get_price(precos, data_sinal, tk)
            if px is None:
                continue

            div_mes = _div_mes_por_acao_sanitizado(
                s=s,
                ano=int(data_sinal.year),
                mes=int(data_sinal.month),
                ticker=tk,
                px_ref=px,
            )
            if div_mes <= 0:
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


def _resolve_ticker_col(precos: pd.DataFrame, ticker: str) -> Optional[str]:
    """
    Resolve variações de ticker (com/sem .SA) para compatibilidade entre módulos.
    Retorna o nome da coluna existente em `precos`, ou None.
    """
    if precos is None or precos.empty:
        return None
    tk = (ticker or "").strip()
    if not tk:
        return None
    if tk in precos.columns:
        return tk
    tk_up = tk.upper()
    if tk_up in precos.columns:
        return tk_up
    if not tk_up.endswith(".SA") and (tk_up + ".SA") in precos.columns:
        return tk_up + ".SA"
    if tk_up.endswith(".SA") and tk_up.replace(".SA", "") in precos.columns:
        return tk_up.replace(".SA", "")
    return None


def _align_ts_to_index(dt_like: Any, index: pd.Index) -> pd.Timestamp:
    """Alinha um timestamp (dt_like) ao fuso horário do índice (DatetimeIndex).

    Evita erro: 'Cannot compare tz-naive and tz-aware datetime-like objects'.
    - Se index é tz-naive: retorna dt tz-naive.
    - Se index é tz-aware: retorna dt tz-aware no mesmo tz do index.
    """
    ts = pd.Timestamp(dt_like)
    if not isinstance(index, pd.DatetimeIndex):
        # fallback: apenas retorna Timestamp sem tz
        return ts.tz_localize(None) if ts.tzinfo is not None else ts

    idx_tz = index.tz
    if idx_tz is None:
        return ts.tz_localize(None) if ts.tzinfo is not None else ts
    # index tz-aware
    if ts.tzinfo is None:
        return ts.tz_localize(idx_tz)
    return ts.tz_convert(idx_tz)


def _resolve_div_key(dividendos_dict: Dict[str, Union[pd.Series, pd.DataFrame]], ticker: str) -> Optional[str]:
    if dividendos_dict is None:
        return None
    tk = (ticker or "").strip()
    if not tk:
        return None
    if tk in dividendos_dict:
        return tk
    tk_up = tk.upper()
    if tk_up in dividendos_dict:
        return tk_up
    if not tk_up.endswith(".SA") and (tk_up + ".SA") in dividendos_dict:
        return tk_up + ".SA"
    if tk_up.endswith(".SA") and tk_up.replace(".SA", "") in dividendos_dict:
        return tk_up.replace(".SA", "")
    return None


def _select_n_heuristica(scores_desc: List[float], eps: float = 0.35) -> int:
    """
    Heurística de N (faixa superior), usando scores já em ordem decrescente.
    """
    if not scores_desc:
        return 1
    if len(scores_desc) == 1:
        return 1
    if len(scores_desc) == 2:
        g12 = float(scores_desc[0] - scores_desc[1])
        return 2 if g12 <= eps else 1

    s1, s2, s3 = float(scores_desc[0]), float(scores_desc[1]), float(scores_desc[2])
    g12 = s1 - s2
    g23 = s2 - s3
    if g12 <= eps and g23 <= eps:
        return 3
    if g12 <= eps and g23 > eps:
        return 2
    return 1


def _apply_cap_soft(weights: Dict[str, float], cap: float, soft: float) -> Dict[str, float]:
    """
    Aplica cap máximo por ativo (cap) com uma zona suave (soft, em pontos percentuais),
    e renormaliza para somar 1.

    Interpretação prática:
    - soft define uma faixa antes do cap onde pesos são "comprimidos" (sem corte brusco).
    - acima de cap: corte duro e redistribuição.
    """
    if not weights:
        return weights
    cap = float(cap)
    soft = float(soft)
    cap = min(max(cap, 0.01), 1.0)
    soft = min(max(soft, 0.0), cap)

    thr = max(0.0, cap - soft)

    w = dict(weights)

    # compressão suave na faixa (thr, cap]
    if soft > 0:
        for k, v in list(w.items()):
            if v <= thr:
                continue
            if v >= cap:
                continue
            # comprime para aproximar do thr (reduz concentração sem cortar)
            # fator 0.5 = compressão moderada (determinística)
            w[k] = thr + 0.5 * (v - thr)

    # corte duro acima do cap
    excess = 0.0
    for k, v in list(w.items()):
        if v > cap:
            excess += (v - cap)
            w[k] = cap

    # redistribui excesso entre os que ficaram abaixo do cap
    if excess > 0:
        room_keys = [k for k, v in w.items() if v < cap - 1e-12]
        if room_keys:
            room_total = sum(max(w[k], 0.0) for k in room_keys)
            if room_total <= 0:
                # redistribuição uniforme
                add = excess / len(room_keys)
                for k in room_keys:
                    w[k] += add
            else:
                for k in room_keys:
                    w[k] += excess * (max(w[k], 0.0) / room_total)

    s = sum(max(v, 0.0) for v in w.values())
    if s <= 0:
        n = len(w)
        return {k: 1.0 / n for k in w}
    return {k: max(v, 0.0) / s for k, v in w.items()}


def _weights_from_scores(
    tickers: List[str],
    score_map: Dict[Tuple[int, str], float],
    ano_ref: int,
    gamma: float,
) -> Dict[str, float]:
    vals: List[Tuple[str, float]] = []
    for tk in tickers:
        v = score_map.get((ano_ref, tk))
        if v is None:
            # tenta variações com/sem .SA (mantém compatibilidade)
            v = score_map.get((ano_ref, tk.replace(".SA", "")))
        if v is None:
            v = np.nan
        vals.append((tk, float(v) if v is not None else np.nan))

    arr = np.array([v for _, v in vals], dtype="float64")
    if np.all(~np.isfinite(arr)):
        n = len(tickers)
        return {tk: 1.0 / n for tk in tickers}

    # desloca para não-negativo e aplica potência (gamma)
    finite = arr[np.isfinite(arr)]
    mn = float(np.min(finite))
    adj = np.where(np.isfinite(arr), arr - mn + 1e-6, 1e-6)
    g = float(gamma)
    g = min(max(g, 0.10), 3.00)
    raw = np.power(adj, g)

    s = float(np.sum(raw))
    if not np.isfinite(s) or s <= 0:
        n = len(tickers)
        return {tk: 1.0 / n for tk in tickers}

    return {tk: float(raw[i] / s) for i, tk in enumerate(tickers)}


def _metrics_ano_ref(
    precos: pd.DataFrame,
    tickers_cols: List[str],
    ano_ref: int,
) -> Tuple[int, float, float, float, float]:
    """
    Retorna métricas para heurística simples:
    (n, vol_mean, vol_p70, corr_mean, vol_mean) [vol_mean duplicado p/ clareza]
    """
    n = int(len(tickers_cols))
    if precos is None or precos.empty or n == 0:
        return n, 0.0, 0.0, 0.0, 0.0

    pm = _ensure_dt_index(precos).resample("M").last()

    # tenta usar o ano_ref; se insuficiente, usa a janela de 12 meses anterior ao jan (ano_ref+1)
    ano_mask = (pm.index.year == int(ano_ref))
    use = pm.loc[ano_mask, tickers_cols].copy()
    if use.shape[0] < 6:
        end = pd.Timestamp(f"{int(ano_ref)+1}-01-01") - pd.Timedelta(days=1)
        start = end - pd.DateOffset(months=12)
        use = pm.loc[(pm.index >= start) & (pm.index <= end), tickers_cols].copy()

    use = use.dropna(how="all", axis=0).dropna(how="all", axis=1)
    if use.shape[0] < 6 or use.shape[1] < 2:
        return n, 0.0, 0.0, 0.0, 0.0

    rets = np.log(use / use.shift(1)).replace([np.inf, -np.inf], np.nan).dropna(how="all")
    if rets.shape[0] < 5:
        return n, 0.0, 0.0, 0.0, 0.0

    vols = rets.std(skipna=True)
    vol_mean = float(np.nanmean(vols.values)) if len(vols) else 0.0
    vol_p70 = float(np.nanpercentile(vols.values, 70)) if len(vols) else 0.0

    corr = rets.corr()
    if corr is None or corr.empty or corr.shape[0] < 2:
        corr_mean = 0.0
    else:
        m = corr.values
        # média fora da diagonal
        mask = ~np.eye(m.shape[0], dtype=bool)
        corr_mean = float(np.nanmean(m[mask])) if np.any(mask) else 0.0
        if not np.isfinite(corr_mean):
            corr_mean = 0.0

    return n, vol_mean, vol_p70, corr_mean, vol_mean


def _params_heuristica_simples(
    n: int,
    vol_mean: float,
    vol_p70: float,
    corr_mean: float,
    gap12: float,
) -> Tuple[float, float, float]:
    """
    Regras discretas (prioridade) para gamma/cap/soft.
    """
    # 1) n pequeno ou correlação alta
    if n <= 3 or corr_mean >= 0.60:
        return 0.80, 0.20, 0.08
    # 2) vol alto vs p70 interno
    if vol_p70 > 0 and vol_mean >= vol_p70:
        return 0.85, 0.22, 0.07
    # 3) líder muito destacado
    if gap12 >= 0.75:
        return 1.05, 0.28, 0.05
    # default
    return 0.90, 0.25, 0.05




# ─────────────────────────────────────────────────────────────
# Heurística calibrada (auto-tuning com walk-back no passado)
# ─────────────────────────────────────────────────────────────

def _candidate_policies() -> List[Tuple[float, float, float]]:
    """Grade pequena de (gamma, cap, soft) com valores plausíveis."""
    gammas = [0.80, 0.90, 1.00, 1.10]
    caps = [0.20, 0.25, 0.30]
    softs = [0.05, 0.07, 0.09]
    out: List[Tuple[float, float, float]] = []
    for g in gammas:
        for c in caps:
            for s in softs:
                # sanidade: soft não pode ser >= cap
                if s >= c:
                    continue
                out.append((float(g), float(c), float(s)))
    return out


def _eval_nav_metrics(nav: pd.Series) -> Tuple[float, float, float]:
    """Retorna (CAGR, vol_mensal, max_drawdown) para uma série de NAV mensal."""
    nav = nav.dropna()
    if len(nav) < 6:
        return 0.0, 0.0, 0.0

    # CAGR
    nav0 = float(nav.iloc[0])
    nav1 = float(nav.iloc[-1])
    if nav0 <= 0 or nav1 <= 0:
        cagr = 0.0
    else:
        years = max(1e-9, (nav.index[-1] - nav.index[0]).days / 365.25)
        cagr = (nav1 / nav0) ** (1.0 / years) - 1.0

    # vol de retornos mensais (log)
    r = np.log(nav / nav.shift(1)).dropna()
    vol = float(r.std(ddof=0)) if len(r) >= 2 else 0.0

    # max drawdown
    roll_max = nav.cummax()
    dd = (nav / roll_max) - 1.0
    mdd = float(dd.min()) if not dd.empty else 0.0

    return float(cagr), float(vol), float(mdd)


def _simulate_window_nav(
    precos: pd.DataFrame,
    divs: Dict[str, pd.Series],
    tickers_cols: List[str],
    weights: Dict[str, float],
    start: pd.Timestamp,
    end: pd.Timestamp,
    aporte_mensal: float,
    cf: float,
) -> pd.Series:
    """
    Simula NAV mensal com aportes mensais e reinvestimento de dividendos
    para um conjunto FIXO de ativos e pesos FIXOS, dentro da janela [start, end).
    """
    # agenda mensal (primeiro dia útil disponível no mês)
    idx = precos.index
    if idx.tz is not None:
        # normaliza para naive comparável
        start = start.tz_localize(None)
        end = end.tz_localize(None)

    datas = []
    cur = pd.Timestamp(year=int(start.year), month=int(start.month), day=1)
    end0 = pd.Timestamp(year=int(end.year), month=int(end.month), day=1)
    while cur < end0:
        # pega primeiro dia disponível >= cur
        loc = idx.searchsorted(cur, side="left")
        if loc < len(idx):
            datas.append(pd.Timestamp(idx[loc]))
        cur = (cur + pd.offsets.MonthBegin(1))

    if len(datas) < 3:
        return pd.Series(dtype="float64")

    carteira = defaultdict(float)
    nav_list = []
    aporte_acum = 0.0

    for dt in datas:
        # reinvestir dividendos
        for col in list(carteira.keys()):
            if carteira[col] <= 0:
                continue
            s = divs.get(col, pd.Series(dtype="float64"))
            if s.empty:
                continue
            px = _get_price(precos, dt, col)
            if px is None or px <= 0:
                continue
            # dividendos do mês (último valor disponível até dt)
            dt_aligned = _align_ts_to_index(dt, s.index)
            dv = float(s.loc[:dt_aligned].iloc[-1]) if not s.loc[:dt_aligned].empty else 0.0
            dv = max(0.0, min(dv, 10.0))  # sanitização defensiva (R$/ação)
            if dv > 0:
                carteira[col] += (carteira[col] * dv) / px

        # aporte mensal conforme pesos (com custos)
        aporte_acum += float(aporte_mensal)
        for col, w in weights.items():
            if col not in tickers_cols:
                continue
            px = _get_price(precos, dt, col)
            if px is None or px <= 0:
                continue
            valor = float(aporte_mensal) * float(w)
            valor_liq = float(valor) * float(cf)
            carteira[col] += valor_liq / px

        # NAV no fechamento de dt
        nav = 0.0
        for col, qtd in carteira.items():
            if qtd <= 0:
                continue
            px = _get_price(precos, dt, col)
            if px is None or px <= 0:
                continue
            nav += float(qtd) * float(px)
        nav_list.append((dt, nav if nav > 0 else np.nan))

    nav_s = pd.Series({d: v for d, v in nav_list}).sort_index()
    return nav_s


def _calibrate_gamma_cap_soft(
    precos: pd.DataFrame,
    divs: Dict[str, pd.Series],
    tickers_cols: List[str],
    tickers_score: List[str],
    score_map: Dict[Tuple[int, str], float],
    ano_ref: int,
    gamma_default: float,
    cap_default: float,
    soft_default: float,
    aporte_mensal: float,
    cf: float,
) -> Tuple[float, float, float]:
    """
    Seleciona (gamma, cap, soft) por auto-tuning em janela passada (walk-back).
    - Usa o TOP-N do ano_ref (já decidido fora).
    - Avalia uma grade pequena de políticas na janela de treino.
    - Aplica shrinkage em direção ao default.
    """
    # janela de treino: últimos 36 meses encerrando em 01/jan do ano_ref
    end = pd.Timestamp(year=int(ano_ref), month=1, day=1)
    start = end - pd.DateOffset(months=36)

    # limita à disponibilidade de dados
    if precos.index.tz is not None:
        idx_min = pd.Timestamp(precos.index.min()).tz_localize(None)
        idx_max = pd.Timestamp(precos.index.max()).tz_localize(None)
    else:
        idx_min = pd.Timestamp(precos.index.min())
        idx_max = pd.Timestamp(precos.index.max())

    start = max(start, idx_min)
    end = min(end, idx_max)
    if end <= start + pd.DateOffset(months=6):
        return float(gamma_default), float(cap_default), float(soft_default)

    best = (float(gamma_default), float(cap_default), float(soft_default))
    best_obj = -1e18

    # pesos base (por score) recalculados por candidato (gamma muda pesos)
    for gamma, cap, soft in _candidate_policies():
        # calcula pesos por score (ticker_score)
        w_score = _weights_from_scores(tickers_score, score_map, ano_ref, gamma=float(gamma))
        w_score = _apply_cap_soft(w_score, cap=float(cap), soft=float(soft))

        # converte para colunas executáveis
        w_exec: Dict[str, float] = {}
        for tk, ws in w_score.items():
            col = _resolve_ticker_col(precos, tk) or _resolve_ticker_col(precos, tk + ".SA")
            if col is None:
                continue
            w_exec[col] = float(ws)

        if len(w_exec) < 1:
            continue

        nav = _simulate_window_nav(
            precos=precos,
            divs=divs,
            tickers_cols=tickers_cols,
            weights=w_exec,
            start=start,
            end=end,
            aporte_mensal=aporte_mensal,
            cf=cf,
        )
        if nav.empty:
            continue

        cagr, vol, mdd = _eval_nav_metrics(nav)

        # função objetivo robusta (penaliza risco)
        obj = float(cagr) - 0.60 * float(vol) + 0.40 * float(mdd)  # mdd é negativo

        if obj > best_obj:
            best_obj = obj
            best = (float(gamma), float(cap), float(soft))

    # shrinkage em direção ao default quando pouca evidência
    nav_len_months = 36
    if precos.index.tz is not None:
        nav_len_months = int(max(1, (end - start).days / 30.0))
    else:
        nav_len_months = int(max(1, (end - start).days / 30.0))

    # quanto mais meses e mais ativos, mais confiável
    k = float(nav_len_months) / 36.0
    m = float(max(1, len(tickers_cols))) / 3.0
    conf = max(0.0, min(1.0, 0.35 + 0.35 * k + 0.30 * m))  # 0.35..1.0

    gamma = (1.0 - conf) * float(gamma_default) + conf * float(best[0])
    cap = (1.0 - conf) * float(cap_default) + conf * float(best[1])
    soft = (1.0 - conf) * float(soft_default) + conf * float(best[2])

    # clamp final
    gamma = float(max(0.60, min(1.40, gamma)))
    cap = float(max(0.10, min(0.45, cap)))
    soft = float(max(0.02, min(cap - 0.01, soft)))

    return gamma, cap, soft
def gerir_carteira_modulada(
    precos: pd.DataFrame,
    df_scores: pd.DataFrame,
    lideres_por_ano: pd.DataFrame,
    dividendos_dict: Dict[str, Union[pd.Series, pd.DataFrame]],
    policy: Optional[Dict] = None,
    aporte_mensal: float = 1000.0,
    deterioracao_limite: float = 0.0,
    registrar_eventos: bool = False,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
):
    """
    Variante modulada da estratégia:
    - N pode ser fixo (manual) ou dinâmico (heurística).
    - aporte é distribuído entre os top-N do ano_ref, com pesos derivados do score e parâmetro gamma.
    - cap/soft limitam concentração e redistribuem pesos.

    Compatível com o contrato do projeto:
      retorna (df_patrimonio, datas_aportes) ou (df_patrimonio, datas_aportes, eventos) se registrar_eventos=True
    """
    precos = _ensure_dt_index(precos)

    if precos is None or precos.empty:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    if df_scores is None or df_scores.empty:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    if lideres_por_ano is None or lideres_por_ano.empty:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    policy = policy or {}
    mode = str(policy.get("mode", "heuristica")).strip().lower()

    # defaults
    eps = float(policy.get("eps", 0.35))
    gamma_default = float(policy.get("gamma", 0.90))
    cap_default = float(policy.get("cap", 0.25))
    soft_default = float(policy.get("soft", 0.05))
    n_manual = int(policy.get("N", 2))

    divs = {}
    for col in list(precos.columns):
        k = _resolve_div_key(dividendos_dict, col)
        divs[col] = _as_div_series(dividendos_dict.get(k)) if k is not None else pd.Series(dtype="float64")

    cf = _cost_factor(fee_bps, slippage_bps)

    anos_scores = sorted(int(a) for a in pd.to_numeric(df_scores.get("Ano"), errors="coerce").dropna().unique())
    if not anos_scores:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    datas_aportes = _build_monthly_schedule(precos, anos_scores, start_year_offset=1)
    if not datas_aportes:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    # scores normalizados
    df_scores2 = df_scores.copy()
    df_scores2["Ano"] = pd.to_numeric(df_scores2["Ano"], errors="coerce")
    df_scores2["ticker"] = df_scores2["ticker"].astype(str)
    df_scores2["Score_Ajustado"] = pd.to_numeric(df_scores2.get("Score_Ajustado"), errors="coerce")

    score_map: Dict[Tuple[int, str], float] = {}
    for _, r in df_scores2.dropna(subset=["Ano", "ticker", "Score_Ajustado"]).iterrows():
        score_map[(int(r["Ano"]), str(r["ticker"]))] = float(r["Score_Ajustado"])

    # líder (top1) por ano via lideres_por_ano (mantém compatibilidade com lógica existente)
    lider_map = {}
    for _, r in lideres_por_ano.dropna(subset=["Ano", "ticker"]).iterrows():
        try:
            lider_map[int(r["Ano"])] = str(r["ticker"])
        except Exception:
            continue

    ano_base = anos_scores[0]

    # cache de escolhas por ano_ref
    escolhas_cache: Dict[int, Dict[str, object]] = {}

    def _get_escolha(ano_ref: int) -> Dict[str, object]:
        if ano_ref in escolhas_cache:
            return escolhas_cache[ano_ref]

        # tickers elegíveis no ano_ref (presentes no score e no preço)
        rows = df_scores2[df_scores2["Ano"] == ano_ref].dropna(subset=["ticker", "Score_Ajustado"])
        if rows.empty:
            escolhas_cache[ano_ref] = {"tickers": [], "weights": {}, "gamma": gamma_default, "cap": cap_default, "soft": soft_default, "N": 0}
            return escolhas_cache[ano_ref]

        rows = rows.sort_values("Score_Ajustado", ascending=False)
        # filtra por disponibilidade em preços (aceita variações)
        tickers_raw = [str(t) for t in rows["ticker"].tolist()]
        tickers_cols: List[str] = []
        tickers_keep: List[str] = []
        scores_keep: List[float] = []
        for tk in tickers_raw:
            col = _resolve_ticker_col(precos, tk)
            if col is None:
                col = _resolve_ticker_col(precos, tk + ".SA")
            if col is None:
                continue
            # manter ticker de score (para score_map) e coluna de preço (para execução)
            tickers_keep.append(tk)
            tickers_cols.append(col)
            scores_keep.append(float(rows.loc[rows["ticker"] == tk, "Score_Ajustado"].iloc[0]))

        if not tickers_keep:
            escolhas_cache[ano_ref] = {"tickers": [], "weights": {}, "gamma": gamma_default, "cap": cap_default, "soft": soft_default, "N": 0}
            return escolhas_cache[ano_ref]

        # calcula N
        if mode == "manual":
            N = int(n_manual)
        else:
            N = _select_n_heuristica(scores_keep[:3], eps=eps)

        N = max(1, min(int(N), int(len(tickers_keep))))

        tickers_keep = tickers_keep[:N]
        tickers_cols = tickers_cols[:N]
        scores_top = scores_keep[:N]

        # gap12
        gap12 = float(scores_top[0] - scores_top[1]) if len(scores_top) >= 2 else float(scores_top[0])

        # parâmetros
        if mode == "manual":
            gamma = float(policy.get("gamma", gamma_default))
            cap = float(policy.get("cap", cap_default))
            soft = float(policy.get("soft", soft_default))
        elif mode == "heuristica_calibrada":
            # Auto-tuning com janela passada (walk-back) + shrinkage em direção ao default
            gamma, cap, soft = _calibrate_gamma_cap_soft(
                precos=precos,
                divs=divs,
                tickers_cols=tickers_cols,
                tickers_score=tickers_keep,
                score_map=score_map,
                ano_ref=ano_ref,
                gamma_default=gamma_default,
                cap_default=cap_default,
                soft_default=soft_default,
                aporte_mensal=aporte_mensal,
                cf=cf,
            )
        elif mode == "heuristica_simples":
            n, vol_mean, vol_p70, corr_mean, _ = _metrics_ano_ref(precos, tickers_cols, ano_ref)
            gamma, cap, soft = _params_heuristica_simples(n=n, vol_mean=vol_mean, vol_p70=vol_p70, corr_mean=corr_mean, gap12=gap12)
        else:
            # heuristica (padrão): N dinâmico e parâmetros fixos
            gamma = gamma_default
            cap = cap_default
            soft = soft_default

        # pesos
        w = _weights_from_scores(tickers_keep, score_map, ano_ref, gamma=gamma)
        w = _apply_cap_soft(w, cap=cap, soft=soft)

        escolhas_cache[ano_ref] = {"tickers": tickers_keep, "weights": w, "gamma": gamma, "cap": cap, "soft": soft, "N": N}
        return escolhas_cache[ano_ref]

    carteira = defaultdict(float)
    aporte_acumulado = 0.0
    registros: List[dict] = []
    eventos = [] if registrar_eventos else None

    for data_sinal in datas_aportes:
        ano_ref = int(data_sinal.year - 1)

        escolha = _get_escolha(ano_ref)
        tickers_sel: List[str] = list(escolha.get("tickers", []))  # tickers do score
        weights: Dict[str, float] = dict(escolha.get("weights", {}))

        # resolve para colunas de preço
        tickers_exec: List[str] = []
        map_exec_to_score: Dict[str, str] = {}
        for tk in tickers_sel:
            col = _resolve_ticker_col(precos, tk) or _resolve_ticker_col(precos, tk + ".SA")
            if col is None:
                continue
            tickers_exec.append(col)
            map_exec_to_score[col] = tk

        if not tickers_exec:
            continue

        # Reinvestimento de dividendos (POR AÇÃO), sanitizado
        for tk_col in list(carteira.keys()):
            if carteira[tk_col] <= 0:
                continue
            s = divs.get(tk_col, pd.Series(dtype="float64"))
            if s.empty:
                continue
            px = _get_price(precos, data_sinal, tk_col)
            if px is None:
                continue
            div_mes = _div_mes_por_acao_sanitizado(
                s=s,
                ano=int(data_sinal.year),
                mes=int(data_sinal.month),
                ticker=tk_col,
                px_ref=px,
            )
            if div_mes <= 0:
                continue
            valor_reinvestido = div_mes * carteira[tk_col]
            carteira[tk_col] += valor_reinvestido / px

        # aporte com custo, distribuído por pesos
        total_a_aportar = float(aporte_mensal) + float(aporte_acumulado)
        aporte_acumulado = 0.0

        # garante pesos apenas para tickers selecionados
        w_exec: Dict[str, float] = {}
        for tk_col in tickers_exec:
            tk_score = map_exec_to_score.get(tk_col, tk_col)
            w_exec[tk_col] = float(weights.get(tk_score, 0.0))

        s_w = sum(w_exec.values())
        if s_w <= 0:
            w_exec = {tk: 1.0 / len(tickers_exec) for tk in tickers_exec}
        else:
            w_exec = {k: v / s_w for k, v in w_exec.items()}

        for tk_col, w in w_exec.items():
            px = _get_price(precos, data_sinal, tk_col)
            if px is None:
                aporte_acumulado += total_a_aportar * float(w)
                continue
            carteira[tk_col] += ((total_a_aportar * float(w)) * cf) / px

        # deterioração (opcional): vende ativos fora do conjunto selecionado se score deteriorar
        if float(deterioracao_limite) > 0:
            lider_destino_score = lider_map.get(ano_ref)  # ticker sem .SA (provável)
            lider_dest_col = None
            if lider_destino_score:
                lider_dest_col = _resolve_ticker_col(precos, lider_destino_score) or _resolve_ticker_col(precos, lider_destino_score + ".SA")

            if lider_dest_col:
                for antiga_col in list(carteira.keys()):
                    if antiga_col in tickers_exec:
                        continue
                    antiga_score = antiga_col.replace(".SA", "")
                    s_ini = score_map.get((ano_base, antiga_score))
                    s_atual = score_map.get((ano_ref, antiga_score))
                    if s_ini is None or s_atual is None or s_ini == 0:
                        continue
                    razao = float(s_atual) / float(s_ini)
                    if razao < float(deterioracao_limite):
                        px_venda = _get_price(precos, data_sinal, antiga_col)
                        px_dest = _get_price(precos, data_sinal, lider_dest_col)
                        if px_venda is None or px_dest is None:
                            continue
                        valor_bruto = carteira[antiga_col] * px_venda
                        valor_liquido = valor_bruto * cf
                        qtd_nova = (valor_liquido * cf) / px_dest
                        carteira.pop(antiga_col, None)
                        carteira[lider_dest_col] += qtd_nova
                        if registrar_eventos:
                            eventos.append({"data": data_sinal.strftime("%Y-%m"), "tipo": "saida", "ticker": antiga_col})

        registro = {"date": data_sinal}
        total = 0.0
        for tk_col, qtd in carteira.items():
            px = _get_price(precos, data_sinal, tk_col)
            if px is None:
                continue
            val = qtd * px
            registro[tk_col] = val
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
    "gerir_carteira_equal_weight_segmento",
    "calcular_patrimonio_selic_macro",
    "gerir_carteira",
    "gerir_carteira_modulada",
]
