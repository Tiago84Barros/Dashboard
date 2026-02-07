from __future__ import annotations

import logging
from dataclasses import dataclass
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Tuple, Union

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
# ==========================================================
# PortfolioPolicy + Aporte Modulado (V16)
# ==========================================================

@dataclass(frozen=True)
class PortfolioPolicy:
    """
    mode:
      - 'manual'              : usa fixed_top_n e parâmetros configuráveis (diagnóstico)
      - 'heuristica'          : N dinâmico, parâmetros fixos do policy
      - 'heuristica_simples'  : N dinâmico + γ/cap/soft automáticos (regras discretas)
      - 'padrao'              : não usado aqui (gerir_carteira já cobre)
    """
    mode: str = "heuristica"
    fixed_top_n: Optional[int] = None
    gamma: float = 0.9
    cap_max: float = 0.25
    cap_soft: float = 0.05
    eps_topn: float = 0.35


def _decidir_top_n_por_score(scores: pd.Series, eps: float) -> int:
    s = scores.dropna().sort_values(ascending=False).values
    n = int(len(s))
    if n <= 1:
        return 1
    if n == 2:
        return 2 if (s[0] - s[1]) <= eps else 1
    g12 = float(s[0] - s[1])
    g23 = float(s[1] - s[2])
    if g12 <= eps and g23 <= eps:
        return 3
    if g12 <= eps and g23 > eps:
        return 2
    return 1


def _auto_params_simples(scores_top: pd.Series, precos_top: pd.DataFrame) -> Tuple[float, float, float]:
    """
    Heurística simples (discreta), por segmento/ano-ref:
      - Se n <= 3 OU corr >= 0.60: gamma=0.80 cap=0.20 soft=0.08
      - Se vol alto (p70):         gamma=0.85 cap=0.22 soft=0.07
      - Se gap12 >= 0.75:          gamma=1.05 cap=0.28 soft=0.05
      - Caso geral:                gamma=0.90 cap=0.25 soft=0.05
    """
    scores_top = scores_top.dropna()
    n = int(len(scores_top))

    precos_top = _ensure_dt_index(precos_top).dropna(how="all")
    if precos_top is None or precos_top.empty or precos_top.shape[1] == 0:
        # fallback conservador
        return 0.90, 0.25, 0.05

    rets = np.log(precos_top / precos_top.shift(1)).dropna(how="all")
    vol = float(rets.std().mean()) if not rets.empty else 0.0

    if rets.shape[1] > 1:
        c = rets.corr()
        corr = float(np.nanmean(c.values))
    else:
        corr = 0.0

    s_ord = scores_top.sort_values(ascending=False)
    gap12 = float(s_ord.iloc[0] - s_ord.iloc[1]) if len(s_ord) >= 2 else 0.0

    if n <= 3 or corr >= 0.60:
        return 0.80, 0.20, 0.08

    try:
        vol_p70 = float(rets.std().quantile(0.70))
    except Exception:
        vol_p70 = vol

    if vol >= vol_p70 and vol_p70 > 0:
        return 0.85, 0.22, 0.07

    if gap12 >= 0.75:
        return 1.05, 0.28, 0.05

    return 0.90, 0.25, 0.05


def gerir_carteira_modulada(
    precos: pd.DataFrame,
    df_scores: pd.DataFrame,
    lideres_por_ano: pd.DataFrame,
    dividendos_dict: Dict[str, Union[pd.Series, pd.DataFrame]],
    aporte_mensal: float = 1000.0,
    deterioracao_limite: float = 0.0,
    registrar_eventos: bool = False,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
    policy: Optional[PortfolioPolicy] = None,
):
    """
    Aporte modulado por score (top N do ano-ref) com cap e zona suave.
    - Mantém interface de gerir_carteira para compatibilidade com advanced.py.
    - Usa apenas informações do ano-ref para decidir N e parâmetros.
    """
    policy = policy or PortfolioPolicy()
    precos = _ensure_dt_index(precos)
    df_scores = df_scores.copy()
    df_scores["Ano"] = pd.to_numeric(df_scores.get("Ano"), errors="coerce")
    df_scores = df_scores.dropna(subset=["Ano", "ticker"])
    df_scores["ticker"] = df_scores["ticker"].astype(str)

    # carteira em quantidade de ações
    carteira_qtd = defaultdict(float)
    registros: List[Dict[str, float]] = []
    eventos: List[Dict[str, str]] = []
    datas_aportes: List[pd.Timestamp] = []

    # determinar anos de referência
    anos_ref = sorted(df_scores["Ano"].dropna().astype(int).unique().tolist())

    # custo efetivo (fee+slippage)
    custo_bps = float(fee_bps) + float(slippage_bps)
    custo_mul = max(0.0, 1.0 - custo_bps / 10000.0)

    # normaliza dividendos como Series por ticker
    div_series: Dict[str, pd.Series] = {}
    for tk, dv in (dividendos_dict or {}).items():
        try:
            div_series[tk] = _as_div_series(dv)
        except Exception:
            continue

    for ano_ref in anos_ref:
        # scores do ano-ref
        s_ano = df_scores[df_scores["Ano"].astype(int) == int(ano_ref)].copy()
        if s_ano.empty:
            continue
        if "Score_Ajustado" not in s_ano.columns:
            continue
        s_ano["Score_Ajustado"] = pd.to_numeric(s_ano["Score_Ajustado"], errors="coerce")
        s_ano = s_ano.dropna(subset=["Score_Ajustado", "ticker"])
        if s_ano.empty:
            continue

        scores = s_ano.set_index("ticker")["Score_Ajustado"]

        # top_n
        if policy.mode in ("heuristica", "heuristica_simples"):
            top_n = _decidir_top_n_por_score(scores, policy.eps_topn)
        elif policy.mode == "manual" and policy.fixed_top_n:
            top_n = int(policy.fixed_top_n)
        else:
            top_n = 1

        top_tickers = scores.sort_values(ascending=False).head(top_n).index.tolist()
        if not top_tickers:
            continue

        # parâmetros (γ/cap/soft)
        if policy.mode == "heuristica_simples":
            gamma, cap_max, cap_soft = _auto_params_simples(scores.loc[top_tickers], precos[top_tickers])
        else:
            gamma, cap_max, cap_soft = float(policy.gamma), float(policy.cap_max), float(policy.cap_soft)

        # janela de aportes: ano seguinte ao ano_ref
        ano_aporte = int(ano_ref) + 1
        # vamos aportar mensalmente na última data de cada mês do ano_aporte
        px_year = precos[precos.index.year == ano_aporte]
        if px_year is None or px_year.empty:
            continue
        meses = px_year.resample("M").last().index

        for data_aporte in meses:
            data_exec = encontrar_proxima_data_valida(pd.Timestamp(data_aporte), precos)
            if data_exec is None:
                continue

            # reinvestir dividendos do mês (se houver)
            for tk, qtd in list(carteira_qtd.items()):
                if qtd <= 0:
                    continue
                if tk in div_series:
                    dv = float(div_series[tk].get(data_exec, 0.0) or 0.0)
                    if dv > 0 and dv <= DIV_POR_ACAO_MAX:
                        px = _get_price(precos, data_exec, tk)
                        if px:
                            # dividendos por ação * qtd -> caixa, reinveste no próprio tk
                            caixa = dv * float(qtd)
                            carteira_qtd[tk] += (caixa * custo_mul) / px

            # calcular valor atual por ativo
            valores: Dict[str, float] = {}
            total = 0.0
            for tk, qtd in carteira_qtd.items():
                px = _get_price(precos, data_exec, tk)
                if px is None or qtd <= 0:
                    continue
                v = float(qtd) * px
                valores[tk] = v
                total += v

            # pesos por score (apenas top_tickers)
            pesos_raw: Dict[str, float] = {}
            for tk in top_tickers:
                sc = float(scores.get(tk, 0.0))
                sc = max(sc, 0.0)
                pesos_raw[tk] = sc ** float(gamma)

            soma_p = float(sum(pesos_raw.values()))
            if soma_p <= 0:
                continue

            # aplicar aporte com cap + zona suave
            for tk in top_tickers:
                w = pesos_raw[tk] / soma_p
                atual = (valores.get(tk, 0.0) / total) if total > 0 else 0.0

                if atual >= cap_max:
                    if cap_soft > 0:
                        fator = max(0.0, 1.0 - (atual - cap_max) / cap_soft)
                    else:
                        fator = 0.0
                else:
                    fator = 1.0

                aporte = float(aporte_mensal) * w * fator
                if aporte <= 0:
                    continue

                px = _get_price(precos, data_exec, tk)
                if px is None:
                    continue

                carteira_qtd[tk] += (aporte * custo_mul) / px

                if registrar_eventos:
                    eventos.append(
                        {
                            "date": str(pd.Timestamp(data_exec).date()),
                            "ticker": tk,
                            "tipo": "APORTE_MODULADO",
                            "valor": f"{aporte:.2f}",
                        }
                    )

            # registrar patrimônio
            total_new = 0.0
            for tk, qtd in carteira_qtd.items():
                px = _get_price(precos, data_exec, tk)
                if px is None or qtd <= 0:
                    continue
                total_new += float(qtd) * px

            registros.append({"date": data_exec, "Patrimônio": total_new})
            datas_aportes.append(pd.Timestamp(data_exec))

    df_patrimonio = pd.DataFrame(registros).set_index("date").sort_index().ffill()
    if "Patrimônio" in df_patrimonio.columns:
        df_patrimonio = df_patrimonio[df_patrimonio["Patrimônio"].fillna(0) != 0]

    if registrar_eventos:
        return df_patrimonio, datas_aportes, eventos

    return df_patrimonio, datas_aportes


rn df_patrimonio, datas_aportes


__all__ = [
    "encontrar_proxima_data_valida",
    "gerir_carteira_simples",
    "gerir_carteira_todas_empresas",
    "calcular_patrimonio_selic_macro",
    "gerir_carteira",
    "PortfolioPolicy",
    "gerir_carteira_modulada",
]
