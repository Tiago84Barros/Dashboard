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


@dataclass(frozen=True)
class PortfolioPolicy:
    """Parâmetros da camada de controle de aporte (TO-BE).

    Observações:
    - O score NÃO muda; apenas o aporte é modulado.
    - Mantém compatibilidade com o motor atual (sem vendas obrigatórias).
    """

    top_n_por_ano: int = 1          # 1 mantém o comportamento atual (líder); >1 habilita "faixa superior"
    gamma: float = 1.0              # expoente do peso por score (0.5 suaviza, >1 concentra)
    tau: float = 0.0                # corte: scores <= tau recebem peso 0
    cap_max: float = 0.25           # limite de peso por ativo no portfólio (0-1). Se exceder, pausa novos aportes
    cap_soft_zone: float = 0.00     # zona suave (0 = corte seco; >0 = redução gradual)
    # Valuation (opcional): só aplica se coluna existir em df_scores
    valuation_col: Optional[str] = None   # ex: "P/VP" ou "PVPA"
    valuation_qmax: float = 0.85          # percentil acima do qual reduz/pausa aportes
    valuation_lambda: float = 10.0        # força do freio por valuation acima do qmax


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


def gerir_carteira_modulada(
    precos: pd.DataFrame,
    df_scores: pd.DataFrame,
    lideres_por_ano: pd.DataFrame,
    dividendos_dict: Dict[str, Union[pd.Series, pd.DataFrame]],
    policy: Optional[PortfolioPolicy] = None,
    aporte_mensal: float = 1000.0,
    registrar_eventos: bool = False,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
):
    """Estratégia de líderes com camada de controle de aporte (score -> pesos).

    - Mantém a ideia original: usa o ano_ref = ano_atual - 1.
    - Por padrão (policy.top_n_por_ano=1), adiciona 1 líder por ano.
    - Quando top_n_por_ano > 1, adiciona a faixa superior do ranking do ano_ref.

    Aportes:
    - Distribui o aporte mensal proporcionalmente ao Score_Ajustado do ano_ref,
      aplicando (i) corte tau, (ii) expoente gamma e (iii) cap de concentração por ativo.
    - Se todos os ativos estiverem capados (ou sem score positivo), faz fallback para pesos iguais
      entre os elegíveis que tenham preço; se ainda assim impossível, acumula o aporte.

    Reinvestimento:
    - Dividendos POR AÇÃO (R$/ação), sanitizados (mesma lógica do motor atual).
    """
    precos = _ensure_dt_index(precos)

    if precos is None or precos.empty:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    if df_scores is None or df_scores.empty:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    if lideres_por_ano is None or lideres_por_ano.empty:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    policy = policy or PortfolioPolicy()

    # defensivo
    top_n = max(1, int(policy.top_n_por_ano))
    gamma = float(policy.gamma)
    tau = float(policy.tau)
    cap_max = float(policy.cap_max)
    cap_soft = max(0.0, float(policy.cap_soft_zone))

    divs = {t: _as_div_series(dividendos_dict.get(t)) for t in precos.columns}
    cf = _cost_factor(fee_bps, slippage_bps)

    anos_scores = sorted(int(a) for a in pd.to_numeric(df_scores.get("Ano"), errors="coerce").dropna().unique())
    if not anos_scores:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    datas_aportes = _build_monthly_schedule(precos, anos_scores, start_year_offset=1)
    if not datas_aportes:
        return (pd.DataFrame(), []) if not registrar_eventos else (pd.DataFrame(), [], [])

    # score map (Ano,ticker) -> score
    df_scores2 = df_scores.copy()
    df_scores2["Ano"] = pd.to_numeric(df_scores2["Ano"], errors="coerce")
    df_scores2["ticker"] = df_scores2["ticker"].astype(str)
    df_scores2["Score_Ajustado"] = pd.to_numeric(df_scores2["Score_Ajustado"], errors="coerce")

    score_map: Dict[Tuple[int, str], float] = {}
    for _, r in df_scores2.dropna(subset=["Ano", "ticker", "Score_Ajustado"]).iterrows():
        score_map[(int(r["Ano"]), str(r["ticker"]))] = float(r["Score_Ajustado"])

    # valuation map (Ano,ticker) -> valuation, opcional
    val_col = policy.valuation_col
    val_map: Dict[Tuple[int, str], float] = {}
    if val_col and (val_col in df_scores2.columns):
        tmpv = df_scores2[["Ano", "ticker", val_col]].copy()
        tmpv[val_col] = pd.to_numeric(tmpv[val_col], errors="coerce")
        tmpv = tmpv.dropna(subset=["Ano", "ticker", val_col])
        for _, r in tmpv.iterrows():
            val_map[(int(r["Ano"]), str(r["ticker"]))] = float(r[val_col])

    # universo por ano_ref: top N do score
    topn_map: Dict[int, List[str]] = {}
    for ano in anos_scores:
        dfy = df_scores2[df_scores2["Ano"] == ano].dropna(subset=["ticker", "Score_Ajustado"]).copy()
        if dfy.empty:
            continue
        dfy = dfy.sort_values("Score_Ajustado", ascending=False)
        topn_map[int(ano)] = dfy["ticker"].astype(str).head(top_n).tolist()

    carteira = defaultdict(float)  # ticker -> qtd ações
    aporte_acumulado = 0.0
    registros: List[dict] = []
    eventos = [] if registrar_eventos else None

    elegiveis_atuais: List[str] = []
    ano_ref_atual: Optional[int] = None

    def _cap_factor(w: float) -> float:
        """Retorna fator [0,1] para reduzir aporte quando ultrapassa cap_max."""
        if not np.isfinite(w) or w < 0:
            return 1.0
        if cap_max <= 0:
            return 1.0
        if w <= cap_max:
            return 1.0
        if cap_soft <= 0:
            return 0.0
        # redução linear suave: zera ao exceder cap_max + cap_soft
        return max(0.0, 1.0 - (w - cap_max) / cap_soft)

    def _val_factor(ano_ref: int, tk: str) -> float:
        if not val_col:
            return 1.0
        v = val_map.get((ano_ref, tk))
        if v is None or not np.isfinite(v):
            return 1.0
        # percentil no universo do ano (proxy simples)
        vals = [vv for (a, t), vv in val_map.items() if a == ano_ref and np.isfinite(vv)]
        if len(vals) < 5:
            return 1.0
        vals = np.array(sorted(vals), dtype=float)
        q = float(np.searchsorted(vals, v, side="right")) / float(len(vals))
        if q <= float(policy.valuation_qmax):
            return 1.0
        lam = float(policy.valuation_lambda)
        return float(np.exp(-lam * (q - float(policy.valuation_qmax))))

    for data_sinal in datas_aportes:
        ano_ref = int(data_sinal.year - 1)

        if ano_ref != ano_ref_atual:
            ano_ref_atual = ano_ref
            # adiciona top-N do ano_ref ao universo elegível
            novos = topn_map.get(ano_ref, [])
            for tk in novos:
                if tk and (tk not in elegiveis_atuais):
                    elegiveis_atuais.append(tk)
                    if registrar_eventos:
                        eventos.append({"data": data_sinal.strftime("%Y-%m"), "tipo": "entrada", "ticker": tk})

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

        # calcula valores atuais e fatores de cap
        valores = {}
        total_val = 0.0
        for tk in list(carteira.keys()):
            px = _get_price(precos, data_sinal, tk)
            if px is None:
                continue
            v = float(carteira[tk]) * float(px)
            if np.isfinite(v) and v > 0:
                valores[tk] = v
                total_val += v

        cap_f = {}
        if total_val > 0:
            for tk, v in valores.items():
                cap_f[tk] = _cap_factor(v / total_val)

        # define elegíveis com preço no mês
        eleg = [t for t in elegiveis_atuais if _get_price(precos, data_sinal, t) is not None]
        if not eleg:
            aporte_acumulado += total_a_aportar
        else:
            # pesos por score no ano_ref
            omegas = {}
            for tk in eleg:
                sc = score_map.get((ano_ref, tk))
                if sc is None or not np.isfinite(sc):
                    sc = 0.0
                s_plus = max(float(sc) - tau, 0.0)
                if s_plus <= 0:
                    w = 0.0
                else:
                    w = float(s_plus) ** gamma

                # cap e valuation (se aplicável)
                w *= float(cap_f.get(tk, 1.0))
                w *= float(_val_factor(ano_ref, tk))
                omegas[tk] = w

            sumw = float(sum(omegas.values()))
            if sumw <= 0:
                # fallback: pesos iguais apenas para quem não está totalmente capado
                eleg2 = [t for t in eleg if cap_f.get(t, 1.0) > 0]
                if not eleg2:
                    aporte_acumulado += total_a_aportar
                else:
                    w_eq = 1.0 / len(eleg2)
                    for tk in eleg2:
                        px = _get_price(precos, data_sinal, tk)
                        if px is None:
                            aporte_acumulado += total_a_aportar * w_eq
                            continue
                        carteira[tk] += ((total_a_aportar * w_eq) * cf) / px
            else:
                for tk in eleg:
                    w = float(omegas.get(tk, 0.0)) / sumw
                    if w <= 0:
                        continue
                    px = _get_price(precos, data_sinal, tk)
                    if px is None:
                        aporte_acumulado += total_a_aportar * w
                        continue
                    carteira[tk] += ((total_a_aportar * w) * cf) / px

        registro = {"date": data_sinal}
        total = 0.0
        for tk, qtd in carteira.items():
            px = _get_price(precos, data_sinal, tk)
            if px is None:
                continue
            val = float(qtd) * float(px)
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
    "gerir_carteira_modulada",
    "PortfolioPolicy",
]
