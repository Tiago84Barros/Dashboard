from __future__ import annotations

from typing import Any, Dict, List, Optional


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _safe_get(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d:
            return d.get(k)
    return None


def _classify_selic_level(selic: Optional[float]) -> str:
    if selic is None:
        return "indefinido"
    if selic >= 12:
        return "muito_alta"
    if selic >= 9:
        return "alta"
    if selic >= 6:
        return "moderada"
    return "baixa"


def _classify_real_rate(real_rate: Optional[float]) -> str:
    if real_rate is None:
        return "indefinido"
    if real_rate >= 6:
        return "muito_restritivo"
    if real_rate >= 4:
        return "restritivo"
    if real_rate >= 2:
        return "neutro_a_restritivo"
    return "baixo"


def _classify_ipca_12m(ipca_12m: Optional[float]) -> str:
    if ipca_12m is None:
        return "indefinido"
    if ipca_12m >= 6:
        return "pressionado"
    if ipca_12m >= 4.5:
        return "acima_da_meta"
    if ipca_12m >= 3:
        return "controlado"
    return "baixo"


def _classify_fx(cambio: Optional[float]) -> str:
    if cambio is None:
        return "indefinido"
    if cambio >= 5.8:
        return "muito_depreciado"
    if cambio >= 5.2:
        return "depreciado"
    if cambio >= 4.8:
        return "intermediario"
    return "apreciado"


def _classify_confidence(icc_delta_12m: Optional[float]) -> str:
    if icc_delta_12m is None:
        return "indefinido"
    if icc_delta_12m >= 5:
        return "melhora_relevante"
    if icc_delta_12m > 0:
        return "leve_melhora"
    if icc_delta_12m <= -5:
        return "deterioracao_relevante"
    return "leve_deterioracao"


def _trend_text(label: str) -> str:
    mapping = {
        "alta": "em alta",
        "queda": "em queda",
        "estavel": "estável",
        "indefinido": "sem tendência clara",
    }
    return mapping.get(label or "", "sem tendência clara")


def build_market_context(macro_context: Dict[str, Any]) -> Dict[str, Any]:
    mensal = macro_context.get("mensal", {}) or {}
    anual = macro_context.get("anual", {}) or {}
    trends = macro_context.get("trends", {}) or {}

    selic_final = _to_float(_safe_get(mensal, "selic_final"))
    ipca_12m = _to_float(_safe_get(mensal, "ipca_12m"))
    cambio_final = _to_float(_safe_get(mensal, "cambio_final"))
    icc_delta_12m = _to_float(_safe_get(mensal, "icc_delta_12m"))
    juros_real = _to_float(
        _safe_get(mensal, "juros_real_ex_ante_12m")
        if _safe_get(mensal, "juros_real_ex_ante_12m") is not None
        else _safe_get(anual, "juros_real_ex_ante")
    )
    pib = _to_float(_safe_get(anual, "pib"))
    divida_publica = _to_float(
        _safe_get(mensal, "divida_publica_final")
        if _safe_get(mensal, "divida_publica_final") is not None
        else _safe_get(anual, "divida_publica")
    )

    selic_regime = _classify_selic_level(selic_final)
    inflation_regime = _classify_ipca_12m(ipca_12m)
    fx_regime = _classify_fx(cambio_final)
    confidence_regime = _classify_confidence(icc_delta_12m)
    real_rate_regime = _classify_real_rate(juros_real)

    domestic_risk_factors: List[str] = []
    if selic_regime in {"muito_alta", "alta"}:
        domestic_risk_factors.append("juros elevados pressionam ativos domésticos sensíveis a desconto e crédito")
    if real_rate_regime in {"muito_restritivo", "restritivo"}:
        domestic_risk_factors.append("juro real elevado reduz folga para empresas dependentes de demanda e capital")
    if inflation_regime in {"pressionado", "acima_da_meta"}:
        domestic_risk_factors.append("inflação ainda exige disciplina adicional de política monetária")
    if fx_regime in {"muito_depreciado", "depreciado"}:
        domestic_risk_factors.append("câmbio pressionado favorece exportadoras e encarece vetores importados")
    if confidence_regime in {"deterioracao_relevante", "leve_deterioracao"}:
        domestic_risk_factors.append("confiança mais fraca pode pesar sobre consumo e atividade doméstica")

    portfolio_tailwinds: List[str] = []
    portfolio_headwinds: List[str] = []

    if selic_regime in {"muito_alta", "alta"}:
        portfolio_tailwinds.append("bancos e negócios defensivos tendem a atravessar melhor o regime")
        portfolio_headwinds.append("small caps e teses dependentes de re-rating ficam mais pressionadas")

    if fx_regime in {"muito_depreciado", "depreciado"}:
        portfolio_tailwinds.append("exportadoras e receitas dolarizadas ganham proteção relativa")
        portfolio_headwinds.append("negócios mais dependentes de insumos importados podem sofrer pressão")

    if inflation_regime in {"pressionado", "acima_da_meta"}:
        portfolio_headwinds.append("ativos sensíveis a custo de capital e consumo discricionário enfrentam ambiente mais exigente")

    international_links = [
        "mudanças nos juros globais afetam fluxo para emergentes e custo de oportunidade local",
        "movimentos de dólar e apetite global por risco impactam re-rating da bolsa brasileira",
        "commodities internacionais influenciam diretamente exportadoras e empresas ligadas a petróleo e minério",
    ]

    selic_trend = _trend_text((trends.get("selic") or {}).get("trend", "indefinido"))
    cambio_trend = _trend_text((trends.get("cambio") or {}).get("trend", "indefinido"))
    ipca_trend = _trend_text((trends.get("ipca_12m") or {}).get("trend", "indefinido"))
    icc_trend = _trend_text((trends.get("icc") or {}).get("trend", "indefinido"))

    regime_summary = (
        f"O regime doméstico atual combina Selic em {selic_final if selic_final is not None else '—'}% ({selic_trend}), "
        f"juro real {real_rate_regime.replace('_', ' ')}, inflação de 12 meses em {ipca_12m if ipca_12m is not None else '—'}% ({ipca_trend}), "
        f"câmbio em {cambio_final if cambio_final is not None else '—'} ({cambio_trend}) e confiança {icc_trend}."
    )

    return {
        "regime_summary": regime_summary,
        "selic_regime": selic_regime,
        "real_rate_regime": real_rate_regime,
        "inflation_regime": inflation_regime,
        "fx_regime": fx_regime,
        "confidence_regime": confidence_regime,
        "macro_trends": {
            "selic": (trends.get("selic") or {}).get("trend", "indefinido"),
            "cambio": (trends.get("cambio") or {}).get("trend", "indefinido"),
            "ipca_12m": (trends.get("ipca_12m") or {}).get("trend", "indefinido"),
            "icc": (trends.get("icc") or {}).get("trend", "indefinido"),
            "juros_real": (trends.get("juros_real") or {}).get("trend", "indefinido"),
            "pib": (trends.get("pib") or {}).get("trend", "indefinido"),
        },
        "pib": pib,
        "divida_publica": divida_publica,
        "domestic_risk_factors": domestic_risk_factors,
        "portfolio_tailwinds": portfolio_tailwinds,
        "portfolio_headwinds": portfolio_headwinds,
        "international_links": international_links,
        "macro_interpretation": macro_context.get("macro_interpretation", []),
    }
