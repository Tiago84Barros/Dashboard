from __future__ import annotations

from typing import Dict, List


DEFAULT_PROFILE = {
    "role_hint": "indefinido",
    "macro_sensitivities": [],
    "style": "indefinido",
    "risk_bucket": "indefinido",
}


ASSET_MACRO_PROFILE: Dict[str, Dict[str, object]] = {
    "BBAS3": {
        "role_hint": "nucleo_renda",
        "macro_sensitivities": ["juros", "credito", "atividade_domestica"],
        "style": "value_dividendos",
        "risk_bucket": "moderado",
    },
    "ITUB4": {
        "role_hint": "qualidade_financeira",
        "macro_sensitivities": ["juros", "credito", "atividade_domestica"],
        "style": "qualidade_renda",
        "risk_bucket": "moderado",
    },
    "PSSA3": {
        "role_hint": "defensivo_renda",
        "macro_sensitivities": ["juros", "atividade_domestica"],
        "style": "defensivo_dividendos",
        "risk_bucket": "moderado",
    },
    "ISAE3": {
        "role_hint": "utility_defensiva",
        "macro_sensitivities": ["juros", "regulacao", "inflacao"],
        "style": "defensivo_renda",
        "risk_bucket": "baixo_moderado",
    },
    "CSMG3": {
        "role_hint": "utility_defensiva",
        "macro_sensitivities": ["juros", "regulacao", "inflacao"],
        "style": "defensivo_renda",
        "risk_bucket": "baixo_moderado",
    },
    "PETR3": {
        "role_hint": "ciclico_renda",
        "macro_sensitivities": ["petroleo", "cambio", "politica", "commodities"],
        "style": "commodity_dividendos",
        "risk_bucket": "alto",
    },
    "PETR4": {
        "role_hint": "ciclico_renda",
        "macro_sensitivities": ["petroleo", "cambio", "politica", "commodities"],
        "style": "commodity_dividendos",
        "risk_bucket": "alto",
    },
    "VALE3": {
        "role_hint": "exportadora_commodity",
        "macro_sensitivities": ["minerio", "china", "cambio", "commodities"],
        "style": "commodity_global",
        "risk_bucket": "alto",
    },
    "BRAP3": {
        "role_hint": "holding_commodity",
        "macro_sensitivities": ["minerio", "china", "cambio", "commodities"],
        "style": "holding_ciclica",
        "risk_bucket": "alto",
    },
    "WEGE3": {
        "role_hint": "qualidade_crescimento",
        "macro_sensitivities": ["atividade_global", "cambio", "juros"],
        "style": "quality_growth",
        "risk_bucket": "moderado",
    },
    "B3SA3": {
        "role_hint": "infra_mercado_capitais",
        "macro_sensitivities": ["juros", "mercado_de_capitais", "apetite_risco"],
        "style": "qualidade_ciclica",
        "risk_bucket": "moderado",
    },
    "DIRR3": {
        "role_hint": "smallcap_domestica",
        "macro_sensitivities": ["juros", "atividade_domestica", "credito"],
        "style": "growth_domestico",
        "risk_bucket": "alto",
    },
    "ROMI3": {
        "role_hint": "industria_ciclica",
        "macro_sensitivities": ["atividade_domestica", "credito", "investimento"],
        "style": "smallcap_ciclica",
        "risk_bucket": "alto",
    },
    "GMAT3": {
        "role_hint": "smallcap_domestica",
        "macro_sensitivities": ["atividade_domestica", "credito", "juros"],
        "style": "smallcap_ciclica",
        "risk_bucket": "alto",
    },
    "MBRF3": {
        "role_hint": "exportadora_alimentos",
        "macro_sensitivities": ["cambio", "commodities", "demanda_externa"],
        "style": "exportadora_renda",
        "risk_bucket": "moderado_alto",
    },
    "DEXP3": {
        "role_hint": "industria_exportadora",
        "macro_sensitivities": ["cambio", "atividade_industrial", "commodities"],
        "style": "industrial_ciclica",
        "risk_bucket": "moderado_alto",
    },
    "HGLG11": {
        "role_hint": "fii_logistica",
        "macro_sensitivities": ["juros", "atividade_domestica", "inflacao"],
        "style": "renda_imobiliaria",
        "risk_bucket": "moderado",
    },
    "KNCR11": {
        "role_hint": "fii_papel",
        "macro_sensitivities": ["juros", "credito", "inflacao"],
        "style": "renda_credito",
        "risk_bucket": "moderado",
    },
    "BRCO11": {
        "role_hint": "fii_logistica",
        "macro_sensitivities": ["juros", "atividade_domestica", "inflacao"],
        "style": "renda_imobiliaria",
        "risk_bucket": "moderado",
    },
    "VISC11": {
        "role_hint": "fii_shopping",
        "macro_sensitivities": ["juros", "atividade_domestica", "consumo"],
        "style": "renda_consumo",
        "risk_bucket": "moderado_alto",
    },
    "HGRE11": {
        "role_hint": "fii_lajes",
        "macro_sensitivities": ["juros", "atividade_domestica", "vacancia"],
        "style": "renda_imobiliaria",
        "risk_bucket": "moderado_alto",
    },
}


def get_asset_macro_profile(ticker: str) -> Dict[str, object]:
    tk = str(ticker or "").strip().upper()
    return ASSET_MACRO_PROFILE.get(tk, DEFAULT_PROFILE.copy())


def build_asset_macro_profiles(tickers: List[str]) -> Dict[str, Dict[str, object]]:
    return {tk: get_asset_macro_profile(tk) for tk in tickers}
