from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import text

from core.db_loader import get_supabase_engine


def _to_float(value: Any) -> Optional[float]:
    """Converte para float, retornando None para NULL do banco (pandas converte NULL → NaN)."""
    try:
        if value is None:
            return None
        f = float(value)
        if f != f:  # NaN != NaN é True — único teste portável sem import math
            return None
        return f
    except Exception:
        return None


def _to_date(value: Any) -> Optional[date]:
    try:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()
        return pd.to_datetime(value).date()
    except Exception:
        return None


def _year_progress_info(ref_date: Optional[date]) -> Dict[str, Any]:
    if ref_date is None:
        return {
            "year": None,
            "month": None,
            "is_closed_year": False,
            "is_partial_year": False,
        }

    return {
        "year": ref_date.year,
        "month": ref_date.month,
        "is_closed_year": ref_date.month == 12,
        "is_partial_year": ref_date.month < 12,
    }


def _classify_annual_indicator(ref_date: Optional[date], indicator_name: str) -> Dict[str, Any]:
    info = _year_progress_info(ref_date)

    if info["year"] is None:
        return {
            "indicator": indicator_name,
            "reference_year": None,
            "reference_month": None,
            "interpretation": "desconhecida",
            "label": f"{indicator_name}_desconhecido",
        }

    if info["is_closed_year"]:
        return {
            "indicator": indicator_name,
            "reference_year": info["year"],
            "reference_month": info["month"],
            "interpretation": "anual_fechado",
            "label": f"{indicator_name}_anual_fechado",
        }

    return {
        "indicator": indicator_name,
        "reference_year": info["year"],
        "reference_month": info["month"],
        "interpretation": "acumulado_no_ano_ate_mes",
        "label": f"{indicator_name}_acumulado_ate_mes",
    }


def _trend_label(current: Optional[float], previous: Optional[float], *, tolerance: float = 1e-9) -> str:
    if current is None or previous is None:
        return "indefinido"
    delta = current - previous
    if abs(delta) <= tolerance:
        return "estavel"
    return "alta" if delta > 0 else "queda"


def _trend_payload(current: Optional[float], previous: Optional[float], *, tolerance: float = 1e-9) -> Dict[str, Any]:
    return {
        "current": current,
        "previous": previous,
        "delta": None if current is None or previous is None else round(current - previous, 4),
        "trend": _trend_label(current, previous, tolerance=tolerance),
    }


def _pick_first_non_null(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _build_macro_interpretation(trends: Dict[str, Dict[str, Any]]) -> List[str]:
    bullets: List[str] = []

    selic = trends.get("selic", {})
    cambio = trends.get("cambio", {})
    ipca_12m = trends.get("ipca_12m", {})
    confiança = trends.get("icc", {})

    if selic.get("trend") == "alta":
        bullets.append("Juros em alta elevam custo de capital e reforçam pressão sobre negócios domésticos dependentes de crédito.")
    elif selic.get("trend") == "queda":
        bullets.append("Juros em queda aliviam desconto de fluxo e podem abrir espaço para ativos domésticos mais sensíveis a re-rating.")

    if cambio.get("trend") == "alta":
        bullets.append("Câmbio em alta favorece receitas dolarizadas e exportadoras, mas pressiona companhias dependentes de insumos importados.")
    elif cambio.get("trend") == "queda":
        bullets.append("Câmbio em queda reduz pressão de custos importados, mas retira parte do hedge natural de exportadoras.")

    if ipca_12m.get("trend") == "alta":
        bullets.append("Inflação acelerando aumenta o risco de manutenção de política monetária restritiva por mais tempo.")
    elif ipca_12m.get("trend") == "queda":
        bullets.append("Inflação desacelerando melhora a previsibilidade macro e reduz o risco de aperto monetário adicional.")

    if confiança.get("trend") == "alta":
        bullets.append("Confiança melhorando tende a sustentar atividade doméstica e consumo.")
    elif confiança.get("trend") == "queda":
        bullets.append("Confiança piorando tende a pesar sobre consumo, investimento e sensibilidade de resultados domésticos.")

    return bullets


def load_latest_macro_context() -> Dict[str, Any]:
    engine = get_supabase_engine()

    q_mensal = text(
        """
        select *
        from public.info_economica_mensal
        order by data desc
        limit 2
        """
    )

    q_anual = text(
        """
        select *
        from public.info_economica
        order by data desc
        limit 2
        """
    )

    mensal_atual: Dict[str, Any] = {}
    mensal_anterior: Dict[str, Any] = {}
    anual_atual: Dict[str, Any] = {}
    anual_anterior: Dict[str, Any] = {}

    with engine.connect() as conn:
        df_m = pd.read_sql_query(q_mensal, conn)
        df_a = pd.read_sql_query(q_anual, conn)

    if not df_m.empty:
        mensal_atual = df_m.iloc[0].to_dict()
        if len(df_m) > 1:
            mensal_anterior = df_m.iloc[1].to_dict()

    if not df_a.empty:
        anual_atual = df_a.iloc[0].to_dict()
        if len(df_a) > 1:
            anual_anterior = df_a.iloc[1].to_dict()

    return build_macro_context(
        mensal=mensal_atual,
        anual=anual_atual,
        mensal_prev=mensal_anterior,
        anual_prev=anual_anterior,
    )


def build_macro_context(
    *,
    mensal: Dict[str, Any],
    anual: Dict[str, Any],
    mensal_prev: Optional[Dict[str, Any]] = None,
    anual_prev: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    mensal_prev = mensal_prev or {}
    anual_prev = anual_prev or {}

    anual_data = _to_date(anual.get("data"))
    ipca_annual_meta = _classify_annual_indicator(anual_data, "ipca")

    # IPCA 12m: meses recentes podem vir NULL no banco enquanto o dado ainda não é publicado.
    # Fallback: usa o valor mais recente não-nulo (linha anterior) para não exibir vazio.
    _ipca_12m = _to_float(mensal.get("IPCA_12m"))
    if _ipca_12m is None:
        _ipca_12m = _to_float(mensal_prev.get("IPCA_12m"))

    mensal_payload = {
        "data": str(mensal.get("data") or ""),
        "selic_final": _to_float(mensal.get("Selic_Final")),
        "selic_media": _to_float(mensal.get("Selic_Media")),
        "cambio_final": _to_float(mensal.get("Cambio_Final")),
        "ipca_mom": _to_float(mensal.get("IPCA_MoM")),
        "ipca_12m": _ipca_12m,
        "icc_final": _to_float(mensal.get("ICC_Final")),
        "icc_media": _to_float(mensal.get("ICC_Media")),
        "icc_delta_12m": _to_float(mensal.get("ICC_delta_12m")),
        "balanca_comercial": _to_float(mensal.get("BALANCA_COMERCIAL")),
        "divida_publica_final": _to_float(mensal.get("Divida_Publica_Final")),
        "juros_real_ex_ante_12m": _to_float(mensal.get("Juros_Real_ExAnte_12m")),
    }

    anual_payload = {
        "data": str(anual.get("data") or ""),
        "selic": _to_float(anual.get("selic")),
        "cambio": _to_float(anual.get("Cambio")),
        "ipca": _to_float(anual.get("ipca")),
        "ipca_interpretation": ipca_annual_meta["interpretation"],
        "ipca_reference_year": ipca_annual_meta["reference_year"],
        "ipca_reference_month": ipca_annual_meta["reference_month"],
        "icc": _to_float(anual.get("ICC")),
        "pib": _to_float(anual.get("PIB")),
        "balanca_comercial": _to_float(anual.get("BALANÇA_COMERCIAL")),
        "icc_delta": _to_float(anual.get("ICC_delta")),
        "divida_publica": _to_float(anual.get("Divida_Publica")),
        "juros_real_ex_ante": _to_float(anual.get("Juros_Real_ExAnte")),
    }

    trends = {
        "selic": _trend_payload(
            mensal_payload["selic_final"],
            _pick_first_non_null(_to_float(mensal_prev.get("Selic_Final")), _to_float(anual_prev.get("selic"))),
            tolerance=0.01,
        ),
        "cambio": _trend_payload(
            mensal_payload["cambio_final"],
            _pick_first_non_null(_to_float(mensal_prev.get("Cambio_Final")), _to_float(anual_prev.get("Cambio"))),
            tolerance=0.01,
        ),
        "ipca_12m": _trend_payload(
            mensal_payload["ipca_12m"],
            _pick_first_non_null(_to_float(mensal_prev.get("IPCA_12m")), _to_float(anual_prev.get("ipca"))),
            tolerance=0.05,
        ),
        "icc": _trend_payload(
            mensal_payload["icc_media"],
            _pick_first_non_null(_to_float(mensal_prev.get("ICC_Media")), _to_float(anual_prev.get("ICC"))),
            tolerance=0.1,
        ),
        "juros_real": _trend_payload(
            mensal_payload["juros_real_ex_ante_12m"],
            _pick_first_non_null(_to_float(mensal_prev.get("Juros_Real_ExAnte_12m")), _to_float(anual_prev.get("Juros_Real_ExAnte"))),
            tolerance=0.05,
        ),
        "pib": _trend_payload(
            anual_payload["pib"],
            _to_float(anual_prev.get("PIB")),
            tolerance=0.05,
        ),
    }

    macro_summary = {
        "reference_date": mensal_payload["data"] or anual_payload["data"],
        "selic_current": mensal_payload["selic_final"],
        "selic_trend": trends["selic"]["trend"],
        "ipca_12m_current": mensal_payload["ipca_12m"],
        "ipca_12m_trend": trends["ipca_12m"]["trend"],
        "cambio_current": mensal_payload["cambio_final"],
        "cambio_trend": trends["cambio"]["trend"],
        "icc_current": mensal_payload["icc_media"],
        "icc_trend": trends["icc"]["trend"],
        "juros_real_current": mensal_payload["juros_real_ex_ante_12m"],
        "juros_real_trend": trends["juros_real"]["trend"],
        "pib_current": anual_payload["pib"],
        "pib_trend": trends["pib"]["trend"],
    }

    return {
        "mensal": mensal_payload,
        "anual": anual_payload,
        "previous": {
            "mensal": {
                "data": str(mensal_prev.get("data") or ""),
                "selic_final": _to_float(mensal_prev.get("Selic_Final")),
                "cambio_final": _to_float(mensal_prev.get("Cambio_Final")),
                "ipca_12m": _to_float(mensal_prev.get("IPCA_12m")),
                "icc_media": _to_float(mensal_prev.get("ICC_Media")),
                "juros_real_ex_ante_12m": _to_float(mensal_prev.get("Juros_Real_ExAnte_12m")),
            },
            "anual": {
                "data": str(anual_prev.get("data") or ""),
                "selic": _to_float(anual_prev.get("selic")),
                "cambio": _to_float(anual_prev.get("Cambio")),
                "ipca": _to_float(anual_prev.get("ipca")),
                "icc": _to_float(anual_prev.get("ICC")),
                "pib": _to_float(anual_prev.get("PIB")),
                "juros_real_ex_ante": _to_float(anual_prev.get("Juros_Real_ExAnte")),
            },
        },
        "trends": trends,
        "macro_summary": macro_summary,
        "macro_interpretation": _build_macro_interpretation(trends),
    }
