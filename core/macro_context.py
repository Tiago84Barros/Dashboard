from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import text

from core.db_loader import get_supabase_engine


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
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


def load_latest_macro_context() -> Dict[str, Any]:
    engine = get_supabase_engine()

    q_mensal = text(
        """
        select *
        from public.info_economica_mensal
        order by data desc
        limit 1
        """
    )

    q_anual = text(
        """
        select *
        from public.info_economica
        order by data desc
        limit 1
        """
    )

    mensal = {}
    anual = {}

    with engine.connect() as conn:
        df_m = pd.read_sql_query(q_mensal, conn)
        df_a = pd.read_sql_query(q_anual, conn)

    if not df_m.empty:
        mensal = df_m.iloc[0].to_dict()

    if not df_a.empty:
        anual = df_a.iloc[0].to_dict()

    return build_macro_context(mensal=mensal, anual=anual)


def build_macro_context(
    *,
    mensal: Dict[str, Any],
    anual: Dict[str, Any],
) -> Dict[str, Any]:
    anual_data = _to_date(anual.get("data"))
    ipca_annual_meta = _classify_annual_indicator(anual_data, "ipca")

    return {
        "mensal": {
            "data": str(mensal.get("data") or ""),
            "selic_final": _to_float(mensal.get("Selic_Final")),
            "selic_media": _to_float(mensal.get("Selic_Media")),
            "cambio_final": _to_float(mensal.get("Cambio_Final")),
            "ipca_mom": _to_float(mensal.get("IPCA_MoM")),
            "ipca_12m": _to_float(mensal.get("IPCA_12m")),
            "icc_final": _to_float(mensal.get("ICC_Final")),
            "icc_media": _to_float(mensal.get("ICC_Media")),
            "icc_delta_12m": _to_float(mensal.get("ICC_delta_12m")),
            "balanca_comercial": _to_float(mensal.get("BALANCA_COMERCIAL")),
            "divida_publica_final": _to_float(mensal.get("Divida_Publica_Final")),
            "juros_real_ex_ante_12m": _to_float(mensal.get("Juros_Real_ExAnte_12m")),
        },
        "anual": {
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
        },
    }
