from __future__ import annotations

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
            "icc": _to_float(anual.get("ICC")),
            "pib": _to_float(anual.get("PIB")),
            "balanca_comercial": _to_float(anual.get("BALANÇA_COMERCIAL")),
            "icc_delta": _to_float(anual.get("ICC_delta")),
            "divida_publica": _to_float(anual.get("Divida_Publica")),
            "juros_real_ex_ante": _to_float(anual.get("Juros_Real_ExAnte")),
        },
    }
