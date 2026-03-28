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
    if cambio >=
