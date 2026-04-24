"""
pipeline_local/utils/hashing.py
Hash estável para deduplicação de linhas no banco local.
"""
from __future__ import annotations

import hashlib
from typing import Any, Sequence


def row_hash(*parts: Any) -> str:
    """
    Retorna SHA-256 hex dos campos concatenados com '|'.
    Garante estabilidade: None vira '', números viram str(float).
    """
    def _fmt(v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, float):
            return f"{v:.6f}"
        return str(v)

    raw = "|".join(_fmt(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def dataframe_row_hash(df: "pd.DataFrame", cols: Sequence[str]) -> "pd.Series":
    """
    Calcula row_hash vetorizado para um DataFrame.
    Uso:
        df['row_hash'] = dataframe_row_hash(df, ['ticker', 'dt_refer', 'cd_conta', 'vl_conta'])
    """
    import pandas as pd
    import hashlib

    def _hash_row(row: pd.Series) -> str:
        raw = "|".join("" if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v)
                       for v in row)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    return df[list(cols)].apply(_hash_row, axis=1)
