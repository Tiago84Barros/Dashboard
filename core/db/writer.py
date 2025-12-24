# core/db/writer.py
from __future__ import annotations

import pandas as pd
from sqlalchemy.engine import Engine


def write_dataframe(
    df: pd.DataFrame,
    *,
    engine: Engine,
    table: str,
    schema: str = "cvm",
    if_exists: str = "append",
    chunksize: int = 2000,
) -> None:
    """
    Grava DataFrame usando to_sql.
    if_exists: 'append' | 'replace' | 'fail'
    """
    if df is None or df.empty:
        return

    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=chunksize,
    )
