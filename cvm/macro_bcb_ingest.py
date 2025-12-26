from __future__ import annotations

from typing import Callable, Optional
import math
import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

SCHEMA = "cvm"
RAW_TABLE = "macro_bcb"
WIDE_TABLE = "info_economica_mensal"

RAW_FULL = f"{SCHEMA}.{RAW_TABLE}"
WIDE_FULL = f"{SCHEMA}.{WIDE_TABLE}"

# Mapeamento das séries do RAW → colunas finais
SERIES_TO_COL = {
    "SELIC": "selic",
    "CAMBIO": "cambio",
    "IPCA_MENSAL": "ipca",
    "ICC": "icc",
    "PIB": "pib",
    "BALANCA_COMERCIAL": "balanca_comercial",
}

REQUIRED_COLS = list(SERIES_TO_COL.values())


def _ensure_wide_table(engine: Engine) -> None:
    ddl = f"""
    create schema if not exists {SCHEMA};

    create table if not exists {WIDE_FULL} (
        data date primary key,
        selic double precision,
        ipca double precision,
        cambio double precision,
        icc double precision,
        pib double precision,
        balanca_comercial double precision,
        fetched_at timestamptz default now()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _load_raw(engine: Engine) -> pd.DataFrame:
    series_list = tuple(SERIES_TO_COL.keys())

    sql = text(f"""
        select
            data::date as data,
            series_name::text as series_name,
            valor::double precision as valor
        from {RAW_FULL}
        where series_name in :series_list
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"series_list": series_list})

    return df


def _to_wide(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()
    df["col"] = df["series_name"].map(SERIES_TO_COL)
    df = df.dropna(subset=["data", "col"])

    df = (
        df.sort_values("data")
        .drop_duplicates(subset=["data", "col"], keep="last")
    )

    wide = df.pivot(index="data", columns="col", values="valor").reset_index()

    # garante TODAS as colunas esperadas
    for col in REQUIRED_COLS:
        if col not in wide.columns:
            wide[col] = None

    wide = wide[["data"] + REQUIRED_COLS].sort_values("data")

    # NaN → None (Postgres-friendly)
    wide = wide.replace({np.nan: None})

    return wide.reset_index(drop=True)


def _upsert_wide(engine: Engine, wide: pd.DataFrame, batch: int = 2000) -> None:
    if wide.empty:
        return

    sql = f"""
    insert into {WIDE_FULL} (
        data, selic, ipca, cambio, icc, pib, balanca_comercial, fetched_at
    ) values (
        :data, :selic, :ipca, :cambio, :icc, :pib, :balanca_comercial, now()
    )
    on conflict (data) do update set
        selic = excluded.selic,
        ipca = excluded.ipca,
        cambio = excluded.cambio,
        icc = excluded.icc,
        pib = excluded.pib,
        balanca_comercial = excluded.balanca_comercial,
        fetched_at = now();
    """

    rows = wide.to_dict("records")

    # blindagem final: garante chave em todas as linhas
    for r in rows:
        for k in REQUIRED_COLS:
            r.setdefault(k, None)

    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(sql), rows[i:i + batch])


def build_info_economica_mensal(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    _ensure_wide_table(engine)

    if progress_cb:
        progress_cb("MACRO (BCB): carregando dados brutos…")

    df_raw = _load_raw(engine)

    if df_raw.empty:
        raise RuntimeError(
            "MACRO (BCB): tabela cvm.macro_bcb não possui séries suficientes."
        )

    if progress_cb:
        progress_cb("MACRO (BCB): transformando para formato mensal…")

    wide = _to_wide(df_raw)

    if progress_cb:
        progress_cb(
            f"MACRO (BCB): upsert em {WIDE_FULL} ({len(wide)} linhas)…"
        )

    _upsert_wide(engine, wide)

    if progress_cb:
        progress_cb("MACRO (BCB): info_economica_mensal atualizada com sucesso.")


def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    build_info_economica_mensal(engine, progress_cb=progress_cb)
