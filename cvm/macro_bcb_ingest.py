# core/ingest/macro_bcb_ingest.py
from __future__ import annotations

from typing import Callable, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.config.settings import START_YEAR

SCHEMA = "cvm"
RAW_TABLE = "macro_bcb"
WIDE_TABLE = "info_economica_mensal"

RAW_FULL = f"{SCHEMA}.{RAW_TABLE}"
WIDE_FULL = f"{SCHEMA}.{WIDE_TABLE}"

# Mapeamento CORRETO (bate com core/macro_catalog.py e o RAW ingerido)
# Regras:
# - Selic: preferir SELIC_EFETIVA; se não houver, cair para SELIC_META
SERIES_TO_COL = {
    "SELIC_EFETIVA": "selic",
    "SELIC_META": "selic",  # fallback
    "CAMBIO_PTX": "cambio",
    "IPCA_MENSAL": "ipca",
    "ICC": "icc",
    "PIB": "pib",
    "BALANCA_COMERCIAL": "balanca_comercial",
}

REQUIRED_COLS = [
    "selic",
    "ipca",
    "cambio",
    "icc",
    "pib",
    "balanca_comercial",
]


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
    sql = text(
        f"""
        select
            data::date as data,
            series_name::text as series_name,
            valor::double precision as valor
        from {RAW_FULL}
        where series_name in :series_list
          and data >= make_date(:start_year, 1, 1)
        """
    )

    with engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={
                "series_list": tuple(SERIES_TO_COL.keys()),
                "start_year": int(START_YEAR),
            },
        )

    return df


def _to_wide(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()
    df["col"] = df["series_name"].map(SERIES_TO_COL)
    df = df.dropna(subset=["data", "col"])

    # Prioridade: manter SELIC_EFETIVA quando coexistir com SELIC_META
    # (menor priority = mais importante)
    df["priority"] = df["series_name"].apply(lambda x: 0 if x == "SELIC_EFETIVA" else 1)

    # Para cada (data, col), escolhe a linha de maior prioridade (priority menor)
    df = (
        df.sort_values(["data", "col", "priority"])
        .drop_duplicates(subset=["data", "col"], keep="first")
        .drop(columns="priority")
    )

    wide = df.pivot(index="data", columns="col", values="valor").reset_index()

    # Garante todas as colunas
    for c in REQUIRED_COLS:
        if c not in wide.columns:
            wide[c] = None

    wide = wide[["data"] + REQUIRED_COLS].sort_values("data").reset_index(drop=True)

    # NaN → None (Postgres-friendly)
    wide = wide.replace({np.nan: None})

    return wide


def _upsert_wide(engine: Engine, wide: pd.DataFrame, batch: int = 5000) -> None:
    if wide.empty:
        return

    sql = f"""
    insert into {WIDE_FULL} (
        data,
        selic,
        ipca,
        cambio,
        icc,
        pib,
        balanca_comercial,
        fetched_at
    ) values (
        :data,
        :selic,
        :ipca,
        :cambio,
        :icc,
        :pib,
        :balanca_comercial,
        now()
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

    # Blindagem final: todas as chaves existem em todas as linhas
    for r in rows:
        for col in REQUIRED_COLS:
            r.setdefault(col, None)

    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(sql), rows[i : i + batch])


def build_info_economica_mensal(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    _ensure_wide_table(engine)

    if progress_cb:
        progress_cb(f"MACRO (BCB): carregando cvm.macro_bcb (>= {START_YEAR})...")

    df_raw = _load_raw(engine)

    if df_raw.empty:
        raise RuntimeError(
            f"MACRO (BCB): cvm.macro_bcb não possui séries necessárias após {START_YEAR}."
        )

    if progress_cb:
        progress_cb("MACRO (BCB): transformando para formato mensal (wide)...")

    wide = _to_wide(df_raw)

    if progress_cb:
        progress_cb(f"MACRO (BCB): upsert em {WIDE_FULL} ({len(wide)} linhas)...")

    _upsert_wide(engine, wide)

    if progress_cb:
        progress_cb("MACRO (BCB): info_economica_mensal atualizada com sucesso.")


def run(engine: Engine, *, progress_cb: Optional[Callable[[str], None]] = None) -> None:
    build_info_economica_mensal(engine, progress_cb=progress_cb)
