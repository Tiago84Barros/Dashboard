from __future__ import annotations

from typing import Callable, Optional
import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ============================================================
# CONFIGURAÇÃO LOCAL (decisão de produto)
# ============================================================

START_YEAR = 2010

# ============================================================
# TABELAS
# ============================================================

SCHEMA = "cvm"
RAW_TABLE = "macro_bcb"
WIDE_TABLE = "info_economica_mensal"

RAW_FULL = f"{SCHEMA}.{RAW_TABLE}"
WIDE_FULL = f"{SCHEMA}.{WIDE_TABLE}"

# ============================================================
# MAPEAMENTO CORRETO (bate com macro_catalog / RAW)
# ============================================================

SERIES_TO_COL = {
    "SELIC_EFETIVA": "selic",
    "SELIC_META": "selic",            # fallback
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

# ============================================================
# DDL
# ============================================================

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

# ============================================================
# LOAD RAW (COM CORTE EM 2010)
# ============================================================

def _load_raw(engine: Engine) -> pd.DataFrame:
    sql = text(f"""
        select
            data::date as data,
            series_name::text as series_name,
            valor::double precision as valor
        from {RAW_FULL}
        where series_name in :series_list
          and data >= make_date(:start_year, 1, 1)
    """)

    with engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={
                "series_list": tuple(SERIES_TO_COL.keys()),
                "start_year": START_YEAR,
            },
        )

    return df

# ============================================================
# TRANSFORMAÇÃO PARA WIDE (MENSAL)
# ============================================================

def _to_wide(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()
    df["col"] = df["series_name"].map(SERIES_TO_COL)
    df = df.dropna(subset=["data", "col"])

    # ordena para permitir fallback SELIC_META → SELIC_EFETIVA
    df["priority"] = df["series_name"].apply(
        lambda x: 0 if x == "SELIC_EFETIVA" else 1
    )

    df = (
        df.sort_values(["data", "col", "priority"])
        .drop_duplicates(subset=["data", "col"], keep="first")
        .drop(columns="priority")
    )

    wide = df.pivot(index="data", columns="col", values="valor").reset_index()

    # garante todas as colunas
    for col in REQUIRED_COLS:
        if col not in wide.columns:
            wide[col] = None

    wide = wide[["data"] + REQUIRED_COLS].sort_values("data")

    # NaN → None (Postgres-safe)
    wide = wide.replace({np.nan: None})

    return wide.reset_index(drop=True)

# ============================================================
# UPSERT
# ============================================================

def _upsert_wide(engine: Engine, wide: pd.DataFrame, batch: int = 2000) -> None:
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

    # blindagem final
    for r in rows:
        for col in REQUIRED_COLS:
            r.setdefault(col, None)

    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(sql), rows[i:i + batch])

# ============================================================
# PIPELINE
# ============================================================

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
            "MACRO (BCB): tabela cvm.macro_bcb não possui séries após 2010."
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
