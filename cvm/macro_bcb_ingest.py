from __future__ import annotations

from typing import Callable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


SCHEMA = "cvm"
RAW_TABLE = "macro_bcb"
RAW_FULL = f"{SCHEMA}.{RAW_TABLE}"

WIDE_ANUAL = f"{SCHEMA}.info_economica"
WIDE_MENSAL = f"{SCHEMA}.info_economica_mensal"


SERIES = {
    "SELIC": "selic",
    "IPCA_MENSAL": "ipca",
    "CAMBIO": "cambio",
    "ICC": "icc",
}


# ─────────────────────────────────────────────────────────────
# Criação das tabelas
# ─────────────────────────────────────────────────────────────

def _ensure_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"create schema if not exists {SCHEMA};"))

        conn.execute(text(f"""
        create table if not exists {WIDE_MENSAL} (
            data date primary key,
            selic double precision,
            ipca double precision,
            cambio double precision,
            icc double precision,
            fetched_at timestamptz default now()
        );
        """))


# ─────────────────────────────────────────────────────────────
# Load RAW
# ─────────────────────────────────────────────────────────────

def _load_raw(engine: Engine) -> pd.DataFrame:
    q = text(f"""
        select
            data::date as data,
            series_name::text,
            valor::double precision
        from {RAW_FULL}
        where series_name in :series
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params={"series": tuple(SERIES.keys())})


# ─────────────────────────────────────────────────────────────
# Transformação mensal
# ─────────────────────────────────────────────────────────────

def _to_mensal(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["col"] = df["series_name"].map(SERIES)
    df = df.dropna(subset=["data", "col"])

    df["data"] = pd.to_datetime(df["data"])
    df["periodo"] = df["data"].dt.to_period("M")

    mensal = (
        df.sort_values("data")
          .groupby(["periodo", "col"])["valor"]
          .last()
          .unstack()
          .reset_index()
    )

    mensal["data"] = mensal["periodo"].dt.to_timestamp("M").dt.date
    mensal = mensal.drop(columns=["periodo"])

    cols = ["data"] + list(SERIES.values())
    for c in SERIES.values():
        if c not in mensal.columns:
            mensal[c] = None

    return mensal[cols].sort_values("data").reset_index(drop=True)


# ─────────────────────────────────────────────────────────────
# UPSERT
# ─────────────────────────────────────────────────────────────

def _upsert_mensal(engine: Engine, df: pd.DataFrame) -> None:
    if df.empty:
        return

    sql = f"""
    insert into {WIDE_MENSAL} (data, selic, ipca, cambio, icc, fetched_at)
    values (:data, :selic, :ipca, :cambio, :icc, now())
    on conflict (data) do update set
        selic = excluded.selic,
        ipca = excluded.ipca,
        cambio = excluded.cambio,
        icc = excluded.icc,
        fetched_at = now();
    """

    with engine.begin() as conn:
        conn.execute(text(sql), df.to_dict("records"))


# ─────────────────────────────────────────────────────────────
# Orquestração
# ─────────────────────────────────────────────────────────────

def run(engine: Engine, *, progress_cb: Optional[Callable[[str], None]] = None) -> None:
    _ensure_tables(engine)

    if progress_cb:
        progress_cb("MACRO: carregando dados brutos (BCB)...")

    df_raw = _load_raw(engine)
    if df_raw.empty:
        raise RuntimeError("Tabela cvm.macro_bcb vazia ou incompleta.")

    if progress_cb:
        progress_cb("MACRO: gerando info_economica_mensal...")

    mensal = _to_mensal(df_raw)
    _upsert_mensal(engine, mensal)

    if progress_cb:
        progress_cb(f"MACRO: info_economica_mensal atualizada ({len(mensal)} linhas).")
