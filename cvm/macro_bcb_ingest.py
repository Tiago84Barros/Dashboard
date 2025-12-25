from __future__ import annotations

from typing import Callable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

SCHEMA = "cvm"
RAW_TABLE = "macro_bcb"

WIDE_TABLE = "info_economica"
MONTHLY_TABLE = "info_economica_mensal"

RAW_FULL = f"{SCHEMA}.{RAW_TABLE}"
WIDE_FULL = f"{SCHEMA}.{WIDE_TABLE}"
MONTHLY_FULL = f"{SCHEMA}.{MONTHLY_TABLE}"

# Mapeia NOMES REAIS do RAW (conforme macro_catalog.py) -> colunas finais
# selic: preferimos SELIC_EFETIVA; se não houver, usamos SELIC_META como fallback
SERIES_TO_COL = {
    "IPCA_MENSAL": "ipca",
    "ICC": "icc",
    "PIB": "pib",
    "BALANCA_COMERCIAL": "balanca_comercial",
    "CAMBIO_PTX": "cambio",
    "SELIC_EFETIVA": "selic_efetiva",
    "SELIC_META": "selic_meta",
}


def _ensure_tables(engine: Engine) -> None:
    ddl_schema = f"create schema if not exists {SCHEMA};"

    ddl_wide = f"""
    create table if not exists {WIDE_FULL} (
      data date primary key,
      selic double precision,
      cambio double precision,
      ipca double precision,
      icc double precision,
      pib double precision,
      balanca_comercial double precision,
      fetched_at timestamptz default now()
    );
    """

    ddl_monthly = f"""
    create table if not exists {MONTHLY_FULL} (
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
        conn.execute(text(ddl_schema))
        conn.execute(text(ddl_wide))
        conn.execute(text(ddl_monthly))


def _load_raw(engine: Engine) -> pd.DataFrame:
    series_list = tuple(SERIES_TO_COL.keys())
    q = text(
        f"""
        select
          data::date as data,
          series_name::text as series_name,
          valor::double precision as valor
        from {RAW_FULL}
        where series_name = any(:series_list)
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"series_list": list(series_list)})
    return df


def _to_wide_daily(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()
    df["col"] = df["series_name"].map(SERIES_TO_COL)
    df = df.dropna(subset=["col", "data"])

    # remove duplicidade por (data, col)
    df = df.sort_values(["data"]).drop_duplicates(subset=["data", "col"], keep="last")

    wide = df.pivot(index="data", columns="col", values="valor").reset_index()

    # selic preferida
    # se selic_efetiva existir, usa; senão usa selic_meta
    if "selic_efetiva" in wide.columns and "selic_meta" in wide.columns:
        wide["selic"] = wide["selic_efetiva"].combine_first(wide["selic_meta"])
    elif "selic_efetiva" in wide.columns:
        wide["selic"] = wide["selic_efetiva"]
    elif "selic_meta" in wide.columns:
        wide["selic"] = wide["selic_meta"]
    else:
        wide["selic"] = None

    # cambio
    if "cambio" not in wide.columns:
        wide["cambio"] = None

    # colunas macro
    for c in ["ipca", "icc", "pib", "balanca_comercial"]:
        if c not in wide.columns:
            wide[c] = None

    wide = wide[["data", "selic", "cambio", "ipca", "icc", "pib", "balanca_comercial"]]
    wide = wide.sort_values("data").reset_index(drop=True)
    return wide


def _to_monthly(wide_daily: pd.DataFrame) -> pd.DataFrame:
    if wide_daily.empty:
        return wide_daily

    df = wide_daily.copy()
    df["data"] = pd.to_datetime(df["data"])
    df = df.set_index("data").sort_index()

    # consolida para mês (último dia do mês)
    # daily -> last; monthly já estará posicionada em datas do mês; last resolve também
    monthly = df.resample("M").last()

    # PIB trimestral: tende a cair em um mês específico do trimestre.
    # Opcional: preencher meses seguintes até próximo valor trimestral (forward-fill).
    # Isso ajuda gráficos e comparações mensais não ficarem "quebrados".
    monthly["pib"] = monthly["pib"].ffill()

    monthly = monthly.reset_index()
    monthly["data"] = monthly["data"].dt.date

    cols = ["data", "selic", "ipca", "cambio", "icc", "pib", "balanca_comercial"]
    for c in cols:
        if c not in monthly.columns:
            monthly[c] = None

    return monthly[cols].sort_values("data").reset_index(drop=True)


def _upsert(engine: Engine, table_full: str, df: pd.DataFrame, batch: int = 2000) -> None:
    if df.empty:
        return

    sql = f"""
    insert into {table_full} (
      data, selic, cambio, ipca, icc, pib, balanca_comercial, fetched_at
    )
    values (
      :data, :selic, :cambio, :ipca, :icc, :pib, :balanca_comercial, now()
    )
    on conflict (data) do update set
      selic = excluded.selic,
      cambio = excluded.cambio,
      ipca = excluded.ipca,
      icc = excluded.icc,
      pib = excluded.pib,
      balanca_comercial = excluded.balanca_comercial,
      fetched_at = now();
    """

    rows = df.to_dict("records")
    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(sql), rows[i : i + batch])


def build_macro_tables(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    _ensure_tables(engine)

    if progress_cb:
        progress_cb("MACRO WIDE: carregando RAW (cvm.macro_bcb)...")

    df_raw = _load_raw(engine)
    if df_raw.empty:
        raise RuntimeError("MACRO WIDE: cvm.macro_bcb não contém as séries necessárias do catálogo.")

    if progress_cb:
        progress_cb(f"MACRO WIDE: RAW carregado ({len(df_raw)} linhas). Gerando info_economica...")

    wide_daily = _to_wide_daily(df_raw)

    if progress_cb:
        progress_cb(f"MACRO WIDE: upsert em {WIDE_FULL} ({len(wide_daily)} linhas)...")

    _upsert(engine, WIDE_FULL, wide_daily)

    if progress_cb:
        progress_cb("MACRO WIDE: gerando info_economica_mensal (resample mês)...")

    wide_monthly = _to_monthly(wide_daily)

    if progress_cb:
        progress_cb(f"MACRO WIDE: upsert em {MONTHLY_FULL} ({len(wide_monthly)} linhas)...")

    _upsert(engine, MONTHLY_FULL, wide_monthly)

    if progress_cb:
        progress_cb("MACRO WIDE: tabelas macro atualizadas com sucesso.")


def run(engine: Engine, *, progress_cb: Optional[Callable[[str], None]] = None) -> None:
    build_macro_tables(engine, progress_cb=progress_cb)
