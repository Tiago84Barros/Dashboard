from __future__ import annotations

import datetime as dt
from typing import Callable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


SCHEMA = "cvm"
RAW_TABLE = "macro_bcb"
WIDE_TABLE = "info_economica"
RAW_FULL = f"{SCHEMA}.{RAW_TABLE}"
WIDE_FULL = f"{SCHEMA}.{WIDE_TABLE}"


# Mapeie aqui os nomes das séries do RAW -> colunas do WIDE
# Ajuste os valores à esquerda exatamente como aparecem em series_name no seu cvm.macro_bcb
SERIES_TO_COL = {
    "SELIC": "selic",
    "CAMBIO": "cambio",
    "IPCA_MENSAL": "ipca",
    "ICC": "icc",
    "PIB": "pib",
    "BALANCA_COMERCIAL": "balanca_comercial",
}


def _ensure_wide_table(engine: Engine) -> None:
    ddl_schema = f"create schema if not exists {SCHEMA};"
    ddl_table = f"""
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
    with engine.begin() as conn:
        conn.execute(text(ddl_schema))
        conn.execute(text(ddl_table))


def _load_raw(engine: Engine) -> pd.DataFrame:
    # Puxa só séries necessárias para o WIDE.
    series_list = tuple(SERIES_TO_COL.keys())
    q = text(
        f"""
        select
          data::date as data,
          series_name::text as series_name,
          valor::double precision as valor
        from {RAW_FULL}
        where series_name in :series_list
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"series_list": series_list})
    return df


def _to_wide(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()
    df["col"] = df["series_name"].map(SERIES_TO_COL)
    df = df.dropna(subset=["col", "data"])

    # Para evitar duplicidades (ex.: série diária vs mensal), você pode precisar
    # escolher última observação do mês/ano. Aqui adotamos "último valor por data".
    df = df.sort_values(["data"]).drop_duplicates(subset=["data", "col"], keep="last")

    wide = df.pivot(index="data", columns="col", values="valor").reset_index()

    # Garante colunas existentes
    for c in SERIES_TO_COL.values():
        if c not in wide.columns:
            wide[c] = None

    # Ordena
    cols = ["data"] + list(SERIES_TO_COL.values())
    wide = wide[cols].sort_values("data").reset_index(drop=True)

    return wide


def _upsert_wide(engine: Engine, wide: pd.DataFrame, batch: int = 2000) -> None:
    if wide.empty:
        return

    sql = f"""
    insert into {WIDE_FULL} (
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

    rows = wide.to_dict("records")
    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(sql), rows[i : i + batch])


def build_info_economica_from_raw(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    _ensure_wide_table(engine)

    if progress_cb:
        progress_cb("MACRO (BCB): carregando tabela bruta cvm.macro_bcb...")

    df_raw = _load_raw(engine)

    if df_raw.empty:
        raise RuntimeError(
            "MACRO (BCB): cvm.macro_bcb não possui as séries necessárias para gerar cvm.info_economica."
        )

    if progress_cb:
        progress_cb("MACRO (BCB): transformando para formato wide (info_economica)...")

    wide = _to_wide(df_raw)

    if progress_cb:
        progress_cb(f"MACRO (BCB): upsert em {WIDE_FULL} ({len(wide)} linhas)...")

    _upsert_wide(engine, wide)

    if progress_cb:
        progress_cb("MACRO (BCB): info_economica atualizada com sucesso.")


# Mantém compatibilidade com seu orquestrador: chama run(engine, progress_cb=...)
def run(engine: Engine, *, progress_cb: Optional[Callable[[str], None]] = None) -> None:
    # O ingest bruto pode continuar existindo (caso seu projeto já preencha macro_bcb).
    # Aqui garantimos a tabela wide que você precisa.
    build_info_economica_from_raw(engine, progress_cb=progress_cb)
