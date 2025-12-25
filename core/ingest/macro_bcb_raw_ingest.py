from __future__ import annotations

import datetime as dt
from typing import Callable, Optional

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.macro_catalog import BCB_SERIES_CATALOG

SCHEMA = "cvm"
RAW_TABLE = "macro_bcb"
RAW_FULL = f"{SCHEMA}.{RAW_TABLE}"

BCB_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"


def _ensure_raw_table(engine: Engine) -> None:
    ddl_schema = f"create schema if not exists {SCHEMA};"
    ddl_table = f"""
    create table if not exists {RAW_FULL} (
      data date not null,
      series_name text not null,
      valor double precision,
      fetched_at timestamptz default now(),
      primary key (data, series_name)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl_schema))
        conn.execute(text(ddl_table))


def _fetch_sgs(codigo: int) -> pd.DataFrame:
    url = BCB_URL.format(codigo=codigo)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()
    df = pd.DataFrame(data)
    if df.empty:
        return df
    # BCB devolve data em dd/mm/yyyy
    df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce").dt.date
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df = df.dropna(subset=["data"])
    return df[["data", "valor"]]


def _upsert_raw(engine: Engine, df: pd.DataFrame, batch: int = 2000) -> None:
    if df.empty:
        return

    sql = f"""
    insert into {RAW_FULL} (data, series_name, valor, fetched_at)
    values (:data, :series_name, :valor, now())
    on conflict (data, series_name) do update set
      valor = excluded.valor,
      fetched_at = now();
    """

    rows = df.to_dict("records")
    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(sql), rows[i : i + batch])


def ingest_macro_bcb_raw(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    _ensure_raw_table(engine)

    total_series = len(BCB_SERIES_CATALOG)
    ok, fail = 0, 0
    frames: list[pd.DataFrame] = []

    if progress_cb:
        progress_cb(f"MACRO RAW: iniciando ingest de {total_series} séries do BCB (SGS).")

    for idx, (series_name, meta) in enumerate(BCB_SERIES_CATALOG.items(), start=1):
        codigo = meta.get("sgs")
        if progress_cb:
            progress_cb(f"MACRO RAW: ({idx}/{total_series}) baixando {series_name} (SGS {codigo})...")

        try:
            df = _fetch_sgs(int(codigo))
            if df.empty:
                # não é erro fatal: só registra
                if progress_cb:
                    progress_cb(f"MACRO RAW: {series_name} retornou 0 linhas.")
                fail += 1
                continue

            df["series_name"] = series_name
            frames.append(df)
            ok += 1

            if progress_cb:
                progress_cb(f"MACRO RAW: {series_name} OK ({len(df)} linhas).")

        except Exception as e:
            fail += 1
            if progress_cb:
                progress_cb(f"MACRO RAW: ERRO em {series_name} (SGS {codigo}): {e}")

    if not frames:
        raise RuntimeError(
            "MACRO RAW: nenhuma série foi ingerida. Verifique conectividade do Streamlit com api.bcb.gov.br "
            "e os códigos SGS em core/macro_catalog.py."
        )

    all_df = pd.concat(frames, ignore_index=True)
    _upsert_raw(engine, all_df)

    if progress_cb:
        progress_cb(
            f"MACRO RAW: concluído. Séries OK: {ok}/{total_series}. Séries com falha/sem dados: {fail}. "
            f"Linhas gravadas (total): {len(all_df)}."
        )


# Compatível com o orquestrador do Configurações
def run(engine: Engine, *, progress_cb: Optional[Callable[[str], None]] = None) -> None:
    ingest_macro_bcb_raw(engine, progress_cb=progress_cb)
