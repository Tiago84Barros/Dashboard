# core/ingest/macro_bcb_raw_ingest.py
from __future__ import annotations

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

    # Tabela alinhada ao seu schema atual (series_code NOT NULL)
    ddl_table = f"""
    create table if not exists {RAW_FULL} (
      data date not null,
      series_code integer not null,
      series_name text not null,
      valor double precision,
      fetched_at timestamptz default now(),
      primary key (data, series_code)
    );
    """

    # Migração defensiva (caso a tabela exista sem essas colunas)
    ddl_migrate = f"""
    alter table {RAW_FULL}
      add column if not exists series_code integer;

    alter table {RAW_FULL}
      add column if not exists fetched_at timestamptz default now();

    -- Garantir NOT NULL em series_code caso já tenha sido criado sem constraint
    -- (não força imediatamente se houver NULL legado; o ingest abaixo não gera NULL)
    """

    # Índice/constraint para ON CONFLICT (data, series_code)
    ddl_index = f"""
    create unique index if not exists macro_bcb_data_series_code_uk
      on {RAW_FULL} (data, series_code);
    """

    with engine.begin() as conn:
        conn.execute(text(ddl_schema))
        conn.execute(text(ddl_table))
        conn.execute(text(ddl_migrate))
        conn.execute(text(ddl_index))


def _fetch_sgs(codigo: int) -> pd.DataFrame:
    """
    Baixa série do BCB SGS e retorna DataFrame com colunas:
      - data (date)
      - valor (float)
    Observação: o BCB pode devolver valor com vírgula decimal (ex.: "13,75").
    """
    url = BCB_URL.format(codigo=codigo)
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    data = r.json()
    df = pd.DataFrame(data)
    if df.empty:
        return df

    # BCB devolve data em dd/mm/yyyy
    df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce").dt.date

    # Parse robusto do "valor" (aceita "13,75", "13.75", etc.)
    s = df["valor"].astype(str).str.strip()
    s = s.str.replace(",", ".", regex=False)
    s = s.str.replace(r"[^0-9\.\-]", "", regex=True)  # limpa ruídos
    df["valor"] = pd.to_numeric(s, errors="coerce")

    df = df.dropna(subset=["data"])
    return df[["data", "valor"]]


def _upsert_raw(engine: Engine, df: pd.DataFrame, batch: int = 2000) -> None:
    if df.empty:
        return

    # Agora inclui series_code e faz conflito por (data, series_code)
    sql = f"""
    insert into {RAW_FULL} (data, series_code, series_name, valor, fetched_at)
    values (:data, :series_code, :series_name, :valor, now())
    on conflict (data, series_code) do update set
      series_name = excluded.series_name,
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
            series_code = int(codigo)

            df = _fetch_sgs(series_code)
            if df.empty:
                fail += 1
                if progress_cb:
                    progress_cb(f"MACRO RAW: {series_name} retornou 0 linhas.")
                continue

            if df["valor"].notna().sum() == 0:
                fail += 1
                if progress_cb:
                    progress_cb(f"MACRO RAW: {series_name} retornou valores inválidos (tudo NULL após parse).")
                continue

            # >>>>>>> AQUI ESTÁ O PONTO CRÍTICO: preencher series_code <<<<<<<
            df["series_code"] = series_code
            df["series_name"] = series_name

            frames.append(df)
            ok += 1

            if progress_cb:
                progress_cb(
                    f"MACRO RAW: {series_name} OK "
                    f"({len(df)} linhas, {int(df['valor'].notna().sum())} valores válidos)."
                )

        except Exception as e:
            fail += 1
            if progress_cb:
                progress_cb(f"MACRO RAW: ERRO em {series_name} (SGS {codigo}): {e}")

    if not frames:
        raise RuntimeError(
            "MACRO RAW: nenhuma série foi ingerida com valores válidos. "
            "Verifique conectividade com api.bcb.gov.br e os códigos SGS em core/macro_catalog.py."
        )

    all_df = pd.concat(frames, ignore_index=True)

    # Garantia final: series_code nunca pode ser nulo
    all_df = all_df.dropna(subset=["data", "series_code", "series_name"])

    _upsert_raw(engine, all_df)

    if progress_cb:
        progress_cb(
            f"MACRO RAW: concluído. Séries OK: {ok}/{total_series}. "
            f"Séries com falha/sem dados: {fail}. Linhas gravadas: {len(all_df)}."
        )


def run(engine: Engine, *, progress_cb: Optional[Callable[[str], None]] = None) -> None:
    ingest_macro_bcb_raw(engine, progress_cb=progress_cb)
