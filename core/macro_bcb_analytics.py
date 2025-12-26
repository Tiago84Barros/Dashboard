# core/macro_bcb_analytics.py
from __future__ import annotations

from typing import Callable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

SCHEMA = "cvm"

RAW_FULL = f"{SCHEMA}.macro_bcb"
MONTHLY_TABLE = "info_economica_mensal"
MONTHLY_FULL = f"{SCHEMA}.{MONTHLY_TABLE}"


def _ensure_monthly_table(engine: Engine) -> None:
    ddl_schema = f"create schema if not exists {SCHEMA};"
    ddl_table = f"""
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
    # garante colunas caso a tabela já exista em outro formato
    alter_cols = [
        f"alter table {MONTHLY_FULL} add column if not exists selic double precision;",
        f"alter table {MONTHLY_FULL} add column if not exists ipca double precision;",
        f"alter table {MONTHLY_FULL} add column if not exists cambio double precision;",
        f"alter table {MONTHLY_FULL} add column if not exists icc double precision;",
        f"alter table {MONTHLY_FULL} add column if not exists pib double precision;",
        f"alter table {MONTHLY_FULL} add column if not exists balanca_comercial double precision;",
        f"alter table {MONTHLY_FULL} add column if not exists fetched_at timestamptz default now();",
    ]

    with engine.begin() as conn:
        conn.execute(text(ddl_schema))
        conn.execute(text(ddl_table))
        for stmt in alter_cols:
            conn.execute(text(stmt))


def _read_raw(engine: Engine) -> pd.DataFrame:
    sql = f"""
    select data, series_name, valor
    from {RAW_FULL}
    where valor is not null;
    """
    df = pd.read_sql(sql, engine)
    if df.empty:
        return df

    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df = df.dropna(subset=["data"])
    return df


def _to_month_end_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte a coluna data para DateTimeIndex e normaliza para fim de mês.
    """
    df = df.sort_values("data")
    df = df.set_index("data")
    return df


def _resample_monthly(series_df: pd.DataFrame, how: str) -> pd.Series:
    """
    series_df: dataframe com index datetime e coluna 'valor'
    how: 'last' | 'mean' | 'ffill'
    """
    if series_df.empty:
        return pd.Series(dtype="float64")

    s = series_df["valor"].astype("float64")
    if how == "mean":
        return s.resample("M").mean()
    if how == "last":
        return s.resample("M").last()
    if how == "ffill":
        return s.resample("M").ffill()
    raise ValueError(f"how inválido: {how}")


def build_info_economica_mensal(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    """
    Constrói cvm.info_economica_mensal a partir de cvm.macro_bcb (RAW),
    respeitando a frequência de cada série e aplicando regras matemáticas.
    """
    _ensure_monthly_table(engine)

    raw = _read_raw(engine)
    if raw.empty:
        raise RuntimeError(
            "ANALYTICS: cvm.macro_bcb (RAW) está vazio (ou só com NULL). "
            "Execute primeiro o macro_bcb_raw_ingest."
        )

    if progress_cb:
        progress_cb(f"ANALYTICS: RAW lido com {len(raw)} linhas (valor != NULL).")

    # mapeamento série -> (coluna destino, regra)
    # ajuste aqui se quiser mudar last/mean
    mapping = {
        "IPCA_MENSAL": ("ipca", "last"),
        "SELIC_META": ("selic", "last"),
        "SELIC_EFETIVA": ("selic", "last"),
        "CAMBIO_PTX": ("cambio", "mean"),
        "ICC": ("icc", "last"),
        "PIB": ("pib", "ffill"),
        "BALANCA_COMERCIAL": ("balanca_comercial", "last"),
    }

    # preferências: se existir SELIC_EFETIVA, ela “ganha” da SELIC_META
    selic_priority = ["SELIC_EFETIVA", "SELIC_META"]

    raw = raw[raw["series_name"].isin(mapping.keys())].copy()
    if raw.empty:
        raise RuntimeError("ANALYTICS: nenhuma série do mapping encontrada no RAW.")

    # prepara base mensal
    monthly_frames = {}

    for series_name, (col, how) in mapping.items():
        df_s = raw.loc[raw["series_name"] == series_name, ["data", "valor"]]
        if df_s.empty:
            if progress_cb:
                progress_cb(f"ANALYTICS: série ausente no RAW: {series_name}.")
            continue

        df_s = _to_month_end_index(df_s)
        s_month = _resample_monthly(df_s, how=how)
        monthly_frames[series_name] = s_month

        if progress_cb:
            progress_cb(f"ANALYTICS: {series_name} -> {col} ({how}) gerou {int(s_month.notna().sum())} meses.")

    # monta dataframe final por coluna
    out = pd.DataFrame(index=pd.DatetimeIndex([], freq=None))

    # IPCA
    if "IPCA_MENSAL" in monthly_frames:
        out["ipca"] = monthly_frames["IPCA_MENSAL"]

    # SELIC: escolhe a melhor disponível
    selic_series = None
    for sname in selic_priority:
        if sname in monthly_frames:
            selic_series = monthly_frames[sname]
            break
    if selic_series is not None:
        out["selic"] = selic_series

    # demais
    if "CAMBIO_PTX" in monthly_frames:
        out["cambio"] = monthly_frames["CAMBIO_PTX"]
    if "ICC" in monthly_frames:
        out["icc"] = monthly_frames["ICC"]
    if "PIB" in monthly_frames:
        out["pib"] = monthly_frames["PIB"]
    if "BALANCA_COMERCIAL" in monthly_frames:
        out["balanca_comercial"] = monthly_frames["BALANCA_COMERCIAL"]

    if out.empty:
        raise RuntimeError("ANALYTICS: não foi possível gerar nenhum mês para info_economica_mensal.")

    # alinha index e converte para date (fim do mês)
    out = out.sort_index()
    out.index = out.index.to_period("M").to_timestamp("M")
    out = out.reset_index().rename(columns={"index": "data"})
    out["data"] = out["data"].dt.date

    # UPSERT
    sql = f"""
    insert into {MONTHLY_FULL} (data, selic, ipca, cambio, icc, pib, balanca_comercial, fetched_at)
    values (:data, :selic, :ipca, :cambio, :icc, :pib, :balanca_comercial, now())
    on conflict (data) do update set
      selic = excluded.selic,
      ipca = excluded.ipca,
      cambio = excluded.cambio,
      icc = excluded.icc,
      pib = excluded.pib,
      balanca_comercial = excluded.balanca_comercial,
      fetched_at = now();
    """

    rows = out.to_dict("records")
    with engine.begin() as conn:
        conn.execute(text(sql), rows)

    if progress_cb:
        progress_cb(f"ANALYTICS: upsert concluído em {MONTHLY_FULL}. Linhas: {len(rows)}.")


def run(engine: Engine, *, progress_cb: Optional[Callable[[str], None]] = None) -> None:
    build_info_economica_mensal(engine, progress_cb=progress_cb)
