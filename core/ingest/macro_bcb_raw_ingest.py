# core/macro_bcb_raw_ingest.py
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


def _parse_valor(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.str.replace(".", "", regex=False)         # remove milhar PT-BR
    s = s.str.replace(",", ".", regex=False)        # vírgula -> ponto
    s = s.str.replace(r"[^0-9\.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")


def _fetch_sgs(codigo: int, *, audit: Optional[Callable[[str], None]] = None) -> pd.DataFrame:
    url = BCB_URL.format(codigo=codigo)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; macro-ingest/1.0)"}

    if audit:
        audit(f"[RAW] GET {url}")

    r = requests.get(url, timeout=60, headers=headers)

    if audit:
        audit(f"[RAW] HTTP {r.status_code} | bytes={len(r.content)}")

    r.raise_for_status()

    data = r.json()
    if audit:
        audit(f"[RAW] JSON items={len(data) if isinstance(data, list) else 'N/A'}")

    df = pd.DataFrame(data)
    if df.empty:
        return df

    df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce").dt.date
    df["valor"] = _parse_valor(df["valor"])

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


def _count_series(engine: Engine, series_name: str) -> tuple[int, int]:
    q = text(
        f"""
        select count(*) as n, count(valor) as n_valor
        from {RAW_FULL}
        where series_name = :s
        """
    )
    with engine.begin() as conn:
        row = conn.execute(q, {"s": series_name}).mappings().first()
    return int(row["n"]), int(row["n_valor"])


def ingest_macro_bcb_raw(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
    audit_cb: Optional[Callable[[str], None]] = None,
) -> None:
    _ensure_raw_table(engine)

    total_series = len(BCB_SERIES_CATALOG)
    if audit_cb:
        audit_cb(f"[RAW] Tabela garantida: {RAW_FULL}")
        audit_cb(f"[RAW] Séries no catálogo: {total_series}")

    frames: list[pd.DataFrame] = []

    for idx, (series_name, meta) in enumerate(BCB_SERIES_CATALOG.items(), start=1):
        codigo = int(meta["sgs"])
        freq = meta.get("freq")

        if progress_cb:
            progress_cb(f"RAW ({idx}/{total_series}) {series_name} ...")

        if audit_cb:
            audit_cb(f"\n[RAW] ===== {series_name} | SGS={codigo} | freq={freq} =====")

        try:
            df = _fetch_sgs(codigo, audit=audit_cb)

            if df.empty:
                if audit_cb:
                    audit_cb(f"[RAW] {series_name}: df vazio (0 linhas).")
                continue

            valid = int(df["valor"].notna().sum())
            if audit_cb:
                audit_cb(
                    f"[RAW] {series_name}: linhas={len(df)} | validos={valid} | "
                    f"min_data={df['data'].min()} | max_data={df['data'].max()}"
                )
                audit_cb(f"[RAW] {series_name}: head=\n{df.head(5).to_string(index=False)}")

            if valid == 0:
                if audit_cb:
                    audit_cb(f"[RAW] {series_name}: TODOS os valores viraram NULL após parse. (descartado)")
                continue

            df["series_name"] = series_name
            frames.append(df)

        except Exception as e:
            if audit_cb:
                audit_cb(f"[RAW] ERRO {series_name}: {repr(e)}")

    if not frames:
        raise RuntimeError(
            "RAW: nenhuma série foi ingerida com valores válidos. "
            "A auditoria acima deve mostrar se a API retornou dados ou se o parse/HTTP falhou."
        )

    all_df = pd.concat(frames, ignore_index=True)

    if audit_cb:
        audit_cb(f"\n[RAW] CONCAT total linhas={len(all_df)} | series_unicas={all_df['series_name'].nunique()}")

    _upsert_raw(engine, all_df)

    if audit_cb:
        audit_cb("[RAW] UPSERT concluído. Contagens no banco (por série):")
        for sname in BCB_SERIES_CATALOG.keys():
            n, n_valor = _count_series(engine, sname)
            audit_cb(f"[RAW] {sname}: n={n} | n_valor={n_valor}")


def run(engine: Engine, *, progress_cb: Optional[Callable[[str], None]] = None, audit_cb: Optional[Callable[[str], None]] = None) -> None:
    ingest_macro_bcb_raw(engine, progress_cb=progress_cb, audit_cb=audit_cb)
