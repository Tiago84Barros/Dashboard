# cvm/setores_ingest.py
from __future__ import annotations

import io
from pathlib import Path
from zipfile import ZipFile
from typing import Callable, Optional

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.config.settings import get_settings

B3_SETOR_ZIP_URL = "https://www.b3.com.br/data/files/57/E6/AA/A1/68C7781064456178AC094EA8/ClassifSetorial.zip"


# =========================
# Helpers CRÍTICOS
# =========================
def _dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df


def _col_as_series(df: pd.DataFrame, col: str) -> pd.Series:
    obj = df[col]
    if isinstance(obj, pd.DataFrame):
        obj = obj.iloc[:, 0]
    return obj


# =========================
def _ensure_table(engine: Engine) -> None:
    ddl = """
    create table if not exists public.setores (
        ticker text primary key,
        "SETOR" text,
        "SUBSETOR" text,
        "SEGMENTO" text,
        nome_empresa text,
        created_at timestamptz not null default now()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _download_b3_excel_zip(timeout_sec: int = 60) -> bytes:
    r = requests.get(B3_SETOR_ZIP_URL, timeout=timeout_sec)
    if r.status_code != 200:
        raise RuntimeError(f"Falha ao baixar ClassifSetorial.zip. HTTP {r.status_code}")
    return r.content


def _parse_b3_classificacao(zip_bytes: bytes) -> pd.DataFrame:
    with ZipFile(io.BytesIO(zip_bytes)) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            df = pd.read_excel(f, skiprows=6)

    df = _dedupe_columns(df)

    df = df.rename(
        columns={
            "SETOR ECONÔMICO": "SETOR",
            "SEGMENTO": "NOME",
            "LISTAGEM": "CÓDIGO",
            "Unnamed: 4": "LISTAGEM",
        }
    )

    df = _dedupe_columns(df)
    df = df.iloc[1:-18]

    # Detecta coluna de ticker
    codigo_col = None
    for c in df.columns:
        s = df[c]
        if s.dtype == "object":
            hits = s.astype(str).str.match(r"^[A-Z]{4}\d{1,2}$").sum()
            if hits > 5:
                codigo_col = c
                break

    if codigo_col is None:
        raise ValueError(f"Não encontrei coluna de ticker na B3. Colunas: {list(df.columns)}")

    if codigo_col != "CÓDIGO":
        df = df.rename(columns={codigo_col: "CÓDIGO"})

    df = _dedupe_columns(df)

    codigo = _col_as_series(df, "CÓDIGO")
    nome = _col_as_series(df, "NOME")

    df.loc[codigo.isna(), "SEGMENTO"] = nome[codigo.isna()]

    df["LISTAGEM"] = _col_as_series(df, "LISTAGEM").fillna("AUSENTE")

    df["SETOR"] = _col_as_series(df, "SETOR").ffill()
    if "SUBSETOR" in df.columns:
        df["SUBSETOR"] = _col_as_series(df, "SUBSETOR").ffill()
    df["SEGMENTO"] = _col_as_series(df, "SEGMENTO").ffill()

    df = df[df["CÓDIGO"].notna()]
    df = df[df["CÓDIGO"] != "CÓDIGO"]

    df["ticker_b3"] = _col_as_series(df, "CÓDIGO").astype(str).str.strip().str.upper()
    df["nome_empresa"] = _col_as_series(df, "NOME").astype(str).str.strip()

    return df[["ticker_b3", "SETOR", "SUBSETOR", "SEGMENTO", "nome_empresa"]]


def _load_cvm_to_ticker(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _dedupe_columns(df)

    cols = {c.lower(): c for c in df.columns}
    df = df.rename(columns={cols["ticker"]: "ticker"})
    df["ticker"] = _col_as_series(df, "ticker").astype(str).str.strip().str.upper()
    df["ticker_base"] = df["ticker"].str.replace(r"\d$", "", regex=True)

    return df[["ticker", "ticker_base"]].drop_duplicates()


def _upsert(engine: Engine, df: pd.DataFrame) -> None:
    sql = """
    insert into public.setores (ticker, "SETOR", "SUBSETOR", "SEGMENTO", nome_empresa)
    values (:ticker, :SETOR, :SUBSETOR, :SEGMENTO, :nome_empresa)
    on conflict (ticker) do update set
      "SETOR" = excluded."SETOR",
      "SUBSETOR" = excluded."SUBSETOR",
      "SEGMENTO" = excluded."SEGMENTO",
      nome_empresa = excluded.nome_empresa;
    """

    with engine.begin() as conn:
        conn.execute(text(sql), df.to_dict("records"))


def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
    timeout_sec: int = 60,
) -> None:
    _ensure_table(engine)

    settings = get_settings()
    map_path = Path(settings.cvm_to_ticker_path)

    if progress_cb:
        progress_cb("SETORES: baixando classificação da B3...")

    zip_bytes = _download_b3_excel_zip(timeout_sec)
    b3 = _parse_b3_classificacao(zip_bytes)

    cvm_map = _load_cvm_to_ticker(map_path)

    b3["ticker_base"] = b3["ticker_b3"].str.replace(r"\d$", "", regex=True)
    merged = b3.merge(cvm_map, on="ticker_base", how="left")

    merged["ticker"] = merged["ticker"].fillna(merged["ticker_b3"])
    merged = merged.drop_duplicates(subset=["ticker"])

    _upsert(engine, merged[["ticker", "SETOR", "SUBSETOR", "SEGMENTO", "nome_empresa"]])

    if progress_cb:
        progress_cb("SETORES: concluído.")
