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


def _ensure_table(engine: Engine) -> None:
    # Você disse que já existe. Ainda assim, garantimos.
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


def _load_cvm_to_ticker(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=",", encoding="utf-8")
    cols = {c.lower(): c for c in df.columns}

    # aceitamos colunas típicas: CD_CVM, Ticker
    if "ticker" not in cols:
        raise ValueError("data/cvm_to_ticker.csv precisa ter coluna 'Ticker' (ou 'ticker').")

    df = df.rename(columns={cols["ticker"]: "ticker"})
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df = df.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"])

    # cria ticker_base (sem número final) para casar com o excel da B3 (que costuma vir sem o dígito)
    df["ticker_base"] = df["ticker"].str.replace(r"\d$", "", regex=True)
    df = df[["ticker", "ticker_base"]].drop_duplicates()
    return df


def _download_b3_excel_zip(timeout_sec: int = 60) -> bytes:
    r = requests.get(B3_SETOR_ZIP_URL, timeout=timeout_sec)
    if r.status_code != 200:
        raise RuntimeError(f"Falha ao baixar ClassifSetorial.zip. HTTP {r.status_code}")
    return r.content


def _parse_b3_classificacao(zip_bytes: bytes) -> pd.DataFrame:
    with ZipFile(io.BytesIO(zip_bytes)) as fold:
        name = fold.namelist()[0]
        with fold.open(name) as f:
            df = pd.read_excel(io=f, skiprows=6)

    # Normalizações equivalentes ao seu notebook
    df = df.rename(
        columns={
            "SETOR ECONÔMICO": "SETOR",
            "SEGMENTO": "NOME",
            "LISTAGEM": "CÓDIGO",
            "Unnamed: 4": "LISTAGEM",
        }
    )[1:-18]

    df.loc[(df["CÓDIGO"].isnull()), "SEGMENTO"] = df.loc[(df["CÓDIGO"].isnull()), "NOME"]
    df = df.dropna(how="all")
    df["LISTAGEM"] = df["LISTAGEM"].fillna("AUSENTE")

    # “herança” das células acima
    if "SUBSETOR" in df.columns:
        df["SUBSETOR"] = df["SUBSETOR"].ffill()
    if "SEGMENTO" in df.columns:
        df["SEGMENTO"] = df["SEGMENTO"].ffill()
    df["SETOR"] = df["SETOR"].ffill()

    # remove cabeçalhos repetidos e linhas inválidas
    df = df.loc[(df["CÓDIGO"] != "CÓDIGO") & (df["CÓDIGO"] != "LISTAGEM") & (~df["CÓDIGO"].isnull())]

    # strip geral
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # reordena / seleciona
    keep = ["CÓDIGO", "NOME", "SETOR", "SUBSETOR", "SEGMENTO", "LISTAGEM"]
    df = df[[c for c in keep if c in df.columns]].copy()

    df = df.rename(columns={"CÓDIGO": "ticker_b3", "NOME": "nome_empresa"})
    df["ticker_b3"] = df["ticker_b3"].astype(str).str.strip().str.upper()
    return df


def _upsert(engine: Engine, df: pd.DataFrame, batch: int = 5000) -> None:
    if df.empty:
        return

    sql = """
    insert into public.setores (ticker, "SETOR", "SUBSETOR", "SEGMENTO", nome_empresa)
    values (:ticker, :SETOR, :SUBSETOR, :SEGMENTO, :nome_empresa)
    on conflict (ticker) do update set
      "SETOR" = excluded."SETOR",
      "SUBSETOR" = excluded."SUBSETOR",
      "SEGMENTO" = excluded."SEGMENTO",
      nome_empresa = excluded.nome_empresa;
    """

    rows = df[["ticker", "SETOR", "SUBSETOR", "SEGMENTO", "nome_empresa"]].to_dict("records")
    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(sql), rows[i : i + batch])


def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
    timeout_sec: int = 60,
) -> None:
    _ensure_table(engine)

    settings = get_settings()
    map_path = Path(settings.cvm_to_ticker_path)
    if not map_path.exists():
        raise FileNotFoundError(f"Não encontrei {map_path}. Coloque o csv no GitHub em data/cvm_to_ticker.csv")

    if progress_cb:
        progress_cb("SETORES: baixando classificação setorial da B3...")

    zip_bytes = _download_b3_excel_zip(timeout_sec=timeout_sec)
    b3 = _parse_b3_classificacao(zip_bytes)

    if progress_cb:
        progress_cb("SETORES: carregando cvm_to_ticker do repositório...")

    cvm_map = _load_cvm_to_ticker(map_path)

    # “ticker_b3” costuma vir sem o dígito: casamos pelo ticker_base
    b3["ticker_base"] = b3["ticker_b3"].str.replace(r"\d$", "", regex=True)
    merged = b3.merge(cvm_map, on="ticker_base", how="left")

    # se não achou no mapa, fallback: tenta usar o próprio ticker_b3 como ticker final
    merged["ticker"] = merged["ticker"].fillna(merged["ticker_b3"])
    merged["ticker"] = merged["ticker"].astype(str).str.strip().str.upper()

    merged = merged.dropna(subset=["ticker"])
    merged = merged.drop_duplicates(subset=["ticker"])

    # garante colunas maiúsculas como no seu app
    for col in ["SETOR", "SUBSETOR", "SEGMENTO"]:
        if col not in merged.columns:
            merged[col] = None

    out = merged[["ticker", "SETOR", "SUBSETOR", "SEGMENTO", "nome_empresa"]].copy()

    if progress_cb:
        progress_cb(f"SETORES: upsert de {len(out):,} linhas no Supabase...".replace(",", "."))

    _upsert(engine, out)

    if progress_cb:
        progress_cb("SETORES: concluído.")
