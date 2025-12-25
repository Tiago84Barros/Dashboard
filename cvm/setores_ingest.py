# cvm/setores_ingest.py
from __future__ import annotations

import io
import re
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


def _dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    # remove colunas duplicadas (pandas às vezes cria quando renomeamos)
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df


def _col_as_series(df: pd.DataFrame, col: str) -> pd.Series:
    obj = df[col]
    if isinstance(obj, pd.DataFrame):  # se virou DF por colunas duplicadas
        obj = obj.iloc[:, 0]
    return obj


def _find_header_row(raw: pd.DataFrame) -> int:
    """
    Encontra a linha do cabeçalho real procurando por 'SETOR ECONÔMICO' ou variações.
    raw é um DataFrame lido com header=None (cada linha vira uma linha).
    """
    targets = {"SETOR ECONÔMICO", "SETOR ECONOMICO", "SETOR"}
    for i in range(min(len(raw), 60)):  # busca nas primeiras linhas
        row = raw.iloc[i].astype(str).str.upper().str.strip()
        if any(t in set(row.values) for t in targets):
            return i
        # também aceita se "SETOR ECONÔMICO" aparecer como substring
        if any("SETOR ECON" in v for v in row.values if isinstance(v, str)):
            return i
    # se não achou, falha com contexto
    raise ValueError(f"Não consegui detectar o header da planilha B3. Primeiras colunas/linhas: {raw.head(15).to_dict()}")


def _read_b3_excel_from_zip(zip_bytes: bytes) -> pd.DataFrame:
    with ZipFile(io.BytesIO(zip_bytes)) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            # 1) lê cru para detectar header
            raw = pd.read_excel(f, header=None)

    header_row = _find_header_row(raw)

    # 2) relê agora com header correto
    with ZipFile(io.BytesIO(zip_bytes)) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            df = pd.read_excel(f, header=header_row)

    return df


def _parse_b3_classificacao(zip_bytes: bytes) -> pd.DataFrame:
    df = _read_b3_excel_from_zip(zip_bytes)
    df = _dedupe_columns(df)

    # padroniza colunas esperadas
    # OBS: dependendo do arquivo, "LISTAGEM" pode estar com nome diferente; deixamos robusto.
    rename_map = {}
    for c in df.columns:
        cu = str(c).strip().upper()
        if cu == "SETOR ECONÔMICO" or cu == "SETOR ECONOMICO":
            rename_map[c] = "SETOR"
        elif cu == "SUBSETOR":
            rename_map[c] = "SUBSETOR"
        elif cu == "SEGMENTO":
            rename_map[c] = "SEGMENTO"
        elif cu in ("SEGMENTO DE LISTAGEM", "LISTAGEM"):
            rename_map[c] = "LISTAGEM"
        elif cu in ("CÓDIGO", "CODIGO", "CÓDIGO DE NEGOCIAÇÃO", "CODIGO DE NEGOCIACAO"):
            rename_map[c] = "CÓDIGO"
        elif cu in ("NOME", "EMPRESA", "DENOMINAÇÃO SOCIAL", "DENOMINACAO SOCIAL"):
            rename_map[c] = "NOME"

    df = df.rename(columns=rename_map)
    df = _dedupe_columns(df)

    # se não veio CÓDIGO, tenta detectar coluna com tickers
    if "CÓDIGO" not in df.columns:
        ticker_re = re.compile(r"^[A-Z]{4}\d{1,2}$")
        best_col = None
        best_score = -1
        for c in df.columns:
            s = df[c]
            if s.dtype != "object":
                continue
            su = s.astype(str).str.strip().str.upper()
            score = su.apply(lambda v: 1 if ticker_re.match(v) else 0).sum()
            if score > best_score:
                best_score = score
                best_col = c
        if best_score > 0 and best_col is not None:
            df = df.rename(columns={best_col: "CÓDIGO"})
        else:
            raise ValueError(f"Não encontrei coluna de ticker na B3. Colunas: {list(df.columns)}")

    # garante colunas mínimas
    for col in ["SETOR", "SUBSETOR", "SEGMENTO", "LISTAGEM", "NOME"]:
        if col not in df.columns:
            df[col] = None

    df = _dedupe_columns(df)

    # ffill hierárquico (B3 usa células mescladas)
    df["SETOR"] = _col_as_series(df, "SETOR").ffill()
    df["SUBSETOR"] = _col_as_series(df, "SUBSETOR").ffill()
    df["SEGMENTO"] = _col_as_series(df, "SEGMENTO").ffill()

    # limpa e remove ruídos
    codigo = _col_as_series(df, "CÓDIGO").astype(str).str.strip().str.upper()
    df["CÓDIGO"] = codigo

    # remove linhas que são cabeçalhos repetidos
    df = df[df["CÓDIGO"].notna()]
    df = df[df["CÓDIGO"] != "CÓDIGO"]
    df = df[df["CÓDIGO"] != "LISTAGEM"]

    # ticker e nome
    df["ticker_b3"] = _col_as_series(df, "CÓDIGO").astype(str).str.strip().str.upper()
    df["nome_empresa"] = _col_as_series(df, "NOME").astype(str).str.strip()

    out = df[["ticker_b3", "SETOR", "SUBSETOR", "SEGMENTO", "nome_empresa"]].copy()
    out = out.dropna(subset=["ticker_b3"])
    out = out.drop_duplicates(subset=["ticker_b3"])
    return out


def _load_cvm_to_ticker(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=",", encoding="utf-8")
    cols = {c.lower(): c for c in df.columns}
    if "ticker" not in cols:
        raise ValueError("cvm_to_ticker.csv precisa ter coluna Ticker/ticker.")

    df = df.rename(columns={cols["ticker"]: "ticker"})
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df = df.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"])
    df["ticker_base"] = df["ticker"].str.replace(r"\d$", "", regex=True)
    return df[["ticker", "ticker_base"]].drop_duplicates()


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
            conn.execute(text(sql), rows[i:i + batch])


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
        raise FileNotFoundError(f"Não encontrei {map_path}. Coloque o csv no repositório (ex.: data/cvm_to_ticker.csv).")

    if progress_cb:
        progress_cb("SETORES: baixando classificação setorial da B3...")

    zip_bytes = _download_b3_excel_zip(timeout_sec=timeout_sec)
    b3 = _parse_b3_classificacao(zip_bytes)

    if progress_cb:
        progress_cb("SETORES: carregando cvm_to_ticker...")

    cvm_map = _load_cvm_to_ticker(map_path)

    # ticker_base (B3) = ticker sem o dígito final
    b3["ticker_base"] = b3["ticker_b3"].str.replace(r"\d$", "", regex=True)

    merged = b3.merge(cvm_map, on="ticker_base", how="left")
    merged["ticker"] = merged["ticker"].fillna(merged["ticker_b3"])
    merged["ticker"] = merged["ticker"].astype(str).str.strip().str.upper()

    merged = merged.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"])

    if progress_cb:
        progress_cb(f"SETORES: upsert de {len(merged):,} linhas...".replace(",", "."))

    _upsert(engine, merged[["ticker", "SETOR", "SUBSETOR", "SEGMENTO", "nome_empresa"]])

    if progress_cb:
        progress_cb("SETORES: concluído.")
