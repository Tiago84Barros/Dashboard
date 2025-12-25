# cvm/setores_ingest.py
from __future__ import annotations

import io
import re
from pathlib import Path
from typing import Callable, Optional
from zipfile import ZipFile

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


def _pick_col_loose(columns: list[str], candidates: list[str]) -> Optional[str]:
    """
    Procura coluna por aproximação, ignorando espaços/underscore/hífen e case.
    """
    def norm(x: str) -> str:
        return (
            str(x).strip().upper()
            .replace(" ", "")
            .replace("_", "")
            .replace("-", "")
            .replace("\n", "")
        )

    norm_map = {norm(c): c for c in columns}
    for cand in candidates:
        k = norm(cand)
        if k in norm_map:
            return norm_map[k]
    return None


def _download_b3_excel_zip(timeout_sec: int = 60) -> bytes:
    r = requests.get(B3_SETOR_ZIP_URL, timeout=timeout_sec)
    if r.status_code != 200:
        raise RuntimeError(f"Falha ao baixar ClassifSetorial.zip. HTTP {r.status_code}")
    return r.content


def _read_excel_from_b3_zip(zip_bytes: bytes) -> pd.DataFrame:
    """
    Lê o arquivo excel dentro do zip, retornando a planilha crua (sem assumir skiprows fixo).
    """
    with ZipFile(io.BytesIO(zip_bytes)) as fold:
        names = fold.namelist()
        if not names:
            raise ValueError("ZIP da B3 veio vazio.")
        name = names[0]
        with fold.open(name) as f:
            # header=None => não assume que a linha 0 é cabeçalho
            raw = pd.read_excel(f, header=None)
    return raw


def _find_header_row(raw: pd.DataFrame) -> int:
    """
    Procura a linha onde aparece algo como 'SETOR ECONÔMICO' (ou variações).
    """
    targets = {
        "SETOR ECONÔMICO",
        "SETOR ECONOMICO",
        "SETOR",
        "SUBSETOR",
        "SEGMENTO",
        "LISTAGEM",
        "CÓDIGO",
        "CODIGO",
    }

    # varre as primeiras ~80 linhas (suficiente para o arquivo da B3)
    max_scan = min(len(raw), 120)
    for i in range(max_scan):
        row = raw.iloc[i].astype(str).str.strip().str.upper().tolist()
        row_set = set([x for x in row if x and x != "NAN"])
        # se a linha tiver pelo menos 2 termos relevantes, é candidata forte a cabeçalho
        if len(row_set.intersection(targets)) >= 2:
            return i

    # fallback: se não achou, assume 0
    return 0


def _build_df_from_raw(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói DataFrame com cabeçalho correto e linhas de dados abaixo dele.
    """
    header_row = _find_header_row(raw)
    header = raw.iloc[header_row].astype(str).str.strip().tolist()
    df = raw.iloc[header_row + 1 :].copy()
    df.columns = header
    df = df.dropna(how="all")
    return df


def _best_ticker_like_col(df: pd.DataFrame) -> Optional[str]:
    """
    Fallback: encontra coluna que parece conter tickers B3 (PETR4, VALE3 etc.)
    """
    ticker_re = re.compile(r"^[A-Z]{4}\d{1,2}$")
    best = None
    best_score = 0
    for c in df.columns:
        s = df[c].astype(str).str.strip().str.upper()
        score = s.map(lambda v: 1 if ticker_re.match(v) else 0).sum()
        if score > best_score:
            best_score = score
            best = c
    return best if best_score > 0 else None


def _parse_b3_classificacao(zip_bytes: bytes) -> pd.DataFrame:
    """
    Parser robusto do Excel B3, sem depender de skiprows fixo.
    Retorna colunas:
      - ticker_b3
      - nome_empresa
      - SETOR
      - SUBSETOR
      - SEGMENTO
    """
    raw = _read_excel_from_b3_zip(zip_bytes)
    df = _build_df_from_raw(raw)

    # limpa nomes das colunas
    df.columns = [str(c).strip() for c in df.columns]

    col_setor = _pick_col_loose(df.columns.tolist(), ["SETOR ECONÔMICO", "SETOR ECONOMICO", "SETOR"])
    col_subsetor = _pick_col_loose(df.columns.tolist(), ["SUBSETOR", "SUB SETOR"])
    col_segmento = _pick_col_loose(df.columns.tolist(), ["SEGMENTO", "SEGMENTO DE LISTAGEM", "SEGMENTO ECONÔMICO", "SEGMENTO ECONOMICO"])

    col_codigo = _pick_col_loose(df.columns.tolist(), ["CÓDIGO", "CODIGO", "CÓDIGO DE NEGOCIAÇÃO", "CODIGO DE NEGOCIACAO", "LISTAGEM", "TICKER"])
    if col_codigo is None:
        col_codigo = _best_ticker_like_col(df)

    if col_codigo is None:
        raise ValueError(f"Não encontrei coluna de ticker/código na planilha da B3. Colunas: {list(df.columns)}")

    # coluna de nome (empresa)
    col_nome = _pick_col_loose(df.columns.tolist(), ["NOME", "EMPRESA", "COMPANHIA", "EMISSOR", "RAZÃO SOCIAL", "RAZAO SOCIAL"])
    if col_nome is None:
        # fallback: usa SEGMENTO se existir; senão deixa vazio
        col_nome = col_segmento

    out = pd.DataFrame(
        {
            "ticker_b3": df[col_codigo].astype(str).str.strip().str.upper(),
            "nome_empresa": df[col_nome].astype(str).str.strip() if col_nome else "",
            "SETOR": df[col_setor].astype(str).str.strip() if col_setor else None,
            "SUBSETOR": df[col_subsetor].astype(str).str.strip() if col_subsetor else None,
            "SEGMENTO": df[col_segmento].astype(str).str.strip() if col_segmento else None,
        }
    )

    # mantém apenas tickers válidos
    out = out.dropna(subset=["ticker_b3"])
    out = out[out["ticker_b3"].str.match(r"^[A-Z]{4}\d{1,2}$", na=False)]

    # forward-fill hierárquico
    for c in ["SETOR", "SUBSETOR", "SEGMENTO"]:
        if c in out.columns:
            out[c] = out[c].replace({"nan": None, "None": None, "": None}).ffill()

    # se SEGMENTO vazio, usa nome_empresa
    out.loc[out["SEGMENTO"].isnull(), "SEGMENTO"] = out.loc[out["SEGMENTO"].isnull(), "nome_empresa"]

    out = out.drop_duplicates(subset=["ticker_b3"], keep="last").reset_index(drop=True)
    return out


def _load_cvm_to_ticker(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=",", encoding="utf-8")
    df.columns = [str(c).strip() for c in df.columns]

    ticker_col = _pick_col_loose(df.columns.tolist(), ["Ticker", "ticker", "TICKER", "CODIGO", "COD_NEGOCIACAO", "SYMBOL"])
    if not ticker_col:
        raise ValueError(
            f"{path.as_posix()} precisa ter uma coluna de ticker (ex.: 'Ticker'). "
            f"Colunas encontradas: {list(df.columns)}"
        )

    df = df.rename(columns={ticker_col: "ticker"})
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df = df.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"])

    df["ticker_base"] = df["ticker"].str.replace(r"\d$", "", regex=True)
    df["ticker_base"] = df["ticker_base"].astype(str).str.strip().str.upper()
    df = df.dropna(subset=["ticker_base"])
    df = df[df["ticker_base"] != ""]

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
        raise FileNotFoundError(f"Não encontrei {map_path}. Coloque o csv no GitHub (ex.: data/cvm_to_ticker.csv).")

    if progress_cb:
        progress_cb("SETORES: baixando classificação setorial da B3...")

    zip_bytes = _download_b3_excel_zip(timeout_sec=timeout_sec)
    b3 = _parse_b3_classificacao(zip_bytes)

    if progress_cb:
        progress_cb(f"SETORES: B3 ok ({len(b3)} linhas). Carregando cvm_to_ticker...")

    cvm_map = _load_cvm_to_ticker(map_path)

    # ticker_base do B3 é o ticker sem dígito final
    b3["ticker_base"] = b3["ticker_b3"].str.replace(r"\d$", "", regex=True)

    merged = b3.merge(cvm_map, on="ticker_base", how="left")

    merged["ticker"] = merged["ticker"].fillna(merged["ticker_b3"])
    merged["ticker"] = merged["ticker"].astype(str).str.strip().str.upper()

    merged = merged.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"])

    for col in ["SETOR", "SUBSETOR", "SEGMENTO"]:
        if col not in merged.columns:
            merged[col] = None

    out = merged[["ticker", "SETOR", "SUBSETOR", "SEGMENTO", "nome_empresa"]].copy()

    if progress_cb:
        progress_cb(f"SETORES: upsert de {len(out):,} linhas no Supabase...".replace(",", "."))

    _upsert(engine, out)

    if progress_cb:
        progress_cb("SETORES: concluído.")
