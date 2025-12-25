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

# Ticker B3 típico: PETR4, VALE3, BBDC4, etc.
_TICKER_RE = re.compile(r"^[A-Z]{4}\d{1,2}$")


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


def _make_unique_cols(cols: list[str]) -> list[str]:
    """
    Garante nomes únicos para colunas duplicadas.
    Ex.: ['nan','nan','SETOR'] -> ['nan','nan__2','SETOR']
    """
    seen: dict[str, int] = {}
    out: list[str] = []
    for c in cols:
        base = ("" if c is None else str(c)).strip()
        if base == "":
            base = "COL"
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}__{seen[base]}")
    return out


def _normalize_cols(cols: list[object]) -> list[str]:
    """
    Normaliza cabeçalho: strip + remove quebras + colapsa espaços.
    Mantém "nan" como texto se vier assim, mas depois será único via _make_unique_cols.
    """
    out = []
    for c in cols:
        s = "" if c is None else str(c)
        s = s.replace("\n", " ").replace("\r", " ").strip()
        s = re.sub(r"\s+", " ", s)
        out.append(s)
    return out


def _download_b3_excel_zip(timeout_sec: int = 60) -> bytes:
    r = requests.get(B3_SETOR_ZIP_URL, timeout=timeout_sec)
    if r.status_code != 200:
        raise RuntimeError(f"Falha ao baixar ClassifSetorial.zip. HTTP {r.status_code}")
    return r.content


def _extract_ticker_from_row(row: pd.Series) -> Optional[str]:
    """
    Varre todas as células da linha e retorna o primeiro valor que pareça ticker.
    Resistente a mudanças de layout e a colunas 'nan/Unnamed'.
    """
    for v in row.values:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip().upper()
        if _TICKER_RE.match(s):
            return s
    return None


def _find_header_row(raw: pd.DataFrame) -> int:
    """
    Acha a linha de cabeçalho procurando palavras típicas.
    """
    keys = ["SETOR", "SUBSETOR", "SEGMENTO", "EMISSOR", "CÓDIGO", "CODIGO", "LISTAGEM"]
    limit = min(len(raw), 80)
    for i in range(limit):
        cells = [str(x).strip().upper() for x in raw.iloc[i].values if not pd.isna(x)]
        joined = " ".join(cells)
        if any(k in joined for k in keys) and ("SETOR" in joined):
            return i
    return -1


def _parse_b3_classificacao(zip_bytes: bytes) -> pd.DataFrame:
    """
    Parser robusto do Excel da B3:
    - Lê a planilha sem depender de cabeçalho fixo.
    - Detecta header por heurística.
    - Torna nomes de colunas únicos (corrige erro do dtype).
    - Extrai ticker por regex linha-a-linha.
    """
    with ZipFile(io.BytesIO(zip_bytes)) as fold:
        name = fold.namelist()[0]
        with fold.open(name) as f:
            raw = pd.read_excel(f, header=None)

    raw = raw.dropna(how="all")
    if raw.empty:
        raise ValueError("Planilha da B3 veio vazia após leitura.")

    hdr_idx = _find_header_row(raw)
    if hdr_idx < 0:
        # fallback conservador
        hdr_idx = 6 if len(raw) > 7 else 0

    header = _normalize_cols(list(raw.iloc[hdr_idx].values))
    header = _make_unique_cols(header)

    df = raw.iloc[hdr_idx + 1 :].copy()
    df.columns = header
    df = df.dropna(how="all").copy()

    # Renomeia colunas conhecidas se existirem
    rename_map = {}
    for c in df.columns:
        cu = str(c).strip().upper()
        if cu in ("SETOR ECONÔMICO", "SETOR ECONOMICO"):
            rename_map[c] = "SETOR"
        elif cu == "SETOR":
            rename_map[c] = "SETOR"
        elif cu == "SUBSETOR":
            rename_map[c] = "SUBSETOR"
        elif cu == "SEGMENTO":
            rename_map[c] = "SEGMENTO"
        elif cu == "EMISSOR":
            rename_map[c] = "EMISSOR"
    if rename_map:
        df = df.rename(columns=rename_map)

    # Garante colunas essenciais (se não existirem)
    for col in ["SETOR", "SUBSETOR", "SEGMENTO", "EMISSOR"]:
        if col not in df.columns:
            df[col] = None

    # Limpa apenas colunas "texto" de forma segura (sem depender de df[col].dtype)
    # (Isso evita exatamente o erro atual quando há colunas duplicadas.)
    obj_cols = [c for c in df.columns if pd.api.types.is_object_dtype(df[c]) or pd.api.types.is_string_dtype(df[c])]
    for c in obj_cols:
        df[c] = df[c].astype(str).str.strip()

    # Forward-fill hierárquico (SETOR/SUBSETOR/SEGMENTO)
    for col in ["SETOR", "SUBSETOR", "SEGMENTO"]:
        if col in df.columns:
            df[col] = df[col].replace({"nan": None, "None": None, "": None}).ffill()

    # EXTRAÇÃO DEFINITIVA DO TICKER
    df["ticker_b3"] = df.apply(_extract_ticker_from_row, axis=1)
    df = df.dropna(subset=["ticker_b3"]).copy()
    df["ticker_b3"] = df["ticker_b3"].astype(str).str.strip().str.upper()

    # Nome da empresa
    if "EMISSOR" in df.columns and not df["EMISSOR"].isna().all():
        df["nome_empresa"] = df["EMISSOR"].astype(str).str.strip()
    else:
        df["nome_empresa"] = None

    out = df[["ticker_b3", "nome_empresa", "SETOR", "SUBSETOR", "SEGMENTO"]].copy()
    out = out.drop_duplicates(subset=["ticker_b3"]).copy()

    # último filtro: garante que ticker é válido
    out = out[out["ticker_b3"].apply(lambda v: bool(_TICKER_RE.match(str(v))))].copy()

    return out


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    cols_upper = {str(c).strip().upper(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().upper()
        if key in cols_upper:
            return cols_upper[key]
    return None


def _load_cvm_to_ticker(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=",", encoding="utf-8")
    df.columns = [str(c).strip() for c in df.columns]

    ticker_col = _pick_col(df, ["Ticker", "ticker", "TICKER", "CODIGO", "COD_NEGOCIACAO", "SYMBOL"])
    if not ticker_col:
        raise ValueError(
            f"{path.as_posix()} precisa ter uma coluna de ticker (ex.: 'Ticker'). "
            f"Colunas encontradas: {list(df.columns)}"
        )

    df = df.rename(columns={ticker_col: "ticker"})
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df = df.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"]).copy()

    df["ticker_base"] = df["ticker"].str.replace(r"\d$", "", regex=True).astype(str).str.strip().str.upper()
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
        raise FileNotFoundError(
            f"Não encontrei {map_path}. Coloque o csv no GitHub (ex.: data/cvm_to_ticker.csv)."
        )

    if progress_cb:
        progress_cb("SETORES: baixando classificação setorial da B3...")

    zip_bytes = _download_b3_excel_zip(timeout_sec=timeout_sec)
    b3 = _parse_b3_classificacao(zip_bytes)

    if b3.empty:
        raise ValueError("SETORES: parser não encontrou nenhum ticker válido na planilha da B3 (0 linhas).")

    if progress_cb:
        progress_cb("SETORES: carregando cvm_to_ticker...")

    cvm_map = _load_cvm_to_ticker(map_path)

    b3["ticker_base"] = b3["ticker_b3"].str.replace(r"\d$", "", regex=True)

    merged = b3.merge(cvm_map, on="ticker_base", how="left")

    merged["ticker"] = merged["ticker"].fillna(merged["ticker_b3"])
    merged["ticker"] = merged["ticker"].astype(str).str.strip().str.upper()

    merged = merged.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"]).copy()

    out = merged[["ticker", "SETOR", "SUBSETOR", "SEGMENTO", "nome_empresa"]].copy()

    if progress_cb:
        progress_cb(f"SETORES: upsert de {len(out):,} linhas no Supabase...".replace(",", "."))

    _upsert(engine, out)

    if progress_cb:
        progress_cb("SETORES: concluído.")
