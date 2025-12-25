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

B3_SETOR_ZIP_URL = (
    "https://www.b3.com.br/data/files/57/E6/AA/A1/68C7781064456178AC094EA8/ClassifSetorial.zip"
)

# Regex de ticker B3 (mais comum). Ex: PETR4, VALE3, BBDC4, etc.
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


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _download_b3_excel_zip(timeout_sec: int = 60) -> bytes:
    r = requests.get(B3_SETOR_ZIP_URL, timeout=timeout_sec)
    if r.status_code != 200:
        raise RuntimeError(f"Falha ao baixar ClassifSetorial.zip. HTTP {r.status_code}")
    return r.content


def _find_header_row(raw: pd.DataFrame) -> Optional[int]:
    """
    Procura uma linha que pareça ser o cabeçalho real.
    A B3 frequentemente contém linhas "soltas" antes do header.
    """
    # Palavras-chave típicas do arquivo
    keys = ["SETOR", "SUBSETOR", "SEGMENTO", "EMISSOR", "LISTAGEM", "CÓDIGO", "CODIGO"]

    # varre as primeiras ~40 linhas para achar algo consistente
    max_scan = min(len(raw), 60)
    for i in range(max_scan):
        row = raw.iloc[i].astype(str).str.upper().tolist()
        hits = 0
        for cell in row:
            if any(k in cell for k in keys):
                hits += 1
        # se a linha tiver várias chaves, é um bom candidato
        if hits >= 2:
            return i
    return None


def _dedupe_columns(cols: list[str]) -> list[str]:
    """
    Evita colunas duplicadas (causa do erro clássico: df['CÓDIGO'] virar DataFrame).
    """
    seen = {}
    out = []
    for c in cols:
        base = c
        if base not in seen:
            seen[base] = 0
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}__{seen[base]}")
    return out


def _select_ticker_column(df: pd.DataFrame) -> Optional[str]:
    """
    Identifica a coluna que mais parece conter tickers, por conteúdo (regex).
    Funciona mesmo quando o nome vem como Unnamed: X.
    """
    best_col = None
    best_score = 0

    for c in df.columns:
        # precisa ser coluna "textual"
        s = df[c]
        # converte para string sem quebrar
        s2 = s.astype(str).str.strip().str.upper()

        # ignora colunas muito vazias
        non_empty = (s2 != "").sum()
        if non_empty == 0:
            continue

        score = s2.apply(lambda v: 1 if _TICKER_RE.match(v) else 0).sum()

        # dá preferência para a coluna com maior volume de tickers válidos
        if score > best_score:
            best_score = score
            best_col = c

    # exige pelo menos alguns matches para ser considerada
    if best_score >= 5:
        return best_col
    return None


def _parse_b3_classificacao(zip_bytes: bytes) -> pd.DataFrame:
    with ZipFile(io.BytesIO(zip_bytes)) as fold:
        name = fold.namelist()[0]
        with fold.open(name) as f:
            # Leitura crua: sem header
            raw = pd.read_excel(io=f, header=None)

    # encontra a linha do header real
    hdr = _find_header_row(raw)

    if hdr is None:
        # fallback: tenta o padrão antigo
        # (alguns arquivos ainda funcionam com skiprows=6)
        with ZipFile(io.BytesIO(zip_bytes)) as fold:
            name = fold.namelist()[0]
            with fold.open(name) as f:
                df = pd.read_excel(io=f, skiprows=6)
        df = _normalize_cols(df)
        df.columns = _dedupe_columns(list(df.columns))
    else:
        # recria df a partir do header encontrado
        header = raw.iloc[hdr].astype(str).str.strip()
        df = raw.iloc[hdr + 1 :].copy()
        df.columns = _dedupe_columns(header.tolist())
        df = _normalize_cols(df)

    # remove linhas totalmente vazias
    df = df.dropna(how="all")

    # normaliza nomes conhecidos (quando existirem)
    rename_map = {
        "SETOR ECONÔMICO": "SETOR",
        "SETOR ECONOMICO": "SETOR",
        "SUBSETOR ECONÔMICO": "SUBSETOR",
        "SUBSETOR ECONOMICO": "SUBSETOR",
        "EMISSOR": "EMISSOR",
        "SEGMENTO": "SEGMENTO",
        "LISTAGEM": "LISTAGEM",
        "CÓDIGO": "CÓDIGO",
        "CODIGO": "CÓDIGO",
        "CÓDIGO DE NEGOCIAÇÃO": "CÓDIGO",
        "CODIGO DE NEGOCIACAO": "CÓDIGO",
        "CÓDIGO_DE_NEGOCIAÇÃO": "CÓDIGO",
        "CODIGO_DE_NEGOCIACAO": "CÓDIGO",
        "CÓDIGO DE NEGOCIACAO": "CÓDIGO",
    }
    # aplica rename por match exato (caso o header venha “limpo”)
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})

    # garante que as colunas macro existam (ou cria vazias)
    for col in ["SETOR", "SUBSETOR", "SEGMENTO", "EMISSOR"]:
        if col not in df.columns:
            df[col] = None

    # resolve ticker: 1) tenta por nomes comuns; 2) tenta por conteúdo (regex)
    ticker_col = None
    for cand in ["CÓDIGO", "CODIGO", "LISTAGEM"]:
        if cand in df.columns:
            ticker_col = cand
            break
    if ticker_col is None:
        ticker_col = _select_ticker_column(df)

    if ticker_col is None:
        raise ValueError(
            f"Não encontrei coluna de ticker na B3. Colunas: {list(df.columns)}"
        )

    # prepara ticker_b3 como Series garantida
    ticker_s = df[ticker_col]
    if isinstance(ticker_s, pd.DataFrame):
        # acontece se houver duplicidade de colunas
        ticker_s = ticker_s.iloc[:, 0]

    df["ticker_b3"] = ticker_s.astype(str).str.strip().str.upper()

    # filtra só tickers válidos
    df = df[df["ticker_b3"].apply(lambda v: bool(_TICKER_RE.match(v)))].copy()

    # nome empresa: prefere EMISSOR, senão tenta achar alguma coluna "NOME"
    if "EMISSOR" in df.columns:
        nome = df["EMISSOR"]
    else:
        nome = pd.Series([None] * len(df), index=df.index)

    df["nome_empresa"] = nome.astype(str).str.strip()

    # fill-down de hierarquia (setor/subsetor/segmento costumam vir em bloco)
    df["SETOR"] = df["SETOR"].ffill()
    df["SUBSETOR"] = df["SUBSETOR"].ffill()
    df["SEGMENTO"] = df["SEGMENTO"].ffill()

    # seleciona saída
    out = df[["ticker_b3", "nome_empresa", "SETOR", "SUBSETOR", "SEGMENTO"]].copy()
    out["ticker_b3"] = out["ticker_b3"].astype(str).str.strip().str.upper()
    out = out.dropna(subset=["ticker_b3"]).drop_duplicates(subset=["ticker_b3"])

    return out


def _load_cvm_to_ticker(path: Path) -> pd.DataFrame:
    """
    Mapa CVM -> Ticker (para alinhar ticker oficial com ticker_base).
    Mantém a lógica estilo algoritmo_2: ticker_base = ticker sem o dígito final.
    """
    df = pd.read_csv(path, sep=",", encoding="utf-8")
    df.columns = [str(c).strip() for c in df.columns]

    # aceita variações
    cols = {c.lower(): c for c in df.columns}
    if "ticker" not in cols and "Ticker" not in df.columns:
        raise ValueError(f"{path.as_posix()} precisa ter coluna Ticker/ticker.")

    ticker_col = cols.get("ticker") or "Ticker"
    df = df.rename(columns={ticker_col: "ticker"})

    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df = df.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"])

    df["ticker_base"] = df["ticker"].str.replace(r"\d$", "", regex=True).str.strip().str.upper()
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

    # 1) baixa e parseia B3
    zip_bytes = _download_b3_excel_zip(timeout_sec=timeout_sec)
    b3 = _parse_b3_classificacao(zip_bytes)

    # 2) carrega map CVM->ticker (para ajustar ticker final, se desejar)
    cvm_map = _load_cvm_to_ticker(map_path)

    # 3) merge por ticker_base (ticker sem o dígito final)
    b3["ticker_base"] = b3["ticker_b3"].str.replace(r"\d$", "", regex=True).str.strip().str.upper()
    merged = b3.merge(cvm_map, on="ticker_base", how="left")

    # 4) ticker final: se o mapa tiver ticker completo, usa; senão usa o ticker_b3
    merged["ticker"] = merged["ticker"].fillna(merged["ticker_b3"])
    merged["ticker"] = merged["ticker"].astype(str).str.strip().str.upper()

    merged = merged.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"])

    out = merged[["ticker", "SETOR", "SUBSETOR", "SEGMENTO", "nome_empresa"]].copy()

    _upsert(engine, out)

    if progress_cb:
        progress_cb("SETORES: concluído.")
