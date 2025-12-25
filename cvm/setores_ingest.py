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
    # Normaliza colunas para facilitar detecção (remove espaços e padroniza underscore)
    df = df.copy()
    df.columns = [str(c).strip().replace(" ", "_").replace("-", "_") for c in df.columns]
    return df


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    cols_upper = {c.upper(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().upper().replace(" ", "_").replace("-", "_")
        if key in cols_upper:
            return cols_upper[key]
    return None


def _load_cvm_to_ticker(path: Path) -> pd.DataFrame:
    """
    Lê o csv de mapeamento e cria 'ticker_base' no padrão do Algoritmo 2:
      - df['Ticker_base'] = df['Ticker'].str[:-1]  (na prática, remove o dígito final do ticker)

    Aqui fazemos uma versão robusta:
      - aceita coluna Ticker/ticker/TICKER
      - não exige CD_CVM
      - ticker_base remove apenas dígito final (ex.: PETR4 -> PETR)
    """
    df = pd.read_csv(path, sep=",", encoding="utf-8")
    df = _normalize_cols(df)

    ticker_col = _pick_col(df, ["Ticker", "ticker", "TICKER", "CODIGO", "COD_NEGOCIACAO", "SYMBOL"])
    if not ticker_col:
        raise ValueError(
            f"{path.as_posix()} precisa ter uma coluna de ticker (ex.: 'Ticker'). "
            f"Colunas encontradas: {list(df.columns)}"
        )

    df = df.rename(columns={ticker_col: "ticker"})
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df = df.dropna(subset=["ticker"])

    # Remover duplicados
    df = df.drop_duplicates(subset=["ticker"])

    # ticker_base no espírito do Algoritmo 2:
    # remove só o dígito final, se existir (PETR4 -> PETR)
    df["ticker_base"] = df["ticker"].str.replace(r"\d$", "", regex=True)

    # Garante consistência e remove bases vazias
    df["ticker_base"] = df["ticker_base"].astype(str).str.strip().str.upper()
    df = df.dropna(subset=["ticker_base"])
    df = df[df["ticker_base"] != ""]

    return df[["ticker", "ticker_base"]].drop_duplicates()


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

    # Normalizações equivalentes ao notebook (Algoritmo 2)
    df = df.rename(
        columns={
            "SETOR ECONÔMICO": "SETOR",
            "SEGMENTO": "NOME",
            "LISTAGEM": "CÓDIGO",
            "Unnamed: 4": "LISTAGEM",
        }
    )[1:-18]

    # Descobre qual coluna contém o "código/ticker" após o rename.
    # A B3 pode mudar cabeçalhos/posições; então não podemos assumir que "CÓDIGO" exista.
    codigo_col = None
    for cand in ["CÓDIGO", "CODIGO", "CÓDIGO DE NEGOCIAÇÃO", "CÓDIGO_DE_NEGOCIAÇÃO", "CÓDIGO_DE_NEGOCIACAO", "LISTAGEM"]:
        if cand in df.columns:
            codigo_col = cand
            break
    
    # Se ainda não achou, tenta identificar a coluna que parece ticker (ex.: PETR4, VALE3)
    if codigo_col is None:
        # escolhe a primeira coluna que tenha muitos valores do tipo [A-Z]{4}\d
        import re
        ticker_re = re.compile(r"^[A-Z]{4}\d{1,2}$")
        best = None
        best_score = -1
        for c in df.columns:
            if df[c].dtype != "object":
                continue
            s = df[c].astype(str).str.strip().str.upper()
            score = s.apply(lambda v: 1 if ticker_re.match(v) else 0).sum()
            if score > best_score:
                best_score = score
                best = c
        if best_score > 0:
            codigo_col = best
    
    if codigo_col is None:
        raise ValueError(f"Não encontrei coluna de código/ticker na planilha da B3. Colunas: {list(df.columns)}")
    
    # Garante nome canônico
    if codigo_col != "CÓDIGO":
        df = df.rename(columns={codigo_col: "CÓDIGO"})
    
    # Preenche SEGMENTO com NOME quando CÓDIGO está vazio
    df.loc[df["CÓDIGO"].isnull(), "SEGMENTO"] = df.loc[df["CÓDIGO"].isnull(), "NOME"]


    df = df.dropna(how="all")
    df["LISTAGEM"] = df["LISTAGEM"].fillna("AUSENTE")

    # ffill (herança das células acima)
    df["SETOR"] = df["SETOR"].ffill()
    if "SUBSETOR" in df.columns:
        df["SUBSETOR"] = df["SUBSETOR"].ffill()
    if "SEGMENTO" in df.columns:
        df["SEGMENTO"] = df["SEGMENTO"].ffill()

    # remove cabeçalhos repetidos e linhas inválidas
    df = df.loc[(df["CÓDIGO"] != "CÓDIGO") & (df["CÓDIGO"] != "LISTAGEM") & (~df["CÓDIGO"].isnull())]

    # strip geral
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # reordena/seleciona (quando existir)
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
        raise FileNotFoundError(f"Não encontrei {map_path}. Coloque o csv no GitHub (ex.: data/cvm_to_ticker.csv).")

    if progress_cb:
        progress_cb("SETORES: baixando classificação setorial da B3...")

    zip_bytes = _download_b3_excel_zip(timeout_sec=timeout_sec)
    b3 = _parse_b3_classificacao(zip_bytes)

    if progress_cb:
        progress_cb("SETORES: carregando cvm_to_ticker...")

    cvm_map = _load_cvm_to_ticker(map_path)

    # Igual ao Algoritmo 2: ticker_base do B3 é o ticker sem dígito final
    b3["ticker_base"] = b3["ticker_b3"].str.replace(r"\d$", "", regex=True)

    merged = b3.merge(cvm_map, on="ticker_base", how="left")

    # Atualiza o ticker final com o ticker completo do mapa (se encontrado)
    merged["ticker"] = merged["ticker"].fillna(merged["ticker_b3"])
    merged["ticker"] = merged["ticker"].astype(str).str.strip().str.upper()

    merged = merged.dropna(subset=["ticker"])
    merged = merged.drop_duplicates(subset=["ticker"])

    # garante colunas
    for col in ["SETOR", "SUBSETOR", "SEGMENTO"]:
        if col not in merged.columns:
            merged[col] = None

    out = merged[["ticker", "SETOR", "SUBSETOR", "SEGMENTO", "nome_empresa"]].copy()

    if progress_cb:
        progress_cb(f"SETORES: upsert de {len(out):,} linhas no Supabase...".replace(",", "."))

    _upsert(engine, out)

    if progress_cb:
        progress_cb("SETORES: concluído.")
