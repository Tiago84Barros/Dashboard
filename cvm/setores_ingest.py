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


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Atenção: não transforme NaN em string "nan" aqui; mantenha como string só na lista final
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    # comparação sem acentos/variações simples
    cols_upper = {str(c).strip().upper(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().upper()
        if key in cols_upper:
            return cols_upper[key]
    return None


def _load_cvm_to_ticker(path: Path) -> pd.DataFrame:
    """
    Lê o csv de mapeamento e cria 'ticker_base' no padrão:
      - ticker_base = ticker sem dígito final (PETR4 -> PETR)

    Aceita variações no nome da coluna de ticker.
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
    df = df.drop_duplicates(subset=["ticker"])

    df["ticker_base"] = df["ticker"].str.replace(r"\d$", "", regex=True).astype(str).str.strip().str.upper()
    df = df.dropna(subset=["ticker_base"])
    df = df[df["ticker_base"] != ""]

    return df[["ticker", "ticker_base"]].drop_duplicates()


def _download_b3_excel_zip(timeout_sec: int = 60) -> bytes:
    r = requests.get(B3_SETOR_ZIP_URL, timeout=timeout_sec)
    if r.status_code != 200:
        raise RuntimeError(f"Falha ao baixar ClassifSetorial.zip. HTTP {r.status_code}")
    return r.content


def _extract_ticker_from_row(row: pd.Series) -> Optional[str]:
    """
    EXTRAÇÃO DEFINITIVA:
    varre todas as células da linha e retorna o primeiro valor que pareça ticker.
    Não depende de nome/posição de coluna (resistente a Unnamed/nan/layout da B3).
    """
    for v in row.values:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip().upper()
        if _TICKER_RE.match(s):
            return s
    return None


def _parse_b3_classificacao(zip_bytes: bytes) -> pd.DataFrame:
    """
    Parser robusto do Excel da B3:
    - Lê a planilha sem depender de cabeçalho fixo.
    - Extrai ticker por regex linha-a-linha (solução definitiva para o erro atual).
    - Tenta identificar colunas de Setor/Subsetor/Segmento/Emissor por nome quando existirem.
    """
    with ZipFile(io.BytesIO(zip_bytes)) as fold:
        name = fold.namelist()[0]
        with fold.open(name) as f:
            # header=None para não depender de uma linha específica como header
            raw = pd.read_excel(f, header=None)

    # Remove linhas totalmente vazias
    raw = raw.dropna(how="all")
    if raw.empty:
        raise ValueError("Planilha da B3 veio vazia após leitura.")

    # Encontra a linha de cabeçalho: primeira linha que contenha 'SETOR' ou 'SETOR ECONÔMICO'
    hdr_idx = None
    for i in range(min(len(raw), 60)):
        row_txt = " ".join([str(x).strip().upper() for x in raw.iloc[i].values if not pd.isna(x)])
        if "SETOR" in row_txt and ("SUBSETOR" in row_txt or "SEGMENTO" in row_txt or "EMISSOR" in row_txt):
            hdr_idx = i
            break

    # Se não achou, assume a mesma heurística antiga: pula 6 e pega a próxima como header
    if hdr_idx is None:
        hdr_idx = 6 if len(raw) > 7 else 0

    header = raw.iloc[hdr_idx].astype(str).str.strip()
    df = raw.iloc[hdr_idx + 1 :].copy()
    df.columns = header.tolist()

    df = _normalize_cols(df)

    # Padroniza nomes conhecidos (quando existirem)
    rename_map = {}
    for c in list(df.columns):
        cu = str(c).strip().upper()
        if cu in ("SETOR ECONÔMICO", "SETOR ECONOMICO"):
            rename_map[c] = "SETOR"
        elif cu == "EMISSOR":
            rename_map[c] = "EMISSOR"
        elif cu == "SUBSETOR":
            rename_map[c] = "SUBSETOR"
        elif cu == "SEGMENTO":
            rename_map[c] = "SEGMENTO"
        elif cu in ("CÓDIGO", "CODIGO", "CÓDIGO DE NEGOCIAÇÃO", "CODIGO DE NEGOCIACAO"):
            # Mesmo que exista, não vamos depender disso para ticker
            rename_map[c] = "CODIGO_TENTATIVO"

    if rename_map:
        df = df.rename(columns=rename_map)

    # GARANTE colunas de hierarquia (se não existirem, cria)
    for col in ["SETOR", "SUBSETOR", "SEGMENTO", "EMISSOR"]:
        if col not in df.columns:
            df[col] = None

    # Strip em texto
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()

    # Forward-fill de hierarquia
    df["SETOR"] = df["SETOR"].replace({"nan": None, "None": None}).ffill()
    df["SUBSETOR"] = df["SUBSETOR"].replace({"nan": None, "None": None}).ffill()
    df["SEGMENTO"] = df["SEGMENTO"].replace({"nan": None, "None": None}).ffill()

    # EXTRAÇÃO DEFINITIVA DO TICKER
    df["ticker_b3"] = df.apply(_extract_ticker_from_row, axis=1)
    df = df.dropna(subset=["ticker_b3"])
    df["ticker_b3"] = df["ticker_b3"].astype(str).str.strip().str.upper()

    # Nome da empresa: prioriza EMISSOR; se não tiver, tenta achar qualquer coluna com 'EMISSOR'/'NOME'
    nome_empresa = df.get("EMISSOR")
    if nome_empresa is None or nome_empresa.isna().all():
        cand_nome = None
        for c in df.columns:
            cu = str(c).upper()
            if "EMISSOR" in cu or cu in ("NOME", "NOME EMPRESA", "COMPANHIA"):
                cand_nome = c
                break
        if cand_nome is not None:
            df["nome_empresa"] = df[cand_nome].astype(str).str.strip()
        else:
            df["nome_empresa"] = None
    else:
        df["nome_empresa"] = df["EMISSOR"].astype(str).str.strip()

    # Seleciona colunas finais
    out = df[["ticker_b3", "nome_empresa", "SETOR", "SUBSETOR", "SEGMENTO"]].copy()

    # Limpeza final
    out = out.dropna(subset=["ticker_b3"])
    out = out.drop_duplicates(subset=["ticker_b3"])

    return out


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

    if progress_cb:
        progress_cb("SETORES: carregando cvm_to_ticker...")

    cvm_map = _load_cvm_to_ticker(map_path)

    # ticker_base do B3: remove dígito final
    b3["ticker_base"] = b3["ticker_b3"].str.replace(r"\d$", "", regex=True)

    merged = b3.merge(cvm_map, on="ticker_base", how="left")

    # Atualiza o ticker final com o ticker completo do mapa (se encontrado)
    merged["ticker"] = merged["ticker"].fillna(merged["ticker_b3"])
    merged["ticker"] = merged["ticker"].astype(str).str.strip().str.upper()

    merged = merged.dropna(subset=["ticker"])
    merged = merged.drop_duplicates(subset=["ticker"])

    # Garante colunas
    for col in ["SETOR", "SUBSETOR", "SEGMENTO"]:
        if col not in merged.columns:
            merged[col] = None

    out = merged[["ticker", "SETOR", "SUBSETOR", "SEGMENTO", "nome_empresa"]].copy()

    if progress_cb:
        progress_cb(f"SETORES: upsert de {len(out):,} linhas no Supabase...".replace(",", "."))

    _upsert(engine, out)

    if progress_cb:
        progress_cb("SETORES: concluído.")
