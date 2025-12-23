from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


B3_URL = "https://www.b3.com.br/data/files/57/E6/AA/A1/68C7781064456178AC094EA8/ClassifSetorial.zip"


@dataclass(frozen=True)
class IngestConfig:
    supabase_db_url: str
    schema: str = "cvm"
    table: str = "setores"
    csv_relpath: str = "data/cvm_to_ticker.csv"


def _get_engine(db_url: str) -> Engine:
    # db_url esperado no formato:
    # postgresql+psycopg2://user:pass@host:5432/postgres?sslmode=require
    return create_engine(db_url, pool_pre_ping=True)


def _repo_root() -> Path:
    # Ajuste: se este arquivo estiver em cvm/, o root é 1 nível acima
    return Path(__file__).resolve().parents[1]


def _load_cvm_to_ticker_from_repo(csv_relpath: str) -> pd.DataFrame:
    path = _repo_root() / csv_relpath
    if not path.exists():
        raise FileNotFoundError(f"Não encontrei o arquivo {csv_relpath} no repo. Caminho esperado: {path}")

    df = pd.read_csv(path)

    # Esperado: coluna "Ticker" (como no seu notebook).
    # Se seu CSV tiver outro nome (ex: "ticker"), ajuste aqui.
    if "Ticker" not in df.columns:
        raise ValueError(f"CSV {path} precisa ter coluna 'Ticker'. Colunas encontradas: {list(df.columns)}")

    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    df["Ticker_base"] = df["Ticker"].str[:-1]  # remove o dígito final (ex: PETR4 -> PETR)
    df = df.rename(columns={"Ticker": "ticker"})
    return df[["Ticker_base", "ticker"]].dropna().drop_duplicates()


def _download_b3_classif_setorial() -> pd.DataFrame:
    resp = requests.get(B3_URL, timeout=60)
    resp.raise_for_status()

    with ZipFile(BytesIO(resp.content)) as z:
        xls_name = z.namelist()[0]
        with z.open(xls_name) as f:
            df = pd.read_excel(f, skiprows=6)

    return df


def _transform_b3_df(df: pd.DataFrame) -> pd.DataFrame:
    # Ajuste de colunas conforme seu notebook (mantendo padrão)
    # OBS: a planilha muda de tempos em tempos; por isso tratamos com tolerância
    rename_map = {
        "SETOR ECONÔMICO": "SETOR",
        "SEGMENTO": "NOME",
        "LISTAGEM": "CÓDIGO",
        "Unnamed: 4": "LISTAGEM",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # recorta linhas de rodapé/cabeçalho repetido (como no notebook)
    # se falhar, seguimos sem cortar para não quebrar
    if len(df) > 30:
        df = df.iloc[1:-18].copy()

    # Garante as colunas mínimas
    needed = {"CÓDIGO", "NOME", "SETOR", "SUBSETOR", "SEGMENTO"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Planilha B3 não veio com colunas esperadas. Faltando: {missing}. Colunas: {list(df.columns)}")

    # Regras do notebook
    df.loc[df["CÓDIGO"].isna(), "SEGMENTO"] = df.loc[df["CÓDIGO"].isna(), "NOME"]
    df = df.dropna(how="all")

    if "LISTAGEM" in df.columns:
        df["LISTAGEM"] = df["LISTAGEM"].fillna("AUSENTE")
    else:
        df["LISTAGEM"] = "AUSENTE"

    df["SETOR"] = df["SETOR"].ffill()
    df["SUBSETOR"] = df["SUBSETOR"].ffill()
    df["SEGMENTO"] = df["SEGMENTO"].ffill()

    df = df.loc[
        (df["CÓDIGO"].notna())
        & (df["CÓDIGO"] != "CÓDIGO")
        & (df["CÓDIGO"] != "LISTAGEM")
    ].copy()

    # strip geral
    for c in ["CÓDIGO", "NOME", "SETOR", "SUBSETOR", "SEGMENTO", "LISTAGEM"]:
        df[c] = df[c].astype(str).str.strip()

    # Mantém apenas o que interessa e renomeia para merge
    df = df.rename(columns={"CÓDIGO": "Ticker_base", "NOME": "nome_empresa"})
    df["Ticker_base"] = df["Ticker_base"].str.upper()
    df["nome_empresa"] = df["nome_empresa"].astype(str)

    df = df[["Ticker_base", "nome_empresa", "SETOR", "SUBSETOR", "SEGMENTO", "LISTAGEM"]]
    return df


def _merge_with_cvm_to_ticker(df_b3: pd.DataFrame, df_map: pd.DataFrame) -> pd.DataFrame:
    # df_map: Ticker_base -> ticker (com número)
    out = df_b3.merge(df_map, on="Ticker_base", how="left")

    # remove linhas sem ticker final
    out = out.dropna(subset=["ticker"]).copy()
    out["ticker"] = out["ticker"].astype(str).str.strip().str.upper()

    # Ajusta colunas finais para o Supabase (conforme sua tabela)
    out = out.drop(columns=["Ticker_base"], errors="ignore")

    out["create_at"] = datetime.now(timezone.utc)  # timestampz

    # Reordena conforme tabela
    out = out[["ticker", "SETOR", "SUBSETOR", "SEGMENTO", "nome_empresa", "create_at"]]
    return out.drop_duplicates(subset=["ticker"])


def _ensure_table_exists(engine: Engine, schema: str, table: str) -> None:
    # Você já tem a tabela; isto só evita falha em ambiente novo
    ddl = f"""
    create schema if not exists {schema};

    create table if not exists {schema}.{table} (
      ticker text primary key,
      "SETOR" text,
      "SUBSETOR" text,
      "SEGMENTO" text,
      nome_empresa text,
      create_at timestamptz
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _upsert_setores(engine: Engine, df: pd.DataFrame, schema: str, table: str) -> int:
    sql = f"""
    insert into {schema}.{table}
      (ticker, "SETOR", "SUBSETOR", "SEGMENTO", nome_empresa, create_at)
    values
      (:ticker, :SETOR, :SUBSETOR, :SEGMENTO, :nome_empresa, :create_at)
    on conflict (ticker) do update set
      "SETOR" = excluded."SETOR",
      "SUBSETOR" = excluded."SUBSETOR",
      "SEGMENTO" = excluded."SEGMENTO",
      nome_empresa = excluded.nome_empresa,
      create_at = excluded.create_at
    ;
    """

    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(text(sql), records)
    return len(records)


def run_ingest(config: IngestConfig) -> None:
    engine = _get_engine(config.supabase_db_url)

    _ensure_table_exists(engine, config.schema, config.table)

    df_map = _load_cvm_to_ticker_from_repo(config.csv_relpath)
    df_raw = _download_b3_classif_setorial()
    df_b3 = _transform_b3_df(df_raw)
    df_final = _merge_with_cvm_to_ticker(df_b3, df_map)

    if df_final.empty:
        raise RuntimeError("DataFrame final ficou vazio. Verifique o CSV cvm_to_ticker e a planilha da B3.")

    n = _upsert_setores(engine, df_final, config.schema, config.table)
    print(f"OK: {n} linhas UPSERT em {config.schema}.{config.table}")


if __name__ == "__main__":
    db_url = os.getenv("SUPABASE_DB_URL", "").strip()
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL não configurada (env ou st.secrets).")

    cfg = IngestConfig(supabase_db_url=db_url)
    run_ingest(cfg)
