# pickup/dados_setores_b3.py
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Iterable, List

import pandas as pd
from sqlalchemy import create_engine, text


@dataclass(frozen=True)
class Config:
    sqlite_path: str
    supabase_db_url: str
    target_schema: str = "public"
    target_table: str = "setores"
    chunk_size: int = 1000


REQUIRED_COLS = ["ticker", "nome_empresa", "setor", "subsetor", "segmento", "listagem"]


def _load_from_sqlite(sqlite_path: str) -> pd.DataFrame:
    if not os.path.exists(sqlite_path):
        raise FileNotFoundError(f"SQLite não encontrado em: {sqlite_path}")

    with sqlite3.connect(sqlite_path) as conn:
        # Leitura direta da tabela 'setores' já consolidada pelo fluxo antigo (Algoritmo_2).
        df = pd.read_sql_query("SELECT * FROM setores", conn)

    if df.empty:
        raise RuntimeError("Tabela SQLite 'setores' está vazia. Nada para migrar.")

    return df


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Normalização defensiva: aceita variações mínimas de nomes e padroniza para o contrato final.
    rename_map = {
        "Ticker": "ticker",
        "ticker": "ticker",
        "CÓDIGO": "ticker",
        "codigo": "ticker",
        "NOME": "nome_empresa",
        "nome": "nome_empresa",
        "nome_empresa": "nome_empresa",
        "SETOR": "setor",
        "setor": "setor",
        "SUBSETOR": "subsetor",
        "subsetor": "subsetor",
        "SEGMENTO": "segmento",
        "segmento": "segmento",
        "LISTAGEM": "listagem",
        "listagem": "listagem",
    }

    # Renomeia somente colunas presentes
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise KeyError(
            "Colunas obrigatórias ausentes na tabela SQLite 'setores': "
            f"{missing}. Colunas encontradas: {list(df.columns)}"
        )

    df = df[REQUIRED_COLS].copy()

    # Limpeza de strings
    for c in REQUIRED_COLS:
        df[c] = df[c].astype("string").str.strip()

    # Regras mínimas coerentes com o Algoritmo_2:
    # - ticker não pode ser vazio/nulo
    # - campos textuais podem ser vazios, mas não devem ser NaN
    df = df.dropna(subset=["ticker"])
    df = df[df["ticker"].str.len() > 0]

    # Preenche vazios como 'AUSENTE' para listagem (padrão do Algoritmo_2)
    df["listagem"] = df["listagem"].fillna("AUSENTE")
    df.loc[df["listagem"].str.len() == 0, "listagem"] = "AUSENTE"

    # Deduplicação por ticker (prioriza a última ocorrência)
    df = df.drop_duplicates(subset=["ticker"], keep="last").reset_index(drop=True)
    return df


def _ensure_table(engine, schema: str, table: str) -> None:
    # Cria tabela (caso não exista) e garante PK/unique no ticker.
    # Obs.: se você já criou a tabela no Supabase via migrations, isso aqui apenas “assegura” o mínimo.
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        ticker TEXT PRIMARY KEY,
        nome_empresa TEXT,
        setor TEXT,
        subsetor TEXT,
        segmento TEXT,
        listagem TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _upsert(engine, schema: str, table: str, df: pd.DataFrame, chunk_size: int = 1000) -> int:
    sql = f"""
    INSERT INTO {schema}.{table} (ticker, nome_empresa, setor, subsetor, segmento, listagem)
    VALUES (:ticker, :nome_empresa, :setor, :subsetor, :segmento, :listagem)
    ON CONFLICT (ticker) DO UPDATE SET
        nome_empresa = EXCLUDED.nome_empresa,
        setor        = EXCLUDED.setor,
        subsetor     = EXCLUDED.subsetor,
        segmento     = EXCLUDED.segmento,
        listagem     = EXCLUDED.listagem;
    """

    total = 0
    rows = df.to_dict(orient="records")

    with engine.begin() as conn:
        for i in range(0, len(rows), chunk_size):
            batch = rows[i : i + chunk_size]
            conn.execute(text(sql), batch)
            total += len(batch)

    return total


def main() -> None:
    supabase_db_url = os.getenv("SUPABASE_DB_URL")
    if not supabase_db_url:
        raise EnvironmentError("SUPABASE_DB_URL não definida. Configure em Secrets/Env Vars.")

    # Padrão do projeto: fonte local dentro do repo
    sqlite_path = os.getenv("SQLITE_METADADOS_PATH", "data/metadados.db")

    cfg = Config(sqlite_path=sqlite_path, supabase_db_url=supabase_db_url)

    print("[setores_b3] Iniciando carga de setores (SQLite -> Supabase)")
    print(f"[setores_b3] Fonte SQLite: {cfg.sqlite_path}")
    print(f"[setores_b3] Destino: {cfg.target_schema}.{cfg.target_table}")

    df_raw = _load_from_sqlite(cfg.sqlite_path)
    print(f"[setores_b3] Linhas lidas do SQLite: {len(df_raw)}")

    df = _normalize(df_raw)
    print(f"[setores_b3] Linhas após normalização/dedup: {len(df)}")

    engine = create_engine(cfg.supabase_db_url, pool_pre_ping=True)
    _ensure_table(engine, cfg.target_schema, cfg.target_table)

    n = _upsert(engine, cfg.target_schema, cfg.target_table, df, cfg.chunk_size)
    print(f"[setores_b3] UPSERT concluído. Linhas gravadas/atualizadas: {n}")
    print("[setores_b3] Execução concluída com sucesso.")


if __name__ == "__main__":
    main()
