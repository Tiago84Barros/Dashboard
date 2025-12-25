from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# Caminho do banco SQLite versionado no repositório.
#
# Observação importante (Streamlit): o diretório de trabalho (CWD) pode variar
# conforme a forma de execução (local, Streamlit Cloud, multipage, etc.).
# Para não "achar" um data/metadados.db errado (ou não achar nenhum),
# resolvemos o caminho a partir da localização deste arquivo.
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # .../Dashboard-Modulos
METADADOS_DB_PATH = PROJECT_ROOT / "data" / "metadados.db"


def _count_remote(engine: Engine) -> int:
    """Conta registros na tabela remota (Supabase)."""
    with engine.connect() as conn:
        return int(conn.execute(text("select count(*) from public.setores")).scalar() or 0)


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


def _load_setores_from_metadados() -> pd.DataFrame:
    if not METADADOS_DB_PATH.exists():
        raise FileNotFoundError(f"Banco não encontrado em {METADADOS_DB_PATH.resolve()}")

    conn = sqlite3.connect(METADADOS_DB_PATH)
    try:
        df = pd.read_sql(
            """
            SELECT
                UPPER(TRIM(ticker))      AS ticker,
                SETOR,
                SUBSETOR,
                SEGMENTO,
                nome_empresa
            FROM setores
            WHERE ticker IS NOT NULL
            """,
            conn,
        )
    finally:
        conn.close()

    df = df.dropna(subset=["ticker"])
    df = df.drop_duplicates(subset=["ticker"])

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

    rows = df.to_dict("records")
    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(sql), rows[i : i + batch])


def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    _ensure_table(engine)

    # Métrica simples para validar se houve efeito no Supabase.
    # (Evita "rodou com êxito" quando, na prática, nada foi persistido.)
    before = 0
    try:
        before = _count_remote(engine)
    except Exception:
        # Se a contagem falhar por qualquer motivo (permissões, schema, etc.),
        # não interrompemos a ingestão; apenas não teremos a métrica de delta.
        before = 0

    if progress_cb:
        progress_cb("SETORES: carregando dados do metadados.db...")

    df = _load_setores_from_metadados()

    if df.empty:
        raise RuntimeError("Tabela setores no metadados.db está vazia.")

    if progress_cb:
        progress_cb(f"SETORES: upsert de {len(df):,} registros...".replace(",", "."))

    _upsert(engine, df)

    after = before
    try:
        after = _count_remote(engine)
    except Exception:
        after = before

    if progress_cb:
        if after > 0:
            delta = after - before
            progress_cb(
                f"SETORES: Supabase agora com {after:,} registros "
                f"(delta {delta:+,}).".replace(",", ".")
            )
        else:
            progress_cb("SETORES: atenção — contagem remota retornou 0.")

    # Se a tabela ainda estiver vazia após o upsert, consideramos falha prática.
    # Isso ajuda a capturar casos de engine apontando para o banco errado.
    if after == 0:
        raise RuntimeError(
            "SETORES: ingestão finalizada, mas a tabela public.setores permanece vazia no Supabase. "
            "Verifique se a SUPABASE_DB_URL aponta para o projeto correto e se a conexão não está indo "
            "para um banco local/ambiente diferente."
        )

    if progress_cb:
        progress_cb("SETORES: concluído.")
