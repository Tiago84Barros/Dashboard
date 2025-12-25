from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


# Observação importante (Streamlit): o diretório de trabalho (CWD) pode variar
# conforme a forma de execução. Se usarmos "data/metadados.db" (caminho relativo),
# o script pode acabar lendo um banco inexistente/errado e ainda assim concluir
# sem escrever nada no Supabase.
#
# Para evitar isso, resolvemos o caminho do metadados.db a partir da localização
# deste arquivo.
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

    # Validação prática: se o script roda, mas nada aparece no Supabase,
    # normalmente é (a) engine apontando para o projeto errado, ou
    # (b) script lendo um metadados.db errado (caminho relativo/CWD).
    # Contar antes/depois nos dá um veredito objetivo.
    before: Optional[int]
    after: Optional[int]
    try:
        before = _count_remote(engine)
    except Exception:
        before = None

    if progress_cb:
        progress_cb("SETORES: carregando dados do metadados.db...")

    df = _load_setores_from_metadados()

    if df.empty:
        raise RuntimeError("Tabela setores no metadados.db está vazia.")

    if progress_cb:
        progress_cb(f"SETORES: upsert de {len(df):,} registros...".replace(",", "."))

    _upsert(engine, df)

    try:
        after = _count_remote(engine)
    except Exception:
        after = None

    if progress_cb and before is not None and after is not None:
        delta = after - before
        progress_cb(
            f"SETORES: Supabase agora com {after:,} registros (delta {delta:+,}).".replace(",", ".")
        )

    # Se conseguimos contar e o resultado final é 0, consideramos falha prática.
    if after == 0:
        raise RuntimeError(
            "SETORES: ingestão finalizada, mas a tabela public.setores permanece vazia no Supabase. "
            "Verifique se a SUPABASE_DB_URL aponta para o projeto correto e se a conexão não está indo "
            "para outro ambiente/banco."
        )

    if progress_cb:
        progress_cb("SETORES: concluído.")
