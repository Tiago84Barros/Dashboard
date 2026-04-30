"""
pipeline_local/utils/duckdb_utils.py
Utilitários de insert/upsert vetorizados para DuckDB.

O SQLAlchemy executemany com parâmetros nomeados é extremamente lento no
duckdb-engine (processa linha a linha internamente). A solução é usar a API
nativa do DuckDB: registrar o DataFrame como view temporária e executar
INSERT INTO ... SELECT * FROM _tmp_df, que é totalmente vetorizado.

ATENÇÃO: engine.dispose() é chamado antes de abrir a conexão nativa porque
DuckDB permite apenas um writer por vez. O SQLAlchemy recria a conexão
automaticamente quando necessário após o dispose.
"""
from __future__ import annotations

import os
from typing import Dict, List, Optional

import pandas as pd


def is_duckdb(engine) -> bool:
    return str(engine.url).startswith("duckdb")


def _get_db_path(engine) -> str:
    path = engine.url.database or ""
    return os.path.normpath(path)


def bulk_insert_duckdb(
    df: pd.DataFrame,
    engine,
    table: str,
    conflict_col: str,
) -> Dict[str, int]:
    """
    INSERT INTO table SELECT * FROM df ON CONFLICT (conflict_col) DO NOTHING.
    Usa API nativa do DuckDB — vetorizado, sem overhead de executemany.
    """
    import duckdb

    cols = ", ".join(df.columns.tolist())
    db_path = _get_db_path(engine)
    engine.dispose()

    con = duckdb.connect(db_path)
    try:
        before = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        con.register("_tmp_insert", df)
        con.execute(
            f"INSERT INTO {table} ({cols}) "
            f"SELECT {cols} FROM _tmp_insert "
            f"ON CONFLICT ({conflict_col}) DO NOTHING"
        )
        after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        con.unregister("_tmp_insert")
        inserted = after - before
        return {"inserted": inserted, "skipped": len(df) - inserted}
    except Exception as exc:
        raise RuntimeError(f"bulk_insert_duckdb falhou em {table}: {exc}") from exc
    finally:
        con.close()


def upsert_duckdb(
    df: pd.DataFrame,
    engine,
    table: str,
    conflict_cols: List[str],
    update_cols: List[str],
) -> Dict[str, int]:
    """
    INSERT INTO table SELECT * FROM df
    ON CONFLICT (conflict_cols) DO UPDATE SET update_cols = EXCLUDED.col.
    Usa API nativa do DuckDB.
    """
    import duckdb

    cols = ", ".join(df.columns.tolist())
    conflict = ", ".join(conflict_cols)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    db_path = _get_db_path(engine)
    engine.dispose()

    con = duckdb.connect(db_path)
    try:
        before = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        con.register("_tmp_upsert", df)
        con.execute(
            f"INSERT INTO {table} ({cols}) "
            f"SELECT {cols} FROM _tmp_upsert "
            f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
        )
        after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        con.unregister("_tmp_upsert")
        upserted = after - before
        return {"inserted": upserted, "updated": len(df) - upserted}
    except Exception as exc:
        raise RuntimeError(f"upsert_duckdb falhou em {table}: {exc}") from exc
    finally:
        con.close()
