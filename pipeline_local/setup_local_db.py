"""
pipeline_local/setup_local_db.py
Inicializa o banco local (DuckDB ou PostgreSQL) aplicando o schema correto.

Uso:
  python -m pipeline_local.setup_local_db

O script detecta automaticamente o tipo de banco pelo LOCAL_DB_URL e aplica
o DDL correto. Se LOCAL_DB_URL não estiver definida, usa DuckDB em
data/pipeline_local.duckdb (criado automaticamente).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from sqlalchemy import create_engine, text


def _resolve_sql_file(url: str) -> Path:
    sql_dir = Path(__file__).parent / "sql"
    if url.startswith("duckdb"):
        return sql_dir / "create_local_tables_duckdb.sql"
    return sql_dir / "create_local_tables_v2.sql"


def _ensure_data_dir() -> None:
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)


def setup(url: str | None = None) -> bool:
    from pipeline_local.config.settings import load_settings

    _ensure_data_dir()
    settings = load_settings()
    url = url or settings.local_db_url
    sql_file = _resolve_sql_file(url)

    print(f"&gt;&gt; Banco local: {url}")
    print(f"&gt;&gt; Schema DDL:  {sql_file.name}")

    if not sql_file.exists():
        print(f"  [ERRO] Arquivo SQL não encontrado: {sql_file}")
        return False

    sql = sql_file.read_text(encoding="utf-8")

    # Divide em statements, remove linhas de comentário do início de cada bloco
    def _strip_comments(block: str) -> str:
        lines = [l for l in block.splitlines() if not l.strip().startswith("--")]
        return "\n".join(lines).strip()

    statements = [_strip_comments(s) for s in sql.split(";")]
    statements = [s for s in statements if s]

    is_duckdb = url.startswith("duckdb")
    engine_kwargs = {"future": True} if is_duckdb else {"pool_pre_ping": True, "future": True}
    engine = create_engine(url, **engine_kwargs)

    errors = []
    for stmt in statements:
        if not stmt:
            continue
        try:
            with engine.begin() as conn:
                conn.execute(text(stmt))
        except Exception as exc:
            err_lower = str(exc).lower()
            # Ignora erros de "já existe" (idempotência)
            if any(k in err_lower for k in ("already exists", "duplicate", "exists")):
                continue
            errors.append(f"{stmt[:60]}... => {exc}")

    engine.dispose()

    if errors:
        print(f"  [ERRO] {len(errors)} statement(s) falharam:")
        for e in errors[:5]:
            print(f"         {e}")
        return False

    print("  [OK]  Schema aplicado com sucesso.")
    return True


def main() -> None:
    print("\n=== pipeline_local — inicialização do banco local ===\n")
    ok = setup()
    if ok:
        print("\nBanco local pronto. Execute agora:")
        print("  python -m pipeline_local.check_setup")
        print("  python -m pipeline_local.run_pipeline --stage extract --source DFP --year-start 2023 --year-end 2023")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
