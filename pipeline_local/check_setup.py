"""
pipeline_local/check_setup.py
Validação rápida do ambiente antes de rodar o pipeline.

Verifica:
  1. LOCAL_DB_URL definida e banco local acessível
  2. Schema pipeline_local existe no banco local
  3. Tabelas mínimas existem
  4. SUPABASE_DB_URL definida e acessível (opcional — necessária para enrich e publish)

Uso:
  python -m pipeline_local.check_setup
"""
from __future__ import annotations

import os
import sys

REQUIRED_TABLES = [
    "cvm_dfp_raw_local",
    "cvm_itr_raw_local",
    "cvm_raw_enriched_local",
    "financials_annual_final_local",
    "financials_quarterly_final_local",
    "pipeline_runs_local",
    "pipeline_quality_checks_local",
    "pipeline_publish_log_local",
]

_OK = "  [OK]"
_FAIL = "  [FAIL]"
_WARN = "  [WARN]"


def check_local_db() -> bool:
    from pipeline_local.config.settings import load_settings
    settings = load_settings()
    local_url = settings.local_db_url
    is_duckdb = local_url.startswith("duckdb")

    if is_duckdb:
        print(f"{_OK}  Usando DuckDB (sem servidor): {local_url}")
    else:
        print(f"{_OK}  LOCAL_DB_URL definida: {local_url[:40]}...")

    try:
        from sqlalchemy import create_engine, text
        kw = {"future": True} if is_duckdb else {"pool_pre_ping": True}
        engine = create_engine(local_url, **kw)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        print(f"{_OK}  Conexão com banco local OK.")
    except Exception as exc:
        print(f"{_FAIL} Não foi possível conectar ao banco local: {exc}")
        return False

    return True


def check_schema_and_tables() -> bool:
    from pipeline_local.config.settings import load_settings
    settings = load_settings()
    local_url = settings.local_db_url
    is_duckdb = local_url.startswith("duckdb")

    try:
        from sqlalchemy import create_engine, text
        kw = {"future": True} if is_duckdb else {"pool_pre_ping": True}
        engine = create_engine(local_url, **kw)

        # Verifica schema verificando diretamente a tabela de controle
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM pipeline_local.pipeline_runs_local LIMIT 1"))
            schema_exists = True
        except Exception:
            schema_exists = False

        if not schema_exists:
            print(f"{_FAIL} Schema 'pipeline_local' não existe. Execute:")
            print("       python -m pipeline_local.setup_local_db")
            engine.dispose()
            return False

        print(f"{_OK}  Schema 'pipeline_local' existe.")

        missing = []
        for table in REQUIRED_TABLES:
            try:
                with engine.connect() as conn:
                    conn.execute(text(f"SELECT 1 FROM pipeline_local.{table} LIMIT 1"))
            except Exception:
                missing.append(table)

        engine.dispose()

        if missing:
            print(f"{_FAIL} Tabelas faltando: {missing}")
            print("       Execute: python -m pipeline_local.setup_local_db")
            return False

        print(f"{_OK}  Todas as {len(REQUIRED_TABLES)} tabelas existem.")
        return True

    except Exception as exc:
        print(f"{_FAIL} Erro ao verificar schema/tabelas: {exc}")
        return False


def check_supabase() -> bool:
    sb_url = (os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or "").strip()
    if not sb_url:
        print(f"{_WARN} SUPABASE_DB_URL não definida.")
        print("       Necessária para: enrich (cvm_account_map), publish, audit.")
        print("       Pipeline de extração e transformação funciona sem ela.")
        return False

    print(f"{_OK}  SUPABASE_DB_URL definida.")

    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(sb_url, pool_pre_ping=True, connect_args={"connect_timeout": 10})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"{_OK}  Conexão com Supabase OK.")
        return True
    except Exception as exc:
        print(f"{_WARN} Supabase definido mas não acessível: {exc}")
        return False


def check_imports() -> bool:
    ok = True
    modules = [
        ("sqlalchemy", "sqlalchemy"),
        ("pandas", "pandas"),
        ("requests", "requests"),
        ("numpy", "numpy"),
    ]
    for name, mod in modules:
        try:
            __import__(mod)
            print(f"{_OK}  {name} instalado.")
        except ImportError:
            print(f"{_FAIL} {name} não encontrado. Execute: pip install {name}")
            ok = False
    return ok


def main() -> None:
    print("\n=== pipeline_local — verificação de ambiente ===\n")

    print("&gt;&gt; Dependências Python:")
    imports_ok = check_imports()

    print("\n&gt;&gt; Banco local:")
    local_ok = check_local_db()

    schema_ok = False
    if local_ok:
        schema_ok = check_schema_and_tables()

    print("\n&gt;&gt; Supabase (opcional para extract, obrigatório para publish):")
    sb_ok = check_supabase()

    print("\n=== Resultado ===")
    if imports_ok and local_ok and schema_ok:
        print("  Pronto para rodar extract e transform.")
        if sb_ok:
            print("  Pronto para rodar publish e audit.")
        else:
            print("  Publish e audit requerem SUPABASE_DB_URL acessível.")
        sys.exit(0)
    else:
        print("  Corrija os itens [FAIL] antes de rodar o pipeline.")
        sys.exit(1)


if __name__ == "__main__":
    main()
