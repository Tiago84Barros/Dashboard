"""
pipeline_local/fix_mappings.py
Reparo pós-extração: corrige ticker e canonical_key nas tabelas locais.

Problema: durante a extração (DFP/ITR) e o enriquecimento (enrich),
  o Supabase estava inacessível → ticker=NULL em todas as linhas raw/enriched
  e canonical_key=NULL (DFP) ou cd_conta bruto (ITR) no enriched.

O que este script faz:
  1. Carrega cvm_to_ticker do Supabase (cd_cvm → ticker)
  2. Carrega cvm_account_map do Supabase (cd_conta → canonical_key)
  3. Registra ambos como tabelas temporárias no DuckDB
  4. UPDATE cvm_dfp_raw_local SET ticker = ...
  5. UPDATE cvm_itr_raw_local SET ticker = ...
  6. UPDATE cvm_raw_enriched_local SET ticker = ...
  7. UPDATE cvm_raw_enriched_local SET canonical_key, qualidade_mapeamento = ...

Uso (no terminal onde SUPABASE_DB_URL está definida):
  python -m pipeline_local.fix_mappings
  -- ou --
  python pipeline_local/fix_mappings.py
"""
from __future__ import annotations

import os
import pathlib
import sys

# Carrega .env se existir (python-dotenv opcional)
_proj_root = pathlib.Path(__file__).parent.parent
_env_file = _proj_root / ".env"
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file)
        print(f"[fix_mappings] .env carregado de {_env_file}")
    except ImportError:
        # Carrega manualmente sem dotenv
        for line in _env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
        print(f"[fix_mappings] .env carregado manualmente de {_env_file}")

import duckdb
import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_DB_PATH = str(_proj_root / "data" / "local_pipeline.duckdb")
_SUPABASE_URL = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or ""

if not _SUPABASE_URL:
    print("[fix_mappings] ERRO: SUPABASE_DB_URL nao definida.")
    print("  Defina no terminal: $env:SUPABASE_DB_URL = 'postgresql://...'")
    print("  Ou crie um arquivo .env na raiz do projeto com: SUPABASE_DB_URL=postgresql://...")
    sys.exit(1)

print(f"[fix_mappings] Supabase URL: {_SUPABASE_URL[:60]}...")
print(f"[fix_mappings] DuckDB: {_DB_PATH}")


# ---------------------------------------------------------------------------
# 1. Carregar tabelas de mapeamento do Supabase
# ---------------------------------------------------------------------------
def _load_from_supabase() -> tuple[pd.DataFrame, pd.DataFrame]:
    engine = create_engine(_SUPABASE_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        print("[fix_mappings] Carregando cvm_to_ticker...")
        ticker_map = pd.read_sql(
            text("SELECT cd_cvm::INTEGER AS cd_cvm, ticker FROM public.cvm_to_ticker WHERE ticker IS NOT NULL"),
            conn,
        )
        print(f"  -> {len(ticker_map)} registros")

        print("[fix_mappings] Carregando cvm_account_map...")
        account_map = pd.read_sql(
            text("""
                SELECT cd_conta, canonical_key, qualidade_mapeamento
                FROM public.cvm_account_map
                WHERE ativo = TRUE
                  AND canonical_key IS NOT NULL
                  AND cd_conta IS NOT NULL
            """),
            conn,
        )
        print(f"  -> {len(account_map)} registros")

    return ticker_map, account_map


# ---------------------------------------------------------------------------
# 2. Aplicar no DuckDB via UPDATE
# ---------------------------------------------------------------------------
def _apply_fixes(ticker_map: pd.DataFrame, account_map: pd.DataFrame) -> None:
    local_temp = pathlib.Path("C:/DuckDBTemp")
    local_temp.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(_DB_PATH)
    try:
        con.execute(f"PRAGMA temp_directory='{local_temp.as_posix()}'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=2")
        con.execute("SET memory_limit='3GB'")
        con.execute("PRAGMA max_temp_directory_size='10GiB'")

        # Registra os mapas como tabelas temporárias
        con.register("_fix_ticker_map", ticker_map)
        con.register("_fix_account_map", account_map)

        # -----------------------------------------------------------------
        # 2a. UPDATE raw DFP — tickers
        # -----------------------------------------------------------------
        print("[fix_mappings] Atualizando ticker em cvm_dfp_raw_local...")
        before = con.execute(
            "SELECT COUNT(*) FROM pipeline_local.cvm_dfp_raw_local WHERE ticker IS NOT NULL"
        ).fetchone()[0]

        con.execute("""
            UPDATE pipeline_local.cvm_dfp_raw_local AS r
            SET ticker = m.ticker
            FROM _fix_ticker_map AS m
            WHERE r.cd_cvm = m.cd_cvm
              AND r.ticker IS NULL
        """)

        after = con.execute(
            "SELECT COUNT(*) FROM pipeline_local.cvm_dfp_raw_local WHERE ticker IS NOT NULL"
        ).fetchone()[0]
        print(f"  -> DFP raw: {before:,} -> {after:,} linhas com ticker (delta: {after - before:,})")

        # -----------------------------------------------------------------
        # 2b. UPDATE raw ITR — tickers
        # -----------------------------------------------------------------
        print("[fix_mappings] Atualizando ticker em cvm_itr_raw_local...")
        before = con.execute(
            "SELECT COUNT(*) FROM pipeline_local.cvm_itr_raw_local WHERE ticker IS NOT NULL"
        ).fetchone()[0]

        con.execute("""
            UPDATE pipeline_local.cvm_itr_raw_local AS r
            SET ticker = m.ticker
            FROM _fix_ticker_map AS m
            WHERE r.cd_cvm = m.cd_cvm
              AND r.ticker IS NULL
        """)

        after = con.execute(
            "SELECT COUNT(*) FROM pipeline_local.cvm_itr_raw_local WHERE ticker IS NOT NULL"
        ).fetchone()[0]
        print(f"  -> ITR raw: {before:,} -> {after:,} linhas com ticker (delta: {after - before:,})")

        # -----------------------------------------------------------------
        # 2c. UPDATE enriched — tickers (via cd_cvm)
        # -----------------------------------------------------------------
        print("[fix_mappings] Atualizando ticker em cvm_raw_enriched_local...")
        before = con.execute(
            "SELECT COUNT(*) FROM pipeline_local.cvm_raw_enriched_local WHERE ticker IS NOT NULL"
        ).fetchone()[0]

        con.execute("""
            UPDATE pipeline_local.cvm_raw_enriched_local AS r
            SET ticker = m.ticker
            FROM _fix_ticker_map AS m
            WHERE r.cd_cvm = m.cd_cvm
              AND r.ticker IS NULL
        """)

        after = con.execute(
            "SELECT COUNT(*) FROM pipeline_local.cvm_raw_enriched_local WHERE ticker IS NOT NULL"
        ).fetchone()[0]
        print(f"  -> Enriched: {before:,} -> {after:,} linhas com ticker (delta: {after - before:,})")

        # -----------------------------------------------------------------
        # 2d. UPDATE enriched — canonical_key (via cd_conta)
        # -----------------------------------------------------------------
        print("[fix_mappings] Atualizando canonical_key em cvm_raw_enriched_local...")
        before_ck = con.execute(
            "SELECT COUNT(*) FROM pipeline_local.cvm_raw_enriched_local WHERE canonical_key IS NOT NULL"
        ).fetchone()[0]

        # Processa um ano por vez para evitar OOM
        anos = [
            r[0] for r in con.execute("""
                SELECT DISTINCT EXTRACT(YEAR FROM dt_refer)::INTEGER AS yr
                FROM pipeline_local.cvm_raw_enriched_local
                ORDER BY yr
            """).fetchall()
        ]
        print(f"  Anos a processar: {anos}")

        for ano in anos:
            con.execute(f"""
                UPDATE pipeline_local.cvm_raw_enriched_local AS r
                SET canonical_key        = m.canonical_key,
                    qualidade_mapeamento = m.qualidade_mapeamento
                FROM _fix_account_map AS m
                WHERE r.cd_conta = m.cd_conta
                  AND EXTRACT(YEAR FROM r.dt_refer)::INTEGER = {ano}
            """)
            count = con.execute(f"""
                SELECT COUNT(*) FROM pipeline_local.cvm_raw_enriched_local
                WHERE canonical_key IS NOT NULL
                  AND EXTRACT(YEAR FROM dt_refer)::INTEGER = {ano}
            """).fetchone()[0]
            print(f"    ano {ano}: {count:,} linhas com canonical_key")

        after_ck = con.execute(
            "SELECT COUNT(*) FROM pipeline_local.cvm_raw_enriched_local WHERE canonical_key IS NOT NULL"
        ).fetchone()[0]
        print(f"  -> canonical_key: {before_ck:,} -> {after_ck:,} (delta: {after_ck - before_ck:,})")

        # -----------------------------------------------------------------
        # Resumo final
        # -----------------------------------------------------------------
        print("\n[fix_mappings] === RESUMO FINAL ===")
        for tbl in [
            "pipeline_local.cvm_dfp_raw_local",
            "pipeline_local.cvm_itr_raw_local",
            "pipeline_local.cvm_raw_enriched_local",
        ]:
            total  = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            w_tick = con.execute(f"SELECT COUNT(*) FROM {tbl} WHERE ticker IS NOT NULL").fetchone()[0]
            pct = 100 * w_tick / total if total else 0
            print(f"  {tbl}: {w_tick:,}/{total:,} com ticker ({pct:.1f}%)")

        ck_total = con.execute(
            "SELECT COUNT(*) FROM pipeline_local.cvm_raw_enriched_local"
        ).fetchone()[0]
        ck_ok = con.execute(
            "SELECT COUNT(*) FROM pipeline_local.cvm_raw_enriched_local WHERE canonical_key IS NOT NULL"
        ).fetchone()[0]
        print(f"  enriched canonical_key: {ck_ok:,}/{ck_total:,} ({100*ck_ok/ck_total:.1f}%)")

    finally:
        con.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ticker_map, account_map = _load_from_supabase()
    _apply_fixes(ticker_map, account_map)
    print("\n[fix_mappings] Concluido. Agora rode:")
    print("  python -m pipeline_local.run_pipeline --stage transform --source DFP")
    print("  python -m pipeline_local.run_pipeline --stage transform --source ITR")


if __name__ == "__main__":
    main()
