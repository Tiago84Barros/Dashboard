"""
pipeline_local/publish_to_supabase.py
Publica financials_annual_final_local e financials_quarterly_final_local → Supabase.

Tabelas destino:
  public."Demonstracoes_Financeiras"      ← annual
  public."Demonstracoes_Financeiras_TRI"  ← quarterly

Uso:
  python -m pipeline_local.publish_to_supabase
  python -m pipeline_local.publish_to_supabase --source annual
  python -m pipeline_local.publish_to_supabase --source quarterly
  python -m pipeline_local.publish_to_supabase --dry-run
"""
from __future__ import annotations

import argparse
import math
import os
import pathlib
import sys
import time
import uuid
from typing import List, Optional
from urllib.parse import urlparse, unquote

# ---------------------------------------------------------------------------
# Carrega .env (mesmo padrão do build_from_raw.py)
# ---------------------------------------------------------------------------
_PROJ = pathlib.Path(__file__).parent.parent
_ENV = _PROJ / ".env"
if _ENV.exists():
    for _line in _ENV.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and "=" in _line and not _line.startswith("#"):
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

_DB_PATH = str(_PROJ / "data" / "local_pipeline.duckdb")
_SUPABASE_URL = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or ""

if not _SUPABASE_URL:
    print("ERRO: SUPABASE_DB_URL nao definida. Configure no .env ou variável de ambiente.")
    sys.exit(1)

import duckdb
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Mapeamento local → Supabase (schema expandido)
# ---------------------------------------------------------------------------
# Colunas presentes em AMBAS as tabelas Supabase (na ordem do INSERT)
_REMOTE_COLS = [
    # Chave
    "Ticker", "data",
    # Identificação
    "Nome", "Source_Doc",
    # DRE
    "Receita_Liquida", "EBIT", "Resultado_Financeiro",
    "Lucro_Antes_IR", "IR_CSLL", "Lucro_Liquido", "LPA",
    # BPA
    "Ativo_Total", "Ativo_Circulante",
    "Caixa", "Aplicacoes_Financeiras", "Contas_Receber",
    "Estoques", "Investimentos", "Imobilizado", "Intangivel",
    # BPP
    "Passivo_Circulante", "Passivo_Nao_Circulante", "Passivo_Total",
    "Divida_CP", "Divida_LP", "Divida_Total", "Divida_Liquida",
    "Fornecedores", "Patrimonio_Liquido", "Participacao_Nao_Ctrl",
    # DFC
    "Caixa_Liquido", "FCI", "FCF",
    # Derivados/legado
    "Dividendos", "Quality_Score",
]

# Colunas de valor que serão atualizadas no ON CONFLICT DO UPDATE
_VALUE_COLS = [c for c in _REMOTE_COLS if c not in ("Ticker", "data")]

_ANNUAL_SELECT = """
    SELECT
        ticker                                      AS "Ticker",
        dt_refer                                    AS "data",
        denom_cia                                   AS "Nome",
        source_doc                                  AS "Source_Doc",

        -- DRE
        receita_liquida                             AS "Receita_Liquida",
        ebit                                        AS "EBIT",
        resultado_financeiro                        AS "Resultado_Financeiro",
        lucro_antes_ir                              AS "Lucro_Antes_IR",
        ir_csll                                     AS "IR_CSLL",
        lucro_liquido                               AS "Lucro_Liquido",
        CASE WHEN ABS(lpa) < 1e13 THEN lpa END     AS "LPA",

        -- BPA
        ativo_total                                 AS "Ativo_Total",
        ativo_circulante                            AS "Ativo_Circulante",
        caixa_equivalentes                          AS "Caixa",
        aplicacoes_financeiras                      AS "Aplicacoes_Financeiras",
        contas_receber                              AS "Contas_Receber",
        estoques                                    AS "Estoques",
        investimentos                               AS "Investimentos",
        imobilizado                                 AS "Imobilizado",
        intangivel                                  AS "Intangivel",

        -- BPP
        passivo_circulante                          AS "Passivo_Circulante",
        passivo_nao_circulante                      AS "Passivo_Nao_Circulante",
        passivo_total                               AS "Passivo_Total",
        divida_cp                                   AS "Divida_CP",
        divida_lp                                   AS "Divida_LP",
        divida_bruta                                AS "Divida_Total",
        divida_liquida                              AS "Divida_Liquida",
        fornecedores                                AS "Fornecedores",
        patrimonio_liquido                          AS "Patrimonio_Liquido",
        participacao_n_controladores                AS "Participacao_Nao_Ctrl",

        -- DFC
        fco                                         AS "Caixa_Liquido",
        fci                                         AS "FCI",
        fcf                                         AS "FCF",

        -- Derivados/legado
        COALESCE(dividendos_declarados,
                 dividendos_jcp_contabeis)          AS "Dividendos",
        quality_score                               AS "Quality_Score"

    FROM pipeline_local.financials_annual_final_local
    ORDER BY ticker, dt_refer
"""

_QTR_SELECT = """
    SELECT
        ticker                                      AS "Ticker",
        dt_refer                                    AS "data",
        denom_cia                                   AS "Nome",
        source_doc                                  AS "Source_Doc",

        -- DRE
        receita_liquida                             AS "Receita_Liquida",
        ebit                                        AS "EBIT",
        resultado_financeiro                        AS "Resultado_Financeiro",
        lucro_antes_ir                              AS "Lucro_Antes_IR",
        ir_csll                                     AS "IR_CSLL",
        lucro_liquido                               AS "Lucro_Liquido",
        CASE WHEN ABS(lpa) < 1e13 THEN lpa END     AS "LPA",

        -- BPA
        ativo_total                                 AS "Ativo_Total",
        ativo_circulante                            AS "Ativo_Circulante",
        caixa_equivalentes                          AS "Caixa",
        aplicacoes_financeiras                      AS "Aplicacoes_Financeiras",
        contas_receber                              AS "Contas_Receber",
        estoques                                    AS "Estoques",
        investimentos                               AS "Investimentos",
        imobilizado                                 AS "Imobilizado",
        intangivel                                  AS "Intangivel",

        -- BPP
        passivo_circulante                          AS "Passivo_Circulante",
        passivo_nao_circulante                      AS "Passivo_Nao_Circulante",
        passivo_total                               AS "Passivo_Total",
        divida_cp                                   AS "Divida_CP",
        divida_lp                                   AS "Divida_LP",
        divida_bruta                                AS "Divida_Total",
        divida_liquida                              AS "Divida_Liquida",
        fornecedores                                AS "Fornecedores",
        patrimonio_liquido                          AS "Patrimonio_Liquido",
        participacao_n_controladores                AS "Participacao_Nao_Ctrl",

        -- DFC
        fco                                         AS "Caixa_Liquido",
        fci                                         AS "FCI",
        fcf                                         AS "FCF",

        -- Derivados/legado (sem dividendos no ITR)
        NULL::DOUBLE                                AS "Dividendos",
        quality_score                               AS "Quality_Score"

    FROM pipeline_local.financials_quarterly_final_local
    ORDER BY ticker, dt_refer
"""

# ---------------------------------------------------------------------------
# DuckDB helper
# ---------------------------------------------------------------------------
def _open_duckdb() -> duckdb.DuckDBPyConnection:
    local_temp = pathlib.Path("C:/DuckDBTemp")
    local_temp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(_DB_PATH, read_only=True)
    con.execute(f"PRAGMA temp_directory='{local_temp.as_posix()}'")
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=2")
    return con


# ---------------------------------------------------------------------------
# Supabase connection via psycopg2
# ---------------------------------------------------------------------------
def _parse_dsn(url: str) -> str:
    """Converte postgresql://user:pass@host:port/db → dsn string para psycopg2."""
    p = urlparse(url)
    password = unquote(p.password or "")
    host = p.hostname or ""
    port = p.port or 5432
    dbname = (p.path or "/postgres").lstrip("/")
    user = p.username or ""
    return f"host={host} port={port} dbname={dbname} user={user} password={password} sslmode=require connect_timeout=30"


def _open_pg() -> psycopg2.extensions.connection:
    dsn = _parse_dsn(_SUPABASE_URL)
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------
def _build_upsert_sql(remote_table: str) -> str:
    cols_sql   = ", ".join(f'"{c}"' for c in _REMOTE_COLS)
    set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in _VALUE_COLS)
    return (
        f'INSERT INTO public."{remote_table}" ({cols_sql}) VALUES %s '
        f'ON CONFLICT ("Ticker", "data") DO UPDATE SET {set_clause}'
    )


def _upsert_batch(
    pg: psycopg2.extensions.connection,
    upsert_sql: str,
    rows: list,
    dry_run: bool,
) -> int:
    if dry_run or not rows:
        return len(rows)
    cur = pg.cursor()
    # None → NULL, float('nan') → NULL, extreme values (>= 1e13) → NULL
    _MAX_VAL = 1e13
    clean = []
    for row in rows:
        clean.append(
            tuple(
                None if (
                    v is None
                    or (isinstance(v, float) and (math.isnan(v) or abs(v) >= _MAX_VAL))
                ) else v
                for v in row
            )
        )
    psycopg2.extras.execute_values(cur, upsert_sql, clean, page_size=500)
    pg.commit()
    cur.close()
    return len(rows)


# ---------------------------------------------------------------------------
# Publish job
# ---------------------------------------------------------------------------
def _publish_job(
    con: duckdb.DuckDBPyConnection,
    pg: psycopg2.extensions.connection,
    label: str,
    select_sql: str,
    remote_table: str,
    dry_run: bool,
    chunk_size: int = 1000,
) -> dict:
    upsert_sql = _build_upsert_sql(remote_table)
    t0 = time.time()

    rel = con.execute(select_sql)
    total_upserted = 0
    total_error = 0
    chunk_n = 0

    print(f"\n[publish] {'[DRY-RUN] ' if dry_run else ''}Job: {label} -> {remote_table}")

    while True:
        batch_df = rel.fetch_df_chunk(chunk_size)
        if batch_df is None or len(batch_df) == 0:
            break
        rows = [tuple(r) for r in batch_df.itertuples(index=False, name=None)]
        try:
            n = _upsert_batch(pg, upsert_sql, rows, dry_run)
            total_upserted += n
        except Exception as exc:
            pg.rollback()
            print(f"  ERRO no chunk {chunk_n}: {exc}")
            total_error += len(rows)
        chunk_n += 1
        print(f"  chunk {chunk_n}: {total_upserted} upserted até agora...", end="\r")

    elapsed = time.time() - t0
    print(f"\n  {'[DRY-RUN] ' if dry_run else ''}Concluído: {total_upserted} linhas | {total_error} erros | {elapsed:.1f}s")
    return {"upserted": total_upserted, "error": total_error, "elapsed_s": round(elapsed, 1)}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(source: str = "all", dry_run: bool = False) -> None:
    run_id = str(uuid.uuid4())
    print(f"[publish_to_supabase] run_id={run_id} source={source} dry_run={dry_run}")

    con = _open_duckdb()
    pg  = _open_pg()

    jobs = []
    if source in ("all", "annual"):
        jobs.append(("annual", _ANNUAL_SELECT, "Demonstracoes_Financeiras"))
    if source in ("all", "quarterly"):
        jobs.append(("quarterly", _QTR_SELECT, "Demonstracoes_Financeiras_TRI"))

    results = {}
    for label, sql, remote_table in jobs:
        results[label] = _publish_job(con, pg, label, sql, remote_table, dry_run)

    con.close()
    pg.close()

    print("\n[publish_to_supabase] === RESUMO ===")
    for label, r in results.items():
        print(f"  {label}: {r['upserted']} upserted | {r['error']} erros | {r['elapsed_s']}s")
    if dry_run:
        print("\n  (DRY-RUN: nenhuma linha foi gravada no Supabase)")
    print("\n[publish_to_supabase] Concluido!")


def main() -> None:
    parser = argparse.ArgumentParser(description="Publica financials locais no Supabase")
    parser.add_argument(
        "--source", choices=["all", "annual", "quarterly"], default="all",
        help="Qual tabela publicar (default: all)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Simula sem gravar no Supabase"
    )
    args = parser.parse_args()
    run(source=args.source, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
