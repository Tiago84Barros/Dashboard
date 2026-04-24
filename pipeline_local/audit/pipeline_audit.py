"""
pipeline_local/audit/pipeline_audit.py
Auditoria e comparação do banco local vs Supabase.

Checks disponíveis:
  1. count_check      — compara contagens de linhas entre local e remoto
  2. sample_check     — compara amostra de valores para um ticker/período
  3. coverage_check   — verifica quais tickers têm dados no local mas não no remoto
  4. quality_check    — verifica quality_score mínimo nas tabelas finais
  5. null_check       — verifica campos críticos nulos na tabela final local
  6. publish_lag_check — lista linhas locais que nunca foram publicadas

Uso:
  python -m pipeline_local.audit.pipeline_audit --check all
  python -m pipeline_local.audit.pipeline_audit --check count_check
  python -m pipeline_local.audit.pipeline_audit --check coverage_check --ticker PETR3
"""
from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import text as sa_text

from pipeline_local.config.connections import get_local_engine, get_supabase_engine
from pipeline_local.utils.logger import get_logger

log = get_logger("pipeline_audit")

_LOCAL_ANNUAL = "pipeline_local.financials_annual_final_local"
_LOCAL_QUARTERLY = "pipeline_local.financials_quarterly_final_local"
_REMOTE_ANNUAL = 'public."Demonstracoes_Financeiras"'
_REMOTE_QUARTERLY = 'public."Demonstracoes_Financeiras_TRI"'

_CRITICAL_COLS = [
    "receita_liquida", "lucro_liquido", "ativo_total",
    "patrimonio_liquido", "divida_bruta",
]


# ---------------------------------------------------------------------------
# 1. COUNT CHECK — contagens local vs remoto
# ---------------------------------------------------------------------------
def count_check(ticker: Optional[str] = None) -> Dict:
    local_engine = get_local_engine()
    remote_engine = get_supabase_engine()
    results = []

    pairs = [
        (_LOCAL_ANNUAL, _REMOTE_ANNUAL, "annual"),
        (_LOCAL_QUARTERLY, _REMOTE_QUARTERLY, "quarterly"),
    ]
    for local_tbl, remote_tbl, label in pairs:
        where = f"WHERE ticker = '{ticker.upper()}'" if ticker else ""
        remote_where = f'WHERE "Ticker" = \'{ticker.upper()}\'' if ticker else ""

        with local_engine.connect() as conn:
            local_count = conn.execute(sa_text(f"SELECT COUNT(*) FROM {local_tbl} {where}")).scalar() or 0
        try:
            with remote_engine.connect() as conn:
                remote_count = conn.execute(sa_text(f"SELECT COUNT(*) FROM {remote_tbl} {remote_where}")).scalar() or 0
        except Exception as exc:
            remote_count = f"ERROR: {exc}"

        status = "pass" if local_count == remote_count else ("warn" if isinstance(remote_count, int) else "fail")
        r = {
            "check": "count_check",
            "table": label,
            "local_count": local_count,
            "remote_count": remote_count,
            "delta": (local_count - remote_count) if isinstance(remote_count, int) else None,
            "status": status,
        }
        results.append(r)
        log.info("count_check", **r)
    return {"check": "count_check", "results": results}


# ---------------------------------------------------------------------------
# 2. SAMPLE CHECK — compara valores de amostra por ticker
# ---------------------------------------------------------------------------
def sample_check(ticker: str, year: Optional[int] = None) -> Dict:
    if not ticker:
        return {"check": "sample_check", "error": "ticker obrigatório"}

    local_engine = get_local_engine()
    remote_engine = get_supabase_engine()
    ticker = ticker.upper()

    year_filter = f"AND EXTRACT(YEAR FROM dt_refer) = {year}" if year else ""
    with local_engine.connect() as conn:
        df_local = pd.read_sql(
            sa_text(f"""
                SELECT ticker, dt_refer, receita_liquida, lucro_liquido, ativo_total, patrimonio_liquido
                FROM {_LOCAL_ANNUAL}
                WHERE ticker = :tk {year_filter}
                ORDER BY dt_refer DESC LIMIT 5
            """),
            conn, params={"tk": ticker},
        )

    remote_year_filter = f"AND EXTRACT(YEAR FROM data) = {year}" if year else ""
    try:
        with remote_engine.connect() as conn:
            df_remote = pd.read_sql(
                sa_text(f"""
                    SELECT "Ticker", data, "Receita_Liquida", "Lucro_Liquido", "Ativo_Total", "Patrimonio_Liquido"
                    FROM {_REMOTE_ANNUAL}
                    WHERE "Ticker" = :tk {remote_year_filter}
                    ORDER BY data DESC LIMIT 5
                """),
                conn, params={"tk": ticker},
            )
    except Exception as exc:
        log.error("Falha ao ler amostra remota", erro=str(exc))
        df_remote = pd.DataFrame()

    log.info("sample_check", ticker=ticker, local_rows=len(df_local), remote_rows=len(df_remote))
    return {
        "check": "sample_check",
        "ticker": ticker,
        "local_sample": df_local.to_dict("records"),
        "remote_sample": df_remote.to_dict("records"),
    }


# ---------------------------------------------------------------------------
# 3. COVERAGE CHECK — tickers no local mas ausentes no remoto
# ---------------------------------------------------------------------------
def coverage_check() -> Dict:
    local_engine = get_local_engine()
    remote_engine = get_supabase_engine()

    with local_engine.connect() as conn:
        local_tickers = {r[0] for r in conn.execute(
            sa_text(f"SELECT DISTINCT ticker FROM {_LOCAL_ANNUAL} WHERE ticker IS NOT NULL")
        ).fetchall()}

    try:
        with remote_engine.connect() as conn:
            remote_tickers = {r[0] for r in conn.execute(
                sa_text(f'SELECT DISTINCT "Ticker" FROM {_REMOTE_ANNUAL} WHERE "Ticker" IS NOT NULL')
            ).fetchall()}
    except Exception as exc:
        log.error("Falha ao ler tickers remotos", erro=str(exc))
        remote_tickers = set()

    only_local = sorted(local_tickers - remote_tickers)
    only_remote = sorted(remote_tickers - local_tickers)
    status = "pass" if not only_local else "warn"

    log.info("coverage_check",
             local_tickers=len(local_tickers),
             remote_tickers=len(remote_tickers),
             only_local=len(only_local),
             only_remote=len(only_remote),
             status=status)

    return {
        "check": "coverage_check",
        "local_tickers": len(local_tickers),
        "remote_tickers": len(remote_tickers),
        "only_in_local": only_local[:50],   # primeiros 50
        "only_in_remote": only_remote[:50],
        "status": status,
    }


# ---------------------------------------------------------------------------
# 4. QUALITY CHECK — quality_score nas tabelas finais
# ---------------------------------------------------------------------------
def quality_check(min_score: float = 40.0) -> Dict:
    local_engine = get_local_engine()
    results = []

    for table, label in [(_LOCAL_ANNUAL, "annual"), (_LOCAL_QUARTERLY, "quarterly")]:
        with local_engine.connect() as conn:
            row = conn.execute(sa_text(f"""
                SELECT
                    COUNT(*) AS total,
                    AVG(quality_score) AS avg_score,
                    MIN(quality_score) AS min_score,
                    COUNT(*) FILTER (WHERE quality_score < :min_score) AS below_threshold
                FROM {table}
                WHERE quality_score IS NOT NULL
            """), {"min_score": min_score}).fetchone()

        total = row[0] or 0
        avg_score = round(float(row[1] or 0), 2)
        min_s = round(float(row[2] or 0), 2)
        below = row[3] or 0
        status = "pass" if below == 0 else ("warn" if below / max(total, 1) < 0.1 else "fail")

        r = {
            "check": "quality_check",
            "table": label,
            "total": total,
            "avg_score": avg_score,
            "min_score": min_s,
            "below_threshold": below,
            "threshold": min_score,
            "status": status,
        }
        results.append(r)
        log.info("quality_check", **r)
    return {"check": "quality_check", "results": results}


# ---------------------------------------------------------------------------
# 5. NULL CHECK — campos críticos nulos
# ---------------------------------------------------------------------------
def null_check() -> Dict:
    local_engine = get_local_engine()
    results = []

    for table, label in [(_LOCAL_ANNUAL, "annual"), (_LOCAL_QUARTERLY, "quarterly")]:
        with local_engine.connect() as conn:
            total = conn.execute(sa_text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0
            if total == 0:
                results.append({"table": label, "status": "warn", "message": "Tabela vazia"})
                continue
            null_counts = {}
            for col in _CRITICAL_COLS:
                try:
                    count = conn.execute(
                        sa_text(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
                    ).scalar() or 0
                    null_counts[col] = count
                except Exception:
                    null_counts[col] = "N/A"

        high_null = {k: v for k, v in null_counts.items() if isinstance(v, int) and v / total > 0.5}
        status = "fail" if high_null else ("warn" if any(isinstance(v, int) and v > 0 for v in null_counts.values()) else "pass")
        r = {
            "check": "null_check",
            "table": label,
            "total_rows": total,
            "null_counts": null_counts,
            "high_null_cols": list(high_null.keys()),
            "status": status,
        }
        results.append(r)
        log.info("null_check", **r)
    return {"check": "null_check", "results": results}


# ---------------------------------------------------------------------------
# 6. PUBLISH LAG CHECK — linhas locais nunca publicadas
# ---------------------------------------------------------------------------
def publish_lag_check() -> Dict:
    local_engine = get_local_engine()
    results = []

    for table, label in [(_LOCAL_ANNUAL, "annual"), (_LOCAL_QUARTERLY, "quarterly")]:
        with local_engine.connect() as conn:
            row = conn.execute(sa_text(f"""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE published_at IS NULL) AS never_published,
                    MAX(published_at) AS last_published
                FROM {table}
            """)).fetchone()

        total = row[0] or 0
        never = row[1] or 0
        last_pub = str(row[2]) if row[2] else "nunca"
        status = "fail" if total > 0 and never == total else ("warn" if never > 0 else "pass")

        r = {
            "check": "publish_lag_check",
            "table": label,
            "total_rows": total,
            "never_published": never,
            "last_published": last_pub,
            "pct_unpublished": round(never / max(total, 1) * 100, 1),
            "status": status,
        }
        results.append(r)
        log.info("publish_lag_check", **r)
    return {"check": "publish_lag_check", "results": results}


# ---------------------------------------------------------------------------
# Orquestrador de checks
# ---------------------------------------------------------------------------
_ALL_CHECKS = {
    "count_check": lambda _: count_check(),
    "sample_check": lambda args: sample_check(args.get("ticker", ""), args.get("year")),
    "coverage_check": lambda _: coverage_check(),
    "quality_check": lambda _: quality_check(),
    "null_check": lambda _: null_check(),
    "publish_lag_check": lambda _: publish_lag_check(),
}


def run_all(ticker: Optional[str] = None, year: Optional[int] = None) -> Dict:
    run_id = str(uuid.uuid4())
    log.info("Iniciando auditoria completa", run_id=run_id)
    args = {"ticker": ticker, "year": year}
    all_results = {}
    overall_status = "pass"

    for name, fn in _ALL_CHECKS.items():
        try:
            result = fn(args)
            all_results[name] = result
            # Agrega status
            nested = result.get("results", [result])
            for r in nested:
                if isinstance(r, dict) and r.get("status") == "fail":
                    overall_status = "fail"
                elif isinstance(r, dict) and r.get("status") == "warn" and overall_status == "pass":
                    overall_status = "warn"
        except Exception as exc:
            log.error("Check falhou", check=name, erro=str(exc))
            all_results[name] = {"error": str(exc), "status": "fail"}
            overall_status = "fail"

    log.summary(
        pipeline="pipeline_audit",
        status=overall_status,
        run_id=run_id,
        checks_run=len(_ALL_CHECKS),
    )
    return {"run_id": run_id, "overall_status": overall_status, "results": all_results}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline local audit tool")
    parser.add_argument("--check", default="all", choices=list(_ALL_CHECKS.keys()) + ["all"])
    parser.add_argument("--ticker", default=None)
    parser.add_argument("--year", type=int, default=None)
    args = parser.parse_args()

    if args.check == "all":
        result = run_all(ticker=args.ticker, year=args.year)
    else:
        fn = _ALL_CHECKS[args.check]
        result = fn({"ticker": args.ticker, "year": args.year})

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
