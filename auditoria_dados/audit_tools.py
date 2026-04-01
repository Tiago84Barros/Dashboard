# auditoria_dados/audit_tools.py
#
# Ferramentas de auditoria automatizada da arquitetura de dados.
#
# Uso:
#   python -m auditoria_dados.audit_tools [--all] [--check NOME]
#
# Checks disponíveis:
#   duplicates       -- duplicatas nas tabelas principais
#   period_gaps      -- lacunas temporais por ticker
#   null_fields      -- campos críticos nulos
#   json_completeness -- cobertura de schema em patch6_runs.result_json
#   doc_coverage     -- tickers sem documentos recentes
#   ticker_sync      -- tickers em DF não presentes em setores
#   macro_coverage   -- cobertura temporal das séries macro
#
# Requer: SUPABASE_DB_URL ou DATABASE_URL no ambiente.
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from functools import lru_cache
from typing import Any, Dict, List

import pandas as pd
from sqlalchemy import create_engine, text


# ────────────────────────────────────────────────────────────────────────────
# Connection
# ────────────────────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def get_engine():
    url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "Defina SUPABASE_DB_URL ou DATABASE_URL no ambiente antes de rodar."
        )
    return create_engine(url, pool_pre_ping=True)


def query(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


# ────────────────────────────────────────────────────────────────────────────
# Result helpers
# ────────────────────────────────────────────────────────────────────────────

class AuditResult:
    def __init__(self, check: str):
        self.check = check
        self.issues: List[Dict[str, Any]] = []
        self.summary: str = ""

    def add(self, severity: str, message: str, detail: Any = None):
        """severity: 'ERROR' | 'WARN' | 'INFO'"""
        self.issues.append({"severity": severity, "message": message, "detail": detail})

    def ok(self, message: str):
        self.summary = f"[OK] {message}"

    def print_report(self):
        print(f"\n{'='*60}")
        print(f"  CHECK: {self.check}")
        print(f"{'='*60}")
        if self.summary:
            print(f"  {self.summary}")
        for issue in self.issues:
            prefix = {"ERROR": "  [ERR] ", "WARN": "  [WARN]", "INFO": "  [INFO]"}.get(
                issue["severity"], "  [?]   "
            )
            print(f"{prefix} {issue['message']}")
            if issue["detail"] is not None and not isinstance(issue["detail"], pd.DataFrame):
                print(f"         {issue['detail']}")
            elif isinstance(issue["detail"], pd.DataFrame) and not issue["detail"].empty:
                print(issue["detail"].to_string(index=False))
        if not self.issues:
            print("  Nenhum problema encontrado.")


# ────────────────────────────────────────────────────────────────────────────
# CHECK 1: Duplicatas
# ────────────────────────────────────────────────────────────────────────────

def check_duplicates() -> AuditResult:
    result = AuditResult("duplicates")

    checks = [
        (
            "Demonstracoes_Financeiras",
            'SELECT "Ticker", "Data", COUNT(*) as cnt '
            'FROM public."Demonstracoes_Financeiras" '
            'GROUP BY "Ticker", "Data" HAVING COUNT(*) > 1 LIMIT 20',
        ),
        (
            "Demonstracoes_Financeiras_TRI",
            'SELECT "Ticker", "Data", COUNT(*) as cnt '
            'FROM public."Demonstracoes_Financeiras_TRI" '
            'GROUP BY "Ticker", "Data" HAVING COUNT(*) > 1 LIMIT 20',
        ),
        (
            "multiplos",
            'SELECT "Ticker", "Data", COUNT(*) as cnt '
            'FROM public.multiplos '
            'GROUP BY "Ticker", "Data" HAVING COUNT(*) > 1 LIMIT 20',
        ),
        (
            "multiplos_TRI",
            'SELECT "Ticker", "Data", COUNT(*) as cnt '
            'FROM public."multiplos_TRI" '
            'GROUP BY "Ticker", "Data" HAVING COUNT(*) > 1 LIMIT 20',
        ),
        (
            "docs_corporativos (por doc_hash)",
            "SELECT doc_hash, COUNT(*) as cnt "
            "FROM public.docs_corporativos "
            "GROUP BY doc_hash HAVING COUNT(*) > 1 LIMIT 20",
        ),
        (
            "patch6_runs (por snapshot_id, ticker, period_ref)",
            "SELECT snapshot_id, ticker, period_ref, COUNT(*) as cnt "
            "FROM public.patch6_runs "
            "GROUP BY snapshot_id, ticker, period_ref HAVING COUNT(*) > 1 LIMIT 20",
        ),
    ]

    any_found = False
    for table, sql in checks:
        try:
            df = query(sql)
            if not df.empty:
                any_found = True
                result.add("ERROR", f"Duplicatas em {table} ({len(df)} grupos)", df)
        except Exception as e:
            result.add("WARN", f"Erro ao checar {table}: {e}")

    if not any_found:
        result.ok("Nenhuma duplicata encontrada nas tabelas principais.")
    return result


# ────────────────────────────────────────────────────────────────────────────
# CHECK 2: Lacunas temporais
# ────────────────────────────────────────────────────────────────────────────

def check_period_gaps(min_years: int = 3) -> AuditResult:
    """
    Para cada ticker em Demonstracoes_Financeiras, verifica se há lacunas
    de mais de 18 meses entre datas consecutivas.
    """
    result = AuditResult("period_gaps")

    sql = """
        SELECT
            "Ticker",
            array_agg("Data" ORDER BY "Data") as datas
        FROM public."Demonstracoes_Financeiras"
        GROUP BY "Ticker"
        HAVING COUNT(*) >= :min_years
    """
    try:
        df = query(sql, {"min_years": min_years})
    except Exception as e:
        result.add("ERROR", f"Falha ao carregar dados: {e}")
        return result

    gaps_found = []
    for _, row in df.iterrows():
        ticker = row["Ticker"]
        dates = sorted(pd.to_datetime(row["datas"]))
        for i in range(1, len(dates)):
            delta_days = (dates[i] - dates[i - 1]).days
            if delta_days > 540:  # >18 meses
                gaps_found.append({
                    "Ticker": ticker,
                    "De": dates[i - 1].date(),
                    "Até": dates[i].date(),
                    "Dias": delta_days,
                })

    if gaps_found:
        result.add(
            "WARN",
            f"{len(gaps_found)} lacuna(s) temporal(is) > 18 meses em Demonstracoes_Financeiras",
            pd.DataFrame(gaps_found).head(30),
        )
    else:
        result.ok(f"Sem lacunas > 18 meses em Demonstracoes_Financeiras (analisados ≥{min_years} anos).")

    # Quarterly: lacunas > 5 trimestres
    sql_tri = """
        SELECT
            "Ticker",
            array_agg("Data" ORDER BY "Data") as datas
        FROM public."Demonstracoes_Financeiras_TRI"
        GROUP BY "Ticker"
        HAVING COUNT(*) >= 4
    """
    try:
        df_tri = query(sql_tri)
        gaps_tri = []
        for _, row in df_tri.iterrows():
            ticker = row["Ticker"]
            dates = sorted(pd.to_datetime(row["datas"]))
            for i in range(1, len(dates)):
                delta_days = (dates[i] - dates[i - 1]).days
                if delta_days > 150:  # >5 meses entre trimestres
                    gaps_tri.append({
                        "Ticker": ticker,
                        "De": dates[i - 1].date(),
                        "Até": dates[i].date(),
                        "Dias": delta_days,
                    })
        if gaps_tri:
            result.add(
                "WARN",
                f"{len(gaps_tri)} lacuna(s) trimestral(is) > 5 meses em Demonstracoes_Financeiras_TRI",
                pd.DataFrame(gaps_tri).head(30),
            )
    except Exception as e:
        result.add("WARN", f"Erro ao checar TRI gaps: {e}")

    return result


# ────────────────────────────────────────────────────────────────────────────
# CHECK 3: Campos nulos críticos
# ────────────────────────────────────────────────────────────────────────────

def check_null_fields() -> AuditResult:
    result = AuditResult("null_fields")

    checks = [
        (
            "docs_corporativos.raw_text NULL",
            "SELECT COUNT(*) as cnt FROM public.docs_corporativos WHERE raw_text IS NULL OR LENGTH(raw_text) < 50",
            "WARN",
            "Documentos sem texto extraído (raw_text NULL ou < 50 chars)",
        ),
        (
            "docs_corporativos.ticker NULL",
            "SELECT COUNT(*) as cnt FROM public.docs_corporativos WHERE ticker IS NULL",
            "ERROR",
            "Documentos sem ticker associado",
        ),
        (
            "docs_corporativos.doc_hash NULL",
            "SELECT COUNT(*) as cnt FROM public.docs_corporativos WHERE doc_hash IS NULL",
            "ERROR",
            "Documentos sem doc_hash (deduplicação comprometida)",
        ),
        (
            "multiplos: tickers sem Preco",
            'SELECT COUNT(DISTINCT "Ticker") as cnt FROM public.multiplos WHERE "Preco" IS NULL',
            "WARN",
            "Tickers com preço nulo em multiplos (yfinance falhou)",
        ),
        (
            "patch6_runs.result_json NULL",
            "SELECT COUNT(*) as cnt FROM public.patch6_runs WHERE result_json IS NULL",
            "ERROR",
            "Runs do Patch6 sem result_json",
        ),
        (
            "setores: tickers sem setor",
            'SELECT COUNT(*) as cnt FROM public.setores WHERE "SETOR" IS NULL OR "SETOR" = \'\'',
            "WARN",
            "Empresas sem classificação setorial",
        ),
    ]

    any_issue = False
    for label, sql, sev, msg in checks:
        try:
            df = query(sql)
            cnt = int(df["cnt"].iloc[0])
            if cnt > 0:
                any_issue = True
                result.add(sev, f"{msg}: {cnt} registro(s)")
        except Exception as e:
            result.add("WARN", f"Erro ao checar '{label}': {e}")

    if not any_issue:
        result.ok("Nenhum campo crítico nulo encontrado.")
    return result


# ────────────────────────────────────────────────────────────────────────────
# CHECK 4: Completude de JSON no patch6_runs
# ────────────────────────────────────────────────────────────────────────────

PATCH6_REQUIRED_FIELDS = [
    "perspectiva_compra",
    "score_qualitativo",
    "confianca_analise",
    "tese_sintese",
    "riscos",
    "catalisadores",
]

PATCH6_RECOMMENDED_FIELDS = [
    "leitura_fundamentalista",
    "consideracoes_finais",
    "evolucao_financeira",
    "consistencia_historica",
    "execucao_gestao",
    "qualidade_narrativa",
]


def check_json_completeness(limit: int = 500) -> AuditResult:
    result = AuditResult("json_completeness")

    sql = f"""
        SELECT ticker, period_ref, result_json, created_at
        FROM public.patch6_runs
        ORDER BY created_at DESC
        LIMIT {limit}
    """
    try:
        df = query(sql)
    except Exception as e:
        result.add("ERROR", f"Falha ao carregar patch6_runs: {e}")
        return result

    if df.empty:
        result.add("INFO", "Nenhum registro em patch6_runs.")
        return result

    missing_required: List[dict] = []
    missing_recommended: List[dict] = []
    invalid_json: List[dict] = []
    schema_versions: Dict[str, int] = {}

    for _, row in df.iterrows():
        rj = row["result_json"]
        if rj is None:
            invalid_json.append({"ticker": row["ticker"], "period_ref": row["period_ref"], "motivo": "NULL"})
            continue
        try:
            obj = json.loads(rj) if isinstance(rj, str) else rj
        except Exception:
            invalid_json.append({"ticker": row["ticker"], "period_ref": row["period_ref"], "motivo": "JSON inválido"})
            continue

        version = obj.get("schema_version", "v1")
        schema_versions[version] = schema_versions.get(version, 0) + 1

        missing_req = [f for f in PATCH6_REQUIRED_FIELDS if f not in obj or obj[f] is None]
        if missing_req:
            missing_required.append({
                "ticker": row["ticker"],
                "period_ref": row["period_ref"],
                "campos": ", ".join(missing_req),
            })

        missing_rec = [f for f in PATCH6_RECOMMENDED_FIELDS if f not in obj or obj[f] is None]
        if len(missing_rec) > 3:
            missing_recommended.append({
                "ticker": row["ticker"],
                "period_ref": row["period_ref"],
                "ausentes": len(missing_rec),
            })

    result.add("INFO", f"Distribuição de schema_version: {schema_versions}")

    if invalid_json:
        result.add("ERROR", f"{len(invalid_json)} result_json inválido(s) ou NULL", pd.DataFrame(invalid_json))
    if missing_required:
        result.add(
            "ERROR",
            f"{len(missing_required)} run(s) com campos obrigatórios ausentes",
            pd.DataFrame(missing_required).head(20),
        )
    if missing_recommended:
        result.add(
            "WARN",
            f"{len(missing_recommended)} run(s) com >3 campos recomendados ausentes",
            pd.DataFrame(missing_recommended).head(20),
        )

    if not invalid_json and not missing_required:
        result.ok(f"{len(df)} runs analisadas — campos obrigatórios OK.")

    return result


# ────────────────────────────────────────────────────────────────────────────
# CHECK 5: Cobertura de documentos por ticker
# ────────────────────────────────────────────────────────────────────────────

def check_doc_coverage(days_back: int = 180) -> AuditResult:
    result = AuditResult("doc_coverage")

    # Tickers com análise recente no Patch6
    sql_tickers = """
        SELECT DISTINCT ticker
        FROM public.patch6_runs
        WHERE created_at >= NOW() - INTERVAL '90 days'
    """
    try:
        df_tickers = query(sql_tickers)
    except Exception as e:
        result.add("ERROR", f"Falha ao listar tickers do Patch6: {e}")
        return result

    if df_tickers.empty:
        result.add("INFO", "Nenhum ticker com análise Patch6 nos últimos 90 dias.")
        return result

    tickers = df_tickers["ticker"].tolist()

    sql_docs = f"""
        SELECT ticker, COUNT(*) as n_docs, MAX(data) as ultimo_doc
        FROM public.docs_corporativos
        WHERE ticker = ANY(:tks)
          AND (data IS NULL OR data >= CURRENT_DATE - INTERVAL '{days_back} days')
        GROUP BY ticker
    """
    try:
        df_docs = query(sql_docs, {"tks": tickers})
    except Exception as e:
        result.add("ERROR", f"Falha ao carregar docs_corporativos: {e}")
        return result

    docs_by_ticker = dict(zip(df_docs["ticker"], df_docs["n_docs"]))
    sem_docs = [tk for tk in tickers if docs_by_ticker.get(tk, 0) == 0]
    poucos_docs = [tk for tk in tickers if 0 < docs_by_ticker.get(tk, 0) < 3]

    if sem_docs:
        result.add(
            "ERROR",
            f"{len(sem_docs)} ticker(s) com análise Patch6 mas SEM documentos nos últimos {days_back} dias",
            sem_docs,
        )
    if poucos_docs:
        result.add(
            "WARN",
            f"{len(poucos_docs)} ticker(s) com menos de 3 documentos nos últimos {days_back} dias",
            poucos_docs,
        )
    if not sem_docs and not poucos_docs:
        result.ok(f"Todos os {len(tickers)} tickers ativos têm ≥3 documentos recentes.")

    return result


# ────────────────────────────────────────────────────────────────────────────
# CHECK 6: Sincronização de tickers entre tabelas
# ────────────────────────────────────────────────────────────────────────────

def check_ticker_sync() -> AuditResult:
    result = AuditResult("ticker_sync")

    sql_df_tickers = 'SELECT DISTINCT "Ticker" FROM public."Demonstracoes_Financeiras"'
    sql_setores = "SELECT DISTINCT ticker FROM public.setores"
    sql_multiplos = 'SELECT DISTINCT "Ticker" FROM public.multiplos'

    try:
        df_tickers = set(query(sql_df_tickers)["Ticker"].str.upper())
        setores_tickers = set(query(sql_setores)["ticker"].str.upper())
        multiplos_tickers = set(query(sql_multiplos)["Ticker"].str.upper())
    except Exception as e:
        result.add("ERROR", f"Falha ao carregar tickers: {e}")
        return result

    # DF mas sem setor
    no_setor = df_tickers - setores_tickers
    if no_setor:
        result.add(
            "WARN",
            f"{len(no_setor)} ticker(s) em Demonstracoes_Financeiras sem classificação em setores",
            sorted(no_setor)[:30],
        )

    # DF mas sem múltiplos
    no_multiplos = df_tickers - multiplos_tickers
    if no_multiplos:
        result.add(
            "WARN",
            f"{len(no_multiplos)} ticker(s) em Demonstracoes_Financeiras sem dados em multiplos",
            sorted(no_multiplos)[:30],
        )

    # Múltiplos mas sem DF
    multiplos_orphan = multiplos_tickers - df_tickers
    if multiplos_orphan:
        result.add(
            "WARN",
            f"{len(multiplos_orphan)} ticker(s) em multiplos sem Demonstracao_Financeira (dados orfãos?)",
            sorted(multiplos_orphan)[:20],
        )

    if not no_setor and not no_multiplos and not multiplos_orphan:
        result.ok(f"Tickers consistentes entre Demonstracoes_Financeiras, setores e multiplos.")

    return result


# ────────────────────────────────────────────────────────────────────────────
# CHECK 7: Cobertura das séries macro
# ────────────────────────────────────────────────────────────────────────────

def check_macro_coverage() -> AuditResult:
    result = AuditResult("macro_coverage")

    expected_series_annual = ["selic", "ipca", "cambio", "pib", "divida_publica"]
    expected_series_monthly = ["selic", "ipca", "cambio"]

    sql_annual = "SELECT * FROM public.info_economica ORDER BY data DESC LIMIT 1"
    sql_monthly = "SELECT data FROM public.info_economica_mensal ORDER BY data DESC LIMIT 1"

    try:
        df_annual = query(sql_annual)
        if df_annual.empty:
            result.add("ERROR", "info_economica está vazia")
        else:
            latest = df_annual.iloc[0]
            latest_date = pd.to_datetime(latest["data"])
            years_ago = (pd.Timestamp.now() - latest_date).days / 365
            if years_ago > 1.5:
                result.add("WARN", f"info_economica: último registro em {latest_date.date()} (>{years_ago:.1f} anos atrás)")
            else:
                result.add("INFO", f"info_economica: último registro em {latest_date.date()}")

            missing_cols = [s for s in expected_series_annual if s not in df_annual.columns]
            if missing_cols:
                result.add("ERROR", f"Colunas macro ausentes em info_economica: {missing_cols}")
    except Exception as e:
        result.add("ERROR", f"Erro ao checar info_economica: {e}")

    try:
        df_monthly = query(sql_monthly)
        if df_monthly.empty:
            result.add("WARN", "info_economica_mensal está vazia")
        else:
            latest_m = pd.to_datetime(df_monthly["data"].iloc[0])
            months_ago = (pd.Timestamp.now() - latest_m).days / 30
            if months_ago > 3:
                result.add("WARN", f"info_economica_mensal: último registro em {latest_m.date()} ({months_ago:.1f} meses atrás)")
            else:
                result.add("INFO", f"info_economica_mensal: último registro em {latest_m.date()}")
    except Exception as e:
        result.add("WARN", f"Erro ao checar info_economica_mensal: {e}")

    return result


# ────────────────────────────────────────────────────────────────────────────
# CLI runner
# ────────────────────────────────────────────────────────────────────────────

ALL_CHECKS = {
    "duplicates": check_duplicates,
    "period_gaps": check_period_gaps,
    "null_fields": check_null_fields,
    "json_completeness": check_json_completeness,
    "doc_coverage": check_doc_coverage,
    "ticker_sync": check_ticker_sync,
    "macro_coverage": check_macro_coverage,
}


def main():
    parser = argparse.ArgumentParser(description="Auditoria da arquitetura de dados")
    parser.add_argument("--all", action="store_true", help="Rodar todos os checks")
    parser.add_argument(
        "--check",
        choices=list(ALL_CHECKS.keys()),
        help="Rodar apenas o check especificado",
    )
    args = parser.parse_args()

    if not args.all and not args.check:
        parser.print_help()
        sys.exit(1)

    checks_to_run = list(ALL_CHECKS.keys()) if args.all else [args.check]

    print(f"\nAuditoria de Dados — {date.today()}")
    print(f"Checks selecionados: {', '.join(checks_to_run)}\n")

    results = []
    for name in checks_to_run:
        print(f"Rodando: {name}...", end=" ", flush=True)
        r = ALL_CHECKS[name]()
        print("OK")
        results.append(r)

    # Print all reports
    for r in results:
        r.print_report()

    # Summary
    total_errors = sum(
        1 for r in results for i in r.issues if i["severity"] == "ERROR"
    )
    total_warns = sum(
        1 for r in results for i in r.issues if i["severity"] == "WARN"
    )
    print(f"\n{'='*60}")
    print(f"  RESUMO: {total_errors} erro(s), {total_warns} aviso(s) em {len(results)} check(s)")
    print(f"{'='*60}\n")

    sys.exit(1 if total_errors > 0 else 0)


if __name__ == "__main__":
    main()
