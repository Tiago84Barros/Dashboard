"""
diagnostico_tickers.py
======================
Diagnóstico de consistência de tickers nas tabelas críticas do banco.

Uso:
    python diagnostico_tickers.py

Requer SUPABASE_DB_URL (ou DATABASE_URL) no ambiente.
"""

from __future__ import annotations

import os
import sys

# ── Carrega .env se existir ──────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Tenta Streamlit Secrets se não houver env var ────────────
db_url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
if not db_url:
    import pathlib
    secrets = pathlib.Path.home() / ".streamlit" / "secrets.toml"
    if secrets.exists():
        for line in secrets.read_text().splitlines():
            if "SUPABASE_DB_URL" in line or "DATABASE_URL" in line:
                db_url = line.split("=", 1)[1].strip().strip('"').strip("'")
                break

if not db_url:
    print("ERRO: SUPABASE_DB_URL não encontrada.")
    print("Export a variável antes de rodar: export SUPABASE_DB_URL='postgresql://...'")
    sys.exit(1)

from sqlalchemy import create_engine, text

engine = create_engine(db_url, pool_pre_ping=True)

SEPARATOR = "=" * 70


def run(sql: str, params: dict | None = None) -> list[dict]:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


def section(title: str) -> None:
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


# ════════════════════════════════════════════════════════════════
#  1. docs_corporativos
# ════════════════════════════════════════════════════════════════
section("TABELA: public.docs_corporativos")

rows = run("""
    SELECT
        count(*)                                         AS total,
        count(*) FILTER (WHERE ticker LIKE '%.SA')      AS com_sa,
        count(*) FILTER (WHERE ticker NOT LIKE '%.SA')  AS sem_sa
    FROM public.docs_corporativos
""")
r = rows[0]
print(f"  Total de registros : {r['total']}")
print(f"  COM    .SA         : {r['com_sa']}")
print(f"  SEM    .SA         : {r['sem_sa']}")

# Tickers com duplicata (ambas as formas)
dups = run("""
    SELECT
        replace(ticker, '.SA', '') AS base,
        array_agg(DISTINCT ticker ORDER BY ticker) AS formas,
        count(*)                                    AS docs
    FROM public.docs_corporativos
    GROUP BY replace(ticker, '.SA', '')
    HAVING count(DISTINCT ticker) > 1
       AND bool_or(ticker LIKE '%.SA')
       AND bool_or(ticker NOT LIKE '%.SA')
    ORDER BY docs DESC
    LIMIT 20
""")
if dups:
    print(f"\n  Tickers com AMBAS as formas no banco ({len(dups)} encontrados):")
    for d in dups:
        print(f"    {d['base']:12s}  formas={d['formas']}  docs={d['docs']}")
else:
    print("\n  Sem duplicatas entre formas — OK")

# Exemplos de tickers com .SA
sa_examples = run("""
    SELECT DISTINCT ticker FROM public.docs_corporativos
    WHERE ticker LIKE '%.SA'
    ORDER BY ticker LIMIT 10
""")
if sa_examples:
    print(f"\n  Exemplos de tickers COM .SA:")
    for e in sa_examples:
        print(f"    {e['ticker']}")
else:
    print("\n  Nenhum ticker com .SA nesta tabela")


# ════════════════════════════════════════════════════════════════
#  2. patch6_runs
# ════════════════════════════════════════════════════════════════
section("TABELA: public.patch6_runs")

rows = run("""
    SELECT
        count(*)                                         AS total,
        count(*) FILTER (WHERE ticker LIKE '%.SA')      AS com_sa,
        count(*) FILTER (WHERE ticker NOT LIKE '%.SA')  AS sem_sa
    FROM public.patch6_runs
""")
r = rows[0]
print(f"  Total de registros : {r['total']}")
print(f"  COM    .SA         : {r['com_sa']}")
print(f"  SEM    .SA         : {r['sem_sa']}")

dups = run("""
    SELECT
        replace(ticker, '.SA', '') AS base,
        array_agg(DISTINCT ticker ORDER BY ticker) AS formas,
        array_agg(DISTINCT period_ref ORDER BY period_ref) AS periodos
    FROM public.patch6_runs
    GROUP BY replace(ticker, '.SA', '')
    HAVING count(DISTINCT ticker) > 1
       AND bool_or(ticker LIKE '%.SA')
       AND bool_or(ticker NOT LIKE '%.SA')
    ORDER BY base
    LIMIT 20
""")
if dups:
    print(f"\n  Tickers com AMBAS as formas ({len(dups)} encontrados):")
    for d in dups:
        print(f"    {d['base']:12s}  formas={d['formas']}  periodos={d['periodos']}")
else:
    print("\n  Sem duplicatas entre formas — OK")

sa_examples = run("""
    SELECT DISTINCT ticker FROM public.patch6_runs
    WHERE ticker LIKE '%.SA'
    ORDER BY ticker LIMIT 10
""")
if sa_examples:
    print(f"\n  Exemplos de tickers COM .SA:")
    for e in sa_examples:
        print(f"    {e['ticker']}")
else:
    print("\n  Nenhum ticker com .SA nesta tabela")

# Conflito por (ticker_base, period_ref) — o mais perigoso
conflicts = run("""
    SELECT
        replace(ticker, '.SA', '') AS base,
        period_ref,
        array_agg(ticker ORDER BY ticker) AS variantes,
        count(*)                           AS runs
    FROM public.patch6_runs
    GROUP BY replace(ticker, '.SA', ''), period_ref
    HAVING count(DISTINCT ticker) > 1
    ORDER BY base, period_ref
    LIMIT 20
""")
if conflicts:
    print(f"\n  CONFLITOS por (ticker_base, period_ref) — registros duplicados:")
    for c in conflicts:
        print(f"    base={c['base']:12s}  period={c['period_ref']}  variantes={c['variantes']}")
else:
    print("\n  Sem conflitos por (ticker, period_ref) — OK")


# ════════════════════════════════════════════════════════════════
#  3. portfolio_snapshot_items
# ════════════════════════════════════════════════════════════════
section("TABELA: public.portfolio_snapshot_items")

# Testa se a tabela existe
try:
    rows = run("""
        SELECT
            count(*)                                         AS total,
            count(*) FILTER (WHERE ticker LIKE '%.SA')      AS com_sa,
            count(*) FILTER (WHERE ticker NOT LIKE '%.SA')  AS sem_sa
        FROM public.portfolio_snapshot_items
    """)
    r = rows[0]
    print(f"  Total de registros : {r['total']}")
    print(f"  COM    .SA         : {r['com_sa']}")
    print(f"  SEM    .SA         : {r['sem_sa']}")

    dups = run("""
        SELECT
            replace(ticker, '.SA', '') AS base,
            array_agg(DISTINCT ticker ORDER BY ticker) AS formas,
            count(*)                                    AS snapshots
        FROM public.portfolio_snapshot_items
        GROUP BY replace(ticker, '.SA', '')
        HAVING count(DISTINCT ticker) > 1
           AND bool_or(ticker LIKE '%.SA')
           AND bool_or(ticker NOT LIKE '%.SA')
        ORDER BY snapshots DESC
        LIMIT 20
    """)
    if dups:
        print(f"\n  Tickers com AMBAS as formas ({len(dups)} encontrados):")
        for d in dups:
            print(f"    {d['base']:12s}  formas={d['formas']}  snapshots={d['snapshots']}")
    else:
        print("\n  Sem duplicatas entre formas — OK")

    sa_examples = run("""
        SELECT DISTINCT ticker FROM public.portfolio_snapshot_items
        WHERE ticker LIKE '%.SA'
        ORDER BY ticker LIMIT 10
    """)
    if sa_examples:
        print(f"\n  Exemplos de tickers COM .SA:")
        for e in sa_examples:
            print(f"    {e['ticker']}")
    else:
        print("\n  Nenhum ticker com .SA nesta tabela")

except Exception as e:
    if "does not exist" in str(e) or "relation" in str(e).lower():
        print("  Tabela não encontrada — verificando nome alternativo...")
        # Try to find the actual table name
        tables = run("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename ILIKE '%snapshot%'
            ORDER BY tablename
        """)
        if tables:
            print(f"  Tabelas snapshot encontradas: {[t['tablename'] for t in tables]}")
        else:
            print("  Nenhuma tabela snapshot encontrada")
    else:
        print(f"  Erro: {e}")


# ════════════════════════════════════════════════════════════════
#  4. Cruzamento: ticker_base em docs mas não em patch6_runs (e vice-versa)
# ════════════════════════════════════════════════════════════════
section("CRUZAMENTO: docs_corporativos x patch6_runs")

cross = run("""
    WITH
    docs_tickers AS (
        SELECT DISTINCT replace(ticker, '.SA', '') AS base
        FROM public.docs_corporativos
    ),
    runs_tickers AS (
        SELECT DISTINCT replace(ticker, '.SA', '') AS base
        FROM public.patch6_runs
    )
    SELECT
        'em docs, não em runs' AS situacao,
        count(*) AS qtd
    FROM docs_tickers
    WHERE base NOT IN (SELECT base FROM runs_tickers)
    UNION ALL
    SELECT
        'em runs, não em docs' AS situacao,
        count(*) AS qtd
    FROM runs_tickers
    WHERE base NOT IN (SELECT base FROM docs_tickers)
    UNION ALL
    SELECT
        'em ambos' AS situacao,
        count(*) AS qtd
    FROM docs_tickers
    WHERE base IN (SELECT base FROM runs_tickers)
""")
for c in cross:
    print(f"  {c['situacao']:30s}: {c['qtd']}")


# ════════════════════════════════════════════════════════════════
#  5. Exemplos concretos: PETR4 e outros tickers comuns
# ════════════════════════════════════════════════════════════════
section("EXEMPLOS CONCRETOS: tickers B3 comuns")

EXEMPLOS = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3", "WEGE3", "RENT3", "MGLU3"]

for tk in EXEMPLOS:
    docs_sem = run("SELECT count(*) AS n FROM public.docs_corporativos WHERE ticker = :t", {"t": tk})[0]["n"]
    docs_com = run("SELECT count(*) AS n FROM public.docs_corporativos WHERE ticker = :t", {"t": tk + ".SA"})[0]["n"]
    try:
        runs_sem = run("SELECT count(*) AS n FROM public.patch6_runs WHERE ticker = :t", {"t": tk})[0]["n"]
        runs_com = run("SELECT count(*) AS n FROM public.patch6_runs WHERE ticker = :t", {"t": tk + ".SA"})[0]["n"]
    except Exception:
        runs_sem = runs_com = "?"
    print(f"  {tk:8s}  docs[sem.SA]={docs_sem:4}  docs[com.SA]={docs_com:4}  "
          f"runs[sem.SA]={runs_sem:4}  runs[com.SA]={runs_com:4}")


# ════════════════════════════════════════════════════════════════
#  6. Veredicto final
# ════════════════════════════════════════════════════════════════
section("VEREDICTO")

# Re-check totals
d = run("SELECT count(*) FILTER (WHERE ticker LIKE '%.SA') AS c FROM public.docs_corporativos")[0]["c"]
try:
    r_sa = run("SELECT count(*) FILTER (WHERE ticker LIKE '%.SA') AS c FROM public.patch6_runs")[0]["c"]
    r_sem = run("SELECT count(*) FILTER (WHERE ticker NOT LIKE '%.SA') AS c FROM public.patch6_runs")[0]["c"]
except Exception:
    r_sa = r_sem = 0

print(f"  docs_corporativos com .SA   : {d}")
print(f"  patch6_runs com .SA         : {r_sa}")
print(f"  patch6_runs sem .SA         : {r_sem}")

if int(d) == 0:
    print("\n  docs_corporativos: LIMPA — todos sem .SA ✓")
    print("  → seguro aplicar normalize_ticker na escrita de docs")
else:
    print(f"\n  docs_corporativos: CONTAMINADA — {d} registros com .SA")
    print("  → necessário migrar antes de padronizar código")

if int(r_sa) == 0:
    print("\n  patch6_runs: LIMPA — todos sem .SA ✓")
    print("  → seguro aplicar normalize_ticker na escrita de patch6_runs")
elif int(r_sem) == 0:
    print(f"\n  patch6_runs: TODA com .SA ({r_sa} registros)")
    print("  → seguro migrar tudo para sem .SA e padronizar código")
else:
    print(f"\n  patch6_runs: MISTA — {r_sa} com .SA e {r_sem} sem .SA")
    print("  → necessário migrar antes de padronizar código")
    print("  → UPDATE public.patch6_runs SET ticker = replace(ticker, '.SA', '') WHERE ticker LIKE '%.SA'")

print(f"\n{SEPARATOR}")
print("  Diagnóstico concluído.")
print(SEPARATOR)
