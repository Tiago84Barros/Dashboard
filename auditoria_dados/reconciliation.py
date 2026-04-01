# auditoria_dados/reconciliation.py
#
# Reconciliação entre o que deveria estar no banco e o que realmente está.
# Compara: esperado → banco → uso pelo app.
#
# Uso:
#   python -m auditoria_dados.reconciliation [--check NOME] [--all]
#
# Checks:
#   dfp_vs_tri        -- DF anual vs TRI trimestral: tickers e períodos em comum
#   df_vs_multiplos   -- Demonstracoes_Financeiras vs multiplos: cobertura
#   docs_vs_chunks    -- docs_corporativos vs docs_corporativos_chunks
#   patch6_vs_docs    -- patch6_runs vs docs disponíveis para RAG
#   ticker_canonical  -- normalização de tickers entre todas as tabelas
#   multiplos_preco   -- tickers sem preço em multiplos (yfinance gap)
#
from __future__ import annotations

import argparse
import os
import sys
from functools import lru_cache
from typing import Dict, List, Set

import pandas as pd
from sqlalchemy import create_engine, text


@lru_cache(maxsize=1)
def get_engine():
    url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("Defina SUPABASE_DB_URL ou DATABASE_URL.")
    return create_engine(url, pool_pre_ping=True)


def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


def header(title: str) -> None:
    print(f"\n{'─'*68}")
    print(f"  {title}")
    print(f"{'─'*68}")


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 1: DFP vs TRI — coerência de universo de tickers
# ─────────────────────────────────────────────────────────────────────────────

def check_dfp_vs_tri() -> None:
    header("RECONCILIAÇÃO: DFP anual vs. TRI trimestral")

    df_tickers = set(q('SELECT DISTINCT UPPER("Ticker") as t FROM public."Demonstracoes_Financeiras"')["t"])
    tri_tickers = set(q('SELECT DISTINCT UPPER("Ticker") as t FROM public."Demonstracoes_Financeiras_TRI"')["t"])

    only_dfp = df_tickers - tri_tickers
    only_tri = tri_tickers - df_tickers
    both = df_tickers & tri_tickers

    print(f"  DFP tickers:        {len(df_tickers)}")
    print(f"  TRI tickers:        {len(tri_tickers)}")
    print(f"  Presentes em ambos: {len(both)}")

    if only_dfp:
        print(f"\n  [ALERTA] {len(only_dfp)} tickers em DFP mas NÃO no TRI:")
        print(f"    {sorted(only_dfp)[:20]}")
        print(f"    → Esses tickers têm histórico anual mas sem trimestral. "
              f"App pode usar DFP sem saber que TRI está ausente.")

    if only_tri:
        print(f"\n  [ALERTA] {len(only_tri)} tickers no TRI mas NÃO no DFP:")
        print(f"    {sorted(only_tri)[:20]}")
        print(f"    → TRI foi ingerido sem confirmação na base anual.")

    # Verificar coerência temporal: último DFP vs último TRI
    df_ultimos = q("""
        SELECT "Ticker", MAX("Data") as ultimo_dfp
        FROM public."Demonstracoes_Financeiras"
        GROUP BY "Ticker"
    """)
    tri_ultimos = q("""
        SELECT "Ticker", MAX("Data") as ultimo_tri
        FROM public."Demonstracoes_Financeiras_TRI"
        GROUP BY "Ticker"
    """)

    df_merged = df_ultimos.merge(tri_ultimos, on="Ticker", how="inner")
    df_merged["ultimo_dfp"] = pd.to_datetime(df_merged["ultimo_dfp"])
    df_merged["ultimo_tri"] = pd.to_datetime(df_merged["ultimo_tri"])
    df_merged["tri_mais_novo"] = df_merged["ultimo_tri"] > df_merged["ultimo_dfp"]

    tri_mais_novo = df_merged[df_merged["tri_mais_novo"]]
    tri_mais_antigo = df_merged[~df_merged["tri_mais_novo"]]

    print(f"\n  Tickers onde TRI é mais recente que DFP: {len(tri_mais_novo)} — esperado (ITR sai antes de DFP)")
    if len(tri_mais_antigo) > len(df_merged) * 0.3:
        print(f"  [ALERTA] {len(tri_mais_antigo)} tickers onde DFP é mais recente que TRI — "
              f"TRI pode estar desatualizado.")


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 2: DF vs Múltiplos — gap de cobertura
# ─────────────────────────────────────────────────────────────────────────────

def check_df_vs_multiplos() -> None:
    header("RECONCILIAÇÃO: Demonstracoes_Financeiras vs. multiplos")

    df_pairs = q("""
        SELECT "Ticker", "Data"::date as data
        FROM public."Demonstracoes_Financeiras"
    """)
    m_pairs = q("""
        SELECT "Ticker", "Data"::date as data
        FROM public.multiplos
    """)

    df_set = set(zip(df_pairs["Ticker"], df_pairs["data"]))
    m_set = set(zip(m_pairs["Ticker"], m_pairs["data"]))

    only_df = df_set - m_set
    only_m = m_set - df_set

    print(f"  Pares (Ticker, Ano) em DF:      {len(df_set)}")
    print(f"  Pares (Ticker, Ano) em múltiplos: {len(m_set)}")
    print(f"  Pares em DF mas sem múltiplos:  {len(only_df)}")
    print(f"  Pares em múltiplos sem DF:      {len(only_m)}")

    if only_df:
        pct_gap = round(100 * len(only_df) / len(df_set), 1)
        print(f"\n  [{'CRÍTICO' if pct_gap > 10 else 'ALERTA'}] {pct_gap}% dos registros de DF sem múltiplos calculados.")
        # Agrupar por ticker para facilitar leitura
        tickers_gap = {t for t, _ in only_df}
        print(f"  Tickers sem múltiplos em pelo menos um período: {len(tickers_gap)}")
        print(f"    ex: {sorted(tickers_gap)[:10]}")

        # Verificar se gap é nos anos mais recentes (pior cenário)
        anos_gap = sorted(set(d for _, d in only_df), reverse=True)
        if anos_gap:
            print(f"  Anos com gap (mais recentes): {[str(a) for a in anos_gap[:5]]}")

    if only_m:
        print(f"\n  [ALERTA] {len(only_m)} registros em múltiplos sem correspondente em DF — dados orfãos.")

    # Verificar zeros em campos de preço (yfinance gap)
    try:
        df_preco_zero = q("""
            SELECT
                EXTRACT(YEAR FROM "Data") as ano,
                COUNT(*) as total,
                SUM(CASE WHEN "P/L" = 0 AND "P/VP" = 0 AND "DY" = 0 THEN 1 ELSE 0 END) as sem_preco
            FROM public.multiplos
            GROUP BY ano
            ORDER BY ano DESC
        """)
        print("\n  Zeros em P/L + P/VP + DY por ano (indicativo de preço ausente):")
        for _, row in df_preco_zero.head(10).iterrows():
            pct_s = round(100 * row["sem_preco"] / max(row["total"], 1), 1)
            flag = "⚠ CRÍTICO" if pct_s > 30 else ("⚠" if pct_s > 10 else "  OK")
            print(f"    {int(row['ano'])}: {int(row['sem_preco'])}/{int(row['total'])} ({pct_s}%) {flag}")
    except Exception as e:
        print(f"  [WARN] Não foi possível verificar zeros por ano: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 3: docs_corporativos vs chunks
# ─────────────────────────────────────────────────────────────────────────────

def check_docs_vs_chunks() -> None:
    header("RECONCILIAÇÃO: docs_corporativos vs. docs_corporativos_chunks")

    try:
        df = q("""
            SELECT
                d.id,
                d.ticker,
                d.fonte,
                LENGTH(COALESCE(d.raw_text, '')) AS chars,
                COUNT(c.id) AS n_chunks
            FROM public.docs_corporativos d
            LEFT JOIN public.docs_corporativos_chunks c ON c.doc_id = d.id
            GROUP BY d.id, d.ticker, d.fonte, chars
        """)
    except Exception as e:
        print(f"  [ERRO] {e}")
        return

    total_docs = len(df)
    docs_com_texto = df[df["chars"] > 300]
    docs_sem_texto = df[df["chars"] <= 300]
    docs_com_texto_sem_chunk = docs_com_texto[docs_com_texto["n_chunks"] == 0]
    docs_com_chunks = df[df["n_chunks"] > 0]

    print(f"  Total de documentos:               {total_docs}")
    print(f"  Com texto válido (>300 chars):     {len(docs_com_texto)}")
    print(f"  Sem texto útil (≤300 chars):       {len(docs_sem_texto)}")
    print(f"  Com texto mas SEM chunks:          {len(docs_com_texto_sem_chunk)}")
    print(f"  Com chunks:                        {len(docs_com_chunks)}")

    if docs_com_texto_sem_chunk.shape[0] > 0:
        pct_orphan = round(100 * len(docs_com_texto_sem_chunk) / max(len(docs_com_texto), 1), 1)
        print(f"\n  [CRÍTICO] {pct_orphan}% dos docs com texto não têm chunks.")
        print(f"  Fontes afetadas: {docs_com_texto_sem_chunk['fonte'].value_counts().to_dict()}")
        print(f"  → RAG via chunks encontra apenas {len(docs_com_chunks)}/{len(docs_com_texto)} docs com texto.")

    # Média de chunks por doc (com chunks)
    if not docs_com_chunks.empty:
        media_chunks = docs_com_chunks["n_chunks"].mean()
        print(f"\n  Média de chunks por documento: {media_chunks:.1f}")
        docs_poucos_chunks = docs_com_chunks[docs_com_chunks["n_chunks"] == 1]
        if not docs_poucos_chunks.empty:
            print(f"  Documentos com apenas 1 chunk: {len(docs_poucos_chunks)} — possível truncamento.")

    # Por fonte
    print("\n  Resumo por fonte:")
    by_fonte = df.groupby("fonte").agg(
        total=("id", "count"),
        com_texto=("chars", lambda x: (x > 300).sum()),
        com_chunks=("n_chunks", lambda x: (x > 0).sum()),
    ).reset_index()
    for _, row in by_fonte.iterrows():
        pct_texto = round(100 * row["com_texto"] / max(row["total"], 1), 1)
        pct_chunks = round(100 * row["com_chunks"] / max(row["total"], 1), 1)
        print(f"    {str(row['fonte']):<20}: {int(row['total'])} docs | {pct_texto}% com texto | {pct_chunks}% com chunks")


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 4: Patch6 vs Docs disponíveis
# ─────────────────────────────────────────────────────────────────────────────

def check_patch6_vs_docs() -> None:
    header("RECONCILIAÇÃO: patch6_runs vs. docs disponíveis para RAG")

    try:
        df_p6 = q("""
            SELECT DISTINCT ticker, period_ref,
                   MAX(created_at) as ultima_analise
            FROM public.patch6_runs
            GROUP BY ticker, period_ref
            ORDER BY ultima_analise DESC
            LIMIT 200
        """)
    except Exception as e:
        print(f"  [ERRO] {e}")
        return

    if df_p6.empty:
        print("  Nenhum run em patch6_runs.")
        return

    tickers_p6 = df_p6["ticker"].unique().tolist()
    print(f"  Tickers analisados pelo Patch6 (recentes): {len(tickers_p6)}")

    # Docs disponíveis por ticker
    try:
        df_docs = q("""
            SELECT ticker, COUNT(*) as n_docs,
                   SUM(CASE WHEN LENGTH(COALESCE(raw_text,'')) > 300 THEN 1 ELSE 0 END) as docs_uteis,
                   MAX(data) as ultimo_doc
            FROM public.docs_corporativos
            WHERE ticker = ANY(:tks)
            GROUP BY ticker
        """, {"tks": tickers_p6})
    except Exception as e:
        print(f"  [ERRO] {e}")
        return

    docs_by_ticker = {r["ticker"]: r for _, r in df_docs.iterrows()}

    sem_docs = []
    poucos_docs = []
    docs_velhos = []

    for ticker in tickers_p6:
        info = docs_by_ticker.get(ticker)
        if info is None:
            sem_docs.append(ticker)
        else:
            if int(info["docs_uteis"]) < 2:
                poucos_docs.append((ticker, int(info["docs_uteis"])))
            if info["ultimo_doc"] is not None:
                import datetime
                d = pd.to_datetime(info["ultimo_doc"])
                if (pd.Timestamp.now() - d).days > 365:
                    docs_velhos.append((ticker, str(d.date())))

    print(f"\n  Tickers sem nenhum documento:        {len(sem_docs)}")
    if sem_docs:
        print(f"    {sem_docs[:15]}")
        print(f"    → Patch6 analisa esses tickers SEM EVIDÊNCIAS documentais.")

    print(f"  Tickers com <2 docs úteis:           {len(poucos_docs)}")
    if poucos_docs:
        print(f"    {poucos_docs[:10]}")

    print(f"  Tickers com último doc >1 ano:       {len(docs_velhos)}")
    if docs_velhos:
        print(f"    {docs_velhos[:10]}")

    # Chunks disponíveis
    try:
        df_chunks = q("""
            SELECT d.ticker, COUNT(c.id) as n_chunks
            FROM public.docs_corporativos d
            LEFT JOIN public.docs_corporativos_chunks c ON c.doc_id = d.id
            WHERE d.ticker = ANY(:tks)
            GROUP BY d.ticker
        """, {"tks": tickers_p6})
        sem_chunks = df_chunks[df_chunks["n_chunks"] == 0]["ticker"].tolist()
        if sem_chunks:
            print(f"\n  Tickers com docs mas SEM CHUNKS: {len(sem_chunks)}")
            print(f"    {sem_chunks[:10]}")
            print(f"    → RAG via chunks retorna vazio para esses tickers.")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 5: Normalização de tickers
# ─────────────────────────────────────────────────────────────────────────────

def check_ticker_canonical() -> None:
    header("RECONCILIAÇÃO: normalização de tickers entre tabelas")

    tables = {
        "Demonstracoes_Financeiras": 'SELECT DISTINCT "Ticker" as t FROM public."Demonstracoes_Financeiras"',
        "Demonstracoes_Financeiras_TRI": 'SELECT DISTINCT "Ticker" as t FROM public."Demonstracoes_Financeiras_TRI"',
        "multiplos": 'SELECT DISTINCT "Ticker" as t FROM public.multiplos',
        'multiplos_TRI': 'SELECT DISTINCT "Ticker" as t FROM public."multiplos_TRI"',
        "setores": "SELECT DISTINCT ticker as t FROM public.setores",
        "docs_corporativos": "SELECT DISTINCT ticker as t FROM public.docs_corporativos WHERE ticker IS NOT NULL",
        "patch6_runs": "SELECT DISTINCT ticker as t FROM public.patch6_runs",
    }

    ticker_sets: Dict[str, Set[str]] = {}
    for table, sql in tables.items():
        try:
            df = q(sql)
            ticker_sets[table] = set(df["t"].dropna().str.strip().str.upper())
        except Exception as e:
            print(f"  [SKIP] {table}: {e}")

    print(f"  {'Tabela':<35} {'N tickers':>10} {'Com .SA':>8} {'Minúsculas':>12}")
    print("  " + "─" * 70)

    for table, tickers in ticker_sets.items():
        n_sa = sum(1 for t in tickers if ".SA" in t)
        n_lower = sum(1 for t in tickers if t != t.upper())
        flag = " ← INCONSISTENTE" if n_sa > 0 or n_lower > 0 else ""
        print(f"  {table:<35} {len(tickers):>10} {n_sa:>8} {n_lower:>12}{flag}")

    # Tickers com .SA em alguma tabela
    all_with_sa = []
    for table, tickers in ticker_sets.items():
        sa_tickers = [t for t in tickers if ".SA" in t]
        if sa_tickers:
            all_with_sa.append((table, sa_tickers[:5]))

    if all_with_sa:
        print("\n  [PROBLEMA] Tickers com sufixo .SA (deveriam ser sem):")
        for table, examples in all_with_sa:
            print(f"    {table}: {examples}")

    # Cross-table: tickers presentes em DF mas ausentes em setores
    if "Demonstracoes_Financeiras" in ticker_sets and "setores" in ticker_sets:
        df_only = ticker_sets["Demonstracoes_Financeiras"] - ticker_sets["setores"]
        if df_only:
            print(f"\n  [ALERTA] {len(df_only)} tickers em DF sem classificação em setores:")
            print(f"    {sorted(df_only)[:15]}")


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 6: Preço ausente em múltiplos por ticker e período
# ─────────────────────────────────────────────────────────────────────────────

def check_multiplos_preco() -> None:
    header("RECONCILIAÇÃO: cobertura de preço em múltiplos (yfinance gap)")

    try:
        df = q("""
            SELECT
                EXTRACT(YEAR FROM "Data") as ano,
                COUNT(DISTINCT "Ticker") as total_tickers,
                COUNT(DISTINCT CASE WHEN "P/L" != 0 OR "P/VP" != 0 THEN "Ticker" END) as tickers_com_preco,
                COUNT(DISTINCT CASE WHEN "P/L" = 0 AND "P/VP" = 0 AND "DY" = 0 THEN "Ticker" END) as tickers_sem_preco
            FROM public.multiplos
            GROUP BY ano
            ORDER BY ano DESC
            LIMIT 15
        """)
    except Exception as e:
        print(f"  [ERRO] {e}")
        return

    print(f"  {'Ano':>6} {'Total':>8} {'Com Preço':>10} {'Sem Preço':>10} {'Cobertura':>10}")
    print("  " + "─" * 50)

    for _, row in df.iterrows():
        ano = int(row["ano"])
        total = int(row["total_tickers"])
        com_preco = int(row["tickers_com_preco"])
        sem_preco = int(row["tickers_sem_preco"])
        pct_cob = round(100 * com_preco / max(total, 1), 1)
        flag = "  ← CRÍTICO (YF_END)" if ano >= 2024 and pct_cob < 50 else (
               "  ← ATENÇÃO" if pct_cob < 80 else "")
        print(f"  {ano:>6} {total:>8} {com_preco:>10} {sem_preco:>10} {pct_cob:>9.1f}%{flag}")

    print(f"\n  NOTA: Se múltiplos de 2024+ têm cobertura de preço próxima de 0%,")
    print(f"  isso confirma o bug de YF_END='2023-12-31' em dados_multiplos_dfp.py.")
    print(f"  Correção: setar env var YF_END para a data atual ao rodar o pipeline.")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

ALL_CHECKS = {
    "dfp_vs_tri": check_dfp_vs_tri,
    "df_vs_multiplos": check_df_vs_multiplos,
    "docs_vs_chunks": check_docs_vs_chunks,
    "patch6_vs_docs": check_patch6_vs_docs,
    "ticker_canonical": check_ticker_canonical,
    "multiplos_preco": check_multiplos_preco,
}


def main():
    parser = argparse.ArgumentParser(description="Reconciliação fonte → banco")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--check", choices=list(ALL_CHECKS.keys()))
    args = parser.parse_args()

    if not args.all and not args.check:
        parser.print_help()
        sys.exit(1)

    to_run = list(ALL_CHECKS.keys()) if args.all else [args.check]

    for name in to_run:
        print(f"\nExecutando: {name}...")
        try:
            ALL_CHECKS[name]()
        except Exception as e:
            print(f"  [ERRO FATAL] {e}")


if __name__ == "__main__":
    main()
