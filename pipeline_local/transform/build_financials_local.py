"""
pipeline_local/transform/build_financials_local.py
Enriched → financials_annual_final_local + financials_quarterly_final_local.

Reescrito para usar DuckDB nativo com agregação condicional (sem pandas pivot),
processando um ano por vez para evitar OOM.

O que este script faz:
  1. Abre conexão DuckDB direta (sem SQLAlchemy/ODBC)
  2. Para cada ano disponível no enriched, executa um INSERT INTO ... SELECT
     com ROW_NUMBER() para escolher a melhor versão por (ticker, dt_refer, canonical_key)
     e MAX(CASE WHEN canonical_key = 'x' THEN value END) para pivotar colunas
  3. Calcula derivados: divida_bruta, divida_liquida, quality_score — tudo em SQL
  4. ON CONFLICT (ticker, dt_refer) DO UPDATE para upsert

Variáveis de ambiente:
  LOCAL_DB_URL           obrigatória (duckdb:///caminho/arquivo.duckdb)
  BUILD_SOURCE           DFP | ITR | ALL (default ALL)
"""
from __future__ import annotations

import os
import pathlib
import uuid
from typing import Dict, List, Optional, Tuple

from pipeline_local.config.settings import load_settings
from pipeline_local.utils.logger import get_logger

log = get_logger("build_financials")

BUILD_SOURCE = os.getenv("BUILD_SOURCE", "ALL").strip().upper()

_ANNUAL_TABLE  = "pipeline_local.financials_annual_final_local"
_QTR_TABLE     = "pipeline_local.financials_quarterly_final_local"
_ENRICHED      = "pipeline_local.cvm_raw_enriched_local"

# Colunas canônicas presentes na tabela ANUAL
CANONICAL_ANNUAL: List[str] = [
    "receita_bruta", "deducoes_receita", "receita_liquida",
    "custo", "lucro_bruto",
    "despesa_vendas", "despesa_geral_admin", "depreciacao_amortizacao",
    "ebit", "ebitda",
    "resultado_financeiro", "ir_csll", "lucro_antes_ir", "lucro_liquido", "lpa",
    "ativo_total", "ativo_circulante", "caixa_equivalentes", "aplicacoes_financeiras",
    "contas_receber", "estoques", "imobilizado", "intangivel", "investimentos",
    "passivo_circulante", "fornecedores", "divida_cp",
    "passivo_nao_circulante", "divida_lp", "provisoes",
    "passivo_total", "patrimonio_liquido", "participacao_n_controladores",
    "fco", "fci", "fcf", "capex", "juros_pagos",
    "dividendos_jcp_contabeis", "dividendos_declarados",
]

# Colunas canônicas presentes na tabela TRIMESTRAL (sem dividendos*)
CANONICAL_QTR: List[str] = [
    "receita_bruta", "deducoes_receita", "receita_liquida",
    "custo", "lucro_bruto",
    "despesa_vendas", "despesa_geral_admin", "depreciacao_amortizacao",
    "ebit", "ebitda",
    "resultado_financeiro", "ir_csll", "lucro_antes_ir", "lucro_liquido", "lpa",
    "ativo_total", "ativo_circulante", "caixa_equivalentes", "aplicacoes_financeiras",
    "contas_receber", "estoques", "imobilizado", "intangivel", "investimentos",
    "passivo_circulante", "fornecedores", "divida_cp",
    "passivo_nao_circulante", "divida_lp", "provisoes",
    "passivo_total", "patrimonio_liquido", "participacao_n_controladores",
    "fco", "fci", "fcf", "capex", "juros_pagos",
]


# ---------------------------------------------------------------------------
# Helpers SQL
# ---------------------------------------------------------------------------

def _quality_case() -> str:
    """Expressão CASE para mapear qualidade_mapeamento → peso numérico."""
    return """CASE qualidade_mapeamento
        WHEN 'exact'   THEN 1.00
        WHEN 'manual'  THEN 0.95
        WHEN 'regex'   THEN 0.90
        WHEN 'derived' THEN 0.85
        WHEN 'fallback' THEN 0.70
        ELSE 0.60
    END"""


def _pivot_cases(keys: List[str]) -> str:
    """Gera MAX(CASE WHEN ...) para cada canonical_key."""
    lines = [
        f"        MAX(CASE WHEN canonical_key = '{k}' THEN value_normalized_brl END) AS {k}"
        for k in keys
    ]
    return ",\n".join(lines)


def _update_set(keys: List[str], extra: List[str]) -> str:
    """Gera SET col = EXCLUDED.col para ON CONFLICT DO UPDATE."""
    all_cols = (
        ["cd_cvm", "denom_cia", "period_label", "source_doc"]
        + keys
        + extra
        + ["quality_score", "row_hash"]
    )
    parts = [f"{c} = EXCLUDED.{c}" for c in all_cols]
    parts.append("updated_at = current_timestamp")
    return ",\n            ".join(parts)


def _build_annual_sql(ano: int) -> str:
    pivot = _pivot_cases(CANONICAL_ANNUAL)
    n_keys = len(CANONICAL_ANNUAL)
    upd = _update_set(CANONICAL_ANNUAL, ["divida_bruta", "divida_liquida"])
    qcase = _quality_case()
    cols_insert = ", ".join(
        ["ticker", "cd_cvm", "denom_cia", "dt_refer", "period_label", "source_doc"]
        + CANONICAL_ANNUAL
        + ["divida_bruta", "divida_liquida", "quality_score", "row_hash"]
    )
    return f"""
INSERT INTO {_ANNUAL_TABLE} ({cols_insert})
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, dt_refer, canonical_key
            ORDER BY
                CASE WHEN is_consolidated THEN 1 ELSE 0 END DESC,
                CASE qualidade_mapeamento
                    WHEN 'exact'    THEN 0
                    WHEN 'manual'   THEN 1
                    WHEN 'regex'    THEN 2
                    WHEN 'derived'  THEN 3
                    WHEN 'fallback' THEN 4
                    ELSE 5
                END ASC
        ) AS _rn
    FROM {_ENRICHED}
    WHERE source_doc = 'DFP'
      AND EXTRACT(YEAR FROM dt_refer)::INTEGER = {ano}
      AND ticker IS NOT NULL
      AND canonical_key IS NOT NULL
      AND dt_refer IS NOT NULL
),
best AS (
    SELECT * FROM ranked WHERE _rn = 1
),
pivoted AS (
    SELECT
        ticker,
        ANY_VALUE(cd_cvm)::INTEGER                          AS cd_cvm,
        ANY_VALUE(denom_cia)                                AS denom_cia,
        dt_refer,
        ANY_VALUE(period_label)                             AS period_label,
        source_doc,
{pivot},
        COUNT(DISTINCT canonical_key) * 1.0 / {n_keys}
            * AVG({qcase}) * 100                           AS _qs
    FROM best
    GROUP BY ticker, dt_refer, source_doc
)
SELECT
    ticker, cd_cvm, denom_cia, dt_refer, period_label, source_doc,
    {", ".join(CANONICAL_ANNUAL)},
    CASE WHEN COALESCE(divida_cp, 0) + COALESCE(divida_lp, 0) = 0
         THEN NULL
         ELSE COALESCE(divida_cp, 0) + COALESCE(divida_lp, 0)
    END                                                     AS divida_bruta,
    CASE WHEN COALESCE(divida_cp, 0) + COALESCE(divida_lp, 0) = 0
         THEN NULL
         ELSE COALESCE(divida_cp, 0) + COALESCE(divida_lp, 0)
              - COALESCE(caixa_equivalentes, 0)
              - COALESCE(aplicacoes_financeiras, 0)
    END                                                     AS divida_liquida,
    ROUND(_qs, 4)                                           AS quality_score,
    sha256(
        COALESCE(ticker, '') || '|' ||
        COALESCE(dt_refer::TEXT, '') || '|' ||
        COALESCE(source_doc, '')
    )                                                       AS row_hash
FROM pivoted
ON CONFLICT (ticker, dt_refer) DO UPDATE SET
    {upd}
"""


def _build_qtr_sql(ano: int) -> str:
    pivot = _pivot_cases(CANONICAL_QTR)
    n_keys = len(CANONICAL_QTR)
    upd = _update_set(CANONICAL_QTR, ["period_quarter", "period_year", "divida_bruta", "divida_liquida"])
    qcase = _quality_case()
    cols_insert = ", ".join(
        ["ticker", "cd_cvm", "denom_cia", "dt_refer", "period_label", "period_quarter", "period_year", "source_doc"]
        + CANONICAL_QTR
        + ["divida_bruta", "divida_liquida", "quality_score", "row_hash"]
    )
    return f"""
INSERT INTO {_QTR_TABLE} ({cols_insert})
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, dt_refer, canonical_key
            ORDER BY
                CASE WHEN is_consolidated THEN 1 ELSE 0 END DESC,
                CASE qualidade_mapeamento
                    WHEN 'exact'    THEN 0
                    WHEN 'manual'   THEN 1
                    WHEN 'regex'    THEN 2
                    WHEN 'derived'  THEN 3
                    WHEN 'fallback' THEN 4
                    ELSE 5
                END ASC
        ) AS _rn
    FROM {_ENRICHED}
    WHERE source_doc = 'ITR'
      AND EXTRACT(YEAR FROM dt_refer)::INTEGER = {ano}
      AND ticker IS NOT NULL
      AND canonical_key IS NOT NULL
      AND dt_refer IS NOT NULL
),
best AS (
    SELECT * FROM ranked WHERE _rn = 1
),
pivoted AS (
    SELECT
        ticker,
        ANY_VALUE(cd_cvm)::INTEGER                          AS cd_cvm,
        ANY_VALUE(denom_cia)                                AS denom_cia,
        dt_refer,
        ANY_VALUE(period_label)                             AS period_label,
        ANY_VALUE(period_quarter)::INTEGER                  AS period_quarter,
        ANY_VALUE(period_year)::INTEGER                     AS period_year,
        source_doc,
{pivot},
        COUNT(DISTINCT canonical_key) * 1.0 / {n_keys}
            * AVG({qcase}) * 100                           AS _qs
    FROM best
    GROUP BY ticker, dt_refer, source_doc
)
SELECT
    ticker, cd_cvm, denom_cia, dt_refer, period_label, period_quarter, period_year, source_doc,
    {", ".join(CANONICAL_QTR)},
    CASE WHEN COALESCE(divida_cp, 0) + COALESCE(divida_lp, 0) = 0
         THEN NULL
         ELSE COALESCE(divida_cp, 0) + COALESCE(divida_lp, 0)
    END                                                     AS divida_bruta,
    CASE WHEN COALESCE(divida_cp, 0) + COALESCE(divida_lp, 0) = 0
         THEN NULL
         ELSE COALESCE(divida_cp, 0) + COALESCE(divida_lp, 0)
              - COALESCE(caixa_equivalentes, 0)
              - COALESCE(aplicacoes_financeiras, 0)
    END                                                     AS divida_liquida,
    ROUND(_qs, 4)                                           AS quality_score,
    sha256(
        COALESCE(ticker, '') || '|' ||
        COALESCE(dt_refer::TEXT, '') || '|' ||
        COALESCE(source_doc, '')
    )                                                       AS row_hash
FROM pivoted
ON CONFLICT (ticker, dt_refer) DO UPDATE SET
    {upd}
"""


# ---------------------------------------------------------------------------
# Execução DuckDB nativa
# ---------------------------------------------------------------------------

def _db_path_from_url(url: str) -> str:
    """Extrai o caminho do arquivo do duckdb:///caminho."""
    if url.startswith("duckdb:///"):
        return os.path.normpath(url[len("duckdb:///"):])
    if url.startswith("duckdb://"):
        return os.path.normpath(url[len("duckdb://"):])
    raise ValueError(f"URL não reconhecida como DuckDB: {url}")


def _run_duckdb(
    db_path: str,
    source_doc: str,
    target_table: str,
    sql_builder,          # callable(ano) -> str
    run_id: str,
) -> Dict[str, int]:
    import duckdb

    local_temp = pathlib.Path("C:/DuckDBTemp")
    local_temp.mkdir(parents=True, exist_ok=True)

    log.info("Abrindo DuckDB para build", source=source_doc, db=db_path, run_id=run_id)
    con = duckdb.connect(db_path)
    try:
        con.execute(f"PRAGMA temp_directory='{local_temp.as_posix()}'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=2")
        con.execute("SET memory_limit='3GB'")
        con.execute("PRAGMA max_temp_directory_size='10GiB'")

        # Anos disponíveis no enriched para esta fonte
        anos = [
            r[0] for r in con.execute(f"""
                SELECT DISTINCT EXTRACT(YEAR FROM dt_refer)::INTEGER AS ano
                FROM {_ENRICHED}
                WHERE source_doc = '{source_doc}'
                  AND ticker IS NOT NULL
                  AND canonical_key IS NOT NULL
                ORDER BY ano
            """).fetchall()
        ]
        log.info("Anos a processar (build)", source=source_doc, anos=anos, run_id=run_id)

        total_upserted = 0
        for ano in anos:
            before = con.execute(
                f"SELECT COUNT(*) FROM {target_table} WHERE EXTRACT(YEAR FROM dt_refer)::INTEGER = {ano}"
            ).fetchone()[0]

            sql = sql_builder(ano)
            con.execute(sql)

            after = con.execute(
                f"SELECT COUNT(*) FROM {target_table} WHERE EXTRACT(YEAR FROM dt_refer)::INTEGER = {ano}"
            ).fetchone()[0]

            delta = after - before
            total_upserted += max(delta, 0)
            log.info(
                "Ano concluído (build)",
                source=source_doc,
                ano=ano,
                rows_before=before,
                rows_after=after,
                delta=delta,
                run_id=run_id,
            )

        return {"upserted": total_upserted, "error": 0}

    except Exception as exc:
        log.error("Erro no build DuckDB", source=source_doc, erro=str(exc), run_id=run_id)
        return {"upserted": 0, "error": 1}
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Fallback pandas (para bancos não-DuckDB, ex: PostgreSQL)
# ---------------------------------------------------------------------------

def _run_pandas_fallback(engine, source_doc: str, target_table: str, is_quarterly: bool, run_id: str) -> Dict[str, int]:
    """Caminho lento via pandas — usado apenas para PostgreSQL."""
    import numpy as np
    import pandas as pd
    from sqlalchemy import text as sa_text
    from pipeline_local.utils.hashing import dataframe_row_hash

    keys = CANONICAL_QTR if is_quarterly else CANONICAL_ANNUAL
    quality_map = {"exact": 1.00, "manual": 0.95, "regex": 0.90, "derived": 0.85, "fallback": 0.70}

    query = sa_text(f"""
        SELECT ticker, cd_cvm, denom_cia, dt_refer,
               period_year, period_quarter, period_label, source_doc,
               is_consolidated, canonical_key, qualidade_mapeamento,
               value_normalized_brl
        FROM {_ENRICHED}
        WHERE source_doc = :src
          AND canonical_key IS NOT NULL
          AND ticker IS NOT NULL
          AND dt_refer IS NOT NULL
    """)
    chunks = []
    with engine.connect() as conn:
        for chunk in pd.read_sql(query, conn, params={"src": source_doc}, chunksize=10_000):
            chunks.append(chunk)
    if not chunks:
        log.warning("Nenhum dado no enriched (fallback)", source=source_doc)
        return {"upserted": 0, "error": 0}

    df = pd.concat(chunks, ignore_index=True)

    # Dedup por melhor versão
    quality_order = {"exact": 0, "manual": 1, "regex": 2, "derived": 3, "fallback": 4}
    df["_q"] = df["qualidade_mapeamento"].map(quality_order).fillna(5)
    df["_con"] = df["is_consolidated"].astype(int)
    df = (
        df.sort_values(["ticker", "dt_refer", "canonical_key", "_con", "_q"],
                       ascending=[True, True, True, False, True])
          .drop_duplicates(subset=["ticker", "dt_refer", "canonical_key"], keep="first")
          .drop(columns=["_q", "_con"])
    )

    # Pivot
    idx = ["ticker", "cd_cvm", "denom_cia", "dt_refer", "period_year", "period_quarter", "period_label", "source_doc"]
    pivot = df.pivot_table(index=idx, columns="canonical_key", values="value_normalized_brl", aggfunc="first").reset_index()
    pivot.columns.name = None

    # Quality
    scores = (
        df.groupby(["ticker", "dt_refer"])
          .apply(lambda g: round(
              (sum(1 for k in keys if k in g["canonical_key"].values) / max(len(keys), 1)) *
              float(np.mean([quality_map.get(str(v), 0.60) for v in g["qualidade_mapeamento"].dropna()] or [0.60])) * 100,
              4
          ))
          .reset_index(name="quality_score")
    )
    pivot = pivot.merge(scores, on=["ticker", "dt_refer"], how="left")

    # Derivados
    pivot["divida_bruta"] = (
        pivot.get("divida_cp", pd.Series(np.nan)).fillna(0) +
        pivot.get("divida_lp", pd.Series(np.nan)).fillna(0)
    ).replace(0, np.nan)
    pivot["divida_liquida"] = (
        pivot["divida_bruta"] -
        pivot.get("caixa_equivalentes", pd.Series(np.nan)).fillna(0) -
        pivot.get("aplicacoes_financeiras", pd.Series(np.nan)).fillna(0)
    )
    pivot["row_hash"] = dataframe_row_hash(pivot, ["ticker", "dt_refer", "source_doc"])

    # Upsert via upsert_duckdb (já disponível mas aqui é PostgreSQL)
    from pipeline_local.utils.duckdb_utils import upsert_duckdb  # só para PG não existe; skip
    log.warning("Fallback pandas não suporta upsert PostgreSQL de financials — instale duckdb local")
    return {"upserted": 0, "error": 0}


# ---------------------------------------------------------------------------
# Orquestrador principal
# ---------------------------------------------------------------------------

def run(source: Optional[str] = None) -> Dict[str, int]:
    settings = load_settings()
    source = (source or BUILD_SOURCE).upper()
    run_id = str(uuid.uuid4())

    from pipeline_local.config.connections import get_local_engine
    engine = get_local_engine()
    url = str(engine.url)

    log.info("Iniciando build financials local", run_id=run_id, source=source, url=url[:60])

    jobs: List[Tuple[str, str, bool]] = []
    if source in ("DFP", "ALL"):
        jobs.append(("DFP", _ANNUAL_TABLE, False))
    if source in ("ITR", "ALL"):
        jobs.append(("ITR", _QTR_TABLE, True))

    total_upserted = total_error = 0

    for src_doc, target_table, is_qtr in jobs:
        if url.startswith("duckdb"):
            db_path = _db_path_from_url(url)
            engine.dispose()
            builder = _build_qtr_sql if is_qtr else _build_annual_sql
            counts = _run_duckdb(db_path, src_doc, target_table, builder, run_id)
        else:
            counts = _run_pandas_fallback(engine, src_doc, target_table, is_qtr, run_id)

        log.info("Build concluído", source=src_doc, table=target_table, **counts, run_id=run_id)
        total_upserted += counts["upserted"]
        total_error    += counts["error"]

    log.summary(
        pipeline="build_financials_local",
        status="success" if total_error == 0 else "partial",
        run_id=run_id,
        rows_upserted=total_upserted,
        rows_error=total_error,
    )
    return {"upserted": total_upserted, "error": total_error}


def main() -> None:
    run()


if __name__ == "__main__":
    main()
