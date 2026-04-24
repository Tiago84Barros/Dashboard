"""
pipeline_local/transform/build_financials_local.py
Enriched → financials_annual_final_local + financials_quarterly_final_local.

O que este script faz:
  1. Lê cvm_raw_enriched_local (já com canonical_key mapeado)
  2. Pivota de linhas (canonical_key, value) para colunas wide
  3. Seleciona melhor versão por (ticker, dt_refer): PENÚLTIMO exercício ou mais recente
  4. Calcula derivados: divida_bruta, divida_liquida
  5. Calcula quality_score
  6. Grava (upsert) em financials_annual_final_local e financials_quarterly_final_local

Variáveis de ambiente:
  LOCAL_DB_URL           obrigatória
  BUILD_SOURCE           DFP | ITR | ALL (default ALL)
  PIPELINE_CHUNK_SIZE    linhas por chunk ao ler enriched (default 10000)
"""
from __future__ import annotations

import os
import uuid
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text as sa_text

from pipeline_local.config.connections import get_local_engine
from pipeline_local.config.settings import load_settings
from pipeline_local.utils.logger import get_logger
from pipeline_local.utils.hashing import dataframe_row_hash

log = get_logger("build_financials")

BUILD_SOURCE = os.getenv("BUILD_SOURCE", "ALL").strip().upper()

CANONICAL_KEYS: List[str] = [
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

_QUALITY_MAP = {"exact": 1.00, "regex": 0.90, "manual": 0.95, "derived": 0.85, "fallback": 0.70}


# ---------------------------------------------------------------------------
# Leitura do enriched
# ---------------------------------------------------------------------------
def _load_enriched_for_source(engine, source_doc: str, chunk_size: int) -> pd.DataFrame:
    """Lê TODOS os dados enriched para um source_doc — em memória para facilitar pivot."""
    query = sa_text(f"""
        SELECT ticker, cd_cvm, denom_cia, dt_refer,
               period_year, period_quarter, period_label, source_doc,
               is_consolidated, canonical_key, qualidade_mapeamento,
               value_normalized_brl
        FROM pipeline_local.cvm_raw_enriched_local
        WHERE source_doc = :src
          AND canonical_key IS NOT NULL
          AND ticker IS NOT NULL
          AND dt_refer IS NOT NULL
        ORDER BY ticker, dt_refer, canonical_key
    """)
    chunks = []
    with engine.connect() as conn:
        for chunk in pd.read_sql(query, conn, params={"src": source_doc}, chunksize=chunk_size):
            chunks.append(chunk)
    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True)


# ---------------------------------------------------------------------------
# Seleção de melhor versão: consolidado > individual; penúltimo exerc > último
# ---------------------------------------------------------------------------
def _select_best(df: pd.DataFrame) -> pd.DataFrame:
    """Para cada (ticker, dt_refer, canonical_key), escolhe a linha de maior qualidade."""
    if df.empty:
        return df
    quality_order = {"exact": 0, "manual": 1, "regex": 2, "derived": 3, "fallback": 4}
    df = df.copy()
    df["_q_order"] = df["qualidade_mapeamento"].map(quality_order).fillna(5)
    # Prefere consolidado
    df["_is_con"] = df["is_consolidated"].astype(int)
    df = (
        df.sort_values(["ticker", "dt_refer", "canonical_key", "_is_con", "_q_order"],
                       ascending=[True, True, True, False, True])
          .drop_duplicates(subset=["ticker", "dt_refer", "canonical_key"], keep="first")
          .drop(columns=["_q_order", "_is_con"])
          .reset_index(drop=True)
    )
    return df


# ---------------------------------------------------------------------------
# Pivot: linhas → colunas wide
# ---------------------------------------------------------------------------
def _pivot_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df_best = _select_best(df)
    pivot = df_best.pivot_table(
        index=["ticker", "cd_cvm", "denom_cia", "dt_refer", "period_year", "period_quarter", "period_label", "source_doc"],
        columns="canonical_key",
        values="value_normalized_brl",
        aggfunc="first",
    ).reset_index()
    pivot.columns.name = None
    return pivot


# ---------------------------------------------------------------------------
# Quality score por linha
# ---------------------------------------------------------------------------
def _compute_quality(df_best: pd.DataFrame) -> pd.Series:
    if df_best.empty:
        return pd.Series(dtype=float)
    grouped = df_best.groupby(["ticker", "dt_refer"])

    def _qs(g: pd.DataFrame) -> float:
        present = sum(1 for k in CANONICAL_KEYS if k in g["canonical_key"].values)
        total = max(len(CANONICAL_KEYS), 1)
        base = present / total
        qs = [_QUALITY_MAP.get(str(v), 0.60) for v in g["qualidade_mapeamento"].dropna()]
        mapping_q = float(np.mean(qs)) if qs else 0.60
        return round(base * mapping_q * 100, 4)

    scores = grouped.apply(_qs).reset_index(name="quality_score")
    return scores


# ---------------------------------------------------------------------------
# Build derivados
# ---------------------------------------------------------------------------
def _add_derived(df: pd.DataFrame) -> pd.DataFrame:
    def _col(name: str) -> pd.Series:
        return df[name] if name in df.columns else pd.Series(np.nan, index=df.index)

    df["divida_bruta"] = _col("divida_cp").fillna(0) + _col("divida_lp").fillna(0)
    df["divida_bruta"] = df["divida_bruta"].replace(0, np.nan)
    df["divida_liquida"] = df["divida_bruta"] - (
        _col("caixa_equivalentes").fillna(0) + _col("aplicacoes_financeiras").fillna(0)
    )
    return df


# ---------------------------------------------------------------------------
# Upsert no banco local
# ---------------------------------------------------------------------------
_ANNUAL_TABLE = "pipeline_local.financials_annual_final_local"
_QTR_TABLE = "pipeline_local.financials_quarterly_final_local"

_BASE_COLS = [
    "ticker", "cd_cvm", "denom_cia", "dt_refer", "period_label", "source_doc",
] + CANONICAL_KEYS + ["divida_bruta", "divida_liquida", "quality_score", "row_hash"]

_QTR_EXTRA = ["period_quarter", "period_year"]


def _upsert_wide(df: pd.DataFrame, engine, table: str, is_quarterly: bool) -> Dict[str, int]:
    from sqlalchemy import text as sa_text
    if df.empty:
        return {"upserted": 0, "error": 0}

    cols = _BASE_COLS.copy()
    if is_quarterly:
        cols = ["ticker", "cd_cvm", "denom_cia", "dt_refer", "period_label", "source_doc",
                "period_quarter", "period_year"] + CANONICAL_KEYS + ["divida_bruta", "divida_liquida", "quality_score", "row_hash"]

    for col in cols:
        if col not in df.columns:
            df[col] = None

    df = df[cols].copy()

    # row_hash por (ticker, dt_refer)
    df["row_hash"] = dataframe_row_hash(df, ["ticker", "dt_refer", "source_doc"])

    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in ("ticker", "dt_refer"))
    upserted = errors = 0

    records = df.where(pd.notna(df), other=None).to_dict("records")
    with engine.begin() as conn:
        for rec in records:
            try:
                conn.execute(
                    sa_text(f"""
                        INSERT INTO {table}
                            ({", ".join(cols)})
                        VALUES
                            ({", ".join(f":{c}" for c in cols)})
                        ON CONFLICT (ticker, dt_refer) DO UPDATE SET
                            {set_clause},
                            updated_at = now()
                    """),
                    rec,
                )
                upserted += 1
            except Exception as exc:
                log.error("Falha no upsert", ticker=rec.get("ticker"), dt=rec.get("dt_refer"), erro=str(exc))
                errors += 1
    return {"upserted": upserted, "error": errors}


# ---------------------------------------------------------------------------
# Orquestrador
# ---------------------------------------------------------------------------
def run(source: Optional[str] = None) -> Dict[str, int]:
    settings = load_settings()
    source = (source or BUILD_SOURCE).upper()
    run_id = str(uuid.uuid4())
    engine = get_local_engine()

    sources_map = {
        "DFP": [("DFP", _ANNUAL_TABLE, False)],
        "ITR": [("ITR", _QTR_TABLE, True)],
        "ALL": [("DFP", _ANNUAL_TABLE, False), ("ITR", _QTR_TABLE, True)],
    }
    jobs = sources_map.get(source, sources_map["ALL"])

    log.info("Iniciando build financials local", run_id=run_id, source=source)

    total_upserted = total_error = 0
    for src_doc, target_table, is_qtr in jobs:
        log.info("Lendo enriched", source=src_doc, run_id=run_id)
        df = _load_enriched_for_source(engine, src_doc, settings.chunk_size)
        if df.empty:
            log.warning("Nenhum dado encontrado no enriched", source=src_doc)
            continue

        # Quality scores
        scores = _compute_quality(df)

        # Pivot
        wide = _pivot_to_wide(df)
        if wide.empty:
            log.warning("Pivot retornou vazio", source=src_doc)
            continue

        # Derivados
        wide = _add_derived(wide)

        # Merge quality score
        wide = wide.merge(scores, on=["ticker", "dt_refer"], how="left")

        counts = _upsert_wide(wide, engine, target_table, is_qtr)
        log.info("Build concluído", source=src_doc, table=target_table, **counts, run_id=run_id)
        total_upserted += counts["upserted"]
        total_error += counts["error"]

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
