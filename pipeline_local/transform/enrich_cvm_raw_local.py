"""
pipeline_local/transform/enrich_cvm_raw_local.py
Raw (DFP + ITR) → cvm_raw_enriched_local.

O que este script faz:
  1. Lê cvm_dfp_raw_local e cvm_itr_raw_local em chunks
  2. Normaliza escala monetária (MIL → x1000, UNIDADE → x1)
  3. Extrai dimensões temporais (period_year, period_quarter, period_label)
  4. Deriva flags de consolidação (is_consolidated, is_individual)
  5. Aplica mapeamento de contas canônicas via cvm_account_map (Supabase)
  6. Grava em pipeline_local.cvm_raw_enriched_local com deduplicação por row_hash

Variáveis de ambiente:
  LOCAL_DB_URL           obrigatória
  SUPABASE_DB_URL        necessária para carregar cvm_account_map
  ENRICH_SOURCE          DFP | ITR | ALL (default ALL)
  ENRICH_YEAR_START      ano mínimo (default sem filtro)
  ENRICH_YEAR_END        ano máximo (default sem filtro)
  PIPELINE_CHUNK_SIZE    linhas por chunk (default 10000)
"""
from __future__ import annotations

import os
import uuid
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text as sa_text

from pipeline_local.config.connections import get_local_engine, get_supabase_engine
from pipeline_local.config.settings import load_settings
from pipeline_local.utils.logger import get_logger
from pipeline_local.utils.hashing import dataframe_row_hash

log = get_logger("enrich_cvm_raw")

ENRICH_SOURCE = os.getenv("ENRICH_SOURCE", "ALL").strip().upper()  # DFP | ITR | ALL
ENRICH_YEAR_START = os.getenv("ENRICH_YEAR_START", "").strip()
ENRICH_YEAR_END = os.getenv("ENRICH_YEAR_END", "").strip()

TARGET_TABLE = "pipeline_local.cvm_raw_enriched_local"

_ESCALA_MAP = {"MIL": 1_000.0, "UNIDADE": 1.0, "MILHÃO": 1_000_000.0, "MILHAO": 1_000_000.0}
_CONSOLIDADO_KEYWORDS = {"con", "consolidado", "consolidated"}


# ---------------------------------------------------------------------------
# Carregamento de regras de mapeamento de contas (cvm_account_map Supabase)
# ---------------------------------------------------------------------------
def _load_account_map() -> pd.DataFrame:
    try:
        engine = get_supabase_engine()
        with engine.connect() as conn:
            df = pd.read_sql(
                sa_text("""
                    SELECT cd_conta, ds_conta_pattern, canonical_key, qualidade_mapeamento, ativo
                    FROM public.cvm_account_map
                    WHERE ativo = TRUE
                    ORDER BY qualidade_mapeamento, cd_conta
                """),
                conn,
            )
        log.info("cvm_account_map carregado", total_regras=len(df))
        return df
    except Exception as exc:
        log.warning("Falha ao carregar cvm_account_map — enriquecimento sem mapeamento", erro=str(exc))
        return pd.DataFrame(columns=["cd_conta", "ds_conta_pattern", "canonical_key", "qualidade_mapeamento"])


def _build_account_index(account_map: pd.DataFrame) -> Dict[str, Tuple[str, str]]:
    """Retorna dict cd_conta → (canonical_key, qualidade_mapeamento) para lookup O(1)."""
    index: Dict[str, Tuple[str, str]] = {}
    for _, row in account_map.iterrows():
        key = str(row.get("cd_conta") or "").strip()
        if key:
            index[key] = (str(row["canonical_key"]), str(row.get("qualidade_mapeamento") or "fallback"))
    return index


# ---------------------------------------------------------------------------
# Transformações por chunk
# ---------------------------------------------------------------------------
def _enrich_chunk(df: pd.DataFrame, account_index: Dict[str, Tuple[str, str]]) -> pd.DataFrame:
    df = df.copy()

    # Datas
    df["dt_refer"] = pd.to_datetime(df.get("dt_refer"), errors="coerce").dt.date

    # Dimensões temporais
    dt_series = pd.to_datetime(df["dt_refer"], errors="coerce")
    df["period_year"] = dt_series.dt.year.astype("Int64")
    df["period_quarter"] = dt_series.dt.quarter.astype("Int64")
    df["period_month"] = dt_series.dt.month.astype("Int64")
    df["period_label"] = (
        df["period_year"].astype(str).str.replace("<NA>", "") + "Q" +
        df["period_quarter"].astype(str).str.replace("<NA>", "")
    )

    # Flags annual/quarterly
    source = df.get("source_doc", pd.Series([""] * len(df)))
    df["is_annual"] = source.str.upper() == "DFP"
    df["is_quarterly"] = source.str.upper() == "ITR"

    # Escala monetária
    escala = df.get("escala_moeda", pd.Series(["UNIDADE"] * len(df))).str.upper().fillna("UNIDADE")
    df["unit_scale_factor"] = escala.map(lambda e: _ESCALA_MAP.get(e, 1.0))

    vl_conta = pd.to_numeric(df.get("vl_conta"), errors="coerce")
    df["value_normalized_brl"] = vl_conta * df["unit_scale_factor"]

    # Consolidação
    grupo = df.get("grupo_demo", pd.Series([""] * len(df))).fillna("").str.lower()
    df["is_consolidated"] = grupo.apply(lambda g: any(kw in g for kw in _CONSOLIDADO_KEYWORDS))
    df["is_individual"] = ~df["is_consolidated"]

    # Profundidade de conta
    cd = df.get("cd_conta", pd.Series([""] * len(df))).fillna("").astype(str)
    df["account_depth"] = cd.str.count(r"\.").astype("Int64") + 1
    df["top_account_code"] = cd.str.split(".").str[0]
    df["account_code_root"] = cd.str.extract(r"^(\d+\.?\d*)")[0]
    df["is_leaf_account"] = df["account_depth"] == df["account_depth"].max()

    # Normalização de nomes
    df["normalized_ds_conta"] = df.get("ds_conta", pd.Series([""] * len(df))).fillna("").str.strip().str.lower()
    df["normalized_denom_cia"] = df.get("denom_cia", pd.Series([""] * len(df))).fillna("").str.strip().str.upper()

    # Mapeamento de conta canônica
    def _lookup(cd_conta: str) -> Tuple[Optional[str], Optional[str]]:
        key = str(cd_conta or "").strip()
        return account_index.get(key, (None, None))

    mapped = cd.apply(_lookup)
    df["canonical_key"] = mapped.apply(lambda t: t[0])
    df["qualidade_mapeamento"] = mapped.apply(lambda t: t[1])

    # fiscal_period_type
    df["fiscal_period_type"] = df["is_annual"].map({True: "annual", False: "quarterly"})

    # row_hash
    hash_cols = [c for c in ("source_doc", "cd_cvm", "tipo_demo", "dt_refer", "cd_conta", "value_normalized_brl", "is_consolidated") if c in df.columns]
    df["row_hash"] = dataframe_row_hash(df, hash_cols)

    return df


# ---------------------------------------------------------------------------
# Carga da raw table em chunks
# ---------------------------------------------------------------------------
def _iter_raw_chunks(engine, source_doc: str, chunk_size: int, year_start: Optional[int], year_end: Optional[int]):
    table = "pipeline_local.cvm_dfp_raw_local" if source_doc == "DFP" else "pipeline_local.cvm_itr_raw_local"
    where_clauses = []
    if year_start:
        where_clauses.append(f"EXTRACT(YEAR FROM dt_refer) >= {year_start}")
    if year_end:
        where_clauses.append(f"EXTRACT(YEAR FROM dt_refer) <= {year_end}")
    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    offset = 0
    while True:
        query = f"""
            SELECT id, source_doc, tipo_demo, grupo_demo, arquivo_origem,
                   cd_cvm, cnpj_cia, denom_cia, ticker, versao, ordem_exerc,
                   dt_refer, dt_ini_exerc, dt_fim_exerc, cd_conta, ds_conta,
                   nivel_conta, conta_pai, vl_conta, escala_moeda, moeda, st_conta_fixa
            FROM {table}
            {where}
            ORDER BY id
            LIMIT {chunk_size} OFFSET {offset}
        """
        with engine.connect() as conn:
            chunk = pd.read_sql(sa_text(query), conn)
        if chunk.empty:
            break
        yield chunk
        offset += chunk_size


# ---------------------------------------------------------------------------
# Inserção no enriched table
# ---------------------------------------------------------------------------
_ENRICHED_COLS = [
    "source_doc", "tipo_demo", "grupo_demo", "arquivo_origem",
    "cd_cvm", "cnpj_cia", "denom_cia", "ticker",
    "dt_refer", "cd_conta", "ds_conta", "conta_pai", "nivel_conta", "vl_conta",
    "period_year", "period_quarter", "period_month", "period_label", "fiscal_period_type",
    "account_depth", "top_account_code", "account_code_root", "is_leaf_account",
    "normalized_ds_conta", "normalized_denom_cia",
    "is_consolidated", "is_individual", "is_annual", "is_quarterly",
    "unit_scale_factor", "value_normalized_brl",
    "canonical_key", "qualidade_mapeamento",
    "row_hash",
]


def _insert_enriched(df: pd.DataFrame, engine, batch_size: int) -> Dict[str, int]:
    for col in _ENRICHED_COLS:
        if col not in df.columns:
            df[col] = None
    df = df[_ENRICHED_COLS].copy()

    insert_sql = sa_text(f"""
        INSERT INTO {TARGET_TABLE}
            ({", ".join(_ENRICHED_COLS)})
        VALUES
            ({", ".join(f":{c}" for c in _ENRICHED_COLS)})
        ON CONFLICT (row_hash) DO NOTHING
    """)

    inserted = skipped = 0
    for start in range(0, len(df), batch_size):
        chunk = df.iloc[start: start + batch_size]
        records = chunk.where(pd.notna(chunk), other=None).to_dict("records")
        try:
            with engine.begin() as conn:
                conn.execute(insert_sql, records)
            inserted += len(records)
        except Exception as exc:
            log.error("Batch enrich falhou, tentando linha a linha", batch_start=start, erro=str(exc))
            with engine.begin() as conn:
                for rec in records:
                    try:
                        conn.execute(insert_sql, rec)
                        inserted += 1
                    except Exception:
                        skipped += 1
    return {"inserted": inserted, "skipped": skipped}


# ---------------------------------------------------------------------------
# Orquestrador
# ---------------------------------------------------------------------------
def run(
    source: Optional[str] = None,
    year_start: Optional[int] = None,
    year_end: Optional[int] = None,
) -> Dict[str, int]:
    settings = load_settings()
    source = (source or ENRICH_SOURCE).upper()
    yr_start = year_start or (int(ENRICH_YEAR_START) if ENRICH_YEAR_START else None)
    yr_end = year_end or (int(ENRICH_YEAR_END) if ENRICH_YEAR_END else None)

    run_id = str(uuid.uuid4())
    engine = get_local_engine()
    account_map = _load_account_map()
    account_index = _build_account_index(account_map)

    sources = ["DFP", "ITR"] if source == "ALL" else [source]
    log.info("Iniciando enriquecimento", run_id=run_id, sources=sources, year_start=yr_start, year_end=yr_end)

    total_inserted = total_skipped = 0
    for src in sources:
        src_inserted = src_skipped = 0
        for chunk in _iter_raw_chunks(engine, src, settings.chunk_size, yr_start, yr_end):
            enriched = _enrich_chunk(chunk, account_index)
            counts = _insert_enriched(enriched, engine, settings.batch_size)
            src_inserted += counts["inserted"]
            src_skipped += counts["skipped"]
        log.info("Fonte processada", source=src, inserted=src_inserted, skipped=src_skipped, run_id=run_id)
        total_inserted += src_inserted
        total_skipped += src_skipped

    log.summary(
        pipeline="enrich_cvm_raw_local",
        status="success",
        run_id=run_id,
        rows_inserted=total_inserted,
        rows_skipped=total_skipped,
    )
    return {"inserted": total_inserted, "skipped": total_skipped}


def main() -> None:
    run()


if __name__ == "__main__":
    main()
