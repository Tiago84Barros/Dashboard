"""
pickup/cvm_map_v2.py
Camada de normalização CVM V2.

Lê public.cvm_financial_raw em lotes, aplica mapeamento de contas de
public.cvm_account_map e grava em public.cvm_financial_normalized.

Pré-requisito: schema CVM V2 aplicado ao banco (DDL institucional).
"""
from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from typing import Iterable, Optional

import pandas as pd
from sqlalchemy import text

from core.db import get_engine

LOG_PREFIX = os.getenv("LOG_PREFIX", "[CVM_MAP_V2]")
READ_CHUNK_SIZE = int(os.getenv("CVM_MAP_READ_CHUNK_SIZE", "25000"))
WRITE_CHUNK_SIZE = int(os.getenv("CVM_MAP_WRITE_CHUNK_SIZE", "2000"))


RAW_COLUMNS = [
    "ticker",
    "cd_cvm",
    "source_doc",
    "tipo_demo",
    "dt_refer",
    "cd_conta",
    "ds_conta",
    "vl_conta",
    "row_hash",
]


def log(msg: str) -> None:
    print(f"{LOG_PREFIX} {msg}", flush=True)


@dataclass(frozen=True)
class CompiledMapping:
    canonical_key: str
    sinal: float
    cd_conta: Optional[str]
    ds_conta_pattern: Optional[str]
    regex: Optional[re.Pattern]


@dataclass(frozen=True)
class MappingBundle:
    exact_map: dict[str, CompiledMapping]
    regex_rules: list[CompiledMapping]


@dataclass
class NormalizeStats:
    raw_rows: int = 0
    mapped_rows: int = 0
    unmatched_rows: int = 0
    skipped_null_value_rows: int = 0


def fetch_raw_chunks(engine=None, chunksize: int = READ_CHUNK_SIZE) -> Iterable[pd.DataFrame]:
    """Carrega public.cvm_financial_raw em lotes, apenas com colunas necessárias."""
    if engine is None:
        engine = get_engine()
    query = text(
        f"SELECT {', '.join(RAW_COLUMNS)} "
        "FROM public.cvm_financial_raw "
        "ORDER BY dt_refer, cd_cvm, row_hash"
    )
    with engine.connect() as conn:
        yield from pd.read_sql(query, conn, chunksize=chunksize)


def fetch_mapping(engine=None) -> pd.DataFrame:
    """Carrega mapeamento de contas ativo de public.cvm_account_map."""
    if engine is None:
        engine = get_engine()
    query = text(
        "SELECT * FROM public.cvm_account_map WHERE ativo = TRUE ORDER BY prioridade"
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def build_mapping_bundle(mappings: pd.DataFrame) -> MappingBundle:
    """Pré-compila regras de mapeamento para reduzir custo por linha."""
    exact_map: dict[str, CompiledMapping] = {}
    regex_rules: list[CompiledMapping] = []

    for _, m in mappings.iterrows():
        cd_conta = str(m.get("cd_conta") or "").strip() or None
        ds_pattern = str(m.get("ds_conta_pattern") or "").strip() or None
        regex_compiled: Optional[re.Pattern] = None

        if ds_pattern:
            try:
                regex_compiled = re.compile(ds_pattern, re.IGNORECASE)
            except re.error:
                log(f"Regex inválida ignorada no mapping: {ds_pattern}")
                ds_pattern = None

        compiled = CompiledMapping(
            canonical_key=str(m["canonical_key"]),
            sinal=float(m.get("sinal") or 1.0),
            cd_conta=cd_conta,
            ds_conta_pattern=ds_pattern,
            regex=regex_compiled,
        )

        if cd_conta and cd_conta not in exact_map:
            exact_map[cd_conta] = compiled
        if regex_compiled is not None:
            regex_rules.append(compiled)

    return MappingBundle(exact_map=exact_map, regex_rules=regex_rules)


def match_row(row: pd.Series, bundle: MappingBundle) -> tuple[Optional[CompiledMapping], str]:
    """Encontra o mapeamento mais específico para uma linha raw.

    Returns:
        (mapping_row, quality) where quality is 'exact' | 'regex' | 'fallback'
    """
    cd_conta = str(row.get("cd_conta") or "").strip()
    if cd_conta:
        exact = bundle.exact_map.get(cd_conta)
        if exact is not None:
            return exact, "exact"

    ds_conta = str(row.get("ds_conta") or "")
    for compiled in bundle.regex_rules:
        if compiled.regex is not None and compiled.regex.search(ds_conta):
            return compiled, "regex"

    return None, "fallback"


def normalize_chunk(df_raw: pd.DataFrame, bundle: MappingBundle) -> tuple[pd.DataFrame, NormalizeStats]:
    """Aplica mapeamento em um lote raw e gera DataFrame normalizado."""
    stats = NormalizeStats(raw_rows=len(df_raw))

    if df_raw.empty:
        return pd.DataFrame(), stats

    results = []
    for _, row in df_raw.iterrows():
        vl = row.get("vl_conta")
        if vl is None or pd.isna(vl):
            stats.skipped_null_value_rows += 1
            continue

        mapping, quality = match_row(row, bundle)
        if mapping is None:
            stats.unmatched_rows += 1
            continue

        results.append(
            {
                "ticker": row.get("ticker"),
                "cd_cvm": row.get("cd_cvm"),
                "source_doc": row.get("source_doc"),
                "tipo_demo": row.get("tipo_demo"),
                "dt_refer": row.get("dt_refer"),
                "canonical_key": mapping.canonical_key,
                "valor": float(vl) * mapping.sinal,
                "unidade": "BRL",
                "qualidade_mapeamento": quality,
                "row_hash": row.get("row_hash"),
            }
        )

    stats.mapped_rows = len(results)
    return pd.DataFrame(results), stats


def save(df: pd.DataFrame, engine=None, chunksize: int = WRITE_CHUNK_SIZE) -> int:
    """Grava em public.cvm_financial_normalized com UPSERT idempotente.

    Usa ON CONFLICT DO NOTHING para que re-execuções não falhem.
    Usa SQL literal com named params para compatibilidade com colunas ENUM
    (cvm_source_doc, cvm_tipo_demo, mapping_quality).
    """
    if engine is None:
        engine = get_engine()
    if df.empty:
        return 0

    cols = [
        "ticker", "cd_cvm", "source_doc", "tipo_demo", "dt_refer",
        "canonical_key", "valor", "unidade", "qualidade_mapeamento", "row_hash",
    ]
    cols = [c for c in cols if c in df.columns]

    sql = text(
        f"""
        INSERT INTO public.cvm_financial_normalized
            ({", ".join(cols)})
        VALUES
            ({", ".join(f":{c}" for c in cols)})
        ON CONFLICT (ticker, source_doc, tipo_demo, dt_refer, canonical_key, row_hash)
        DO NOTHING
        """
    )

    df_to_write = df[cols].copy()
    if "dt_refer" in df_to_write.columns:
        df_to_write["dt_refer"] = pd.to_datetime(df_to_write["dt_refer"], errors="coerce").dt.date
    df_to_write = df_to_write.astype(object).where(pd.notna(df_to_write), None)

    records = df_to_write.to_dict(orient="records")
    inserted = 0
    for i in range(0, len(records), chunksize):
        batch = records[i: i + chunksize]
        with engine.begin() as conn:
            conn.execute(sql, batch)
        inserted += len(batch)

    return inserted


def main() -> None:
    try:
        from core.cvm_v2_schema_check import assert_v2_schema_ready
        assert_v2_schema_ready()
    except ImportError:
        pass

    engine = get_engine()
    t0 = time.time()

    log("Carregando public.cvm_account_map (ativo=TRUE) …")
    mappings = fetch_mapping(engine)
    log(f"Mapeamentos ativos: {len(mappings)}")
    if mappings.empty:
        log("cvm_account_map não contém registros ativos — nada para mapear.")
        return

    bundle = build_mapping_bundle(mappings)
    log(
        f"Regras compiladas: {len(bundle.exact_map)} exatas | "
        f"{len(bundle.regex_rules)} regex"
    )

    total_raw = 0
    total_mapped = 0
    total_unmatched = 0
    total_skipped_null = 0
    total_inserted = 0
    chunk_count = 0

    log(f"Lendo public.cvm_financial_raw em lotes de {READ_CHUNK_SIZE:,} linhas …")
    for chunk_count, df_chunk in enumerate(fetch_raw_chunks(engine, READ_CHUNK_SIZE), start=1):
        log(f"Chunk {chunk_count}: raw carregado com {len(df_chunk):,} linhas")
        df_norm, stats = normalize_chunk(df_chunk, bundle)

        total_raw += stats.raw_rows
        total_mapped += stats.mapped_rows
        total_unmatched += stats.unmatched_rows
        total_skipped_null += stats.skipped_null_value_rows

        if df_norm.empty:
            log(
                f"Chunk {chunk_count}: nenhuma linha normalizada | "
                f"sem match={stats.unmatched_rows:,} | valor nulo={stats.skipped_null_value_rows:,}"
            )
            continue

        inserted = save(df_norm, engine, chunksize=WRITE_CHUNK_SIZE)
        total_inserted += inserted
        log(
            f"Chunk {chunk_count}: normalizadas={len(df_norm):,} | inseridas={inserted:,} | "
            f"sem match={stats.unmatched_rows:,} | valor nulo={stats.skipped_null_value_rows:,}"
        )

    elapsed = round(time.time() - t0, 1)

    if chunk_count == 0:
        log("cvm_financial_raw está vazio — nada para normalizar.")
        return

    log(
        "Normalização concluída | "
        f"chunks={chunk_count:,} | raw={total_raw:,} | normalizadas={total_mapped:,} | "
        f"inseridas={total_inserted:,} | sem match={total_unmatched:,} | "
        f"valor nulo={total_skipped_null:,} | tempo={elapsed}s"
    )


if __name__ == "__main__":
    main()
