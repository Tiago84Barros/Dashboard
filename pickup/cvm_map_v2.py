"""
pickup/cvm_map_v2.py
Camada de normalização CVM V2.

Lê public.cvm_financial_raw, aplica mapeamento de contas de
public.cvm_account_map e grava em public.cvm_financial_normalized.

Pré-requisito: schema CVM V2 aplicado ao banco (DDL institucional).
"""
from __future__ import annotations

import os
import re
import time
from typing import Optional

import pandas as pd
from sqlalchemy import text

from core.db import get_engine

from datetime import datetime as _dt

LOG_PREFIX = os.getenv("LOG_PREFIX", "[CVM_MAP_V2]")
# Linhas lidas por chunk do banco — mantém uso de memória controlado.
RAW_CHUNK_SIZE = int(os.getenv("MAP_CHUNK_SIZE", "50000"))
# Linhas por batch no INSERT.
INSERT_CHUNK_SIZE = int(os.getenv("MAP_INSERT_CHUNK", "5000"))


def log(msg: str) -> None:
    print(f"{LOG_PREFIX} {msg}", flush=True)


def _register_run(run_id: str, status: str, metrics: dict) -> None:
    """Registra o run de normalização em cvm_ingestion_runs."""
    try:
        import json as _json
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO public.cvm_ingestion_runs
                        (run_id, source_doc, status, metrics, finished_at, updated_at)
                    VALUES
                        (:run_id, 'MAP_V2', :status, :metrics::jsonb, NOW(), NOW())
                    ON CONFLICT (run_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        metrics = EXCLUDED.metrics,
                        finished_at = NOW(),
                        updated_at = NOW()
                    """
                ),
                {
                    "run_id": run_id,
                    "status": status,
                    "metrics": _json.dumps(metrics, ensure_ascii=False, default=str),
                },
            )
    except Exception as exc:
        log(f"[WARN] _register_run falhou (não crítico): {exc}")


def fetch_mapping(engine=None) -> pd.DataFrame:
    """Carrega mapeamento de contas ativo de public.cvm_account_map."""
    if engine is None:
        engine = get_engine()
    query = text(
        "SELECT cd_conta, ds_conta_pattern, canonical_key, sinal "
        "FROM public.cvm_account_map WHERE ativo = TRUE ORDER BY prioridade"
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def _build_exact_index(mappings: pd.DataFrame) -> dict:
    """Pré-compila índice cd_conta → (canonical_key, sinal, quality) para lookup O(1)."""
    idx = {}
    for _, m in mappings.iterrows():
        cd = m.get("cd_conta")
        if cd and pd.notna(cd):
            key = str(cd).strip()
            if key and key not in idx:
                idx[key] = (m["canonical_key"], float(m.get("sinal") or 1.0), "exact")
    return idx


def _build_regex_list(mappings: pd.DataFrame) -> list:
    """Pré-compila lista de (compiled_pattern, canonical_key, sinal) para lookup sequencial."""
    result = []
    for _, m in mappings.iterrows():
        pat = m.get("ds_conta_pattern")
        if pat and pd.notna(pat):
            try:
                compiled = re.compile(str(pat), re.IGNORECASE)
                result.append((compiled, m["canonical_key"], float(m.get("sinal") or 1.0)))
            except re.error:
                pass
    return result


def _apply_mapping_chunk(chunk: pd.DataFrame, exact_idx: dict, regex_list: list) -> pd.DataFrame:
    """Aplica mapeamento vetorizado em um chunk do DataFrame raw.

    Estratégia:
    1. Match exato por cd_conta via dicionário pré-compilado (O(1) por linha).
    2. Para linhas sem match exato, tenta regex sequencialmente.
    Linhas sem nenhum match são descartadas.
    """
    if chunk.empty:
        return pd.DataFrame()

    chunk = chunk[chunk["vl_conta"].notna()].copy()
    if chunk.empty:
        return pd.DataFrame()

    # Exact match: vectorized via map
    cd_series = chunk["cd_conta"].astype(str).str.strip()
    exact_hits = cd_series.map(exact_idx)

    rows_out = []
    unmatched_mask = exact_hits.isna()

    # Process exact matches
    matched = chunk[~unmatched_mask].copy()
    if not matched.empty:
        def _unpack(row):
            hit = exact_idx.get(str(row["cd_conta"]).strip())
            if hit is None:
                return None
            canonical_key, sinal, quality = hit
            return {
                "ticker": row["ticker"],
                "cd_cvm": row["cd_cvm"],
                "source_doc": row["source_doc"],
                "tipo_demo": row["tipo_demo"],
                "dt_refer": row["dt_refer"],
                "canonical_key": canonical_key,
                "valor": float(row["vl_conta"]) * sinal,
                "unidade": "BRL",
                "qualidade_mapeamento": quality,
                "row_hash": row["row_hash"],
            }
        for rec in matched.apply(_unpack, axis=1):
            if rec is not None:
                rows_out.append(rec)

    # Process unmatched via regex (only if regex_list is non-empty)
    if regex_list:
        unmatched = chunk[unmatched_mask].copy()
        for _, row in unmatched.iterrows():
            ds = str(row.get("ds_conta") or "")
            for compiled_pat, canonical_key, sinal in regex_list:
                if compiled_pat.search(ds):
                    rows_out.append({
                        "ticker": row["ticker"],
                        "cd_cvm": row["cd_cvm"],
                        "source_doc": row["source_doc"],
                        "tipo_demo": row["tipo_demo"],
                        "dt_refer": row["dt_refer"],
                        "canonical_key": canonical_key,
                        "valor": float(row["vl_conta"]) * sinal,
                        "unidade": "BRL",
                        "qualidade_mapeamento": "regex",
                        "row_hash": row["row_hash"],
                    })
                    break

    return pd.DataFrame(rows_out) if rows_out else pd.DataFrame()


def save_chunk(df: pd.DataFrame, engine, sql: text, cols: list) -> int:
    """Grava um chunk em public.cvm_financial_normalized."""
    if df.empty:
        return 0
    records = df[cols].where(pd.notnull(df[cols]), None).to_dict(orient="records")
    inserted = 0
    for i in range(0, len(records), INSERT_CHUNK_SIZE):
        batch = records[i: i + INSERT_CHUNK_SIZE]
        with engine.begin() as conn:
            conn.execute(sql, batch)
        inserted += len(batch)
    return inserted


def main() -> None:
    # ── Validação de pré-condição: schema V2 deve existir ──────────────────
    try:
        from core.cvm_v2_schema_check import assert_v2_schema_ready
        assert_v2_schema_ready()
    except ImportError:
        pass   # módulo de checagem não disponível — prossegue

    run_id = f"map_v2_{_dt.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
    engine = get_engine()
    t0 = time.time()

    log("Carregando public.cvm_account_map (ativo=TRUE) …")
    mappings = fetch_mapping(engine)
    log(f"Mapeamentos ativos: {len(mappings)}")

    if mappings.empty:
        log("ERRO: cvm_account_map não contém registros ativos — pipeline interrompido.")
        log("Popule public.cvm_account_map com cd_conta/ds_conta_pattern, canonical_key e ativo=TRUE.")
        _register_run(run_id, "failed", {"message": "cvm_account_map vazio ou sem registros ativo=TRUE"})
        return

    # Pré-compila índices para performance
    exact_idx = _build_exact_index(mappings)
    regex_list = _build_regex_list(mappings)
    log(f"Índice exato: {len(exact_idx)} entradas | Regex: {len(regex_list)} padrões")

    cols_upsert = [
        "ticker", "cd_cvm", "source_doc", "tipo_demo", "dt_refer",
        "canonical_key", "valor", "unidade", "qualidade_mapeamento", "row_hash",
    ]
    sql_upsert = text(
        f"""
        INSERT INTO public.cvm_financial_normalized
            ({", ".join(cols_upsert)})
        VALUES
            ({", ".join(f":{c}" for c in cols_upsert)})
        ON CONFLICT (ticker, source_doc, tipo_demo, dt_refer, canonical_key, row_hash)
        DO NOTHING
        """
    )

    # ── Leitura e normalização em chunks ───────────────────────────────────
    raw_cols = "ticker, cd_cvm, source_doc, tipo_demo, dt_refer, cd_conta, ds_conta, vl_conta, row_hash"
    count_query = text("SELECT COUNT(*) FROM public.cvm_financial_raw")
    with engine.connect() as conn:
        total_raw = conn.execute(count_query).scalar() or 0
    log(f"Total de linhas raw a processar: {total_raw:,} | chunk_size={RAW_CHUNK_SIZE:,}")

    total_inserted = 0
    total_matched = 0
    chunk_num = 0

    raw_query = f"SELECT {raw_cols} FROM public.cvm_financial_raw ORDER BY source_doc, dt_refer"

    with engine.connect() as conn:
        for chunk in pd.read_sql(text(raw_query), conn, chunksize=RAW_CHUNK_SIZE):
            chunk_num += 1
            t_chunk = time.time()

            df_norm = _apply_mapping_chunk(chunk, exact_idx, regex_list)
            total_matched += len(df_norm)

            if not df_norm.empty:
                ins = save_chunk(df_norm, engine, sql_upsert, cols_upsert)
                total_inserted += ins

            elapsed_chunk = round(time.time() - t_chunk, 1)
            log(
                f"  Chunk {chunk_num}: {len(chunk):,} linhas raw "
                f"→ {len(df_norm):,} mapeadas, {total_inserted:,} acumuladas "
                f"({elapsed_chunk}s)"
            )

    elapsed = round(time.time() - t0, 1)
    log(
        f"Normalização concluída: {total_raw:,} raw lidas, "
        f"{total_matched:,} mapeadas, {total_inserted:,} inseridas em {elapsed}s."
    )

    final_status = "success" if total_inserted > 0 else "failed"
    _register_run(
        run_id,
        final_status,
        {
            "message": f"{total_raw:,} raw lidas, {total_matched:,} mapeadas, {total_inserted:,} inseridas em {elapsed}s.",
            "total_raw": total_raw,
            "total_matched": total_matched,
            "total_inserted": total_inserted,
            "elapsed_s": elapsed,
        },
    )

    if total_inserted == 0 and total_raw > 0:
        log(
            "AVISO: 0 linhas inseridas apesar de raw ter dados. "
            "Verifique se cd_conta em cvm_account_map corresponde aos valores reais em cvm_financial_raw."
        )


if __name__ == "__main__":
    main()
