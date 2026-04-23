"""
pickup/cvm_map_v2.py
Camada de normalização CVM V2.

Lê public.cvm_financial_raw, aplica mapeamento de contas de
public.cvm_account_map e grava em public.cvm_financial_normalized.

Pré-requisito: schema CVM V2 aplicado ao banco (DDL institucional).
"""
from __future__ import annotations

import os
import time
from typing import Any

import pandas as pd
from sqlalchemy import text

from core.db import get_engine
from core.cvm_rule_engine import build_rule_indexes, prepare_rules, select_best_rule

from datetime import datetime as _dt

LOG_PREFIX = os.getenv("LOG_PREFIX", "[CVM_MAP_V2]")
RAW_CHUNK_SIZE = int(os.getenv("MAP_CHUNK_SIZE", "10000"))
INSERT_CHUNK_SIZE = int(os.getenv("MAP_INSERT_CHUNK", "5000"))
MAP_SOURCE_DOC = (os.getenv("MAP_SOURCE_DOC") or "ALL").strip().upper()
MAP_TIPO_DEMO = (os.getenv("MAP_TIPO_DEMO") or "ALL").strip().upper()
MAP_YEAR_START = (os.getenv("MAP_YEAR_START") or "").strip()
MAP_YEAR_END = (os.getenv("MAP_YEAR_END") or "").strip()
MAP_MAX_ROWS = (os.getenv("MAP_MAX_ROWS") or "").strip()
MAP_ONLY_RULE_CODES = (os.getenv("MAP_ONLY_RULE_CODES") or "0").strip().lower() in {"1", "true", "yes", "y", "on"}


def log(msg: str) -> None:
    print(f"{LOG_PREFIX} {msg}", flush=True)


def _safe_int_env(value: str | None) -> int | None:
    if value in (None, "", "0"):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _register_run(run_id: str, status: str, metrics: dict) -> None:
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
                        (:run_id, 'MAP_V2', :status, CAST(:metrics AS jsonb), NOW(), NOW())
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


def _get_table_columns(engine, schema: str, table: str) -> set[str]:
    query = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
    return {str(r[0]) for r in rows}


def fetch_mapping(engine=None) -> pd.DataFrame:
    if engine is None:
        engine = get_engine()

    available = _get_table_columns(engine, "public", "cvm_account_map")
    desired = [
        "cd_conta",
        "ds_conta_pattern",
        "canonical_key",
        "sinal",
        "prioridade",
        "priority",
        "confidence_score",
        "rule_scope",
        "source_doc",
        "statement_type",
        "parent_cd_conta",
        "level_min",
        "level_max",
        "sector",
        "company_cvm",
        "valid_from",
        "valid_to",
        "notes",
    ]

    select_parts = []
    for col in desired:
        if col in available:
            select_parts.append(col)
        else:
            select_parts.append(f"NULL AS {col}")

    order_expr = "COALESCE(priority, prioridade, 0) DESC, COALESCE(confidence_score, 1.0) DESC"
    query = text(
        f"SELECT {', '.join(select_parts)} "
        f"FROM public.cvm_account_map WHERE ativo = TRUE ORDER BY {order_expr}"
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def _extract_rule_codes(rules: pd.DataFrame) -> list[str]:
    if rules is None or rules.empty or "cd_conta" not in rules.columns:
        return []
    codes = (
        rules["cd_conta"]
        .astype(str)
        .str.strip()
        .replace({"nan": "", "None": ""})
    )
    uniq = [code for code in codes.tolist() if code]
    return sorted(set(uniq))


def _apply_mapping_chunk(
    chunk: pd.DataFrame,
    fast_exact_idx: dict,
    exact_candidates_idx: dict,
    regex_rules: list,
) -> tuple[pd.DataFrame, dict[str, int]]:
    stats = {
        "rows_input": int(len(chunk)),
        "rows_non_null": 0,
        "mapped_fast": 0,
        "mapped_contextual": 0,
        "mapped_regex": 0,
        "ambiguous": 0,
        "unmatched": 0,
    }

    if chunk.empty:
        return pd.DataFrame(), stats

    chunk = chunk[chunk["vl_conta"].notna()].copy()
    stats["rows_non_null"] = int(len(chunk))
    if chunk.empty:
        return pd.DataFrame(), stats

    rows_out: list[dict[str, Any]] = []

    for row in chunk.to_dict(orient="records"):
        cd_conta = str(row.get("cd_conta") or "").strip()
        selected_rule = None
        quality = None
        conflict = False

        fast_rule = fast_exact_idx.get(cd_conta)
        if fast_rule is not None:
            selected_rule = fast_rule
            quality = "exact_fast"
            stats["mapped_fast"] += 1
        else:
            candidates = exact_candidates_idx.get(cd_conta, [])
            if candidates:
                selected_rule, conflict = select_best_rule(row, candidates, require_pattern=False)
                if conflict:
                    stats["ambiguous"] += 1
                    continue
                if selected_rule is not None:
                    quality = "exact_context"
                    stats["mapped_contextual"] += 1

            if selected_rule is None and regex_rules:
                selected_rule, conflict = select_best_rule(row, regex_rules, require_pattern=True)
                if conflict:
                    stats["ambiguous"] += 1
                    continue
                if selected_rule is not None:
                    quality = "regex_context"
                    stats["mapped_regex"] += 1

        if selected_rule is None:
            stats["unmatched"] += 1
            continue

        sinal = float(selected_rule.get("sinal_effective") or 1.0)
        rows_out.append(
            {
                "ticker": row.get("ticker"),
                "cd_cvm": row.get("cd_cvm"),
                "source_doc": row.get("source_doc"),
                "tipo_demo": row.get("tipo_demo"),
                "dt_refer": row.get("dt_refer"),
                "canonical_key": selected_rule.get("canonical_key"),
                "valor": float(row.get("vl_conta")) * sinal,
                "unidade": "BRL",
                "qualidade_mapeamento": quality,
                "row_hash": row.get("row_hash"),
            }
        )

    return (pd.DataFrame(rows_out) if rows_out else pd.DataFrame()), stats


def save_chunk(df: pd.DataFrame, engine, sql: text, cols: list[str]) -> int:
    if df.empty:
        return 0
    records = df[cols].where(pd.notnull(df[cols]), None).to_dict(orient="records")
    inserted = 0
    for i in range(0, len(records), INSERT_CHUNK_SIZE):
        batch = records[i : i + INSERT_CHUNK_SIZE]
        with engine.begin() as conn:
            conn.execute(sql, batch)
        inserted += len(batch)
    return inserted


def _build_raw_filters(rule_codes: list[str] | None = None) -> tuple[str, dict[str, Any], str]:
    filters = []
    params: dict[str, Any] = {}
    labels = []

    if MAP_SOURCE_DOC and MAP_SOURCE_DOC != "ALL":
        filters.append("source_doc = :map_source_doc")
        params["map_source_doc"] = MAP_SOURCE_DOC
        labels.append(f"source_doc={MAP_SOURCE_DOC}")

    if MAP_TIPO_DEMO and MAP_TIPO_DEMO != "ALL":
        filters.append("UPPER(tipo_demo) = :map_tipo_demo")
        params["map_tipo_demo"] = MAP_TIPO_DEMO
        labels.append(f"tipo_demo={MAP_TIPO_DEMO}")

    year_start = _safe_int_env(MAP_YEAR_START)
    if year_start is not None:
        filters.append("EXTRACT(YEAR FROM dt_refer::date) >= :map_year_start")
        params["map_year_start"] = year_start
        labels.append(f"ano_inicial={year_start}")

    year_end = _safe_int_env(MAP_YEAR_END)
    if year_end is not None:
        filters.append("EXTRACT(YEAR FROM dt_refer::date) <= :map_year_end")
        params["map_year_end"] = year_end
        labels.append(f"ano_final={year_end}")

    if MAP_ONLY_RULE_CODES:
        valid_codes = [code for code in (rule_codes or []) if code]
        if valid_codes:
            filters.append("cd_conta = ANY(:map_rule_codes)")
            params["map_rule_codes"] = valid_codes
            labels.append(f"only_rule_codes={len(valid_codes)}")
        else:
            labels.append("only_rule_codes=0")

    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
    label = ", ".join(labels) if labels else "sem filtros"
    return where_sql, params, label


def main() -> None:
    try:
        from core.cvm_v2_schema_check import assert_v2_schema_ready
        assert_v2_schema_ready()
    except ImportError:
        pass

    run_id = f"map_v2_{_dt.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
    engine = get_engine()
    t0 = time.time()

    log("Carregando public.cvm_account_map (ativo=TRUE) …")
    mappings = fetch_mapping(engine)
    log(f"Mapeamentos ativos: {len(mappings)}")

    if mappings.empty:
        log("ERRO: cvm_account_map não contém registros ativos — pipeline interrompido.")
        log("Popule public.cvm_account_map com regras ativas antes de executar a normalização.")
        _register_run(run_id, "failed", {"message": "cvm_account_map vazio ou sem registros ativo=TRUE"})
        return

    rules = prepare_rules(mappings)
    if rules.empty:
        log("ERRO: nenhuma regra utilizável foi encontrada em cvm_account_map.")
        _register_run(run_id, "failed", {"message": "nenhuma regra utilizável encontrada em cvm_account_map"})
        return

    fast_exact_idx, exact_candidates_idx, regex_rules = build_rule_indexes(rules)
    rule_codes = _extract_rule_codes(rules)
    log(
        "Regras preparadas: "
        f"fast_exact={len(fast_exact_idx)} | "
        f"exact_contextual={sum(len(v) for v in exact_candidates_idx.values())} | "
        f"regex={len(regex_rules)} | "
        f"rule_codes={len(rule_codes)}"
    )

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

    raw_cols = (
        "ticker, cd_cvm, source_doc, tipo_demo, dt_refer, "
        "cd_conta, ds_conta, conta_pai, nivel_conta, vl_conta, row_hash"
    )
    where_sql, query_params, filters_label = _build_raw_filters(rule_codes=rule_codes)

    count_query = text(f"SELECT COUNT(*) FROM public.cvm_financial_raw {where_sql}")
    with engine.connect() as conn:
        total_raw = conn.execute(count_query, query_params).scalar() or 0
    log(
        f"Total de linhas raw a processar: {total_raw:,} | "
        f"chunk_size={RAW_CHUNK_SIZE:,} | filtros={filters_label}"
    )

    if total_raw == 0:
        msg = f"Nenhuma linha encontrada para os filtros atuais ({filters_label})."
        log(msg)
        _register_run(run_id, "failed", {"message": msg, "filters": filters_label, "chunk_size": RAW_CHUNK_SIZE})
        return

    total_inserted = 0
    total_matched = 0
    total_ambiguous = 0
    total_unmatched = 0
    total_fast = 0
    total_contextual = 0
    total_regex = 0
    chunk_num = 0

    raw_query = f"SELECT {raw_cols} FROM public.cvm_financial_raw {where_sql}"
    max_rows = _safe_int_env(MAP_MAX_ROWS)
    if max_rows is not None:
        raw_query += " LIMIT :map_max_rows"
        query_params = {**query_params, "map_max_rows": max_rows}
        filters_label = f"{filters_label}, max_rows={max_rows}" if filters_label != "sem filtros" else f"max_rows={max_rows}"

    try:
        with engine.connect() as conn:
            for chunk in pd.read_sql(text(raw_query), conn, params=query_params, chunksize=RAW_CHUNK_SIZE):
                chunk_num += 1
                t_chunk = time.time()

                df_norm, chunk_stats = _apply_mapping_chunk(
                    chunk,
                    fast_exact_idx,
                    exact_candidates_idx,
                    regex_rules,
                )
                total_matched += len(df_norm)
                total_ambiguous += chunk_stats["ambiguous"]
                total_unmatched += chunk_stats["unmatched"]
                total_fast += chunk_stats["mapped_fast"]
                total_contextual += chunk_stats["mapped_contextual"]
                total_regex += chunk_stats["mapped_regex"]

                if not df_norm.empty:
                    ins = save_chunk(df_norm, engine, sql_upsert, cols_upsert)
                    total_inserted += ins

                elapsed_chunk = round(time.time() - t_chunk, 1)
                log(
                    f"  Chunk {chunk_num}: {len(chunk):,} raw "
                    f"→ {len(df_norm):,} mapeadas "
                    f"(fast={chunk_stats['mapped_fast']:,}, ctx={chunk_stats['mapped_contextual']:,}, regex={chunk_stats['mapped_regex']:,}, "
                    f"amb={chunk_stats['ambiguous']:,}, sem_match={chunk_stats['unmatched']:,}) | "
                    f"{total_inserted:,} inseridas acumuladas ({elapsed_chunk}s)"
                )
    except Exception as exc:
        msg = str(exc)
        if "statement timeout" in msg.lower() or "querycanceled" in msg.lower():
            timeout_msg = "Timeout ao ler public.cvm_financial_raw. Use lote menor por documento/anos e reduza o volume por execução."
            log(f"ERRO: {timeout_msg}")
            _register_run(
                run_id,
                "failed",
                {
                    "message": timeout_msg,
                    "filters": filters_label,
                    "total_raw": total_raw,
                    "chunk_size": RAW_CHUNK_SIZE,
                    "mapped_fast": total_fast,
                    "mapped_contextual": total_contextual,
                    "mapped_regex": total_regex,
                    "ambiguous_rows": total_ambiguous,
                    "unmatched_rows": total_unmatched,
                },
            )
            raise
        raise

    elapsed = round(time.time() - t0, 1)
    log(
        f"Normalização concluída: {total_raw:,} raw lidas, "
        f"{total_matched:,} mapeadas, {total_inserted:,} inseridas em {elapsed}s. "
        f"fast={total_fast:,}, ctx={total_contextual:,}, regex={total_regex:,}, "
        f"amb={total_ambiguous:,}, sem_match={total_unmatched:,}."
    )

    final_status = "success" if total_inserted > 0 else "failed"
    _register_run(
        run_id,
        final_status,
        {
            "message": (
                f"{total_raw:,} raw lidas, {total_matched:,} mapeadas, {total_inserted:,} inseridas em {elapsed}s. "
                f"fast={total_fast:,}, ctx={total_contextual:,}, regex={total_regex:,}, "
                f"amb={total_ambiguous:,}, sem_match={total_unmatched:,}."
            ),
            "filters": filters_label,
            "total_raw": total_raw,
            "total_matched": total_matched,
            "total_inserted": total_inserted,
            "mapped_fast": total_fast,
            "mapped_contextual": total_contextual,
            "mapped_regex": total_regex,
            "ambiguous_rows": total_ambiguous,
            "unmatched_rows": total_unmatched,
            "elapsed_s": elapsed,
            "chunk_size": RAW_CHUNK_SIZE,
            "only_rule_codes": MAP_ONLY_RULE_CODES,
            "rule_codes_count": len(rule_codes),
        },
    )

    if total_inserted == 0 and total_raw > 0:
        log(
            "AVISO: 0 linhas inseridas apesar de raw ter dados. "
            "Verifique regras ativas, validade temporal, prioridade, conflitos e filtros aplicados em cvm_account_map."
        )


if __name__ == "__main__":
    main()
