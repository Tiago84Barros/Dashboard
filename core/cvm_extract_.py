# cvm_extract_v2_progress.py
# Versão com progresso incremental por ano

import json
from sqlalchemy import text

def _update_run_progress(run_id, **kwargs):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE cvm_ingestion_runs
                SET metrics = COALESCE(metrics, '{}'::jsonb) || :metrics,
                    updated_at = NOW()
                WHERE run_id = :run_id
            """),
            {
                "run_id": run_id,
                "metrics": json.dumps(kwargs)
            }
        )

def run_extract_incremental(run_id: str):
    session = build_session()
    ticker_map = _load_ticker_map()

    years, last_year = _discover_years_and_last_year(session)
    total_years = len(years)

    raw_rows_accum = 0
    inserted_rows_accum = 0

    for idx, year in enumerate(years, start=1):
        _update_run_progress(
            run_id,
            current_year=year,
            years_done=idx - 1,
            years_total=total_years,
            raw_rows_accum=raw_rows_accum,
            inserted_rows_accum=inserted_rows_accum,
            stage="processing_year"
        )

        data = process_year(session, year, ticker_map)
        rows = data.get("rows", [])

        raw_rows_accum += len(rows)

        if rows:
            inserted = upsert_cvm_financial_raw(rows, run_id)
            inserted_rows_accum += inserted

        _update_run_progress(
            run_id,
            current_year=year,
            years_done=idx,
            years_total=total_years,
            raw_rows_accum=raw_rows_accum,
            inserted_rows_accum=inserted_rows_accum,
            stage="year_done"
        )

    return inserted_rows_accum, last_year
