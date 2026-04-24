"""
pipeline_local/publish/publish_financials_supabase.py
Publicação seletiva: financials_annual/quarterly_final_local → Supabase.

Estratégia de publish:
  - Chave natural annual:    (ticker, dt_refer) → public.Demonstracoes_Financeiras
  - Chave natural quarterly: (ticker, dt_refer) → public.Demonstracoes_Financeiras_TRI
  - Upsert com detecção de mudança por row_hash
  - Dry-run disponível para validação antes de publicar
  - Log de publicação em pipeline_local.pipeline_publish_log_local

Variáveis de ambiente:
  LOCAL_DB_URL        obrigatória
  SUPABASE_DB_URL     obrigatória para publish
  PUBLISH_MODE        dry_run | publish (default dry_run — seguro por padrão)
  PUBLISH_SOURCE      annual | quarterly | all (default all)
  PUBLISH_TICKERS     lista separada por vírgula para publish parcial (opcional)
  PUBLISH_YEAR_START  ano mínimo a publicar (opcional)
  PUBLISH_YEAR_END    ano máximo a publicar (opcional)
  PUBLISH_BATCH_SIZE  linhas por batch de upsert no Supabase (default 500)

Uso típico:
  # Ver o que seria publicado sem alterar nada
  PUBLISH_MODE=dry_run python -m pipeline_local.publish.publish_financials_supabase

  # Publicar de fato
  PUBLISH_MODE=publish python -m pipeline_local.publish.publish_financials_supabase

  # Publish parcial de tickers específicos
  PUBLISH_MODE=publish PUBLISH_TICKERS=PETR3,VALE3 python -m pipeline_local.publish.publish_financials_supabase
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text as sa_text

from pipeline_local.config.connections import get_local_engine, get_supabase_engine
from pipeline_local.config.settings import load_settings
from pipeline_local.utils.logger import get_logger

log = get_logger("publish_financials")

PUBLISH_MODE = os.getenv("PUBLISH_MODE", "dry_run").strip().lower()       # dry_run | publish
PUBLISH_SOURCE = os.getenv("PUBLISH_SOURCE", "all").strip().lower()       # annual | quarterly | all
PUBLISH_TICKERS_RAW = os.getenv("PUBLISH_TICKERS", "").strip()
PUBLISH_TICKERS: List[str] = [t.strip().upper() for t in PUBLISH_TICKERS_RAW.split(",") if t.strip()] if PUBLISH_TICKERS_RAW else []
PUBLISH_YEAR_START = os.getenv("PUBLISH_YEAR_START", "").strip()
PUBLISH_YEAR_END = os.getenv("PUBLISH_YEAR_END", "").strip()
PUBLISH_BATCH_SIZE = int(os.getenv("PUBLISH_BATCH_SIZE", "500"))

# Mapeamento local → Supabase
_PUBLISH_JOBS = {
    "annual": {
        "local_table": "pipeline_local.financials_annual_final_local",
        "remote_table": 'public."Demonstracoes_Financeiras"',
        "natural_key": ["Ticker", "data"],
        "local_key": ["ticker", "dt_refer"],
    },
    "quarterly": {
        "local_table": "pipeline_local.financials_quarterly_final_local",
        "remote_table": 'public."Demonstracoes_Financeiras_TRI"',
        "natural_key": ["Ticker", "data"],
        "local_key": ["ticker", "dt_refer"],
    },
}

# Mapeamento de colunas local → Supabase (nome legado do app)
_COL_MAP_TO_REMOTE = {
    "ticker": "Ticker",
    "dt_refer": "data",
    "denom_cia": "Nome",
    "receita_liquida": "Receita_Liquida",
    "lucro_bruto": "Lucro_Bruto",
    "lucro_liquido": "Lucro_Liquido",
    "ebit": "EBIT",
    "ebitda": "EBITDA",
    "lpa": "LPA",
    "ativo_total": "Ativo_Total",
    "ativo_circulante": "Ativo_Circulante",
    "caixa_equivalentes": "Caixa",
    "patrimonio_liquido": "Patrimonio_Liquido",
    "divida_bruta": "Divida_Bruta",
    "divida_liquida": "Divida_Liquida",
    "fco": "FCO",
    "capex": "CAPEX",
    "passivo_circulante": "Passivo_Circulante",
    "passivo_nao_circulante": "Passivo_Nao_Circulante",
    "quality_score": "quality_score",
    "source_doc": "source_doc",
}


# ---------------------------------------------------------------------------
# Leitura local
# ---------------------------------------------------------------------------
def _load_local(local_table: str, local_key: List[str]) -> pd.DataFrame:
    settings = load_settings()
    engine = get_local_engine()

    where_clauses: List[str] = []
    if PUBLISH_TICKERS:
        tickers_sql = ", ".join(f"'{t}'" for t in PUBLISH_TICKERS)
        where_clauses.append(f"ticker IN ({tickers_sql})")
    if PUBLISH_YEAR_START:
        where_clauses.append(f"EXTRACT(YEAR FROM dt_refer) >= {PUBLISH_YEAR_START}")
    if PUBLISH_YEAR_END:
        where_clauses.append(f"EXTRACT(YEAR FROM dt_refer) <= {PUBLISH_YEAR_END}")

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    query = f"SELECT * FROM {local_table} {where} ORDER BY ticker, dt_refer"

    with engine.connect() as conn:
        df = pd.read_sql(sa_text(query), conn)
    return df


# ---------------------------------------------------------------------------
# Mapeamento de colunas para o schema legado do Supabase
# ---------------------------------------------------------------------------
def _map_to_remote_schema(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    rename = {k: v for k, v in _COL_MAP_TO_REMOTE.items() if k in df.columns}
    df = df.rename(columns=rename)

    # Garante que Ticker está sem .SA
    if "Ticker" in df.columns:
        df["Ticker"] = df["Ticker"].astype(str).str.replace(".SA", "", regex=False).str.strip().str.upper()

    # data como string YYYY-MM-DD
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.strftime("%Y-%m-%d")

    return df


# ---------------------------------------------------------------------------
# Leitura do estado atual no Supabase (para detecção de delta)
# ---------------------------------------------------------------------------
def _load_remote_hashes(remote_table: str, natural_key: List[str], tickers: Optional[List[str]]) -> Dict[tuple, str]:
    """Retorna dict (Ticker, data) → quality_score para detecção de mudança."""
    try:
        engine = get_supabase_engine()
        where = ""
        if tickers:
            tks = ", ".join(f"'{t}'" for t in tickers)
            where = f'WHERE "Ticker" IN ({tks})'
        with engine.connect() as conn:
            rows = conn.execute(
                sa_text(f'SELECT "Ticker", "data", quality_score FROM {remote_table} {where}')
            ).fetchall()
        return {(str(r[0]), str(r[1])): r[2] for r in rows}
    except Exception as exc:
        log.warning("Não foi possível ler estado remoto", tabela=remote_table, erro=str(exc))
        return {}


# ---------------------------------------------------------------------------
# Upsert no Supabase
# ---------------------------------------------------------------------------
def _upsert_remote(df: pd.DataFrame, remote_table: str, natural_key: List[str]) -> Dict[str, int]:
    engine = get_supabase_engine()
    cols = [c for c in df.columns if c in list(_COL_MAP_TO_REMOTE.values())]
    if not cols:
        return {"upserted": 0, "error": 0}

    for col in natural_key:
        if col not in cols:
            cols.insert(0, col)

    df = df[[c for c in cols if c in df.columns]].copy()
    set_clause = ", ".join(
        f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c not in natural_key
    )
    key_cols_sql = ", ".join(f'"{c}"' for c in natural_key)
    insert_cols_sql = ", ".join(f'"{c}"' for c in df.columns)
    values_sql = ", ".join(f":{c}" for c in df.columns)

    upserted = errors = 0
    records = df.where(pd.notna(df), other=None).to_dict("records")

    for i in range(0, len(records), PUBLISH_BATCH_SIZE):
        batch = records[i: i + PUBLISH_BATCH_SIZE]
        try:
            with engine.begin() as conn:
                for rec in batch:
                    conn.execute(
                        sa_text(f"""
                            INSERT INTO {remote_table} ({insert_cols_sql})
                            VALUES ({values_sql})
                            ON CONFLICT ({key_cols_sql}) DO UPDATE SET
                                {set_clause}
                        """),
                        rec,
                    )
                    upserted += 1
        except Exception as exc:
            log.error("Falha no batch de upsert", tabela=remote_table, erro=str(exc), batch_start=i)
            errors += len(batch)
    return {"upserted": upserted, "error": errors}


# ---------------------------------------------------------------------------
# Registro do publish log
# ---------------------------------------------------------------------------
def _log_publish(run_id: str, target_table: str, mode: str, counts: Dict[str, int], key_columns: List[str]) -> None:
    try:
        engine = get_local_engine()
        with engine.begin() as conn:
            conn.execute(
                sa_text("""
                    INSERT INTO pipeline_local.pipeline_publish_log_local
                        (run_id, target_table, publish_mode, rows_published, rows_skipped, rows_error, key_columns, metrics)
                    VALUES
                        (:run_id, :target_table, :publish_mode, :rows_published, :rows_skipped, :rows_error, :key_columns, CAST(:metrics AS jsonb))
                """),
                {
                    "run_id": run_id,
                    "target_table": target_table,
                    "publish_mode": mode,
                    "rows_published": counts.get("upserted", 0),
                    "rows_skipped": counts.get("skipped", 0),
                    "rows_error": counts.get("error", 0),
                    "key_columns": key_columns,
                    "metrics": json.dumps(counts, default=str),
                },
            )
    except Exception as exc:
        log.warning("Falha ao gravar publish log", erro=str(exc))


# ---------------------------------------------------------------------------
# Orquestrador principal
# ---------------------------------------------------------------------------
def run(
    mode: Optional[str] = None,
    source: Optional[str] = None,
    tickers: Optional[List[str]] = None,
) -> Dict[str, int]:
    mode = (mode or PUBLISH_MODE).lower()
    source = (source or PUBLISH_SOURCE).lower()
    tickers = tickers or PUBLISH_TICKERS or None
    run_id = str(uuid.uuid4())

    log.info("Iniciando publish", run_id=run_id, mode=mode, source=source, tickers=tickers)

    if mode not in ("dry_run", "publish"):
        raise ValueError(f"PUBLISH_MODE inválido: {mode}. Use 'dry_run' ou 'publish'.")

    jobs = {k: v for k, v in _PUBLISH_JOBS.items() if source in ("all", k)}
    total_upserted = total_skipped = total_error = 0

    for job_name, job in jobs.items():
        log.info("Processando job", job=job_name, local=job["local_table"], remote=job["remote_table"])

        df_local = _load_local(job["local_table"], job["local_key"])
        if df_local.empty:
            log.warning("Tabela local vazia — pulando", job=job_name)
            continue

        remote_hashes = _load_remote_hashes(
            job["remote_table"], job["natural_key"],
            tickers=(tickers or df_local["ticker"].unique().tolist()),
        )

        df_remote = _map_to_remote_schema(df_local)

        # Detectar linhas que mudaram (ou são novas)
        changed_mask = df_remote.apply(
            lambda row: remote_hashes.get((row.get("Ticker", ""), str(row.get("data", "")))) != row.get("quality_score"),
            axis=1,
        )
        df_to_publish = df_remote[changed_mask]
        skipped_count = len(df_remote) - len(df_to_publish)

        log.info(
            "Delta calculado",
            job=job_name,
            total=len(df_remote),
            changed=len(df_to_publish),
            skipped=skipped_count,
        )

        if mode == "dry_run":
            log.info("[DRY RUN] Nenhuma linha publicada", job=job_name, would_publish=len(df_to_publish))
            counts = {"upserted": 0, "skipped": skipped_count, "error": 0, "would_publish": len(df_to_publish)}
        else:
            counts = _upsert_remote(df_to_publish, job["remote_table"], job["natural_key"])
            counts["skipped"] = skipped_count
            log.info("Publish concluído", job=job_name, **counts)

            # Marca published_at na tabela local
            _mark_published(job["local_table"], df_to_publish)

        _log_publish(run_id, job["remote_table"], mode, counts, job["natural_key"])

        total_upserted += counts.get("upserted", 0)
        total_skipped += counts.get("skipped", 0)
        total_error += counts.get("error", 0)

    log.summary(
        pipeline=f"publish_financials_{mode}",
        status="success" if total_error == 0 else "partial",
        run_id=run_id,
        rows_published=total_upserted,
        rows_skipped=total_skipped,
        rows_error=total_error,
        mode=mode,
    )
    return {"published": total_upserted, "skipped": total_skipped, "error": total_error}


def _mark_published(local_table: str, df: pd.DataFrame) -> None:
    """Atualiza published_at nas linhas que acabaram de ser publicadas."""
    if df.empty or "Ticker" not in df.columns or "data" not in df.columns:
        return
    try:
        engine = get_local_engine()
        with engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(
                    sa_text(f"""
                        UPDATE {local_table}
                        SET published_at = now()
                        WHERE ticker = :ticker AND dt_refer = :dt_refer::date
                    """),
                    {"ticker": row["Ticker"], "dt_refer": row["data"]},
                )
    except Exception as exc:
        log.warning("Falha ao marcar published_at", erro=str(exc))


def main() -> None:
    run()


if __name__ == "__main__":
    main()
