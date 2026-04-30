"""
pipeline_local/extract/extract_cvm_itr_local.py
Extração de ITRs da CVM → banco local PostgreSQL.

Wrapper do mesmo padrão de extract_cvm_dfp_local.py, adaptado para ITR.
Escreve em pipeline_local.cvm_itr_raw_local (banco LOCAL, não Supabase).

Variáveis de ambiente:
  LOCAL_DB_URL          obrigatória
  PIPELINE_START_YEAR   ano inicial (default 2015 — ITR tem histórico menor)
  PIPELINE_END_YEAR     ano final (default: ano atual)
  PIPELINE_BATCH_SIZE   linhas por batch de insert (default 5000)
"""
from __future__ import annotations

import io
import os
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline_local.config.connections import get_local_engine
from pipeline_local.config.settings import load_settings
from pipeline_local.utils.logger import get_logger
from pipeline_local.utils.hashing import dataframe_row_hash

log = get_logger("extract_cvm_itr")

CVM_ITR_BASE_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"
DEMO_TYPES = ("DRE", "BPA", "BPP", "DFC_MI", "DFC_MD", "DMPL", "DVA")
SOURCE_DOC = "ITR"
TARGET_TABLE = "pipeline_local.cvm_itr_raw_local"

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "180"))
FORCAR_REDOWNLOAD = os.getenv("FORCAR_REDOWNLOAD", "0").strip() == "1"
MAX_ANOS_POR_RUN = int(os.getenv("MAX_ANOS_POR_RUN", "0")) or 9999

CACHE_DIR = Path(os.getenv("CVM_CACHE_DIR_ITR", ".cache_cvm_itr_local"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

_ticker_map: Optional[Dict[int, str]] = None


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _list_available_years(session: requests.Session) -> List[int]:
    resp = session.get(CVM_ITR_BASE_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    years: List[int] = []
    for line in resp.text.splitlines():
        for part in line.split('"'):
            if part.startswith("itr_cia_aberta_") and part.endswith(".zip"):
                try:
                    year = int(part.split("_")[-1].replace(".zip", ""))
                    years.append(year)
                except ValueError:
                    pass
    return sorted(set(years))


def _cached_zip(year: int, session: requests.Session) -> Optional[bytes]:
    cache_file = CACHE_DIR / f"itr_{year}.zip"
    if cache_file.exists() and not FORCAR_REDOWNLOAD:
        return cache_file.read_bytes()
    url = f"{CVM_ITR_BASE_URL}itr_cia_aberta_{year}.zip"
    log.info("Baixando ZIP ITR", ano=year, url=url)
    resp = session.get(url, timeout=REQUEST_TIMEOUT)
    if resp.status_code == 404:
        log.warning("ZIP ITR não encontrado (404)", ano=year)
        return None
    resp.raise_for_status()
    data = resp.content
    cache_file.write_bytes(data)
    return data


def _get_ticker_map() -> Dict[int, str]:
    global _ticker_map
    if _ticker_map is not None:
        return _ticker_map
    try:
        from pipeline_local.config.connections import get_supabase_engine
        from sqlalchemy import text
        engine = get_supabase_engine()
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT cd_cvm, ticker FROM public.cvm_to_ticker")).fetchall()
        _ticker_map = {int(r[0]): str(r[1]) for r in rows if r[0] and r[1]}
    except Exception as exc:
        log.warning("Falha ao carregar ticker map", erro=str(exc))
        _ticker_map = {}
    return _ticker_map


def _parse_zip(zip_bytes: bytes) -> Iterator[pd.DataFrame]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for fname in zf.namelist():
            fname_upper = fname.upper()
            matched_demo = next((d for d in DEMO_TYPES if f"_{d}_" in fname_upper), None)
            if matched_demo is None:
                continue
            with zf.open(fname) as f:
                try:
                    df = pd.read_csv(f, sep=";", encoding="latin-1", dtype=str, on_bad_lines="skip")
                except Exception as exc:
                    log.error("Falha ao ler CSV ITR", arquivo=fname, erro=str(exc))
                    continue
            if df.empty:
                continue
            df.columns = [c.strip().upper() for c in df.columns]
            df["tipo_demo"] = matched_demo
            df["arquivo_origem"] = fname
            yield df


_COLUMN_MAP = {
    "CD_CVM": "cd_cvm", "CNPJ_CIA": "cnpj_cia", "DENOM_CIA": "denom_cia",
    "DT_REFER": "dt_refer", "DT_INI_EXERC": "dt_ini_exerc", "DT_FIM_EXERC": "dt_fim_exerc",
    "VERSAO": "versao", "ORDEM_EXERC": "ordem_exerc", "CD_CONTA": "cd_conta",
    "DS_CONTA": "ds_conta", "NIVEL_CONTA": "nivel_conta", "VL_CONTA": "vl_conta",
    "ESCALA_MOEDA": "escala_moeda", "MOEDA": "moeda", "ST_CONTA_FIXA": "st_conta_fixa",
    "GRUPO_ITR": "grupo_demo", "GRUPO_DFP": "grupo_demo",
}


def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    ticker_map = _get_ticker_map()
    rename = {k: v for k, v in _COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)
    df["source_doc"] = SOURCE_DOC

    if "cd_cvm" in df.columns:
        df["cd_cvm"] = pd.to_numeric(df["cd_cvm"], errors="coerce").astype("Int64")
        df["ticker"] = df["cd_cvm"].map(lambda x: ticker_map.get(int(x), None) if pd.notna(x) else None)
    else:
        df["ticker"] = None

    for col in ("dt_refer", "dt_ini_exerc", "dt_fim_exerc"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    if "vl_conta" in df.columns:
        df["vl_conta"] = pd.to_numeric(df["vl_conta"].astype(str).str.replace(",", "."), errors="coerce")

    for col in ("versao", "nivel_conta"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    hash_cols = [c for c in ("source_doc", "cd_cvm", "tipo_demo", "dt_refer", "versao", "cd_conta", "vl_conta", "ordem_exerc") if c in df.columns]
    df["row_hash"] = dataframe_row_hash(df, hash_cols)
    return df


_KEEP_COLS = [
    "source_doc", "tipo_demo", "grupo_demo", "arquivo_origem",
    "cd_cvm", "cnpj_cia", "denom_cia", "ticker",
    "versao", "ordem_exerc", "dt_refer", "dt_ini_exerc", "dt_fim_exerc",
    "cd_conta", "ds_conta", "nivel_conta", "vl_conta",
    "escala_moeda", "moeda", "st_conta_fixa", "row_hash",
]


def _insert_batch(df: pd.DataFrame, engine, batch_size: int) -> Dict[str, int]:
    for col in _KEEP_COLS:
        if col not in df.columns:
            df[col] = None
    df = df[_KEEP_COLS].copy()

    if str(engine.url).startswith("duckdb"):
        from pipeline_local.utils.duckdb_utils import bulk_insert_duckdb
        try:
            return bulk_insert_duckdb(df, engine, TARGET_TABLE, "row_hash")
        except Exception as exc:
            log.error("bulk_insert_duckdb falhou (ITR)", erro=str(exc))
            return {"inserted": 0, "skipped": len(df)}

    from sqlalchemy import text as sa_text
    insert_sql = sa_text(f"""
        INSERT INTO {TARGET_TABLE} ({", ".join(_KEEP_COLS)})
        VALUES ({", ".join(f":{c}" for c in _KEEP_COLS)})
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
            log.error("Batch falhou, tentando linha a linha", batch_start=start, erro=str(exc))
            with engine.begin() as conn:
                for rec in records:
                    try:
                        conn.execute(insert_sql, rec)
                        inserted += 1
                    except Exception:
                        skipped += 1
    return {"inserted": inserted, "skipped": skipped}


def _years_already_loaded(engine) -> set:
    from sqlalchemy import text as sa_text
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                sa_text(f"SELECT DISTINCT EXTRACT(YEAR FROM dt_refer)::int FROM {TARGET_TABLE}")
            ).fetchall()
        return {int(r[0]) for r in rows if r[0]}
    except Exception:
        return set()


def run(
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    force_reload: bool = False,
) -> Dict[str, int]:
    settings = load_settings()
    start_year = start_year or settings.start_year
    end_year = end_year or (settings.end_year or datetime.now().year)

    run_id = str(uuid.uuid4())
    engine = get_local_engine()
    session = _build_session()

    log.info("Iniciando extração ITR local", run_id=run_id, start_year=start_year, end_year=end_year)

    available = _list_available_years(session)
    target_years = [y for y in available if start_year <= y <= end_year]

    if not force_reload:
        loaded = _years_already_loaded(engine)
        target_years = [y for y in target_years if y not in loaded]

    if not target_years:
        log.info("Nenhum ano novo para processar (ITR)", run_id=run_id)
        return {"inserted": 0, "skipped": 0, "anos_processados": 0}

    target_years = target_years[:MAX_ANOS_POR_RUN]
    log.info("Anos ITR a processar", anos=target_years, run_id=run_id)

    total_inserted = total_skipped = 0
    for year in target_years:
        zip_bytes = _cached_zip(year, session)
        if zip_bytes is None:
            continue
        year_inserted = 0
        for df_raw in _parse_zip(zip_bytes):
            df = _prepare_df(df_raw)
            counts = _insert_batch(df, engine, settings.batch_size)
            year_inserted += counts["inserted"]
            total_skipped += counts["skipped"]
        log.info("Ano ITR processado", ano=year, inserted=year_inserted, run_id=run_id)
        total_inserted += year_inserted

    log.summary(
        pipeline="extract_cvm_itr_local",
        status="success",
        run_id=run_id,
        rows_inserted=total_inserted,
        rows_skipped=total_skipped,
        anos_processados=len(target_years),
    )
    return {"inserted": total_inserted, "skipped": total_skipped, "anos_processados": len(target_years)}


def main() -> None:
    run()


if __name__ == "__main__":
    main()
