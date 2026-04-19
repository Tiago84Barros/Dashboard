import hashlib
import io
import json
import os
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core.db import get_engine

try:
    from auditoria_dados.ingestion_log import IngestionLog as _IngestionLog
    from auditoria_dados.ingestion_log import validate_required_columns
except ImportError:
    _IngestionLog = None
    validate_required_columns = None


URLS = {
    "DFP": "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/",
    "ITR": "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/",
}

DOC_TYPE = os.getenv("CVM_DOC_TYPE", "DFP").strip().upper()
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))
ANO_INICIAL = int(os.getenv("ANO_INICIAL", "2010"))
ULTIMO_ANO = int(os.getenv("ULTIMO_ANO", "0"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "180"))
CACHE_ZIPS = os.getenv("CACHE_ZIPS", "1").strip() == "1"
FORCAR_REDOWNLOAD = os.getenv("FORCAR_REDOWNLOAD", "0").strip() == "1"
BATCH_SIZE_INSERT = int(os.getenv("BATCH_SIZE_INSERT", "3000"))
LOG_PREFIX = os.getenv("LOG_PREFIX", f"[{DOC_TYPE}_RAW_V2]")
RESUME_LAST = os.getenv("CVM_RESUME", "0").strip() == "1"

BASE_DIR = Path(__file__).resolve().parent
TICKER_PATH = Path(os.getenv("TICKER_PATH", str(BASE_DIR / "cvm_to_ticker.csv")))
CACHE_DIR = Path(os.getenv("CVM_CACHE_DIR", str(BASE_DIR / f".cache_cvm_{DOC_TYPE.lower()}_v2")))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DEMO_TYPES = ("DRE", "BPA", "BPP", "DFC", "DMPL", "DVA")
_RUN_LOG = None


def log(msg: str, level: str = "INFO", **fields) -> None:
    rendered = f"{LOG_PREFIX} {msg}"
    if _RUN_LOG:
        _RUN_LOG.log(level, "pipeline_log", message=msg, rendered=rendered, **fields)
        return
    print(rendered, flush=True)


def build_session() -> requests.Session:
    retry = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("HEAD", "GET"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=MAX_WORKERS * 2,
        pool_maxsize=MAX_WORKERS * 2,
    )
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": f"dashboard-financeiro-{DOC_TYPE.lower()}-raw-v2/1.0",
            "Accept": "*/*",
            "Connection": "keep-alive",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _cache_zip_path(year: int) -> Path:
    return CACHE_DIR / f"{DOC_TYPE.lower()}_cia_aberta_{year}.zip"


def _url_zip_year(year: int) -> str:
    return URLS[DOC_TYPE] + f"{DOC_TYPE.lower()}_cia_aberta_{year}.zip"


def _head_ok(session: requests.Session, url: str) -> bool:
    try:
        r = session.head(url, timeout=30, allow_redirects=True)
        return r.status_code == 200
    except requests.RequestException:
        return False


def _last_available_year(session: requests.Session, year_max: Optional[int] = None, max_back: int = 12) -> int:
    if year_max is None:
        year_max = datetime.now().year
    for year in range(year_max, year_max - max_back - 1, -1):
        if _head_ok(session, _url_zip_year(year)):
            return year
    return year_max - max_back


def _download_year_zip(session: requests.Session, year: int) -> Tuple[bytes, int, bool]:
    url = _url_zip_year(year)
    cache_path = _cache_zip_path(year)

    if CACHE_ZIPS and cache_path.exists() and not FORCAR_REDOWNLOAD:
        content = cache_path.read_bytes()
        log(f"Usando cache local do ano {year}: {cache_path.name}", year=year, bytes=len(content), stage="cache_hit")
        return content, len(content), True

    log(f"Baixando ZIP {DOC_TYPE} {year}...", year=year, stage="downloading_zip")
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Falha ao baixar ano {year} (status={r.status_code})")

    content = r.content
    if CACHE_ZIPS:
        cache_path.write_bytes(content)
    return content, len(content), False


def _normalize_value_scale(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "VL_CONTA" not in df.columns or "ESCALA_MOEDA" not in df.columns:
        return df

    out = df.copy()
    out["VL_CONTA"] = pd.to_numeric(out["VL_CONTA"], errors="coerce")
    scale = out["ESCALA_MOEDA"].astype(str).str.strip().str.upper()

    factors = pd.Series(1.0, index=out.index)
    factors.loc[scale.isin(["MIL", "MILHAR", "MILHARES"])] = 1_000.0
    factors.loc[scale.isin(["MILHAO", "MILHÃO", "MILHOES", "MILHÕES"])] = 1_000_000.0
    factors.loc[scale.isin(["BILHAO", "BILHÃO", "BILHOES", "BILHÕES"])] = 1_000_000_000.0

    if "CD_CONTA" in out.columns:
        cd = out["CD_CONTA"].astype(str)
        mask_per_share = cd.str.startswith("3.99", na=False)
        factors.loc[mask_per_share] = 1.0

    out["VL_CONTA"] = out["VL_CONTA"] * factors
    return out


def _load_ticker_map() -> pd.DataFrame:
    if not TICKER_PATH.exists():
        raise FileNotFoundError(f"Não encontrei o arquivo CVM->Ticker em: {TICKER_PATH}")

    df = pd.read_csv(TICKER_PATH, sep=",", encoding="utf-8")
    if validate_required_columns:
        validate_required_columns(df, ["CVM", "Ticker"], context="Mapa CVM->Ticker", logger=_RUN_LOG)

    out = df[["CVM", "Ticker"]].copy()
    out["Ticker"] = out["Ticker"].astype(str).str.upper().str.strip()
    out = out[out["Ticker"].ne("")].drop_duplicates(subset=["CVM"], keep="last")
    return out.rename(columns={"CVM": "CD_CVM"})


def _infer_demo_type(filename: str) -> Optional[str]:
    name = filename.upper()
    for demo in DEMO_TYPES:
        if demo in name:
            return demo
    return None


def _infer_group_demo(filename: str, df: pd.DataFrame) -> Optional[str]:
    name = filename.upper()
    if "_CON_" in name:
        return "consolidado"
    if "_IND_" in name:
        return "individual"

    if "GRUPO_DFP" in df.columns:
        values = df["GRUPO_DFP"].dropna().astype(str).str.strip().str.lower().unique().tolist()
        if len(values) == 1:
            return values[0]
    return None


def _extract_payload(row: pd.Series) -> dict:
    keys = [
        "DENOM_CIA",
        "CNPJ_CIA",
        "VERSAO",
        "ORDEM_EXERC",
        "GRUPO_DFP",
        "MOEDA",
        "ESCALA_MOEDA",
        "ST_CONTA_FIXA",
        "NIVEL_CONTA",
        "CD_CONTA_PAI",
        "DS_CONTA_PAI",
        "DT_INI_EXERC",
        "DT_FIM_EXERC",
    ]
    payload = {}
    for key in keys:
        if key in row.index and pd.notna(row[key]):
            value = row[key]
            if isinstance(value, (pd.Timestamp, datetime)):
                value = value.strftime("%Y-%m-%d")
            payload[key] = value
    return payload


def _safe_date(value) -> Optional[str]:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def _row_hash(source_doc: str, tipo_demo: str, arquivo_origem: str, cd_cvm: object, dt_refer: str, cd_conta: object, ds_conta: object, vl_conta: object, ordem_exerc: object, versao: object) -> str:
    raw = "|".join(
        [
            str(source_doc or ""),
            str(tipo_demo or ""),
            str(arquivo_origem or ""),
            str(cd_cvm or ""),
            str(dt_refer or ""),
            str(cd_conta or ""),
            str(ds_conta or ""),
            str(vl_conta or ""),
            str(ordem_exerc or ""),
            str(versao or ""),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _ensure_run_control_columns() -> None:
    engine = get_engine()
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(
                '''
                ALTER TABLE public.cvm_ingestion_runs
                ADD COLUMN IF NOT EXISTS stop_requested boolean NOT NULL DEFAULT false,
                ADD COLUMN IF NOT EXISTS last_completed_year integer,
                ADD COLUMN IF NOT EXISTS current_year integer,
                ADD COLUMN IF NOT EXISTS current_file text,
                ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz,
                ADD COLUMN IF NOT EXISTS downloaded_bytes bigint NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS cached_bytes bigint NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS processed_files integer NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS total_files integer,
                ADD COLUMN IF NOT EXISTS rows_raw bigint NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS rows_inserted bigint NOT NULL DEFAULT 0
                '''
            )
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()


def _update_run_progress(run_id: str, **kwargs) -> None:
    engine = get_engine()
    metrics_patch = json.dumps(kwargs, ensure_ascii=False)

    column_map = {
        "current_year": "current_year",
        "current_file": "current_file",
        "downloaded_bytes_accum": "downloaded_bytes",
        "cached_bytes_accum": "cached_bytes",
        "processed_files_done": "processed_files",
        "processed_files_total": "total_files",
        "raw_rows_accum": "rows_raw",
        "inserted_rows_accum": "rows_inserted",
        "last_completed_year": "last_completed_year",
    }
    set_parts = ["metrics = COALESCE(metrics, '{}'::jsonb) || %s::jsonb", "updated_at = NOW()", "heartbeat_at = NOW()"]
    params: List[object] = [metrics_patch]

    for key, column in column_map.items():
        if key in kwargs and kwargs[key] is not None:
            set_parts.append(f"{column} = %s")
            params.append(kwargs[key])

    if "stop_requested" in kwargs and kwargs["stop_requested"] is not None:
        set_parts.append("stop_requested = %s")
        params.append(bool(kwargs["stop_requested"]))

    params.append(run_id)
    sql = f"UPDATE public.cvm_ingestion_runs SET {', '.join(set_parts)} WHERE run_id = %s"

    raw_conn = get_engine().raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(sql, tuple(params))
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()


def _should_stop(run_id: str) -> bool:
    raw_conn = get_engine().raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute("SELECT COALESCE(stop_requested, false) FROM public.cvm_ingestion_runs WHERE run_id = %s", (run_id,))
            row = cur.fetchone()
            return bool(row[0]) if row else False
    finally:
        raw_conn.close()


def _discover_years_and_last_year(session: requests.Session) -> Tuple[List[int], int]:
    last_year = ULTIMO_ANO if ULTIMO_ANO > 0 else _last_available_year(session, datetime.now().year, max_back=12)
    years = list(range(ANO_INICIAL, last_year + 1))
    if not years:
        raise RuntimeError("Intervalo de anos vazio. Verifique ANO_INICIAL/ULTIMO_ANO.")
    return years, last_year


def _list_relevant_files(zip_bytes: bytes) -> List[str]:
    names: List[str] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_ref:
        for filename in zip_ref.namelist():
            if filename.endswith(".csv") and _infer_demo_type(filename) is not None:
                names.append(filename)
    return names


def _empty_year_result() -> Dict[str, object]:
    return {"rows": [], "errors": [], "processed_files": 0, "total_files": 0}


def process_year(session: requests.Session, year: int, ticker_map: pd.DataFrame, run_id: Optional[str] = None, progress_state: Optional[dict] = None) -> Dict[str, object]:
    result = _empty_year_result()
    progress_state = progress_state or {}
    try:
        raw_zip, zip_size, from_cache = _download_year_zip(session, year)
        result["zip_size"] = zip_size
        result["from_cache"] = from_cache
    except Exception as exc:
        message = f"{DOC_TYPE} {year}: erro ao baixar ZIP: {exc}"
        result["errors"].append(message)
        log(message, level="ERROR", year=year, stage="download_failed")
        return result

    try:
        relevant_files = _list_relevant_files(raw_zip)
        result["total_files"] = len(relevant_files)
        base_files_done = int(progress_state.get("processed_files_done", 0))

        with zipfile.ZipFile(io.BytesIO(raw_zip)) as zip_ref:
            processed_in_year = 0
            for file_idx, filename in enumerate(relevant_files, start=1):
                if run_id and _should_stop(run_id):
                    log(f"Parada solicitada pelo usuário durante o ano {year}.", level="WARN", year=year, file=filename, stage="stop_requested")
                    result["stop_requested"] = True
                    return result

                if run_id:
                    _update_run_progress(
                        run_id,
                        current_year=year,
                        current_file=filename,
                        processed_files_done=base_files_done + processed_in_year,
                        processed_files_total=base_files_done + len(relevant_files),
                        stage="processing_file",
                        file_index_in_year=file_idx,
                        files_in_year=len(relevant_files),
                    )

                demo_type = _infer_demo_type(filename)
                if demo_type is None:
                    continue

                with zip_ref.open(filename) as csvfile:
                    try:
                        df = pd.read_csv(csvfile, sep=";", decimal=",", encoding="ISO-8859-1")
                        if validate_required_columns:
                            validate_required_columns(
                                df,
                                ["CD_CVM", "DT_REFER", "CD_CONTA", "VL_CONTA"],
                                context=f"{DOC_TYPE} {year} {filename}",
                                logger=_RUN_LOG,
                            )

                        df = _normalize_value_scale(df)
                        group_demo = _infer_group_demo(filename, df)
                        df = df.merge(ticker_map, how="left", on="CD_CVM")

                        for row in df.to_dict(orient="records"):
                            dt_refer = _safe_date(row.get("DT_REFER"))
                            if dt_refer is None:
                                continue

                            dt_ini = _safe_date(row.get("DT_INI_EXERC"))
                            dt_fim = _safe_date(row.get("DT_FIM_EXERC"))
                            payload = _extract_payload(pd.Series(row))
                            row_hash = _row_hash(
                                source_doc=DOC_TYPE,
                                tipo_demo=demo_type,
                                arquivo_origem=filename,
                                cd_cvm=row.get("CD_CVM"),
                                dt_refer=dt_refer,
                                cd_conta=row.get("CD_CONTA"),
                                ds_conta=row.get("DS_CONTA"),
                                vl_conta=row.get("VL_CONTA"),
                                ordem_exerc=row.get("ORDEM_EXERC"),
                                versao=row.get("VERSAO"),
                            )

                            result["rows"].append(
                                {
                                    "source_doc": DOC_TYPE,
                                    "tipo_demo": demo_type,
                                    "grupo_demo": group_demo,
                                    "arquivo_origem": filename,
                                    "cd_cvm": int(row["CD_CVM"]) if pd.notna(row.get("CD_CVM")) else None,
                                    "cnpj_cia": str(row.get("CNPJ_CIA") or "").strip() or None,
                                    "denom_cia": str(row.get("DENOM_CIA") or "").strip() or None,
                                    "ticker": str(row.get("Ticker") or "").strip().upper() or None,
                                    "versao": int(row["VERSAO"]) if pd.notna(row.get("VERSAO")) else None,
                                    "ordem_exerc": str(row.get("ORDEM_EXERC") or "").strip() or None,
                                    "dt_refer": dt_refer,
                                    "dt_ini_exerc": dt_ini,
                                    "dt_fim_exerc": dt_fim,
                                    "cd_conta": str(row.get("CD_CONTA") or "").strip() or None,
                                    "ds_conta": str(row.get("DS_CONTA") or "").strip() or None,
                                    "nivel_conta": int(row["NIVEL_CONTA"]) if pd.notna(row.get("NIVEL_CONTA")) else None,
                                    "conta_pai": str(row.get("CD_CONTA_PAI") or "").strip() or None,
                                    "vl_conta": None if pd.isna(row.get("VL_CONTA")) else float(row.get("VL_CONTA")),
                                    "escala_moeda": str(row.get("ESCALA_MOEDA") or "").strip() or None,
                                    "moeda": str(row.get("MOEDA") or "").strip() or None,
                                    "st_conta_fixa": str(row.get("ST_CONTA_FIXA") or "").strip() or None,
                                    "row_hash": row_hash,
                                    "payload": json.dumps(payload, ensure_ascii=False) if payload else None,
                                }
                            )
                        processed_in_year += 1
                    except Exception as exc:
                        message = f"{DOC_TYPE} {year}: erro ao processar arquivo {filename}: {exc}"
                        result["errors"].append(message)
                        log(message, level="ERROR", year=year, file=filename, stage="file_processing_failed")
        result["processed_files"] = processed_in_year
        return result
    except zipfile.BadZipFile:
        message = f"{DOC_TYPE} {year}: ZIP inválido."
        result["errors"].append(message)
        log(message, level="ERROR", year=year, stage="bad_zip")
        return result


def upsert_cvm_financial_raw(rows: List[dict], run_id: Optional[str]) -> int:
    if not rows:
        log("Nenhuma linha raw para gravar.", level="WARN")
        return 0

    df = pd.DataFrame(rows)
    df["run_id"] = run_id

    before_dedup = len(df)
    df = df.drop_duplicates(
        subset=["source_doc", "tipo_demo", "arquivo_origem", "cd_cvm", "dt_refer", "cd_conta", "row_hash"],
        keep="last",
    ).reset_index(drop=True)
    removed = before_dedup - len(df)
    if removed > 0:
        log(
            f"RAW removeu {removed} duplicata(s) no lote antes do insert.",
            level="WARN",
            duplicates_removed=removed,
            stage="pre_insert_dedup",
        )

    cols = [
        "run_id", "source_doc", "tipo_demo", "grupo_demo", "arquivo_origem", "cd_cvm", "cnpj_cia", "denom_cia", "ticker",
        "versao", "ordem_exerc", "dt_refer", "dt_ini_exerc", "dt_fim_exerc", "cd_conta", "ds_conta", "nivel_conta", "conta_pai",
        "vl_conta", "escala_moeda", "moeda", "st_conta_fixa", "row_hash", "payload",
    ]
    values = [tuple(x) for x in df[cols].itertuples(index=False, name=None)]

    sql = f'''
    INSERT INTO public.cvm_financial_raw ({", ".join(cols)}) VALUES %s
    ON CONFLICT (source_doc, tipo_demo, arquivo_origem, cd_cvm, dt_refer, cd_conta, row_hash)
    DO UPDATE SET
        run_id = EXCLUDED.run_id,
        grupo_demo = EXCLUDED.grupo_demo,
        cnpj_cia = EXCLUDED.cnpj_cia,
        denom_cia = EXCLUDED.denom_cia,
        ticker = EXCLUDED.ticker,
        versao = EXCLUDED.versao,
        ordem_exerc = EXCLUDED.ordem_exerc,
        dt_ini_exerc = EXCLUDED.dt_ini_exerc,
        dt_fim_exerc = EXCLUDED.dt_fim_exerc,
        ds_conta = EXCLUDED.ds_conta,
        nivel_conta = EXCLUDED.nivel_conta,
        conta_pai = EXCLUDED.conta_pai,
        vl_conta = EXCLUDED.vl_conta,
        escala_moeda = EXCLUDED.escala_moeda,
        moeda = EXCLUDED.moeda,
        st_conta_fixa = EXCLUDED.st_conta_fixa,
        payload = EXCLUDED.payload,
        updated_at = NOW();
    '''

    engine = get_engine()
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = 'public' AND t.relname = 'cvm_financial_raw' AND c.contype IN ('u', 'p') LIMIT 1
                """
            )
            if cur.fetchone() is None:
                raise RuntimeError('A tabela public.cvm_financial_raw precisa ter UNIQUE/PK antes do insert institucional.')
            execute_values(cur, sql, values, page_size=BATCH_SIZE_INSERT)
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()

    log(f"Upsert concluído em cvm_financial_raw: {len(df)} linhas.", rows=len(df))
    return len(df)


def _load_resume_anchor(source_doc: str) -> Tuple[Optional[str], Optional[int]]:
    raw_conn = get_engine().raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(
                '''
                SELECT run_id, COALESCE(last_completed_year, 0)
                FROM public.cvm_ingestion_runs
                WHERE source_doc = %s
                  AND status IN ('stopped', 'failed')
                ORDER BY updated_at DESC NULLS LAST, started_at DESC NULLS LAST
                LIMIT 1
                ''',
                (source_doc,),
            )
            row = cur.fetchone()
            if not row:
                return None, None
            return row[0], int(row[1]) if row[1] else None
    finally:
        raw_conn.close()


def _create_run(source_doc: str, year_start: int, year_end: Optional[int], resumed_from_run_id: Optional[str] = None, resumed_from_year: Optional[int] = None) -> str:
    run_id = f"{source_doc.lower()}_raw_v2_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
    params = {
        "doc_type": source_doc,
        "max_workers": MAX_WORKERS,
        "request_timeout": REQUEST_TIMEOUT,
        "cache_zips": CACHE_ZIPS,
        "force_redownload": FORCAR_REDOWNLOAD,
        "resume_requested": RESUME_LAST,
        "resumed_from_run_id": resumed_from_run_id,
        "resumed_from_year": resumed_from_year,
    }
    raw_conn = get_engine().raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(
                '''
                INSERT INTO public.cvm_ingestion_runs
                (run_id, source_doc, status, ano_inicial, ano_final, params, stop_requested, heartbeat_at)
                VALUES (%s, %s, 'running', %s, %s, %s::jsonb, false, NOW())
                ''',
                (run_id, source_doc, year_start, year_end, json.dumps(params, ensure_ascii=False)),
            )
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()
    return run_id


def _finish_run(run_id: str, status: str, last_year: Optional[int], metrics: Optional[dict] = None, errors: Optional[dict] = None) -> None:
    raw_conn = get_engine().raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(
                '''
                UPDATE public.cvm_ingestion_runs
                SET status = %s,
                    ultimo_ano_disponivel = %s,
                    metrics = COALESCE(metrics, '{}'::jsonb) || %s::jsonb,
                    errors = %s::jsonb,
                    finished_at = NOW(),
                    updated_at = NOW(),
                    heartbeat_at = NOW()
                WHERE run_id = %s
                ''',
                (status, last_year, json.dumps(metrics or {}, ensure_ascii=False), json.dumps(errors or {}, ensure_ascii=False), run_id),
            )
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()


def run_extract_incremental(run_id: str) -> Tuple[int, int, str]:
    session = build_session()
    ticker_map = _load_ticker_map()
    years, last_year = _discover_years_and_last_year(session)

    resumed_from_run_id, resumed_from_year = (None, None)
    if RESUME_LAST:
        resumed_from_run_id, resumed_from_year = _load_resume_anchor(DOC_TYPE)
        if resumed_from_year:
            years = [year for year in years if year > resumed_from_year]
            log(
                f"Retomando execução a partir do ano seguinte ao último concluído: {resumed_from_year}.",
                stage="resume_anchor_found",
                resumed_from_run_id=resumed_from_run_id,
                resumed_from_year=resumed_from_year,
            )

    total_years = len(years)
    if total_years == 0:
        return 0, last_year, "success"

    raw_rows_accum = 0
    inserted_rows_accum = 0
    downloaded_bytes_accum = 0
    cached_bytes_accum = 0
    processed_files_done = 0
    started_at = time.time()

    for idx, year in enumerate(years, start=1):
        if _should_stop(run_id):
            return inserted_rows_accum, last_year, "stopped"

        _update_run_progress(
            run_id,
            current_year=year,
            years_done=idx - 1,
            years_total=total_years,
            raw_rows_accum=raw_rows_accum,
            inserted_rows_accum=inserted_rows_accum,
            downloaded_bytes_accum=downloaded_bytes_accum,
            cached_bytes_accum=cached_bytes_accum,
            processed_files_done=processed_files_done,
            stage="processing_year",
            elapsed_seconds=round(time.time() - started_at, 1),
            resumed_from_year=resumed_from_year,
            resumed_from_run_id=resumed_from_run_id,
        )

        data = process_year(
            session,
            year,
            ticker_map,
            run_id=run_id,
            progress_state={"processed_files_done": processed_files_done},
        )
        year_errors = list(data.get("errors", []))
        if year_errors:
            raise RuntimeError(f"{DOC_TYPE} {year}: {year_errors[:3]}")

        if data.get("from_cache"):
            cached_bytes_accum += int(data.get("zip_size") or 0)
        else:
            downloaded_bytes_accum += int(data.get("zip_size") or 0)
        processed_files_done += int(data.get("processed_files") or 0)

        if data.get("stop_requested"):
            _update_run_progress(
                run_id,
                current_year=year,
                raw_rows_accum=raw_rows_accum,
                inserted_rows_accum=inserted_rows_accum,
                downloaded_bytes_accum=downloaded_bytes_accum,
                cached_bytes_accum=cached_bytes_accum,
                processed_files_done=processed_files_done,
                stage="stop_requested",
                elapsed_seconds=round(time.time() - started_at, 1),
            )
            return inserted_rows_accum, last_year, "stopped"

        rows = data.get("rows", [])
        raw_rows_accum += len(rows)

        if rows:
            inserted = upsert_cvm_financial_raw(rows, run_id)
            inserted_rows_accum += inserted

        _update_run_progress(
            run_id,
            current_year=year,
            current_file=None,
            years_done=idx,
            years_total=total_years,
            raw_rows_accum=raw_rows_accum,
            inserted_rows_accum=inserted_rows_accum,
            downloaded_bytes_accum=downloaded_bytes_accum,
            cached_bytes_accum=cached_bytes_accum,
            processed_files_done=processed_files_done,
            processed_files_total=processed_files_done,
            last_completed_year=year,
            stage="year_done",
            elapsed_seconds=round(time.time() - started_at, 1),
        )

    return inserted_rows_accum, last_year, "success"


def main() -> None:
    global _RUN_LOG

    if DOC_TYPE not in URLS:
        raise ValueError(
            f"CVM_DOC_TYPE inválido: '{DOC_TYPE}'. Deve ser 'DFP' ou 'ITR'. Defina a variável de ambiente corretamente antes de executar."
        )

    try:
        from core.cvm_v2_schema_check import assert_v2_schema_ready
        assert_v2_schema_ready()
    except ImportError:
        pass

    _ensure_run_control_columns()
    session = build_session()
    _, last_year = _discover_years_and_last_year(session)
    resumed_from_run_id, resumed_from_year = _load_resume_anchor(DOC_TYPE) if RESUME_LAST else (None, None)
    run_id = _create_run(DOC_TYPE, ANO_INICIAL, ULTIMO_ANO or last_year or None, resumed_from_run_id, resumed_from_year)
    log(f"Run criada: {run_id}")

    try:
        if _IngestionLog:
            with _IngestionLog(f"{DOC_TYPE.lower()}_raw_v2") as _log_ctx:
                _RUN_LOG = _log_ctx
                _log_ctx.set_params({"ano_inicial": ANO_INICIAL, "ultimo_ano": ULTIMO_ANO or "auto", "resume": RESUME_LAST})
                inserted, discovered_last_year, final_status = run_extract_incremental(run_id)
                _log_ctx.set_metric("ultimo_ano_disponivel", discovered_last_year)
                _log_ctx.add_rows(inserted=inserted)
                _finish_run(
                    run_id,
                    status=final_status,
                    last_year=discovered_last_year,
                    metrics={"inserted_rows": inserted, "final_status": final_status, "resume": RESUME_LAST},
                    errors=None,
                )
        else:
            inserted, discovered_last_year, final_status = run_extract_incremental(run_id)
            _finish_run(
                run_id,
                status=final_status,
                last_year=discovered_last_year,
                metrics={"inserted_rows": inserted, "final_status": final_status, "resume": RESUME_LAST},
                errors=None,
            )

        if final_status == "stopped":
            log(f"Ingestão RAW V2 interrompida sob demanda | Doc: {DOC_TYPE} | Run: {run_id}", level="WARN", stage="final_stopped", run_id=run_id)
        else:
            log(f"Ingestão RAW V2 concluída com sucesso | Doc: {DOC_TYPE} | Run: {run_id}", stage="final_success", run_id=run_id)
    except Exception as exc:
        _finish_run(run_id, status="failed", last_year=None, errors={"message": str(exc)})
        log(f"Falha na ingestão RAW V2: {exc}", level="ERROR", stage="final_failed", run_id=run_id)
        raise
    finally:
        _RUN_LOG = None


if __name__ == "__main__":
    main()
