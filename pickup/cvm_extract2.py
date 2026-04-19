import hashlib
import io
import json
import os
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core.db import get_engine
from sqlalchemy import text

try:
    from auditoria_dados.ingestion_log import IngestionLog as _IngestionLog
    from auditoria_dados.ingestion_log import validate_required_columns
except ImportError:
    _IngestionLog = None
    validate_required_columns = None


# =========================================================
# CONFIG
# =========================================================
URLS = {
    "DFP": "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/",
    "ITR": "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/",
}

DOC_TYPE = os.getenv("CVM_DOC_TYPE", "DFP").strip().upper()
# Nota: a validação de DOC_TYPE é feita em main() para evitar falha em import.

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))
ANO_INICIAL = int(os.getenv("ANO_INICIAL", "2010"))
ULTIMO_ANO = int(os.getenv("ULTIMO_ANO", "0"))  # 0 => auto
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "180"))
CACHE_ZIPS = os.getenv("CACHE_ZIPS", "1").strip() == "1"
FORCAR_REDOWNLOAD = os.getenv("FORCAR_REDOWNLOAD", "0").strip() == "1"
BATCH_SIZE_INSERT = int(os.getenv("BATCH_SIZE_INSERT", "3000"))
LOG_PREFIX = os.getenv("LOG_PREFIX", f"[{DOC_TYPE}_RAW_V2]")

BASE_DIR = Path(__file__).resolve().parent
TICKER_PATH = Path(os.getenv("TICKER_PATH", str(BASE_DIR / "cvm_to_ticker.csv")))
CACHE_DIR = Path(os.getenv("CVM_CACHE_DIR", str(BASE_DIR / f".cache_cvm_{DOC_TYPE.lower()}_v2")))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DEMO_TYPES = ("DRE", "BPA", "BPP", "DFC", "DMPL", "DVA")
_RUN_LOG = None


# =========================================================
# LOG
# =========================================================
def log(msg: str, level: str = "INFO", **fields) -> None:
    rendered = f"{LOG_PREFIX} {msg}"
    if _RUN_LOG:
        _RUN_LOG.log(level, "pipeline_log", message=msg, rendered=rendered, **fields)
        return
    print(rendered, flush=True)


# =========================================================
# HTTP
# =========================================================
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


# =========================================================
# HELPERS
# =========================================================
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


def _download_year_zip(session: requests.Session, year: int) -> bytes:
    url = _url_zip_year(year)
    cache_path = _cache_zip_path(year)

    if CACHE_ZIPS and cache_path.exists() and not FORCAR_REDOWNLOAD:
        log(f"Usando cache local do ano {year}: {cache_path.name}")
        return cache_path.read_bytes()

    log(f"Baixando ZIP {DOC_TYPE} {year}...")
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Falha ao baixar ano {year} (status={r.status_code})")

    content = r.content
    if CACHE_ZIPS:
        cache_path.write_bytes(content)
    return content


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

    # Contas por ação não devem ser multiplicadas.
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
        values = (
            df["GRUPO_DFP"].dropna().astype(str).str.strip().str.lower().unique().tolist()
        )
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


def _row_hash(
    source_doc: str,
    tipo_demo: str,
    arquivo_origem: str,
    cd_cvm: object,
    dt_refer: str,
    cd_conta: object,
    ds_conta: object,
    vl_conta: object,
    ordem_exerc: object,
    versao: object,
) -> str:
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




def _discover_years_and_last_year(session: requests.Session) -> Tuple[List[int], int]:
    last_year = ULTIMO_ANO if ULTIMO_ANO > 0 else _last_available_year(session, datetime.now().year, max_back=12)
    years = list(range(ANO_INICIAL, last_year + 1))
    if not years:
        raise RuntimeError("Intervalo de anos vazio. Verifique ANO_INICIAL/ULTIMO_ANO.")
    return years, last_year


def _update_run_progress(run_id: str, **kwargs: Any) -> None:
    engine = get_engine()
    metrics = json.dumps(kwargs, ensure_ascii=False)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE public.cvm_ingestion_runs
                SET metrics = COALESCE(metrics, '{}'::jsonb) || CAST(:metrics AS jsonb),
                    updated_at = NOW()
                WHERE run_id = :run_id
                """
            ),
            {"run_id": run_id, "metrics": metrics},
        )


def run_extract_incremental(run_id: str) -> Tuple[int, int, int]:
    session = build_session()
    ticker_map = _load_ticker_map()
    years, last_year = _discover_years_and_last_year(session)
    total_years = len(years)

    raw_rows_accum = 0
    inserted_rows_accum = 0

    log(
        f"Coletando {DOC_TYPE} RAW V2 de forma incremental no intervalo {years[0]}..{last_year}",
        stage="incremental_start",
        years_total=total_years,
    )

    _update_run_progress(
        run_id,
        current_year=None,
        years_done=0,
        years_total=total_years,
        raw_rows_accum=0,
        inserted_rows_accum=0,
        stage="starting",
    )

    for idx, year in enumerate(years, start=1):
        _update_run_progress(
            run_id,
            current_year=year,
            years_done=idx - 1,
            years_total=total_years,
            raw_rows_accum=raw_rows_accum,
            inserted_rows_accum=inserted_rows_accum,
            stage="processing_year",
        )

        data = process_year(session, year, ticker_map)
        year_errors = list(data.get("errors", []))
        if year_errors:
            preview = year_errors[:3]
            _update_run_progress(
                run_id,
                current_year=year,
                years_done=idx - 1,
                years_total=total_years,
                raw_rows_accum=raw_rows_accum,
                inserted_rows_accum=inserted_rows_accum,
                stage="failed_year",
                failed_year=year,
                failed_errors=preview,
            )
            raise RuntimeError(f"Falhas no ano {year} {DOC_TYPE}: {preview}")

        rows = list(data.get("rows", []))
        raw_rows_accum += len(rows)
        inserted_now = 0

        if rows:
            inserted_now = upsert_cvm_financial_raw(rows, run_id)
            inserted_rows_accum += inserted_now

        _update_run_progress(
            run_id,
            current_year=year,
            years_done=idx,
            years_total=total_years,
            raw_rows_accum=raw_rows_accum,
            inserted_rows_accum=inserted_rows_accum,
            rows_year=len(rows),
            inserted_year=inserted_now,
            stage="year_done",
        )

        if _RUN_LOG:
            _RUN_LOG.set_metric("current_year", year)
            _RUN_LOG.set_metric("years_done", idx)
            _RUN_LOG.set_metric("years_total", total_years)
            _RUN_LOG.set_metric("raw_rows_accum", raw_rows_accum)
            _RUN_LOG.set_metric("inserted_rows_accum", inserted_rows_accum)

        log(
            f"Ano {year} processado com sucesso. Linhas raw: {len(rows)} | inseridas: {inserted_now}",
            year=year,
            rows_year=len(rows),
            inserted_year=inserted_now,
            years_done=idx,
            years_total=total_years,
            stage="year_success",
        )

    _update_run_progress(
        run_id,
        current_year=last_year,
        years_done=total_years,
        years_total=total_years,
        raw_rows_accum=raw_rows_accum,
        inserted_rows_accum=inserted_rows_accum,
        stage="completed",
    )
    return raw_rows_accum, inserted_rows_accum, last_year


# =========================================================
# EXTRACTION
# =========================================================
def _empty_year_result() -> Dict[str, object]:
    return {"rows": [], "errors": []}


def process_year(session: requests.Session, year: int, ticker_map: pd.DataFrame) -> Dict[str, object]:
    result = _empty_year_result()
    try:
        raw_zip = _download_year_zip(session, year)
    except Exception as exc:
        message = f"{DOC_TYPE} {year}: erro ao baixar ZIP: {exc}"
        result["errors"].append(message)
        log(message, level="ERROR", year=year, stage="download_failed")
        return result

    try:
        with zipfile.ZipFile(io.BytesIO(raw_zip)) as zip_ref:
            for filename in zip_ref.namelist():
                if not filename.endswith(".csv"):
                    continue
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

                        # Mantém tudo, inclusive exercícios reprocessados, para a camada raw.
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
                    except Exception as exc:
                        message = f"{DOC_TYPE} {year}: erro ao processar arquivo {filename}: {exc}"
                        result["errors"].append(message)
                        log(message, level="ERROR", year=year, file=filename, stage="file_processing_failed")
        return result
    except zipfile.BadZipFile:
        message = f"{DOC_TYPE} {year}: ZIP inválido."
        result["errors"].append(message)
        log(message, level="ERROR", year=year, stage="bad_zip")
        return result


def collect_raw_rows() -> Tuple[List[dict], int]:
    """Compatibilidade retroativa: mantém coleta em memória para usos legados."""
    session = build_session()
    ticker_map = _load_ticker_map()
    years, last_year = _discover_years_and_last_year(session)

    log(f"Coletando {DOC_TYPE} RAW V2 do intervalo {years[0]}..{last_year}")
    start = time.time()
    all_rows: List[dict] = []

    for year in years:
        data = process_year(session, year, ticker_map)
        year_errors = list(data.get("errors", []))
        if year_errors:
            preview = year_errors[:2]
            raise RuntimeError(f"Falhas no ano {year} {DOC_TYPE}: {preview}")
        year_rows = list(data.get("rows", []))
        all_rows.extend(year_rows)
        log(f"Ano {year} processado com sucesso. Linhas raw: {len(year_rows)}", year=year, stage="year_success")

    elapsed = round(time.time() - start, 1)
    log(f"[OK] Coleta raw concluída em {elapsed}s.")
    return all_rows, last_year


# =========================================================
# DB
# =========================================================
def _assert_raw_unique_ready(cur) -> None:
    cur.execute(
        """
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = 'cvm_financial_raw'
          AND c.contype IN ('u', 'p')
        LIMIT 1
        """
    )
    if cur.fetchone() is None:
        raise RuntimeError(
            'A tabela public.cvm_financial_raw precisa ter UNIQUE/PK antes do insert institucional.'
        )


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
        "run_id",
        "source_doc",
        "tipo_demo",
        "grupo_demo",
        "arquivo_origem",
        "cd_cvm",
        "cnpj_cia",
        "denom_cia",
        "ticker",
        "versao",
        "ordem_exerc",
        "dt_refer",
        "dt_ini_exerc",
        "dt_fim_exerc",
        "cd_conta",
        "ds_conta",
        "nivel_conta",
        "conta_pai",
        "vl_conta",
        "escala_moeda",
        "moeda",
        "st_conta_fixa",
        "row_hash",
        "payload",
    ]

    values = [tuple(x) for x in df[cols].itertuples(index=False, name=None)]

    sql = f'''
    INSERT INTO public.cvm_financial_raw (
        {", ".join(cols)}
    ) VALUES %s
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
        updated_at = NOW()
    ;
    '''

    engine = get_engine()
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            _assert_raw_unique_ready(cur)
            execute_values(cur, sql, values, page_size=BATCH_SIZE_INSERT)
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()

    log(f"Upsert concluído em cvm_financial_raw: {len(df)} linhas.", rows=len(df))
    return len(df)


# =========================================================
# RUN REGISTRY
# =========================================================
def _create_run(source_doc: str, year_start: int, year_end: Optional[int]) -> str:
    run_id = f"{source_doc.lower()}_raw_v2_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
    engine = get_engine()
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(
                '''
                INSERT INTO public.cvm_ingestion_runs
                (run_id, source_doc, status, ano_inicial, ano_final, params)
                VALUES (%s, %s, 'running', %s, %s, %s::jsonb)
                ''',
                (
                    run_id,
                    source_doc,
                    year_start,
                    year_end,
                    json.dumps(
                        {
                            "doc_type": source_doc,
                            "max_workers": MAX_WORKERS,
                            "request_timeout": REQUEST_TIMEOUT,
                            "cache_zips": CACHE_ZIPS,
                            "force_redownload": FORCAR_REDOWNLOAD,
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()
    return run_id


def _finish_run(run_id: str, status: str, last_year: Optional[int], metrics: Optional[dict] = None, errors: Optional[dict] = None) -> None:
    engine = get_engine()
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(
                '''
                UPDATE public.cvm_ingestion_runs
                SET status = %s,
                    ultimo_ano_disponivel = %s,
                    metrics = %s::jsonb,
                    errors = %s::jsonb,
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE run_id = %s
                ''',
                (
                    status,
                    last_year,
                    json.dumps(metrics or {}, ensure_ascii=False),
                    json.dumps(errors or {}, ensure_ascii=False),
                    run_id,
                ),
            )
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    global _RUN_LOG

    # ── Validações de pré-condição ──────────────────────────────────────────
    if DOC_TYPE not in URLS:
        raise ValueError(
            f"CVM_DOC_TYPE inválido: '{DOC_TYPE}'. Deve ser 'DFP' ou 'ITR'. "
            "Defina a variável de ambiente corretamente antes de executar."
        )

    # Valida schema CVM V2 antes de qualquer operação no banco
    try:
        from core.cvm_v2_schema_check import assert_v2_schema_ready
        assert_v2_schema_ready()
    except ImportError:
        pass   # módulo de checagem não disponível — prossegue sem validação

    run_id = _create_run(DOC_TYPE, ANO_INICIAL, ULTIMO_ANO or None)
    log(f"Run criada: {run_id}")

    try:
        if _IngestionLog:
            with _IngestionLog(f"{DOC_TYPE.lower()}_raw_v2") as _log_ctx:
                _RUN_LOG = _log_ctx
                _log_ctx.set_params({"ano_inicial": ANO_INICIAL, "ultimo_ano": ULTIMO_ANO or "auto"})
                raw_rows, inserted, last_year = run_extract_incremental(run_id)
                _log_ctx.set_metric("ultimo_ano_disponivel", last_year)
                _log_ctx.set_metric("raw_rows", raw_rows)
                _log_ctx.set_metric("inserted_rows", inserted)
                _log_ctx.add_rows(inserted=inserted)
                _finish_run(
                    run_id,
                    status="success",
                    last_year=last_year,
                    metrics={
                        "current_year": last_year,
                        "years_done": max(last_year - ANO_INICIAL + 1, 0),
                        "years_total": max(last_year - ANO_INICIAL + 1, 0),
                        "raw_rows_accum": raw_rows,
                        "inserted_rows_accum": inserted,
                        "raw_rows": raw_rows,
                        "inserted_rows": inserted,
                        "stage": "final_success",
                    },
                )
        else:
            raw_rows, inserted, last_year = run_extract_incremental(run_id)
            _finish_run(
                run_id,
                status="success",
                last_year=last_year,
                metrics={
                    "current_year": last_year,
                    "years_done": max(last_year - ANO_INICIAL + 1, 0),
                    "years_total": max(last_year - ANO_INICIAL + 1, 0),
                    "raw_rows_accum": raw_rows,
                    "inserted_rows_accum": inserted,
                    "raw_rows": raw_rows,
                    "inserted_rows": inserted,
                    "stage": "final_success",
                },
            )

        log(
            f"Ingestão RAW V2 concluída com sucesso | Doc: {DOC_TYPE} | Run: {run_id}",
            stage="final_success",
            run_id=run_id,
            raw_rows=raw_rows,
            inserted_rows=inserted,
            last_year=last_year,
        )
    except Exception as exc:
        _finish_run(run_id, status="failed", last_year=None, errors={"message": str(exc)})
        log(f"Falha na ingestão RAW V2: {exc}", level="ERROR", stage="final_failed", run_id=run_id)
        raise
    finally:
        _RUN_LOG = None


if __name__ == "__main__":
    main()
