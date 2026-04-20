import hashlib
import io
import json
import os
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

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
# Máximo de anos processados por execução.
# 0 = sem limite (processa todos os anos pendentes de uma vez).
# Use valor baixo (ex: 2) apenas se o servidor travar por memória/timeout.
_max_anos_env = int(os.getenv("MAX_ANOS_POR_RUN", "0"))
MAX_ANOS_POR_RUN = _max_anos_env if _max_anos_env > 0 else 9999

BASE_DIR = Path(__file__).resolve().parent
TICKER_PATH = Path(os.getenv("TICKER_PATH", str(BASE_DIR / "cvm_to_ticker.csv")))
CACHE_DIR = Path(os.getenv("CVM_CACHE_DIR", str(BASE_DIR / f".cache_cvm_{DOC_TYPE.lower()}_v2")))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Ordem importa: substrings mais específicas ANTES das genéricas.
# Os arquivos CVM usam DFC_MI (método indireto) e DFC_MD (método direto).
# Se "DFC" viesse primeiro, ele matcharia DFC_MI e DFC_MD erroneamente,
# inserindo tipo_demo="DFC" que não existe no ENUM cvm_tipo_demo → batch inteira falha.
DEMO_TYPES = ("DRE", "BPA", "BPP", "DFC_MI", "DFC_MD", "DMPL", "DVA")
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
        content = cache_path.read_bytes()
        log(
            f"Usando cache local do ano {year}: {cache_path.name}",
            year=year,
            stage="download_cache_hit",
            url=url,
            zip_size_bytes=len(content),
        )
        return content

    log(f"Baixando ZIP {DOC_TYPE} {year}...", year=year, stage="download_start", url=url)
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    if r.status_code == 404:
        raise FileNotFoundError(f"Ano {year} não disponível na CVM (404) — ignorando.")
    if r.status_code != 200:
        raise RuntimeError(f"Falha ao baixar ano {year} (status={r.status_code})")

    content = r.content
    if CACHE_ZIPS:
        cache_path.write_bytes(content)
    log(
        f"Download concluído ZIP {DOC_TYPE} {year}",
        year=year,
        stage="download_done",
        url=url,
        http_status=r.status_code,
        zip_size_bytes=len(content),
    )
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
    out["CD_CVM"] = pd.to_numeric(out["CVM"], errors="coerce").astype("Int64")
    out["Ticker"] = out["Ticker"].astype(str).str.upper().str.strip()
    out = out[out["Ticker"].ne("") & out["CD_CVM"].notna()].drop_duplicates(subset=["CD_CVM"], keep="last")
    return out[["CD_CVM", "Ticker"]]


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


# =========================================================
# FILTROS DE VALUATION
# =========================================================

# Apenas demonstrações consolidadas necessárias para valuation.
# Individual (holding sem subsidiárias) e DVA/DMPL são ignorados.
DEMOS_VALUATION = frozenset(["DRE", "BPA", "BPP", "DFC_MI", "DFC_MD"])

# Nível máximo de conta a importar.
# Nível 1-3 cobre os agregados usados em valuation (ex: 3.01, 3.02, 3.03).
# Nível 4+ são subcontas de detalhe desnecessárias (ex: 3.06.01.01.02).
NIVEL_CONTA_MAX = int(os.getenv("NIVEL_CONTA_MAX", "3"))

# Rótulo do exercício atual dentro de cada arquivo.
# Evidência validada no ZIP real dfp_cia_aberta_2025.zip:
# ORDEM_EXERC vem como "ÚLTIMO" / "PENÚLTIMO".
# Mantemos também variantes antigas/alternativas por compatibilidade.
ORDEM_EXERC_VALIDO = frozenset([
    "ÚLTIMO",
    "ULTIMO",
    "ÚLTIMO EXERCÍCIO",
    "ULTIMO EXERCICIO",
])


def _arquivo_relevante(filename: str) -> bool:
    """Retorna True se o arquivo CSV do ZIP deve ser processado para valuation.

    Regras:
    - Deve ser arquivo consolidado (_con_): individual ignora subsidiárias.
    - Deve ser uma das demos essenciais: DRE, BPA, BPP, DFC_MI, DFC_MD.
    - DMPL e DVA são ignorados — irrelevantes para valuation.
    """
    name = filename.upper()
    if "_IND_" in name:
        return False   # individual: apenas holding, sem subsidiárias
    demo_type = _infer_demo_type(filename)
    return demo_type in DEMOS_VALUATION


# =========================================================
# EXTRACTION
# =========================================================
def _empty_year_result() -> Dict[str, object]:
    return {
        "rows": [],
        "errors": [],
        "skipped_files": 0,
        "skipped_rows": 0,
        "metrics": {
            "zip_url": None,
            "zip_size_bytes": 0,
            "zip_entries_total": 0,
            "zip_csv_total": 0,
            "accepted_csvs": [],
            "ignored_csvs": [],
            "csv_rows_read": 0,
            "rows_after_parse": 0,
            "rows_after_filters": 0,
            "rows_after_ticker_merge": 0,
            "rows_with_ticker": 0,
            "rows_without_ticker": 0,
            "rows_prepared": 0,
            "rows_missing_dt_refer": 0,
        },
    }


def process_year(session: requests.Session, year: int, ticker_map: pd.DataFrame) -> Dict[str, object]:
    result = _empty_year_result()
    result["metrics"]["zip_url"] = _url_zip_year(year)

    # ── Download ────────────────────────────────────────────────────────────
    try:
        raw_zip = _download_year_zip(session, year)
        result["metrics"]["zip_size_bytes"] = len(raw_zip)
    except FileNotFoundError as exc:
        log(f"  — {exc}", year=year, stage="year_not_available")
        result["not_available"] = True
        return result
    except Exception as exc:
        message = f"{DOC_TYPE} {year}: erro ao baixar ZIP: {exc}"
        result["errors"].append(message)
        log(message, level="ERROR", year=year, stage="download_failed")
        return result

    zip_kb = len(raw_zip) // 1024
    log(f"  ZIP baixado: OK — {zip_kb:,} KB", year=year, stage="zip_downloaded", zip_size_bytes=len(raw_zip))

    # ── Inspeciona conteúdo do ZIP ──────────────────────────────────────────
    try:
        zip_buf = io.BytesIO(raw_zip)
        with zipfile.ZipFile(zip_buf) as zip_ref:
            all_names = zip_ref.namelist()
            all_csv = [f for f in all_names if f.endswith(".csv")]
            relevant = [f for f in all_csv if _arquivo_relevante(f)]
            irrelevant = [f for f in all_csv if not _arquivo_relevante(f)]
            result["metrics"]["zip_entries_total"] = len(all_names)
            result["metrics"]["zip_csv_total"] = len(all_csv)
            result["metrics"]["accepted_csvs"] = relevant
            result["metrics"]["ignored_csvs"] = irrelevant

            log(
                f"  Arquivos no ZIP: {len(all_names)} total | {len(all_csv)} CSVs | "
                f"{len(relevant)} relevantes | {len(irrelevant)} ignorados",
                year=year,
                stage="zip_inventory",
                zip_entries_total=len(all_names),
                zip_csv_total=len(all_csv),
                accepted_csvs=relevant,
                ignored_csvs=irrelevant,
            )

            if not all_csv:
                msg = f"{DOC_TYPE} {year}: ZIP sem arquivos CSV — estrutura pode ter mudado."
                result["errors"].append(msg)
                log(msg, level="ERROR", year=year, stage="zip_without_csv")
                return result

            if not relevant:
                msg = (
                    f"{DOC_TYPE} {year}: nenhum arquivo passou pelo filtro _arquivo_relevante(). "
                    f"Estrutura/nomenclatura do ZIP pode ter mudado. CSVs encontrados: {all_csv}"
                )
                result["errors"].append(msg)
                result["skipped_files"] += len(irrelevant)
                log(
                    msg,
                    level="ERROR",
                    year=year,
                    stage="no_relevant_csv",
                    csvs_found=all_csv,
                    demos_valuation=sorted(DEMOS_VALUATION),
                )
                return result

            for filename in relevant:
                demo_type = _infer_demo_type(filename)
                with zip_ref.open(filename) as csvfile:
                    try:
                        df = pd.read_csv(csvfile, sep=";", decimal=",", encoding="ISO-8859-1")
                        n_lidas = len(df)
                        result["metrics"]["csv_rows_read"] += n_lidas
                        log(
                            f"    [{filename}] Linhas lidas: {n_lidas}",
                            year=year,
                            file=filename,
                            stage="csv_read",
                            rows_read=n_lidas,
                            columns=list(df.columns),
                        )

                        if df.empty:
                            log(f"    [{filename}] CSV vazio — pulando.", year=year, file=filename, stage="csv_empty")
                            continue

                        if validate_required_columns:
                            validate_required_columns(
                                df,
                                ["CD_CVM", "DT_REFER", "CD_CONTA", "VL_CONTA"],
                                context=f"{DOC_TYPE} {year} {filename}",
                                logger=_RUN_LOG,
                            )

                        # Normaliza tipos mínimos antes de filtros/merge
                        df = df.copy()
                        df["CD_CVM"] = pd.to_numeric(df["CD_CVM"], errors="coerce").astype("Int64")
                        result["metrics"]["rows_after_parse"] += len(df)

                        # ── Filtro 2: ORDEM_EXERC ───────────────────────────
                        if "ORDEM_EXERC" in df.columns:
                            valores_unicos = df["ORDEM_EXERC"].astype(str).str.strip().unique().tolist()
                            mask_exerc = (
                                df["ORDEM_EXERC"]
                                .astype(str).str.strip().str.upper()
                                .isin(ORDEM_EXERC_VALIDO)
                            )
                            n_antes = len(df)
                            df = df[mask_exerc].copy()
                            log(
                                f"    [{filename}] Após filtro ORDEM_EXERC: {n_antes} → {len(df)}",
                                year=year,
                                file=filename,
                                stage="filter_ordem_exerc",
                                rows_before=n_antes,
                                rows_after=len(df),
                                ordem_exerc_values=valores_unicos,
                                ordem_exerc_valid=sorted(ORDEM_EXERC_VALIDO),
                            )
                            if df.empty:
                                result["skipped_rows"] += n_antes
                                continue
                        else:
                            log(
                                f"    [{filename}] Coluna ORDEM_EXERC ausente — sem filtro de exercício.",
                                year=year,
                                file=filename,
                                stage="filter_ordem_exerc_skipped",
                            )

                        # ── Filtro 3: NIVEL_CONTA ───────────────────────────
                        if "NIVEL_CONTA" in df.columns:
                            niveis = sorted(df["NIVEL_CONTA"].dropna().unique().tolist())
                            n_antes = len(df)
                            df = df[
                                pd.to_numeric(df["NIVEL_CONTA"], errors="coerce")
                                .fillna(99).le(NIVEL_CONTA_MAX)
                            ].copy()
                            log(
                                f"    [{filename}] Após filtro NIVEL_CONTA <= {NIVEL_CONTA_MAX}: {n_antes} → {len(df)}",
                                year=year,
                                file=filename,
                                stage="filter_nivel_conta",
                                rows_before=n_antes,
                                rows_after=len(df),
                                nivel_conta_values=niveis,
                                nivel_conta_max=NIVEL_CONTA_MAX,
                            )
                            if df.empty:
                                result["skipped_rows"] += n_antes
                                continue
                        else:
                            log(
                                f"    [{filename}] Coluna NIVEL_CONTA ausente — sem filtro de nível.",
                                year=year,
                                file=filename,
                                stage="filter_nivel_conta_skipped",
                            )

                        result["metrics"]["rows_after_filters"] += len(df)
                        result["skipped_rows"] += n_lidas - len(df)
                        df = _normalize_value_scale(df)

                        # ── Merge com ticker_map ────────────────────────────
                        n_antes_merge = len(df)
                        df = df.merge(ticker_map, how="left", on="CD_CVM")
                        with_ticker = int(df["Ticker"].notna().sum()) if "Ticker" in df.columns else 0
                        without_ticker = len(df) - with_ticker
                        result["metrics"]["rows_after_ticker_merge"] += len(df)
                        result["metrics"]["rows_with_ticker"] += with_ticker
                        result["metrics"]["rows_without_ticker"] += without_ticker
                        log(
                            f"    [{filename}] Após merge ticker_map: {n_antes_merge} → {len(df)}",
                            year=year,
                            file=filename,
                            stage="ticker_merge",
                            rows_before=n_antes_merge,
                            rows_after=len(df),
                            ticker_map_size=len(ticker_map),
                            rows_with_ticker=with_ticker,
                            rows_without_ticker=without_ticker,
                        )

                        group_demo = _infer_group_demo(filename, df)
                        n_rows_antes = len(result["rows"])

                        # ── Monta linhas ────────────────────────────────────
                        sem_dt_refer = 0
                        for row in df.to_dict(orient="records"):
                            dt_refer = _safe_date(row.get("DT_REFER"))
                            if dt_refer is None:
                                sem_dt_refer += 1
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
                            result["rows"].append({
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
                            })

                        adicionadas = len(result["rows"]) - n_rows_antes
                        result["metrics"]["rows_missing_dt_refer"] += sem_dt_refer
                        result["metrics"]["rows_prepared"] += adicionadas
                        log(
                            f"    [{filename}] Linhas preparadas para insert: {adicionadas}",
                            year=year,
                            file=filename,
                            stage="rows_prepared",
                            rows_prepared=adicionadas,
                            rows_missing_dt_refer=sem_dt_refer,
                        )

                    except Exception as exc:
                        import traceback as _tb
                        message = (f"{DOC_TYPE} {year}: erro ao processar {filename}: {exc}\n"
                                   + _tb.format_exc())
                        result["errors"].append(message)
                        log(message, level="ERROR", year=year, file=filename, stage="file_processing_failed")

        total = len(result["rows"])
        log(
            f"  RESUMO {DOC_TYPE} {year}: {total} linhas úteis | "
            f"{result['skipped_files']} arquivos ignorados | "
            f"{result['skipped_rows']} linhas filtradas",
            year=year,
            stage="year_summary",
            metrics=result["metrics"],
        )

        if total == 0 and not result["errors"]:
            msg = (
                f"{DOC_TYPE} {year}: process_year() retornou 0 linhas sem erro explícito. "
                f"Possível perda em filtros, DT_REFER ou estrutura nova do CSV. "
                f"Métricas: {json.dumps(result['metrics'], ensure_ascii=False)}"
            )
            result["errors"].append(msg)
            log(msg, level="ERROR", year=year, stage="year_zero_rows_without_explicit_error")

        return result

    except zipfile.BadZipFile:
        message = f"{DOC_TYPE} {year}: ZIP inválido ou corrompido."
        result["errors"].append(message)
        log(message, level="ERROR", year=year, stage="bad_zip")
        return result


# =========================================================
# DB
# =========================================================
def _get_years_with_data(source_doc: str) -> set:
    """Retorna conjunto de anos que já possuem linhas em cvm_financial_raw.

    Usado para pular anos já processados em execuções anteriores,
    permitindo retomada incremental sem re-processar dados existentes.
    """
    try:
        from sqlalchemy import text as sa_text
        engine = get_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                sa_text(
                    """
                    SELECT DISTINCT EXTRACT(YEAR FROM dt_refer::date)::int AS ano
                    FROM public.cvm_financial_raw
                    WHERE source_doc = :doc
                    ORDER BY ano
                    """
                ),
                {"doc": source_doc},
            ).fetchall()
        result = {int(r[0]) for r in rows if r[0] is not None}
        return result
    except Exception as exc:
        log(f"[WARN] Não foi possível verificar anos já processados: {exc} — prosseguindo sem filtro.", level="WARN")
        return set()


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

    log(f"Upsert concluído em cvm_financial_raw: {len(df)} linhas.", rows=len(df), stage="upsert_done")
    return len(df)


# =========================================================
# RUN REGISTRY
# =========================================================
def _update_run_progress(run_id: str, metrics: dict) -> None:
    """Atualiza o campo metrics de um run em andamento. Falha silenciosamente."""
    try:
        engine = get_engine()
        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.cvm_ingestion_runs
                    SET metrics = %s::jsonb, updated_at = NOW()
                    WHERE run_id = %s
                    """,
                    (json.dumps(metrics, ensure_ascii=False), run_id),
                )
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()
    except Exception as exc:
        log(f"[WARN] _update_run_progress falhou (não crítico): {exc}", level="WARN")


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

    # ── Validações de pré-condição (antes de tocar o banco) ────────────────
    if DOC_TYPE not in URLS:
        raise ValueError(
            f"CVM_DOC_TYPE inválido: '{DOC_TYPE}'. Deve ser 'DFP' ou 'ITR'. "
            "Defina a variável de ambiente corretamente antes de executar."
        )

    try:
        from core.cvm_v2_schema_check import assert_v2_schema_ready
        assert_v2_schema_ready()
    except ImportError:
        pass   # módulo de checagem não disponível — prossegue sem validação

    # ── Descoberta do intervalo de anos (antes de criar o run) ─────────────
    log(f"Iniciando {DOC_TYPE} RAW V2. Descobrindo intervalo de anos disponíveis na CVM…")
    session = build_session()
    ticker_map = _load_ticker_map()
    log(f"Ticker map carregado: {len(ticker_map)} empresas.", stage="ticker_map_loaded", ticker_map_size=len(ticker_map))

    last_year = int(ULTIMO_ANO) if ULTIMO_ANO else _last_available_year(session)
    all_years = list(range(ANO_INICIAL, last_year + 1))

    # ── Filtra anos já com dados no banco (retomada incremental) ───────────
    log("Verificando anos já processados no banco…")
    years_with_data = _get_years_with_data(DOC_TYPE)
    if years_with_data:
        log(f"Anos já com dados ({len(years_with_data)}): {sorted(years_with_data)}")
    else:
        log("Nenhum dado encontrado ainda — iniciando do zero.")

    years_pending = [y for y in all_years if y not in years_with_data]

    if not years_pending:
        log(f"Todos os {len(all_years)} anos já estão no banco. Nada a fazer.")
        return

    # ── Limita ao máximo por execução para evitar timeout/OOM ──────────────
    years = years_pending[:MAX_ANOS_POR_RUN]
    anos_restantes_apos = len(years_pending) - len(years)

    log(
        f"Anos pendentes: {len(years_pending)} | "
        f"Processando agora: {len(years)} ({years[0]}–{years[-1]}) | "
        f"Restam após esta execução: {anos_restantes_apos} | "
        f"Limite por run: MAX_ANOS_POR_RUN={MAX_ANOS_POR_RUN}"
    )

    # ── Cria o run registry DEPOIS de saber o intervalo real ───────────────
    run_id = _create_run(DOC_TYPE, years[0], years[-1])
    log(f"Run criada: {run_id}")

    # Progresso inicial
    progress: dict = {
        "stage": "processing",
        "anos_neste_lote": years,
        "anos_ja_no_banco": sorted(years_with_data),
        "anos_restantes_apos_run": anos_restantes_apos,
        "years_total": len(years),
        "years_done": 0,
        "years_failed": 0,
        "failed_years_count": 0,
        "current_year": None,
        "raw_rows_accum": 0,
        "inserted_rows_accum": 0,
        "last_event_at": datetime.utcnow().isoformat(),
        "message": f"Iniciando lote: {years[0]}–{years[-1]} ({len(years)} anos)…",
        "year_details": {},
    }
    _update_run_progress(run_id, progress)

    all_errors: List[str] = []

    try:
        if _IngestionLog:
            _log_ctx = _IngestionLog(f"{DOC_TYPE.lower()}_raw_v2")
            _log_ctx.__enter__()
            _RUN_LOG = _log_ctx
            _log_ctx.set_params({"ano_inicial": ANO_INICIAL, "ultimo_ano": last_year})
        else:
            _log_ctx = None

        try:
            for year in years:
                progress["current_year"] = year
                progress["stage"] = "processing"
                progress["message"] = f"Processando ano {year}…"
                progress["last_event_at"] = datetime.utcnow().isoformat()
                _update_run_progress(run_id, progress)

                t_year = time.time()
                log(f"▶ Processando {DOC_TYPE} {year}…", year=year, stage="year_start")
                year_result = process_year(session, year, ticker_map)

                year_metrics = year_result.get("metrics", {}) or {}
                year_errors = year_result.get("errors", []) or []

                # Ano não disponível na CVM (ex: ITR 2010) — pula sem contar como erro
                if year_result.get("not_available"):
                    progress["years_done"] += 1
                    progress["message"] = f"Ano {year} não disponível na CVM — pulado."
                    progress["year_details"][str(year)] = {
                        "status": "not_available",
                        "metrics": year_metrics,
                        "errors": year_errors,
                    }
                    _update_run_progress(run_id, progress)
                    continue

                rows = year_result["rows"]
                errors = year_errors
                all_errors.extend(errors)

                progress["raw_rows_accum"] += len(rows)
                progress["skipped_files_accum"] = progress.get("skipped_files_accum", 0) + year_result.get("skipped_files", 0)
                progress["skipped_rows_accum"] = progress.get("skipped_rows_accum", 0) + year_result.get("skipped_rows", 0)
                progress["last_event_at"] = datetime.utcnow().isoformat()
                progress["year_details"][str(year)] = {
                    "status": "processed",
                    "metrics": year_metrics,
                    "errors": errors,
                    "rows_prepared": len(rows),
                }

                inserted = 0
                if rows:
                    try:
                        inserted = upsert_cvm_financial_raw(rows, run_id)
                        progress["inserted_rows_accum"] += inserted
                        progress["year_details"][str(year)]["rows_inserted"] = inserted
                        log(
                            f"  ✓ Ano {year}: {len(rows)} linhas raw, {inserted} inseridas em {round(time.time() - t_year, 1)}s.",
                            year=year,
                            stage="year_upsert_done",
                            rows_prepared=len(rows),
                            rows_inserted=inserted,
                        )
                    except Exception as upsert_exc:
                        import traceback as _tb
                        msg = (
                            f"{DOC_TYPE} {year}: erro no upsert: {upsert_exc}\n"
                            + _tb.format_exc()
                        )
                        all_errors.append(msg)
                        errors.append(msg)
                        progress["year_details"][str(year)]["status"] = "upsert_failed"
                        progress["year_details"][str(year)]["rows_inserted"] = 0
                        log(msg, level="ERROR", year=year, stage="year_upsert_failed")
                        progress["years_failed"] += 1
                        progress["failed_years_count"] = progress["years_failed"]
                        progress["message"] = f"ERRO upsert ano {year}: {upsert_exc}"
                        _update_run_progress(run_id, progress)
                        continue
                else:
                    log(
                        f"  — Ano {year}: nenhuma linha retornada (erros: {len(errors)}).",
                        year=year,
                        stage="year_zero_rows",
                        year_metrics=year_metrics,
                    )

                if errors or inserted == 0:
                    if inserted == 0 and not errors:
                        msg = (
                            f"{DOC_TYPE} {year}: 0 linhas inseridas sem exceção. "
                            f"Métricas do ano: {json.dumps(year_metrics, ensure_ascii=False)}"
                        )
                        errors.append(msg)
                        all_errors.append(msg)
                        log(msg, level="ERROR", year=year, stage="year_zero_insert_explicit_error")
                    progress["years_failed"] += 1
                    progress["failed_years_count"] = progress["years_failed"]
                    progress["year_details"][str(year)]["status"] = "failed"
                else:
                    progress["years_done"] += 1
                    progress["year_details"][str(year)]["status"] = "success"

                progress["message"] = (
                    f"Ano {year} concluído. Acumulado: {progress['raw_rows_accum']} raw, "
                    f"{progress['inserted_rows_accum']} inseridas."
                )
                _update_run_progress(run_id, progress)

            # ── Determina status final ──────────────────────────────────────
            years_done = progress["years_done"]
            years_failed = progress["years_failed"]
            inserted_total = progress["inserted_rows_accum"]

            if inserted_total == 0:
                final_status = "failed"
                all_errors.append(
                    f"{DOC_TYPE}: execução terminou com 0 inserts totais. Verifique metrics.year_details e logs dos filtros."
                )
            elif years_failed == 0:
                final_status = "success"
            elif years_done == 0:
                final_status = "failed"
            else:
                final_status = "partial_success"

            final_metrics = {
                **progress,
                "stage": "finished",
                "current_year": None,
                "message": (
                    f"Concluído: {years_done}/{len(years)} anos OK, "
                    f"{years_failed} com erro. "
                    f"Raw rows: {progress['raw_rows_accum']}, "
                    f"Inseridas: {progress['inserted_rows_accum']}."
                ),
                "last_event_at": datetime.utcnow().isoformat(),
            }

            errors_payload = {"errors": all_errors[-200:]} if all_errors else {}

            _finish_run(
                run_id,
                status=final_status,
                last_year=last_year,
                metrics=final_metrics,
                errors=errors_payload,
            )

            if _log_ctx:
                _log_ctx.set_metric("ultimo_ano_disponivel", last_year)
                _log_ctx.set_metric("raw_rows", progress["raw_rows_accum"])
                _log_ctx.add_rows(inserted=progress["inserted_rows_accum"])

            log(
                f"Ingestão RAW V2 {final_status} | Doc: {DOC_TYPE} | Run: {run_id} | raw={progress['raw_rows_accum']} inserted={progress['inserted_rows_accum']}",
                stage="final",
                run_id=run_id,
            )

        except Exception as exc:
            err_msg = str(exc)
            all_errors.append(err_msg)
            _finish_run(
                run_id,
                status="failed",
                last_year=last_year,
                metrics={
                    **progress,
                    "stage": "aborted",
                    "message": f"Exceção fatal: {err_msg}",
                    "last_event_at": datetime.utcnow().isoformat(),
                },
                errors={"errors": all_errors[-200:]},
            )
            log(f"Falha fatal na ingestão RAW V2: {exc}", level="ERROR", stage="final_failed", run_id=run_id)
            raise

        finally:
            if _log_ctx:
                try:
                    _log_ctx.__exit__(None, None, None)
                except Exception:
                    pass
            _RUN_LOG = None

    except Exception:
        _RUN_LOG = None
        raise


def diagnostico_ano(year: int = 2023) -> None:
    """Executa process_year() para um único ano com log detalhado.

    Uso:
        python pickup/cvm_extract_v2.py diag 2023
        python pickup/cvm_extract_v2.py diag 2022

    Não grava nada no banco — apenas mostra o que seria processado.
    """
    print(f"\n{'='*60}")
    print(f"DIAGNÓSTICO {DOC_TYPE} — ano {year}")
    print(f"{'='*60}")
    print(f"URL base  : {URLS.get(DOC_TYPE, 'INVÁLIDO')}")
    print(f"URL do ZIP: {_url_zip_year(year)}")
    print(f"DEMOS_VALUATION: {DEMOS_VALUATION}")
    print(f"NIVEL_CONTA_MAX: {NIVEL_CONTA_MAX}")
    print(f"ORDEM_EXERC_VALIDO: {ORDEM_EXERC_VALIDO}")
    print(f"ticker_map: {TICKER_PATH} (existe={TICKER_PATH.exists()})")
    print(f"{'='*60}\n")

    session = build_session()
    try:
        ticker_map = _load_ticker_map()
        print(f"Ticker map carregado: {len(ticker_map)} empresas\n")
    except Exception as exc:
        print(f"ERRO ao carregar ticker_map: {exc}")
        return

    result = process_year(session, year, ticker_map)

    print(f"\n{'='*60}")
    print(f"RESULTADO FINAL — {DOC_TYPE} {year}")
    print(f"{'='*60}")
    print(f"not_available  : {result.get('not_available', False)}")
    print(f"rows           : {len(result['rows'])}")
    print(f"errors         : {len(result['errors'])}")
    print(f"skipped_files  : {result.get('skipped_files', 0)}")
    print(f"skipped_rows   : {result.get('skipped_rows', 0)}")
    print(f"metrics        : {json.dumps(result.get('metrics', {}), ensure_ascii=False, indent=2)}")

    if result["errors"]:
        print("\nERROS:")
        for e in result["errors"]:
            print(f"  {e}")

    if result["rows"]:
        print(f"\nPrimeiras 3 linhas:")
        for r in result["rows"][:3]:
            print(f"  {r}")
    else:
        print("\n⚠️  ZERO linhas retornadas — verifique os logs acima para identificar o filtro causador.")


if __name__ == "__main__":
    import sys as _sys
    if len(_sys.argv) >= 2 and _sys.argv[1] == "diag":
        _year = int(_sys.argv[2]) if len(_sys.argv) >= 3 else 2023
        diagnostico_ano(_year)
    else:
        main()
