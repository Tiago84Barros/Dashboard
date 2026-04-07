import io
import os
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

try:
    from auditoria_dados.ingestion_log import IngestionLog as _IngestionLog
    from auditoria_dados.ingestion_log import validate_required_columns
    from auditoria_dados.ingestion_log import validate_key_columns
    from auditoria_dados.ingestion_log import validate_unique_rows
except ImportError:
    _IngestionLog = None
    validate_required_columns = None
    validate_key_columns = None
    validate_unique_rows = None

import numpy as np
import pandas as pd
import psycopg2
import requests
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ATENÇÃO:
# 'Dividendos' aqui representam valores CONTÁBEIS TOTAIS (DFP/CVM).
# NÃO são dividendos por ação e NÃO devem ser usados em backtests de reinvestimento.

# =========================
# CONFIG
# =========================
URL_BASE_DFP = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))
ANO_INICIAL = int(os.getenv("ANO_INICIAL", "2010"))
ULTIMO_ANO = int(os.getenv("ULTIMO_ANO", "0"))  # 0 => auto
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "180"))
CACHE_ZIPS = os.getenv("CACHE_ZIPS", "1").strip() == "1"
FORCAR_REDOWNLOAD = os.getenv("FORCAR_REDOWNLOAD", "0").strip() == "1"
BATCH_SIZE_UPSERT = int(os.getenv("BATCH_SIZE_UPSERT", "5000"))
LOG_PREFIX = os.getenv("LOG_PREFIX", "[DFP]")

SUPABASE_DB_URL = (
    os.getenv("SUPABASE_DB_URL", "").strip()
    or os.getenv("SUPABASE_DB_URL_PG", "").strip()
)

BASE_DIR = Path(__file__).resolve().parent
TICKER_PATH = Path(os.getenv("TICKER_PATH", str(BASE_DIR / "cvm_to_ticker.csv")))
CACHE_DIR = Path(os.getenv("CVM_CACHE_DIR", str(BASE_DIR / ".cache_cvm_dfp")))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

LPA_ABS_MAX_DB = 1e14 - 1
_RUN_LOG = None


# =========================
# LOG
# =========================
def log(msg: str, level: str = "INFO", **fields) -> None:
    rendered = f"{LOG_PREFIX} {msg}"
    if _RUN_LOG:
        _RUN_LOG.log(level, "pipeline_log", message=msg, rendered=rendered, **fields)
        return
    print(rendered, flush=True)


# =========================
# HTTP / RETRY
# =========================
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
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS * 2, pool_maxsize=MAX_WORKERS * 2)
    session = requests.Session()
    session.headers.update({
        "User-Agent": "dashboard-financeiro-dfp/1.0",
        "Accept": "*/*",
        "Connection": "keep-alive",
    })
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# =========================
# DB URL / DSN
# =========================
def _normalize_db_url(db_url: str) -> str:
    db_url = (db_url or "").strip()
    if not db_url:
        raise RuntimeError("Defina  (connection string Postgres do Supabase).")

    # psycopg2 não aceita o prefixo do SQLAlchemy
    db_url = re.sub(r"^postgresql\+psycopg2://", "postgresql://", db_url, flags=re.IGNORECASE)
    db_url = re.sub(r"^postgres://", "postgresql://", db_url, flags=re.IGNORECASE)

    parsed = urlparse(db_url)
    if parsed.scheme not in {"postgresql", "postgres"}:
        raise RuntimeError(
            "SUPABASE_DB_URL inválida. Use formato 'postgresql://usuario:senha@host:5432/postgres'."
        )

    if not parsed.hostname or not parsed.path or parsed.path == "/":
        raise RuntimeError("SUPABASE_DB_URL incompleta: host ou database ausente.")

    normalized = urlunparse(("postgresql", parsed.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    return normalized


def _empty_year_result() -> Dict[str, List[pd.DataFrame] | List[str]]:
    return {"DRE": [], "BPA": [], "BPP": [], "DFC_MI": [], "errors": []}


def _assert_unique_key_ready(cur, table_name: str, key_columns: Tuple[str, ...]) -> None:
    cur.execute(
        """
        SELECT 1
        FROM pg_index i
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = %s
          AND i.indisunique
          AND (
              SELECT array_agg(a.attname ORDER BY x.ord)
              FROM unnest(i.indkey) WITH ORDINALITY AS x(attnum, ord)
              JOIN pg_attribute a
                ON a.attrelid = t.oid
               AND a.attnum = x.attnum
              WHERE x.attnum > 0
          ) = %s::text[]
        LIMIT 1
        """,
        (table_name, list(key_columns)),
    )
    if cur.fetchone() is None:
        raise RuntimeError(
            f'A tabela public."{table_name}" precisa de UNIQUE/PK em {key_columns} para ON CONFLICT.'
        )


# =========================
# UTIL: descobrir último ano disponível na CVM
# =========================
def _url_zip_ano(ano: int) -> str:
    return URL_BASE_DFP + f"dfp_cia_aberta_{ano}.zip"


def _cache_zip_path(ano: int) -> Path:
    return CACHE_DIR / f"dfp_cia_aberta_{ano}.zip"


def _head_ok(session: requests.Session, url: str) -> bool:
    try:
        r = session.head(url, timeout=30, allow_redirects=True)
        return r.status_code == 200
    except requests.RequestException:
        return False


def _ultimo_ano_disponivel(session: requests.Session, ano_max: Optional[int] = None, max_back: int = 12) -> int:
    if ano_max is None:
        ano_max = datetime.now().year

    for ano in range(ano_max, ano_max - max_back - 1, -1):
        if _head_ok(session, _url_zip_ano(ano)):
            return ano

    return ano_max - max_back


# =========================
# DOWNLOAD / CACHE ZIP
# =========================
def _baixar_zip_ano(session: requests.Session, ano: int) -> bytes:
    url = _url_zip_ano(ano)
    cache_path = _cache_zip_path(ano)

    if CACHE_ZIPS and cache_path.exists() and not FORCAR_REDOWNLOAD:
        log(f"Usando cache local do ano {ano}: {cache_path.name}")
        return cache_path.read_bytes()

    log(f"Baixando ZIP DFP {ano}...")
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Falha ao baixar ano {ano} (status={r.status_code})")

    content = r.content
    if CACHE_ZIPS:
        cache_path.write_bytes(content)
    return content


# =========================
# NORMALIZAÇÃO DE ESCALA (CVM) — COM EXCEÇÃO PARA CONTAS POR AÇÃO
# =========================
def _normalizar_vl_conta_por_escala(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "VL_CONTA" not in df.columns or "ESCALA_MOEDA" not in df.columns:
        return df

    out = df.copy()
    out["VL_CONTA"] = pd.to_numeric(out["VL_CONTA"], errors="coerce")

    escala = out["ESCALA_MOEDA"].astype(str).str.strip().str.upper()

    fatores = pd.Series(1.0, index=out.index)
    fatores.loc[escala.isin(["MIL", "MILHAR", "MILHARES"])] = 1_000.0
    fatores.loc[escala.isin(["MILHAO", "MILHÃO", "MILHOES", "MILHÕES"])] = 1_000_000.0
    fatores.loc[escala.isin(["BILHAO", "BILHÃO", "BILHOES", "BILHÕES"])] = 1_000_000_000.0

    if "CD_CONTA" in out.columns:
        cd = out["CD_CONTA"].astype(str)
        mask_por_acao = cd.str.startswith("3.99", na=False)
        fatores.loc[mask_por_acao] = 1.0

    out["VL_CONTA"] = out["VL_CONTA"] * fatores
    return out


# =========================
# NORMALIZAÇÃO LPA
# =========================
def _normalizar_lpa_series(s: pd.Series) -> pd.Series:
    s2 = pd.to_numeric(s, errors="coerce").astype("float64")

    for _ in range(8):
        mask = s2.abs() > 1e6
        if not mask.any():
            break
        s2.loc[mask] = s2.loc[mask] / 1000.0

    s2.loc[s2.abs() >= LPA_ABS_MAX_DB] = np.nan
    return s2.fillna(0).round(6)


# =========================
# COLETA DFP (PARALELISMO)
# =========================
def processar_ano_dfp(session: requests.Session, ano: int):
    result = _empty_year_result()
    try:
        raw_zip = _baixar_zip_ano(session, ano)
    except Exception as e:
        message = f"DFP {ano}: erro ao baixar ZIP: {e}"
        result["errors"].append(message)
        log(message, level="ERROR", year=ano, stage="download_failed")
        return result

    try:
        with zipfile.ZipFile(io.BytesIO(raw_zip)) as zip_ref:
            for arquivo in zip_ref.namelist():
                if arquivo.endswith(".csv") and "_con_" in arquivo:
                    with zip_ref.open(arquivo) as csvfile:
                        try:
                            df_temp = pd.read_csv(csvfile, sep=";", decimal=",", encoding="ISO-8859-1")
                            if validate_required_columns:
                                validate_required_columns(
                                    df_temp,
                                    ["CD_CVM", "DT_REFER", "CD_CONTA", "VL_CONTA"],
                                    context=f"DFP {ano} {arquivo}",
                                    logger=_RUN_LOG,
                                )

                            if "ORDEM_EXERC" in df_temp.columns:
                                df_temp = df_temp[df_temp["ORDEM_EXERC"] == "ÚLTIMO"]

                            df_temp = _normalizar_vl_conta_por_escala(df_temp)

                            up = arquivo.upper()
                            if "DRE" in up:
                                result["DRE"].append(df_temp)
                            elif "BPA" in up:
                                result["BPA"].append(df_temp)
                            elif "BPP" in up:
                                result["BPP"].append(df_temp)
                            elif "DFC" in up:
                                result["DFC_MI"].append(df_temp)

                        except Exception as e:
                            message = f"DFP {ano}: erro ao processar arquivo {arquivo}: {e}"
                            result["errors"].append(message)
                            if _RUN_LOG:
                                _RUN_LOG.increment_metric("arquivos_com_erro")
                            log(
                                message,
                                level="ERROR",
                                year=ano,
                                file=arquivo,
                                stage="file_processing_failed",
                            )
            if not result["DRE"]:
                result["errors"].append(f"DFP {ano}: nenhum arquivo DRE consolidado válido encontrado.")
            return result
    except zipfile.BadZipFile:
        message = f"DFP {ano}: ZIP inválido."
        result["errors"].append(message)
        if _RUN_LOG:
            _RUN_LOG.increment_metric("zips_invalidos")
        log(message, level="ERROR", year=ano, stage="bad_zip")
        return result



def coletar_dfp() -> tuple[dict, int]:
    session = build_session()

    ultimo_ano = ULTIMO_ANO if ULTIMO_ANO > 0 else _ultimo_ano_disponivel(session, ano_max=datetime.now().year, max_back=12)
    anos = list(range(ANO_INICIAL, ultimo_ano + 1))

    df_dict_dfp = {"DRE": pd.DataFrame(), "BPA": pd.DataFrame(), "BPP": pd.DataFrame(), "DFC_MI": pd.DataFrame()}

    if not anos:
        if _RUN_LOG:
            _RUN_LOG.add_warning("Intervalo de anos vazio. Verifique ANO_INICIAL/ULTIMO_ANO.")
        log("Intervalo de anos vazio. Verifique ANO_INICIAL/ULTIMO_ANO.", level="WARN")
        return df_dict_dfp, ultimo_ano

    log(f"Coletando DFP do intervalo {ANO_INICIAL}..{ultimo_ano}")
    start = time.time()
    failed_years: Dict[int, List[str]] = {}

    futures = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for ano in anos:
            futures[executor.submit(processar_ano_dfp, session, ano)] = ano

        for future in as_completed(futures):
            ano = futures[future]
            try:
                df_temp_dict = future.result()
                year_errors = list(df_temp_dict.get("errors", []))
                if year_errors:
                    failed_years[ano] = year_errors
                    if _RUN_LOG:
                        _RUN_LOG.increment_metric("anos_com_erro")
                    log(
                        f"DFP {ano}: falhou com {len(year_errors)} erro(s).",
                        level="ERROR",
                        year=ano,
                        stage="year_failed",
                        errors=year_errors[:3],
                    )
                    continue
                for key in df_dict_dfp.keys():
                    if df_temp_dict.get(key):
                        df_dict_dfp[key] = pd.concat([df_dict_dfp[key]] + df_temp_dict[key], ignore_index=True)
                log(f"Ano {ano} processado com sucesso.", level="INFO", year=ano, stage="year_success")
                if _RUN_LOG:
                    _RUN_LOG.increment_metric("anos_processados")
            except Exception as e:
                failed_years[ano] = [str(e)]
                if _RUN_LOG:
                    _RUN_LOG.increment_metric("anos_com_erro")
                log(f"Falha no ano {ano}: {e}", level="ERROR", year=ano, stage="future_failed")

    elapsed = round(time.time() - start, 1)
    log(f"[OK] Coleta de dados anuais (DFP) concluída em {elapsed}s. (anos {ANO_INICIAL}..{ultimo_ano})")
    if failed_years:
        failed_preview = {ano: errs[:2] for ano, errs in sorted(failed_years.items())}
        raise RuntimeError(f"Falhas em {len(failed_years)} ano(s) DFP: {failed_preview}")
    return df_dict_dfp, ultimo_ano


# =========================
# CONSOLIDAÇÃO
# =========================
def montar_df_consolidado(df_dict_dfp: dict) -> pd.DataFrame:
    if df_dict_dfp["DRE"] is None or df_dict_dfp["DRE"].empty:
        return pd.DataFrame()

    if validate_required_columns:
        validate_required_columns(
            df_dict_dfp["DRE"],
            ["DENOM_CIA", "CD_CVM", "DT_REFER", "CD_CONTA", "VL_CONTA"],
            context="DFP DRE consolidado",
            logger=_RUN_LOG,
        )

    empresas = (
        df_dict_dfp["DRE"][["CD_CVM"]]
        .drop_duplicates(subset=["CD_CVM"])
        .set_index("CD_CVM")
    )

    def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        df = df.copy()
        if "DT_REFER" in df.columns:
            df["DT_REFER"] = pd.to_datetime(df["DT_REFER"], errors="coerce")
        return df

    def _serie_conta(df_conta: pd.DataFrame, idx: pd.DatetimeIndex) -> pd.Series:
        if df_conta is None or df_conta.empty:
            return pd.Series(index=idx, dtype="float64")
        dfc = df_conta[["DT_REFER", "VL_CONTA"]].copy()
        dfc["DT_REFER"] = pd.to_datetime(dfc["DT_REFER"], errors="coerce")
        dfc["VL_CONTA"] = pd.to_numeric(dfc["VL_CONTA"], errors="coerce")
        s = dfc.groupby("DT_REFER", dropna=True)["VL_CONTA"].sum()
        return s.reindex(idx)

    def _idx_base(conta_receita: pd.DataFrame,
                  empresa_bpa: pd.DataFrame,
                  empresa_dre: pd.DataFrame,
                  empresa_bpp: pd.DataFrame,
                  empresa_dfc: pd.DataFrame) -> pd.DatetimeIndex:
        if conta_receita is not None and not conta_receita.empty:
            idx = pd.to_datetime(conta_receita["DT_REFER"].unique(), errors="coerce")
        else:
            bpa_ativo_total = empresa_bpa[empresa_bpa["CD_CONTA"] == "1"] if empresa_bpa is not None else pd.DataFrame()
            if bpa_ativo_total is not None and not bpa_ativo_total.empty:
                idx = pd.to_datetime(bpa_ativo_total["DT_REFER"].unique(), errors="coerce")
            else:
                idx = None
                for _df in [empresa_dre, empresa_bpa, empresa_bpp, empresa_dfc]:
                    if _df is not None and not _df.empty and "DT_REFER" in _df.columns:
                        idx = pd.to_datetime(_df["DT_REFER"].unique(), errors="coerce")
                        break
                if idx is None:
                    return pd.DatetimeIndex([])
        idx = pd.DatetimeIndex(idx).dropna().unique().sort_values()
        return idx

    df_consolidado = pd.DataFrame()

    for cd_cvm in empresas.index:
        empresa_dre = _to_dt(df_dict_dfp["DRE"][df_dict_dfp["DRE"]["CD_CVM"] == cd_cvm])
        empresa_bpa = _to_dt(df_dict_dfp["BPA"][df_dict_dfp["BPA"]["CD_CVM"] == cd_cvm])
        empresa_bpp = _to_dt(df_dict_dfp["BPP"][df_dict_dfp["BPP"]["CD_CVM"] == cd_cvm])
        empresa_dfc = _to_dt(df_dict_dfp["DFC_MI"][df_dict_dfp["DFC_MI"]["CD_CVM"] == cd_cvm])

        conta_receita = empresa_dre[empresa_dre["CD_CONTA"] == "3.01"] if empresa_dre is not None else pd.DataFrame()
        idx = _idx_base(conta_receita, empresa_bpa, empresa_dre, empresa_bpp, empresa_dfc)
        if len(idx) == 0:
            continue

        df_empresa = pd.DataFrame(index=idx)
        df_empresa.index.name = "DT_REFER"

        conta_ebit = empresa_dre[empresa_dre["CD_CONTA"] == "3.05"]
        conta_lucro_liquido = empresa_dre[
            empresa_dre["DS_CONTA"].isin([
                "Lucro/Prejuízo Consolidado do Período",
                "Lucro ou Prejuízo Líquido Consolidado do Período"
            ])
        ]
        conta_lpa = empresa_dre[empresa_dre["CD_CONTA"] == "3.99.01.01"]

        conta_ativo_total = empresa_bpa[empresa_bpa["CD_CONTA"] == "1"]

        bpa_101 = empresa_bpa[empresa_bpa["CD_CONTA"] == "1.01"]
        if (bpa_101 is not None) and (not bpa_101.empty) and ("Ativo Circulante" in bpa_101["DS_CONTA"].values):
            conta_ativo_circulante = bpa_101
        else:
            conta_ativo_circulante = empresa_bpa[empresa_bpa["DS_CONTA"].isin([
                "Caixa e Equivalentes de Caixa",
                "Caixa",
                "Aplicações de Liquidez",
                "Ativos Financeiros Avaliados ao Valor Justo através do Resultado",
                "Ativos Financeiros Avaliados ao Valor Justo através de Outros Resultados Abrangentes",
                "Aplicações em Depósitos Interfinanceiros",
                "Aplicações no Mercado Aberto",
                "Derivativos",
                "Imposto de Renda e Contribuição Social - Correntes"
            ])]

        conta_caixa_e_equivalentes = empresa_bpa[empresa_bpa["DS_CONTA"] == "Caixa e Equivalentes de Caixa"]

        bpp_201 = empresa_bpp[empresa_bpp["CD_CONTA"] == "2.01"]
        if (bpp_201 is not None) and (not bpp_201.empty) and ("Passivo Circulante" in bpp_201["DS_CONTA"].values):
            conta_passivo_circulante = bpp_201
        else:
            conta_passivo_circulante = empresa_bpp[empresa_bpp["DS_CONTA"].isin([
                "Passivos Financeiros Avaliados ao Valor Justo através do Resultado",
                "Passivos Financeiros ao Custo Amortizado",
                "Depósitos",
                "Captações no Mercado Aberto",
                "Recursos Mercado Interfinanceiro",
                "Outras Captações",
                "Obrigações por emissão de títulos e valores mobiliários e outras obrigações",
                "Outros passivos financeiros",
                "Provisões",
                "Provisões trabalhistas, fiscais e cíveis"
            ])]

        conta_patrimonio_liquido = empresa_bpp[empresa_bpp["DS_CONTA"].isin(["Patrimônio Líquido Consolidado"])]
        conta_passivo_circulante_financeiro = empresa_bpp[empresa_bpp["CD_CONTA"].astype(str).str.startswith("2.01.04", na=False)]
        conta_passivo_nao_circulante_financeiro = empresa_bpp[empresa_bpp["CD_CONTA"].astype(str).str.startswith("2.02.01", na=False)]
        conta_passivo_total = empresa_bpp[empresa_bpp["CD_CONTA"] == "2"]

        conta_dividendos = empresa_dfc[empresa_dfc["DS_CONTA"].isin([
            "Dividendos", "Dividendos pagos", "Dividendos Pagos", "Pagamento de Dividendos",
            "Pagamento de Dividendos e JCP", "Pagamentos de Dividendos e JCP",
            "Dividendos Pagos a Acionistas", "Dividendos/JCP Pagos a Acionistas",
            "JCP e dividendos pagos e acionistas", "Dividendos e Juros s/Capital Próprio",
            "Dividendos e Juros sobre o Capital Próprio Pagos",
        ])]
        conta_dividendos_nctrl = empresa_dfc[
            empresa_dfc["DS_CONTA"].isin(["Dividendos ou juros sobre o capital próprio pagos aos acionistas não controladores"])
        ]
        conta_fco = empresa_dfc[empresa_dfc["CD_CONTA"] == "6.01"]

        df_empresa["CD_CVM"] = cd_cvm
        df_empresa["Data"] = df_empresa.index

        df_empresa["Receita Líquida"] = _serie_conta(conta_receita, idx)
        df_empresa["Ebit"] = _serie_conta(conta_ebit, idx)
        df_empresa["Lucro Líquido"] = _serie_conta(conta_lucro_liquido, idx)
        df_empresa["Lucro por Ação"] = _normalizar_lpa_series(_serie_conta(conta_lpa, idx))
        df_empresa["Ativo Total"] = _serie_conta(conta_ativo_total, idx)
        df_empresa["Ativo Circulante"] = _serie_conta(conta_ativo_circulante, idx)
        df_empresa["Caixa e Equivalentes"] = _serie_conta(conta_caixa_e_equivalentes, idx)
        df_empresa["Passivo Circulante"] = _serie_conta(conta_passivo_circulante, idx)
        df_empresa["Patrimônio Líquido"] = _serie_conta(conta_patrimonio_liquido, idx)
        df_empresa["Passivo Total"] = _serie_conta(conta_passivo_total, idx)
        df_empresa["Passivo Circulante Financeiro"] = _serie_conta(conta_passivo_circulante_financeiro, idx)
        df_empresa["Passivo Não Circulante Financeiro"] = _serie_conta(conta_passivo_nao_circulante_financeiro, idx)
        df_empresa["Dividendos"] = _serie_conta(conta_dividendos, idx)
        df_empresa["Dividendos Ncontroladores"] = _serie_conta(conta_dividendos_nctrl, idx)
        df_empresa["Caixa Líquido"] = _serie_conta(conta_fco, idx)

        cols_to_convert = [
            "Receita Líquida", "Ebit", "Lucro Líquido",
            "Ativo Total", "Ativo Circulante", "Passivo Circulante", "Passivo Total",
            "Passivo Circulante Financeiro", "Passivo Não Circulante Financeiro",
            "Caixa e Equivalentes", "Dividendos", "Dividendos Ncontroladores",
            "Patrimônio Líquido", "Caixa Líquido"
        ]
        for col in cols_to_convert:
            df_empresa[col] = pd.to_numeric(df_empresa[col], errors="coerce").fillna(0)

        df_empresa["Passivo Total"] = df_empresa["Passivo Total"] - df_empresa["Patrimônio Líquido"]
        df_empresa["Divida Total"] = df_empresa["Passivo Circulante Financeiro"] + df_empresa["Passivo Não Circulante Financeiro"]
        df_empresa["Dívida Líquida"] = df_empresa["Divida Total"] - df_empresa["Caixa e Equivalentes"]
        df_empresa["Dividendos Totais"] = (df_empresa["Dividendos"] + df_empresa["Dividendos Ncontroladores"]).abs()

        colunas_desejadas = [
            "CD_CVM", "Data", "Receita Líquida", "Ebit", "Lucro Líquido", "Lucro por Ação",
            "Ativo Total", "Ativo Circulante", "Passivo Circulante", "Passivo Total",
            "Divida Total", "Patrimônio Líquido", "Dividendos Totais", "Caixa Líquido", "Dívida Líquida",
        ]
        df_selecionado = df_empresa[colunas_desejadas].reset_index(drop=True)
        df_consolidado = pd.concat([df_consolidado, df_selecionado], ignore_index=True)

    return df_consolidado.fillna(0)


# =========================
# TICKER + REORDENAÇÃO
# =========================
def adicionar_ticker(df_consolidado: pd.DataFrame) -> pd.DataFrame:
    if not TICKER_PATH.exists():
        raise FileNotFoundError(f"Não encontrei o arquivo CVM->Ticker em: {TICKER_PATH}")

    cvm_to_ticker = pd.read_csv(TICKER_PATH, sep=",", encoding="utf-8")
    if validate_required_columns:
        validate_required_columns(
            cvm_to_ticker,
            ["CVM", "Ticker"],
            context="Mapa CVM->Ticker DFP",
            logger=_RUN_LOG,
        )
    if validate_key_columns:
        validate_key_columns(
            cvm_to_ticker,
            ["CVM", "Ticker"],
            context="Mapa CVM->Ticker DFP",
            logger=_RUN_LOG,
        )
    if validate_unique_rows:
        validate_unique_rows(
            cvm_to_ticker,
            ["CVM"],
            context="Mapa CVM->Ticker DFP",
            logger=_RUN_LOG,
        )
    df = pd.merge(df_consolidado, cvm_to_ticker, left_on="CD_CVM", right_on="CVM")
    df = df.drop(columns=["CD_CVM", "CVM"])
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.strftime("%Y-%m-%d")

    colunas = [
        "Ticker", "Data", "Receita Líquida", "Ebit", "Lucro Líquido", "Lucro por Ação",
        "Ativo Total", "Ativo Circulante", "Passivo Circulante", "Passivo Total", "Divida Total",
        "Patrimônio Líquido", "Dividendos Totais", "Caixa Líquido", "Dívida Líquida"
    ]
    out = df[colunas].copy()
    out["Ticker"] = out["Ticker"].astype(str).str.upper().str.strip()
    out["Data"] = pd.to_datetime(out["Data"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out[out["Ticker"].ne("") & out["Data"].notna()].copy()
    before_dedup = len(out)
    dup_preview = (
        out[out.duplicated(subset=["Ticker", "Data"], keep=False)][["Ticker", "Data"]]
        .head(10)
        .to_dict(orient="records")
    )
    out = (
        out.sort_values(["Ticker", "Data"])
        .drop_duplicates(subset=["Ticker", "Data"], keep="last")
        .reset_index(drop=True)
    )
    duplicates_removed = before_dedup - len(out)
    if duplicates_removed > 0:
        log(
            f"DFP com ticker removeu {duplicates_removed} duplicata(s) por (Ticker, Data).",
            level="WARN",
            duplicates_removed=duplicates_removed,
            duplicates_preview=dup_preview,
            stage="ticker_merge_dedup",
        )
    if _RUN_LOG:
        _RUN_LOG.set_metric("df_ticker_rows_before_dedup", before_dedup)
        _RUN_LOG.set_metric("df_ticker_duplicates_removed", duplicates_removed)
    if validate_key_columns:
        validate_key_columns(
            out,
            ["Ticker", "Data"],
            context="DFP com ticker",
            logger=_RUN_LOG,
        )
    if validate_unique_rows:
        validate_unique_rows(
            out,
            ["Ticker", "Data"],
            context="DFP com ticker",
            logger=_RUN_LOG,
        )
    return out


# =========================
# FILTRO
# =========================
def filtrar_empresas(df_consolidado: pd.DataFrame, ultimo_ano_disponivel: int) -> pd.DataFrame:
    colunas_essenciais = ["Receita Líquida"]
    tickers_aprovados = []

    for ticker in df_consolidado["Ticker"].dropna().unique():
        df_empresa = df_consolidado[df_consolidado["Ticker"] == ticker]
        anos_disponiveis = sorted(pd.to_datetime(df_empresa["Data"], errors="coerce").dt.year.dropna().unique())
        if not anos_disponiveis:
            continue

        primeiro_ano = int(anos_disponiveis[0])
        ultimo_ano = int(anos_disponiveis[-1])
        anos_esperados = list(range(primeiro_ano, ultimo_ano + 1))

        dados_continuos = anos_disponiveis == anos_esperados
        termina_no_ultimo_ano = ultimo_ano >= ultimo_ano_disponivel
        colunas_com_faltas = df_empresa[colunas_essenciais].isna().sum().sum()

        if dados_continuos and termina_no_ultimo_ano and (colunas_com_faltas / max(df_empresa.shape[0], 1) <= 0.1):
            tickers_aprovados.append(ticker)

    out = df_consolidado[df_consolidado["Ticker"].isin(tickers_aprovados)].copy()
    log(f"Filtro final aprovou {out['Ticker'].nunique() if not out.empty else 0} tickers.")
    if _RUN_LOG:
        _RUN_LOG.set_metric("tickers_filtrados", int(out["Ticker"].nunique() if not out.empty else 0))
    return out


# =========================
# GRAVAÇÃO NO SUPABASE
# =========================
def upsert_supabase_demonstracoes_financeiras(df_filtrado: pd.DataFrame) -> None:
    if df_filtrado is None or df_filtrado.empty:
        if _RUN_LOG:
            _RUN_LOG.add_warning("Nenhuma linha DFP para gravar (após filtros).")
        log("Nenhuma linha DFP para gravar (após filtros).", level="WARN")
        return

    dsn = _normalize_db_url(SUPABASE_DB_URL)

    df_db = pd.DataFrame({
        "Ticker": df_filtrado["Ticker"],
        "Data": df_filtrado["Data"],
        "Receita_Liquida": df_filtrado["Receita Líquida"],
        "EBIT": df_filtrado["Ebit"],
        "Lucro_Liquido": df_filtrado["Lucro Líquido"],
        "LPA": df_filtrado["Lucro por Ação"],
        "Ativo_Total": df_filtrado["Ativo Total"],
        "Ativo_Circulante": df_filtrado["Ativo Circulante"],
        "Passivo_Circulante": df_filtrado["Passivo Circulante"],
        "Passivo_Total": df_filtrado["Passivo Total"],
        "Divida_Total": df_filtrado["Divida Total"],
        "Patrimonio_Liquido": df_filtrado["Patrimônio Líquido"],
        "Dividendos": df_filtrado["Dividendos Totais"],
        "Caixa_Liquido": df_filtrado["Caixa Líquido"],
        "Divida_Liquida": df_filtrado["Dívida Líquida"],
    })

    df_db["Data"] = pd.to_datetime(df_db["Data"], errors="coerce").dt.date
    if validate_key_columns:
        validate_key_columns(
            df_db,
            ["Ticker", "Data"],
            context="DFP pré-upsert",
            logger=_RUN_LOG,
        )

    money_cols = [
        "Receita_Liquida", "EBIT", "Lucro_Liquido", "Ativo_Total", "Ativo_Circulante",
        "Passivo_Circulante", "Passivo_Total", "Divida_Total", "Patrimonio_Liquido",
        "Dividendos", "Caixa_Liquido", "Divida_Liquida"
    ]
    for c in money_cols:
        df_db[c] = pd.to_numeric(df_db[c], errors="coerce").round(2)

    df_db["LPA"] = _normalizar_lpa_series(df_db["LPA"])
    df_db = df_db.fillna(0)
    before_dedup = len(df_db)
    df_db = (
        df_db.sort_values(["Ticker", "Data"])
             .drop_duplicates(subset=["Ticker", "Data"], keep="last")
             .reset_index(drop=True)
    )
    duplicates_removed = before_dedup - len(df_db)
    if duplicates_removed > 0:
        log(
            f"DFP pré-upsert removeu {duplicates_removed} duplicata(s) por (Ticker, Data).",
            level="WARN",
            duplicates_removed=duplicates_removed,
            stage="pre_upsert_dedup",
        )
    if _RUN_LOG:
        _RUN_LOG.set_metric("df_rows_before_dedup", before_dedup)
        _RUN_LOG.set_metric("df_duplicates_removed", duplicates_removed)
    if validate_unique_rows:
        validate_unique_rows(
            df_db,
            ["Ticker", "Data"],
            context="DFP pré-upsert deduplicado",
            logger=_RUN_LOG,
        )

    max_lpa = float(pd.to_numeric(df_db["LPA"], errors="coerce").abs().max())
    if max_lpa >= LPA_ABS_MAX_DB:
        raise ValueError(f"LPA ainda fora do limite do banco (max abs={max_lpa}). Abortando para proteção.")

    cols = list(df_db.columns)
    values = [tuple(x) for x in df_db.itertuples(index=False, name=None)]

    sql = f'''
    INSERT INTO public."Demonstracoes_Financeiras"
    ({", ".join([f'"{c}"' for c in cols])})
    VALUES %s
    ON CONFLICT ("Ticker","Data") DO UPDATE SET
      "Receita_Liquida" = EXCLUDED."Receita_Liquida",
      "EBIT" = EXCLUDED."EBIT",
      "Lucro_Liquido" = EXCLUDED."Lucro_Liquido",
      "LPA" = EXCLUDED."LPA",
      "Ativo_Total" = EXCLUDED."Ativo_Total",
      "Ativo_Circulante" = EXCLUDED."Ativo_Circulante",
      "Passivo_Circulante" = EXCLUDED."Passivo_Circulante",
      "Passivo_Total" = EXCLUDED."Passivo_Total",
      "Divida_Total" = EXCLUDED."Divida_Total",
      "Patrimonio_Liquido" = EXCLUDED."Patrimonio_Liquido",
      "Dividendos" = EXCLUDED."Dividendos",
      "Caixa_Liquido" = EXCLUDED."Caixa_Liquido",
      "Divida_Liquida" = EXCLUDED."Divida_Liquida"
    ;
    '''

    log(f"Conectando ao banco e fazendo upsert de {len(df_db)} linhas...")
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            _assert_unique_key_ready(cur, "Demonstracoes_Financeiras", ("Ticker", "Data"))
            execute_values(cur, sql, values, page_size=BATCH_SIZE_UPSERT)
        conn.commit()

    log(f"Upsert concluído: {len(df_db)} linhas em Demonstracoes_Financeiras.", level="INFO", rows=len(df_db))
    return len(df_db)


# =========================
# MAIN
# =========================
def main():
    global _RUN_LOG
    if _IngestionLog:
        with _IngestionLog("dfp") as _log_ctx:
            _RUN_LOG = _log_ctx
            _log_ctx.set_params({"ano_inicial": ANO_INICIAL, "ultimo_ano": ULTIMO_ANO or "auto"})
            df_dict_dfp, ultimo_ano_disponivel = coletar_dfp()
            _log_ctx.set_metric("ultimo_ano_disponivel", ultimo_ano_disponivel)
            df_consolidado = montar_df_consolidado(df_dict_dfp)
            if df_consolidado.empty:
                _log_ctx.add_warning("Nenhum dado consolidado retornado pela DFP.")
                log("Nenhum dado consolidado retornado pela DFP.", level="WARN")
                return

            validate_required_columns(
                df_consolidado,
                ["CD_CVM", "Data", "Receita Líquida", "Ativo Total", "Patrimônio Líquido"],
                context="DFP consolidado final",
                logger=_log_ctx,
            )
            _log_ctx.set_metric("df_consolidado_linhas", len(df_consolidado))
            _log_ctx.set_metric("df_consolidado_tickers", int(df_consolidado["CD_CVM"].nunique()))

            df_consolidado = adicionar_ticker(df_consolidado)
            validate_required_columns(
                df_consolidado,
                ["Ticker", "Data", "Receita Líquida", "Ativo Total", "Patrimônio Líquido"],
                context="DFP com ticker",
                logger=_log_ctx,
            )
            df_filtrado = filtrar_empresas(df_consolidado, ultimo_ano_disponivel=ultimo_ano_disponivel)
            _log_ctx.set_metric("df_filtrado_linhas", len(df_filtrado))
            _log_ctx.set_metric("df_filtrado_tickers", int(df_filtrado["Ticker"].nunique() if not df_filtrado.empty else 0))
            rows = upsert_supabase_demonstracoes_financeiras(df_filtrado)
            if rows:
                _log_ctx.add_rows(inserted=rows)
    else:
        df_dict_dfp, ultimo_ano_disponivel = coletar_dfp()
        df_consolidado = montar_df_consolidado(df_dict_dfp)
        if df_consolidado.empty:
            log("Nenhum dado consolidado retornado pela DFP.", level="WARN")
            return
        df_consolidado = adicionar_ticker(df_consolidado)
        df_filtrado = filtrar_empresas(df_consolidado, ultimo_ano_disponivel=ultimo_ano_disponivel)
        upsert_supabase_demonstracoes_financeiras(df_filtrado)
    _RUN_LOG = None


if __name__ == "__main__":
    main()
