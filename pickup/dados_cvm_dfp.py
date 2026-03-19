import io
import os
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urlunparse

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

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL_PG", os.getenv("SUPABASE_DB_URL", "")).strip()

BASE_DIR = Path(__file__).resolve().parent
TICKER_PATH = Path(os.getenv("TICKER_PATH", str(BASE_DIR / "cvm_to_ticker.csv")))
CACHE_DIR = Path(os.getenv("CVM_CACHE_DIR", str(BASE_DIR / ".cache_cvm_dfp")))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

LPA_ABS_MAX_DB = 1e14 - 1


# =========================
# LOG
# =========================
def log(msg: str) -> None:
    print(f"{LOG_PREFIX} {msg}")


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
        raise RuntimeError("Defina SUPABASE_DB_URL (connection string Postgres do Supabase).")

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
    try:
        raw_zip = _baixar_zip_ano(session, ano)
    except Exception as e:
        log(f"[WARN] Erro ao baixar o arquivo para o ano {ano}: {e}")
        return None

    try:
        with zipfile.ZipFile(io.BytesIO(raw_zip)) as zip_ref:
            df_temp_dict = {"DRE": [], "BPA": [], "BPP": [], "DFC_MI": []}

            for arquivo in zip_ref.namelist():
                if arquivo.endswith(".csv") and "_con_" in arquivo:
                    with zip_ref.open(arquivo) as csvfile:
                        try:
                            df_temp = pd.read_csv(csvfile, sep=";", decimal=",", encoding="ISO-8859-1")

                            if "ORDEM_EXERC" in df_temp.columns:
                                df_temp = df_temp[df_temp["ORDEM_EXERC"] == "ÚLTIMO"]

                            df_temp = _normalizar_vl_conta_por_escala(df_temp)

                            up = arquivo.upper()
                            if "DRE" in up:
                                df_temp_dict["DRE"].append(df_temp)
                            elif "BPA" in up:
                                df_temp_dict["BPA"].append(df_temp)
                            elif "BPP" in up:
                                df_temp_dict["BPP"].append(df_temp)
                            elif "DFC" in up:
                                df_temp_dict["DFC_MI"].append(df_temp)

                        except Exception as e:
                            log(f"[WARN] Erro ao processar {arquivo} no ano {ano}: {e}")
            return df_temp_dict
    except zipfile.BadZipFile:
        log(f"[WARN] ZIP inválido para o ano {ano}.")
        return None



def coletar_dfp() -> tuple[dict, int]:
    session = build_session()

    ultimo_ano = ULTIMO_ANO if ULTIMO_ANO > 0 else _ultimo_ano_disponivel(session, ano_max=datetime.now().year, max_back=12)
    anos = list(range(ANO_INICIAL, ultimo_ano + 1))

    df_dict_dfp = {"DRE": pd.DataFrame(), "BPA": pd.DataFrame(), "BPP": pd.DataFrame(), "DFC_MI": pd.DataFrame()}

    if not anos:
        log("[WARN] Intervalo de anos vazio. Verifique ANO_INICIAL/ULTIMO_ANO.")
        return df_dict_dfp, ultimo_ano

    log(f"Coletando DFP do intervalo {ANO_INICIAL}..{ultimo_ano}")
    start = time.time()

    futures = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for ano in anos:
            futures[executor.submit(processar_ano_dfp, session, ano)] = ano

        for future in as_completed(futures):
            ano = futures[future]
            try:
                df_temp_dict = future.result()
                if df_temp_dict is None:
                    continue
                for key in df_dict_dfp.keys():
                    if df_temp_dict.get(key):
                        df_dict_dfp[key] = pd.concat([df_dict_dfp[key]] + df_temp_dict[key], ignore_index=True)
                log(f"Ano {ano} processado com sucesso.")
            except Exception as e:
                log(f"[WARN] Falha no ano {ano}: {e}")

    elapsed = round(time.time() - start, 1)
    log(f"[OK] Coleta de dados anuais (DFP) concluída em {elapsed}s. (anos {ANO_INICIAL}..{ultimo_ano})")
    return df_dict_dfp, ultimo_ano


# =========================
# CONSOLIDAÇÃO
# =========================
def montar_df_consolidado(df_dict_dfp: dict) -> pd.DataFrame:
    if df_dict_dfp["DRE"] is None or df_dict_dfp["DRE"].empty:
        return pd.DataFrame()

    empresas = (
        df_dict_dfp["DRE"][["DENOM_CIA", "CD_CVM"]]
        .drop_duplicates()
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
    df = pd.merge(df_consolidado, cvm_to_ticker, left_on="CD_CVM", right_on="CVM")
    df = df.drop(columns=["CD_CVM", "CVM"])
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.strftime("%Y-%m-%d")

    colunas = [
        "Ticker", "Data", "Receita Líquida", "Ebit", "Lucro Líquido", "Lucro por Ação",
        "Ativo Total", "Ativo Circulante", "Passivo Circulante", "Passivo Total", "Divida Total",
        "Patrimônio Líquido", "Dividendos Totais", "Caixa Líquido", "Dívida Líquida"
    ]
    return df[colunas]


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
    return out


# =========================
# GRAVAÇÃO NO SUPABASE
# =========================
def upsert_supabase_demonstracoes_financeiras(df_filtrado: pd.DataFrame) -> None:
    if df_filtrado is None or df_filtrado.empty:
        log("[WARN] Nenhuma linha DFP para gravar (após filtros).")
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

    money_cols = [
        "Receita_Liquida", "EBIT", "Lucro_Liquido", "Ativo_Total", "Ativo_Circulante",
        "Passivo_Circulante", "Passivo_Total", "Divida_Total", "Patrimonio_Liquido",
        "Dividendos", "Caixa_Liquido", "Divida_Liquida"
    ]
    for c in money_cols:
        df_db[c] = pd.to_numeric(df_db[c], errors="coerce").round(2)

    df_db["LPA"] = _normalizar_lpa_series(df_db["LPA"])
    df_db = df_db.fillna(0)
    df_db = (
        df_db.sort_values(["Ticker", "Data"])
             .drop_duplicates(subset=["Ticker", "Data"], keep="last")
             .reset_index(drop=True)
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
            execute_values(cur, sql, values, page_size=BATCH_SIZE_UPSERT)
        conn.commit()

    log(f"[OK] Upsert concluído: {len(df_db)} linhas em Demonstracoes_Financeiras.")


# =========================
# MAIN
# =========================
def main():
    df_dict_dfp, ultimo_ano_disponivel = coletar_dfp()
    df_consolidado = montar_df_consolidado(df_dict_dfp)
    if df_consolidado.empty:
        log("[WARN] Nenhum dado consolidado retornado pela DFP.")
        return

    df_consolidado = adicionar_ticker(df_consolidado)
    df_filtrado = filtrar_empresas(df_consolidado, ultimo_ano_disponivel=ultimo_ano_disponivel)
    upsert_supabase_demonstracoes_financeiras(df_filtrado)


if __name__ == "__main__":
    main()
