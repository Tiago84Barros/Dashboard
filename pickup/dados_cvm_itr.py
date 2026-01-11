# pickup/dados_cvm_itr.py
import io
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values


# =========================
# CONFIG
# =========================
URL_BASE_ITR = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
ANO_INICIAL = int(os.getenv("ANO_INICIAL", "2010"))

ULTIMO_ANO = int(os.getenv("ULTIMO_ANO", "0"))  # 0 = automático

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "").strip()

BASE_DIR = Path(__file__).resolve().parent
TICKER_PATH = BASE_DIR / "cvm_to_ticker.csv"

LPA_ABS_MAX_DB = 1e14 - 1


# =========================
# UTIL — ÚLTIMO ANO DISPONÍVEL
# =========================
def _ultimo_ano_disponivel(prefix: str, ano_max: int | None = None, max_back: int = 12) -> int:
    if ano_max is None:
        ano_max = datetime.now().year

    for ano in range(ano_max, ano_max - max_back - 1, -1):
        url = URL_BASE_ITR + f"{prefix}_{ano}.zip"
        try:
            r = requests.head(url, timeout=20, allow_redirects=True)
            if r.status_code == 200:
                return ano
        except requests.RequestException:
            pass

    return ano_max - max_back


if ULTIMO_ANO <= 0:
    ULTIMO_ANO = _ultimo_ano_disponivel("itr_cia_aberta", datetime.now().year)


# =========================
# NORMALIZAÇÃO DE ESCALA
# =========================
def normalizar_escala(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df = df.copy()
    df["VL_CONTA"] = pd.to_numeric(df["VL_CONTA"], errors="coerce")

    escala = df["ESCALA_MOEDA"].astype(str).str.upper()
    fator = pd.Series(1.0, index=df.index)

    fator.loc[escala.isin(["MIL"])] = 1_000
    fator.loc[escala.isin(["MILHAO", "MILHÃO"])] = 1_000_000
    fator.loc[escala.isin(["BILHAO", "BILHÃO"])] = 1_000_000_000

    # ❌ nunca aplicar escala em contas por ação
    mask_lpa = df["CD_CONTA"].astype(str).str.startswith("3.99", na=False)
    fator.loc[mask_lpa] = 1.0

    df["VL_CONTA"] = df["VL_CONTA"] * fator
    return df


def normalizar_lpa(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")

    for _ in range(8):
        mask = s.abs() > 1e6
        if not mask.any():
            break
        s.loc[mask] /= 1000

    s.loc[s.abs() >= LPA_ABS_MAX_DB] = np.nan
    return s.fillna(0).round(6)


# =========================
# COLETA ITR
# =========================
def processar_ano_itr(ano: int):
    url = URL_BASE_ITR + f"itr_cia_aberta_{ano}.zip"
    r = requests.get(url, timeout=180)
    if r.status_code != 200:
        return None

    with zipfile.ZipFile(io.BytesIO(r.content)) as zipf:
        dfs = []
        for arq in zipf.namelist():
            if arq.endswith(".csv") and "_con_" in arq:
                with zipf.open(arq) as f:
                    df = pd.read_csv(f, sep=";", decimal=",", encoding="ISO-8859-1")
                    df = df[df["ORDEM_EXERC"] == "ÚLTIMO"]
                    df = normalizar_escala(df)
                    dfs.append(df)
        if dfs:
            return pd.concat(dfs, ignore_index=True)
    return None


def coletar_itr():
    anos = list(range(ANO_INICIAL, ULTIMO_ANO))
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        resultados = ex.map(processar_ano_itr, anos)

    dfs = [r for r in resultados if r is not None]
    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


# =========================
# CONSOLIDAÇÃO
# =========================
def consolidar_itr(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df["DT_REFER"] = pd.to_datetime(df["DT_REFER"], errors="coerce")

    def conta(cod):
        return df[df["CD_CONTA"] == cod]

    out = (
        conta("3.01")[["CD_CVM", "DT_REFER", "VL_CONTA"]]
        .rename(columns={"VL_CONTA": "Receita Líquida"})
    )

    out["Ebit"] = conta("3.05").set_index(["CD_CVM", "DT_REFER"])["VL_CONTA"].values
    out["Lucro Líquido"] = conta("3.11").set_index(["CD_CVM", "DT_REFER"])["VL_CONTA"].values

    lpa = conta("3.99.01.01").groupby(["CD_CVM", "DT_REFER"])["VL_CONTA"].sum()
    out["Lucro por Ação"] = normalizar_lpa(lpa.values)

    out = out.dropna(subset=["DT_REFER"])
    out = out.drop_duplicates(subset=["CD_CVM", "DT_REFER"])

    return out.reset_index(drop=True)


# =========================
# TICKER
# =========================
def adicionar_ticker(df: pd.DataFrame) -> pd.DataFrame:
    mapa = pd.read_csv(TICKER_PATH)
    df = df.merge(mapa, left_on="CD_CVM", right_on="CVM", how="inner")
    df = df.drop(columns=["CD_CVM", "CVM"])
    df["Data"] = pd.to_datetime(df["DT_REFER"]).dt.date
    return df.drop(columns=["DT_REFER"])


# =========================
# UPSERT
# =========================
def upsert_supabase_itr(df: pd.DataFrame):
    if df.empty:
        print("[WARN] Nenhuma linha ITR para gravar.")
        return

    df_db = pd.DataFrame({
        "Ticker": df["Ticker"],
        "Data": df["Data"],
        "Receita_Liquida": df["Receita Líquida"],
        "EBIT": df["Ebit"],
        "Lucro_Liquido": df["Lucro Líquido"],
        "LPA": df["Lucro por Ação"],
    }).fillna(0)

    sql = """
    INSERT INTO public."Demonstracoes_Financeiras_TRI"
    ("Ticker","Data","Receita_Liquida","EBIT","Lucro_Liquido","LPA")
    VALUES %s
    ON CONFLICT ("Ticker","Data") DO UPDATE SET
      "Receita_Liquida" = EXCLUDED."Receita_Liquida",
      "EBIT" = EXCLUDED."EBIT",
      "Lucro_Liquido" = EXCLUDED."Lucro_Liquido",
      "LPA" = EXCLUDED."LPA";
    """

    values = [tuple(x) for x in df_db.itertuples(index=False, name=None)]

    with psycopg2.connect(SUPABASE_DB_URL) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=5000)
        conn.commit()

    print(f"[OK] Upsert ITR concluído: {len(df_db)} linhas.")


# =========================
# MAIN
# =========================
def main():
    df_raw = coletar_itr()
    df_cons = consolidar_itr(df_raw)
    df_tick = adicionar_ticker(df_cons)
    upsert_supabase_itr(df_tick)


if __name__ == "__main__":
    main()
