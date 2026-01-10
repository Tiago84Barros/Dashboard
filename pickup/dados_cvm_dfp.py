# pickup/dados_cvm_dfp.py
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

from .cvm_quality import normalize_vl_conta, apply_balance_dq


# =========================
# CONFIG
# =========================
URL_BASE = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"

ANO_INICIAL = int(os.getenv("ANO_INICIAL", "2010"))
ULTIMO_ANO = int(os.getenv("ULTIMO_ANO", "0"))  # 0 = modo automático
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))

# Data Quality
DQ_TOL_PCT = float(os.getenv("DQ_TOL_PCT", "0.02"))
DQ_ACCEPT_WARNING = os.getenv("DQ_ACCEPT_WARNING", "0").strip() in ("1", "true", "True", "YES", "yes")

# Supabase Postgres connection string
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "").strip()

# Mapa CVM -> Ticker
TICKER_PATH = Path(__file__).resolve().parent / "cvm_to_ticker.csv"


# =========================
# Helpers: Descobrir último ano disponível
# =========================
def _ultimo_ano_disponivel(url_base: str, prefix: str, ano_max: int | None = None, max_back: int = 8) -> int:
    if ano_max is None:
        ano_max = datetime.now().year

    for ano in range(ano_max, ano_max - max_back - 1, -1):
        url = f"{url_base}{prefix}_{ano}.zip"
        try:
            r = requests.head(url, timeout=20, allow_redirects=True)
            if r.status_code == 200:
                return ano
        except requests.RequestException:
            pass

    return ano_max - max_back


if ULTIMO_ANO <= 0:
    ULTIMO_ANO = _ultimo_ano_disponivel(URL_BASE, "dfp_cia_aberta", ano_max=datetime.now().year, max_back=12)


# =========================
# Download / Leitura CVM
# =========================
def _baixar_zip_dfp(ano: int) -> bytes:
    url = f"{URL_BASE}dfp_cia_aberta_{ano}.zip"
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return r.content


def _ler_csv_do_zip(zbytes: bytes, nome_csv: str) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        with zf.open(nome_csv) as csvfile:
            df = pd.read_csv(csvfile, sep=";", decimal=",", encoding="ISO-8859-1")

    # Normalização determinística de escala (CVM)
    df = normalize_vl_conta(df)
    if "VL_CONTA_NORM" in df.columns:
        df["VL_CONTA"] = df["VL_CONTA_NORM"]

    if "DT_REFER" in df.columns:
        df["DT_REFER"] = pd.to_datetime(df["DT_REFER"], errors="coerce")

    return df


def coletar_dfp() -> dict:
    anos = list(range(ANO_INICIAL, ULTIMO_ANO + 1))

    csvs = {
        "DRE": lambda a: f"dfp_cia_aberta_DRE_con_{a}.csv",
        "BPA": lambda a: f"dfp_cia_aberta_BPA_con_{a}.csv",
        "BPP": lambda a: f"dfp_cia_aberta_BPP_con_{a}.csv",
        "DFC": lambda a: f"dfp_cia_aberta_DFC_MD_con_{a}.csv",
        "DVA": lambda a: f"dfp_cia_aberta_DVA_con_{a}.csv",
    }

    bucket = {k: [] for k in csvs.keys()}

    def _job(ano: int):
        zbytes = _baixar_zip_dfp(ano)
        out = {}
        for k, fn in csvs.items():
            try:
                out[k] = _ler_csv_do_zip(zbytes, fn(ano))
            except KeyError:
                out[k] = pd.DataFrame()
        return out

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for out in ex.map(_job, anos):
            for k in bucket.keys():
                if out[k] is not None and not out[k].empty:
                    bucket[k].append(out[k])

    return {k: (pd.concat(v, ignore_index=True) if v else pd.DataFrame()) for k, v in bucket.items()}


# =========================
# Consolidação
# =========================
def montar_df_consolidado(df_dict_dfp: dict) -> pd.DataFrame:
    if df_dict_dfp.get("DRE") is None or df_dict_dfp["DRE"].empty:
        return pd.DataFrame()

    dre_all = df_dict_dfp["DRE"].copy()
    if "DT_REFER" in dre_all.columns:
        dre_all["DT_REFER"] = pd.to_datetime(dre_all["DT_REFER"], errors="coerce")

    # 1 nome por CD_CVM (último observado)
    empresas = (
        dre_all[["CD_CVM", "DENOM_CIA", "DT_REFER"]]
        .dropna(subset=["CD_CVM"])
        .sort_values("DT_REFER", kind="mergesort")
        .groupby("CD_CVM", as_index=True)["DENOM_CIA"]
        .last()
        .to_frame()
    )

    def _serie_conta(df_conta: pd.DataFrame, idx: pd.DatetimeIndex) -> pd.Series:
        if df_conta is None or df_conta.empty:
            return pd.Series(index=idx, dtype="float64")

        dfc = df_conta[["DT_REFER", "VL_CONTA"]].copy()
        dfc["DT_REFER"] = pd.to_datetime(dfc["DT_REFER"], errors="coerce")
        dfc["VL_CONTA"] = pd.to_numeric(dfc["VL_CONTA"], errors="coerce")
        s = dfc.groupby("DT_REFER", dropna=True)["VL_CONTA"].sum()
        return s.reindex(idx)

    df_consolidado = []

    for CD_CVM in empresas.index:
        empresa_dre = dre_all[dre_all["CD_CVM"] == CD_CVM]
        empresa_bpa = df_dict_dfp["BPA"][df_dict_dfp["BPA"]["CD_CVM"] == CD_CVM] if df_dict_dfp.get("BPA") is not None else pd.DataFrame()
        empresa_bpp = df_dict_dfp["BPP"][df_dict_dfp["BPP"]["CD_CVM"] == CD_CVM] if df_dict_dfp.get("BPP") is not None else pd.DataFrame()

        # índice base: receita (3.01) -> ativo total (1) -> DT_REFER do DRE
        conta_receita = empresa_dre[empresa_dre["CD_CONTA"] == "3.01"]
        if not conta_receita.empty:
            idx = pd.DatetimeIndex(pd.to_datetime(conta_receita["DT_REFER"].unique(), errors="coerce")).dropna().unique().sort_values()
        else:
            bpa_ativo_total = empresa_bpa[empresa_bpa["CD_CONTA"] == "1"] if empresa_bpa is not None else pd.DataFrame()
            if bpa_ativo_total is not None and not bpa_ativo_total.empty:
                idx = pd.DatetimeIndex(pd.to_datetime(bpa_ativo_total["DT_REFER"].unique(), errors="coerce")).dropna().unique().sort_values()
            else:
                idx = pd.DatetimeIndex(pd.to_datetime(empresa_dre["DT_REFER"].unique(), errors="coerce")).dropna().unique().sort_values()

        if len(idx) == 0:
            continue

        # DRE
        receita = _serie_conta(empresa_dre[empresa_dre["CD_CONTA"] == "3.01"], idx)
        ebit = _serie_conta(empresa_dre[empresa_dre["CD_CONTA"] == "3.05"], idx)

        lucro = _serie_conta(empresa_dre[empresa_dre["CD_CONTA"] == "3.11"], idx)
        if lucro.isna().all() and "DS_CONTA" in empresa_dre.columns:
            sel = empresa_dre[empresa_dre["DS_CONTA"].astype(str).str.contains(r"Lucro|Preju[ií]zo", case=False, na=False)]
            lucro = _serie_conta(sel, idx)

        lpa = _serie_conta(empresa_dre[empresa_dre["CD_CONTA"] == "3.99.01.01"], idx)

        # BPA
        ativo_total = _serie_conta(empresa_bpa[empresa_bpa["CD_CONTA"] == "1"], idx)
        ativo_circ = _serie_conta(empresa_bpa[empresa_bpa["CD_CONTA"] == "1.01"], idx)
        caixa = _serie_conta(empresa_bpa[empresa_bpa["CD_CONTA"] == "1.01.01"], idx)

        # BPP
        passivo_total = _serie_conta(empresa_bpp[empresa_bpp["CD_CONTA"] == "2"], idx)
        passivo_circ = _serie_conta(empresa_bpp[empresa_bpp["CD_CONTA"] == "2.01"], idx)
        pl = _serie_conta(empresa_bpp[empresa_bpp["CD_CONTA"] == "2.03"], idx)

        # Dívida (fallback por texto)
        div_total = pd.Series(index=idx, dtype="float64")
        if empresa_bpp is not None and not empresa_bpp.empty and "DS_CONTA" in empresa_bpp.columns:
            sel = empresa_bpp[empresa_bpp["DS_CONTA"].astype(str).str.contains("emprést|financi", case=False, na=False)]
            div_total = _serie_conta(sel, idx)

        caixa_liq = caixa
        div_liq = div_total - caixa_liq

        nome = empresas.at[CD_CVM, "DENOM_CIA"]

        df_empresa = pd.DataFrame({
            "CD_CVM": CD_CVM,
            "Nome": nome,
            "Data": idx,
            "Receita Líquida": receita.values,
            "Ebit": ebit.values,
            "Lucro Líquido": lucro.values,
            "Lucro por Ação": lpa.values,
            "Ativo Total": ativo_total.values,
            "Ativo Circulante": ativo_circ.values,
            "Passivo Circulante": passivo_circ.values,
            "Passivo Total": passivo_total.values,
            "Divida Total": div_total.values,
            "Patrimônio Líquido": pl.values,
            "Dividendos Totais": np.nan,
            "Caixa Líquido": caixa_liq.values,
            "Dívida Líquida": div_liq.values,
        })

        df_consolidado.append(df_empresa)

    return pd.concat(df_consolidado, ignore_index=True) if df_consolidado else pd.DataFrame()


# =========================
# Mapeamento robusto CVM -> Ticker
# =========================
def _read_cvm_to_ticker_csv(path: Path) -> pd.DataFrame:
    """
    Lê cvm_to_ticker.csv de forma robusta (separador e cabeçalhos variáveis).
    Retorna DataFrame com colunas normalizadas: CD_CVM (int/float coerente) e Ticker (str).
    """
    # tenta separadores comuns
    last_err = None
    for sep in [",", ";", "\t", "|"]:
        try:
            df = pd.read_csv(path, sep=sep, encoding="utf-8-sig")
            if df.shape[1] >= 2:
                break
        except Exception as e:
            last_err = e
            df = None
    if df is None:
        raise RuntimeError(f"Falha ao ler {path}. Erro: {last_err}")

    # normaliza nomes
    cols = {c: str(c).strip().replace("\ufeff", "") for c in df.columns}
    df = df.rename(columns=cols)

    # candidatos a CD_CVM
    cvm_candidates = [
        "CD_CVM", "cd_cvm", "CVM", "cvm", "Codigo_CVM", "codigo_cvm", "COD_CVM", "cod_cvm"
    ]
    # candidatos a Ticker
    ticker_candidates = [
        "Ticker", "ticker", "TICKER", "codigo", "CODIGO", "Codigo", "codigo_negociacao", "CODIGO_NEGOCIACAO"
    ]

    def pick_col(cands):
        for c in cands:
            if c in df.columns:
                return c
        # tentativa por contains
        low = {c: c.lower() for c in df.columns}
        for c in df.columns:
            if "cvm" in low[c]:
                return c if cands is cvm_candidates else None
        return None

    cvm_col = pick_col(cvm_candidates)
    tick_col = None
    for c in ticker_candidates:
        if c in df.columns:
            tick_col = c
            break

    if cvm_col is None:
        raise KeyError(f"cvm_to_ticker.csv sem coluna de CVM reconhecível. Colunas: {list(df.columns)}")
    if tick_col is None:
        # tenta achar alguma coluna com "tick" ou "ticker"
        for c in df.columns:
            cl = c.lower()
            if "tick" in cl:
                tick_col = c
                break
    if tick_col is None:
        raise KeyError(f"cvm_to_ticker.csv sem coluna de Ticker reconhecível. Colunas: {list(df.columns)}")

    out = df[[cvm_col, tick_col]].copy()
    out = out.rename(columns={cvm_col: "CD_CVM", tick_col: "Ticker"})

    out["CD_CVM"] = pd.to_numeric(out["CD_CVM"], errors="coerce")
    out["Ticker"] = out["Ticker"].astype(str).str.strip().replace({"": np.nan, "nan": np.nan, "None": np.nan})

    out = out.dropna(subset=["CD_CVM", "Ticker"]).drop_duplicates(subset=["CD_CVM"], keep="last")
    return out


def adicionar_ticker(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    mapa = _read_cvm_to_ticker_csv(TICKER_PATH)

    out = df.copy()
    out["CD_CVM"] = pd.to_numeric(out["CD_CVM"], errors="coerce")
    out = out.merge(mapa[["CD_CVM", "Ticker"]], on="CD_CVM", how="left")
    return out


def filtrar_empresas(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    out = out[out["Ticker"].notna()].reset_index(drop=True)
    return out


# =========================
# DQ gate + Auditoria
# =========================
def aplicar_dq_e_filtrar(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df is None or df.empty:
        return df, pd.DataFrame()

    tmp = df.copy()
    tmp["Data"] = pd.to_datetime(tmp["Data"], errors="coerce")

    bal = pd.DataFrame({
        "Ativo_Total": pd.to_numeric(tmp.get("Ativo Total"), errors="coerce"),
        "Ativo_Circulante": pd.to_numeric(tmp.get("Ativo Circulante"), errors="coerce"),
        "Passivo_Total": pd.to_numeric(tmp.get("Passivo Total"), errors="coerce"),
        "Passivo_Circulante": pd.to_numeric(tmp.get("Passivo Circulante"), errors="coerce"),
        "Patrimonio_Liquido": pd.to_numeric(tmp.get("Patrimônio Líquido"), errors="coerce"),
    }, index=tmp["Data"])
    bal.index.name = "DT_REFER"

    bal_dq = apply_balance_dq(bal, tol_pct=DQ_TOL_PCT)

    df_dq = pd.DataFrame({
        "Ticker": tmp["Ticker"].astype(str),
        "Data": tmp["Data"].dt.date,
        "dq_status": bal_dq["dq_status"].values,
        "dq_flags": bal_dq["dq_flags"].values,
        "dq_balance_diff_pct": bal_dq["dq_balance_diff_pct"].values,
    })

    if DQ_ACCEPT_WARNING:
        ok_mask = df_dq["dq_status"].isin(["OK", "WARNING"])
    else:
        ok_mask = df_dq["dq_status"].eq("OK")

    df_ok = df.loc[ok_mask.values].copy().reset_index(drop=True)
    return df_ok, df_dq


def upsert_supabase_dq(df_dq: pd.DataFrame) -> None:
    if df_dq is None or df_dq.empty:
        return
    if not SUPABASE_DB_URL:
        raise RuntimeError("Defina SUPABASE_DB_URL (connection string Postgres do Supabase).")

    ddl = """
    CREATE TABLE IF NOT EXISTS public.dq_demonstracoes_financeiras (
      "Ticker" text NOT NULL,
      "Data" date NOT NULL,
      dq_status text NOT NULL,
      dq_flags jsonb NULL,
      dq_balance_diff_pct double precision NULL,
      updated_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY ("Ticker","Data")
    );
    """

    df2 = df_dq.copy()
    df2["Data"] = pd.to_datetime(df2["Data"], errors="coerce").dt.date
    df2 = (
        df2.sort_values(["Ticker", "Data"])
           .drop_duplicates(subset=["Ticker", "Data"], keep="last")
           .reset_index(drop=True)
    )

    cols = ["Ticker", "Data", "dq_status", "dq_flags", "dq_balance_diff_pct"]
    values = [tuple(x) for x in df2[cols].itertuples(index=False, name=None)]

    sql = """
    INSERT INTO public.dq_demonstracoes_financeiras
    ("Ticker","Data","dq_status","dq_flags","dq_balance_diff_pct")
    VALUES %s
    ON CONFLICT ("Ticker","Data") DO UPDATE SET
      dq_status = EXCLUDED.dq_status,
      dq_flags = EXCLUDED.dq_flags,
      dq_balance_diff_pct = EXCLUDED.dq_balance_diff_pct,
      updated_at = now();
    """

    with psycopg2.connect(SUPABASE_DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
            execute_values(cur, sql, values, page_size=5000)
        conn.commit()


def upsert_supabase_demonstracoes_financeiras(df_filtrado: pd.DataFrame) -> None:
    if df_filtrado is None or df_filtrado.empty:
        print("[WARN] Nenhuma linha DFP para gravar (após filtros/DQ).")
        return
    if not SUPABASE_DB_URL:
        raise RuntimeError("Defina SUPABASE_DB_URL (connection string Postgres do Supabase).")

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
    df_db = (
        df_db.sort_values(["Ticker", "Data"])
             .drop_duplicates(subset=["Ticker", "Data"], keep="last")
             .reset_index(drop=True)
    )

    cols = list(df_db.columns)
    values = [tuple(x) for x in df_db.itertuples(index=False, name=None)]

    sql = f"""
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
    """

    with psycopg2.connect(SUPABASE_DB_URL) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=5000)
        conn.commit()

    print(f"[OK] Gravado no Supabase (DFP): {len(df_db)} linhas (até {ULTIMO_ANO})")


def main():
    df_dict_dfp = coletar_dfp()
    df_consolidado = montar_df_consolidado(df_dict_dfp)
    df_consolidado = adicionar_ticker(df_consolidado)
    df_filtrado = filtrar_empresas(df_consolidado)

    # DQ gate + auditoria
    df_ok, df_dq = aplicar_dq_e_filtrar(df_filtrado)
    upsert_supabase_dq(df_dq)

    # grava somente OK (ou OK+WARNING conforme env)
    upsert_supabase_demonstracoes_financeiras(df_ok)
