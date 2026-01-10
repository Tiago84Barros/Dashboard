import io
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values

from .cvm_quality import normalize_vl_conta, apply_balance_dq

pd.set_option("future.no_silent_downcasting", True)

# ======================
# CONFIG
# ======================
URL_BASE = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"

ANO_INICIAL = int(os.getenv("ANO_INICIAL", "2010"))
ULTIMO_ANO = int(os.getenv("ULTIMO_ANO", "2025"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))

# DQ
DQ_TOL_PCT = float(os.getenv("DQ_TOL_PCT", "0.02"))
DQ_ACCEPT_WARNING = os.getenv("DQ_ACCEPT_WARNING", "0").strip() in ("1", "true", "True", "YES", "yes")

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "").strip()

# ======================
# Util
# ======================
def _baixar_zip_itr(ano: int) -> bytes:
    url = f"{URL_BASE}itr_cia_aberta_{ano}.zip"
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return r.content


def _ler_csv_do_zip(zbytes: bytes, nome_csv: str) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        with zf.open(nome_csv) as csvfile:
            df = pd.read_csv(csvfile, sep=";", decimal=",", encoding="ISO-8859-1")
    df = normalize_vl_conta(df)
    if "VL_CONTA_NORM" in df.columns:
        df["VL_CONTA"] = df["VL_CONTA_NORM"]
    return df


def coletar_itr() -> dict:
    anos = list(range(ANO_INICIAL, ULTIMO_ANO + 1))

    csvs = {
        "DRE": lambda a: f"itr_cia_aberta_DRE_con_{a}.csv",
        "BPA": lambda a: f"itr_cia_aberta_BPA_con_{a}.csv",
        "BPP": lambda a: f"itr_cia_aberta_BPP_con_{a}.csv",
        "DFC": lambda a: f"itr_cia_aberta_DFC_MD_con_{a}.csv",
    }

    bucket = {k: [] for k in csvs.keys()}

    def _job(ano: int):
        zbytes = _baixar_zip_itr(ano)
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


def montar_df_consolidado(df_dict_itr: dict) -> pd.DataFrame:
    empresas = (
        df_dict_itr["BPA"][["DENOM_CIA", "CD_CVM"]]
        .drop_duplicates()
        .set_index("CD_CVM")
    )

    def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        df = df.copy()
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

    df_out = []

    for CD_CVM in empresas.index:
        empresa_dre = _to_dt(df_dict_itr["DRE"][df_dict_itr["DRE"]["CD_CVM"] == CD_CVM])
        empresa_bpa = _to_dt(df_dict_itr["BPA"][df_dict_itr["BPA"]["CD_CVM"] == CD_CVM])
        empresa_bpp = _to_dt(df_dict_itr["BPP"][df_dict_itr["BPP"]["CD_CVM"] == CD_CVM])

        idx = pd.DatetimeIndex(pd.to_datetime(empresa_bpa["DT_REFER"].unique(), errors="coerce")).dropna().unique().sort_values()
        if len(idx) == 0:
            continue

        # DRE (trimestral)
        receita = _serie_conta(empresa_dre[empresa_dre["CD_CONTA"] == "3.01"], idx)
        ebit = _serie_conta(empresa_dre[empresa_dre["CD_CONTA"] == "3.05"], idx)
        lucro = _serie_conta(empresa_dre[empresa_dre["CD_CONTA"] == "3.11"], idx)
        lpa = _serie_conta(empresa_dre[empresa_dre["CD_CONTA"] == "3.99.01.01"], idx)

        # BPA/BPP
        ativo_total = _serie_conta(empresa_bpa[empresa_bpa["CD_CONTA"] == "1"], idx)
        ativo_circ = _serie_conta(empresa_bpa[empresa_bpa["CD_CONTA"] == "1.01"], idx)
        caixa = _serie_conta(empresa_bpa[empresa_bpa["CD_CONTA"] == "1.01.01"], idx)

        passivo_total = _serie_conta(empresa_bpp[empresa_bpp["CD_CONTA"] == "2"], idx)
        passivo_circ = _serie_conta(empresa_bpp[empresa_bpp["CD_CONTA"] == "2.01"], idx)
        pl = _serie_conta(empresa_bpp[empresa_bpp["CD_CONTA"] == "2.03"], idx)

        # Dívida (mantém fallback por DS_CONTA, mas sem inventar zero)
        div_total = pd.Series(index=idx, dtype="float64")
        if empresa_bpp is not None and not empresa_bpp.empty and "DS_CONTA" in empresa_bpp.columns:
            sel = empresa_bpp[empresa_bpp["DS_CONTA"].astype(str).str.contains("emprést|financi", case=False, na=False)]
            div_total = _serie_conta(sel, idx)

        caixa_liq = caixa
        div_liq = div_total - caixa_liq

        df_empresa = pd.DataFrame({
            "CD_CVM": CD_CVM,
            "Nome": empresas.loc[CD_CVM, "DENOM_CIA"],
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
            "Dividendos Totais": pd.NA,
            "Caixa Líquido": caixa_liq.values,
            "Dívida Líquida": div_liq.values,
        })

        df_out.append(df_empresa)

    return pd.concat(df_out, ignore_index=True) if df_out else pd.DataFrame()


def adicionar_ticker(df: pd.DataFrame) -> pd.DataFrame:
    # No seu projeto, normalmente você já junta via setores/setores.
    # Mantive o padrão de depender de mapeamento CVM->Ticker se existir em outro lugar.
    # Se você já tem função central no core, recomendo substituir aqui.
    return df


def filtrar_empresas(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    out = out[out["Ticker"].notna()].reset_index(drop=True) if "Ticker" in out.columns else out
    return out


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
        "Ticker": tmp.get("Ticker", pd.Series([""] * len(tmp))).astype(str),
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
    CREATE TABLE IF NOT EXISTS public.dq_demonstracoes_financeiras_tri (
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
    INSERT INTO public.dq_demonstracoes_financeiras_tri
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


def upsert_supabase(df: pd.DataFrame) -> None:
    if not SUPABASE_DB_URL:
        raise RuntimeError("Defina SUPABASE_DB_URL com a connection string Postgres do Supabase.")

    df_db = pd.DataFrame({
        "Ticker": df["Ticker"],
        "Data": df["Data"],
        "Receita_Liquida": df["Receita Líquida"],
        "EBIT": df["Ebit"],
        "Lucro_Liquido": df["Lucro Líquido"],
        "LPA": df["Lucro por Ação"],
        "Ativo_Total": df["Ativo Total"],
        "Ativo_Circulante": df["Ativo Circulante"],
        "Passivo_Circulante": df["Passivo Circulante"],
        "Passivo_Total": df["Passivo Total"],
        "Divida_Total": df["Divida Total"],
        "Patrimonio_Liquido": df["Patrimônio Líquido"],
        "Dividendos": df["Dividendos Totais"],
        "Caixa_Liquido": df["Caixa Líquido"],
        "Divida_Liquida": df["Dívida Líquida"],
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
    INSERT INTO public."Demonstracoes_Financeiras_TRI"
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

    print(f"[OK] Gravado no Supabase (ITR): {len(df_db)} linhas")


def main():
    df_dict_itr = coletar_itr()
    df = montar_df_consolidado(df_dict_itr)
    df = filtrar_empresas(df)

    df, df_dq = aplicar_dq_e_filtrar(df)
    upsert_supabase_dq(df_dq)
    upsert_supabase(df)
