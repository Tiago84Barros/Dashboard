# pickup/dados_macro_brasil.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
import time
import requests

import pandas as pd
from bcb import sgs
from sqlalchemy import create_engine, text




# ----------------------------
# Config
# ----------------------------
@dataclass(frozen=True)
class MacroConfig:
    start_date: date
    end_date: date
    max_years_chunk: int
    icc_mode: str  # "final" or "mean"
    write_monthly: bool
    schema: str = "public"
    table_annual: str = "info_economica"
    table_monthly: str = "info_economica_mensal"


def _env_date(name: str, default: str) -> date:
    v = os.getenv(name, default).strip()
    return datetime.strptime(v, "%Y-%m-%d").date()


def load_config() -> MacroConfig:
    start = _env_date("MACRO_START_DATE", "2010-01-01")
    end = datetime.today().date() - timedelta(days=2)
    max_years = int(os.getenv("MACRO_MAX_YEARS_CHUNK", "10"))
    icc_mode = os.getenv("ICC_MODE", "final").strip().lower()
    if icc_mode not in ("final", "mean"):
        icc_mode = "final"
    write_monthly = os.getenv("MACRO_WRITE_MONTHLY", "0").strip() in ("1", "true", "True", "yes", "YES")

    return MacroConfig(
        start_date=start,
        end_date=end,
        max_years_chunk=max_years,
        icc_mode=icc_mode,
        write_monthly=write_monthly,
    )


# ----------------------------
# Series map (BCB/SGS)
# ----------------------------
# Códigos vindos do seu Algoritmo_3 original:
# 432 selic; 433 ipca; 1 cambio; 22707 balanca_comercial; 14 icc; 4380 pib; 4502 divida_publica
SERIES: Dict[str, int] = {
    "selic": 432,
    "ipca": 433,
    "cambio": 1,
    "balanca_comercial": 22707,
    "icc": 4393,
    "pib": 4380,
    "divida_publica": 4502,
}


# ----------------------------
# Fetch helpers
# ----------------------------
def _sgs_url(code: int, start: date, end: date) -> str:
    # API oficial SGS (JSON)
    di = start.strftime("%d/%m/%Y")
    df = end.strftime("%d/%m/%Y")
    return f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados?formato=json&dataInicial={di}&dataFinal={df}"


def _fetch_sgs_json(code: int, start: date, end: date, timeout: int = 30, max_retries: int = 5) -> list:
    url = _sgs_url(code, start, end)
    headers = {"User-Agent": "Mozilla/5.0 (MacroBot/1.0)"}

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 404:
                # SGS retorna 404 quando NÃO HÁ VALORES no intervalo
                txt = (r.text or "").strip()
                if "Value(s) not found" in txt:
                    return []
                raise RuntimeError(f"HTTP 404 no SGS (não esperado): {txt[:200]}")
            
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code} no SGS: {r.text[:200]}")


            txt = (r.text or "").strip()
            if not txt:
                raise RuntimeError("Resposta vazia do SGS (body vazio).")

            # O endpoint retorna uma lista JSON: [{"data":"dd/mm/aaaa","valor":"x"}, ...]
            data = r.json()

            if not isinstance(data, list):
                raise RuntimeError(f"Resposta SGS não é lista JSON. Início: {txt[:200]}")

            return data

        except Exception as e:
            last_err = e
            # backoff exponencial leve
            sleep_s = min(2 ** attempt, 20)
            print(f"[WARN] SGS falhou (serie={code}) tentativa {attempt}/{max_retries}: {e} | retry em {sleep_s}s")
            time.sleep(sleep_s)

    raise RuntimeError(f"Falha definitiva ao coletar SGS serie={code} ({start}..{end}): {last_err}")


def fetch_series_chunked(name: str, code: int, start_date: date, end_date: date, max_years: int = 10) -> pd.DataFrame:
    chunks: List[pd.DataFrame] = []
    window = timedelta(days=int(max_years * 365.25))
    cursor = start_date

    while cursor <= end_date:
        end_chunk = min(cursor + window, end_date)

        raw = _fetch_sgs_json(code, cursor, end_chunk)
        if raw:
            df_chunk = pd.DataFrame(raw)
            # normaliza nomes e tipos
            df_chunk["data"] = pd.to_datetime(df_chunk["data"], format="%d/%m/%Y", errors="coerce")
            df_chunk["valor"] = pd.to_numeric(df_chunk["valor"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
            df_chunk = df_chunk.dropna(subset=["data"]).set_index("data")[["valor"]]
            df_chunk = df_chunk.rename(columns={"valor": name})
            chunks.append(df_chunk)

        cursor = end_chunk + timedelta(days=1)

    if not chunks:
        out = pd.DataFrame(columns=[name])
        out.index = pd.to_datetime(out.index)
        return out

    df_full = pd.concat(chunks).sort_index()
    df_full = df_full[~df_full.index.duplicated(keep="first")]
    df_full.index = pd.to_datetime(df_full.index)
    return df_full


def fetch_all(cfg: MacroConfig) -> Dict[str, pd.DataFrame]:
    dados: Dict[str, pd.DataFrame] = {}
    for name, code in SERIES.items():
        dados[name] = fetch_series_chunked(name, code, cfg.start_date, cfg.end_date, cfg.max_years_chunk)
    return dados


# ----------------------------
# Transform: annual
# ----------------------------
def _annual_last(df: pd.DataFrame, col: str) -> pd.DataFrame:
    # Ano calendário, data no fim do ano (YE-DEC)
    out = df.resample("YE-DEC").last()
    out.columns = [col]
    return out


def _annual_mean(df: pd.DataFrame, col: str) -> pd.DataFrame:
    out = df.resample("YE-DEC").mean()
    out.columns = [col]
    return out


def _annual_sum(df: pd.DataFrame, col: str) -> pd.DataFrame:
    out = df.resample("YE-DEC").sum()
    out.columns = [col]
    return out


def _annual_compound_pct(df: pd.DataFrame, col: str) -> pd.DataFrame:
    # Para taxas percentuais mensais (ex: IPCA): acumula no ano
    out = df.resample("YE-DEC").apply(lambda x: (1 + (x / 100.0)).prod() - 1)
    out.columns = [col]
    return out


def build_annual_df(dados: Dict[str, pd.DataFrame], icc_mode: str) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []

    # PIB (trimestral) -> soma anual (fluxo)
    if not dados["pib"].empty:
        pib = _annual_sum(dados["pib"], "PIB")
        parts.append(pib)

    # Selic, Câmbio, Dívida Pública -> último do ano (nível)
    for nm, col in [("selic", "Selic"), ("cambio", "Cambio"), ("divida_publica", "Divida_Publica")]:
        if not dados[nm].empty:
            parts.append(_annual_last(dados[nm], col))

    # Balança comercial -> soma anual (fluxo)
    if not dados["balanca_comercial"].empty:
        parts.append(_annual_sum(dados["balanca_comercial"], "BALANCA_COMERCIAL"))

    # IPCA -> acumulado anual composto
    if not dados["ipca"].empty:
        ipca_anual = _annual_compound_pct(dados["ipca"], "IPCA")
        ipca_anual["IPCA"] = ipca_anual["IPCA"] * 100
        parts.append(ipca_anual)

    # ICC (nível) -> recomendado: último do ano OU média anual
    if not dados["icc"].empty:
        if icc_mode == "mean":
            icc = _annual_mean(dados["icc"], "ICC")
        else:
            icc = _annual_last(dados["icc"], "ICC")
        parts.append(icc)

    if not parts:
        return pd.DataFrame()

    df = pd.concat(parts, axis=1).sort_index()
    df.index.name = "Data"
    df = df.reset_index()

    # ICC_delta (YoY em nível)
    if "ICC" in df.columns:
        df["ICC_delta"] = df["ICC"].diff()

    # Juros real ex-ante (aprox): Selic final - IPCA anual
    if "Selic" in df.columns and "IPCA" in df.columns:
        df["Juros_Real_ExAnte"] = df["Selic"] - df["IPCA"]  # IPCA está em fração (ex: 0.045); converte para %
        # Se você preferir IPCA em %, comente o *100.0 acima e grave IPCA como %.

    # Ajuste de tipos/datas
    df["Data"] = pd.to_datetime(df["Data"], utc=True)

    # Limpeza: manter apenas colunas relevantes
    keep = ["Data", "Selic", "Cambio", "IPCA", "ICC", "ICC_delta", "PIB", "BALANCA_COMERCIAL", "Divida_Publica", "Juros_Real_ExAnte"]
    df = df[[c for c in keep if c in df.columns]]

    # Remove linhas onde todos os indicadores (exceto Data) são NaN
    value_cols = [c for c in df.columns if c != "Data"]
    df = df.dropna(subset=value_cols, how="all")
    
    return df


# ----------------------------
# Transform: monthly (optional)
# ----------------------------
def _month_end_index(df: pd.DataFrame) -> pd.DataFrame:
    # Alinha índices para fim do mês (M)
    if df.empty:
        return df
    out = df.copy()
    out.index = pd.to_datetime(out.index)
    out = out.sort_index()
    # reamostra para fim do mês, preservando frequência com last/mean conforme o caso
    return out


def build_monthly_df(dados: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Selic final e média do mês (nível)
    selic_final = dados["selic"].resample("M").last().rename(columns={"selic": "Selic_Final"}) if not dados["selic"].empty else None
    selic_media = dados["selic"].resample("M").mean().rename(columns={"selic": "Selic_Media"}) if not dados["selic"].empty else None

    # Câmbio final do mês
    cambio_final = dados["cambio"].resample("M").last().rename(columns={"cambio": "Cambio_Final"}) if not dados["cambio"].empty else None

    # Dívida pública final do mês
    div_final = dados["divida_publica"].resample("M").last().rename(columns={"divida_publica": "Divida_Publica_Final"}) if not dados["divida_publica"].empty else None

    # IPCA mensal (MoM) e 12m acumulado (composto)
    ipca_mom = dados["ipca"].resample("M").last().rename(columns={"ipca": "IPCA_MoM"}) if not dados["ipca"].empty else None
    ipca_12m = None
    if ipca_mom is not None and not ipca_mom.empty:
        # composto 12m: (1+moM/100).rolling(12).prod()-1
        tmp = (1 + (ipca_mom["IPCA_MoM"] / 100.0))
        ipca_12m = (tmp.rolling(12).apply(lambda x: x.prod(), raw=False) - 1).to_frame("IPCA_12m")
        ipca_12m["IPCA_12m"] = ipca_12m["IPCA_12m"] * 100.0  # em %

    # ICC final e média do mês (nível) + delta 12m (nível)
    icc_final = dados["icc"].resample("M").last().rename(columns={"icc": "ICC_Final"}) if not dados["icc"].empty else None
    icc_media = dados["icc"].resample("M").mean().rename(columns={"icc": "ICC_Media"}) if not dados["icc"].empty else None
    icc_delta_12m = None
    if icc_final is not None and not icc_final.empty:
        icc_delta_12m = icc_final["ICC_Final"].diff(12).to_frame("ICC_delta_12m")

    # Balança comercial (mensal já é mensal; por segurança soma no mês)
    bal = dados["balanca_comercial"].resample("M").sum().rename(columns={"balanca_comercial": "BALANCA_COMERCIAL"}) if not dados["balanca_comercial"].empty else None

    parts = [p for p in [selic_final, selic_media, cambio_final, ipca_mom, ipca_12m, icc_final, icc_media, icc_delta_12m, bal, div_final] if p is not None]

    if not parts:
        return pd.DataFrame()

    dfm = pd.concat(parts, axis=1).sort_index()
    dfm.index.name = "Data"
    dfm = dfm.reset_index()
    dfm["Data"] = pd.to_datetime(dfm["Data"], utc=True)

    # Juros real ex-ante 12m (aprox): Selic final - IPCA_12m
    if "Selic_Final" in dfm.columns and "IPCA_12m" in dfm.columns:
        dfm["Juros_Real_ExAnte_12m"] = dfm["Selic_Final"] - dfm["IPCA_12m"]

    # Seleção final (tabela mensal)
    keep = [
        "Data",
        "Selic_Final", "Selic_Media",
        "Cambio_Final",
        "IPCA_MoM", "IPCA_12m",
        "ICC_Final", "ICC_Media", "ICC_delta_12m",
        "BALANCA_COMERCIAL",
        "Divida_Publica_Final",
        "Juros_Real_ExAnte_12m",
    ]
    dfm = dfm[[c for c in keep if c in dfm.columns]]
    return dfm


# ----------------------------
# DB: schema introspection + upsert
# ----------------------------
def get_engine():
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL não definida nas variáveis de ambiente.")
    return create_engine(db_url)


def table_columns(engine, schema: str, table: str) -> List[str]:
    q = text("""
        select column_name
        from information_schema.columns
        where table_schema = :schema and table_name = :table
        order by ordinal_position
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"schema": schema, "table": table}).fetchall()
    return [r[0] for r in rows]


def upsert_dataframe(engine, schema: str, table: str, df: pd.DataFrame, pk: str = "Data") -> Tuple[int, List[str]]:
    if df.empty:
        return 0, []

    cols_db = table_columns(engine, schema, table)
    if not cols_db:
        raise RuntimeError(f"Tabela {schema}.{table} não encontrada (ou sem colunas).")

    # Ajuste: nomes no DF podem não bater em case; tentamos mapear por lower()
    map_db = {c.lower(): c for c in cols_db}
    df_cols = list(df.columns)

    # Converte colunas DF para os nomes reais do DB quando possível
    rename = {}
    for c in df_cols:
        lc = c.lower()
        if lc in map_db:
            rename[c] = map_db[lc]
    df2 = df.rename(columns=rename)

    # Mantém somente interseção
    cols_insert = [c for c in df2.columns if c in cols_db]
    if pk not in cols_insert and pk.lower() in map_db:
        pk = map_db[pk.lower()]
    if pk not in cols_insert:
        raise RuntimeError(f"PK '{pk}' não está presente no DataFrame após mapeamento para colunas do DB.")

    df2 = df2[cols_insert].copy()

    # Monta SQL UPSERT
    cols_sql = ", ".join([f'"{c}"' for c in cols_insert])
    placeholders = ", ".join([f"%({c})s" for c in cols_insert])

    upd_cols = [c for c in cols_insert if c != pk]
    set_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in upd_cols])

    sql = f'''
        insert into "{schema}"."{table}" ({cols_sql})
        values ({placeholders})
        on conflict ("{pk}") do update
        set {set_sql}
    '''

    records = df2.to_dict(orient="records")

    with engine.begin() as conn:
        conn.exec_driver_sql(sql, records)

    return len(records), cols_insert


# ----------------------------
# Main entrypoint
# ----------------------------
def main() -> None:
    cfg = load_config()
    print("== Macro Brasil (BCB/SGS) ==")
    print(f"Período: {cfg.start_date} -> {cfg.end_date}")
    print(f"Chunk: {cfg.max_years_chunk} anos | ICC_MODE: {cfg.icc_mode} | Mensal: {cfg.write_monthly}")

    dados = fetch_all(cfg)

    # ANUAL
    df_annual = build_annual_df(dados, cfg.icc_mode)
    print(f"Anual: {len(df_annual)} linhas")

    engine = get_engine()
    n_annual, cols_annual = upsert_dataframe(engine, cfg.schema, cfg.table_annual, df_annual, pk="Data")
    print(f"Upsert anual OK: {n_annual} registros em {cfg.schema}.{cfg.table_annual}")
    print(f"Colunas gravadas (anual): {cols_annual}")

    # MENSAL (opcional)
    if cfg.write_monthly:
        try:
            df_monthly = build_monthly_df(dados)
            print(f"Mensal: {len(df_monthly)} linhas")
            n_month, cols_month = upsert_dataframe(engine, cfg.schema, cfg.table_monthly, df_monthly, pk="Data")
            print(f"Upsert mensal OK: {n_month} registros em {cfg.schema}.{cfg.table_monthly}")
            print(f"Colunas gravadas (mensal): {cols_month}")
        except Exception as e:
            print(f"[WARN] Mensal não gravado: {e}")

    print("Concluído.")


if __name__ == "__main__":
    main()
