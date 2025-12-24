# Antigo Algoritmo 3

from __future__ import annotations

import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.engine import Engine
from bcb import sgs


# ============================================================
# Configurações (mantidas do notebook)
# ============================================================
START_DATE = datetime.strptime("01/01/2010", "%d/%m/%Y").date()
END_DATE = datetime.today().date()

IND_ECONOMICOS = {
    432: "selic",
    433: "ipca",
    1: "cambio",
    22707: "balanca_comercial",
    14: "icc",
    4380: "pib",
    4502: "divida_publica",
}


# ============================================================
# Funções auxiliares (lógica idêntica ao notebook)
# ============================================================
def _fetch_series_chunked(name, code, start_date, end_date, max_years=10):
    chunks = []
    window = timedelta(days=int(max_years * 365.25))
    cursor = start_date

    while cursor <= end_date:
        end_chunk = min(cursor + window, end_date)
        df_chunk = sgs.get({name: code}, start=cursor, end=end_chunk)
        df_chunk.columns = [name]
        chunks.append(df_chunk)
        cursor = end_chunk + timedelta(days=1)

    df_full = pd.concat(chunks)
    df_full = df_full[~df_full.index.duplicated(keep="first")]
    return df_full


def _calcular_acumulado_anual(df, coluna):
    return df.resample("YE").apply(lambda x: (1 + x / 100).prod() - 1)


def _ultimo_dia_util(df):
    return df.resample("YE").last()


def _somar_anual(df):
    return df.resample("YE").sum()


# ============================================================
# Infraestrutura Supabase
# ============================================================
def _ensure_table(engine: Engine):
    ddl = """
    create schema if not exists cvm;

    create table if not exists cvm.info_economica (
        Data date primary key,
        selic double precision,
        cambio double precision,
        ipca double precision,
        icc double precision,
        pib double precision,
        balanca_comercial double precision,
        divida_publica double precision
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


# ============================================================
# Função principal (pipeline)
# ============================================================
def run(engine: Engine) -> pd.DataFrame:
    """
    Algoritmo 3 — Ingestão de Indicadores Macroeconômicos (BCB)

    Conversão fiel do notebook:
    - coleta via SGS (python-bcb)
    - consolidação anual
    - persistência no Supabase
    """

    _ensure_table(engine)

    # --------------------------------------------------------
    # 1) Coleta das séries
    # --------------------------------------------------------
    dados = {}
    for code, name in IND_ECONOMICOS.items():
        dados[name] = _fetch_series_chunked(name, code, START_DATE, END_DATE)

    # --------------------------------------------------------
    # 2) Consolidação anual
    # --------------------------------------------------------
    dados_anuais = pd.DataFrame()

    for name, df in dados.items():
        if name == "pib":
            dados_anuais = pd.concat([dados_anuais, _somar_anual(df)], axis=1)
        elif name in ["selic", "cambio", "divida_publica"]:
            dados_anuais = pd.concat([dados_anuais, _ultimo_dia_util(df)], axis=1)
        elif name == "balanca_comercial":
            dados_anuais = pd.concat([dados_anuais, _somar_anual(df)], axis=1)
        else:  # ipca, icc
            dados_anuais = pd.concat(
                [dados_anuais, _calcular_acumulado_anual(df, name)], axis=1
            )

    # --------------------------------------------------------
    # 3) DataFrame final (igual notebook)
    # --------------------------------------------------------
    indicadores_economicos = pd.DataFrame(
        {
            "Data": dados_anuais.index,
            "selic": dados_anuais.get("selic"),
            "cambio": dados_anuais.get("cambio"),
            "ipca": dados_anuais.get("ipca"),
            "icc": dados_anuais.get("icc"),
            "pib": dados_anuais.get("pib"),
            "balanca_comercial": dados_anuais.get("balanca_comercial"),
            "divida_publica": dados_anuais.get("divida_publica"),
        }
    )

    indicadores_economicos["Data"] = pd.to_datetime(indicadores_economicos["Data"]).dt.date
    indicadores_economicos = indicadores_economicos.where(
        pd.notnull(indicadores_economicos), None
    )

    # --------------------------------------------------------
    # 4) Persistência (UPSERT)
    # --------------------------------------------------------
    upsert = """
    insert into cvm.info_economica (
        Data, selic, cambio, ipca, icc, pib, balanca_comercial, divida_publica
    )
    values (
        :Data, :selic, :cambio, :ipca, :icc, :pib, :balanca_comercial, :divida_publica
    )
    on conflict (Data) do update set
        selic = excluded.selic,
        cambio = excluded.cambio,
        ipca = excluded.ipca,
        icc = excluded.icc,
        pib = excluded.pib,
        balanca_comercial = excluded.balanca_comercial,
        divida_publica = excluded.divida_publica;
    """

    with engine.begin() as conn:
        conn.execute(
            text(upsert),
            indicadores_economicos.to_dict(orient="records"),
        )

    return indicadores_economicos
