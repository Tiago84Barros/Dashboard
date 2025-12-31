import os
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values


# ======================
# CONFIG
# ======================
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")  # obrigatório
YF_START = os.getenv("YF_START", "2010-01-01")
YF_END = os.getenv("YF_END", "2023-12-31")

# Para reduzir ruído do yfinance, como no notebook
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


# ======================
# DB READ (Supabase)
# ======================
def carregar_demonstracoes() -> pd.DataFrame:
    if not SUPABASE_DB_URL:
        raise RuntimeError("Defina SUPABASE_DB_URL com a connection string Postgres do Supabase.")

    sql = 'SELECT * FROM public."Demonstracoes_Financeiras";'

    with psycopg2.connect(SUPABASE_DB_URL) as conn:
        df = pd.read_sql_query(sql, conn)

    # Fidelidade ao notebook
    df["Dividendos"] = df["Dividendos"].astype(float)
    df["Data"] = pd.to_datetime(df["Data"])
    df["Ano"] = df["Data"].dt.year
    df["ticker_yf"] = df["Ticker"] + ".SA"

    return df


# ======================
# YFINANCE (preço médio anual)
# ======================
def obter_precos_medios_anuais(tickers_yf: np.ndarray) -> pd.DataFrame:
    df_precos_medios = pd.DataFrame()

    for ticker in tickers_yf:
        try:
            prices = yf.download(ticker, start=YF_START, end=YF_END, progress=False)

            # Notebook achata MultiIndex se existir
            if isinstance(prices.columns, pd.MultiIndex):
                prices.columns = prices.columns.droplevel(1)

            if prices.empty:
                continue

            prices["Ano"] = prices.index.year
            preco_medio_anual = prices.groupby("Ano")["Close"].mean().reset_index()

            preco_medio_anual["ticker_yf"] = ticker
            df_precos_medios = pd.concat([df_precos_medios, preco_medio_anual], ignore_index=True)

        except Exception:
            continue

    df_precos_medios.rename(columns={"Close": "Preco_Medio_Anual"}, inplace=True)
    df_precos_medios["Ano"] = df_precos_medios["Ano"].astype(int)

    return df_precos_medios


# ======================
# CÁLCULO DOS MÚLTIPLOS
# ======================
def calcular_multiplos(df_demonstracoes: pd.DataFrame) -> pd.DataFrame:
    df_precos_medios = obter_precos_medios_anuais(df_demonstracoes["ticker_yf"].unique())

    df_demonstracoes = pd.merge(
        df_demonstracoes,
        df_precos_medios,
        on=["ticker_yf", "Ano"],
        how="left",
    )

    df_multiplos = pd.DataFrame()
    df_multiplos["Ticker"] = df_demonstracoes["Ticker"]
    df_multiplos["Data"] = df_demonstracoes["Data"]

    # 1) Liquidez Corrente
    df_multiplos["Liquidez_Corrente"] = np.where(
        df_demonstracoes["Passivo_Circulante"] > 0,
        df_demonstracoes["Ativo_Circulante"] / df_demonstracoes["Passivo_Circulante"],
        0,
    )

    # 2) Estrutura de capital
    df_multiplos["Endividamento_Total"] = df_demonstracoes["Passivo_Total"] / df_demonstracoes["Ativo_Total"]
    df_multiplos["Alavancagem_Financeira"] = df_demonstracoes["Divida_Liquida"] / df_demonstracoes["Patrimonio_Liquido"]

    # 3) Rentabilidade (percentual)
    df_multiplos["Margem_Operacional"] = (df_demonstracoes["EBIT"] / df_demonstracoes["Receita_Liquida"]) * 100
    df_multiplos["Margem_Liquida"] = (df_demonstracoes["Lucro_Liquido"] / df_demonstracoes["Receita_Liquida"]) * 100
    df_multiplos["ROE"] = (df_demonstracoes["Lucro_Liquido"] / df_demonstracoes["Patrimonio_Liquido"]) * 100
    df_multiplos["ROA"] = (df_demonstracoes["Lucro_Liquido"] / df_demonstracoes["Ativo_Total"]) * 100
    df_multiplos["ROIC"] = (df_demonstracoes["EBIT"] / (df_demonstracoes["Ativo_Total"] - df_demonstracoes["Passivo_Circulante"])) * 100

    # 4) Valor
    df_multiplos["N_Acoes"] = abs(df_demonstracoes["Lucro_Liquido"]) / abs(df_demonstracoes["LPA"])
    df_multiplos["DY"] = (df_demonstracoes["Dividendos"] / df_multiplos["N_Acoes"]) / df_demonstracoes["Preco_Medio_Anual"]
    df_multiplos["P/L"] = df_demonstracoes["Preco_Medio_Anual"] / df_demonstracoes["LPA"]
    df_multiplos["P/VP"] = df_demonstracoes["Preco_Medio_Anual"] / (df_demonstracoes["Patrimonio_Liquido"] / df_multiplos["N_Acoes"])
    df_multiplos["Payout"] = df_demonstracoes["Dividendos"] / df_demonstracoes["Lucro_Liquido"]

    colunas_multiplos_desejadas = [
        "Ticker",
        "Data",
        "Liquidez_Corrente",
        "Endividamento_Total",
        "Alavancagem_Financeira",
        "Margem_Operacional",
        "Margem_Liquida",
        "ROE",
        "ROA",
        "ROIC",
        "DY",
        "P/L",
        "P/VP",
        "Payout",
    ]

    df_out = df_multiplos[[c for c in colunas_multiplos_desejadas if c in df_multiplos.columns]].copy()

    # Higienização mínima (evita NaN/inf no Postgres; o notebook não trata, mas é necessário para carga)
    df_out.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_out.fillna(0, inplace=True)

    # Garantir Data compatível (Postgres TIMESTAMPTZ aceita datetime)
    df_out["Data"] = pd.to_datetime(df_out["Data"])

    return df_out


# ======================
# DB WRITE (Supabase) - "REPLACE" equivalente
# ======================
def substituir_tabela_multiplos(df_multiplos: pd.DataFrame) -> None:
    """
    Emula o if_exists='replace' do SQLite/pandas:
    - TRUNCATE na tabela public."multiplos"
    - INSERT em lote
    """
    if not SUPABASE_DB_URL:
        raise RuntimeError("Defina SUPABASE_DB_URL com a connection string Postgres do Supabase.")

    # Atenção a colunas com "/" — precisam de aspas duplas no SQL
    cols = list(df_multiplos.columns)
    values = [tuple(x) for x in df_multiplos.itertuples(index=False, name=None)]

    insert_sql = f'''
        INSERT INTO public."multiplos"
        ({", ".join([f'"{c}"' for c in cols])})
        VALUES %s
    '''

    with psycopg2.connect(SUPABASE_DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute('TRUNCATE TABLE public."multiplos";')
            execute_values(cur, insert_sql, values, page_size=5000)
        conn.commit()

    print(f"[OK] multiplos substituída no Supabase: {len(df_multiplos)} linhas.")


def main():
    df_demonstracoes = carregar_demonstracoes()
    df_multiplos = calcular_multiplos(df_demonstracoes)
    substituir_tabela_multiplos(df_multiplos)


if __name__ == "__main__":
    main()
