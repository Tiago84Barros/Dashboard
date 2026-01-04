# pickup/dados_multiplos_itr.py
from __future__ import annotations

import os
import sys
import pandas as pd
import numpy as np
import sqlalchemy as sa
import yfinance as yf
from datetime import timedelta

# =========================
# Config
# =========================
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
if not SUPABASE_DB_URL:
    raise RuntimeError("SUPABASE_DB_URL não definida")

ENGINE = sa.create_engine(SUPABASE_DB_URL)
TABELA_ORIGEM = 'public."Demonstracoes_Financeiras_TRI"'
TABELA_DESTINO = "public.multiplos_TRI"

# =========================
# Helpers
# =========================
def log(msg: str):
    print(msg, flush=True)

def rolling_ttm(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col].rolling(4, min_periods=4).sum()

def get_preco_medio_trimestre(ticker: str, data: pd.Timestamp) -> float | None:
    try:
        ini = data - pd.DateOffset(months=3)
        fim = data + timedelta(days=5)
        hist = yf.download(f"{ticker}.SA", start=ini, end=fim, progress=False)
        if hist.empty:
            return None
        return float(hist["Close"].mean())
    except Exception:
        return None

# =========================
# Main
# =========================
def main():
    log("🔹 Carregando demonstrações trimestrais do Supabase...")
    df = pd.read_sql(f"SELECT * FROM {TABELA_ORIGEM}", ENGINE)

    if df.empty:
        log("⚠️ Nenhum dado encontrado.")
        return

    df["Data"] = pd.to_datetime(df["Data"])
    df = df.sort_values(["Ticker", "Data"])

    resultados = []

    for ticker, g in df.groupby("Ticker"):
        g = g.sort_values("Data").copy()

        # -------- TTM (fluxo)
        g["Receita_12M"] = rolling_ttm(g, "Receita_Liquida")
        g["EBIT_12M"] = rolling_ttm(g, "EBIT")
        g["Lucro_12M"] = rolling_ttm(g, "Lucro_Liquido")
        g["Dividendos_12M"] = rolling_ttm(g, "Dividendos")
        g["LPA_12M"] = rolling_ttm(g, "LPA")

        # -------- Último trimestre (estoque)
        cols_last = [
            "Ativo_Total",
            "Ativo_Circulante",
            "Passivo_Total",
            "Passivo_Circulante",
            "Patrimonio_Liquido",
            "Divida_Liquida",
        ]

        for _, row in g.dropna(subset=["Receita_12M"]).iterrows():
            preco = get_preco_medio_trimestre(ticker, row["Data"])
            if not preco or preco <= 0:
                continue

            try:
                liquidez = row["Ativo_Circulante"] / row["Passivo_Circulante"] if row["Passivo_Circulante"] > 0 else None
                endiv = row["Passivo_Total"] / row["Ativo_Total"] if row["Ativo_Total"] > 0 else None
                alav = row["Divida_Liquida"] / row["Patrimonio_Liquido"] if row["Patrimonio_Liquido"] > 0 else None

                margem_op = row["EBIT_12M"] / row["Receita_12M"] if row["Receita_12M"] else None
                margem_liq = row["Lucro_12M"] / row["Receita_12M"] if row["Receita_12M"] else None

                roe = row["Lucro_12M"] / row["Patrimonio_Liquido"] if row["Patrimonio_Liquido"] else None
                roa = row["Lucro_12M"] / row["Ativo_Total"] if row["Ativo_Total"] else None
                roic = row["EBIT_12M"] / (row["Ativo_Total"] - row["Passivo_Circulante"]) if (row["Ativo_Total"] - row["Passivo_Circulante"]) > 0 else None

                dy = row["Dividendos_12M"] / preco if preco else None
                pl = preco / row["LPA_12M"] if row["LPA_12M"] else None
                pvp = preco / (row["Patrimonio_Liquido"] / row["Numero_Acoes"]) if row.get("Numero_Acoes") else None
                payout = row["Dividendos_12M"] / row["Lucro_12M"] if row["Lucro_12M"] else None

                resultados.append({
                    "Ticker": ticker,
                    "Data": row["Data"],
                    "Liquidez_Corrente": liquidez,
                    "Endividamento_Total": endiv,
                    "Alavancagem_Financeira": alav,
                    "Margem_Operacional": margem_op,
                    "Margem_Liquida": margem_liq,
                    "ROE": roe,
                    "ROA": roa,
                    "ROIC": roic,
                    "DY": dy,
                    "P/L": pl,
                    "P/VP": pvp,
                    "Payout": payout,
                })
            except Exception as e:
                log(f"⚠️ Erro em {ticker} {row['Data']}: {e}")

    df_out = pd.DataFrame(resultados)

    if df_out.empty:
        log("⚠️ Nenhum múltiplo gerado.")
        return

    log(f"🔹 Gravando {len(df_out)} linhas em {TABELA_DESTINO} (UPSERT)...")

    with ENGINE.begin() as conn:
        conn.execute(sa.text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_multiplos_tri
            ON public.multiplos_TRI ("Ticker","Data");
        """))
        df_out.to_sql(
            "multiplos_TRI",
            conn,
            if_exists="append",
            index=False,
            method="multi"
        )

    log("✅ Múltiplos trimestrais atualizados com sucesso.")

if __name__ == "__main__":
    main()
