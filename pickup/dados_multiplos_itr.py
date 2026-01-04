# pickup/dados_multiplos_itr.py
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
import yfinance as yf

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
if not SUPABASE_DB_URL:
    raise RuntimeError("SUPABASE_DB_URL não definida")

ENGINE = sa.create_engine(SUPABASE_DB_URL)

ORIGEM = 'public."Demonstracoes_Financeiras_TRI"'
DEST_SCHEMA = "public"
DEST_TABLE = "multiplos_TRI"  # no DB é public."multiplos_TRI"


def log(msg: str) -> None:
    print(msg, flush=True)


def to_utc_midnight_ts(d: pd.Timestamp) -> pd.Timestamp:
    # origem é date -> destino é timestamptz
    # grava como 00:00:00Z
    if pd.isna(d):
        return d
    dt = pd.Timestamp(d).to_pydatetime()
    if isinstance(dt, datetime) and dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return pd.Timestamp(dt.astimezone(timezone.utc))


def rolling_ttm(df: pd.DataFrame, col: str) -> pd.Series:
    # TRI já é trimestral isolado -> TTM = soma dos últimos 4 TRI
    return df[col].rolling(4, min_periods=4).sum()


def preco_medio_trimestre(ticker: str, data: pd.Timestamp) -> float | None:
    # janela ~3 meses antes até poucos dias após a data de fechamento
    ini = (data - pd.DateOffset(months=3)).date()
    fim = (data + timedelta(days=7)).date()
    try:
        hist = yf.download(f"{ticker}.SA", start=ini, end=fim, progress=False)
        if hist.empty:
            return None
        px = float(hist["Close"].mean())
        return px if px > 0 else None
    except Exception:
        return None


def shares_outstanding(ticker: str) -> float | None:
    # para estimar Market Cap -> P/VP = MarketCap / PL
    # nem sempre disponível
    try:
        t = yf.Ticker(f"{ticker}.SA")
        info = getattr(t, "info", {}) or {}
        so = info.get("sharesOutstanding")
        if so and so > 0:
            return float(so)
        return None
    except Exception:
        return None


def upsert(df_out: pd.DataFrame) -> None:
    meta = sa.MetaData()
    table = sa.Table(DEST_TABLE, meta, schema=DEST_SCHEMA, autoload_with=ENGINE)

    # ON CONFLICT requer unique index (criamos se não existir)
    with ENGINE.begin() as conn:
        conn.execute(sa.text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_multiplos_tri_ticker_data
            ON public."multiplos_TRI" ("Ticker","Data");
        """))

    records = df_out.to_dict(orient="records")
    stmt = insert(table).values(records)

    key_cols = ["Ticker", "Data"]
    update_cols = [c.name for c in table.columns if c.name not in key_cols]

    stmt = stmt.on_conflict_do_update(
        index_elements=[table.c["Ticker"], table.c["Data"]],
        set_={c: getattr(stmt.excluded, c) for c in update_cols},
    )

    with ENGINE.begin() as conn:
        conn.execute(stmt)

    log(f"[OK] UPSERT em public.\"multiplos_TRI\": {len(df_out)} linhas.")


def main() -> None:
    log("[INFO] Lendo Demonstracoes_Financeiras_TRI do Supabase...")
    df = pd.read_sql(f"SELECT * FROM {ORIGEM}", ENGINE)

    if df.empty:
        log("[WARN] Origem vazia.")
        return

    # Tipos
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df = df.dropna(subset=["Ticker", "Data"]).sort_values(["Ticker", "Data"])

    # colunas obrigatórias (conforme seu DDL)
    required = [
        "Receita_Liquida", "EBIT", "Lucro_Liquido", "Dividendos", "LPA",
        "Ativo_Total", "Ativo_Circulante", "Passivo_Circulante", "Passivo_Total",
        "Patrimonio_Liquido", "Divida_Liquida"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        log(f"[ERROR] Colunas faltando na TRI: {missing}")
        return

    resultados: list[dict] = []

    # cache simples para sharesOutstanding (não chamar yfinance repetidamente)
    shares_cache: dict[str, float | None] = {}

    for ticker, g in df.groupby("Ticker", sort=False):
        g = g.sort_values("Data").copy()

        # TTM fluxos (já isolado)
        g["Receita_12M"] = rolling_ttm(g, "Receita_Liquida")
        g["EBIT_12M"] = rolling_ttm(g, "EBIT")
        g["Lucro_12M"] = rolling_ttm(g, "Lucro_Liquido")
        g["Dividendos_12M"] = rolling_ttm(g, "Dividendos")
        g["LPA_12M"] = rolling_ttm(g, "LPA")

        g_ttm = g.dropna(subset=["Receita_12M", "EBIT_12M", "Lucro_12M", "Dividendos_12M", "LPA_12M"])
        if g_ttm.empty:
            continue

        # shares (uma vez por ticker)
        if ticker not in shares_cache:
            shares_cache[ticker] = shares_outstanding(ticker)

        for _, row in g_ttm.iterrows():
            data = pd.Timestamp(row["Data"])
            px = preco_medio_trimestre(ticker, data)
            if px is None:
                continue

            # estoques (último TRI)
            ativo = float(row["Ativo_Total"]) if row["Ativo_Total"] is not None else None
            ativo_c = float(row["Ativo_Circulante"]) if row["Ativo_Circulante"] is not None else None
            passivo = float(row["Passivo_Total"]) if row["Passivo_Total"] is not None else None
            passivo_c = float(row["Passivo_Circulante"]) if row["Passivo_Circulante"] is not None else None
            pl = float(row["Patrimonio_Liquido"]) if row["Patrimonio_Liquido"] is not None else None
            divliq = float(row["Divida_Liquida"]) if row["Divida_Liquida"] is not None else None

            # fluxos TTM
            receita = float(row["Receita_12M"]) if row["Receita_12M"] is not None else None
            ebit = float(row["EBIT_12M"]) if row["EBIT_12M"] is not None else None
            lucro = float(row["Lucro_12M"]) if row["Lucro_12M"] is not None else None
            div = float(row["Dividendos_12M"]) if row["Dividendos_12M"] is not None else None
            lpa = float(row["LPA_12M"]) if row["LPA_12M"] is not None else None

            liquidez = (ativo_c / passivo_c) if (ativo_c is not None and passivo_c not in (None, 0)) else None
            endiv = (passivo / ativo) if (passivo is not None and ativo not in (None, 0)) else None
            alav = (divliq / pl) if (divliq is not None and pl not in (None, 0)) else None

            margem_op = (ebit / receita) if (ebit is not None and receita not in (None, 0)) else None
            margem_liq = (lucro / receita) if (lucro is not None and receita not in (None, 0)) else None

            roe = (lucro / pl) if (lucro is not None and pl not in (None, 0)) else None
            roa = (lucro / ativo) if (lucro is not None and ativo not in (None, 0)) else None

            base_roic = (ativo - passivo_c) if (ativo is not None and passivo_c is not None) else None
            roic = (ebit / base_roic) if (ebit is not None and base_roic not in (None, 0)) else None

            dy = (div / px) if (div is not None and px) else None
            pl_mult = (px / lpa) if (lpa is not None and lpa != 0) else None

            # P/VP via MarketCap / PL (quando sharesOutstanding existir)
            pvp = None
            so = shares_cache.get(ticker)
            if so and pl not in (None, 0):
                market_cap = px * so
                pvp = market_cap / pl

            payout = (div / lucro) if (div is not None and lucro not in (None, 0)) else None

            resultados.append({
                "Ticker": ticker,
                "Data": to_utc_midnight_ts(data),
                "Liquidez_Corrente": liquidez,
                "Endividamento_Total": endiv,
                "Alavancagem_Financeira": alav,
                "Margem_Operacional": margem_op,
                "Margem_Liquida": margem_liq,
                "ROE": roe,
                "ROA": roa,
                "ROIC": roic,
                "DY": dy,
                "P/L": pl_mult,
                "P/VP": pvp,
                "Payout": payout,
            })
