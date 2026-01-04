# pickup/dados_multiplos_itr.py
from __future__ import annotations

import os
from datetime import timedelta

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
import yfinance as yf

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
if not SUPABASE_DB_URL:
    raise RuntimeError("SUPABASE_DB_URL não definida")

ENGINE = sa.create_engine(SUPABASE_DB_URL)

TABELA_ORIGEM = 'public."Demonstracoes_Financeiras_TRI"'
SCHEMA_DESTINO = "public"
NOME_TABELA_DESTINO = "multiplos_TRI"  # SQLAlchemy vai emitir "multiplos_TRI" (aspas) por ser MixedCase no DB

def log(msg: str) -> None:
    print(msg, flush=True)

def rolling_ttm(s: pd.Series) -> pd.Series:
    return s.rolling(4, min_periods=4).sum()

def preco_medio_trimestre(ticker: str, data: pd.Timestamp) -> float | None:
    # Janela ~3 meses antes até alguns dias depois do fechamento
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

def upsert_multiplos_tri(df_out: pd.DataFrame) -> None:
    if df_out.empty:
        log("⚠️ df_out vazio — nada para gravar.")
        return

    meta = sa.MetaData()
    # Reflete a tabela existente com nomes/aspas corretos
    tabela = sa.Table(
        NOME_TABELA_DESTINO,
        meta,
        schema=SCHEMA_DESTINO,
        autoload_with=ENGINE,
    )

    # Converte Data para timestamptz coerente (UTC)
    df_out["Data"] = pd.to_datetime(df_out["Data"], utc=True)

    # Registros
    records = df_out.to_dict(orient="records")

    stmt = insert(tabela).values(records)

    # Campos que serão atualizados (todos, exceto chave)
    chave = ["Ticker", "Data"]
    update_cols = [c.name for c in tabela.columns if c.name not in chave]

    stmt = stmt.on_conflict_do_update(
        index_elements=[tabela.c["Ticker"], tabela.c["Data"]],
        set_={col: getattr(stmt.excluded, col) for col in update_cols},
    )

    with ENGINE.begin() as conn:
        conn.execute(stmt)

    log(f"✅ UPSERT concluído em {SCHEMA_DESTINO}.\"{NOME_TABELA_DESTINO}\" ({len(df_out)} linhas).")

def main() -> None:
    log("[INFO] Lendo demonstrações trimestrais do Supabase...")
    df = pd.read_sql(f"SELECT * FROM {TABELA_ORIGEM}", ENGINE)

    if df.empty:
        log("[WARN] Nenhum dado retornado de Demonstracoes_Financeiras_TRI.")
        return

    df["Data"] = pd.to_datetime(df["Data"], utc=True, errors="coerce")
    df = df.dropna(subset=["Ticker", "Data"]).sort_values(["Ticker", "Data"])

    # Valida colunas mínimas (ajuste se seus nomes forem diferentes)
    required = [
        "Receita_Liquida", "EBIT", "Lucro_Liquido", "Dividendos", "LPA",
        "Ativo_Total", "Ativo_Circulante", "Passivo_Total", "Passivo_Circulante",
        "Patrimonio_Liquido"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        log(f"[ERROR] Colunas faltando na origem TRI: {missing}")
        return

    resultados: list[dict] = []

    for ticker, g in df.groupby("Ticker", sort=False):
        g = g.sort_values("Data").copy()

        # TTM (fluxos)
        g["Receita_12M"] = rolling_ttm(g["Receita_Liquida"])
        g["EBIT_12M"] = rolling_ttm(g["EBIT"])
        g["Lucro_12M"] = rolling_ttm(g["Lucro_Liquido"])
        g["Dividendos_12M"] = rolling_ttm(g["Dividendos"])
        g["LPA_12M"] = rolling_ttm(g["LPA"])

        # Apenas datas com TTM completo
        g_ttm = g.dropna(subset=["Receita_12M", "EBIT_12M", "Lucro_12M", "Dividendos_12M", "LPA_12M"])
        if g_ttm.empty:
            continue

        for _, row in g_ttm.iterrows():
            data = row["Data"]
            px = preco_medio_trimestre(ticker, data)
            if px is None:
                continue

            # Indicadores (estoques = último TRI da linha; fluxos = TTM)
            ativo = row["Ativo_Total"]
            ativo_c = row["Ativo_Circulante"]
            passivo = row["Passivo_Total"]
            passivo_c = row["Passivo_Circulante"]
            pl = row["Patrimonio_Liquido"]

            receita = row["Receita_12M"]
            ebit = row["EBIT_12M"]
            lucro = row["Lucro_12M"]
            div = row["Dividendos_12M"]
            lpa = row["LPA_12M"]

            liquidez = (ativo_c / passivo_c) if passivo_c and passivo_c != 0 else None
            endiv = (passivo / ativo) if ativo and ativo != 0 else None

            # se existir Divida_Liquida na TRI, usamos; senão, alavancagem fica nula
            alav = None
            if "Divida_Liquida" in df.columns and pl and pl != 0:
                alav = (row["Divida_Liquida"] / pl)

            margem_op = (ebit / receita) if receita else None
            margem_liq = (lucro / receita) if receita else None

            roe = (lucro / pl) if pl else None
            roa = (lucro / ativo) if ativo else None

            base_roic = (ativo - passivo_c) if (ativo is not None and passivo_c is not None) else None
            roic = (ebit / base_roic) if base_roic and base_roic != 0 else None

            dy = (div / px) if px else None
            pl_mult = (px / lpa) if lpa and lpa != 0 else None

            # P/VP exige VPA ou número de ações. Se tiver VPA na TRI, usamos.
            pvp = None
            if "VPA" in df.columns and row.get("VPA") not in (None, 0):
                pvp = px / row["VPA"]

            payout = (div / lucro) if lucro and lucro != 0 else None

            resultados.append({
                "Ticker": ticker,
                "Data": data,
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

    df_out = pd.DataFrame(resultados)

    if df_out.empty:
        log("[WARN] Nenhuma linha gerada (provável falta de preço ou TTM incompleto).")
        return

    # Sanity check rápido
    log(f"[INFO] Linhas geradas: {len(df_out)} | Tickers: {df_out['Ticker'].nunique()}")
    log("[INFO] Gravando via UPSERT (Ticker, Data)...")
    upsert_multiplos_tri(df_out)

if __name__ == "__main__":
    main()
