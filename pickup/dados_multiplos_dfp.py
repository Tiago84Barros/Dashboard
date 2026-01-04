import os
import logging
from typing import Iterable, List, Tuple

import numpy as np
import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text


# ======================
# CONFIG
# ======================
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")  # obrigatório

YF_START = os.getenv("YF_START", "2010-01-01")
YF_END = os.getenv("YF_END", "2023-12-31")
YF_BATCH_SIZE = int(os.getenv("YF_BATCH_SIZE", "50"))  # lote de tickers por download

# para reduzir ruído do yfinance
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


# ======================
# UTILS
# ======================
def _safe_div(n: pd.Series, d: pd.Series) -> pd.Series:
    d = d.replace(0, np.nan)
    out = n / d
    return out.replace([np.inf, -np.inf], np.nan)


def _chunked(it: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(it), n):
        yield it[i : i + n]


# ======================
# DB READ (Supabase)
# ======================
def carregar_demonstracoes() -> pd.DataFrame:
    if not SUPABASE_DB_URL:
        raise RuntimeError("Defina SUPABASE_DB_URL com a connection string Postgres do Supabase.")

    engine = create_engine(SUPABASE_DB_URL)

    sql = text('SELECT * FROM public."Demonstracoes_Financeiras";')

    with engine.connect() as conn:
        df = pd.read_sql_query(sql, conn)

    if "Dividendos" in df.columns:
        df["Dividendos"] = df["Dividendos"].astype(float)

    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")

    # normaliza TZ
    if df["Data"].dt.tz is None:
        df["Data"] = df["Data"].dt.tz_localize("UTC")
    else:
        df["Data"] = df["Data"].dt.tz_convert("UTC")

    df["Ano"] = df["Data"].dt.year
    df["ticker_yf"] = df["Ticker"].astype(str) + ".SA"

    df = df[df["Ticker"].notna() & df["Data"].notna()].copy()
    return df



# ======================
# YFINANCE (preço médio anual) - em lotes
# ======================
def obter_precos_medios_anuais(tickers_yf: np.ndarray) -> pd.DataFrame:
    tickers_list = sorted({t for t in tickers_yf if isinstance(t, str) and t.strip()})
    if not tickers_list:
        return pd.DataFrame(columns=["ticker_yf", "Ano", "Preco_Medio_Anual"])

    rows: List[Tuple[str, int, float]] = []

    for batch in _chunked(tickers_list, YF_BATCH_SIZE):
        try:
            data = yf.download(
                tickers=" ".join(batch),
                start=YF_START,
                end=YF_END,
                progress=False,
                group_by="ticker",
                threads=True,
                auto_adjust=False,
            )

            if data is None or data.empty:
                continue

            # Caso 1: MultiIndex (ticker, campo)
            if isinstance(data.columns, pd.MultiIndex):
                # queremos o Close
                # estrutura típica: columns = [(TICKER, 'Open'), ...]
                # extrai Close de cada ticker
                close = data.xs("Close", axis=1, level=1, drop_level=False)
                # close ainda pode ter MultiIndex; vamos construir por ticker
                for t in batch:
                    if (t, "Close") not in close.columns:
                        continue
                    s = close[(t, "Close")].dropna()
                    if s.empty:
                        continue
                    df_t = s.to_frame("Close")
                    df_t["Ano"] = df_t.index.year
                    g = df_t.groupby("Ano")["Close"].mean()
                    for ano, pm in g.items():
                        rows.append((t, int(ano), float(pm)))

            # Caso 2: colunas simples (apenas um ticker)
            else:
                if "Close" not in data.columns:
                    continue
                s = data["Close"].dropna()
                if s.empty:
                    continue
                df_t = s.to_frame("Close")
                df_t["Ano"] = df_t.index.year
                g = df_t.groupby("Ano")["Close"].mean()
                # batch aqui provavelmente tinha 1 ticker, mas garantimos:
                t = batch[0]
                for ano, pm in g.items():
                    rows.append((t, int(ano), float(pm)))

        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=["ticker_yf", "Ano", "Preco_Medio_Anual"])

    df_precos = pd.DataFrame(rows, columns=["ticker_yf", "Ano", "Preco_Medio_Anual"])
    df_precos["Ano"] = df_precos["Ano"].astype(int)

    return df_precos


# ======================
# CÁLCULO DOS MÚLTIPLOS (lógica do Algoritmo_4)
# ======================
def calcular_multiplos(df_demonstracoes: pd.DataFrame) -> pd.DataFrame:
    df_precos = obter_precos_medios_anuais(df_demonstracoes["ticker_yf"].unique())

    df = df_demonstracoes.merge(
        df_precos,
        on=["ticker_yf", "Ano"],
        how="left",
        validate="m:1",
    )

    out = pd.DataFrame()
    out["Ticker"] = df["Ticker"].astype(str)
    out["Data"] = df["Data"]

    # 1) Liquidez Corrente
    out["Liquidez_Corrente"] = np.where(
        df["Passivo_Circulante"] > 0,
        _safe_div(df["Ativo_Circulante"], df["Passivo_Circulante"]),
        0.0,
    )

    # 2) Estrutura de capital
    out["Endividamento_Total"] = _safe_div(df["Passivo_Total"], df["Ativo_Total"]).fillna(0.0)
    out["Alavancagem_Financeira"] = _safe_div(df["Divida_Liquida"], df["Patrimonio_Liquido"]).fillna(0.0)

    # 3) Rentabilidade (%)
    out["Margem_Operacional"] = (_safe_div(df["EBIT"], df["Receita_Liquida"]) * 100).fillna(0.0)
    out["Margem_Liquida"] = (_safe_div(df["Lucro_Liquido"], df["Receita_Liquida"]) * 100).fillna(0.0)
    out["ROE"] = (_safe_div(df["Lucro_Liquido"], df["Patrimonio_Liquido"]) * 100).fillna(0.0)
    out["ROA"] = (_safe_div(df["Lucro_Liquido"], df["Ativo_Total"]) * 100).fillna(0.0)

    base_roic = (df["Ativo_Total"] - df["Passivo_Circulante"])
    out["ROIC"] = (_safe_div(df["EBIT"], base_roic) * 100).fillna(0.0)

    # 4) Valor
    # N_Acoes = |Lucro_Liquido| / |LPA|
    # Observação: LPA=0 gera NaN -> zera
    n_acoes = _safe_div(df["Lucro_Liquido"].abs(), df["LPA"].abs()).fillna(0.0)

    # DY = (Dividendos / N_Acoes) / Preco_Medio_Anual
    # Se Preco_Medio_Anual não existir, zera
    dy_num = _safe_div(df["Dividendos"], n_acoes).fillna(0.0)
    out["DY"] = _safe_div(dy_num, df["Preco_Medio_Anual"]).fillna(0.0)

    out["P/L"] = _safe_div(df["Preco_Medio_Anual"], df["LPA"]).fillna(0.0)

    vpa = _safe_div(df["Patrimonio_Liquido"], n_acoes).fillna(0.0)
    out["P/VP"] = _safe_div(df["Preco_Medio_Anual"], vpa).fillna(0.0)

    out["Payout"] = _safe_div(df["Dividendos"], df["Lucro_Liquido"]).fillna(0.0)

    # schema tem coluna Payout; também existe Payout no notebook
    # schema também tem "Payout" e "P/L", "P/VP"

    # normalizações finais
    out.replace([np.inf, -np.inf], np.nan, inplace=True)
    out.fillna(0.0, inplace=True)

    # garante tz UTC e tipo datetime compatível com TIMESTAMPTZ
    out["Data"] = pd.to_datetime(out["Data"], errors="coerce")
    if out["Data"].dt.tz is None:
        out["Data"] = out["Data"].dt.tz_localize("UTC")
    else:
        out["Data"] = out["Data"].dt.tz_convert("UTC")

    # remove linhas sem Data/Ticker
    out = out[out["Ticker"].notna() & out["Data"].notna()].copy()

    # ordenação opcional para facilitar debug
    out.sort_values(["Ticker", "Data"], inplace=True)

    return out


# ======================
# DB WRITE (Supabase) - UPSERT
# ======================
def upsert_multiplos(df_multiplos: pd.DataFrame) -> None:
    if not SUPABASE_DB_URL:
        raise RuntimeError("Defina SUPABASE_DB_URL com a connection string Postgres do Supabase.")

    if df_multiplos.empty:
        print("[INFO] Nenhuma linha para gravar em public.multiplos.")
        return

    cols = list(df_multiplos.columns)
    values = [tuple(x) for x in df_multiplos.itertuples(index=False, name=None)]

    # cria unique index necessário para ON CONFLICT
    ddl_unique = """
    create unique index if not exists uq_multiplos_ticker_data
    on public.multiplos ("Ticker","Data");
    """

    insert_sql = f"""
        INSERT INTO public.multiplos
        ({", ".join([f'"{c}"' for c in cols])})
        VALUES %s
        ON CONFLICT ("Ticker","Data") DO UPDATE SET
        {", ".join([f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in ("Ticker", "Data")])}
    """

    with psycopg2.connect(SUPABASE_DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_unique)
            execute_values(cur, insert_sql, values, page_size=5000)
        conn.commit()

    print(f"[OK] UPSERT public.multiplos: {len(df_multiplos)} linhas processadas.")


def main() -> None:
    print("[INFO] Carregando Demonstracoes_Financeiras do Supabase...")
    df_demonstracoes = carregar_demonstracoes()
    print(f"[INFO] Linhas de demonstrações: {len(df_demonstracoes)}")

    print("[INFO] Calculando múltiplos (Algoritmo_4)...")
    df_multiplos = calcular_multiplos(df_demonstracoes)
    print(f"[INFO] Linhas de múltiplos geradas: {len(df_multiplos)}")

    print("[INFO] Gravando no Supabase (UPSERT)...")
    upsert_multiplos(df_multiplos)

    print("[DONE] Rotina dados_multiplos_dfp concluída.")


if __name__ == "__main__":
    main()
