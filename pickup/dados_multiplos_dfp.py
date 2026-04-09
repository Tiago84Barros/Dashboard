import os
import logging
from typing import Iterable, List, Tuple

import numpy as np
import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

try:
    from core.db import get_engine
except Exception:  # pragma: no cover
    get_engine = None

try:
    from auditoria_dados.ingestion_log import IngestionLog as _IngestionLog
    from auditoria_dados.ingestion_log import validate_required_columns
    from auditoria_dados.ingestion_log import validate_key_columns
    from auditoria_dados.ingestion_log import validate_unique_rows
except ImportError:
    _IngestionLog = None
    validate_required_columns = None
    validate_key_columns = None
    validate_unique_rows = None


# ======================
# CONFIG
# ======================
SUPABASE_DB_URL = (
    os.getenv("SUPABASE_DB_URL", "").strip()
    or os.getenv("SUPABASE_DB_URL_PG", "").strip()
)

YF_START = os.getenv("YF_START", "2010-01-01")
YF_END = os.getenv("YF_END", "2023-12-31")
YF_BATCH_SIZE = int(os.getenv("YF_BATCH_SIZE", "50"))  # lote de tickers por download

# para reduzir ruído do yfinance
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
_RUN_LOG = None


def log(msg: str, level: str = "INFO", **fields) -> None:
    if _RUN_LOG:
        _RUN_LOG.log(level, "pipeline_log", message=msg, **fields)
        return
    print(msg, flush=True)


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


def _normalize_ticker_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper()


def _assert_unique_key_ready(cur, table_name: str, key_columns: Tuple[str, ...]) -> None:
    cur.execute(
        """
        SELECT 1
        FROM pg_index i
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = %s
          AND i.indisunique
          AND (
              SELECT array_agg(a.attname::text ORDER BY x.ord)
              FROM unnest(i.indkey) WITH ORDINALITY AS x(attnum, ord)
              JOIN pg_attribute a
                ON a.attrelid = t.oid
               AND a.attnum = x.attnum
              WHERE x.attnum > 0
          ) = %s::text[]
        LIMIT 1
        """,
        (table_name, list(key_columns)),
    )
    if cur.fetchone() is None:
        raise RuntimeError(
            f'A tabela public."{table_name}" precisa de UNIQUE/PK em {key_columns} para ON CONFLICT.'
        )


# ======================
# DB READ (Supabase)
# ======================
def carregar_demonstracoes() -> pd.DataFrame:
    if not SUPABASE_DB_URL:
        raise RuntimeError("Defina SUPABASE_DB_URL com a connection string Postgres do Supabase.")

    engine = get_engine() if get_engine is not None else create_engine(SUPABASE_DB_URL)

    sql = text('SELECT * FROM public."Demonstracoes_Financeiras";')

    with engine.connect() as conn:
        df = pd.read_sql_query(sql, conn)

    # No banco físico a coluna pode estar como `data` minúsculo; no pandas
    # mantemos `Data` para compatibilidade com o restante do pipeline.
    if "data" in df.columns and "Data" not in df.columns:
        df = df.rename(columns={"data": "Data"})

    if validate_required_columns:
        validate_required_columns(
            df,
            ["Ticker", "Data", "Receita_Liquida", "Passivo_Circulante", "Ativo_Circulante", "LPA"],
            context="Demonstracoes_Financeiras para múltiplos DFP",
            logger=_RUN_LOG,
        )

    if "Dividendos" in df.columns:
        df["Dividendos"] = df["Dividendos"].astype(float)

    df["Ticker"] = _normalize_ticker_series(df["Ticker"])
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")

    # normaliza TZ
    if df["Data"].dt.tz is None:
        df["Data"] = df["Data"].dt.tz_localize("UTC")
    else:
        df["Data"] = df["Data"].dt.tz_convert("UTC")

    df["Ano"] = df["Data"].dt.year
    df["ticker_yf"] = df["Ticker"] + ".SA"

    df = df[df["Ticker"].notna() & df["Data"].notna()].copy()
    if validate_key_columns:
        validate_key_columns(
            df,
            ["Ticker", "Data"],
            context="Demonstracoes_Financeiras normalizado para múltiplos DFP",
            logger=_RUN_LOG,
        )
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
                log(
                    f"Yahoo sem dados para batch com {len(batch)} ticker(s).",
                    level="WARN",
                    stage="yf_empty_batch",
                    batch_size=len(batch),
                    batch_preview=batch[:5],
                )
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

        except Exception as e:
            if _RUN_LOG:
                _RUN_LOG.increment_metric("yf_batches_failed")
            log(
                f"Falha no download do Yahoo para batch com {len(batch)} ticker(s): {e}",
                level="ERROR",
                stage="yf_batch_failed",
                batch_size=len(batch),
                batch_preview=batch[:5],
            )
            continue

    if not rows:
        return pd.DataFrame(columns=["ticker_yf", "Ano", "Preco_Medio_Anual"])

    df_precos = pd.DataFrame(rows, columns=["ticker_yf", "Ano", "Preco_Medio_Anual"])
    df_precos["Ano"] = df_precos["Ano"].astype(int)
    if validate_unique_rows:
        validate_unique_rows(
            df_precos,
            ["ticker_yf", "Ano"],
            context="Preços médios anuais Yahoo",
            logger=_RUN_LOG,
        )

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
    if _RUN_LOG:
        _RUN_LOG.set_metric("preco_rows", len(df_precos))
        _RUN_LOG.set_metric("preco_tickers", int(df_precos["ticker_yf"].nunique() if not df_precos.empty else 0))
        _RUN_LOG.set_metric("preco_missing_rows", int(df["Preco_Medio_Anual"].isna().sum()))

    out = pd.DataFrame()
    out["Ticker"] = _normalize_ticker_series(df["Ticker"])
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
    if validate_key_columns:
        validate_key_columns(
            out,
            ["Ticker", "Data"],
            context="Múltiplos DFP calculados",
            logger=_RUN_LOG,
        )

    # ordenação opcional para facilitar debug
    out.sort_values(["Ticker", "Data"], inplace=True)
    if _RUN_LOG:
        _RUN_LOG.set_metric("multiplos_rows_gerados", len(out))
        _RUN_LOG.set_metric("multiplos_tickers_gerados", int(out["Ticker"].nunique() if not out.empty else 0))

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

    df_multiplos = df_multiplos.copy()
    df_multiplos["Ticker"] = _normalize_ticker_series(df_multiplos["Ticker"])
    df_multiplos["Data"] = pd.to_datetime(df_multiplos["Data"], errors="coerce", utc=True)
    if validate_key_columns:
        validate_key_columns(
            df_multiplos,
            ["Ticker", "Data"],
            context="Múltiplos DFP pré-upsert",
            logger=_RUN_LOG,
        )

    before_dedup = len(df_multiplos)
    df_multiplos = (
        df_multiplos.sort_values(["Ticker", "Data"])
        .drop_duplicates(subset=["Ticker", "Data"], keep="last")
        .reset_index(drop=True)
    )
    duplicates_removed = before_dedup - len(df_multiplos)
    if duplicates_removed > 0:
        log(
            f"Múltiplos DFP pré-upsert removeu {duplicates_removed} duplicata(s) por (Ticker, Data).",
            level="WARN",
            stage="pre_upsert_dedup",
            duplicates_removed=duplicates_removed,
        )
    if validate_unique_rows:
        validate_unique_rows(
            df_multiplos,
            ["Ticker", "Data"],
            context="Múltiplos DFP pré-upsert deduplicado",
            logger=_RUN_LOG,
        )

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
            _assert_unique_key_ready(cur, "multiplos", ("Ticker", "Data"))
            execute_values(cur, insert_sql, values, page_size=5000)
        conn.commit()

    log(f"UPSERT public.multiplos: {len(df_multiplos)} linhas processadas.", rows=len(df_multiplos))


def main() -> None:
    global _RUN_LOG
    if _IngestionLog:
        with _IngestionLog("multiplos_dfp") as run:
            _RUN_LOG = run
            run.set_params({"yf_start": YF_START, "yf_end": YF_END, "yf_batch_size": YF_BATCH_SIZE})
            log("Carregando Demonstracoes_Financeiras do Supabase...")
            df_demonstracoes = carregar_demonstracoes()
            run.set_metric("demonstracoes_rows", len(df_demonstracoes))
            run.set_metric("demonstracoes_tickers", int(df_demonstracoes["Ticker"].nunique() if not df_demonstracoes.empty else 0))
            log(f"Linhas de demonstrações: {len(df_demonstracoes)}")

            log("Calculando múltiplos (Algoritmo_4)...")
            df_multiplos = calcular_multiplos(df_demonstracoes)
            validate_required_columns(
                df_multiplos,
                ["Ticker", "Data", "Liquidez_Corrente", "P/L", "P/VP", "DY"],
                context="Múltiplos DFP calculados",
                logger=run,
            )
            if validate_key_columns:
                validate_key_columns(
                    df_multiplos,
                    ["Ticker", "Data"],
                    context="Múltiplos DFP calculados",
                    logger=run,
                )
            log(f"Linhas de múltiplos geradas: {len(df_multiplos)}")

            log("Gravando no Supabase (UPSERT)...")
            upsert_multiplos(df_multiplos)
            run.add_rows(inserted=len(df_multiplos))
            log("Rotina dados_multiplos_dfp concluída.")
    else:
        df_demonstracoes = carregar_demonstracoes()
        df_multiplos = calcular_multiplos(df_demonstracoes)
        upsert_multiplos(df_multiplos)
    _RUN_LOG = None


if __name__ == "__main__":
    main()
