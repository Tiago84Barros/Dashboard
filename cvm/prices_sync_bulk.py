# cvm/prices_sync_bulk.py
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable, Optional, Dict, Any, List

import pandas as pd
import yfinance as yf
from sqlalchemy import text
from sqlalchemy.engine import Engine


# -----------------------------
# Config
# -----------------------------
DEFAULT_START = "2010-01-01"


@dataclass
class SyncStats:
    total: int = 0
    ok: int = 0
    fail: int = 0
    empty: int = 0
    rows_inserted: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "total": self.total,
            "ok": self.ok,
            "fail": self.fail,
            "empty": self.empty,
            "rows_inserted": self.rows_inserted,
        }


# -----------------------------
# Helpers
# -----------------------------
def _norm_ticker(t: str) -> str:
    t = (t or "").strip().upper()
    return t.replace(".SA", "")


def _ensure_prices_table(engine: Engine, table: str) -> None:
    """
    Opcional: cria tabela se não existir.
    Se você já criou a tabela no Supabase, pode manter isso sem problemas.
    """
    schema, _, name = table.partition(".")
    if not name:
        schema, name = "public", schema  # caso sem schema

    ddl = f"""
    create table if not exists {schema}.{name} (
        ticker text not null,
        date date not null,
        close double precision,
        fetched_at timestamptz not null default now(),
        primary key (ticker, date)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _normalize_prices_df(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza o DataFrame do yfinance para conter:
    - date (date)
    - close (float)
    Lida com:
    - MultiIndex
    - colunas 'Close'/'Adj Close' (caso não tenha 'Close')
    - DataFrame vazio
    """
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "close"])

    df = raw.copy()

    # yfinance às vezes devolve MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        # geralmente nível 0 é OHLCV
        df.columns = df.columns.get_level_values(0)

    # Normaliza nomes
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Reset index para trazer a data como coluna
    df = df.reset_index()

    # Nome da coluna de data pode variar: Date / Datetime / index
    # Após reset_index, normalmente é 'Date' ou o nome do índice
    cols_lower = [str(c).strip().lower() for c in df.columns]
    df.columns = cols_lower

    # achar coluna de data
    date_col = None
    for candidate in ("date", "datetime", "index"):
        if candidate in df.columns:
            date_col = candidate
            break
    if date_col is None:
        # assume primeira coluna é data
        date_col = df.columns[0]

    # escolher coluna de preço de fechamento
    if "close" in df.columns:
        close_col = "close"
    elif "adj close" in df.columns:
        # em alguns ativos o Yahoo devolve apenas Adj Close
        close_col = "adj close"
    elif "adj_close" in df.columns:
        close_col = "adj_close"
    else:
        # não veio coluna de fechamento em nenhum formato reconhecido
        raise ValueError("Retorno do Yahoo sem coluna de fechamento (close/adj close).")

    out = df[[date_col, close_col]].rename(columns={date_col: "date", close_col: "close"})

    # converte date
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out = out.dropna(subset=["date"])

    # converte close
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["close"])

    return out.reset_index(drop=True)


def _download_prices_yf(
    ticker_sa: str,
    start: str,
    end: Optional[str],
    retries: int,
    pause_s: float,
) -> pd.DataFrame:
    """
    Download robusto com retry/backoff.
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            raw = yf.download(
                ticker_sa,
                start=start,
                end=end,
                progress=False,
                auto_adjust=False,
                threads=False,
                group_by="column",
            )
            return _normalize_prices_df(raw)
        except Exception as e:
            last_err = e
            # backoff simples
            time.sleep(pause_s * attempt)
    raise last_err  # type: ignore[misc]


def _upsert_prices(engine: Engine, table: str, ticker: str, df: pd.DataFrame) -> int:
    """
    Faz UPSERT por (ticker, date).
    Espera tabela com PK (ticker,date) e coluna close.
    """
    if df.empty:
        return 0

    schema, _, name = table.partition(".")
    if not name:
        schema, name = "public", schema

    # Monta payload
    payload = [
        {"ticker": ticker, "date": r["date"], "close": float(r["close"])}
        for r in df.to_dict("records")
    ]

    sql = text(
        f"""
        insert into {schema}.{name} (ticker, date, close, fetched_at)
        values (:ticker, :date, :close, now())
        on conflict (ticker, date)
        do update set
            close = excluded.close,
            fetched_at = excluded.fetched_at
        """
    )

    with engine.begin() as conn:
        conn.execute(sql, payload)

    return len(payload)


# -----------------------------
# Public API
# -----------------------------
def sync_prices_universe(
    engine: Engine,
    tickers: Iterable[str],
    *,
    start: str = DEFAULT_START,
    end: Optional[str] = None,
    table: str = "cvm.prices_b3",
    retries: int = 3,
    pause_s: float = 0.7,
    per_ticker_sleep_s: float = 0.15,
) -> Dict[str, Any]:
    """
    Sincroniza preços (2010→hoje, por padrão) para o universo.
    - Não aborta o job em caso de falha em um ticker.
    - Retorna estatísticas.
    """
    tickers_list: List[str] = [_norm_ticker(t) for t in tickers if str(t).strip()]
    tickers_list = sorted(set(tickers_list))

    stats = SyncStats(total=len(tickers_list))

    # garante tabela
    _ensure_prices_table(engine, table)

    for t in tickers_list:
        ticker_sa = f"{t}.SA"
        try:
            df = _download_prices_yf(
                ticker_sa=ticker_sa,
                start=start,
                end=end,
                retries=retries,
                pause_s=pause_s,
            )

            if df.empty:
                stats.empty += 1
                continue

            n = _upsert_prices(engine, table, t, df)
            stats.rows_inserted += n
            stats.ok += 1

        except Exception:
            stats.fail += 1
        finally:
            # evita rate limit
            if per_ticker_sleep_s > 0:
                time.sleep(per_ticker_sleep_s)

    return stats.as_dict()
