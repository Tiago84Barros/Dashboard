# pickup/dados_multiplos_itr.py
from __future__ import annotations

import os
import re
import logging
from datetime import timezone
from typing import Dict, Optional, List

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
import yfinance as yf

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

# =========================
# Silenciar ruído do yfinance
# =========================
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# =========================
# Config
# =========================
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
if not SUPABASE_DB_URL:
    raise RuntimeError("SUPABASE_DB_URL não definida")

ENGINE = sa.create_engine(SUPABASE_DB_URL)

ORIGEM_TRI = 'public."Demonstracoes_Financeiras_TRI"'
DEST_SCHEMA = "public"
DEST_TABLE = "multiplos_TRI"  # DB: public."multiplos_TRI"

YF_BATCH_SIZE = int(os.getenv("YF_BATCH_SIZE", "50"))
YF_MAX_TICKERS = int(os.getenv("YF_MAX_TICKERS", "0"))  # 0 = sem limite
SKIP_PRICE = os.getenv("SKIP_PRICE", "0") == "1"
_RUN_LOG = None


def log(msg: str, level: str = "INFO", **fields) -> None:
    if _RUN_LOG:
        _RUN_LOG.log(level, "pipeline_log", message=msg, **fields)
        return
    print(msg, flush=True)


def _normalize_ticker_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper()


def ticker_valido_yf(t: str) -> bool:
    """
    Aceita padrões comuns B3:
      - 4 letras + 1 dígito (PETR4)
      - 4 letras + 2 dígitos (units: BPAC11, KLBN11)
      - BDRs e tickers com 2 dígitos finais (ex.: NUBR33)
    """
    t = (t or "").strip().upper()
    return bool(re.fullmatch(r"[A-Z]{4}\d{1,2}", t))

def cap(x, lo, hi):
    if x is None:
        return None
    try:
        if np.isnan(x) or np.isinf(x):
            return None
    except Exception:
        pass
    return float(min(max(x, lo), hi))


def to_utc_midnight_timestamptz(d: pd.Timestamp) -> pd.Timestamp:
    """
    Origem TRI: date
    Destino multiplos_TRI: timestamptz
    Grava como 00:00:00 UTC (normalize).
    """
    ts = pd.to_datetime(d, errors="coerce")
    if pd.isna(ts):
        return ts
    # Garantir tz=UTC
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    else:
        ts = ts.tz_convert(timezone.utc)
    return ts.normalize()


def rolling_ttm(series: pd.Series) -> pd.Series:
    # TRI já é trimestral isolado -> TTM = soma últimos 4 TRI
    return series.rolling(4, min_periods=4).sum()


def _to_yf_symbol(ticker: str) -> str:
    return f"{ticker}.SA"


def baixar_precos_quarter_mean(
    tickers: List[str],
    date_min: pd.Timestamp,
    date_max: pd.Timestamp,
    batch_size: int = 50,
) -> pd.DataFrame:
    """
    Retorna DataFrame com colunas:
      - Ticker (sem .SA)
      - Data (quarter-end, timestamptz UTC midnight)
      - Preco_Medio_TRIM (média do Close no trimestre)

    Observação:
      - resample("Q") gera datas de quarter-end.
      - Para casar com TRI, vamos mapear TRI->quarter-end no loop.
    """
    if not tickers:
        return pd.DataFrame(columns=["Ticker", "Data", "Preco_Medio_TRIM"])

    # janela um pouco mais ampla
    start = (pd.to_datetime(date_min) - pd.DateOffset(months=4)).date()
    end = (pd.to_datetime(date_max) + pd.DateOffset(days=10)).date()

    out = []
    total = len(tickers)

    for i in range(0, total, batch_size):
        batch = tickers[i : i + batch_size]
        symbols = [_to_yf_symbol(t) for t in batch]

        log(f"[YF] Baixando preços {i+1}-{min(i+batch_size, total)}/{total} ({start} → {end}) ...")

        try:
            dfp = yf.download(
                tickers=symbols,
                start=start,
                end=end,
                group_by="ticker",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
        except Exception as e:
            if _RUN_LOG:
                _RUN_LOG.increment_metric("yf_batches_failed")
            log(
                f"Falha no download Yahoo para batch {i+1}-{min(i+batch_size, total)}: {e}",
                level="ERROR",
                stage="yf_batch_failed",
                batch_preview=batch[:5],
                batch_size=len(batch),
            )
            continue

        if dfp is None or dfp.empty:
            log(
                f"Yahoo sem dados para batch {i+1}-{min(i+batch_size, total)}.",
                level="WARN",
                stage="yf_empty_batch",
                batch_preview=batch[:5],
                batch_size=len(batch),
            )
            continue

        # Extrair Close de forma robusta
        close = None
        if isinstance(dfp.columns, pd.MultiIndex):
            # comum: (ticker, field)
            if "Close" in dfp.columns.get_level_values(-1):
                close = dfp.xs("Close", axis=1, level=-1)
            # alternativo: (field, ticker)
            elif "Close" in dfp.columns.get_level_values(0):
                close = dfp.xs("Close", axis=1, level=0)
        else:
            # caso de 1 ticker com colunas simples
            if "Close" in dfp.columns and len(symbols) == 1:
                close = dfp[["Close"]].rename(columns={"Close": symbols[0]})

        if close is None or close.empty:
            continue

        close.index = pd.to_datetime(close.index)

        # preço médio por trimestre
        qmean = close.resample("QE").mean()

        # long format
        for sym in qmean.columns:
            tkr = sym.replace(".SA", "")
            tmp = qmean[[sym]].reset_index()
            if "Date" in tmp.columns:
                tmp = tmp.rename(columns={"Date": "Data"})
            elif "index" in tmp.columns:
                tmp = tmp.rename(columns={"index": "Data"})
            tmp = tmp.rename(columns={sym: "Preco_Medio_TRIM"})
            tmp["Ticker"] = tkr
            out.append(tmp[["Ticker", "Data", "Preco_Medio_TRIM"]])

    if not out:
        return pd.DataFrame(columns=["Ticker", "Data", "Preco_Medio_TRIM"])

    df_preco = pd.concat(out, ignore_index=True)
    df_preco["Data"] = pd.to_datetime(df_preco["Data"], utc=True).dt.normalize()
    df_preco["Preco_Medio_TRIM"] = pd.to_numeric(df_preco["Preco_Medio_TRIM"], errors="coerce")
    df_preco = df_preco.dropna(subset=["Ticker", "Data", "Preco_Medio_TRIM"])
    df_preco = df_preco[df_preco["Preco_Medio_TRIM"] > 0]
    df_preco["Ticker"] = _normalize_ticker_series(df_preco["Ticker"])
    if validate_unique_rows:
        validate_unique_rows(
            df_preco,
            ["Ticker", "Data"],
            context="Preços médios trimestrais Yahoo",
            logger=_RUN_LOG,
        )
    return df_preco


def shares_outstanding(ticker: str) -> Optional[float]:
    """
    Para P/VP via MarketCap/PL.
    Nem sempre existe para B3 no Yahoo.
    """
    try:
        t = yf.Ticker(_to_yf_symbol(ticker))
        info = getattr(t, "info", {}) or {}
        so = info.get("sharesOutstanding")
        if so and so > 0:
            return float(so)
    except Exception:
        if _RUN_LOG:
            _RUN_LOG.increment_metric("shares_lookup_failed")
        pass
    return None


def garantir_unique_index() -> None:
    with ENGINE.begin() as conn:
        conn.execute(sa.text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_multiplos_tri_ticker_data
            ON public."multiplos_TRI" ("Ticker","Data");
        """))


def upsert_multiplos(df_out: pd.DataFrame) -> None:
    if df_out.empty:
        log("[WARN] df_out vazio — nada para gravar.")
        return

    df_out = df_out.copy()
    df_out["Ticker"] = _normalize_ticker_series(df_out["Ticker"])
    df_out["Data"] = pd.to_datetime(df_out["Data"], errors="coerce", utc=True)
    if validate_key_columns:
        validate_key_columns(
            df_out,
            ["Ticker", "Data"],
            context='Múltiplos TRI pré-upsert',
            logger=_RUN_LOG,
        )

    before_dedup = len(df_out)
    df_out = (
        df_out.sort_values(["Ticker", "Data"])
        .drop_duplicates(subset=["Ticker", "Data"], keep="last")
        .reset_index(drop=True)
    )
    duplicates_removed = before_dedup - len(df_out)
    if duplicates_removed > 0:
        log(
            f"Múltiplos TRI pré-upsert removeu {duplicates_removed} duplicata(s) por (Ticker, Data).",
            level="WARN",
            stage="pre_upsert_dedup",
            duplicates_removed=duplicates_removed,
        )
    if validate_unique_rows:
        validate_unique_rows(
            df_out,
            ["Ticker", "Data"],
            context='Múltiplos TRI pré-upsert deduplicado',
            logger=_RUN_LOG,
        )

    meta = sa.MetaData()
    table = sa.Table(DEST_TABLE, meta, schema=DEST_SCHEMA, autoload_with=ENGINE)

    records = df_out.to_dict(orient="records")
    stmt = insert(table).values(records)

    key_cols = ["Ticker", "Data"]
    payload_cols = set(df_out.columns)
    update_cols = [c.name for c in table.columns if c.name not in key_cols and c.name in payload_cols]

    stmt = stmt.on_conflict_do_update(
        index_elements=[table.c["Ticker"], table.c["Data"]],
        set_={c: getattr(stmt.excluded, c) for c in update_cols},
    )

    with ENGINE.begin() as conn:
        conn.execute(stmt)

    log(f"[OK] UPSERT concluído: {len(df_out)} linhas em public.\"multiplos_TRI\".")


def main() -> None:
    global _RUN_LOG
    if _IngestionLog:
        with _IngestionLog("multiplos_tri") as run:
            _RUN_LOG = run
            run.set_params({"yf_batch_size": YF_BATCH_SIZE, "yf_max_tickers": YF_MAX_TICKERS, "skip_price": SKIP_PRICE})
            _main_impl(run)
    else:
        _main_impl(None)
    _RUN_LOG = None


def _main_impl(run) -> None:
    log("[INFO] Lendo Demonstracoes_Financeiras_TRI do Supabase...")
    df = pd.read_sql(f"SELECT * FROM {ORIGEM_TRI}", ENGINE)

    if run:
        run.set_metric("tri_source_rows", len(df))
    if df.empty:
        if run:
            run.add_warning("Origem TRI vazia.")
        log("[WARN] Origem TRI vazia.")
        return

    # =========================================================
    # NORMALIZAÇÃO CRÍTICA DO TICKER (antes de qualquer lógica)
    # =========================================================
    df["Ticker"] = _normalize_ticker_series(df["Ticker"])

    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df = df.dropna(subset=["Ticker", "Data"]).sort_values(["Ticker", "Data"])
    if validate_key_columns:
        validate_key_columns(
            df,
            ["Ticker", "Data"],
            context="Origem Demonstracoes_Financeiras_TRI normalizada para múltiplos TRI",
            logger=run,
        )

    # Validar colunas conforme seu DDL
    required = [
        "Receita_Liquida", "EBIT", "Lucro_Liquido", "Dividendos", "LPA",
        "Ativo_Total", "Ativo_Circulante", "Passivo_Circulante", "Passivo_Total",
        "Patrimonio_Liquido", "Divida_Liquida",
    ]
    if validate_required_columns:
        validate_required_columns(
            df,
            ["Ticker", "Data"] + required,
            context="Origem Demonstracoes_Financeiras_TRI para múltiplos TRI",
            logger=run,
        )

    # Lista de tickers (com filtro para Yahoo)
    tickers_all = sorted(df["Ticker"].unique().tolist())
    tickers_ok = [t for t in tickers_all if ticker_valido_yf(t)]
    tickers_skip = [t for t in tickers_all if t not in set(tickers_ok)]

    if tickers_skip:
        log(f"[WARN] {len(tickers_skip)} tickers fora do padrão Yahoo foram ignorados para preço (ex.: {tickers_skip[:10]}...).")

    if YF_MAX_TICKERS and YF_MAX_TICKERS > 0:
        tickers_ok = tickers_ok[:YF_MAX_TICKERS]
        df = df[df["Ticker"].isin(tickers_ok + tickers_skip)].copy()
        log(f"[INFO] YF_MAX_TICKERS ativo: preços só para {len(tickers_ok)} tickers.")

    date_min = pd.to_datetime(df["Data"].min())
    date_max = pd.to_datetime(df["Data"].max())
    log(f"[INFO] TRI tickers total: {len(tickers_all)} | tickers p/ preço: {len(tickers_ok)} | período: {date_min.date()} → {date_max.date()}")

    # Baixar preços em lote
    df_preco = pd.DataFrame(columns=["Ticker", "Data", "Preco_Medio_TRIM"])
    if not SKIP_PRICE and tickers_ok:
        log("[INFO] Baixando preços trimestrais via yfinance (batch)...")
        df_preco = baixar_precos_quarter_mean(tickers_ok, date_min, date_max, batch_size=YF_BATCH_SIZE)
        log(f"[INFO] Preços retornados: {len(df_preco)} linhas (Ticker×Quarter).")
    elif SKIP_PRICE:
        log("[INFO] SKIP_PRICE=1 ativo: DY, P/L e P/VP ficarão NULL.")
    else:
        log("[WARN] Nenhum ticker elegível para yfinance. DY, P/L e P/VP ficarão NULL.")

    # Cache de shares outstanding (somente tickers elegíveis)
    shares_cache: Dict[str, Optional[float]] = {}

    resultados: list[dict] = []
    total_tickers = df["Ticker"].nunique()
    if run:
        run.set_metric("tri_tickers_total", total_tickers)
        run.set_metric("tri_tickers_price_eligible", len(tickers_ok))

    for idx, (ticker, g) in enumerate(df.groupby("Ticker", sort=False), start=1):
        if idx == 1 or idx % 10 == 0:
            log(f"[PROG] Processando ticker {idx}/{total_tickers}: {ticker}")

        g = g.sort_values("Data").copy()

        # TTM fluxos
        g["Receita_12M"] = rolling_ttm(g["Receita_Liquida"])
        g["EBIT_12M"] = rolling_ttm(g["EBIT"])
        g["Lucro_12M"] = rolling_ttm(g["Lucro_Liquido"])
        g["Dividendos_12M"] = rolling_ttm(g["Dividendos"])
        g["LPA_12M"] = rolling_ttm(g["LPA"])

        g_ttm = g.dropna(subset=["Receita_12M", "EBIT_12M", "Lucro_12M", "Dividendos_12M", "LPA_12M"])
        if g_ttm.empty:
            continue

        # shares outstanding (uma vez por ticker) — só se for elegível e se usarmos preço
        if (not SKIP_PRICE) and ticker_valido_yf(ticker) and (ticker not in shares_cache):
            shares_cache[ticker] = shares_outstanding(ticker)

        for _, row in g_ttm.iterrows():
            data_tri = pd.Timestamp(row["Data"])

            # -------- lookup do preço trimestral (quarter-end)
            px = None
            if (not SKIP_PRICE) and (not df_preco.empty) and ticker_valido_yf(ticker):
                # TRI date -> quarter-end
                q_end = pd.to_datetime(data_tri).to_period("Q").end_time
                q_key = pd.to_datetime(q_end, utc=True).normalize()

                hit = df_preco[(df_preco["Ticker"] == ticker) & (df_preco["Data"] == q_key)]
                if not hit.empty:
                    # evitar warnings future: usar iloc[0]
                    px = float(hit["Preco_Medio_TRIM"].iloc[0])

            # Estoques (último TRI)
            ativo = float(row["Ativo_Total"]) if row["Ativo_Total"] is not None else None
            ativo_c = float(row["Ativo_Circulante"]) if row["Ativo_Circulante"] is not None else None
            passivo = float(row["Passivo_Total"]) if row["Passivo_Total"] is not None else None
            passivo_c = float(row["Passivo_Circulante"]) if row["Passivo_Circulante"] is not None else None
            pl = float(row["Patrimonio_Liquido"]) if row["Patrimonio_Liquido"] is not None else None
            divliq = float(row["Divida_Liquida"]) if row["Divida_Liquida"] is not None else None

            # Fluxos (TTM)
            receita = float(row["Receita_12M"]) if row["Receita_12M"] is not None else None
            ebit = float(row["EBIT_12M"]) if row["EBIT_12M"] is not None else None
            lucro = float(row["Lucro_12M"]) if row["Lucro_12M"] is not None else None
            div = float(row["Dividendos_12M"]) if row["Dividendos_12M"] is not None else None
            lpa = float(row["LPA_12M"]) if row["LPA_12M"] is not None else None

            # Contábeis
            liquidez = (ativo_c / passivo_c) if (ativo_c is not None and passivo_c not in (None, 0)) else None
            endiv = (passivo / ativo) if (passivo is not None and ativo not in (None, 0)) else None
            alav = (divliq / pl) if (divliq is not None and pl not in (None, 0)) else None

            margem_op = (ebit / receita) if (ebit is not None and receita not in (None, 0)) else None
            margem_liq = (lucro / receita) if (lucro is not None and receita not in (None, 0)) else None

            roe = (lucro / pl) if (lucro is not None and pl not in (None, 0)) else None
            roa = (lucro / ativo) if (lucro is not None and ativo not in (None, 0)) else None

            base_roic = (ativo - passivo_c) if (ativo is not None and passivo_c is not None) else None
            roic = (ebit / base_roic) if (ebit is not None and base_roic not in (None, 0)) else None

            # Com preço (se não tiver, ficam NULL)
            dy = (div / px) if (px not in (None, 0) and div is not None) else None
            pl_mult = (px / lpa) if (px not in (None, 0) and lpa not in (None, 0)) else None

            pvp = None
            if (px not in (None, 0)) and (pl not in (None, 0)) and (not SKIP_PRICE) and ticker_valido_yf(ticker):
                so = shares_cache.get(ticker)
                if so and so > 0:
                    market_cap = px * so
                    pvp = market_cap / pl

            payout = (div / lucro) if (div is not None and lucro not in (None, 0)) else None

            # =========================
            # CAP / WINSORIZATION (ANTI-OUTLIER)
            # =========================
            dy = cap(dy, 0.0, 0.30)            # 0% a 30% a.a.
            payout = cap(payout, 0.0, 2.0)     # 0% a 200%
            pl_mult = cap(pl_mult, -200.0, 200.0)

            resultados.append({
                "Ticker": ticker,
                "Data": to_utc_midnight_timestamptz(data_tri),  # timestamptz 00:00Z
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
        if run:
            run.add_warning("Nenhuma linha gerada (TTM incompleto ou sem dados).")
        log("[WARN] Nenhuma linha gerada (TTM incompleto ou sem dados).")
        return

    log(f"[INFO] Linhas geradas: {len(df_out)} | Tickers: {df_out['Ticker'].nunique()}")
    if run:
        run.set_metric("multiplos_tri_rows", len(df_out))
        run.set_metric("multiplos_tri_tickers", int(df_out["Ticker"].nunique()))
    df_out["Ticker"] = _normalize_ticker_series(df_out["Ticker"])
    df_out["Data"] = pd.to_datetime(df_out["Data"], errors="coerce", utc=True)
    if validate_required_columns:
        validate_required_columns(
            df_out,
            ["Ticker", "Data", "Liquidez_Corrente", "P/L", "P/VP", "DY"],
            context="Múltiplos TRI calculados",
            logger=run,
        )
    if validate_key_columns:
        validate_key_columns(
            df_out,
            ["Ticker", "Data"],
            context="Múltiplos TRI calculados",
            logger=run,
        )

    # Sanity checks
    for col in ["Liquidez_Corrente", "Endividamento_Total", "ROE", "P/L", "DY"]:
        if col in df_out.columns:
            s = pd.to_numeric(df_out[col], errors="coerce").dropna()
            if not s.empty:
                log(f"[CHECK] {col}: p1={s.quantile(0.01):.6f} med={s.median():.6f} p99={s.quantile(0.99):.6f}")

    log("[INFO] Garantindo UNIQUE INDEX para UPSERT...")
    garantir_unique_index()

    log("[INFO] Gravando em public.\"multiplos_TRI\" via UPSERT (Ticker, Data)...")
    upsert_multiplos(df_out)
    if run:
        run.add_rows(inserted=len(df_out))

    log("[DONE] Rotina concluída com sucesso.")


if __name__ == "__main__":
    main()
