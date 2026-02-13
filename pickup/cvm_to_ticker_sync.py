# pickup/cvm_to_ticker_sync.py
from __future__ import annotations

import io
import os
import re
import zipfile
from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd
import requests
from sqlalchemy import create_engine, text


# =========================
# CONFIG
# =========================
@dataclass(frozen=True)
class Config:
    supabase_db_url: str
    target_schema: str = "public"
    target_table: str = "cvm_to_ticker"

    # CVM: cadastro de companhias (traz CD_CVM + CNPJ)
    cvm_cadastro_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"

    # B3: arquivo que precisa conter TICKER + CNPJ do emissor (ou equivalente)
    # -> deixe por ENV para você plugar o link “oficial” que você usar
    b3_instrumentos_url: str = os.getenv("B3_INSTRUMENTOS_URL", "").strip()

    timeout_sec: int = 60


# =========================
# UTILS
# =========================
def _only_digits(s: str) -> str:
    return re.sub(r"\D+", "", str(s or ""))


def _cnpj_raiz(cnpj: str) -> str:
    d = _only_digits(cnpj)
    return d[:8] if len(d) >= 8 else ""


def _looks_like_equity_ticker(t: str) -> bool:
    t = (t or "").strip().upper()
    # padrões comuns B3 para ações/units: PETR4, VALE3, EGIE3, TAEE11 etc.
    return bool(re.fullmatch(r"[A-Z]{4}\d{1,2}", t))


def _pick_best_ticker(tickers: list[str]) -> Optional[str]:
    """
    Heurística para escolher UM ticker por CVM (mantendo compatibilidade com teu CSV atual).
    Preferência típica: ON(3) > PN(4) > UNIT(11) > outros.
    """
    if not tickers:
        return None
    uniq = sorted({t.strip().upper() for t in tickers if t and str(t).strip()})
    if not uniq:
        return None

    def score(t: str) -> tuple[int, str]:
        # menor score = melhor
        if t.endswith("3"):
            s = 0
        elif t.endswith("4"):
            s = 1
        elif t.endswith("11"):
            s = 2
        else:
            s = 9
        return (s, t)

    return sorted(uniq, key=score)[0]


def _download_bytes(url: str, timeout: int) -> bytes:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content


def _read_csv_or_zip(content: bytes) -> pd.DataFrame:
    """
    Aceita CSV puro ou ZIP com 1 CSV dentro.
    """
    # tenta zip
    if content[:4] == b"PK\x03\x04":
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            names = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if not names:
                raise ValueError("ZIP baixado não contém CSV.")
            # pega o primeiro CSV
            with z.open(names[0]) as f:
                data = f.read()
        content = data

    # tenta ler com ; e latin-1 (padrão BR), fallback para utf-8
    for (sep, enc) in [(";", "latin-1"), (";", "utf-8"), (",", "utf-8"), (",", "latin-1")]:
        try:
            return pd.read_csv(io.BytesIO(content), sep=sep, encoding=enc)
        except Exception:
            pass

    raise ValueError("Não consegui ler o arquivo como CSV (nem em ; nem em ,).")


def _find_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    cols = list(df.columns)
    norm = {c.lower().strip(): c for c in cols}
    for cand in candidates:
        key = cand.lower().strip()
        if key in norm:
            return norm[key]
    return None


# =========================
# LOADERS
# =========================
def _load_cvm_cadastro(cfg: Config) -> pd.DataFrame:
    content = _download_bytes(cfg.cvm_cadastro_url, cfg.timeout_sec)
    df = _read_csv_or_zip(content)

    col_cvm = _find_col(df, ["CD_CVM", "cd_cvm", "codigo_cvm", "cvm"])
    col_cnpj = _find_col(df, ["CNPJ_CIA", "cnpj_cia", "cnpj"])
    col_nome = _find_col(df, ["DENOM_SOCIAL", "denom_social", "nome_empresarial", "razao_social"])

    if not col_cvm or not col_cnpj:
        raise KeyError(
            f"[CVM] Não encontrei colunas CD_CVM/CNPJ. Colunas disponíveis: {list(df.columns)}"
        )

    out = pd.DataFrame({
        "cvm": pd.to_numeric(df[col_cvm], errors="coerce").astype("Int64"),
        "cnpj_raiz": df[col_cnpj].map(_cnpj_raiz),
        "nome_cvm": (df[col_nome].astype("string").str.strip() if col_nome else pd.Series([""] * len(df))),
    })

    out = out.dropna(subset=["cvm"])
    out = out[out["cnpj_raiz"].str.len() == 8]
    out = out.drop_duplicates(subset=["cvm"], keep="last").reset_index(drop=True)
    return out


def _load_b3_instrumentos(cfg: Config) -> pd.DataFrame:
    if not cfg.b3_instrumentos_url:
        raise EnvironmentError(
            "B3_INSTRUMENTOS_URL não definido. "
            "Defina esta env var com a URL do arquivo da B3 que contenha TICKER e CNPJ do emissor."
        )

    content = _download_bytes(cfg.b3_instrumentos_url, cfg.timeout_sec)
    df = _read_csv_or_zip(content)

    col_ticker = _find_col(df, ["TICKER", "ticker", "CODIGO_NEGOCIACAO", "codigo_negociacao", "cod_negociacao"])
    col_cnpj = _find_col(df, ["CNPJ_EMISSOR", "cnpj_emissor", "CNPJ", "cnpj"])
    col_nome = _find_col(df, ["NOME_EMISSOR", "nome_emissor", "NOME_EMPRESA", "nome_empresa", "EMISSOR", "emissor"])

    if not col_ticker or not col_cnpj:
        raise KeyError(
            f"[B3] Não encontrei colunas de ticker/CNPJ. Colunas disponíveis: {list(df.columns)}"
        )

    out = pd.DataFrame({
        "ticker": df[col_ticker].astype("string").str.strip().str.upper(),
        "cnpj_raiz": df[col_cnpj].map(_cnpj_raiz),
        "nome_b3": (df[col_nome].astype("string").str.strip() if col_nome else pd.Series([""] * len(df))),
    })

    out = out.dropna(subset=["ticker"])
    out = out[out["ticker"].map(_looks_like_equity_ticker)]
    out = out[out["cnpj_raiz"].str.len() == 8]
    out = out.drop_duplicates(subset=["ticker"], keep="last").reset_index(drop=True)
    return out


# =========================
# DB (DDL + UPSERT)
# =========================
def _ensure_table(engine, schema: str, table: str) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
      cvm        INTEGER PRIMARY KEY,
      ticker     TEXT NOT NULL,
      cnpj_raiz  TEXT,
      nome_cvm   TEXT,
      nome_b3    TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE UNIQUE INDEX IF NOT EXISTS {table}_ticker_uq
    ON {schema}.{table} (ticker);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _upsert(engine, schema: str, table: str, df: pd.DataFrame, chunk_size: int = 2000) -> int:
    sql = f"""
    INSERT INTO {schema}.{table} (cvm, ticker, cnpj_raiz, nome_cvm, nome_b3, updated_at)
    VALUES (:cvm, :ticker, :cnpj_raiz, :nome_cvm, :nome_b3, now())
    ON CONFLICT (cvm) DO UPDATE SET
      ticker     = EXCLUDED.ticker,
      cnpj_raiz  = EXCLUDED.cnpj_raiz,
      nome_cvm   = EXCLUDED.nome_cvm,
      nome_b3    = EXCLUDED.nome_b3,
      updated_at = now();
    """

    rows = df.to_dict(orient="records")
    total = 0
    with engine.begin() as conn:
        for i in range(0, len(rows), chunk_size):
            conn.execute(text(sql), rows[i : i + chunk_size])
            total += len(rows[i : i + chunk_size])
    return total


# =========================
# MAIN
# =========================
def main() -> None:
    supabase_db_url = os.getenv("SUPABASE_DB_URL", "").strip()
    if not supabase_db_url:
        raise EnvironmentError("SUPABASE_DB_URL não definida. Configure em Secrets/Env Vars.")

    cfg = Config(supabase_db_url=supabase_db_url)

    print("[cvm_to_ticker] Iniciando atualização CVM -> Ticker (B3)")
    print(f"[cvm_to_ticker] CVM cadastro: {cfg.cvm_cadastro_url}")
    print(f"[cvm_to_ticker] B3 instrumentos: {cfg.b3_instrumentos_url or '(ENV B3_INSTRUMENTOS_URL vazio)'}")

    df_cvm = _load_cvm_cadastro(cfg)
    print(f"[cvm_to_ticker] CVM: {len(df_cvm)} companhias com CNPJ raiz")

    df_b3 = _load_b3_instrumentos(cfg)
    print(f"[cvm_to_ticker] B3: {len(df_b3)} tickers com CNPJ raiz")

    # Join por CNPJ raiz
    merged = df_b3.merge(df_cvm, on="cnpj_raiz", how="inner")
    if merged.empty:
        raise RuntimeError(
            "Join B3 x CVM resultou em 0 linhas. "
            "Verifique se o arquivo da B3 realmente contém CNPJ do emissor (ou se o CNPJ está em outro campo)."
        )

    # Escolher 1 ticker por CVM (compatível com teu CSV atual)
    best = (
        merged.groupby("cvm", as_index=False)
        .agg(
            ticker=("ticker", lambda s: _pick_best_ticker(list(s))),
            cnpj_raiz=("cnpj_raiz", "first"),
            nome_cvm=("nome_cvm", "first"),
            nome_b3=("nome_b3", "first"),
        )
        .dropna(subset=["ticker"])
    )

    print(f"[cvm_to_ticker] Linhas finais (1 ticker por CVM): {len(best)}")
    print("[cvm_to_ticker] Amostra:")
    print(best.head(10).to_string(index=False))

    engine = create_engine(cfg.supabase_db_url, pool_pre_ping=True)
    _ensure_table(engine, cfg.target_schema, cfg.target_table)

    n = _upsert(engine, cfg.target_schema, cfg.target_table, best)
    print(f"[cvm_to_ticker] UPSERT concluído: {n} registros gravados/atualizados em {cfg.target_schema}.{cfg.target_table}")


if __name__ == "__main__":
    main()
