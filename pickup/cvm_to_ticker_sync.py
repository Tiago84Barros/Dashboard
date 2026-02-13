from __future__ import annotations

import io
import os
import re
import zipfile
from datetime import date, timedelta
from dataclasses import dataclass
from typing import Optional

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
    cvm_cadastro_url: str = "https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"
    b3_base_url: str = os.getenv(
        "B3_INSTRUMENTS_BASE",
        "https://arquivos.b3.com.br/tabelas/InstrumentsConsolidated"
    )
    timeout_sec: int = 20


# =========================
# UTILIDADES
# =========================

def _only_digits(s: str) -> str:
    return re.sub(r"\D+", "", str(s or ""))


def _cnpj_raiz(cnpj: str) -> str:
    d = _only_digits(cnpj)
    return d[:8] if len(d) >= 8 else ""


def _looks_like_equity_ticker(t: str) -> bool:
    t = (t or "").strip().upper()
    return bool(re.fullmatch(r"[A-Z]{4}\d{1,2}", t))


def _pick_best_ticker(tickers: list[str]) -> Optional[str]:
    if not tickers:
        return None

    uniq = sorted(set(tickers))

    def score(t: str):
        if t.endswith("3"):
            return (0, t)
        elif t.endswith("4"):
            return (1, t)
        elif t.endswith("11"):
            return (2, t)
        else:
            return (9, t)

    return sorted(uniq, key=score)[0]


def _download_bytes(url: str, timeout: int) -> bytes:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content


def _read_csv_or_zip(content: bytes) -> pd.DataFrame:
    if content[:4] == b"PK\x03\x04":
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            names = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if not names:
                raise ValueError("ZIP sem CSV.")
            with z.open(names[0]) as f:
                content = f.read()

    for sep in [";", ","]:
        try:
            return pd.read_csv(io.BytesIO(content), sep=sep, encoding="latin-1")
        except Exception:
            continue

    raise ValueError("Falha ao ler CSV da B3.")


# =========================
# B3 - BUSCA DATA MAIS RECENTE
# =========================

def _get_latest_b3_url(base_url: str, max_days_back: int = 10) -> str:
    for i in range(max_days_back):
        d = (date.today() - timedelta(days=i)).strftime("%Y-%m-%d")
        url = f"{base_url}/{d}?lang=pt"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                print(f"[B3] Arquivo encontrado: {d}")
                return url
        except Exception:
            continue

    raise RuntimeError("Nenhum arquivo recente encontrado na B3.")


# =========================
# LOADERS
# =========================

def _load_cvm(cfg: Config) -> pd.DataFrame:
    print("[CVM] Baixando cadastro...")
    content = _download_bytes(cfg.cvm_cadastro_url, cfg.timeout_sec)
    df = pd.read_csv(io.BytesIO(content), sep=";", encoding="latin-1")

    df = df.rename(columns={
        "CD_CVM": "cvm",
        "CNPJ_CIA": "cnpj",
        "DENOM_SOCIAL": "nome"
    })

    df["cnpj_raiz"] = df["cnpj"].map(_cnpj_raiz)
    df = df.dropna(subset=["cvm", "cnpj_raiz"])
    df = df[df["cnpj_raiz"].str.len() == 8]

    return df[["cvm", "cnpj_raiz", "nome"]].drop_duplicates("cvm")


def _load_b3(cfg: Config) -> pd.DataFrame:
    print("[B3] Buscando arquivo consolidado...")

    # 1️⃣ Descobrir a data mais recente válida
    for i in range(10):
        d = (date.today() - timedelta(days=i)).strftime("%Y-%m-%d")
        meta_url = f"{cfg.b3_base_url}/{d}?lang=pt"

        try:
            r = requests.get(meta_url, timeout=10)
            if r.status_code == 200:
                meta = r.json()
                file_name = meta.get("fileName")
                if file_name:
                    print(f"[B3] Arquivo encontrado: {file_name}")
                    break
        except Exception:
            continue
    else:
        raise RuntimeError("Nenhum arquivo recente encontrado na B3.")

    # 2️⃣ Baixar o CSV real
    file_url = f"https://arquivos.b3.com.br/{file_name}"
    content = _download_bytes(file_url, cfg.timeout_sec)
    df = _read_csv_or_zip(content)

    print("[B3] Colunas disponíveis:", df.columns.tolist())

    # 3️⃣ Ajuste dinâmico de colunas
    possible_ticker = ["Ticker", "TICKER", "Código de Negociação", "CODIGO_NEGOCIACAO"]
    possible_cnpj = ["CNPJ Emissor", "CNPJ_EMISSOR", "CNPJ"]

    col_ticker = next((c for c in possible_ticker if c in df.columns), None)
    col_cnpj = next((c for c in possible_cnpj if c in df.columns), None)

    if not col_ticker or not col_cnpj:
        raise KeyError(f"Colunas não encontradas. Disponíveis: {df.columns.tolist()}")

    df["ticker"] = df[col_ticker].astype(str).str.strip().str.upper()
    df["cnpj_raiz"] = df[col_cnpj].map(_cnpj_raiz)

    df = df[df["ticker"].map(_looks_like_equity_ticker)]
    df = df[df["cnpj_raiz"].str.len() == 8]

    return df[["ticker", "cnpj_raiz"]].drop_duplicates()

# =========================
# DB
# =========================

def _ensure_table(engine, schema: str, table: str):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        cvm INTEGER PRIMARY KEY,
        ticker TEXT NOT NULL,
        cnpj_raiz TEXT,
        nome_cvm TEXT,
        updated_at TIMESTAMPTZ DEFAULT now()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _upsert(engine, schema: str, table: str, df: pd.DataFrame):
    sql = f"""
    INSERT INTO {schema}.{table} (cvm, ticker, cnpj_raiz, nome_cvm, updated_at)
    VALUES (:cvm, :ticker, :cnpj_raiz, :nome_cvm, now())
    ON CONFLICT (cvm) DO UPDATE SET
        ticker = EXCLUDED.ticker,
        cnpj_raiz = EXCLUDED.cnpj_raiz,
        nome_cvm = EXCLUDED.nome_cvm,
        updated_at = now();
    """

    with engine.begin() as conn:
        conn.execute(text(sql), df.to_dict(orient="records"))


# =========================
# MAIN
# =========================

def main():
    db_url = os.getenv("SUPABASE_DB_URL", "").strip()
    if not db_url:
        raise EnvironmentError("SUPABASE_DB_URL não definida.")

    cfg = Config(supabase_db_url=db_url)

    df_cvm = _load_cvm(cfg)
    print(f"[CVM] {len(df_cvm)} companhias válidas.")

    df_b3 = _load_b3(cfg)
    print(f"[B3] {len(df_b3)} tickers válidos.")

    merged = df_b3.merge(df_cvm, on="cnpj_raiz", how="inner")

    if merged.empty:
        raise RuntimeError("Join resultou vazio. Abortando atualização.")

    final = (
        merged.groupby("cvm", as_index=False)
        .agg(
            ticker=("ticker", lambda x: _pick_best_ticker(list(x))),
            cnpj_raiz=("cnpj_raiz", "first"),
            nome_cvm=("nome", "first"),
        )
        .dropna(subset=["ticker"])
    )

    print(f"[JOIN] {len(final)} registros finais.")

    engine = create_engine(cfg.supabase_db_url, pool_pre_ping=True)
    _ensure_table(engine, cfg.target_schema, cfg.target_table)
    _upsert(engine, cfg.target_schema, cfg.target_table, final)

    print("[OK] Atualização concluída com sucesso.")


if __name__ == "__main__":
    main()
