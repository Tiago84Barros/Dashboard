# pickup/cvm_to_ticker_sync.py
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text


@dataclass(frozen=True)
class Config:
    supabase_db_url: str
    target_schema: str = "public"
    target_table: str = "cvm_to_ticker"


# -------------------------
# Helpers
# -------------------------
def _only_digits(s: str) -> str:
    return re.sub(r"\D+", "", str(s or ""))


def _cnpj_raiz(cnpj: str) -> str:
    d = _only_digits(cnpj)
    return d[:8] if len(d) >= 8 else ""


def _looks_like_equity_ticker(t: str) -> bool:
    t = (t or "").strip().upper()
    return bool(re.fullmatch(r"[A-Z]{4}\d{1,2}", t))


def _pick_best_ticker(tickers: list[str]) -> Optional[str]:
    """
    Escolhe 1 ticker por CVM para manter compatibilidade com seu CSV atual.
    Preferência: ON(3) > PN(4) > UNIT(11) > demais.
    """
    uniq = sorted({(t or "").strip().upper() for t in tickers if str(t or "").strip()})
    if not uniq:
        return None

    def score(t: str) -> tuple[int, str]:
        if t.endswith("3"):
            return (0, t)
        if t.endswith("4"):
            return (1, t)
        if t.endswith("11"):
            return (2, t)
        return (9, t)

    return sorted(uniq, key=score)[0]


# -------------------------
# DB
# -------------------------
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


# -------------------------
# Main routine
# -------------------------
def main() -> None:
    db_url = os.getenv("SUPABASE_DB_URL", "").strip()
    if not db_url:
        raise EnvironmentError("SUPABASE_DB_URL não definida no ambiente/secrets.")

    cfg = Config(supabase_db_url=db_url)

    # Import local (para falhar com mensagem clara se faltar dependencia)
    try:
        from tradingcomdados import b3, cvm  # type: ignore
    except Exception as e:
        raise ImportError(
            "Dependência 'tradingcomdados' não instalada. "
            "Adicione 'tradingcomdados==1.4.10' ao requirements e redeploy."
        ) from e

    print("[cvm_to_ticker] Baixando ativos B3 via tradingcomdados...")
    df_ativos = b3.get_assets_list()

    # Tentativa de padronizar nomes de colunas comuns
    # Seu exemplo usa: ticker, name, cnpj, segment
    required_cols = {"ticker", "cnpj"}
    missing = required_cols - set(map(str.lower, df_ativos.columns))
    if missing:
        # tenta mapear por variações
        cols_lower = {c.lower(): c for c in df_ativos.columns}
        if "ticker" not in cols_lower or "cnpj" not in cols_lower:
            raise KeyError(
                f"[B3] df_ativos não contém colunas esperadas (ticker, cnpj). "
                f"Colunas disponíveis: {list(df_ativos.columns)}"
            )

    # Normaliza nomes das colunas para minúsculo
    df_ativos.columns = [c.lower() for c in df_ativos.columns]

    # Filtra ações (se existir 'segment')
    if "segment" in df_ativos.columns:
        # no exemplo: CASH = mercado à vista
        df_ativos = df_ativos[df_ativos["segment"].astype(str).str.upper().eq("CASH")].copy()

    df_ativos["ticker"] = df_ativos["ticker"].astype(str).str.strip().str.upper()
    df_ativos = df_ativos[df_ativos["ticker"].map(_looks_like_equity_ticker)]
    df_ativos["cnpj_raiz"] = df_ativos["cnpj"].map(_cnpj_raiz)
    df_ativos = df_ativos[df_ativos["cnpj_raiz"].str.len() == 8]

    nome_b3_col = "name" if "name" in df_ativos.columns else None
    if nome_b3_col:
        df_ativos["nome_b3"] = df_ativos[nome_b3_col].astype(str).str.strip()
    else:
        df_ativos["nome_b3"] = ""

    print(f"[cvm_to_ticker] B3: {len(df_ativos)} linhas após filtros.")

    print("[cvm_to_ticker] Baixando cadastro CVM via tradingcomdados...")
    df_cadastral = cvm.get_ca_cadastro()

    # Esperado: CNPJ_CIA, CD_CVM (como no seu exemplo)
    cols = {c.lower(): c for c in df_cadastral.columns}
    if "cnpj_cia" not in cols or "cd_cvm" not in cols:
        raise KeyError(
            f"[CVM] df_cadastral não contém colunas esperadas (CNPJ_CIA, CD_CVM). "
            f"Colunas disponíveis: {list(df_cadastral.columns)}"
        )

    cnpj_col = cols["cnpj_cia"]
    cvm_col = cols["cd_cvm"]
    nome_cvm_col = cols.get("denom_social") or cols.get("denominação_social") or cols.get("nome_empresarial")

    df_cadastral = df_cadastral.rename(columns={cnpj_col: "cnpj_cia", cvm_col: "cvm"})
    df_cadastral["cnpj_raiz"] = df_cadastral["cnpj_cia"].map(_cnpj_raiz)
    df_cadastral = df_cadastral[df_cadastral["cnpj_raiz"].str.len() == 8].copy()
    df_cadastral["cvm"] = pd.to_numeric(df_cadastral["cvm"], errors="coerce").astype("Int64")
    df_cadastral = df_cadastral.dropna(subset=["cvm"])

    if nome_cvm_col:
        df_cadastral["nome_cvm"] = df_cadastral[nome_cvm_col].astype(str).str.strip()
    else:
        df_cadastral["nome_cvm"] = ""

    print(f"[cvm_to_ticker] CVM: {len(df_cadastral)} companhias com CNPJ raiz.")

    print("[cvm_to_ticker] Cruzando por CNPJ raiz...")
    merged = df_ativos.merge(df_cadastral[["cvm", "cnpj_raiz", "nome_cvm"]], on="cnpj_raiz", how="inner")

    if merged.empty:
        raise RuntimeError("Join B3 x CVM resultou em 0 linhas. Verifique colunas/formatos de CNPJ.")

    # 1 ticker por CVM (compatível com teu fluxo atual)
    final = (
        merged.groupby("cvm", as_index=False)
        .agg(
            ticker=("ticker", lambda s: _pick_best_ticker(list(s))),
            cnpj_raiz=("cnpj_raiz", "first"),
            nome_cvm=("nome_cvm", "first"),
            nome_b3=("nome_b3", "first"),
        )
        .dropna(subset=["ticker"])
    )

    # Sanity check: evita “update vazio” ou muito pequeno por falha de fonte
    if len(final) < 50:
        raise RuntimeError(
            f"Tabela final muito pequena ({len(final)}). Abortando para não sobrescrever com dado ruim."
        )

    print(f"[cvm_to_ticker] Linhas finais: {len(final)}")
    print(final.head(10).to_string(index=False))

    engine = create_engine(cfg.supabase_db_url, pool_pre_ping=True)
    _ensure_table(engine, cfg.target_schema, cfg.target_table)
    n = _upsert(engine, cfg.target_schema, cfg.target_table, final)

    print(f"[cvm_to_ticker] UPSERT concluído: {n} linhas gravadas/atualizadas em {cfg.target_schema}.{cfg.target_table}")


if __name__ == "__main__":
    main()
