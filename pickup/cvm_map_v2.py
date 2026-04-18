"""
pickup/cvm_map_v2.py
Camada de normalização CVM V2.

Lê public.cvm_financial_raw, aplica mapeamento de contas de
public.cvm_account_map e grava em public.cvm_financial_normalized.

Pré-requisito: schema CVM V2 aplicado ao banco (DDL institucional).
"""
from __future__ import annotations

import os
import re
import time
from typing import Optional

import pandas as pd
from sqlalchemy import text

from core.db import get_engine

LOG_PREFIX = os.getenv("LOG_PREFIX", "[CVM_MAP_V2]")


def log(msg: str) -> None:
    print(f"{LOG_PREFIX} {msg}", flush=True)


def fetch_raw(engine=None) -> pd.DataFrame:
    """Carrega todos os registros de public.cvm_financial_raw."""
    if engine is None:
        engine = get_engine()
    query = text("SELECT * FROM public.cvm_financial_raw")
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def fetch_mapping(engine=None) -> pd.DataFrame:
    """Carrega mapeamento de contas ativo de public.cvm_account_map."""
    if engine is None:
        engine = get_engine()
    query = text(
        "SELECT * FROM public.cvm_account_map WHERE ativo = TRUE ORDER BY prioridade"
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def match_row(
    row: pd.Series,
    mappings: pd.DataFrame,
) -> tuple[Optional[pd.Series], str]:
    """Encontra o mapeamento mais específico para uma linha raw.

    Returns:
        (mapping_row, quality)  where quality is 'exact' | 'regex' | 'fallback'
    """
    for _, m in mappings.iterrows():
        # Correspondência exata por código de conta
        if m.get("cd_conta") and row.get("cd_conta") == m["cd_conta"]:
            return m, "exact"
        # Correspondência por padrão regex no nome da conta
        if m.get("ds_conta_pattern") and pd.notna(m.get("ds_conta_pattern")):
            try:
                if re.search(
                    str(m["ds_conta_pattern"]),
                    str(row.get("ds_conta") or ""),
                    re.IGNORECASE,
                ):
                    return m, "regex"
            except re.error:
                pass
    return None, "fallback"


def normalize(df_raw: pd.DataFrame, mappings: pd.DataFrame) -> pd.DataFrame:
    """Aplica mapeamento e gera DataFrame normalizado."""
    if df_raw.empty:
        log("cvm_financial_raw está vazio — nada para normalizar.")
        return pd.DataFrame()

    if mappings.empty:
        log("cvm_account_map não contém registros ativos — nada para mapear.")
        return pd.DataFrame()

    results = []
    for _, row in df_raw.iterrows():
        mapping, quality = match_row(row, mappings)
        if mapping is None:
            continue

        sinal = float(mapping.get("sinal") or 1.0)
        vl = row.get("vl_conta")
        if vl is None or pd.isna(vl):
            continue

        results.append(
            {
                "ticker": row.get("ticker"),
                "cd_cvm": row.get("cd_cvm"),
                "source_doc": row.get("source_doc"),
                "tipo_demo": row.get("tipo_demo"),
                "dt_refer": row.get("dt_refer"),
                "canonical_key": mapping["canonical_key"],
                "valor": float(vl) * sinal,
                "unidade": "BRL",
                "qualidade_mapeamento": quality,
                "row_hash": row.get("row_hash"),
            }
        )

    return pd.DataFrame(results)


def save(df: pd.DataFrame, engine=None) -> int:
    """Grava em public.cvm_financial_normalized via append."""
    if engine is None:
        engine = get_engine()
    if df.empty:
        return 0
    df.to_sql(
        "cvm_financial_normalized",
        engine,
        schema="public",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000,
    )
    return len(df)


def main() -> None:
    # ── Validação de pré-condição: schema V2 deve existir ──────────────────
    try:
        from core.cvm_v2_schema_check import assert_v2_schema_ready
        assert_v2_schema_ready()
    except ImportError:
        pass   # módulo de checagem não disponível — prossegue

    engine = get_engine()
    t0 = time.time()

    log("Carregando public.cvm_financial_raw …")
    df_raw = fetch_raw(engine)
    log(f"Linhas raw carregadas: {len(df_raw)}")

    log("Carregando public.cvm_account_map (ativo=TRUE) …")
    mappings = fetch_mapping(engine)
    log(f"Mapeamentos ativos: {len(mappings)}")

    df_norm = normalize(df_raw, mappings)

    if df_norm.empty:
        log("Nenhum dado normalizado — verifique cvm_account_map e cvm_financial_raw.")
        return

    log(f"Linhas normalizadas: {len(df_norm)}. Gravando em public.cvm_financial_normalized …")
    inserted = save(df_norm, engine)
    elapsed = round(time.time() - t0, 1)
    log(f"Normalização concluída: {inserted} linhas inseridas em {elapsed}s.")


if __name__ == "__main__":
    main()
