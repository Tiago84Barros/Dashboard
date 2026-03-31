from __future__ import annotations

import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def get_supabase_engine() -> Engine:
    """
    Recomendado: reaproveitar aqui a MESMA lógica já usada no projeto para criar o engine do Supabase.
    Abaixo vai um fallback padrão por DATABASE_URL, caso seu projeto já exponha essa env var.
    """
    db_url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL/SUPABASE_DB_URL não configurada. "
            "Conecte aqui a mesma função/engine que já existe no projeto."
        )
    return create_engine(db_url, pool_pre_ping=True)


def load_setores_from_supabase() -> pd.DataFrame:
    engine = get_supabase_engine()

    # Ajuste os nomes de colunas abaixo para bater com o seu schema real no Supabase
    # (ex.: ticker, empresa, setor, subsetor, segmento).
    sql = text("""
        SELECT
            ticker,
            setor,
            subsetor,
            segmento
        FROM public.setores
        WHERE ticker IS NOT NULL
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)

    # Normaliza para o contrato esperado pelo basic.py (maiúsculas nas chaves setoriais)
    rename_map = {
        "setor": "SETOR",
        "subsetor": "SUBSETOR",
        "segmento": "SEGMENTO",
        "TICKER": "ticker",
        "ticker": "ticker",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})

    # Garantias defensivas (evitar quebra do layout)
    for col in ["SETOR", "SUBSETOR", "SEGMENTO"]:
        if col not in df.columns:
            df[col] = ""

    df["ticker"] = df["ticker"].astype(str).str.replace(".SA", "", regex=False).str.upper()

    return df
