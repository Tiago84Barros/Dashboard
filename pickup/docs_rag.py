
# (conteúdo reduzido para exemplo - substitua pelo código completo fornecido anteriormente)
from __future__ import annotations
from typing import Any, Dict, List, Sequence, Optional
import pandas as pd
from sqlalchemy import text
from core.db_loader import get_supabase_engine

def _norm_ticker(t: str) -> str:
    return (t or "").upper().replace(".SA", "").strip()

def _read_df(sql: str, params: Dict[str, Any]) -> pd.DataFrame:
    engine = get_supabase_engine()
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)

def get_docs_by_tickers(tickers: Sequence[str], limit_per_ticker: int = 30):
    tks = [_norm_ticker(t) for t in tickers if t]
    out = {}
    for tk in tks:
        df = _read_df(
            """
            SELECT id, ticker, data, fonte, tipo, titulo, url, raw_text
            FROM public.docs_corporativos
            WHERE ticker = :tk
            ORDER BY COALESCE(data, created_at::date) DESC
            LIMIT :limit
            """,
            {"tk": tk, "limit": int(limit_per_ticker)},
        )
        out[tk] = df.to_dict(orient="records") if df is not None and not df.empty else []
    return out
