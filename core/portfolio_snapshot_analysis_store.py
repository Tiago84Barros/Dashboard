
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import text

from core.db import get_engine as get_supabase_engine

TABLE = "public.portfolio_snapshot_analysis"


def save_snapshot_analysis(snapshot_id: str, rows: List[Dict[str, Any]]) -> None:
    if not snapshot_id:
        raise ValueError("snapshot_id é obrigatório")

    engine = get_supabase_engine()
    with engine.begin() as conn:
        conn.execute(text(f"delete from {TABLE} where snapshot_id = :sid"), {"sid": snapshot_id})

        if not rows:
            return

        stmt = text(f"""
            insert into {TABLE} (
                snapshot_id, ticker, rank_geral, rank_segmento, setor, subsetor, segmento,
                score_final, classe_forca,
                score_qualidade, score_valuation, score_dividendos, score_crescimento, score_consistencia,
                penal_crowding, penal_lideranca, penal_plato, penal_total,
                roe, roic, margem_bruta, margem_ebitda, margem_liquida,
                dividend_yield, p_vp, slope_receita,
                drivers_positivos, drivers_negativos, motivos_selecao, analysis_json
            ) values (
                :snapshot_id, :ticker, :rank_geral, :rank_segmento, :setor, :subsetor, :segmento,
                :score_final, :classe_forca,
                :score_qualidade, :score_valuation, :score_dividendos, :score_crescimento, :score_consistencia,
                :penal_crowding, :penal_lideranca, :penal_plato, :penal_total,
                :roe, :roic, :margem_bruta, :margem_ebitda, :margem_liquida,
                :dividend_yield, :p_vp, :slope_receita,
                cast(:drivers_positivos as jsonb), cast(:drivers_negativos as jsonb),
                cast(:motivos_selecao as jsonb), cast(:analysis_json as jsonb)
            )
        """)

        payload = []
        for row in rows:
            item = dict(row)
            item["snapshot_id"] = snapshot_id
            for k in ["drivers_positivos", "drivers_negativos", "motivos_selecao", "analysis_json"]:
                item[k] = json.dumps(item.get(k) or ([] if k != 'analysis_json' else {}), ensure_ascii=False)
            payload.append(item)
        conn.execute(stmt, payload)


def load_snapshot_analysis(snapshot_id: str) -> pd.DataFrame:
    if not snapshot_id:
        return pd.DataFrame()

    engine = get_supabase_engine()
    with engine.connect() as conn:
        df = pd.read_sql_query(
            text(f"""
                select *
                from {TABLE}
                where snapshot_id = :sid
                order by coalesce(rank_geral, 999999), ticker
            """),
            conn,
            params={"sid": snapshot_id},
        )

    if df is None or df.empty:
        return pd.DataFrame()

    for col in ["drivers_positivos", "drivers_negativos", "motivos_selecao", "analysis_json"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x if isinstance(x, (dict, list)) else _safe_json_loads(x))
    return df


def _safe_json_loads(value: Any) -> Any:
    if value is None or value == "":
        return []
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return []
