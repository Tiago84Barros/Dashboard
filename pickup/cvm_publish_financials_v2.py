# cvm_publish_financials_v2.py
# Publicação institucional da camada normalizada CVM para demonstracoes_financeiras_v2

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Iterable, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from core.db import get_engine


LOG_PREFIX = os.getenv("LOG_PREFIX", "[CVM_PUBLISH_V2]")
RUN_ID = os.getenv("RUN_ID") or f"publish_v2_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"


CANONICAL_KEYS = [
    "receita_bruta",
    "deducoes_receita",
    "receita_liquida",
    "custo",
    "lucro_bruto",
    "despesa_vendas",
    "despesa_geral_admin",
    "depreciacao_amortizacao",
    "ebit",
    "ebitda",
    "resultado_financeiro",
    "ir_csll",
    "lucro_antes_ir",
    "lucro_liquido",
    "lpa",
    "ativo_total",
    "ativo_circulante",
    "caixa_equivalentes",
    "aplicacoes_financeiras",
    "contas_receber",
    "estoques",
    "imobilizado",
    "intangivel",
    "investimentos",
    "passivo_circulante",
    "fornecedores",
    "divida_cp",
    "passivo_nao_circulante",
    "divida_lp",
    "provisoes",
    "passivo_total",
    "patrimonio_liquido",
    "participacao_n_controladores",
    "fco",
    "fci",
    "fcf",
    "capex",
    "juros_pagos",
    "dividendos_jcp_contabeis",
    "dividendos_declarados",
]


NUMERIC_COLS_2DP = [c for c in CANONICAL_KEYS if c != "lpa"] + [
    "divida_bruta",
    "divida_liquida",
    "quality_score",
]


def log(msg: str) -> None:
    print(f"{LOG_PREFIX} {msg}", flush=True)


def _safe_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _quality_score(group: pd.DataFrame, available_cols: Iterable[str]) -> float:
    cols = list(available_cols)
    present = sum(1 for c in cols if c in group["canonical_key"].values)
    total = max(len(cols), 1)
    base = present / total

    quality_map = {
        "exact": 1.00,
        "regex": 0.90,
        "manual": 0.95,
        "derived": 0.85,
        "fallback": 0.70,
    }

    qs = [quality_map.get(str(v), 0.60) for v in group["qualidade_mapeamento"].dropna().tolist()]
    mapping_quality = float(np.mean(qs)) if qs else 0.60
    return round(base * mapping_quality * 100, 4)


def fetch_best_source() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            ticker,
            dt_refer,
            canonical_key,
            valor,
            source_doc,
            qualidade_mapeamento,
            created_at
        FROM public.vw_cvm_normalized_best_source
        WHERE canonical_key = ANY(:keys)
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"keys": CANONICAL_KEYS})
    if df.empty:
        return df
    df["dt_refer"] = pd.to_datetime(df["dt_refer"], errors="coerce").dt.date
    return df


def build_wide(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    df = (
        df.sort_values(["ticker", "dt_refer", "canonical_key", "created_at"])
          .drop_duplicates(subset=["ticker", "dt_refer", "canonical_key"], keep="last")
          .reset_index(drop=True)
    )

    wide = (
        df.pivot_table(
            index=["ticker", "dt_refer"],
            columns="canonical_key",
            values="valor",
            aggfunc="last",
        )
        .reset_index()
        .rename(columns={"dt_refer": "data"})
    )
    wide.columns.name = None

    for col in CANONICAL_KEYS:
        if col not in wide.columns:
            wide[col] = np.nan

    meta = (
        df.groupby(["ticker", "dt_refer"])
          .agg(
              source_doc_principal=("source_doc", lambda s: s.mode().iloc[0] if not s.mode().empty else None),
          )
          .reset_index()
          .rename(columns={"dt_refer": "data"})
    )

    quality_rows = []
    for (ticker, dt_refer), g in df.groupby(["ticker", "dt_refer"], dropna=False):
        quality_rows.append({
            "ticker": ticker,
            "data": dt_refer,
            "quality_score": _quality_score(g, CANONICAL_KEYS),
            "quality_flags": json.dumps({
                "missing_critical": [],
                "derived_fields": [],
                "sources_seen": sorted(set([str(x) for x in g["source_doc"].dropna().unique().tolist()])),
            }, ensure_ascii=False),
        })
    quality_df = pd.DataFrame(quality_rows)

    out = wide.merge(meta, on=["ticker", "data"], how="left").merge(quality_df, on=["ticker", "data"], how="left")

    out["ebitda"] = out["ebitda"].where(
        out["ebitda"].notna(),
        out["ebit"].fillna(0) + out["depreciacao_amortizacao"].fillna(0),
    )

    out["divida_bruta"] = out["divida_cp"].fillna(0) + out["divida_lp"].fillna(0)
    out["divida_liquida"] = (
        out["divida_bruta"].fillna(0)
        - out["caixa_equivalentes"].fillna(0)
        - out["aplicacoes_financeiras"].fillna(0)
    )

    out["fcf"] = out["fcf"].where(
        out["fcf"].notna(),
        out["fco"].fillna(0) - out["capex"].abs().fillna(0),
    )

    out["source_priority"] = out["source_doc_principal"].map({
        "DFP": "DFP_CONSOLIDADO",
        "ITR": "ITR_CONSOLIDADO",
    }).fillna("UNKNOWN")

    out["run_id"] = RUN_ID

    critical = ["receita_liquida", "ebit", "lucro_liquido", "ativo_total", "patrimonio_liquido"]
    quality_flags_out = []
    for _, row in out.iterrows():
        flags = json.loads(row["quality_flags"]) if isinstance(row["quality_flags"], str) else {}
        flags.setdefault("missing_critical", [])
        flags.setdefault("derived_fields", [])

        for c in critical:
            if _safe_float(row.get(c)) is None:
                flags["missing_critical"].append(c)

        if _safe_float(row.get("ebitda")) is not None:
            flags["derived_fields"].append("ebitda")
        if _safe_float(row.get("fcf")) is not None:
            flags["derived_fields"].append("fcf")
        if _safe_float(row.get("divida_bruta")) is not None:
            flags["derived_fields"].append("divida_bruta")
        if _safe_float(row.get("divida_liquida")) is not None:
            flags["derived_fields"].append("divida_liquida")

        flags["missing_critical"] = sorted(set(flags["missing_critical"]))
        flags["derived_fields"] = sorted(set(flags["derived_fields"]))
        quality_flags_out.append(json.dumps(flags, ensure_ascii=False))

    out["quality_flags"] = quality_flags_out

    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["data"] = pd.to_datetime(out["data"], errors="coerce").dt.date

    for col in NUMERIC_COLS_2DP:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(2)

    if "lpa" in out.columns:
        out["lpa"] = pd.to_numeric(out["lpa"], errors="coerce").round(6)

    final_cols = [
        "ticker",
        "data",
        "receita_bruta",
        "deducoes_receita",
        "receita_liquida",
        "custo",
        "lucro_bruto",
        "despesa_vendas",
        "despesa_geral_admin",
        "depreciacao_amortizacao",
        "ebit",
        "ebitda",
        "resultado_financeiro",
        "ir_csll",
        "lucro_antes_ir",
        "lucro_liquido",
        "lpa",
        "ativo_total",
        "ativo_circulante",
        "caixa_equivalentes",
        "aplicacoes_financeiras",
        "contas_receber",
        "estoques",
        "imobilizado",
        "intangivel",
        "investimentos",
        "passivo_circulante",
        "fornecedores",
        "divida_cp",
        "passivo_nao_circulante",
        "divida_lp",
        "provisoes",
        "passivo_total",
        "patrimonio_liquido",
        "participacao_n_controladores",
        "fco",
        "fci",
        "fcf",
        "capex",
        "juros_pagos",
        "dividendos_jcp_contabeis",
        "dividendos_declarados",
        "divida_bruta",
        "divida_liquida",
        "source_priority",
        "source_doc_principal",
        "quality_score",
        "quality_flags",
        "run_id",
    ]

    for c in final_cols:
        if c not in out.columns:
            out[c] = None

    out = out[final_cols].copy()
    out = out[out["ticker"].ne("") & out["data"].notna()].reset_index(drop=True)
    return out


def upsert_demonstracoes_financeiras_v2(df: pd.DataFrame) -> int:
    if df.empty:
        log("Nenhuma linha para publicar em demonstracoes_financeiras_v2.")
        return 0

    engine = get_engine()
    cols = list(df.columns)
    quoted_cols = ", ".join([f'"{c}"' for c in cols])
    update_cols = [c for c in cols if c not in ("ticker", "data")]
    update_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])

    sql = f"""
        INSERT INTO public.demonstracoes_financeiras_v2 ({quoted_cols})
        VALUES ({", ".join([f":{c}" for c in cols])})
        ON CONFLICT (ticker, data) DO UPDATE SET
        {update_sql}
    """

    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(text(sql), records)

    log(f"Publicação concluída: {len(df)} linhas em demonstracoes_financeiras_v2.")
    return len(df)


def main() -> None:
    # ── Validação de pré-condição: schema V2 deve existir ──────────────────
    try:
        from core.cvm_v2_schema_check import assert_v2_schema_ready
        assert_v2_schema_ready()
    except ImportError:
        pass   # módulo de checagem não disponível — prossegue

    log("Lendo vw_cvm_normalized_best_source...")
    df = fetch_best_source()

    if df.empty:
        log("Nenhum dado encontrado na view priorizada.")
        return

    log(f"Linhas lidas: {len(df)}")
    wide = build_wide(df)

    if wide.empty:
        log("Nenhuma linha publicada após pivot e derivação.")
        return

    rows = upsert_demonstracoes_financeiras_v2(wide)
    log(f"Processo finalizado com sucesso | rows={rows} | run_id={RUN_ID}")


if __name__ == "__main__":
    main()
