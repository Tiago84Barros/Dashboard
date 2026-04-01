"""
Core CVM ETL utilities focused on:
- Deterministic value normalization using ESCALA_MOEDA
- Account extraction by CD_CONTA anchors and prefix trees
- Data Quality (DQ) gates to prevent incoherent rows from contaminating curated tables
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple
import json
import pandas as pd
import numpy as np

_SCALE_MULT = {
    "UNIDADE": 1.0,
    "UNIDADES": 1.0,
    "MIL": 1000.0,
    "MILHAR": 1000.0,
    "MILHARES": 1000.0,
}

def normalize_vl_conta(df: pd.DataFrame, *, value_col: str = "VL_CONTA") -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()

    if "DT_REFER" in out.columns:
        out["DT_REFER"] = pd.to_datetime(out["DT_REFER"], errors="coerce")

    out["VL_CONTA_NUM"] = pd.to_numeric(out[value_col], errors="coerce")

    escala = out.get("ESCALA_MOEDA")
    if escala is None:
        out["VL_CONTA_NORM"] = out["VL_CONTA_NUM"]
        out["DQ_SCALE_FLAG"] = "NO_ESCALA_MOEDA"
        return out

    esc = escala.astype(str).str.upper().str.strip()
    mult = esc.map(_SCALE_MULT)

    out["DQ_SCALE_FLAG"] = np.where(mult.isna(), "UNKNOWN_ESCALA_MOEDA", "OK")
    out["VL_CONTA_NORM"] = out["VL_CONTA_NUM"] * mult.fillna(1.0)
    return out

def _prefer_fixed(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "ST_CONTA_FIXA" not in df.columns:
        return df
    fixed = df[df["ST_CONTA_FIXA"].astype(str).str.upper().str.strip() == "S"]
    return fixed if not fixed.empty else df

def sum_by_anchor(df: pd.DataFrame, cd_conta: str) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype="float64")
    dfx = _prefer_fixed(df)
    sel = dfx[dfx["CD_CONTA"].astype(str) == str(cd_conta)]
    if sel.empty:
        return pd.Series(dtype="float64")
    return sel.groupby("DT_REFER", dropna=True)["VL_CONTA_NORM"].sum()

def sum_by_prefix(df: pd.DataFrame, prefix: str) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype="float64")
    dfx = _prefer_fixed(df)
    p = str(prefix)
    cd = dfx["CD_CONTA"].astype(str)
    mask = (cd == p) | cd.str.startswith(p + ".")
    sel = dfx[mask]
    if sel.empty:
        return pd.Series(dtype="float64")
    return sel.groupby("DT_REFER", dropna=True)["VL_CONTA_NORM"].sum()

def pick_anchor_or_prefix(df: pd.DataFrame, anchor: str, prefix: str) -> Tuple[pd.Series, str]:
    s_anchor = sum_by_anchor(df, anchor)
    if not s_anchor.empty:
        return s_anchor, "ANCHOR"
    s_pref = sum_by_prefix(df, prefix)
    if not s_pref.empty:
        return s_pref, "PREFIX"
    return pd.Series(dtype="float64"), "MISSING"

@dataclass(frozen=True)
class BalanceMapping:
    ativo_total: str = "1"
    ativo_circulante: str = "1.01"
    caixa_prefix: str = "1.01.01"
    ativo_nao_circulante: str = "1.02"
    passivo_total: str = "2"
    passivo_circulante: str = "2.01"
    passivo_nao_circulante: str = "2.02"
    patrimonio_liquido: str = "2.03"

BALANCE = BalanceMapping()

def apply_balance_dq(df_bal: pd.DataFrame, tol_pct: float = 0.02) -> pd.DataFrame:
    """
    df_bal index must be datetime-like and named DT_REFER.
    Required columns:
      Ativo_Total, Passivo_Total, Patrimonio_Liquido
    Optional columns:
      Ativo_Circulante, Passivo_Circulante
    """
    if df_bal is None or df_bal.empty:
        return df_bal
    out = df_bal.copy()

    a = pd.to_numeric(out.get("Ativo_Total"), errors="coerce")
    p = pd.to_numeric(out.get("Passivo_Total"), errors="coerce")
    pl = pd.to_numeric(out.get("Patrimonio_Liquido"), errors="coerce")

    rhs = p + pl
    diff = a - rhs

    out["dq_balance_diff"] = diff
    out["dq_balance_diff_pct"] = (diff.abs() / a.abs()).replace([np.inf, -np.inf], np.nan)
    out["dq_balance_ok"] = out["dq_balance_diff_pct"] <= tol_pct

    # Optional sanity checks
    if "Ativo_Circulante" in out.columns:
        ac = pd.to_numeric(out.get("Ativo_Circulante"), errors="coerce")
        out["dq_ac_le_ativo"] = (ac.abs() <= a.abs()) | ac.isna() | a.isna()
    else:
        out["dq_ac_le_ativo"] = True

    if "Passivo_Circulante" in out.columns:
        pc = pd.to_numeric(out.get("Passivo_Circulante"), errors="coerce")
        out["dq_pc_le_passivo"] = (pc.abs() <= p.abs()) | pc.isna() | p.isna()
    else:
        out["dq_pc_le_passivo"] = True

    flags = []
    status = []
    for _, row in out.iterrows():
        f = []
        if pd.notna(row.get("dq_balance_ok")) and row.get("dq_balance_ok") is False:
            f.append("BALANCE_IDENTITY_FAIL")
        if pd.notna(row.get("dq_ac_le_ativo")) and row.get("dq_ac_le_ativo") is False:
            f.append("AC_GT_ATIVO")
        if pd.notna(row.get("dq_pc_le_passivo")) and row.get("dq_pc_le_passivo") is False:
            f.append("PC_GT_PASSIVO")

        for col in ["Ativo_Total", "Passivo_Total", "Patrimonio_Liquido"]:
            if pd.isna(row.get(col)):
                f.append(f"MISSING_{col.upper()}")

        flags.append(f)

        if "BALANCE_IDENTITY_FAIL" in f:
            status.append("FAIL")
        elif any(k in ("AC_GT_ATIVO", "PC_GT_PASSIVO") for k in f):
            status.append("WARNING")
        else:
            status.append("OK")

    out["dq_flags"] = [json.dumps(x, ensure_ascii=False) for x in flags]
    out["dq_status"] = status
    return out
