"""
Core CVM ETL utilities focused on:
- Deterministic value normalization using ESCALA_MOEDA
- Account extraction by CD_CONTA anchors and prefix trees (no DS_CONTA heuristics by default)
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

def build_balance_snapshot(bpa: pd.DataFrame, bpp: pd.DataFrame) -> pd.DataFrame:
    ativo_total, src_at = pick_anchor_or_prefix(bpa, BALANCE.ativo_total, BALANCE.ativo_total)
    ativo_circ, src_ac = pick_anchor_or_prefix(bpa, BALANCE.ativo_circulante, BALANCE.ativo_circulante)
    caixa, src_cx = pick_anchor_or_prefix(bpa, BALANCE.caixa_prefix, BALANCE.caixa_prefix)
    anc, src_anc = pick_anchor_or_prefix(bpa, BALANCE.ativo_nao_circulante, BALANCE.ativo_nao_circulante)

    passivo_total, src_pt = pick_anchor_or_prefix(bpp, BALANCE.passivo_total, BALANCE.passivo_total)
    pc, src_pc = pick_anchor_or_prefix(bpp, BALANCE.passivo_circulante, BALANCE.passivo_circulante)
    pnc, src_pnc = pick_anchor_or_prefix(bpp, BALANCE.passivo_nao_circulante, BALANCE.passivo_nao_circulante)
    pl, src_pl = pick_anchor_or_prefix(bpp, BALANCE.patrimonio_liquido, BALANCE.patrimonio_liquido)

    idx = pd.DatetimeIndex(sorted(set(ativo_total.index) | set(ativo_circ.index) | set(caixa.index) | set(anc.index) |
                                  set(passivo_total.index) | set(pc.index) | set(pnc.index) | set(pl.index)))
    if len(idx) == 0:
        return pd.DataFrame()

    df = pd.DataFrame(index=idx)
    df.index.name = "DT_REFER"

    df["Ativo_Total"] = ativo_total.reindex(idx)
    df["Ativo_Circulante"] = ativo_circ.reindex(idx)
    df["Caixa"] = caixa.reindex(idx)
    df["Ativo_Nao_Circulante"] = anc.reindex(idx)

    df["Passivo_Total"] = passivo_total.reindex(idx)
    df["Passivo_Circulante"] = pc.reindex(idx)
    df["Passivo_Nao_Circulante"] = pnc.reindex(idx)
    df["Patrimonio_Liquido"] = pl.reindex(idx)

    df["_src_Ativo_Total"] = src_at
    df["_src_Ativo_Circulante"] = src_ac
    df["_src_Caixa"] = src_cx
    df["_src_Ativo_Nao_Circulante"] = src_anc
    df["_src_Passivo_Total"] = src_pt
    df["_src_Passivo_Circulante"] = src_pc
    df["_src_Passivo_Nao_Circulante"] = src_pnc
    df["_src_Patrimonio_Liquido"] = src_pl
    return df

@dataclass(frozen=True)
class DREMapping:
    receita_liquida: str = "3.01"
    ebit: str = "3.05"
    lucro_liquido_anchor: str = "3.11"
    lpa: str = "3.99.01.01"

DRE = DREMapping()

def build_dre_snapshot(dre: pd.DataFrame) -> pd.DataFrame:
    if dre is None or dre.empty:
        return pd.DataFrame()

    receita, src_r = pick_anchor_or_prefix(dre, DRE.receita_liquida, DRE.receita_liquida)
    ebit, src_e = pick_anchor_or_prefix(dre, DRE.ebit, DRE.ebit)
    lpa, src_lpa = pick_anchor_or_prefix(dre, DRE.lpa, DRE.lpa)

    lucro = sum_by_anchor(dre, DRE.lucro_liquido_anchor)
    src_ll = "ANCHOR"
    if lucro.empty:
        dfx = _prefer_fixed(dre)
        ds = dfx.get("DS_CONTA")
        if ds is not None:
            sel = dfx[ds.astype(str).str.contains(r"Lucro|Preju[ií]zo", case=False, na=False)]
            sel = sel[sel["CD_CONTA"].astype(str).str.startswith("3.")]
            if not sel.empty:
                lucro = sel.groupby("DT_REFER", dropna=True)["VL_CONTA_NORM"].sum()
                src_ll = "TEXT_FALLBACK"
            else:
                src_ll = "MISSING"
        else:
            src_ll = "MISSING"

    idx = pd.DatetimeIndex(sorted(set(receita.index) | set(ebit.index) | set(lucro.index) | set(lpa.index)))
    if len(idx) == 0:
        return pd.DataFrame()

    df = pd.DataFrame(index=idx)
    df.index.name = "DT_REFER"
    df["Receita_Liquida"] = receita.reindex(idx)
    df["EBIT"] = ebit.reindex(idx)
    df["Lucro_Liquido"] = lucro.reindex(idx)
    df["LPA"] = lpa.reindex(idx)

    df["_src_Receita_Liquida"] = src_r
    df["_src_EBIT"] = src_e
    df["_src_Lucro_Liquido"] = src_ll
    df["_src_LPA"] = src_lpa
    return df

@dataclass(frozen=True)
class DFCMapping:
    cfo_prefix: str = "6.01"
    cfi_prefix: str = "6.02"
    cff_prefix: str = "6.03"
    capex_keywords: tuple[str, ...] = ("aquisi", "imobiliz", "intang", "capex")

DFC = DFCMapping()

def build_dfc_snapshot(dfc: pd.DataFrame) -> pd.DataFrame:
    if dfc is None or dfc.empty:
        return pd.DataFrame()

    cfo, src_cfo = pick_anchor_or_prefix(dfc, DFC.cfo_prefix, DFC.cfo_prefix)
    cfi, src_cfi = pick_anchor_or_prefix(dfc, DFC.cfi_prefix, DFC.cfi_prefix)
    cff, src_cff = pick_anchor_or_prefix(dfc, DFC.cff_prefix, DFC.cff_prefix)

    capex = pd.Series(dtype="float64")
    src_capex = "MISSING"
    dfx = _prefer_fixed(dfc)
    cd = dfx["CD_CONTA"].astype(str)
    mask_cfi = (cd == DFC.cfi_prefix) | cd.str.startswith(DFC.cfi_prefix + ".")
    sel = dfx[mask_cfi]
    if not sel.empty and "DS_CONTA" in sel.columns:
        ds = sel["DS_CONTA"].astype(str).str.lower()
        mask = np.zeros(len(sel), dtype=bool)
        for kw in DFC.capex_keywords:
            mask |= ds.str.contains(kw, na=False)
        sel2 = sel[mask]
        if not sel2.empty:
            capex = sel2.groupby("DT_REFER", dropna=True)["VL_CONTA_NORM"].sum()
            src_capex = "TEXT_WITHIN_CFI"

    idx = pd.DatetimeIndex(sorted(set(cfo.index) | set(cfi.index) | set(cff.index) | set(capex.index)))
    if len(idx) == 0:
        return pd.DataFrame()

    df = pd.DataFrame(index=idx)
    df.index.name = "DT_REFER"
    df["CFO"] = cfo.reindex(idx)
    df["CFI"] = cfi.reindex(idx)
    df["CFF"] = cff.reindex(idx)
    df["CAPEX"] = capex.reindex(idx)
    df["FCF"] = df["CFO"] - df["CAPEX"]

    df["_src_CFO"] = src_cfo
    df["_src_CFI"] = src_cfi
    df["_src_CFF"] = src_cff
    df["_src_CAPEX"] = src_capex
    return df

def apply_balance_dq(df_bal: pd.DataFrame, tol_pct: float = 0.02) -> pd.DataFrame:
    if df_bal is None or df_bal.empty:
        return df_bal
    out = df_bal.copy()
    a = out["Ativo_Total"]
    rhs = out["Passivo_Total"] + out["Patrimonio_Liquido"]
    diff = a - rhs

    out["dq_balance_diff"] = diff
    out["dq_balance_diff_pct"] = (diff.abs() / a.abs()).replace([np.inf, -np.inf], np.nan)
    out["dq_balance_ok"] = out["dq_balance_diff_pct"].fillna(np.nan) <= tol_pct

    out["dq_ac_le_ativo"] = (out["Ativo_Circulante"].abs() <= out["Ativo_Total"].abs()) | out["Ativo_Circulante"].isna() | out["Ativo_Total"].isna()
    out["dq_pc_le_passivo"] = (out["Passivo_Circulante"].abs() <= out["Passivo_Total"].abs()) | out["Passivo_Circulante"].isna() | out["Passivo_Total"].isna()

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
