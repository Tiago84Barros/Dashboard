# macro_bcb_ingest.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

SCHEMA = "cvm"
RAW_FULL = f"{SCHEMA}.macro_bcb"
MENSAL_FULL = f"{SCHEMA}.info_economica_mensal"


# Mapeamento do RAW -> colunas analíticas
SERIES_TO_COL = {
    "IPCA_MENSAL": "ipca",
    "SELIC_META": "selic",
    "SELIC_EFETIVA": "selic_efetiva",
    "CAMBIO_PTX": "cambio",
    "ICC": "icc",
    "PIB": "pib",
    "BALANCA_COMERCIAL": "balanca_comercial",
}


@dataclass(frozen=True)
class Rule:
    freq: str  # "daily" | "monthly" | "quarterly"
    reducer: str  # "last" | "mean"
    fill: str  # "none" | "asof" | "ffill"
    ffill_limit: int = 0
    interpolate: Optional[str] = None  # "time" ou None


RULES = {
    # Diárias
    "SELIC_META": Rule(freq="daily", reducer="last", fill="asof", ffill_limit=45),
    "SELIC_EFETIVA": Rule(freq="daily", reducer="last", fill="asof", ffill_limit=45),
    "CAMBIO_PTX": Rule(freq="daily", reducer="mean", fill="asof", ffill_limit=45),

    # Mensais
    "IPCA_MENSAL": Rule(freq="monthly", reducer="last", fill="ffill", ffill_limit=2),
    "ICC": Rule(freq="monthly", reducer="last", fill="ffill", ffill_limit=2),
    "BALANCA_COMERCIAL": Rule(freq="monthly", reducer="last", fill="ffill", ffill_limit=2),

    # Trimestral
    "PIB": Rule(freq="quarterly", reducer="last", fill="ffill", ffill_limit=4, interpolate=None),
}


def _ensure_mensal_table(engine: Engine) -> None:
    ddl_schema = f"create schema if not exists {SCHEMA};"
    ddl = f"""
    create table if not exists {MENSAL_FULL} (
      data date primary key,
      selic double precision,
      selic_efetiva double precision,
      cambio double precision,
      ipca double precision,
      icc double precision,
      pib double precision,
      balanca_comercial double precision,
      fetched_at timestamptz default now()
    );
    """
    alter = f"""
    alter table {MENSAL_FULL}
      add column if not exists selic double precision,
      add column if not exists selic_efetiva double precision,
      add column if not exists cambio double precision,
      add column if not exists ipca double precision,
      add column if not exists icc double precision,
      add column if not exists pib double precision,
      add column if not exists balanca_comercial double precision,
      add column if not exists fetched_at timestamptz default now();
    """
    with engine.begin() as conn:
        conn.execute(text(ddl_schema))
        conn.execute(text(ddl))
        conn.execute(text(alter))


def _load_raw(engine: Engine, series_name: str) -> pd.DataFrame:
    q = text(
        f"""
        select data::date as data, valor::double precision as valor
        from {RAW_FULL}
        where series_name = :series_name
        order by data
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"series_name": series_name})
    if df.empty:
        return df
    df["data"] = pd.to_datetime(df["data"])
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    return df.dropna(subset=["data"])


def _monthly_index(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    start_me = (start + pd.offsets.MonthEnd(0)).normalize()
    end_me = (end + pd.offsets.MonthEnd(0)).normalize()
    return pd.date_range(start=start_me, end=end_me, freq="M")


def _reduce_to_monthly(df: pd.DataFrame, rule: Rule) -> pd.Series:
    """
    df: columns [data(datetime64), valor(float)]
    output: Series index = month_end, values = reduced values
    """
    if df.empty:
        return pd.Series(dtype="float64")

    s = df.set_index("data")["valor"].sort_index()

    # Reamostragem para mês-fim
    if rule.reducer == "last":
        m = s.resample("M").last()
    elif rule.reducer == "mean":
        m = s.resample("M").mean()
    else:
        raise ValueError(f"Reducer inválido: {rule.reducer}")

    # Interpolação opcional (ex.: PIB trimestral -> mensal)
    if rule.interpolate:
        m = m.interpolate(method=rule.interpolate)

    # Preenchimento controlado
    if rule.fill == "ffill" and rule.ffill_limit > 0:
        m = m.ffill(limit=rule.ffill_limit)
    return m


def _asof_fill(monthly_base: pd.Series, daily_df: pd.DataFrame, max_days: int = 45) -> pd.Series:
    """
    Preenche meses vazios usando o último valor disponível até o mês-fim (asof).
    """
    if daily_df.empty:
        return monthly_base

    d = daily_df.copy()
    d = d.dropna(subset=["data"]).sort_values("data")
    d["data"] = pd.to_datetime(d["data"])
    d["valor"] = pd.to_numeric(d["valor"], errors="coerce")

    # dataframe alvo com month_end
    target = monthly_base.index.to_frame(index=False, name="month_end").sort_values("month_end")

    # asof merge: pega último ponto <= month_end
    merged = pd.merge_asof(
        target,
        d.rename(columns={"data": "day"})[["day", "valor"]].sort_values("day"),
        left_on="month_end",
        right_on="day",
        direction="backward",
        tolerance=pd.Timedelta(days=max_days),
    )
    filled = monthly_base.copy()
    # só preenche onde estava NaN
    mask = filled.isna()
    filled.loc[mask] = merged.loc[mask.values, "valor"].values
    return filled


def _build_info_economica_mensal(engine: Engine, *, progress_cb: Optional[Callable[[str], None]] = None) -> pd.DataFrame:
    # Determina janela temporal com base no RAW
    q = text(f"select min(data)::date as min_d, max(data)::date as max_d from {RAW_FULL};")
    with engine.connect() as conn:
        r = conn.execute(q).mappings().first()

    if not r or r["min_d"] is None or r["max_d"] is None:
        raise RuntimeError("MACRO: cvm.macro_bcb está vazio. Rode o ingest RAW primeiro.")

    start = pd.to_datetime(r["min_d"])
    end = pd.to_datetime(r["max_d"])
    idx = _monthly_index(start, end)

    out = pd.DataFrame({"data": idx})

    for series_name, col in SERIES_TO_COL.items():
        rule = RULES.get(series_name)
        if rule is None:
            # sem regra: não cria coluna
            continue

        if progress_cb:
            progress_cb(f"MACRO: processando {series_name} -> {col} ({rule.freq}, {rule.reducer})")

        df = _load_raw(engine, series_name)

        # base mensal (reamostrado)
        monthly = _reduce_to_monthly(df, rule)
        monthly = monthly.reindex(idx)  # garante índice completo

        # se for diária e estiver faltando, asof-fill é mais correto do que ffill puro
        if rule.freq == "daily":
            monthly = _asof_fill(monthly, df, max_days=rule.ffill_limit)

        # preenchimento final (se configurado)
        if rule.fill == "ffill" and rule.ffill_limit > 0:
            monthly = monthly.ffill(limit=rule.ffill_limit)

        out[col] = monthly.values

    # Converte data para date
    out["data"] = pd.to_datetime(out["data"]).dt.date
    return out


def _upsert_mensal(engine: Engine, df: pd.DataFrame, batch: int = 2000) -> None:
    if df.empty:
        return

    cols = ["data"] + [c for c in df.columns if c != "data"]
    col_list = ", ".join(cols)
    val_list = ", ".join([f":{c}" for c in cols])
    set_list = ", ".join([f"{c} = excluded.{c}" for c in cols if c != "data"])

    sql = f"""
    insert into {MENSAL_FULL} ({col_list}, fetched_at)
    values ({val_list}, now())
    on conflict (data) do update set
      {set_list},
      fetched_at = now();
    """

    rows = df.to_dict("records")
    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(sql), rows[i : i + batch])


def run(engine: Engine, *, progress_cb: Optional[Callable[[str], None]] = None) -> None:
    _ensure_mensal_table(engine)

    if progress_cb:
        progress_cb("MACRO: construindo info_economica_mensal com regras por série (resample/asof/interp)...")

    df_m = _build_info_economica_mensal(engine, progress_cb=progress_cb)

    # Checagem mínima: evita “sucesso falso”
    filled_counts = {c: int(df_m[c].notna().sum()) for c in df_m.columns if c != "data"}
    if progress_cb:
        progress_cb(f"MACRO: preenchimento (not null) por coluna: {filled_counts}")

    _upsert_mensal(engine, df_m)

    if progress_cb:
        progress_cb("MACRO: info_economica_mensal atualizada com sucesso.")
