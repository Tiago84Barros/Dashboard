from __future__ import annotations

from typing import Callable, Optional, Dict

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.macro_catalog import BCB_SERIES_CATALOG

SCHEMA = "cvm"
RAW_TABLE = "macro_bcb"
RAW_FULL = f"{SCHEMA}.{RAW_TABLE}"

WIDE_TABLE = "info_economica"
WIDE_M_TABLE = "info_economica_mensal"

WIDE_FULL = f"{SCHEMA}.{WIDE_TABLE}"
WIDE_M_FULL = f"{SCHEMA}.{WIDE_M_TABLE}"


# ───────────────────────────────────────────────────────────────
# Qual série do RAW alimenta cada coluna analítica
# (chaves devem bater com macro_catalog.py e com o RAW)
# ───────────────────────────────────────────────────────────────
SERIES_TO_COL = {
    "SELIC_EFETIVA": "selic",            # ou troque para SELIC_META se preferir
    "CAMBIO_PTX": "cambio",
    "IPCA_MENSAL": "ipca",
    "ICC": "icc",
    "PIB": "pib",
    "BALANCA_COMERCIAL": "balanca_comercial",
}


# ───────────────────────────────────────────────────────────────
# DDL + migração (evita UndefinedColumn)
# ───────────────────────────────────────────────────────────────
def _ensure_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"create schema if not exists {SCHEMA};"))


def _ensure_table_wide(engine: Engine) -> None:
    """
    Tabela com chave por 'data' (date). Mantém colunas completas.
    """
    ddl = f"""
    create table if not exists {WIDE_FULL} (
      data date primary key,
      selic double precision,
      cambio double precision,
      ipca double precision,
      icc double precision,
      pib double precision,
      balanca_comercial double precision,
      fetched_at timestamptz default now()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

    # Migração defensiva: adiciona colunas se a tabela já existe “capada”
    _add_missing_cols(engine, WIDE_FULL, include_pib=True)


def _ensure_table_wide_mensal(engine: Engine) -> None:
    """
    Tabela mensal. Também inclui pib e balanca para evitar seu erro atual.
    """
    ddl = f"""
    create table if not exists {WIDE_M_FULL} (
      data date primary key,
      selic double precision,
      cambio double precision,
      ipca double precision,
      icc double precision,
      pib double precision,
      balanca_comercial double precision,
      fetched_at timestamptz default now()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

    # Migração defensiva: adiciona colunas ausentes
    _add_missing_cols(engine, WIDE_M_FULL, include_pib=True)


def _add_missing_cols(engine: Engine, table_full: str, include_pib: bool = True) -> None:
    cols = [
        ("selic", "double precision"),
        ("cambio", "double precision"),
        ("ipca", "double precision"),
        ("icc", "double precision"),
        ("balanca_comercial", "double precision"),
        ("fetched_at", "timestamptz"),
    ]
    if include_pib:
        cols.insert(4, ("pib", "double precision"))

    with engine.begin() as conn:
        for col, typ in cols:
            conn.execute(text(f"alter table {table_full} add column if not exists {col} {typ};"))


# ───────────────────────────────────────────────────────────────
# Leitura RAW
# ───────────────────────────────────────────────────────────────
def _load_raw(engine: Engine) -> pd.DataFrame:
    """
    Carrega do RAW apenas as séries necessárias (SERIES_TO_COL.keys()).
    """
    series_list = tuple(SERIES_TO_COL.keys())

    q = text(
        f"""
        select
          data::date as data,
          series_name::text as series_name,
          valor::double precision as valor
        from {RAW_FULL}
        where series_name in :series_list
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"series_list": series_list})
    return df


# ───────────────────────────────────────────────────────────────
# Normalização para mensal (respeita freq do catálogo)
# ───────────────────────────────────────────────────────────────
def _to_monthly_series(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna um DataFrame mensal (data = último dia do mês) com colunas:
      data, selic, cambio, ipca, icc, pib, balanca_comercial
    """

    if df_raw.empty:
        return df_raw

    df = df_raw.copy()
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df = df.dropna(subset=["data", "series_name"])
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    # Converte series_name -> coluna destino
    df["col"] = df["series_name"].map(SERIES_TO_COL)
    df = df.dropna(subset=["col"])

    # Se houver duplicidade no mesmo dia (raro), pega o último
    df = df.sort_values(["series_name", "data"]).drop_duplicates(subset=["series_name", "data"], keep="last")

    # Vamos construir por coluna, respeitando a freq do catálogo
    month_end_index = None
    series_monthly: Dict[str, pd.Series] = {}

    for series_name, col in SERIES_TO_COL.items():
        s = df.loc[df["series_name"] == series_name, ["data", "valor"]].set_index("data")["valor"]
        if s.empty:
            continue

        freq = (BCB_SERIES_CATALOG.get(series_name, {}) or {}).get("freq", "")

        # Define o índice mensal global (range)
        s = s.sort_index()
        start = s.index.min().to_period("M").to_timestamp("M")
        end = s.index.max().to_period("M").to_timestamp("M")
        idx_m = pd.date_range(start=start, end=end, freq="M")
        if month_end_index is None:
            month_end_index = idx_m
        else:
            # expande para cobrir tudo
            month_end_index = month_end_index.union(idx_m)

        if freq == "daily":
            # último valor do mês
            sm = s.resample("M").last()
            sm = sm.reindex(idx_m)
        elif freq == "monthly":
            # garante alinhamento em month-end
            # se vier dia 01 do mês (ou qualquer dia), joga para month-end e pega último do mês
            sm = s.copy()
            sm.index = sm.index.to_period("M").to_timestamp("M")
            sm = sm.groupby(sm.index).last()
            sm = sm.reindex(idx_m)
        elif freq == "quarterly":
            # alinha em quarter-end e propaga para meses do trimestre
            sq = s.copy()
            sq.index = sq.index.to_period("Q").to_timestamp("Q")
            sq = sq.groupby(sq.index).last()

            # cria mensal e forward-fill dentro do trimestre
            sm = sq.reindex(idx_m, method=None)
            sm = sm.ffill()

            # opcional: se quiser suavizar buracos longos, interpola no tempo
            sm = sm.astype("float").interpolate(method="time", limit_direction="both")
        else:
            # fallback conservador: último valor do mês
            sm = s.resample("M").last().reindex(idx_m)

        series_monthly[col] = sm

    if month_end_index is None:
        return pd.DataFrame(columns=["data"] + list(SERIES_TO_COL.values()))

    # Monta wide mensal
    out = pd.DataFrame(index=month_end_index)

    for col in SERIES_TO_COL.values():
        if col in series_monthly:
            out[col] = series_monthly[col]
        else:
            out[col] = pd.NA

    out = out.reset_index().rename(columns={"index": "data"})
    out["data"] = out["data"].dt.date

    # Ordena colunas
    cols = ["data"] + list(SERIES_TO_COL.values())
    out = out[cols].sort_values("data").reset_index(drop=True)

    return out


# ───────────────────────────────────────────────────────────────
# Upsert (WIDE e WIDE_M)
# ───────────────────────────────────────────────────────────────
def _upsert(engine: Engine, table_full: str, wide: pd.DataFrame, batch: int = 2000) -> None:
    if wide.empty:
        return

    sql = f"""
    insert into {table_full} (
      data, selic, cambio, ipca, icc, pib, balanca_comercial, fetched_at
    )
    values (
      :data, :selic, :cambio, :ipca, :icc, :pib, :balanca_comercial, now()
    )
    on conflict (data) do update set
      selic = excluded.selic,
      cambio = excluded.cambio,
      ipca = excluded.ipca,
      icc = excluded.icc,
      pib = excluded.pib,
      balanca_comercial = excluded.balanca_comercial,
      fetched_at = now();
    """

    rows = wide.to_dict("records")
    with engine.begin() as conn:
        for i in range(0, len(rows), batch):
            conn.execute(text(sql), rows[i : i + batch])


# ───────────────────────────────────────────────────────────────
# Pipeline público
# ───────────────────────────────────────────────────────────────
def build_info_economica(engine: Engine, *, progress_cb: Optional[Callable[[str], None]] = None) -> None:
    _ensure_schema(engine)
    _ensure_table_wide(engine)
    _ensure_table_wide_mensal(engine)

    if progress_cb:
        progress_cb("MACRO WIDE: carregando RAW (cvm.macro_bcb)...")

    df_raw = _load_raw(engine)

    if df_raw.empty:
        raise RuntimeError("MACRO WIDE: cvm.macro_bcb está vazio para as séries necessárias.")

    # Log útil: contagem de séries realmente presentes
    if progress_cb:
        present = df_raw["series_name"].value_counts(dropna=False).to_dict()
        progress_cb(f"MACRO WIDE: séries encontradas no RAW: {present}")

    if progress_cb:
        progress_cb("MACRO WIDE: normalizando para mensal (month-end) e construindo wide...")

    wide_m = _to_monthly_series(df_raw)

    if wide_m.empty:
        raise RuntimeError("MACRO WIDE: transformação mensal retornou vazio (verifique RAW e catálogo).")

    if progress_cb:
        filled = {c: int(wide_m[c].notna().sum()) for c in SERIES_TO_COL.values()}
        progress_cb(f"MACRO WIDE: preenchimento mensal (contagem não-null): {filled}")

    # Você pode optar por gravar o mesmo conteúdo em info_economica e info_economica_mensal.
    # Aqui mantemos ambos idênticos (mensal). Se no futuro quiser uma diária, criamos outro builder.
    if progress_cb:
        progress_cb(f"MACRO WIDE: upsert em {WIDE_M_FULL} ({len(wide_m)} linhas)...")
    _upsert(engine, WIDE_M_FULL, wide_m)

    if progress_cb:
        progress_cb(f"MACRO WIDE: upsert em {WIDE_FULL} ({len(wide_m)} linhas)...")
    _upsert(engine, WIDE_FULL, wide_m)

    if progress_cb:
        progress_cb("MACRO WIDE: concluído com sucesso.")


def run(engine: Engine, *, progress_cb: Optional[Callable[[str], None]] = None) -> None:
    build_info_economica(engine, progress_cb=progress_cb)
