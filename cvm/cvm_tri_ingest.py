#Antigo Algoritmo 5
from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable, Optional, Tuple

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.sync_state import ensure_sync_table, get_state, set_state

URL_BASE_ITR = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"

DEFAULT_START_YEAR = 2010
DEFAULT_END_YEAR = 2025

STATE_TARGET_START = "cvm:itr_target_start_year"
STATE_TARGET_END = "cvm:itr_target_end_year"
STATE_NEXT_YQ = "cvm:itr_next_yq"         # "YYYYQ" (ex. 20254 para 2025T4)
STATE_LAST_DONE = "cvm:itr_last_completed_yq"


@dataclass(frozen=True)
class ItrConfig:
    start_year: int = DEFAULT_START_YEAR
    end_year: int = DEFAULT_END_YEAR
    quarters_per_run: int = 1
    ticker_map_path: Optional[str] = None
    timeout_sec: int = 120


def _find_ticker_map(ticker_map_path: Optional[str]) -> Path:
    if ticker_map_path:
        p = Path(ticker_map_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"ticker_map_path informado não existe: {ticker_map_path}")

    candidates = [
        Path("cvm") / "cvm_to_ticker.csv",
        Path("data") / "cvm_to_ticker.csv",
        Path(__file__).resolve().parent / "cvm_to_ticker.csv",
        Path(__file__).resolve().parent.parent / "cvm" / "cvm_to_ticker.csv",
        Path(__file__).resolve().parent.parent / "data" / "cvm_to_ticker.csv",
    ]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError("Não encontrei cvm_to_ticker.csv (coloque em cvm/ ou informe ticker_map_path).")


def _ensure_table(engine: Engine) -> None:
    ddl = """
    create schema if not exists cvm;

    create table if not exists cvm.demonstracoes_financeiras_tri (
        ticker text not null,
        data date not null,

        receita_liquida double precision,
        ebit double precision,
        lucro_liquido double precision,

        primary key (ticker, data)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _download_year_zip(ano: int, timeout_sec: int) -> bytes:
    url = URL_BASE_ITR + f"itr_cia_aberta_{ano}.zip"
    r = requests.get(url, timeout=timeout_sec)
    if r.status_code != 200:
        raise RuntimeError(f"Falha ao baixar ITR {ano}. HTTP {r.status_code}. URL={url}")
    return r.content


def _read_consolidated_csvs(zip_bytes: bytes) -> dict[str, pd.DataFrame]:
    out = {"DRE": []}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for name in z.namelist():
            if not (name.endswith(".csv") and "_con_" in name.lower()):
                continue
            u = name.upper()
            if "DRE" not in u:
                continue
            with z.open(name) as f:
                df = pd.read_csv(
                    f,
                    sep=";",
                    decimal=",",
                    encoding="ISO-8859-1",
                    low_memory=False,
                )
            if "ORDEM_EXERC" in df.columns:
                df = df[df["ORDEM_EXERC"] == "ÚLTIMO"]
            out["DRE"].append(df)

    return {"DRE": (pd.concat(out["DRE"], ignore_index=True) if out["DRE"] else pd.DataFrame())}


def _pick_value(df: pd.DataFrame, cd_conta: Optional[str] = None, ds_conta_in: Optional[list[str]] = None) -> pd.DataFrame:
    if df.empty:
        return df
    x = df.copy()
    if cd_conta is not None and "CD_CONTA" in x.columns:
        x = x[x["CD_CONTA"] == cd_conta]
    if ds_conta_in is not None and "DS_CONTA" in x.columns:
        x = x[x["DS_CONTA"].isin(ds_conta_in)]
    x = x.sort_values("DT_REFER").drop_duplicates(subset=["CD_CVM", "DT_REFER"], keep="first")
    return x[["CD_CVM", "DT_REFER", "VL_CONTA"]]


def _build_quarter_frame(df_dre: pd.DataFrame) -> pd.DataFrame:
    receita = _pick_value(df_dre, cd_conta="3.01").rename(columns={"VL_CONTA": "receita_liquida"})
    ebit = _pick_value(df_dre, cd_conta="3.05").rename(columns={"VL_CONTA": "ebit"})
    lucro = _pick_value(
        df_dre,
        ds_conta_in=[
            "Lucro/Prejuízo Consolidado do Período",
            "Lucro ou Prejuízo Líquido Consolidado do Período",
        ],
    ).rename(columns={"VL_CONTA": "lucro_liquido"})

    base = receita.merge(ebit, on=["CD_CVM", "DT_REFER"], how="outer").merge(lucro, on=["CD_CVM", "DT_REFER"], how="outer")
    if base.empty:
        return pd.DataFrame()

    base["data"] = pd.to_datetime(base["DT_REFER"]).dt.date
    base = base.drop(columns=["DT_REFER"])
    return base


def _load_cvm_to_ticker(map_path: Path) -> pd.DataFrame:
    df = pd.read_csv(map_path, sep=",", encoding="utf-8")
    cols = {c.lower(): c for c in df.columns}
    if "cd_cvm" not in cols or "ticker" not in cols:
        raise ValueError("cvm_to_ticker.csv precisa ter CD_CVM e Ticker.")
    df = df.rename(columns={cols["cd_cvm"]: "CD_CVM", cols["ticker"]: "ticker"})
    df["CD_CVM"] = pd.to_numeric(df["CD_CVM"], errors="coerce").astype("Int64")
    return df.dropna(subset=["CD_CVM", "ticker"])[["CD_CVM", "ticker"]].drop_duplicates()


def _upsert(engine: Engine, df: pd.DataFrame, batch_size: int = 8000) -> None:
    if df.empty:
        return

    sql = """
    insert into cvm.demonstracoes_financeiras_tri (
        ticker, data,
        receita_liquida, ebit, lucro_liquido
    ) values (
        :ticker, :data,
        :receita_liquida, :ebit, :lucro_liquido
    )
    on conflict (ticker, data) do update set
        receita_liquida = excluded.receita_liquida,
        ebit = excluded.ebit,
        lucro_liquido = excluded.lucro_liquido;
    """

    rows = df[["ticker", "data", "receita_liquida", "ebit", "lucro_liquido"]].to_dict(orient="records")
    with engine.begin() as conn:
        for i in range(0, len(rows), batch_size):
            conn.execute(text(sql), rows[i:i + batch_size])


def _yq_from_date(d: date) -> str:
    q = (d.month - 1) // 3 + 1
    return f"{d.year}{q}"


def _resolve_yq_to_process(engine: Engine, cfg: ItrConfig) -> Optional[Tuple[int, int]]:
    ensure_sync_table(engine)
    set_state(engine, STATE_TARGET_START, str(cfg.start_year))
    set_state(engine, STATE_TARGET_END, str(cfg.end_year))

    st_next = get_state(engine, STATE_NEXT_YQ)
    if not st_next or not st_next.get("value"):
        # começa do fim: end_year T4
        year, q = cfg.end_year, 4
    else:
        raw = str(st_next["value"]).strip()
        year = int(raw[:4])
        q = int(raw[4:5])

    if year < cfg.start_year:
        return None
    return year, q


def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
    start_year: int = DEFAULT_START_YEAR,
    end_year: int = DEFAULT_END_YEAR,
    quarters_per_run: int = 1,
    ticker_map_path: Optional[str] = None,
    timeout_sec: int = 120,
) -> None:
    cfg = ItrConfig(
        start_year=start_year,
        end_year=end_year,
        quarters_per_run=quarters_per_run,
        ticker_map_path=ticker_map_path,
        timeout_sec=timeout_sec,
    )

    _ensure_table(engine)
    map_path = _find_ticker_map(cfg.ticker_map_path)
    df_map = _load_cvm_to_ticker(map_path)

    for _ in range(cfg.quarters_per_run):
        yq = _resolve_yq_to_process(engine, cfg)
        if yq is None:
            if progress_cb:
                progress_cb("ITR já está completo dentro do range definido.")
            return

        year, q = yq
        if progress_cb:
            progress_cb(f"ITR: processando {year}T{q}...")

        zip_bytes = _download_year_zip(year, timeout_sec=cfg.timeout_sec)
        d = _read_consolidated_csvs(zip_bytes)

        df_all = _build_quarter_frame(d["DRE"])
        if df_all.empty:
            # marca e avança
            set_state(engine, STATE_LAST_DONE, f"{year}{q}")
            # decrementa trimestre
            q2 = q - 1
            y2 = year
            if q2 == 0:
                y2 -= 1
                q2 = 4
            set_state(engine, STATE_NEXT_YQ, f"{y2}{q2}")
            if progress_cb:
                progress_cb(f"ITR: {year}T{q} sem dados úteis (marcado como concluído).")
            continue

        # filtra para o trimestre alvo
        df_all["data"] = pd.to_datetime(df_all["data"])
        df_all["yq"] = df_all["data"].dt.to_period("Q").astype(str)  # ex "2025Q4"
        target = f"{year}Q{q}"
        df_q = df_all[df_all["yq"] == target].copy()
        df_q["data"] = df_q["data"].dt.date
        df_q = df_q.drop(columns=["yq"])

        if df_q.empty:
            set_state(engine, STATE_LAST_DONE, f"{year}{q}")
        else:
            df_q["CD_CVM"] = pd.to_numeric(df_q["CD_CVM"], errors="coerce").astype("Int64")
            df_q = df_q.merge(df_map, on="CD_CVM", how="inner")

            for c in ["receita_liquida", "ebit", "lucro_liquido"]:
                if c in df_q.columns:
                    df_q[c] = pd.to_numeric(df_q[c], errors="coerce")

            _upsert(engine, df_q)

            set_state(engine, STATE_LAST_DONE, f"{year}{q}")

        # próximo trimestre (retrocedendo)
        q2 = q - 1
        y2 = year
        if q2 == 0:
            y2 -= 1
            q2 = 4
        set_state(engine, STATE_NEXT_YQ, f"{y2}{q2}")

        if progress_cb:
            progress_cb(f"ITR: {year}T{q} concluído. Próximo: {y2}T{q2}")
