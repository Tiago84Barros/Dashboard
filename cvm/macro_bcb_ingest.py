# cvm/macro_bcb_ingest.py
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Callable, Optional, Dict

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine


# API pública SGS (BCB)
# Ex: https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json
SGS_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"


# Séries macro mais usadas (se alguma falhar, o job continua e registra)
# Observação: códigos SGS podem variar conforme o indicador escolhido.
# Se quiser trocar/expandir, edite este dicionário.
DEFAULT_SERIES: Dict[str, int] = {
    "SELIC_META_AA": 432,      # taxa Selic (meta) - série bem comum; se a sua não retornar, troque o código
    "IPCA_MENSAL": 433,        # IPCA variação mensal - idem
    "CAMBIO_USD_BRL": 1,       # câmbio comercial (pode variar); se falhar, troque o código
}


@dataclass(frozen=True)
class MacroConfig:
    start_date: dt.date
    end_date: dt.date
    timeout_sec: int = 60


def _ensure_table(engine: Engine) -> None:
    ddl = """
    create schema if not exists cvm;

    create table if not exists cvm.macro_bcb (
        series_name text not null,
        series_code integer not null,
        data date not null,
        valor double precision,
        primary key (series_code, data)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _fetch_sgs_series(code: int, cfg: MacroConfig) -> pd.DataFrame:
    params = {
        "formato": "json",
        "dataInicial": cfg.start_date.strftime("%d/%m/%Y"),
        "dataFinal": cfg.end_date.strftime("%d/%m/%Y"),
    }
    url = SGS_URL.format(code=code)

    r = requests.get(url, params=params, timeout=cfg.timeout_sec)
    if r.status_code != 200:
        raise RuntimeError(f"BCB SGS HTTP {r.status_code} para série {code}. URL={r.url}")

    data = r.json()
    if not isinstance(data, list) or len(data) == 0:
        return pd.DataFrame(columns=["data", "valor"])

    df = pd.DataFrame(data)
    # esperado: "data" (dd/mm/aaaa) e "valor" (string numérica)
    if "data" not in df.columns or "valor" not in df.columns:
        return pd.DataFrame(columns=["data", "valor"])

    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce").dt.date
    df["valor"] = pd.to_numeric(df["valor"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    df = df.dropna(subset=["data"])
    df = df.sort_values("data").drop_duplicates(subset=["data"], keep="last")

    return df[["data", "valor"]]


def _upsert(engine: Engine, df: pd.DataFrame, batch_size: int = 5000) -> None:
    if df.empty:
        return

    sql = """
    insert into cvm.macro_bcb (series_name, series_code, data, valor)
    values (:series_name, :series_code, :data, :valor)
    on conflict (series_code, data) do update set
        series_name = excluded.series_name,
        valor = excluded.valor;
    """

    rows = df.to_dict(orient="records")
    with engine.begin() as conn:
        for i in range(0, len(rows), batch_size):
            conn.execute(text(sql), rows[i:i + batch_size])


def run(
    engine: Engine,
    *,
    progress_cb: Optional[Callable[[str], None]] = None,
    timeout_sec: int = 60,
    # Janela padrão: últimos 10 anos (ajuste se quiser)
    years_back: int = 10,
    series: Optional[Dict[str, int]] = None,
) -> None:
    _ensure_table(engine)

    if series is None:
        series = DEFAULT_SERIES

    end_date = dt.date.today()
    start_date = dt.date(end_date.year - int(years_back), 1, 1)
    cfg = MacroConfig(start_date=start_date, end_date=end_date, timeout_sec=int(timeout_sec))

    ok = 0
    fail = 0

    for name, code in series.items():
        try:
            if progress_cb:
                progress_cb(f"MACRO: baixando {name} (SGS {code})...")

            df = _fetch_sgs_series(int(code), cfg)
            if df.empty:
                # não quebra o pipeline
                if progress_cb:
                    progress_cb(f"MACRO: {name} (SGS {code}) sem dados (ignorado).")
                continue

            df["series_name"] = str(name)
            df["series_code"] = int(code)

            _upsert(engine, df)
            ok += 1

            if progress_cb:
                progress_cb(f"MACRO: {name} (SGS {code}) concluído.")
        except Exception as e:
            fail += 1
            if progress_cb:
                progress_cb(f"MACRO: {name} (SGS {code}) falhou: {e}")

    if progress_cb:
        progress_cb(f"MACRO: finalizado. OK={ok}, FALHAS={fail}.")
