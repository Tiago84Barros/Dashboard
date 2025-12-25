import requests
import pandas as pd
from sqlalchemy import text
from core.db_supabase import get_engine
from core.macro_catalog import BCB_SERIES_CATALOG

BCB_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"

def fetch_series(codigo: int) -> pd.DataFrame:
    url = BCB_URL.format(codigo=codigo)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    df["data"] = pd.to_datetime(df["data"], dayfirst=True)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    return df[["data", "valor"]]

def ingest_macro_bcb():
    engine = get_engine()
    rows = []

    for series_name, meta in BCB_SERIES_CATALOG.items():
        df = fetch_series(meta["sgs"])
        df["series_name"] = series_name
        rows.append(df)

    full = pd.concat(rows, ignore_index=True)

    with engine.begin() as conn:
        conn.execute(text("""
            create table if not exists cvm.macro_bcb (
                data date not null,
                series_name text not null,
                valor double precision,
                primary key (data, series_name)
            );
        """))

        full.to_sql(
            "macro_bcb",
            con=conn,
            schema="cvm",
            if_exists="append",
            index=False,
            method="multi"
        )

def run(engine):
    ingest_macro_bcb(engine)

if __name__ == "__main__":
    ingest_macro_bcb()
