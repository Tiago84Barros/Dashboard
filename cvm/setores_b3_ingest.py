from __future__ import annotations

import os
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


B3_URL = "https://www.b3.com.br/data/files/57/E6/AA/A1/68C7781064456178AC094EA8/ClassifSetorial.zip"


def _get_engine() -> Engine:
    # 1) Streamlit Secrets: st.secrets["SUPABASE_DB_URL"]
    # 2) Local/CI: env var SUPABASE_DB_URL
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL não configurada (ENV).")

    return create_engine(db_url, pool_pre_ping=True)


def _baixar_setores_b3() -> pd.DataFrame:
    r = requests.get(B3_URL, timeout=60)
    r.raise_for_status()

    with ZipFile(BytesIO(r.content)) as zf:
        xls_name = zf.namelist()[0]
        with zf.open(xls_name) as f:
            df = pd.read_excel(f, skiprows=6)

    # limpeza equivalente ao seu notebook
    df = df.rename(
        columns={
            "SETOR ECONÔMICO": "SETOR",
            "SEGMENTO": "NOME",
            "LISTAGEM": "CÓDIGO",
            "Unnamed: 4": "LISTAGEM",
        }
    )[1:-18]

    df.loc[df["CÓDIGO"].isnull(), "SEGMENTO"] = df.loc[df["CÓDIGO"].isnull(), "NOME"]
    df = df.dropna(how="all")
    df["LISTAGEM"] = df["LISTAGEM"].fillna("AUSENTE")

    df["SETOR"] = df["SETOR"].ffill()
    df["SUBSETOR"] = df["SUBSETOR"].ffill()
    df["SEGMENTO"] = df["SEGMENTO"].ffill()

    df = df.loc[(df["CÓDIGO"] != "CÓDIGO") & (df["CÓDIGO"] != "LISTAGEM") & (~df["CÓDIGO"].isnull())]

    # strip
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype(str).str.strip()

    df = df[["CÓDIGO", "NOME", "SETOR", "SUBSETOR", "SEGMENTO", "LISTAGEM"]].copy()
    df = df.rename(columns={"CÓDIGO": "ticker", "NOME": "nome_empresa"})

    # remove vazios
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df = df[df["ticker"].ne("")]

    return df.reset_index(drop=True)


def _carregar_mapeamento_cvm_to_ticker(engine: Engine) -> pd.DataFrame:
    """
    Você tinha isso no CSV do Drive. Aqui você tem 2 opções:

    A) Criar uma tabela no Supabase (recomendado) e carregar via SQL:
       cvm.cvm_to_ticker(ticker_base, ticker)

    B) Versionar um CSV no repositório e ler localmente.
    """
    # OPÇÃO A (tabela no Supabase):
    sql = "select ticker_base, ticker from cvm.cvm_to_ticker"
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


def _aplicar_mapeamento(df_setores: pd.DataFrame, df_map: pd.DataFrame) -> pd.DataFrame:
    # replica sua lógica: ticker_base = ticker sem número final
    df = df_setores.copy()
    df["ticker_base"] = df["ticker"].astype(str)

    m = df_map.copy()
    m["ticker_base"] = m["ticker_base"].astype(str).str.strip()
    m["ticker"] = m["ticker"].astype(str).str.strip()

    out = df.merge(m[["ticker_base", "ticker"]], on="ticker_base", how="left", suffixes=("", "_full"))
    out["ticker"] = out["ticker_full"].fillna(out["ticker"])
    out = out.drop(columns=["ticker_base", "ticker_full"], errors="ignore")

    # normaliza
    out["ticker"] = out["ticker"].astype(str).str.strip().str.upper()
    return out


def _filtrar_tickers_aprovados(engine: Engine, df: pd.DataFrame) -> pd.DataFrame:
    """
    Equivalente a: SELECT DISTINCT Ticker FROM Demonstracoes_Financeiras (SQLite).
    No Supabase, adapte para o nome real da sua tabela.
    Exemplo aqui: cvm.demonstracoes_financeiras (ajuste se for outro nome).
    """
    sql = "select distinct ticker from cvm.demonstracoes_financeiras"
    with engine.connect() as conn:
        aprovados = pd.read_sql(sql, conn)

    aprovados_set = set(aprovados["ticker"].astype(str).str.strip().str.upper().tolist())
    out = df.copy()
    out["ticker"] = out["ticker"].astype(str).str.strip().str.upper()
    return out[out["ticker"].isin(aprovados_set)].reset_index(drop=True)


def upsert_setores(engine: Engine, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    # garante colunas
    cols = ["ticker", "nome_empresa", "SETOR", "SUBSETOR", "SEGMENTO", "LISTAGEM"]
    # na transformação acima já temos "setor/subsetor/segmento/listagem" em maiúsculo?
    # aqui normaliza para colunas finais:
    df2 = df.rename(
        columns={
            "SETOR": "setor",
            "SUBSETOR": "subsetor",
            "SEGMENTO": "segmento",
            "LISTAGEM": "listagem",
        }
    ).copy()

    df2 = df2[["ticker", "nome_empresa", "setor", "subsetor", "segmento", "listagem"]].copy()
    df2 = df2.dropna(subset=["ticker"])
    df2["ticker"] = df2["ticker"].astype(str).str.strip().str.upper()

    rows = df2.to_dict(orient="records")

    sql = text(
        """
        insert into cvm.setores (ticker, nome_empresa, setor, subsetor, segmento, listagem, updated_at)
        values (:ticker, :nome_empresa, :setor, :subsetor, :segmento, :listagem, now())
        on conflict (ticker) do update
        set
          nome_empresa = excluded.nome_empresa,
          setor = excluded.setor,
          subsetor = excluded.subsetor,
          segmento = excluded.segmento,
          listagem = excluded.listagem,
          updated_at = now()
        """
    )

    with engine.begin() as conn:
        conn.execute(sql, rows)

    return len(rows)


def run_ingest() -> None:
    engine = _get_engine()

    df_b3 = _baixar_setores_b3()

    # se você não tiver cvm_to_ticker no Supabase ainda, comente o bloco abaixo
    try:
        df_map = _carregar_mapeamento_cvm_to_ticker(engine)
        df_b3 = _aplicar_mapeamento(df_b3, df_map)
    except Exception:
        pass

    # se você não quiser filtrar por aprovados, comente esse bloco
    try:
        df_b3 = _filtrar_tickers_aprovados(engine, df_b3)
    except Exception:
        pass

    n = upsert_setores(engine, df_b3)
    print(f"OK: {n} linhas upsert em cvm.setores")


if __name__ == "__main__":
    run_ingest()
