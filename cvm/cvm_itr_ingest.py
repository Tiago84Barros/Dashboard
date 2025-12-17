# Antigo Algoritmo_5

from __future__ import annotations

import io
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ============================================================
# Configurações (mantidas do notebook)
# ============================================================
URL_BASE_ITR = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"
ANO_INICIAL = 2010
ULTIMO_ANO = 2026            # range(ANO_INICIAL, ULTIMO_ANO)
ULTIMO_ANO_DISPONIVEL = 2025


# ============================================================
# Download e leitura
# ============================================================
def _baixar_zip_itr(ano: int) -> bytes | None:
    url = f"{URL_BASE_ITR}itr_cia_aberta_{ano}.zip"
    r = requests.get(url, timeout=180)
    if r.status_code != 200:
        return None
    return r.content


def _processar_ano_itr(ano: int):
    content = _baixar_zip_itr(ano)
    if content is None:
        return None

    dfs = {"DRE": [], "BPA": [], "BPP": [], "DFC_MI": []}

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        for nome in z.namelist():
            if not (nome.endswith(".csv") and "_con_" in nome.lower()):
                continue

            with z.open(nome) as f:
                df = pd.read_csv(
                    f,
                    sep=";",
                    decimal=",",
                    encoding="ISO-8859-1",
                    low_memory=False,
                )

                if "ORDEM_EXERC" in df.columns:
                    df = df[df["ORDEM_EXERC"] == "ÚLTIMO"]

                nome_u = nome.upper()
                if "DRE" in nome_u:
                    dfs["DRE"].append(df)
                elif "BPA" in nome_u:
                    dfs["BPA"].append(df)
                elif "BPP" in nome_u:
                    dfs["BPP"].append(df)
                elif "DFC" in nome_u:
                    dfs["DFC_MI"].append(df)

    return dfs


# ============================================================
# Infra banco (Supabase)
# ============================================================
def _ensure_table(engine: Engine):
    ddl = """
    create schema if not exists cvm;

    create table if not exists cvm.Demonstracoes_Financeiras_TRI (
        Ticker text not null,
        Data date not null,

        Receita_Liquida double precision,
        EBIT double precision,
        Lucro_Liquido double precision,
        LPA double precision,

        Ativo_Total double precision,
        Ativo_Circulante double precision,
        Passivo_Circulante double precision,
        Passivo_Total double precision,

        Divida_Total double precision,
        Patrimonio_Liquido double precision,

        Dividendos double precision,
        Caixa_Liquido double precision,
        Divida_Liquida double precision,

        primary key (Ticker, Data)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _load_cvm_to_ticker(path: str) -> pd.DataFrame:
    return pd.read_csv(path, sep=",", encoding="utf-8")


# ============================================================
# Função principal (pipeline)
# ============================================================
def run(
    engine: Engine,
    *,
    ano_inicial: int = ANO_INICIAL,
    ultimo_ano: int = ULTIMO_ANO,
    ultimo_ano_disponivel: int = ULTIMO_ANO_DISPONIVEL,
    ticker_map_path: str | None = None,
) -> pd.DataFrame:
    """
    Algoritmo 5 — ITR trimestral (conversão fiel do notebook)

    - Download ITR consolidado (_con_)
    - Extração por contas (DRE, BPA, BPP, DFC)
    - Consolidação por empresa e data
    - Merge CVM → Ticker
    - Filtro de continuidade
    - Persistência em Supabase (UPSERT)
    """

    _ensure_table(engine)

    # --------------------------------------------------------
    # 1) Coleta paralela
    # --------------------------------------------------------
    dados = {"DRE": pd.DataFrame(), "BPA": pd.DataFrame(), "BPP": pd.DataFrame(), "DFC_MI": pd.DataFrame()}

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = executor.map(_processar_ano_itr, range(ano_inicial, ultimo_ano))

    for r in results:
        if r is None:
            continue
        for k in dados:
            if r[k]:
                dados[k] = pd.concat([dados[k]] + r[k], ignore_index=True)

    if dados["DRE"].empty:
        return pd.DataFrame()

    # --------------------------------------------------------
    # 2) Consolidação (mesma lógica do notebook)
    # --------------------------------------------------------
    empresas = dados["DRE"][["DENOM_CIA", "CD_CVM"]].drop_duplicates().set_index("CD_CVM")
    df_final = pd.DataFrame()

    for cd_cvm, _ in empresas.iterrows():
        dre = dados["DRE"][dados["DRE"]["CD_CVM"] == cd_cvm]
        bpa = dados["BPA"][dados["BPA"]["CD_CVM"] == cd_cvm]
        bpp = dados["BPP"][dados["BPP"]["CD_CVM"] == cd_cvm]
        dfc = dados["DFC_MI"][dados["DFC_MI"]["CD_CVM"] == cd_cvm]

        def serie_cd(df, cd):
            d = df[df["CD_CONTA"] == cd]
            if d.empty:
                return pd.Series(dtype="float64")
            d = d.drop_duplicates(subset=["DT_REFER"])
            d.index = pd.to_datetime(d["DT_REFER"])
            return pd.to_numeric(d["VL_CONTA"], errors="coerce")

        def serie_desc(df, descs):
            d = df[df["DS_CONTA"].isin(descs)]
            if d.empty:
                return pd.Series(dtype="float64")
            d = d.drop_duplicates(subset=["DT_REFER"])
            d.index = pd.to_datetime(d["DT_REFER"])
            return pd.to_numeric(d["VL_CONTA"], errors="coerce")

        receita = serie_cd(dre, "3.01")
        ebit = serie_cd(dre, "3.05")
        lucro = serie_desc(
            dre,
            [
                "Lucro/Prejuízo Consolidado do Período",
                "Lucro ou Prejuízo Líquido Consolidado do Período",
            ],
        )
        lpa = serie_cd(dre, "3.99.01.01")

        ativo_total = serie_cd(bpa, "1")
        ativo_circ = serie_cd(bpa, "1.01")

        passivo_total = serie_cd(bpp, "2")
        passivo_circ = serie_cd(bpp, "2.01")
        patrimonio = serie_cd(bpp, "2.02")

        dividendos = serie_cd(dfc, "6.01")
        caixa = serie_cd(dfc, "6.01")

        divida_total = passivo_total
        divida_liquida = divida_total - caixa

        df_emp = pd.DataFrame(
            {
                "CD_CVM": cd_cvm,
                "Data": receita.index,
                "Receita Líquida": receita.values,
                "Ebit": ebit.reindex(receita.index).values,
                "Lucro Líquido": lucro.reindex(receita.index).values,
                "Lucro por Ação": lpa.reindex(receita.index).values,
                "Ativo Total": ativo_total.reindex(receita.index).values,
                "Ativo Circulante": ativo_circ.reindex(receita.index).values,
                "Passivo Circulante": passivo_circ.reindex(receita.index).values,
                "Passivo Total": passivo_total.reindex(receita.index).values,
                "Divida Total": divida_total.reindex(receita.index).values,
                "Patrimônio Líquido": patrimonio.reindex(receita.index).values,
                "Dividendos": dividendos.reindex(receita.index).values,
                "Caixa Líquido": caixa.reindex(receita.index).values,
                "Dívida Líquida": divida_liquida.reindex(receita.index).values,
            }
        )

        df_final = pd.concat([df_final, df_emp], ignore_index=True)

    # --------------------------------------------------------
    # 3) Merge CVM → Ticker
    # --------------------------------------------------------
    if ticker_map_path is None:
        ticker_map_path = os.getenv("TICKER_MAP_PATH", "data/cvm_to_ticker.csv")

    mapa = _load_cvm_to_ticker(ticker_map_path)
    df_final = pd.merge(df_final, mapa, left_on="CD_CVM", right_on="CVM")
    df_final = df_final.drop(columns=["CD_CVM", "CVM"])
    df_final["Data"] = pd.to_datetime(df_final["Data"]).dt.date

    # --------------------------------------------------------
    # 4) Filtro de continuidade (igual notebook)
    # --------------------------------------------------------
    tickers_ok = []

    for ticker in df_final["Ticker"].unique():
        df_t = df_final[df_final["Ticker"] == ticker]
        anos = sorted(pd.to_datetime(df_t["Data"]).dt.year.unique())
        if anos == list(range(min(anos), max(anos) + 1)) and max(anos) >= ultimo_ano_disponivel:
            tickers_ok.append(ticker)

    df_final = df_final[df_final["Ticker"].isin(tickers_ok)]

    # --------------------------------------------------------
    # 5) Persistência (UPSERT)
    # --------------------------------------------------------
    upsert = """
    insert into cvm.Demonstracoes_Financeiras_TRI (
        Ticker, Data,
        Receita_Liquida, EBIT, Lucro_Liquido, LPA,
        Ativo_Total, Ativo_Circulante, Passivo_Circulante, Passivo_Total,
        Divida_Total, Patrimonio_Liquido,
        Dividendos, Caixa_Liquido, Divida_Liquida
    )
    values (
        :Ticker, :Data,
        :Receita_Liquida, :EBIT, :Lucro_Liquido, :LPA,
        :Ativo_Total, :Ativo_Circulante, :Passivo_Circulante, :Passivo_Total,
        :Divida_Total, :Patrimonio_Liquido,
        :Dividendos, :Caixa_Liquido, :Divida_Liquida
    )
    on conflict (Ticker, Data) do update set
        Receita_Liquida = excluded.Receita_Liquida,
        EBIT = excluded.EBIT,
        Lucro_Liquido = excluded.Lucro_Liquido,
        LPA = excluded.LPA,
        Ativo_Total = excluded.Ativo_Total,
        Ativo_Circulante = excluded.Ativo_Circulante,
        Passivo_Circulante = excluded.Passivo_Circulante,
        Passivo_Total = excluded.Passivo_Total,
        Divida_Total = excluded.Divida_Total,
        Patrimonio_Liquido = excluded.Patrimonio_Liquido,
        Dividendos = excluded.Dividendos,
        Caixa_Liquido = excluded.Caixa_Liquido,
        Divida_Liquida = excluded.Divida_Liquida;
    """

    df_final = df_final.where(pd.notnull(df_final), None)

    payload = []
    for _, r in df_final.iterrows():
        payload.append(
            {
                "Ticker": r["Ticker"],
                "Data": r["Data"],
                "Receita_Liquida": r["Receita Líquida"],
                "EBIT": r["Ebit"],
                "Lucro_Liquido": r["Lucro Líquido"],
                "LPA": r["Lucro por Ação"],
                "Ativo_Total": r["Ativo Total"],
                "Ativo_Circulante": r["Ativo Circulante"],
                "Passivo_Circulante": r["Passivo Circulante"],
                "Passivo_Total": r["Passivo Total"],
                "Divida_Total": r["Divida Total"],
                "Patrimonio_Liquido": r["Patrimônio Líquido"],
                "Dividendos": r["Dividendos"],
                "Caixa_Liquido": r["Caixa Líquido"],
                "Divida_Liquida": r["Dívida Líquida"],
            }
        )

    with engine.begin() as conn:
        if payload:
            conn.execute(text(upsert), payload)

    return df_final
