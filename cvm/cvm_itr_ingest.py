# Antigo Algoritmo 5
from __future__ import annotations

import io
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ============================================================
# Configurações
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
# Infra banco (Supabase/Postgres) — SAFE (sem DROP)
# ============================================================
def _ensure_schema_and_table(engine: Engine):
    """
    Cria schema/tabela apenas se não existirem.
    - Sem DROP
    - Tabela/colunas em lowercase para evitar aspas no Postgres
    """
    ddl = """
    create schema if not exists cvm;

    create table if not exists cvm.demonstracoes_financeiras_tri (
        ticker text not null,
        data date not null,

        receita_liquida double precision,
        ebit double precision,
        lucro_liquido double precision,
        lpa double precision,

        ativo_total double precision,
        ativo_circulante double precision,
        passivo_circulante double precision,
        passivo_total double precision,

        divida_total double precision,
        patrimonio_liquido double precision,

        dividendos double precision,
        caixa_liquido double precision,
        divida_liquida double precision,

        primary key (ticker, data)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _resolve_ticker_map_path(explicit: str | None) -> str:
    """
    Resolve o caminho do cvm_to_ticker.csv de forma robusta.
    Prioridade:
    1) parâmetro explícito
    2) env TICKER_MAP_PATH (Streamlit Secrets)
    3) cvm/cvm_to_ticker.csv (mesma pasta do módulo)
    4) data/cvm_to_ticker.csv (fallback legado)
    """
    candidates: list[Path] = []

    if explicit:
        candidates.append(Path(explicit))

    env = os.getenv("TICKER_MAP_PATH")
    if env:
        candidates.append(Path(env))

    here = Path(__file__).resolve().parent
    candidates.append(here / "cvm_to_ticker.csv")                 # cvm/cvm_to_ticker.csv
    candidates.append(here.parent / "data" / "cvm_to_ticker.csv") # data/cvm_to_ticker.csv

    for p in candidates:
        if p.exists():
            return str(p)

    raise FileNotFoundError(
        "Não encontrei o arquivo cvm_to_ticker.csv. "
        "Defina TICKER_MAP_PATH nos Secrets do Streamlit "
        "ou garanta que exista cvm/cvm_to_ticker.csv no repositório."
    )


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
    ITR trimestral (consolidadas _con_)

    - Download ITR consolidado
    - Extração por contas (DRE, BPA, BPP, DFC)
    - Consolidação por empresa e data
    - Merge CVM → Ticker
    - Filtro de continuidade
    - Persistência em Supabase (UPSERT em lote)
    """

    _ensure_schema_and_table(engine)

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
    # 2) Consolidação
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
        caixa = serie_cd(dfc, "6.01")  # mantendo a mesma lógica do seu arquivo

        divida_total = passivo_total
        divida_liquida = divida_total - caixa

        idx = receita.index
        df_emp = pd.DataFrame(
            {
                "CD_CVM": cd_cvm,
                "Data": idx,
                "Receita Líquida": receita.values,
                "Ebit": ebit.reindex(idx).values,
                "Lucro Líquido": lucro.reindex(idx).values,
                "Lucro por Ação": lpa.reindex(idx).values,
                "Ativo Total": ativo_total.reindex(idx).values,
                "Ativo Circulante": ativo_circ.reindex(idx).values,
                "Passivo Circulante": passivo_circ.reindex(idx).values,
                "Passivo Total": passivo_total.reindex(idx).values,
                "Divida Total": divida_total.reindex(idx).values,
                "Patrimônio Líquido": patrimonio.reindex(idx).values,
                "Dividendos": dividendos.reindex(idx).values,
                "Caixa Líquido": caixa.reindex(idx).values,
                "Dívida Líquida": divida_liquida.reindex(idx).values,
            }
        )

        df_final = pd.concat([df_final, df_emp], ignore_index=True)

    # --------------------------------------------------------
    # 3) Merge CVM → Ticker
    # --------------------------------------------------------
    map_path = _resolve_ticker_map_path(ticker_map_path)
    mapa = _load_cvm_to_ticker(map_path)

    df_final = pd.merge(df_final, mapa, left_on="CD_CVM", right_on="CVM")
    df_final = df_final.drop(columns=["CD_CVM", "CVM"])
    df_final["Data"] = pd.to_datetime(df_final["Data"]).dt.date

    # --------------------------------------------------------
    # 4) Filtro de continuidade
    # --------------------------------------------------------
    tickers_ok = []

    for ticker in df_final["Ticker"].unique():
        df_t = df_final[df_final["Ticker"] == ticker]
        anos = sorted(pd.to_datetime(df_t["Data"]).dt.year.unique())
        if anos and anos == list(range(min(anos), max(anos) + 1)) and max(anos) >= ultimo_ano_disponivel:
            tickers_ok.append(ticker)

    df_final = df_final[df_final["Ticker"].isin(tickers_ok)]

    # --------------------------------------------------------
    # 5) Persistência (UPSERT em lote)
    # --------------------------------------------------------
    upsert = """
    insert into cvm.demonstracoes_financeiras_tri (
        ticker, data,
        receita_liquida, ebit, lucro_liquido, lpa,
        ativo_total, ativo_circulante, passivo_circulante, passivo_total,
        divida_total, patrimonio_liquido,
        dividendos, caixa_liquido, divida_liquida
    )
    values (
        :ticker, :data,
        :receita_liquida, :ebit, :lucro_liquido, :lpa,
        :ativo_total, :ativo_circulante, :passivo_circulante, :passivo_total,
        :divida_total, :patrimonio_liquido,
        :dividendos, :caixa_liquido, :divida_liquida
    )
    on conflict (ticker, data) do update set
        receita_liquida = excluded.receita_liquida,
        ebit = excluded.ebit,
        lucro_liquido = excluded.lucro_liquido,
        lpa = excluded.lpa,
        ativo_total = excluded.ativo_total,
        ativo_circulante = excluded.ativo_circulante,
        passivo_circulante = excluded.passivo_circulante,
        passivo_total = excluded.passivo_total,
        divida_total = excluded.divida_total,
        patrimonio_liquido = excluded.patrimonio_liquido,
        dividendos = excluded.dividendos,
        caixa_liquido = excluded.caixa_liquido,
        divida_liquida = excluded.divida_liquida;
    """

    df_final = df_final.where(pd.notnull(df_final), None)

    payload = []
    for _, r in df_final.iterrows():
        payload.append(
            {
                "ticker": r["Ticker"],
                "data": r["Data"],
                "receita_liquida": r["Receita Líquida"],
                "ebit": r["Ebit"],
                "lucro_liquido": r["Lucro Líquido"],
                "lpa": r["Lucro por Ação"],
                "ativo_total": r["Ativo Total"],
                "ativo_circulante": r["Ativo Circulante"],
                "passivo_circulante": r["Passivo Circulante"],
                "passivo_total": r["Passivo Total"],
                "divida_total": r["Divida Total"],
                "patrimonio_liquido": r["Patrimônio Líquido"],
                "dividendos": r["Dividendos"],
                "caixa_liquido": r["Caixa Líquido"],
                "divida_liquida": r["Dívida Líquida"],
            }
        )

    BATCH = 3000
    with engine.begin() as conn:
        for i in range(0, len(payload), BATCH):
            conn.execute(text(upsert), payload[i : i + BATCH])

    return df_final
