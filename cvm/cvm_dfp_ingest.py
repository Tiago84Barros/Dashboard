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

ULTIMO_ANO = 2026
URL_BASE_DFP = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
ULTIMO_ANO_DISPONIVEL = 2023


def _processar_ano_dfp(ano: int):
    url = URL_BASE_DFP + f"dfp_cia_aberta_{ano}.zip"
    response = requests.get(url, timeout=180)

    if response.status_code != 200:
        print(f"Erro ao baixar o arquivo para o ano {ano}")
        return None

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
        df_temp_dict = {"DRE": [], "BPA": [], "BPP": [], "DFC_MI": []}

        for arquivo in zip_ref.namelist():
            if arquivo.endswith(".csv") and "_con_" in arquivo:
                with zip_ref.open(arquivo) as csvfile:
                    df_temp = pd.read_csv(
                        csvfile,
                        sep=";",
                        decimal=",",
                        encoding="ISO-8859-1",
                        low_memory=False,
                    )

                    if "ORDEM_EXERC" in df_temp.columns:
                        df_temp = df_temp[df_temp["ORDEM_EXERC"] == "ÚLTIMO"]

                    up = arquivo.upper()
                    if "DRE" in up:
                        df_temp_dict["DRE"].append(df_temp)
                    elif "BPA" in up:
                        df_temp_dict["BPA"].append(df_temp)
                    elif "BPP" in up:
                        df_temp_dict["BPP"].append(df_temp)
                    elif "DFC" in up:
                        df_temp_dict["DFC_MI"].append(df_temp)

    return df_temp_dict


def _ensure_schema_only(engine: Engine):
    # Apenas garante o schema. NÃO cria tabela e muito menos apaga.
    with engine.begin() as conn:
        conn.execute(text("create schema if not exists cvm;"))


def _resolve_ticker_map_path(explicit: str | None) -> str:
    """
    Resolve o caminho do cvm_to_ticker.csv de forma robusta.
    Prioridade:
    1) parâmetro explícito
    2) env TICKER_MAP_PATH
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
    candidates.append(here / "cvm_to_ticker.csv")              # cvm/cvm_to_ticker.csv
    candidates.append(here.parent / "data" / "cvm_to_ticker.csv")  # data/cvm_to_ticker.csv

    for p in candidates:
        if p.exists():
            return str(p)

    raise FileNotFoundError(
        "Não encontrei o arquivo cvm_to_ticker.csv. Tente: "
        "1) setar TICKER_MAP_PATH no Streamlit Secrets/Environment, ou "
        "2) garantir que exista cvm/cvm_to_ticker.csv no repositório."
    )


def _load_cvm_to_ticker(path: str) -> pd.DataFrame:
    return pd.read_csv(path, sep=",", encoding="utf-8")


def run(
    engine: Engine,
    *,
    ultimo_ano: int = ULTIMO_ANO,
    ultimo_ano_disponivel: int = ULTIMO_ANO_DISPONIVEL,
    ticker_map_path: str | None = None,
) -> pd.DataFrame:
    _ensure_schema_only(engine)

    df_dict_dfp = {"DRE": pd.DataFrame(), "BPA": pd.DataFrame(), "BPP": pd.DataFrame(), "DFC_MI": pd.DataFrame()}

    with ThreadPoolExecutor(max_workers=8) as executor:
        df_results_dfp = list(executor.map(_processar_ano_dfp, range(2010, ultimo_ano)))

    for df_temp_dict in df_results_dfp:
        if df_temp_dict is None:
            continue
        for key in df_dict_dfp.keys():
            if df_temp_dict[key]:
                df_dict_dfp[key] = pd.concat([df_dict_dfp[key]] + df_temp_dict[key], ignore_index=True)

    if df_dict_dfp["DRE"].empty:
        return pd.DataFrame()

    empresas = df_dict_dfp["DRE"][["DENOM_CIA", "CD_CVM"]].drop_duplicates().set_index("CD_CVM")
    df_consolidado = pd.DataFrame()

    for CD_CVM, _ in empresas.iterrows():
        df_empresa = pd.DataFrame()

        empresa_dre = df_dict_dfp["DRE"][df_dict_dfp["DRE"]["CD_CVM"] == CD_CVM]
        empresa_bpa = df_dict_dfp["BPA"][df_dict_dfp["BPA"]["CD_CVM"] == CD_CVM]
        empresa_bpp = df_dict_dfp["BPP"][df_dict_dfp["BPP"]["CD_CVM"] == CD_CVM]
        empresa_dfc = df_dict_dfp["DFC_MI"][df_dict_dfp["DFC_MI"]["CD_CVM"] == CD_CVM]

        def _serie_cd(df, cd):
            d = df[df["CD_CONTA"] == cd]
            if d.empty:
                return pd.Series(dtype="float64")
            d = d.drop_duplicates(subset=["DT_REFER"], keep="first")
            d.index = pd.to_datetime(d["DT_REFER"])
            return pd.to_numeric(d["VL_CONTA"], errors="coerce")

        def _serie_desc(df, descs):
            d = df[df["DS_CONTA"].isin(descs)]
            if d.empty:
                return pd.Series(dtype="float64")
            d = d.drop_duplicates(subset=["DT_REFER"], keep="first")
            d.index = pd.to_datetime(d["DT_REFER"])
            return pd.to_numeric(d["VL_CONTA"], errors="coerce")

        receita = _serie_cd(empresa_dre, "3.01")
        ebit = _serie_cd(empresa_dre, "3.05")
        lucro = _serie_desc(
            empresa_dre,
            ["Lucro/Prejuízo Consolidado do Período", "Lucro ou Prejuízo Líquido Consolidado do Período"],
        )
        lpa = _serie_cd(empresa_dre, "3.99.01.01")

        ativo_total = _serie_cd(empresa_bpa, "1")
        ativo_circ = _serie_cd(empresa_bpa, "1.01")

        passivo_total = _serie_cd(empresa_bpp, "2")
        passivo_circ = _serie_cd(empresa_bpp, "2.01")
        patrimonio = _serie_cd(empresa_bpp, "2.02")

        dividendos = _serie_cd(empresa_dfc, "6.01")
        caixa_liq = _serie_cd(empresa_dfc, "6.01")  # mantendo a mesma lógica do seu notebook original

        divida_total = passivo_total
        divida_liquida = divida_total - caixa_liq

        base_idx = receita.index
        df_empresa = pd.DataFrame(
            {
                "CD_CVM": CD_CVM,
                "Data": base_idx,
                "Receita Líquida": receita.values,
                "Ebit": ebit.reindex(base_idx).values,
                "Lucro Líquido": lucro.reindex(base_idx).values,
                "Lucro por Ação": lpa.reindex(base_idx).values,
                "Ativo Total": ativo_total.reindex(base_idx).values,
                "Ativo Circulante": ativo_circ.reindex(base_idx).values,
                "Passivo Circulante": passivo_circ.reindex(base_idx).values,
                "Passivo Total": passivo_total.reindex(base_idx).values,
                "Divida Total": divida_total.reindex(base_idx).values,
                "Patrimônio Líquido": patrimonio.reindex(base_idx).values,
                "Dividendos Totais": dividendos.reindex(base_idx).values,
                "Caixa Líquido": caixa_liq.reindex(base_idx).values,
                "Dívida Líquida": divida_liquida.reindex(base_idx).values,
            }
        )

        df_consolidado = pd.concat([df_consolidado, df_empresa], ignore_index=True)

    map_path = _resolve_ticker_map_path(ticker_map_path)
    cvm_to_ticker = _load_cvm_to_ticker(map_path)

    df_consolidado = pd.merge(df_consolidado, cvm_to_ticker, left_on="CD_CVM", right_on="CVM")
    df_consolidado = df_consolidado.drop(columns=["CD_CVM", "CVM"])

    df_consolidado["Data"] = pd.to_datetime(df_consolidado["Data"]).dt.date

    # filtro de consistência (mantido)
    colunas_essenciais = ["Receita Líquida"]
    tickers_aprovados = []
    for ticker in df_consolidado["Ticker"].unique():
        df_empresa = df_consolidado[df_consolidado["Ticker"] == ticker]
        anos = sorted(pd.to_datetime(df_empresa["Data"]).dt.year.unique())
        if not anos:
            continue
        cont = anos == list(range(min(anos), max(anos) + 1))
        termina = max(anos) >= ultimo_ano_disponivel
        faltas = df_empresa[colunas_essenciais].isna().sum().sum()
        if cont and termina and (faltas / max(df_empresa.shape[0], 1) <= 0.1):
            tickers_aprovados.append(ticker)

    df_final = df_consolidado[df_consolidado["Ticker"].isin(tickers_aprovados)]
    df_final = df_final.where(pd.notnull(df_final), None)

    upsert_sql = """
    insert into cvm.demonstracoes_financeiras (
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

    payload = []
    for _, row in df_final.iterrows():
        payload.append(
            {
                "ticker": row["Ticker"],
                "data": row["Data"],
                "receita_liquida": row.get("Receita Líquida"),
                "ebit": row.get("Ebit"),
                "lucro_liquido": row.get("Lucro Líquido"),
                "lpa": row.get("Lucro por Ação"),
                "ativo_total": row.get("Ativo Total"),
                "ativo_circulante": row.get("Ativo Circulante"),
                "passivo_circulante": row.get("Passivo Circulante"),
                "passivo_total": row.get("Passivo Total"),
                "divida_total": row.get("Divida Total"),
                "patrimonio_liquido": row.get("Patrimônio Líquido"),
                "dividendos": row.get("Dividendos Totais"),
                "caixa_liquido": row.get("Caixa Líquido"),
                "divida_liquida": row.get("Dívida Líquida"),
            }
        )

    # batch para não explodir timeout/memória
    BATCH = 3000
    with engine.begin() as conn:
        for i in range(0, len(payload), BATCH):
            conn.execute(text(upsert_sql), payload[i : i + BATCH])

    return df_final
