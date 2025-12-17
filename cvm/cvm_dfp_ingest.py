from __future__ import annotations

import io
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine


# -----------------------------
# Config (mantendo lógica do notebook)
# -----------------------------
ULTIMO_ANO = 2026  # Ano do último lançamento de dados (range(2010, ULTIMO_ANO))
URL_BASE_DFP = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
ULTIMO_ANO_DISPONIVEL = 2023  # usado no filtro de continuidade


def _processar_ano_dfp(ano: int):
    url = URL_BASE_DFP + f"dfp_cia_aberta_{ano}.zip"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Erro ao baixar o arquivo para o ano {ano}")
        return None

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
        df_temp_dict = {"DRE": [], "BPA": [], "BPP": [], "DFC_MI": []}

        for arquivo in zip_ref.namelist():
            if arquivo.endswith(".csv") and "_con_" in arquivo:  # Apenas arquivos consolidados
                with zip_ref.open(arquivo) as csvfile:
                    try:
                        df_temp = pd.read_csv(
                            csvfile,
                            sep=";",
                            decimal=",",
                            encoding="ISO-8859-1",
                        )

                        # Filtrar apenas registros marcados como "ÚLTIMO"
                        df_temp = df_temp[df_temp["ORDEM_EXERC"] == "ÚLTIMO"]

                        # Identificar tipo de arquivo e armazenar no dicionário temporário
                        if "DRE" in arquivo.upper():
                            df_temp_dict["DRE"].append(df_temp)
                        elif "BPA" in arquivo.upper():
                            df_temp_dict["BPA"].append(df_temp)
                        elif "BPP" in arquivo.upper():
                            df_temp_dict["BPP"].append(df_temp)
                        elif "DFC" in arquivo.upper():
                            df_temp_dict["DFC_MI"].append(df_temp)

                    except Exception as e:
                        print(f"Erro ao processar {arquivo} no ano {ano}: {e}")

    return df_temp_dict


def _ensure_table(engine: Engine):
    # Mantém a tabela final igual à do notebook, porém em schema cvm e Postgres types.
    ddl = """
    create schema if not exists cvm;

    drop table if exists cvm.Demonstracoes_Financeiras;

    create table if not exists cvm.Demonstracoes_Financeiras (
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


def run(
    engine: Engine,
    *,
    ultimo_ano: int = ULTIMO_ANO,
    ultimo_ano_disponivel: int = ULTIMO_ANO_DISPONIVEL,
    ticker_map_path: str | None = None,
) -> pd.DataFrame:
    """
    Algoritmo 1 convertido do notebook:
    - Baixa DFP (2010..ultimo_ano-1) em paralelo
    - Consolida DRE/BPA/BPP/DFC_MI
    - Extrai contas e monta df_consolidado
    - Faz merge com cvm_to_ticker e reorganiza colunas
    - Filtra empresas com dados contínuos e baixa % de faltas
    - Grava em cvm.Demonstracoes_Financeiras no Supabase
    Retorna df_consolidado_filtrado
    """

    # -----------------------------
    # 1) Coleta DFP em paralelo
    # -----------------------------
    df_dict_dfp = {"DRE": pd.DataFrame(), "BPA": pd.DataFrame(), "BPP": pd.DataFrame(), "DFC_MI": pd.DataFrame()}

    df_results_dfp = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        df_results_dfp = list(executor.map(_processar_ano_dfp, range(2010, ultimo_ano)))

    for df_temp_dict in df_results_dfp:
        if df_temp_dict is None:
            continue

        for key in df_dict_dfp.keys():
            if df_temp_dict[key]:
                df_dict_dfp[key] = pd.concat([df_dict_dfp[key]] + df_temp_dict[key], ignore_index=True)

    print("Coleta de dados anuais (DFP) concluída!")

    # -----------------------------
    # 2) Consolidação e extrações (mesma lógica do notebook)
    # -----------------------------
    empresas = df_dict_dfp["DRE"][["DENOM_CIA", "CD_CVM"]].drop_duplicates().set_index("CD_CVM")
    df_consolidado = pd.DataFrame()

    for CD_CVM, row in empresas.iterrows():
        df_empresa = pd.DataFrame()

        empresa_dre = df_dict_dfp["DRE"][df_dict_dfp["DRE"]["CD_CVM"] == CD_CVM]
        empresa_bpa = df_dict_dfp["BPA"][df_dict_dfp["BPA"]["CD_CVM"] == CD_CVM]
        empresa_bpp = df_dict_dfp["BPP"][df_dict_dfp["BPP"]["CD_CVM"] == CD_CVM]
        empresa_dfc = df_dict_dfp["DFC_MI"][df_dict_dfp["DFC_MI"]["CD_CVM"] == CD_CVM]

        # Receita Líquida
        conta_receita = empresa_dre[empresa_dre["CD_CONTA"] == "3.01"]
        conta_receita = conta_receita[~conta_receita.duplicated(subset=["DT_REFER"], keep="first")]
        conta_receita.index = pd.to_datetime(conta_receita["DT_REFER"])

        # EBIT
        conta_ebit = empresa_dre[empresa_dre["CD_CONTA"] == "3.05"]
        conta_ebit = conta_ebit[~conta_ebit.duplicated(subset=["DT_REFER"], keep="first")]
        conta_ebit.index = pd.to_datetime(conta_ebit["DT_REFER"])

        # Lucro Líquido (por DS_CONTA)
        conta_lucro_liquido = empresa_dre[
            empresa_dre["DS_CONTA"].isin(
                ["Lucro/Prejuízo Consolidado do Período", "Lucro ou Prejuízo Líquido Consolidado do Período"]
            )
        ]
        conta_lucro_liquido = conta_lucro_liquido[~conta_lucro_liquido.duplicated(subset=["DT_REFER"], keep="first")]
        conta_lucro_liquido.index = pd.to_datetime(conta_lucro_liquido["DT_REFER"])

        # LPA
        conta_LPA = empresa_dre[empresa_dre["CD_CONTA"] == "3.99.01.01"]
        conta_LPA = conta_LPA[~conta_LPA.duplicated(subset=["DT_REFER"], keep="first")]
        conta_LPA.index = pd.to_datetime(conta_LPA["DT_REFER"])

        # Ativo Total
        conta_ativo_total = empresa_bpa[empresa_bpa["CD_CONTA"] == "1"]
        conta_ativo_total = conta_ativo_total[~conta_ativo_total.duplicated(subset=["DT_REFER"], keep="first")]
        conta_ativo_total.index = pd.to_datetime(conta_ativo_total["DT_REFER"])

        # Ativo Circulante
        conta_ativo_circulante = empresa_bpa[empresa_bpa["CD_CONTA"] == "1.01"]
        conta_ativo_circulante = conta_ativo_circulante[~conta_ativo_circulante.duplicated(subset=["DT_REFER"], keep="first")]
        conta_ativo_circulante.index = pd.to_datetime(conta_ativo_circulante["DT_REFER"])

        # Passivo Circulante
        conta_passivo_circulante = empresa_bpp[empresa_bpp["CD_CONTA"] == "2.01"]
        conta_passivo_circulante = conta_passivo_circulante[~conta_passivo_circulante.duplicated(subset=["DT_REFER"], keep="first")]
        conta_passivo_circulante.index = pd.to_datetime(conta_passivo_circulante["DT_REFER"])

        # Passivo Total
        conta_passivo_total = empresa_bpp[empresa_bpp["CD_CONTA"] == "2"]
        conta_passivo_total = conta_passivo_total[~conta_passivo_total.duplicated(subset=["DT_REFER"], keep="first")]
        conta_passivo_total.index = pd.to_datetime(conta_passivo_total["DT_REFER"])

        # Patrimônio Líquido
        conta_patrimonio_liquido = empresa_bpp[empresa_bpp["CD_CONTA"] == "2.02"]
        conta_patrimonio_liquido = conta_patrimonio_liquido[~conta_patrimonio_liquido.duplicated(subset=["DT_REFER"], keep="first")]
        conta_patrimonio_liquido.index = pd.to_datetime(conta_patrimonio_liquido["DT_REFER"])

        # Caixa e equivalentes
        conta_caixa_e_equivalentes = empresa_bpa[empresa_bpa["CD_CONTA"] == "1.01"]
        conta_caixa_e_equivalentes = conta_caixa_e_equivalentes[~conta_caixa_e_equivalentes.duplicated(subset=["DT_REFER"], keep="first")]
        conta_caixa_e_equivalentes.index = pd.to_datetime(conta_caixa_e_equivalentes["DT_REFER"])

        # Dividendos (DFC)
        conta_dividendos = empresa_dfc[empresa_dfc["CD_CONTA"] == "6.01"]
        conta_dividendos = conta_dividendos[~conta_dividendos.duplicated(subset=["DT_REFER"], keep="first")]
        conta_dividendos.index = pd.to_datetime(conta_dividendos["DT_REFER"])

        conta_dividendos_Ncontroladores = empresa_dfc[empresa_dfc["CD_CONTA"] == "6.01"]
        conta_dividendos_Ncontroladores = conta_dividendos_Ncontroladores[
            ~conta_dividendos_Ncontroladores.duplicated(subset=["DT_REFER"], keep="first")
        ]
        conta_dividendos_Ncontroladores.index = pd.to_datetime(conta_dividendos_Ncontroladores["DT_REFER"])

        # FCO (Caixa Líquido no notebook)
        conta_FCO = empresa_dfc[empresa_dfc["CD_CONTA"] == "6.01"]
        conta_FCO = conta_FCO[~conta_FCO.duplicated(subset=["DT_REFER"], keep="first")]
        conta_FCO.index = pd.to_datetime(conta_FCO["DT_REFER"])

        # Dívidas (financeiras) — lógica do notebook soma contas por nível
        conta_passivo_circulante_financeiro = empresa_bpp[empresa_bpp["CD_CONTA"] == "2.01"]
        conta_passivo_circulante_financeiro = conta_passivo_circulante_financeiro[
            ~conta_passivo_circulante_financeiro.duplicated(subset=["DT_REFER"], keep="first")
        ]
        conta_passivo_circulante_financeiro.index = pd.to_datetime(conta_passivo_circulante_financeiro["DT_REFER"])

        conta_passivo_nao_circulante_financeiro = empresa_bpp[empresa_bpp["CD_CONTA"] == "2.02"]
        conta_passivo_nao_circulante_financeiro = conta_passivo_nao_circulante_financeiro[
            ~conta_passivo_nao_circulante_financeiro.duplicated(subset=["DT_REFER"], keep="first")
        ]
        conta_passivo_nao_circulante_financeiro.index = pd.to_datetime(conta_passivo_nao_circulante_financeiro["DT_REFER"])

        # Montagem df_empresa (mesma estrutura do notebook)
        df_empresa["CD_CVM"] = conta_receita["CD_CVM"]
        df_empresa["Data"] = pd.to_datetime(conta_receita["DT_REFER"])
        df_empresa["Receita Líquida"] = conta_receita["VL_CONTA"]
        df_empresa["Ebit"] = conta_ebit["VL_CONTA"]
        df_empresa["Lucro Líquido"] = conta_lucro_liquido["VL_CONTA"]
        df_empresa["Lucro por Ação"] = conta_LPA["VL_CONTA"]
        df_empresa["Ativo Total"] = conta_ativo_total["VL_CONTA"]
        df_empresa["Ativo Circulante"] = conta_ativo_circulante["VL_CONTA"]
        df_empresa["Passivo Circulante"] = conta_passivo_circulante["VL_CONTA"]
        df_empresa["Passivo Total"] = conta_passivo_total["VL_CONTA"]
        df_empresa["Patrimônio Líquido"] = conta_patrimonio_liquido["VL_CONTA"]
        df_empresa["Dividendos"] = conta_dividendos["VL_CONTA"]
        df_empresa["Dividendos Ncontroladores"] = conta_dividendos_Ncontroladores["VL_CONTA"]
        df_empresa["Caixa Líquido"] = conta_FCO["VL_CONTA"]

        df_empresa["Passivo Circulante Financeiro"] = conta_passivo_circulante_financeiro.groupby(level=0)["VL_CONTA"].sum()
        df_empresa["Passivo Não Circulante Financeiro"] = conta_passivo_nao_circulante_financeiro.groupby(level=0)["VL_CONTA"].sum()

        # Conversões numéricas (igual notebook)
        df_empresa["Passivo Total"] = pd.to_numeric(conta_passivo_total["VL_CONTA"], errors="coerce")
        df_empresa["Passivo Circulante"] = pd.to_numeric(df_empresa["Passivo Circulante"], errors="coerce")
        df_empresa["Passivo Circulante Financeiro"] = pd.to_numeric(
            conta_passivo_circulante_financeiro["VL_CONTA"], errors="coerce"
        )
        df_empresa["Passivo Não Circulante Financeiro"] = pd.to_numeric(
            conta_passivo_nao_circulante_financeiro["VL_CONTA"], errors="coerce"
        )
        df_empresa["Patrimônio Líquido"] = pd.to_numeric(conta_patrimonio_liquido["VL_CONTA"], errors="coerce")
        df_empresa["Caixa e Equivalentes"] = pd.to_numeric(df_empresa.get("Caixa e Equivalentes"), errors="coerce")
        df_empresa["Dividendos"] = pd.to_numeric(df_empresa["Dividendos"], errors="coerce")

        # Derivações finais (igual notebook)
        df_empresa["Divida Total"] = df_empresa["Passivo Circulante Financeiro"] + df_empresa["Passivo Não Circulante Financeiro"]
        df_empresa["Divida Total"] = pd.to_numeric(df_empresa["Divida Total"], errors="coerce")

        df_empresa["Dívida Líquida"] = df_empresa["Divida Total"] - pd.to_numeric(df_empresa["Caixa Líquido"], errors="coerce")
        df_empresa["Dívida Líquida"] = pd.to_numeric(df_empresa["Dívida Líquida"], errors="coerce")

        df_consolidado = pd.concat([df_consolidado, df_empresa], ignore_index=True)

    # -----------------------------
    # 3) Merge com cvm_to_ticker e reorganização (igual notebook)
    # -----------------------------
    if ticker_map_path is None:
        # padrão para rodar via projeto local
        ticker_map_path = os.getenv("TICKER_MAP_PATH", "data/cvm_to_ticker.csv")

    cvm_to_ticker = _load_cvm_to_ticker(ticker_map_path)

    df_consolidado = pd.merge(df_consolidado, cvm_to_ticker, left_on="CD_CVM", right_on="CVM")
    df_consolidado = df_consolidado.drop(columns=["CD_CVM", "CVM"])

    df_consolidado["Data"] = pd.to_datetime(df_consolidado["Data"])
    df_consolidado["Data"] = df_consolidado["Data"].dt.strftime("%Y-%m-%d")

    colunas = [
        "Ticker",
        "Data",
        "Receita Líquida",
        "Ebit",
        "Lucro Líquido",
        "Lucro por Ação",
        "Ativo Total",
        "Ativo Circulante",
        "Passivo Circulante",
        "Passivo Total",
        "Divida Total",
        "Patrimônio Líquido",
        "Dividendos",
        "Caixa Líquido",
        "Dívida Líquida",
    ]
    df_consolidado = df_consolidado[colunas]

    # Ajuste para manter compatibilidade com o insert do notebook:
    # no notebook, ao inserir usa 'Dividendos Totais'
    df_consolidado = df_consolidado.rename(columns={"Dividendos": "Dividendos Totais"})

    # -----------------------------
    # 4) Filtro de consistência (igual notebook)
    # -----------------------------
    colunas_essenciais = ["Receita Líquida"]
    tickers_aprovados = []

    for ticker in df_consolidado["Ticker"].unique():
        df_empresa = df_consolidado[df_consolidado["Ticker"] == ticker]

        anos_disponiveis = sorted(pd.to_datetime(df_empresa["Data"]).dt.year.unique())
        primeiro_ano = anos_disponiveis[0]
        ultimo_ano = anos_disponiveis[-1]
        anos_esperados = list(range(primeiro_ano, ultimo_ano + 1))

        print(f"\n🔎 Analisando {ticker}")
        print(f"  - Anos disponíveis: {anos_disponiveis}")
        print(f"  - Anos esperados: {anos_esperados}")
        print(f"  - Dados contínuos? {anos_disponiveis == anos_esperados}")

        dados_continuos = anos_disponiveis == anos_esperados
        termina_no_ultimo_ano = ultimo_ano >= ultimo_ano_disponivel

        print(f"  - Termina no último ano? {termina_no_ultimo_ano}")

        colunas_com_faltas = df_empresa[colunas_essenciais].isna().sum().sum()
        print(f"  - Valores ausentes nas colunas essenciais: {colunas_com_faltas}")

        if dados_continuos and termina_no_ultimo_ano and (colunas_com_faltas / df_empresa.shape[0] <= 0.1):
            tickers_aprovados.append(ticker)

    df_consolidado_filtrado = df_consolidado[df_consolidado["Ticker"].isin(tickers_aprovados)]

    # -----------------------------
    # 5) Gravação no Supabase (equivalente ao SQLite do notebook)
    # -----------------------------
    _ensure_table(engine)

    upsert_sql = """
    insert into cvm.Demonstracoes_Financeiras (
        Ticker, Data, Receita_Liquida, EBIT, Lucro_Liquido, LPA,
        Ativo_Total, Ativo_Circulante, Passivo_Circulante, Passivo_Total,
        Divida_Total, Patrimonio_Liquido, Dividendos, Caixa_Liquido, Divida_Liquida
    )
    values (
        :Ticker, :Data, :Receita_Liquida, :EBIT, :Lucro_Liquido, :LPA,
        :Ativo_Total, :Ativo_Circulante, :Passivo_Circulante, :Passivo_Total,
        :Divida_Total, :Patrimonio_Liquido, :Dividendos, :Caixa_Liquido, :Divida_Liquida
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

    payload = []
    for _, row in df_consolidado_filtrado.iterrows():
        payload.append(
            {
                "Ticker": row["Ticker"],
                "Data": row.get("Data"),
                "Receita_Liquida": row.get("Receita Líquida"),
                "EBIT": row.get("Ebit"),
                "Lucro_Liquido": row.get("Lucro Líquido"),
                "LPA": row.get("Lucro por Ação"),
                "Ativo_Total": row.get("Ativo Total"),
                "Ativo_Circulante": row.get("Ativo Circulante"),
                "Passivo_Circulante": row.get("Passivo Circulante"),
                "Passivo_Total": row.get("Passivo Total"),
                "Divida_Total": row.get("Divida Total"),
                "Patrimonio_Liquido": row.get("Patrimônio Líquido"),
                "Dividendos": row.get("Dividendos Totais"),
                "Caixa_Liquido": row.get("Caixa Líquido"),
                "Divida_Liquida": row.get("Dívida Líquida"),
            }
        )

    with engine.begin() as conn:
        if payload:
            conn.execute(text(upsert_sql), payload)

    return df_consolidado_filtrado
