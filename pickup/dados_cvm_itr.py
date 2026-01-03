import io
import os
import zipfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values


# ======================
# CONFIG
# ======================
URL_BASE = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"

ANO_INICIAL = int(os.getenv("ANO_INICIAL", "2010"))
ULTIMO_ANO = int(os.getenv("ULTIMO_ANO", "2025"))  # no notebook: Ultimo_ano = 2025 e range(2010, Ultimo_ano)
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))

ULTIMO_ANO_DISPONIVEL = int(os.getenv("ULTIMO_ANO_DISPONIVEL", "2023"))  # no notebook: 2023

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")  # obrigatório

BASE_DIR = Path(__file__).resolve().parent
TICKER_PATH = Path(os.getenv("TICKER_PATH", str(BASE_DIR / "cvm_to_ticker.csv")))


# ======================
# COLETA ITR (PARALELO)
# ======================
def processar_ano_itr(ano: int):
    url = URL_BASE + f"itr_cia_aberta_{ano}.zip"
    response = requests.get(url, timeout=180)
    if response.status_code != 200:
        print(f"[WARN] Falha no download {ano}: HTTP {response.status_code}")
        return None

    df_temp_dict = {"DRE": [], "BPA": [], "BPP": [], "DFC_MI": []}

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        for nome in z.namelist():
            # o notebook trabalha com CSVs consolidados: "_con_"
            if not (nome.endswith(".csv") and "_con_" in nome):
                continue

            with z.open(nome) as f:
                df = pd.read_csv(f, sep=";", decimal=",", encoding="ISO-8859-1")

            # filtro do notebook
            if "ORDEM_EXERC" in df.columns:
                df = df[df["ORDEM_EXERC"] == "ÚLTIMO"]

            upper = nome.upper()
            if "DRE" in upper:
                df_temp_dict["DRE"].append(df)
            elif "BPA" in upper:
                df_temp_dict["BPA"].append(df)
            elif "BPP" in upper:
                df_temp_dict["BPP"].append(df)
            elif "DFC" in upper:
                df_temp_dict["DFC_MI"].append(df)

    return df_temp_dict


def coletar_itr():
    df_dict_itr = {
        "DRE": pd.DataFrame(),
        "BPA": pd.DataFrame(),
        "BPP": pd.DataFrame(),
        "DFC_MI": pd.DataFrame(),
    }

    anos = range(ANO_INICIAL, ULTIMO_ANO)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(processar_ano_itr, anos))

    for df_temp_dict in results:
        if df_temp_dict is None:
            continue
        for key in df_dict_itr.keys():
            if df_temp_dict[key]:
                df_dict_itr[key] = pd.concat([df_dict_itr[key]] + df_temp_dict[key], ignore_index=True)

    return df_dict_itr


# ======================
# TRANSFORMAÇÃO (igual à estrutura do notebook)
# ======================
def montar_consolidado(df_dict_itr: dict) -> pd.DataFrame:
    empresas = df_dict_itr["DRE"][["DENOM_CIA", "CD_CVM"]].drop_duplicates().set_index("CD_CVM")
    df_consolidado = pd.DataFrame()

    for CD_CVM in empresas.index:
        df_empresa = pd.DataFrame()

        empresa_dre = df_dict_itr["DRE"][df_dict_itr["DRE"]["CD_CVM"] == CD_CVM]
        empresa_bpa = df_dict_itr["BPA"][df_dict_itr["BPA"]["CD_CVM"] == CD_CVM]
        empresa_bpp = df_dict_itr["BPP"][df_dict_itr["BPP"]["CD_CVM"] == CD_CVM]
        empresa_dfc = df_dict_itr["DFC_MI"][df_dict_itr["DFC_MI"]["CD_CVM"] == CD_CVM]

        # Receita / EBIT
        conta_receita = empresa_dre[empresa_dre["CD_CONTA"] == "3.01"]
        conta_receita = conta_receita[~conta_receita.duplicated(subset=["DT_REFER"], keep="first")]
        conta_receita.index = pd.to_datetime(conta_receita["DT_REFER"])

        conta_ebit = empresa_dre[empresa_dre["CD_CONTA"] == "3.05"]
        conta_ebit = conta_ebit[~conta_ebit.duplicated(subset=["DT_REFER"], keep="first")]
        conta_ebit.index = pd.to_datetime(conta_ebit["DT_REFER"])

        # Lucro líquido (por descrição)
        conta_lucro_liquido = empresa_dre[
            empresa_dre["DS_CONTA"].isin(
                [
                    "Lucro/Prejuízo Consolidado do Período",
                    "Lucro ou Prejuízo Líquido Consolidado do Período",
                ]
            )
        ]
        conta_lucro_liquido = conta_lucro_liquido[~conta_lucro_liquido.duplicated(subset=["DT_REFER"], keep="first")]
        conta_lucro_liquido.index = pd.to_datetime(conta_lucro_liquido["DT_REFER"])

        # LPA
        conta_LPA = empresa_dre[empresa_dre["CD_CONTA"] == "3.99.01.01"]
        conta_LPA = conta_LPA[~conta_LPA.duplicated(subset=["DT_REFER"], keep="first")]
        conta_LPA.index = pd.to_datetime(conta_LPA["DT_REFER"])

        # Ativo total
        conta_ativo_total = empresa_bpa[empresa_bpa["CD_CONTA"] == "1"]
        conta_ativo_total = conta_ativo_total[~conta_ativo_total.duplicated(subset=["DT_REFER"], keep="first")]
        conta_ativo_total.index = pd.to_datetime(conta_ativo_total["DT_REFER"])

        # Ativo circulante
        conta_ativo_circulante = empresa_bpa[empresa_bpa["CD_CONTA"] == "1.01"]
        conta_ativo_circulante = conta_ativo_circulante[~conta_ativo_circulante.duplicated(subset=["DT_REFER"], keep="first")]
        conta_ativo_circulante.index = pd.to_datetime(conta_ativo_circulante["DT_REFER"])
        df_empresa["Ativo Circulante"] = conta_ativo_circulante["VL_CONTA"]

        # Caixa e equivalentes
        conta_caixa_e_equivalentes = empresa_bpa[empresa_bpa["CD_CONTA"].isin(["1.01.01", "1.01.01.01"])]
        conta_caixa_e_equivalentes = conta_caixa_e_equivalentes[
            ~conta_caixa_e_equivalentes.duplicated(subset=["DT_REFER"], keep="first")
        ]
        conta_caixa_e_equivalentes.index = pd.to_datetime(conta_caixa_e_equivalentes["DT_REFER"])

        # Passivo circulante
        conta_passivo_circulante = empresa_bpp[empresa_bpp["CD_CONTA"] == "2.01"]
        conta_passivo_circulante = conta_passivo_circulante[
            ~conta_passivo_circulante.duplicated(subset=["DT_REFER"], keep="first")
        ]
        conta_passivo_circulante.index = pd.to_datetime(conta_passivo_circulante["DT_REFER"])
        df_empresa["Passivo Circulante"] = conta_passivo_circulante["VL_CONTA"]

        # Passivo total (2) e PL (2.03)
        conta_passivo_total = empresa_bpp[empresa_bpp["CD_CONTA"] == "2"]
        conta_passivo_total = conta_passivo_total[~conta_passivo_total.duplicated(subset=["DT_REFER"], keep="first")]
        conta_passivo_total.index = pd.to_datetime(conta_passivo_total["DT_REFER"])

        conta_patrimonio_liquido = empresa_bpp[empresa_bpp["CD_CONTA"] == "2.03"]
        conta_patrimonio_liquido = conta_patrimonio_liquido[
            ~conta_patrimonio_liquido.duplicated(subset=["DT_REFER"], keep="first")
        ]
        conta_patrimonio_liquido.index = pd.to_datetime(conta_patrimonio_liquido["DT_REFER"])

        # Passivo circulante financeiro / não circulante financeiro (como no notebook)
        conta_passivo_circ_fin = empresa_bpp[empresa_bpp["DS_CONTA"].str.contains("emprést", case=False, na=False)]
        conta_passivo_circ_fin = conta_passivo_circ_fin[
            ~conta_passivo_circ_fin.duplicated(subset=["DT_REFER"], keep="first")
        ]
        conta_passivo_circ_fin.index = pd.to_datetime(conta_passivo_circ_fin["DT_REFER"])
        df_empresa["Passivo Circulante Financeiro"] = conta_passivo_circ_fin["VL_CONTA"]

        conta_passivo_nc_fin = empresa_bpp[empresa_bpp["DS_CONTA"].str.contains("emprést", case=False, na=False)]
        conta_passivo_nc_fin = conta_passivo_nc_fin[
            ~conta_passivo_nc_fin.duplicated(subset=["DT_REFER"], keep="first")
        ]
        conta_passivo_nc_fin.index = pd.to_datetime(conta_passivo_nc_fin["DT_REFER"])
        df_empresa["Passivo Não Circulante Financeiro"] = conta_passivo_nc_fin["VL_CONTA"]

        # Dividendos (DFC): controladores e não controladores (como no notebook)
        conta_dividendos = empresa_dfc[empresa_dfc["DS_CONTA"].str.contains("dividend", case=False, na=False)]
        conta_dividendos = conta_dividendos[~conta_dividendos.duplicated(subset=["DT_REFER"], keep="first")]
        conta_dividendos.index = pd.to_datetime(conta_dividendos["DT_REFER"])

        conta_dividendos_Ncontroladores = empresa_dfc[
            empresa_dfc["DS_CONTA"].str.contains("n[oã]o control", case=False, na=False)
        ]
        conta_dividendos_Ncontroladores = conta_dividendos_Ncontroladores[
            ~conta_dividendos_Ncontroladores.duplicated(subset=["DT_REFER"], keep="first")
        ]
        conta_dividendos_Ncontroladores.index = pd.to_datetime(conta_dividendos_Ncontroladores["DT_REFER"])

        # FCO (6.01) -> Caixa Líquido (como no notebook)
        conta_FCO = empresa_dfc[empresa_dfc["CD_CONTA"] == "6.01"]
        conta_FCO = conta_FCO[~conta_FCO.duplicated(subset=["DT_REFER"], keep="first")]
        conta_FCO.index = pd.to_datetime(conta_FCO["DT_REFER"])

        # Montagem base
        df_empresa["CD_CVM"] = conta_receita["CD_CVM"]
        df_empresa["Data"] = pd.to_datetime(conta_receita["DT_REFER"])
        df_empresa["Receita Líquida"] = conta_receita["VL_CONTA"]
        df_empresa["Ebit"] = conta_ebit["VL_CONTA"]
        df_empresa["Lucro Líquido"] = conta_lucro_liquido["VL_CONTA"]
        df_empresa["Lucro por Ação"] = conta_LPA["VL_CONTA"]
        df_empresa["Ativo Total"] = conta_ativo_total["VL_CONTA"]

        df_empresa["Caixa e Equivalentes"] = conta_caixa_e_equivalentes["VL_CONTA"]
        df_empresa["Patrimônio Líquido"] = conta_patrimonio_liquido["VL_CONTA"]
        df_empresa["Passivo Total"] = conta_passivo_total["VL_CONTA"]

        df_empresa["Dividendos"] = conta_dividendos["VL_CONTA"]
        df_empresa["Dividendos Ncontroladores"] = conta_dividendos_Ncontroladores["VL_CONTA"]
        df_empresa["Caixa Líquido"] = conta_FCO["VL_CONTA"]

        # Normalização numérica (como no notebook)
        cols_to_convert = [
            "Passivo Total",
            "Caixa e Equivalentes",
            "Passivo Circulante Financeiro",
            "Passivo Não Circulante Financeiro",
            "Patrimônio Líquido",
            "Dividendos",
            "Dividendos Ncontroladores",
            "Caixa Líquido",
            "Ativo Circulante",
            "Passivo Circulante",
        ]
        for col in cols_to_convert:
            df_empresa[col] = pd.to_numeric(df_empresa[col], errors="coerce").fillna(0)

        # Derivações finais (as mesmas do notebook)
        df_empresa["Passivo Total"] = df_empresa["Passivo Total"] - df_empresa["Patrimônio Líquido"]
        df_empresa["Divida Total"] = df_empresa["Passivo Circulante Financeiro"] + df_empresa["Passivo Não Circulante Financeiro"]
        df_empresa["Dívida Líquida"] = df_empresa["Divida Total"] - df_empresa["Caixa e Equivalentes"]
        df_empresa["Dividendos Totais"] = (df_empresa["Dividendos"] + df_empresa["Dividendos Ncontroladores"]).abs()

        # Seleção final (15 colunas esperadas)
        colunas_desejadas = [
            "CD_CVM",
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
            "Dividendos Totais",
            "Caixa Líquido",
            "Dívida Líquida",
        ]

        df_empresa = df_empresa[colunas_desejadas].fillna(0)
        df_consolidado = pd.concat([df_consolidado, df_empresa], ignore_index=True)

    return df_consolidado.fillna(0)


def adicionar_ticker_e_filtrar(df_consolidado: pd.DataFrame) -> pd.DataFrame:
    cvm_to_ticker = pd.read_csv(TICKER_PATH, sep=",", encoding="utf-8")

    df_consolidado = pd.merge(df_consolidado, cvm_to_ticker, left_on="CD_CVM", right_on="CVM")
    df_consolidado = df_consolidado.drop(columns=["CD_CVM", "CVM"])

    df_consolidado["Data"] = pd.to_datetime(df_consolidado["Data"]).dt.strftime("%Y-%m-%d")

    colunas = [
        "Ticker", "Data",
        "Receita Líquida", "Ebit", "Lucro Líquido", "Lucro por Ação",
        "Ativo Total", "Ativo Circulante", "Passivo Circulante", "Passivo Total", "Divida Total",
        "Patrimônio Líquido", "Dividendos Totais", "Caixa Líquido", "Dívida Líquida",
    ]
    df_consolidado = df_consolidado[colunas]

    # Filtro do notebook (continuidade por ano + termina no último ano disponível + faltas <= 10%)
    colunas_essenciais = ["Receita Líquida"]
    tickers_aprovados = []

    for ticker in df_consolidado["Ticker"].unique():
        df_empresa = df_consolidado[df_consolidado["Ticker"] == ticker].copy()
        df_empresa["Ano"] = pd.to_datetime(df_empresa["Data"]).dt.year

        anos_disponiveis = sorted(df_empresa["Ano"].unique().tolist())
        if not anos_disponiveis:
            continue

        primeiro_ano = anos_disponiveis[0]
        ultimo_ano = anos_disponiveis[-1]
        anos_esperados = list(range(primeiro_ano, ultimo_ano + 1))

        dados_continuos = anos_disponiveis == anos_esperados
        termina_no_ultimo_ano = ultimo_ano >= ULTIMO_ANO_DISPONIVEL

        colunas_com_faltas = df_empresa[colunas_essenciais].isna().sum().sum()

        if dados_continuos and termina_no_ultimo_ano and (colunas_com_faltas / max(df_empresa.shape[0], 1) <= 0.1):
            tickers_aprovados.append(ticker)

    df_filtrado = df_consolidado[df_consolidado["Ticker"].isin(tickers_aprovados)].copy()
    return df_filtrado


# ======================
# LOAD SUPABASE (UPSERT)
# ======================
def upsert_supabase(df: pd.DataFrame) -> None:

    if not SUPABASE_DB_URL:
        raise RuntimeError("Defina SUPABASE_DB_URL com a connection string Postgres do Supabase.")

    # Mapeamento para os nomes EXATOS das colunas do Supabase
    df_db = pd.DataFrame({
        "Ticker": df["Ticker"],
        "Data": df["Data"],
        "Receita_Liquida": df["Receita Líquida"],
        "EBIT": df["Ebit"],
        "Lucro_Liquido": df["Lucro Líquido"],
        "LPA": df["Lucro por Ação"],
        "Ativo_Total": df["Ativo Total"],
        "Ativo_Circulante": df["Ativo Circulante"],
        "Passivo_Circulante": df["Passivo Circulante"],
        "Passivo_Total": df["Passivo Total"],
        "Divida_Total": df["Divida Total"],
        "Patrimonio_Liquido": df["Patrimônio Líquido"],
        "Dividendos": df["Dividendos Totais"],
        "Caixa_Liquido": df["Caixa Líquido"],
        "Divida_Liquida": df["Dívida Líquida"],
    }).fillna(0)

    # -------------------------
    # Deduplicação obrigatória
    # -------------------------
    
    # Normaliza Data
    df_db["Data"] = pd.to_datetime(df_db["Data"], errors="coerce").dt.date
    
    # Diagnóstico ANTES da deduplicação
    dup = df_db.duplicated(subset=["Ticker", "Data"]).sum()
    if dup:
        print(
            f"[WARN] Encontradas {dup} duplicatas de (Ticker, Data) "
            "no lote ITR/TRI. Mantendo a última ocorrência."
        )
    
    # Deduplicação efetiva
    df_db = (
        df_db
        .sort_values(["Ticker", "Data"])
        .drop_duplicates(subset=["Ticker", "Data"], keep="last")
        .reset_index(drop=True)
    )


    cols = list(df_db.columns)
    values = [tuple(x) for x in df_db.itertuples(index=False, name=None)]

    sql = f"""
    INSERT INTO public."Demonstracoes_Financeiras_TRI"
    ({", ".join([f'"{c}"' for c in cols])})
    VALUES %s
    ON CONFLICT ("Ticker","Data") DO UPDATE SET
      "Receita_Liquida" = EXCLUDED."Receita_Liquida",
      "EBIT" = EXCLUDED."EBIT",
      "Lucro_Liquido" = EXCLUDED."Lucro_Liquido",
      "LPA" = EXCLUDED."LPA",
      "Ativo_Total" = EXCLUDED."Ativo_Total",
      "Ativo_Circulante" = EXCLUDED."Ativo_Circulante",
      "Passivo_Circulante" = EXCLUDED."Passivo_Circulante",
      "Passivo_Total" = EXCLUDED."Passivo_Total",
      "Divida_Total" = EXCLUDED."Divida_Total",
      "Patrimonio_Liquido" = EXCLUDED."Patrimonio_Liquido",
      "Dividendos" = EXCLUDED."Dividendos",
      "Caixa_Liquido" = EXCLUDED."Caixa_Liquido",
      "Divida_Liquida" = EXCLUDED."Divida_Liquida"
    ;
    """

    with psycopg2.connect(SUPABASE_DB_URL) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=5000)
        conn.commit()

    print(f"[OK] Gravado no Supabase: {len(df_db)} linhas em Demonstracoes_Financeiras_TRI.")


def main():
    df_dict_itr = coletar_itr()
    df_consolidado = montar_consolidado(df_dict_itr)
    df_filtrado = adicionar_ticker_e_filtrar(df_consolidado)
    upsert_supabase(df_filtrado)


if __name__ == "__main__":
    main()
