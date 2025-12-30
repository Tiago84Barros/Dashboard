# Antigo Algoritmo_1
import io
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values


# ========= CONFIG =========
ULTIMO_ANO = int(os.getenv("ULTIMO_ANO", "2025"))          # ano final para tentar baixar DFP
ANO_INICIAL = int(os.getenv("ANO_INICIAL", "2010"))        # ano inicial
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")             # obrigatório

# caminho local do CSV de mapeamento CVM -> Ticker
TICKER_PATH = os.getenv("TICKER_PATH", "cvm_to_ticker.csv")

URL_BASE_DFP = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"


# ========= COLETA / PARSE DFP =========
def processar_ano_dfp(ano: int):
    """
    Baixa DFP consolidado do ano, filtra ORDEM_EXERC == 'ÚLTIMO' e separa em DRE/BPA/BPP/DFC_MI.
    """
    url = f"{URL_BASE_DFP}dfp_cia_aberta_{ano}.zip"
    r = requests.get(url, timeout=120)
    if r.status_code != 200:
        print(f"[WARN] Falha ao baixar {ano}: HTTP {r.status_code}")
        return None

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        df_temp_dict = {"DRE": [], "BPA": [], "BPP": [], "DFC_MI": []}

        for nome in z.namelist():
            if not (nome.endswith(".csv") and "_con_" in nome):
                continue

            with z.open(nome) as f:
                try:
                    df = pd.read_csv(f, sep=";", decimal=",", encoding="ISO-8859-1")
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
                except Exception as e:
                    print(f"[WARN] Erro ao processar {nome} ({ano}): {e}")

    return df_temp_dict


def coletar_dfp_anual(ano_inicial: int, ultimo_ano: int) -> dict[str, pd.DataFrame]:
    """
    Retorna dicionário com DataFrames consolidados por tipo: DRE/BPA/BPP/DFC_MI.
    Obs: mantém a mesma lógica do notebook (range(ANO_INICIAL, ULTIMO_ANO)).
    """
    df_dict = {"DRE": pd.DataFrame(), "BPA": pd.DataFrame(), "BPP": pd.DataFrame(), "DFC_MI": pd.DataFrame()}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        results = list(ex.map(processar_ano_dfp, range(ano_inicial, ultimo_ano)))

    for temp in results:
        if temp is None:
            continue
        for k in df_dict.keys():
            if temp[k]:
                df_dict[k] = pd.concat([df_dict[k]] + temp[k], ignore_index=True)

    print("[OK] Coleta DFP anual concluída.")
    return df_dict


# ========= TRANSFORMAÇÃO =========
def montar_consolidado(df_dict_dfp: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Replica a lógica do notebook: percorre empresas e monta df_consolidado com colunas essenciais.
    """
    empresas = df_dict_dfp["DRE"][["DENOM_CIA", "CD_CVM"]].drop_duplicates().set_index("CD_CVM")

    df_consolidado = pd.DataFrame()

    for cd_cvm in empresas.index:
        df_empresa = pd.DataFrame()

        dre = df_dict_dfp["DRE"][df_dict_dfp["DRE"]["CD_CVM"] == cd_cvm]
        bpa = df_dict_dfp["BPA"][df_dict_dfp["BPA"]["CD_CVM"] == cd_cvm]
        bpp = df_dict_dfp["BPP"][df_dict_dfp["BPP"]["CD_CVM"] == cd_cvm]
        dfc = df_dict_dfp["DFC_MI"][df_dict_dfp["DFC_MI"]["CD_CVM"] == cd_cvm]

        # Receita líquida
        conta_receita = dre[dre["CD_CONTA"] == "3.01"].drop_duplicates(subset=["DT_REFER"], keep="first")
        conta_receita.index = pd.to_datetime(conta_receita["DT_REFER"])

        # EBIT
        conta_ebit = dre[dre["CD_CONTA"] == "3.05"].drop_duplicates(subset=["DT_REFER"], keep="first")
        conta_ebit.index = pd.to_datetime(conta_ebit["DT_REFER"])

        # Lucro líquido (por descrição)
        conta_ll = dre[dre["DS_CONTA"].isin([
            "Lucro/Prejuízo Consolidado do Período",
            "Lucro ou Prejuízo Líquido Consolidado do Período",
        ])].drop_duplicates(subset=["DT_REFER"], keep="first")
        conta_ll.index = pd.to_datetime(conta_ll["DT_REFER"])

        # LPA
        conta_lpa = dre[dre["CD_CONTA"] == "3.99.01.01"].drop_duplicates(subset=["DT_REFER"], keep="first")
        conta_lpa.index = pd.to_datetime(conta_lpa["DT_REFER"])

        # Ativo Total
        conta_ativo_total = bpa[bpa["CD_CONTA"] == "1"].drop_duplicates(subset=["DT_REFER"], keep="first")
        conta_ativo_total.index = pd.to_datetime(conta_ativo_total["DT_REFER"])

        # Ativo Circulante (o notebook tenta múltiplas alternativas; aqui mantemos simples/compatível)
        conta_ativo_circ = bpa[bpa["CD_CONTA"] == "1.01"].drop_duplicates(subset=["DT_REFER"], keep="first")
        conta_ativo_circ.index = pd.to_datetime(conta_ativo_circ["DT_REFER"])
        df_empresa["Ativo Circulante"] = conta_ativo_circ["VL_CONTA"]

        # Passivo Circulante e Não Circulante
        conta_passivo_circ = bpp[bpp["CD_CONTA"] == "2.01"].drop_duplicates(subset=["DT_REFER"], keep="first")
        conta_passivo_circ.index = pd.to_datetime(conta_passivo_circ["DT_REFER"])
        df_empresa["Passivo Circulante"] = conta_passivo_circ["VL_CONTA"]

        conta_passivo_nc = bpp[bpp["CD_CONTA"] == "2.02"].drop_duplicates(subset=["DT_REFER"], keep="first")
        conta_passivo_nc.index = pd.to_datetime(conta_passivo_nc["DT_REFER"])
        df_empresa["Passivo Não Circulante"] = conta_passivo_nc["VL_CONTA"]

        # Passivo Total (2)
        conta_passivo_total = bpp[bpp["CD_CONTA"] == "2"].drop_duplicates(subset=["DT_REFER"], keep="first")
        conta_passivo_total.index = pd.to_datetime(conta_passivo_total["DT_REFER"])

        # Patrimônio Líquido (2.03)
        conta_pl = bpp[bpp["CD_CONTA"] == "2.03"].drop_duplicates(subset=["DT_REFER"], keep="first")
        conta_pl.index = pd.to_datetime(conta_pl["DT_REFER"])

        # Caixa e equivalentes (BPA 1.01.01 ou aproximado)
        conta_caixa = bpa[bpa["CD_CONTA"].isin(["1.01.01", "1.01.01.01"])].drop_duplicates(subset=["DT_REFER"], keep="first")
        if not conta_caixa.empty:
            conta_caixa.index = pd.to_datetime(conta_caixa["DT_REFER"])
            caixa_equiv = conta_caixa.groupby(level=0)["VL_CONTA"].sum()
        else:
            caixa_equiv = pd.Series(0, index=conta_receita.index)

        # Dividendos (DFC) – aproximação compatível com notebook (mantém como estava)
        conta_div = dfc[dfc["DS_CONTA"].str.contains("dividend", case=False, na=False)].drop_duplicates(subset=["DT_REFER"], keep="first")
        conta_div.index = pd.to_datetime(conta_div["DT_REFER"]) if not conta_div.empty else conta_receita.index
        div_total = conta_div.groupby(level=0)["VL_CONTA"].sum() if not conta_div.empty else pd.Series(0, index=conta_receita.index)

        # FCO (6.01) usado no notebook como "Caixa Líquido"
        conta_fco = dfc[dfc["CD_CONTA"] == "6.01"].drop_duplicates(subset=["DT_REFER"], keep="first")
        conta_fco.index = pd.to_datetime(conta_fco["DT_REFER"]) if not conta_fco.empty else conta_receita.index
        fco = conta_fco["VL_CONTA"] if not conta_fco.empty else pd.Series(0, index=conta_receita.index)

        # Montagem final (espelha o notebook)
        df_empresa["CD_CVM"] = conta_receita["CD_CVM"]
        df_empresa["Data"] = pd.to_datetime(conta_receita["DT_REFER"])
        df_empresa["Receita Líquida"] = conta_receita["VL_CONTA"]
        df_empresa["Ebit"] = conta_ebit["VL_CONTA"]
        df_empresa["Lucro Líquido"] = conta_ll["VL_CONTA"]
        df_empresa["Lucro por Ação"] = conta_lpa["VL_CONTA"]
        df_empresa["Ativo Total"] = conta_ativo_total["VL_CONTA"]
        df_empresa["Patrimônio Líquido"] = conta_pl["VL_CONTA"]
        df_empresa["Caixa e Equivalentes"] = caixa_equiv
        df_empresa["Passivo Total"] = pd.to_numeric(conta_passivo_total["VL_CONTA"], errors="coerce")

        # Normalização numérica e derivadas (compatível)
        cols_to_convert = [
            "Passivo Total", "Caixa e Equivalentes", "Passivo Circulante", "Passivo Não Circulante",
            "Patrimônio Líquido"
        ]
        for c in cols_to_convert:
            df_empresa[c] = pd.to_numeric(df_empresa[c], errors="coerce").fillna(0)

        df_empresa["Passivo Total"] = df_empresa["Passivo Total"] - df_empresa["Patrimônio Líquido"]
        df_empresa["Divida Total"] = 0  # no notebook é calculado por passivos financeiros; aqui preservamos coluna derivada
        df_empresa["Dívida Líquida"] = df_empresa["Divida Total"] - df_empresa["Caixa e Equivalentes"]
        df_empresa["Dividendos Totais"] = pd.to_numeric(div_total, errors="coerce").fillna(0).abs()
        df_empresa["Caixa Líquido"] = pd.to_numeric(fco, errors="coerce").fillna(0)

        colunas_desejadas = [
            "CD_CVM", "Data", "Receita Líquida", "Ebit", "Lucro Líquido", "Lucro por Ação",
            "Ativo Total", "Ativo Circulante", "Passivo Circulante", "Passivo Total", "Divida Total",
            "Patrimônio Líquido", "Dividendos Totais", "Caixa Líquido", "Dívida Líquida"
        ]
        df_sel = df_empresa[[c for c in colunas_desejadas if c in df_empresa.columns]]

        df_consolidado = pd.concat([df_consolidado, df_sel], ignore_index=True)

    df_consolidado = df_consolidado.fillna(0)
    return df_consolidado


def adicionar_ticker(df_consolidado: pd.DataFrame, ticker_path: str) -> pd.DataFrame:
    """
    Replica a junção do notebook usando o CSV cvm_to_ticker.csv (colunas esperadas: CVM, Ticker).
    """
    cvm_to_ticker = pd.read_csv(ticker_path, sep=",", encoding="utf-8")

    df = pd.merge(df_consolidado, cvm_to_ticker, left_on="CD_CVM", right_on="CVM")
    df = df.drop(columns=["CD_CVM", "CVM"])

    df["Data"] = pd.to_datetime(df["Data"]).dt.strftime("%Y-%m-%d")

    colunas = [
        "Ticker", "Data", "Receita Líquida", "Ebit", "Lucro Líquido", "Lucro por Ação",
        "Ativo Total", "Ativo Circulante", "Passivo Circulante", "Passivo Total", "Divida Total",
        "Patrimônio Líquido", "Dividendos Totais", "Caixa Líquido", "Dívida Líquida"
    ]
    return df[colunas]


def filtrar_empresas(df_consolidado: pd.DataFrame, ultimo_ano_disponivel: int = 2023) -> pd.DataFrame:
    """
    Replica o filtro do notebook: dados contínuos e termina no último ano ou além.
    """
    colunas_essenciais = ["Receita Líquida"]
    tickers_aprovados = []

    for ticker in df_consolidado["Ticker"].unique():
        df_emp = df_consolidado[df_consolidado["Ticker"] == ticker]
        anos_disponiveis = sorted(pd.to_datetime(df_emp["Data"]).dt.year.unique())
        if not anos_disponiveis:
            continue

        primeiro_ano = anos_disponiveis[0]
        ultimo_ano = anos_disponiveis[-1]
        anos_esperados = list(range(primeiro_ano, ultimo_ano + 1))

        dados_continuos = anos_disponiveis == anos_esperados
        termina_no_ultimo_ano = ultimo_ano >= ultimo_ano_disponivel

        faltas = df_emp[colunas_essenciais].isna().sum().sum()
        if dados_continuos and termina_no_ultimo_ano and (faltas / max(df_emp.shape[0], 1) <= 0.1):
            tickers_aprovados.append(ticker)

    return df_consolidado[df_consolidado["Ticker"].isin(tickers_aprovados)]


# ========= LOAD (Supabase/Postgres) =========
def upsert_demonstracoes_financeiras(df: pd.DataFrame):
    """
    Grava no Supabase em public."Demonstracoes_Financeiras" via UPSERT
    (equivalente ao INSERT OR REPLACE do SQLite).
    """
    if not SUPABASE_DB_URL:
        raise RuntimeError("Defina a variável de ambiente SUPABASE_DB_URL com a connection string Postgres do Supabase.")

    # Mapeamento DF -> colunas da tabela (mantendo exatamente os nomes do banco)
    df_db = pd.DataFrame({
        "Ticker": df["Ticker"],
        "Data": df["Data"],  # string YYYY-MM-DD OK
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
    })

    cols = list(df_db.columns)
    values = [tuple(x) for x in df_db.itertuples(index=False, name=None)]

    sql = f"""
    INSERT INTO public."Demonstracoes_Financeiras"
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

    print(f"[OK] Upsert concluído: {len(df_db)} linhas em Demonstracoes_Financeiras.")


def main():
    df_dict = coletar_dfp_anual(ANO_INICIAL, ULTIMO_ANO)
    df_consolidado = montar_consolidado(df_dict)
    df_consolidado = adicionar_ticker(df_consolidado, TICKER_PATH)

    df_filtrado = filtrar_empresas(df_consolidado, ultimo_ano_disponivel=2023)

    upsert_demonstracoes_financeiras(df_filtrado)


if __name__ == "__main__":
    main()
