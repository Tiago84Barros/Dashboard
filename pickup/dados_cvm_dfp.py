import io
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values


# =========================
# CONFIG (equivalente ao notebook)
# =========================
ULTIMO_ANO = int(os.getenv("ULTIMO_ANO", "2025"))          # no notebook: Ultimo_ano = 2025
ANO_INICIAL = int(os.getenv("ANO_INICIAL", "2010"))        # no notebook: range(2010, Ultimo_ano)
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))

ULTIMO_ANO_DISPONIVEL = int(os.getenv("ULTIMO_ANO_DISPONIVEL", "2023"))  # no notebook: 2023

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")  # obrigatório

URL_BASE_DFP = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"

BASE_DIR = Path(__file__).resolve().parent
TICKER_PATH = Path(os.getenv("TICKER_PATH", str(BASE_DIR / "cvm_to_ticker.csv")))


# =========================
# COLETA DFP (PARALELISMO)
# =========================
def processar_ano_dfp(ano: int):
    url = URL_BASE_DFP + f"dfp_cia_aberta_{ano}.zip"
    response = requests.get(url, timeout=180)

    if response.status_code != 200:
        print(f"[WARN] Erro ao baixar o arquivo para o ano {ano}")
        return None

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
        df_temp_dict = {"DRE": [], "BPA": [], "BPP": [], "DFC_MI": []}

        for arquivo in zip_ref.namelist():
            if arquivo.endswith(".csv") and "_con_" in arquivo:
                with zip_ref.open(arquivo) as csvfile:
                    try:
                        df_temp = pd.read_csv(csvfile, sep=";", decimal=",", encoding="ISO-8859-1")
                        df_temp = df_temp[df_temp["ORDEM_EXERC"] == "ÚLTIMO"]

                        if "DRE" in arquivo.upper():
                            df_temp_dict["DRE"].append(df_temp)
                        elif "BPA" in arquivo.upper():
                            df_temp_dict["BPA"].append(df_temp)
                        elif "BPP" in arquivo.upper():
                            df_temp_dict["BPP"].append(df_temp)
                        elif "DFC" in arquivo.upper():
                            df_temp_dict["DFC_MI"].append(df_temp)

                    except Exception as e:
                        print(f"[WARN] Erro ao processar {arquivo} no ano {ano}: {e}")

    return df_temp_dict


def coletar_dfp():
    df_dict_dfp = {"DRE": pd.DataFrame(), "BPA": pd.DataFrame(), "BPP": pd.DataFrame(), "DFC_MI": pd.DataFrame()}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        df_results_dfp = list(executor.map(processar_ano_dfp, range(ANO_INICIAL, ULTIMO_ANO)))

    for df_temp_dict in df_results_dfp:
        if df_temp_dict is None:
            continue
        for key in df_dict_dfp.keys():
            if df_temp_dict[key]:
                df_dict_dfp[key] = pd.concat([df_dict_dfp[key]] + df_temp_dict[key], ignore_index=True)

    print("[OK] Coleta de dados anuais (DFP) concluída!")
    return df_dict_dfp

# =========================
# CONSOLIDAÇÃO (fiel ao notebook)
# =========================

def montar_df_consolidado(df_dict_dfp: dict) -> pd.DataFrame:
    empresas = (
        df_dict_dfp["DRE"][["DENOM_CIA", "CD_CVM"]]
        .drop_duplicates()
        .set_index("CD_CVM")
    )

    def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
        """Garante DT_REFER em datetime (sem alterar outras colunas)."""
        if df is None or df.empty:
            return df
        df = df.copy()
        df["DT_REFER"] = pd.to_datetime(df["DT_REFER"], errors="coerce")
        return df

    def _serie_conta(df_conta: pd.DataFrame, idx: pd.DatetimeIndex) -> pd.Series:
        """
        Converte um dataframe 'conta_*' em uma série numérica por data:
        - agrupa por DT_REFER (coluna), somando duplicatas
        - reindexa para idx
        - retorna float com NaN onde não existe
        """
        if df_conta is None or df_conta.empty:
            return pd.Series(index=idx, dtype="float64")

        dfc = df_conta[["DT_REFER", "VL_CONTA"]].copy()
        dfc["DT_REFER"] = pd.to_datetime(dfc["DT_REFER"], errors="coerce")
        dfc["VL_CONTA"] = pd.to_numeric(dfc["VL_CONTA"], errors="coerce")
        s = dfc.groupby("DT_REFER", dropna=True)["VL_CONTA"].sum()
        return s.reindex(idx)

    def _idx_base(conta_receita: pd.DataFrame,
                  empresa_bpa: pd.DataFrame,
                  empresa_dre: pd.DataFrame,
                  empresa_bpp: pd.DataFrame,
                  empresa_dfc: pd.DataFrame) -> pd.DatetimeIndex:
        """Escolhe um índice de datas robusto e ordenado."""
        if conta_receita is not None and not conta_receita.empty:
            idx = pd.to_datetime(conta_receita["DT_REFER"].unique(), errors="coerce")
        else:
            bpa_ativo_total = empresa_bpa[empresa_bpa["CD_CONTA"] == "1"] if empresa_bpa is not None else pd.DataFrame()
            if bpa_ativo_total is not None and not bpa_ativo_total.empty:
                idx = pd.to_datetime(bpa_ativo_total["DT_REFER"].unique(), errors="coerce")
            else:
                idx = None
                for _df in [empresa_dre, empresa_bpa, empresa_bpp, empresa_dfc]:
                    if _df is not None and not _df.empty:
                        idx = pd.to_datetime(_df["DT_REFER"].unique(), errors="coerce")
                        break
                if idx is None:
                    return pd.DatetimeIndex([])
        idx = pd.DatetimeIndex(idx).dropna().unique().sort_values()
        return idx

    df_consolidado = pd.DataFrame()

    for CD_CVM in empresas.index:
        empresa_dre = _to_dt(df_dict_dfp["DRE"][df_dict_dfp["DRE"]["CD_CVM"] == CD_CVM])
        empresa_bpa = _to_dt(df_dict_dfp["BPA"][df_dict_dfp["BPA"]["CD_CVM"] == CD_CVM])
        empresa_bpp = _to_dt(df_dict_dfp["BPP"][df_dict_dfp["BPP"]["CD_CVM"] == CD_CVM])
        empresa_dfc = _to_dt(df_dict_dfp["DFC_MI"][df_dict_dfp["DFC_MI"]["CD_CVM"] == CD_CVM])

        # Receita base (3.01)
        conta_receita = empresa_dre[empresa_dre["CD_CONTA"] == "3.01"] if empresa_dre is not None else pd.DataFrame()

        idx = _idx_base(conta_receita, empresa_bpa, empresa_dre, empresa_bpp, empresa_dfc)
        if len(idx) == 0:
            continue

        df_empresa = pd.DataFrame(index=idx)
        df_empresa.index.name = "DT_REFER"

        # ========= DRE =========
        conta_ebit = empresa_dre[empresa_dre["CD_CONTA"] == "3.05"]
        conta_lucro_liquido = empresa_dre[
            empresa_dre["DS_CONTA"].isin([
                "Lucro/Prejuízo Consolidado do Período",
                "Lucro ou Prejuízo Líquido Consolidado do Período"
            ])
        ]
        conta_lpa = empresa_dre[empresa_dre["CD_CONTA"] == "3.99.01.01"]

        # ========= BPA =========
        conta_ativo_total = empresa_bpa[empresa_bpa["CD_CONTA"] == "1"]

        # Ativo Circulante
        bpa_101 = empresa_bpa[empresa_bpa["CD_CONTA"] == "1.01"]
        if (bpa_101 is not None) and (not bpa_101.empty) and ("Ativo Circulante" in bpa_101["DS_CONTA"].values):
            conta_ativo_circulante = bpa_101
        else:
            conta_ativo_circulante = empresa_bpa[empresa_bpa["DS_CONTA"].isin([
                "Caixa e Equivalentes de Caixa",
                "Caixa",
                "Aplicações de Liquidez",
                "Ativos Financeiros Avaliados ao Valor Justo através do Resultado",
                "Ativos Financeiros Avaliados ao Valor Justo através de Outros Resultados Abrangentes",
                "Aplicações em Depósitos Interfinanceiros",
                "Aplicações no Mercado Aberto",
                "Derivativos",
                "Imposto de Renda e Contribuição Social - Correntes"
            ])]

        # Ativo Não Circulante
        bpa_102 = empresa_bpa[empresa_bpa["CD_CONTA"] == "1.02"]
        if (bpa_102 is not None) and (not bpa_102.empty) and ("Ativo Não Circulante" in bpa_102["DS_CONTA"].values):
            conta_ativo_nao_circulante = bpa_102
        else:
            conta_ativo_nao_circulante = empresa_bpa[empresa_bpa["DS_CONTA"].isin([
                "Depósito Compulsório Banco Central",
                "Imposto de Renda e Contribuição Social - Diferidos",
                "Ativos Não Correntes a Venda",
                "Ativos de Operações Descontinuadas",
                "Investimentos",
                "Participações em Coligadas",
                "Propriedades para Investimento",
                "Imobilizado",
                "Intangível",
                "Goodwill",
                "Depreciação Acumulada",
                "Amortização Acumulada"
            ])]

        conta_caixa_e_equivalentes = empresa_bpa[empresa_bpa["DS_CONTA"] == "Caixa e Equivalentes de Caixa"]

        # ========= BPP =========
        # Passivo Circulante
        bpp_201 = empresa_bpp[empresa_bpp["CD_CONTA"] == "2.01"]
        if (bpp_201 is not None) and (not bpp_201.empty) and ("Passivo Circulante" in bpp_201["DS_CONTA"].values):
            conta_passivo_circulante = bpp_201
        else:
            conta_passivo_circulante = empresa_bpp[empresa_bpp["DS_CONTA"].isin([
                "Passivos Financeiros Avaliados ao Valor Justo através do Resultado",
                "Passivos Financeiros ao Custo Amortizado",
                "Depósitos",
                "Captações no Mercado Aberto",
                "Recursos Mercado Interfinanceiro",
                "Outras Captações",
                "Obrigações por emissão de títulos e valores mobiliários e outras obrigações",
                "Outros passivos financeiros",
                "Provisões",
                "Provisões trabalhistas, fiscais e cíveis"
            ])]

        # Passivo Não Circulante
        bpp_202 = empresa_bpp[empresa_bpp["CD_CONTA"] == "2.02"]
        if (bpp_202 is not None) and (not bpp_202.empty) and ("Passivo Não Circulante" in bpp_202["DS_CONTA"].values):
            conta_passivo_nao_circulante = bpp_202
        else:
            conta_passivo_nao_circulante = empresa_bpp[empresa_bpp["DS_CONTA"].isin([
                "Obrigações de Longo Prazo",
                "Provisões",
                "Passivos por Impostos Diferidos",
                "Passivos por Contrato de Seguros e Previdência Complementar",
                "Outros Passivos"
            ])]

        # PL (mantive seu critério)
        conta_patrimonio_liquido = empresa_bpp[empresa_bpp["DS_CONTA"].isin(["Patrimônio Líquido Consolidado"])]

        # Passivo financeiro (mantive seu critério startswith)
        conta_passivo_circulante_financeiro = empresa_bpp[empresa_bpp["CD_CONTA"].astype(str).str.startswith("2.01.04", na=False)]
        conta_passivo_nao_circulante_financeiro = empresa_bpp[empresa_bpp["CD_CONTA"].astype(str).str.startswith("2.02.01", na=False)]

        # Passivo total (CD_CONTA 2)
        conta_passivo_total = empresa_bpp[empresa_bpp["CD_CONTA"] == "2"]

        # ========= DFC =========
        conta_dividendos = empresa_dfc[empresa_dfc["DS_CONTA"].isin([
            "Dividendos",
            "Dividendos pagos",
            "Dividendos Pagos",
            "Pagamento de Dividendos",
            "Pagamento de Dividendos e JCP",
            "Pagamentos de Dividendos e JCP",
            "Pagamento de dividendos e JCP",
            "Dividendos Pagos a Acionistas",
            "Dividendos/JCP Pagos a Acionistas",
            "JCP e dividendos pagos e acionistas",
            "Dividendos e Juros s/Capital Próprio",
            "Pgto de Dividendos/Juros s/ Capital Próprio",
            "Dividendos e Juros sobre o Capital Próprio Pagos",
            "Dividendos e juros sobre o capital próprio pagos",
            "Dividendos e Juros Sobre Capital Próprios Pagos",
            "Dividendos ou Juros sobre Capital Próprio Pagos",
            "Pagamento de Dividendos e juros sobre capital próprio",
            "Pagamento de Dividendos e Juros s/Capital Próprio",
            "Pagamento de dividendos e juros sobre o capital próprio",
            "Dividendos ou juros sobre o capital próprio pagos aos acionistas controladores"
        ])]
        conta_dividendos_nctrl = empresa_dfc[
            empresa_dfc["DS_CONTA"].isin(["Dividendos ou juros sobre o capital próprio pagos aos acionistas não controladores"])
        ]
        conta_fco = empresa_dfc[empresa_dfc["CD_CONTA"] == "6.01"]

        # ========= Montagem (sempre via _serie_conta) =========
        df_empresa["CD_CVM"] = CD_CVM
        df_empresa["Data"] = df_empresa.index  # 1 linha por data, consistente

        df_empresa["Receita Líquida"] = _serie_conta(conta_receita, idx)
        df_empresa["Ebit"] = _serie_conta(conta_ebit, idx)
        df_empresa["Lucro Líquido"] = _serie_conta(conta_lucro_liquido, idx)
        df_empresa["Lucro por Ação"] = _serie_conta(conta_lpa, idx)

        df_empresa["Ativo Total"] = _serie_conta(conta_ativo_total, idx)
        df_empresa["Ativo Circulante"] = _serie_conta(conta_ativo_circulante, idx)
        df_empresa["Ativo Não Circulante"] = _serie_conta(conta_ativo_nao_circulante, idx)

        df_empresa["Caixa e Equivalentes"] = _serie_conta(conta_caixa_e_equivalentes, idx)

        df_empresa["Passivo Circulante"] = _serie_conta(conta_passivo_circulante, idx)
        df_empresa["Passivo Não Circulante"] = _serie_conta(conta_passivo_nao_circulante, idx)

        df_empresa["Patrimônio Líquido"] = _serie_conta(conta_patrimonio_liquido, idx)

        df_empresa["Passivo Total"] = _serie_conta(conta_passivo_total, idx)

        df_empresa["Passivo Circulante Financeiro"] = _serie_conta(conta_passivo_circulante_financeiro, idx)
        df_empresa["Passivo Não Circulante Financeiro"] = _serie_conta(conta_passivo_nao_circulante_financeiro, idx)

        df_empresa["Dividendos"] = _serie_conta(conta_dividendos, idx)
        df_empresa["Dividendos Ncontroladores"] = _serie_conta(conta_dividendos_nctrl, idx)

        df_empresa["Caixa Líquido"] = _serie_conta(conta_fco, idx)

        # ========= Numérico + fill =========
        cols_to_convert = [
            "Receita Líquida","Ebit","Lucro Líquido","Lucro por Ação",
            "Ativo Total","Ativo Circulante","Passivo Circulante","Passivo Total",
            "Passivo Circulante Financeiro","Passivo Não Circulante Financeiro",
            "Caixa e Equivalentes","Dividendos","Dividendos Ncontroladores",
            "Patrimônio Líquido","Caixa Líquido"
        ]
        for col in cols_to_convert:
            df_empresa[col] = pd.to_numeric(df_empresa[col], errors="coerce").fillna(0)

        # ========= Derivadas (mantidas) =========
        # Mantém sua interpretação: Passivo Total "exigível" = (CD_CONTA 2) - PL
        df_empresa["Passivo Total"] = df_empresa["Passivo Total"] - df_empresa["Patrimônio Líquido"]

        df_empresa["Divida Total"] = (
            df_empresa["Passivo Circulante Financeiro"] + df_empresa["Passivo Não Circulante Financeiro"]
        )
        df_empresa["Dívida Líquida"] = df_empresa["Divida Total"] - df_empresa["Caixa e Equivalentes"]
        df_empresa["Dividendos Totais"] = (df_empresa["Dividendos"] + df_empresa["Dividendos Ncontroladores"]).abs()

        # ========= Seleção final =========
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
        colunas_existentes = [c for c in colunas_desejadas if c in df_empresa.columns]

        df_selecionado = (
            df_empresa[colunas_existentes]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0)
            .reset_index(drop=True)  # evita carregar DT_REFER como índice no consolidado
        )

        df_consolidado = pd.concat([df_consolidado, df_selecionado], ignore_index=True)

    return df_consolidado.fillna(0)


# =========================
# TICKER + REORDENAÇÃO
# =========================
def adicionar_ticker(df_consolidado: pd.DataFrame) -> pd.DataFrame:
    if not TICKER_PATH.exists():
        raise FileNotFoundError(f"Não encontrei o arquivo CVM->Ticker em: {TICKER_PATH}")

    cvm_to_ticker = pd.read_csv(TICKER_PATH, sep=",", encoding="utf-8")

    expected_columns = [
        "CD_CVM", "Data", "Receita Líquida", "Ebit", "Lucro Líquido", "Lucro por Ação",
        "Ativo Total", "Ativo Circulante", "Passivo Circulante", "Passivo Total", "Divida Total",
        "Patrimônio Líquido", "Dividendos Totais", "Caixa Líquido", "Dívida Líquida"
    ]
    # (no notebook a lista é usada como “verificação”; mantemos a referência sem bloquear execução)

    df = pd.merge(df_consolidado, cvm_to_ticker, left_on="CD_CVM", right_on="CVM")
    df = df.drop(columns=["CD_CVM", "CVM"])

    df["Data"] = pd.to_datetime(df["Data"]).dt.strftime("%Y-%m-%d")

    colunas = [
        "Ticker", "Data", "Receita Líquida", "Ebit", "Lucro Líquido", "Lucro por Ação",
        "Ativo Total", "Ativo Circulante", "Passivo Circulante", "Passivo Total", "Divida Total",
        "Patrimônio Líquido", "Dividendos Totais", "Caixa Líquido", "Dívida Líquida"
    ]
    return df[colunas]


# =========================
# FILTRO DE EMPRESAS (fiel ao notebook)
# =========================
def filtrar_empresas(df_consolidado: pd.DataFrame) -> pd.DataFrame:
    colunas_essenciais = ["Receita Líquida"]
    tickers_aprovados = []

    for ticker in df_consolidado["Ticker"].unique():
        df_empresa = df_consolidado[df_consolidado["Ticker"] == ticker]

        anos_disponiveis = sorted(pd.to_datetime(df_empresa["Data"]).dt.year.unique())
        if not anos_disponiveis:
            continue

        primeiro_ano = anos_disponiveis[0]
        ultimo_ano = anos_disponiveis[-1]
        anos_esperados = list(range(primeiro_ano, ultimo_ano + 1))

        # logs do notebook (mantidos)
        print(f"\n🔎 Analisando {ticker}")
        print(f"  - Anos disponíveis: {anos_disponiveis}")
        print(f"  - Anos esperados: {anos_esperados}")
        print(f"  - Dados contínuos? {anos_disponiveis == anos_esperados}")

        dados_continuos = anos_disponiveis == anos_esperados
        termina_no_ultimo_ano = ultimo_ano >= ULTIMO_ANO_DISPONIVEL

        print(f"  - Termina no último ano? {termina_no_ultimo_ano}")

        colunas_com_faltas = df_empresa[colunas_essenciais].isna().sum().sum()
        print(f"  - Valores ausentes nas colunas essenciais: {colunas_com_faltas}")

        if dados_continuos and termina_no_ultimo_ano and (colunas_com_faltas / df_empresa.shape[0] <= 0.1):
            tickers_aprovados.append(ticker)

    return df_consolidado[df_consolidado["Ticker"].isin(tickers_aprovados)].copy()


# =========================
# GRAVAÇÃO NO SUPABASE (equivalente ao INSERT OR REPLACE)
# =========================
def upsert_supabase_demonstracoes_financeiras(df_filtrado: pd.DataFrame) -> None:
    if not SUPABASE_DB_URL:
        raise RuntimeError("Defina SUPABASE_DB_URL (connection string Postgres do Supabase).")

    # Mapeamento fiel ao INSERT do SQLite:
    # (Ticker, Data, Receita_Liquida, EBIT, Lucro_Liquido, LPA, Ativo_Total, Ativo_Circulante, Passivo_Circulante,
    #  Passivo_Total, Divida_Total, Patrimonio_Liquido, Dividendos, Caixa_Liquido, Divida_Liquida)
    df_db = pd.DataFrame({
        "Ticker": df_filtrado["Ticker"],
        "Data": df_filtrado["Data"],
        "Receita_Liquida": df_filtrado["Receita Líquida"],
        "EBIT": df_filtrado["Ebit"],
        "Lucro_Liquido": df_filtrado["Lucro Líquido"],
        "LPA": df_filtrado["Lucro por Ação"],
        "Ativo_Total": df_filtrado["Ativo Total"],
        "Ativo_Circulante": df_filtrado["Ativo Circulante"],
        "Passivo_Circulante": df_filtrado["Passivo Circulante"],
        "Passivo_Total": df_filtrado["Passivo Total"],
        "Divida_Total": df_filtrado["Divida Total"],
        "Patrimonio_Liquido": df_filtrado["Patrimônio Líquido"],
        "Dividendos": df_filtrado["Dividendos Totais"],
        "Caixa_Liquido": df_filtrado["Caixa Líquido"],
        "Divida_Liquida": df_filtrado["Dívida Líquida"],
    }).fillna(0)

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
            # =========================
            # DIAGNÓSTICO DE OVERFLOW
            # =========================
            df_debug = df_filtrado.copy()
            
            # garantir tipos numéricos
            for col in df_debug.columns:
                if col not in ["Ticker", "Data"]:
                    df_debug[col] = pd.to_numeric(df_debug[col], errors="coerce")
            
            # limite conservador (bem abaixo do erro do Postgres)
            LIMITE = 1e13  # 10 trilhões
            
            numericos = df_debug.drop(columns=["Ticker", "Data"])
            max_por_coluna = numericos.abs().max().sort_values(ascending=False)
            
            print("\n=== DIAGNÓSTICO OVERFLOW ===")
            print("Top 15 maiores valores absolutos por coluna:")
            print(max_por_coluna.head(15))
            
            colunas_problema = max_por_coluna[max_por_coluna > LIMITE].index.tolist()
            
            if colunas_problema:
                print("\nColunas com valores acima do limite:", colunas_problema)
            
                linhas_problema = df_debug[
                    numericos[colunas_problema].abs().max(axis=1) > LIMITE
                ]
            
                print("\nExemplos de linhas problemáticas:")
                print(
                    linhas_problema[
                        ["Ticker", "Data"] + colunas_problema
                    ].head(20)
                )
            
                raise ValueError(
                    "ABORTADO PROPOSITALMENTE: valores financeiros fora de escala detectados. "
                    "Verifique o log acima."
                )

            execute_values(cur, sql, values, page_size=5000)
        conn.commit()

    print(f"[OK] Upsert concluído: {len(df_db)} linhas em Demonstracoes_Financeiras.")


def main():
    df_dict_dfp = coletar_dfp()
    df_consolidado = montar_df_consolidado(df_dict_dfp)
    df_consolidado = adicionar_ticker(df_consolidado)
    df_filtrado = filtrar_empresas(df_consolidado)
    upsert_supabase_demonstracoes_financeiras(df_filtrado)


if __name__ == "__main__":
    main()
