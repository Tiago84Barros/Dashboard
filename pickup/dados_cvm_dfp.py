# pickup/dados_cvm_dfp.py
import io
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values

# ATENÇÃO:
# 'Dividendos' aqui representam valores CONTÁBEIS TOTAIS (DFP/CVM).
# NÃO são dividendos por ação e NÃO devem ser usados em backtests de reinvestimento.

# =========================
# CONFIG
# =========================
URL_BASE_DFP = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
ANO_INICIAL = int(os.getenv("ANO_INICIAL", "2010"))

# Se ULTIMO_ANO for definido no ambiente, respeita. Se não, descobre automaticamente.
ULTIMO_ANO = int(os.getenv("ULTIMO_ANO", "0"))  # 0 => auto

# usado no filtro original (mantido)
ULTIMO_ANO_DISPONIVEL = int(os.getenv("ULTIMO_ANO_DISPONIVEL", "2023"))

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "").strip()  # obrigatório

BASE_DIR = Path(__file__).resolve().parent
TICKER_PATH = Path(os.getenv("TICKER_PATH", str(BASE_DIR / "cvm_to_ticker.csv")))

# Limites de segurança para evitar overflow do Postgres numeric(20,6): |x| < 1e14
LPA_ABS_MAX_DB = 1e14 - 1  # margem


# =========================
# UTIL: descobrir último ano disponível na CVM (robusto)
# =========================
def _ultimo_ano_disponivel(prefix: str, ano_max: int | None = None, max_back: int = 12) -> int:
    if ano_max is None:
        ano_max = datetime.now().year

    for ano in range(ano_max, ano_max - max_back - 1, -1):
        url = URL_BASE_DFP + f"{prefix}_{ano}.zip"
        try:
            r = requests.head(url, timeout=20, allow_redirects=True)
            if r.status_code == 200:
                return ano
        except requests.RequestException:
            pass

    return ano_max - max_back


if ULTIMO_ANO <= 0:
    ULTIMO_ANO = _ultimo_ano_disponivel("dfp_cia_aberta", ano_max=datetime.now().year, max_back=12)


# =========================
# NORMALIZAÇÃO DE ESCALA (CVM) — COM EXCEÇÃO PARA CONTAS POR AÇÃO
# =========================
def _normalizar_vl_conta_por_escala(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza VL_CONTA usando ESCALA_MOEDA quando disponível.

    CRÍTICO:
    - NÃO aplica multiplicador em contas por ação (ex.: CD_CONTA começando com '3.99'),
      pois isso gera distorções e pode causar overflow em LPA.
    """
    if df is None or df.empty:
        return df
    if "VL_CONTA" not in df.columns or "ESCALA_MOEDA" not in df.columns:
        return df

    out = df.copy()
    out["VL_CONTA"] = pd.to_numeric(out["VL_CONTA"], errors="coerce")

    escala = out["ESCALA_MOEDA"].astype(str).str.strip().str.upper()

    fatores = pd.Series(1.0, index=out.index)
    fatores.loc[escala.isin(["MIL", "MILHAR", "MILHARES"])] = 1_000.0
    fatores.loc[escala.isin(["MILHAO", "MILHÃO", "MILHOES", "MILHÕES"])] = 1_000_000.0
    fatores.loc[escala.isin(["BILHAO", "BILHÃO", "BILHOES", "BILHÕES"])] = 1_000_000_000.0

    # Exceção: contas por ação (3.99.*) NÃO devem ser escaladas
    if "CD_CONTA" in out.columns:
        cd = out["CD_CONTA"].astype(str)
        mask_por_acao = cd.str.startswith("3.99", na=False)
        fatores.loc[mask_por_acao] = 1.0

    out["VL_CONTA"] = out["VL_CONTA"] * fatores
    return out


# =========================
# NORMALIZAÇÃO LPA (para evitar overflow e distorções)
# =========================
def _normalizar_lpa_series(s: pd.Series) -> pd.Series:
    """
    Traz LPA para uma faixa plausível e garante que não estoure o numeric(20,6).

    Estratégia:
    - converte para float
    - divide por 1000 sucessivamente enquanto |x| for muito grande
    - se ainda for absurdo (>= 1e14), zera (mais seguro que abortar o pipeline)
    """
    s2 = pd.to_numeric(s, errors="coerce").astype("float64")

    # primeiro: reduzir casos obviamente fora de escala
    # se passar de 1e6 por ação, é quase certamente escala errada
    for _ in range(8):
        mask = s2.abs() > 1e6
        if not mask.any():
            break
        s2.loc[mask] = s2.loc[mask] / 1000.0

    # segundo: proteger o banco (numeric(20,6) exige < 1e14)
    s2.loc[s2.abs() >= LPA_ABS_MAX_DB] = np.nan

    return s2.fillna(0).round(6)


# =========================
# COLETA DFP (PARALELISMO)
# =========================
def processar_ano_dfp(ano: int):
    url = URL_BASE_DFP + f"dfp_cia_aberta_{ano}.zip"
    response = requests.get(url, timeout=180)

    if response.status_code != 200:
        print(f"[WARN] Erro ao baixar o arquivo para o ano {ano} (status={response.status_code})")
        return None

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
        df_temp_dict = {"DRE": [], "BPA": [], "BPP": [], "DFC_MI": []}

        for arquivo in zip_ref.namelist():
            if arquivo.endswith(".csv") and "_con_" in arquivo:
                with zip_ref.open(arquivo) as csvfile:
                    try:
                        df_temp = pd.read_csv(csvfile, sep=";", decimal=",", encoding="ISO-8859-1")

                        # seu filtro original
                        if "ORDEM_EXERC" in df_temp.columns:
                            df_temp = df_temp[df_temp["ORDEM_EXERC"] == "ÚLTIMO"]

                        # escala CVM com exceção para 3.99*
                        df_temp = _normalizar_vl_conta_por_escala(df_temp)

                        up = arquivo.upper()
                        if "DRE" in up:
                            df_temp_dict["DRE"].append(df_temp)
                        elif "BPA" in up:
                            df_temp_dict["BPA"].append(df_temp)
                        elif "BPP" in up:
                            df_temp_dict["BPP"].append(df_temp)
                        elif "DFC" in up:
                            df_temp_dict["DFC_MI"].append(df_temp)

                    except Exception as e:
                        print(f"[WARN] Erro ao processar {arquivo} no ano {ano}: {e}")

    return df_temp_dict


def coletar_dfp():
    df_dict_dfp = {"DRE": pd.DataFrame(), "BPA": pd.DataFrame(), "BPP": pd.DataFrame(), "DFC_MI": pd.DataFrame()}

    # mantém seu padrão original: ULTIMO_ANO não incluso
    anos = list(range(ANO_INICIAL, ULTIMO_ANO))
    if not anos:
        print("[WARN] Intervalo de anos vazio. Verifique ANO_INICIAL/ULTIMO_ANO.")
        return df_dict_dfp

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        df_results_dfp = list(executor.map(processar_ano_dfp, anos))

    for df_temp_dict in df_results_dfp:
        if df_temp_dict is None:
            continue
        for key in df_dict_dfp.keys():
            if df_temp_dict.get(key):
                df_dict_dfp[key] = pd.concat([df_dict_dfp[key]] + df_temp_dict[key], ignore_index=True)

    print(f"[OK] Coleta de dados anuais (DFP) concluída! (anos {ANO_INICIAL}..{ULTIMO_ANO-1})")
    return df_dict_dfp


# =========================
# CONSOLIDAÇÃO (fiel ao seu script)
# =========================
def montar_df_consolidado(df_dict_dfp: dict) -> pd.DataFrame:
    if df_dict_dfp["DRE"] is None or df_dict_dfp["DRE"].empty:
        return pd.DataFrame()

    empresas = (
        df_dict_dfp["DRE"][["DENOM_CIA", "CD_CVM"]]
        .drop_duplicates()
        .set_index("CD_CVM")
    )

    def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        df = df.copy()
        if "DT_REFER" in df.columns:
            df["DT_REFER"] = pd.to_datetime(df["DT_REFER"], errors="coerce")
        return df

    def _serie_conta(df_conta: pd.DataFrame, idx: pd.DatetimeIndex) -> pd.Series:
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
        if conta_receita is not None and not conta_receita.empty:
            idx = pd.to_datetime(conta_receita["DT_REFER"].unique(), errors="coerce")
        else:
            bpa_ativo_total = empresa_bpa[empresa_bpa["CD_CONTA"] == "1"] if empresa_bpa is not None else pd.DataFrame()
            if bpa_ativo_total is not None and not bpa_ativo_total.empty:
                idx = pd.to_datetime(bpa_ativo_total["DT_REFER"].unique(), errors="coerce")
            else:
                idx = None
                for _df in [empresa_dre, empresa_bpa, empresa_bpp, empresa_dfc]:
                    if _df is not None and not _df.empty and "DT_REFER" in _df.columns:
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

        conta_caixa_e_equivalentes = empresa_bpa[empresa_bpa["DS_CONTA"] == "Caixa e Equivalentes de Caixa"]

        # ========= BPP =========
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

        conta_patrimonio_liquido = empresa_bpp[empresa_bpp["DS_CONTA"].isin(["Patrimônio Líquido Consolidado"])]
        conta_passivo_circulante_financeiro = empresa_bpp[empresa_bpp["CD_CONTA"].astype(str).str.startswith("2.01.04", na=False)]
        conta_passivo_nao_circulante_financeiro = empresa_bpp[empresa_bpp["CD_CONTA"].astype(str).str.startswith("2.02.01", na=False)]
        conta_passivo_total = empresa_bpp[empresa_bpp["CD_CONTA"] == "2"]

        # ========= DFC =========
        conta_dividendos = empresa_dfc[empresa_dfc["DS_CONTA"].isin([
            "Dividendos", "Dividendos pagos", "Dividendos Pagos", "Pagamento de Dividendos",
            "Pagamento de Dividendos e JCP", "Pagamentos de Dividendos e JCP",
            "Dividendos Pagos a Acionistas", "Dividendos/JCP Pagos a Acionistas",
            "JCP e dividendos pagos e acionistas", "Dividendos e Juros s/Capital Próprio",
            "Dividendos e Juros sobre o Capital Próprio Pagos",
        ])]
        conta_dividendos_nctrl = empresa_dfc[
            empresa_dfc["DS_CONTA"].isin(["Dividendos ou juros sobre o capital próprio pagos aos acionistas não controladores"])
        ]
        conta_fco = empresa_dfc[empresa_dfc["CD_CONTA"] == "6.01"]

        # ========= Montagem =========
        df_empresa["CD_CVM"] = CD_CVM
        df_empresa["Data"] = df_empresa.index

        df_empresa["Receita Líquida"] = _serie_conta(conta_receita, idx)
        df_empresa["Ebit"] = _serie_conta(conta_ebit, idx)
        df_empresa["Lucro Líquido"] = _serie_conta(conta_lucro_liquido, idx)

        # LPA: série + normalização robusta
        df_empresa["Lucro por Ação"] = _serie_conta(conta_lpa, idx)
        df_empresa["Lucro por Ação"] = _normalizar_lpa_series(df_empresa["Lucro por Ação"])

        df_empresa["Ativo Total"] = _serie_conta(conta_ativo_total, idx)
        df_empresa["Ativo Circulante"] = _serie_conta(conta_ativo_circulante, idx)

        df_empresa["Caixa e Equivalentes"] = _serie_conta(conta_caixa_e_equivalentes, idx)

        df_empresa["Passivo Circulante"] = _serie_conta(conta_passivo_circulante, idx)
        df_empresa["Patrimônio Líquido"] = _serie_conta(conta_patrimonio_liquido, idx)

        df_empresa["Passivo Total"] = _serie_conta(conta_passivo_total, idx)

        df_empresa["Passivo Circulante Financeiro"] = _serie_conta(conta_passivo_circulante_financeiro, idx)
        df_empresa["Passivo Não Circulante Financeiro"] = _serie_conta(conta_passivo_nao_circulante_financeiro, idx)

        df_empresa["Dividendos"] = _serie_conta(conta_dividendos, idx)
        df_empresa["Dividendos Ncontroladores"] = _serie_conta(conta_dividendos_nctrl, idx)

        df_empresa["Caixa Líquido"] = _serie_conta(conta_fco, idx)

        # Converte numéricos (exceto LPA já tratado)
        cols_to_convert = [
            "Receita Líquida", "Ebit", "Lucro Líquido",
            "Ativo Total", "Ativo Circulante", "Passivo Circulante", "Passivo Total",
            "Passivo Circulante Financeiro", "Passivo Não Circulante Financeiro",
            "Caixa e Equivalentes", "Dividendos", "Dividendos Ncontroladores",
            "Patrimônio Líquido", "Caixa Líquido"
        ]
        for col in cols_to_convert:
            df_empresa[col] = pd.to_numeric(df_empresa[col], errors="coerce").fillna(0)

        # Derivadas (preservando comportamento)
        df_empresa["Passivo Total"] = df_empresa["Passivo Total"] - df_empresa["Patrimônio Líquido"]
        df_empresa["Divida Total"] = df_empresa["Passivo Circulante Financeiro"] + df_empresa["Passivo Não Circulante Financeiro"]
        df_empresa["Dívida Líquida"] = df_empresa["Divida Total"] - df_empresa["Caixa e Equivalentes"]
        df_empresa["Dividendos Totais"] = (df_empresa["Dividendos"] + df_empresa["Dividendos Ncontroladores"]).abs()

        colunas_desejadas = [
            "CD_CVM", "Data", "Receita Líquida", "Ebit", "Lucro Líquido", "Lucro por Ação",
            "Ativo Total", "Ativo Circulante", "Passivo Circulante", "Passivo Total",
            "Divida Total", "Patrimônio Líquido", "Dividendos Totais", "Caixa Líquido", "Dívida Líquida",
        ]
        df_selecionado = df_empresa[colunas_desejadas].reset_index(drop=True)
        df_consolidado = pd.concat([df_consolidado, df_selecionado], ignore_index=True)

    return df_consolidado.fillna(0)


# =========================
# TICKER + REORDENAÇÃO (mantém padrão)
# =========================
def adicionar_ticker(df_consolidado: pd.DataFrame) -> pd.DataFrame:
    if not TICKER_PATH.exists():
        raise FileNotFoundError(f"Não encontrei o arquivo CVM->Ticker em: {TICKER_PATH}")

    cvm_to_ticker = pd.read_csv(TICKER_PATH, sep=",", encoding="utf-8")

    df = pd.merge(df_consolidado, cvm_to_ticker, left_on="CD_CVM", right_on="CVM")
    df = df.drop(columns=["CD_CVM", "CVM"])

    df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.strftime("%Y-%m-%d")

    colunas = [
        "Ticker", "Data", "Receita Líquida", "Ebit", "Lucro Líquido", "Lucro por Ação",
        "Ativo Total", "Ativo Circulante", "Passivo Circulante", "Passivo Total", "Divida Total",
        "Patrimônio Líquido", "Dividendos Totais", "Caixa Líquido", "Dívida Líquida"
    ]
    return df[colunas]


# =========================
# FILTRO (mantém padrão)
# =========================
def filtrar_empresas(df_consolidado: pd.DataFrame) -> pd.DataFrame:
    colunas_essenciais = ["Receita Líquida"]
    tickers_aprovados = []

    for ticker in df_consolidado["Ticker"].unique():
        df_empresa = df_consolidado[df_consolidado["Ticker"] == ticker]

        anos_disponiveis = sorted(pd.to_datetime(df_empresa["Data"], errors="coerce").dt.year.dropna().unique())
        if not anos_disponiveis:
            continue

        primeiro_ano = anos_disponiveis[0]
        ultimo_ano = anos_disponiveis[-1]
        anos_esperados = list(range(primeiro_ano, ultimo_ano + 1))

        dados_continuos = anos_disponiveis == anos_esperados
        termina_no_ultimo_ano = ultimo_ano >= ULTIMO_ANO_DISPONIVEL

        colunas_com_faltas = df_empresa[colunas_essenciais].isna().sum().sum()

        if dados_continuos and termina_no_ultimo_ano and (colunas_com_faltas / df_empresa.shape[0] <= 0.1):
            tickers_aprovados.append(ticker)

    return df_consolidado[df_consolidado["Ticker"].isin(tickers_aprovados)].copy()


# =========================
# GRAVAÇÃO NO SUPABASE (alinhado ao schema)
# =========================
def upsert_supabase_demonstracoes_financeiras(df_filtrado: pd.DataFrame) -> None:
    if df_filtrado is None or df_filtrado.empty:
        print("[WARN] Nenhuma linha DFP para gravar (após filtros).")
        return

    if not SUPABASE_DB_URL:
        raise RuntimeError("Defina SUPABASE_DB_URL (connection string Postgres do Supabase).")

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
    })

    df_db["Data"] = pd.to_datetime(df_db["Data"], errors="coerce").dt.date

    money_cols = [
        "Receita_Liquida", "EBIT", "Lucro_Liquido", "Ativo_Total", "Ativo_Circulante",
        "Passivo_Circulante", "Passivo_Total", "Divida_Total", "Patrimonio_Liquido",
        "Dividendos", "Caixa_Liquido", "Divida_Liquida"
    ]
    for c in money_cols:
        df_db[c] = pd.to_numeric(df_db[c], errors="coerce").round(2)

    # LPA protegido contra overflow (numeric(20,6))
    df_db["LPA"] = _normalizar_lpa_series(df_db["LPA"])

    df_db = df_db.fillna(0)

    df_db = (
        df_db.sort_values(["Ticker", "Data"])
             .drop_duplicates(subset=["Ticker", "Data"], keep="last")
             .reset_index(drop=True)
    )

    # Diagnóstico rápido (se ainda houver outliers)
    max_lpa = float(pd.to_numeric(df_db["LPA"], errors="coerce").abs().max())
    if max_lpa >= LPA_ABS_MAX_DB:
        raise ValueError(f"LPA ainda fora do limite do banco (max abs={max_lpa}). Abortando para proteção.")

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
    df_dict_dfp = coletar_dfp()
    df_consolidado = montar_df_consolidado(df_dict_dfp)
    df_consolidado = adicionar_ticker(df_consolidado)
    df_filtrado = filtrar_empresas(df_consolidado)
    upsert_supabase_demonstracoes_financeiras(df_filtrado)


if __name__ == "__main__":
    main()
