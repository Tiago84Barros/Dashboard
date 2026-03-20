# pickup/dados_cvm_itr.py
import io
import os
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Iterable, List

import numpy as np
import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values


# =========================
# CONFIG
# =========================
URL_BASE_ITR = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"

ANO_INICIAL = int(os.getenv("ANO_INICIAL", "2010"))
ULTIMO_ANO = int(os.getenv("ULTIMO_ANO", "0"))  # 0 => auto

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "240"))
UPSERT_PAGE_SIZE = int(os.getenv("UPSERT_PAGE_SIZE", "5000"))

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL_PG", "").strip()

BASE_DIR = Path(__file__).resolve().parent
TICKER_PATH = Path(os.getenv("TICKER_PATH", str(BASE_DIR / "cvm_to_ticker.csv")))

# Proteção numeric(20,6) (LPA)
LPA_ABS_MAX_DB = 1e14 - 1


# =========================
# UTIL — último ano disponível
# =========================
def _ultimo_ano_disponivel(prefix: str, ano_max: Optional[int] = None, max_back: int = 12) -> int:
    if ano_max is None:
        ano_max = datetime.now().year

    for ano in range(ano_max, ano_max - max_back - 1, -1):
        url = f"{URL_BASE_ITR}{prefix}_{ano}.zip"
        try:
            r = requests.head(url, timeout=20, allow_redirects=True)
            if r.status_code == 200:
                return ano
        except requests.RequestException:
            pass

    return ano_max - max_back


if ULTIMO_ANO <= 0:
    ULTIMO_ANO = _ultimo_ano_disponivel("itr_cia_aberta", datetime.now().year, max_back=12)


def _anos_processamento() -> Iterable[int]:
    # inclui o último ano disponível/desejado
    return range(ANO_INICIAL, ULTIMO_ANO + 1)


# =========================
# NORMALIZAÇÃO ESCALA (com exceção p/ 3.99*)
# =========================
def _normalizar_vl_conta_por_escala(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "VL_CONTA" not in df.columns:
        return df

    out = df.copy()
    out["VL_CONTA"] = pd.to_numeric(out["VL_CONTA"], errors="coerce")

    if "ESCALA_MOEDA" not in out.columns:
        return out

    escala = out["ESCALA_MOEDA"].astype(str).str.strip().str.upper()
    fatores = pd.Series(1.0, index=out.index)

    fatores.loc[escala.isin(["MIL", "MILHAR", "MILHARES"])] = 1_000.0
    fatores.loc[escala.isin(["MILHAO", "MILHÃO", "MILHOES", "MILHÕES"])] = 1_000_000.0
    fatores.loc[escala.isin(["BILHAO", "BILHÃO", "BILHOES", "BILHÕES"])] = 1_000_000_000.0

    # Exceção crítica: contas por ação NÃO escalonam
    if "CD_CONTA" in out.columns:
        cd = out["CD_CONTA"].astype(str)
        mask_por_acao = cd.str.startswith("3.99", na=False)
        fatores.loc[mask_por_acao] = 1.0

    out["VL_CONTA"] = out["VL_CONTA"] * fatores
    return out


def _normalizar_lpa_series(s: pd.Series) -> pd.Series:
    s2 = pd.to_numeric(s, errors="coerce").astype("float64")

    # Reduz escala quebrada
    for _ in range(8):
        mask = s2.abs() > 1e6
        if not mask.any():
            break
        s2.loc[mask] = s2.loc[mask] / 1000.0

    # Proteção do banco
    s2.loc[s2.abs() >= LPA_ABS_MAX_DB] = np.nan
    return s2.fillna(0).round(6)


# =========================
# DOWNLOAD / LEITURA DO ZIP (ano a ano)
# =========================
def _baixar_zip_itr(ano: int) -> Optional[bytes]:
    url = f"{URL_BASE_ITR}itr_cia_aberta_{ano}.zip"
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            print(f"[WARN] ITR {ano}: download falhou (status={r.status_code})")
            return None
        return r.content
    except requests.RequestException as e:
        print(f"[WARN] ITR {ano}: erro de rede: {e}")
        return None


def _ler_csvs_consolidados(zip_bytes: bytes, contains_upper: str) -> pd.DataFrame:
    """
    Lê e concatena CSVs consolidados do ZIP cujo nome contenha `contains_upper`.
    Ex.: 'DRE', 'BPA', 'BPP', 'DFC_MI', 'DFC_MD'
    """
    out: List[pd.DataFrame] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for name in z.namelist():
            up = name.upper()
            if name.endswith(".csv") and "_CON_" in up and contains_upper in up:
                with z.open(name) as f:
                    df = pd.read_csv(f, sep=";", decimal=",", encoding="ISO-8859-1")

                    # padrão: ORDEM_EXERC == ÚLTIMO
                    if "ORDEM_EXERC" in df.columns:
                        df = df[df["ORDEM_EXERC"] == "ÚLTIMO"]

                    df = _normalizar_vl_conta_por_escala(df)
                    out.append(df)

    if not out:
        return pd.DataFrame()
    return pd.concat(out, ignore_index=True)


# =========================
# HELPERS DE CONSOLIDAÇÃO
# =========================
def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    df["DT_REFER"] = pd.to_datetime(df.get("DT_REFER"), errors="coerce")
    df["VL_CONTA"] = pd.to_numeric(df.get("VL_CONTA"), errors="coerce")
    return df


def _serie(df: pd.DataFrame, mask: pd.Series) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype="float64")
    d = df.loc[mask, ["CD_CVM", "DT_REFER", "VL_CONTA"]].copy()
    d = d.dropna(subset=["CD_CVM", "DT_REFER"])
    if d.empty:
        return pd.Series(dtype="float64")
    return d.groupby(["CD_CVM", "DT_REFER"])["VL_CONTA"].sum()


DIVIDENDOS_DS_CANDIDATOS = {
    "DIVIDENDOS",
    "DIVIDENDOS PAGOS",
    "PAGAMENTO DE DIVIDENDOS",
    "PAGAMENTO DE DIVIDENDOS E JCP",
    "PAGAMENTOS DE DIVIDENDOS E JCP",
    "DIVIDENDOS PAGOS A ACIONISTAS",
    "DIVIDENDOS/JCP PAGOS A ACIONISTAS",
    "DIVIDENDOS E JUROS S/CAPITAL PRÓPRIO",
    "DIVIDENDOS E JUROS SOBRE O CAPITAL PRÓPRIO PAGOS",
}

DIVIDENDOS_NCTRL_DS = {
    "DIVIDENDOS OU JUROS SOBRE O CAPITAL PRÓPRIO PAGOS AOS ACIONISTAS NÃO CONTROLADORES"
}

LUCRO_DS_CANDIDATOS = {
    "LUCRO/PREJUÍZO CONSOLIDADO DO PERÍODO",
    "LUCRO OU PREJUÍZO LÍQUIDO CONSOLIDADO DO PERÍODO",
    "LUCRO/PREJUÍZO DO PERÍODO",
    "LUCRO OU PREJUÍZO DO PERÍODO",
}


# =========================
# CONSOLIDAÇÃO COMPLETA (ITR) — por ano
# =========================
def _consolidar_itr_ano(df_dre: pd.DataFrame, df_bpa: pd.DataFrame, df_bpp: pd.DataFrame, df_dfc: pd.DataFrame) -> pd.DataFrame:
    df_dre = _to_dt(df_dre)
    df_bpa = _to_dt(df_bpa)
    df_bpp = _to_dt(df_bpp)
    df_dfc = _to_dt(df_dfc)

    # Normalizações de texto
    if df_dre is not None and not df_dre.empty and "DS_CONTA" in df_dre.columns:
        df_dre["DS_CONTA_UP"] = df_dre["DS_CONTA"].astype(str).str.strip().str.upper()
    if df_dfc is not None and not df_dfc.empty and "DS_CONTA" in df_dfc.columns:
        df_dfc["DS_CONTA_UP"] = df_dfc["DS_CONTA"].astype(str).str.strip().str.upper()
    if df_bpa is not None and not df_bpa.empty and "DS_CONTA" in df_bpa.columns:
        df_bpa["DS_CONTA_UP"] = df_bpa["DS_CONTA"].astype(str).str.strip().str.upper()
    if df_bpp is not None and not df_bpp.empty and "DS_CONTA" in df_bpp.columns:
        df_bpp["DS_CONTA_UP"] = df_bpp["DS_CONTA"].astype(str).str.strip().str.upper()

    # ========= DRE =========
    receita = _serie(df_dre, df_dre["CD_CONTA"].astype(str) == "3.01")
    ebit = _serie(df_dre, df_dre["CD_CONTA"].astype(str) == "3.05")

    # lucro: preferir DS_CONTA candidatos, porque CD pode variar em alguns casos
    lucro = _serie(df_dre, df_dre.get("DS_CONTA_UP", "").isin(LUCRO_DS_CANDIDATOS))

    # LPA (3.99.01.01)
    lpa = _serie(df_dre, df_dre["CD_CONTA"].astype(str) == "3.99.01.01")
    lpa = _normalizar_lpa_series(lpa)

    # ========= BPA =========
    ativo_total = _serie(df_bpa, df_bpa["CD_CONTA"].astype(str) == "1")

    # Ativo circulante: se existir linha 1.01 com DS "Ativo Circulante" use, senão fallback por itens (mantém padrão DFP)
    bpa_101 = df_bpa[df_bpa["CD_CONTA"].astype(str) == "1.01"] if df_bpa is not None else pd.DataFrame()
    if (bpa_101 is not None) and (not bpa_101.empty) and (bpa_101.get("DS_CONTA_UP", "") == "ATIVO CIRCULANTE").any():
        ativo_circ = _serie(df_bpa, df_bpa["CD_CONTA"].astype(str) == "1.01")
    else:
        # fallback: soma itens de curto prazo (como no DFP)
        itens = {
            "CAIXA E EQUIVALENTES DE CAIXA",
            "CAIXA",
            "APLICAÇÕES DE LIQUIDEZ",
            "ATIVOS FINANCEIROS AVALIADOS AO VALOR JUSTO ATRAVÉS DO RESULTADO",
            "ATIVOS FINANCEIROS AVALIADOS AO VALOR JUSTO ATRAVÉS DE OUTROS RESULTADOS ABRANGENTES",
            "APLICAÇÕES EM DEPÓSITOS INTERFINANCEIROS",
            "APLICAÇÕES NO MERCADO ABERTO",
            "DERIVATIVOS",
            "IMPOSTO DE RENDA E CONTRIBUIÇÃO SOCIAL - CORRENTES",
        }
        ativo_circ = _serie(df_bpa, df_bpa.get("DS_CONTA_UP", "").isin(itens))

    caixa_eq = _serie(df_bpa, df_bpa.get("DS_CONTA_UP", "") == "CAIXA E EQUIVALENTES DE CAIXA")

    # ========= BPP =========
    # Passivo circulante: preferir 2.01 com DS "Passivo Circulante"
    bpp_201 = df_bpp[df_bpp["CD_CONTA"].astype(str) == "2.01"] if df_bpp is not None else pd.DataFrame()
    if (bpp_201 is not None) and (not bpp_201.empty) and (bpp_201.get("DS_CONTA_UP", "") == "PASSIVO CIRCULANTE").any():
        passivo_circ = _serie(df_bpp, df_bpp["CD_CONTA"].astype(str) == "2.01")
    else:
        itens = {
            "PASSIVOS FINANCEIROS AVALIADOS AO VALOR JUSTO ATRAVÉS DO RESULTADO",
            "PASSIVOS FINANCEIROS AO CUSTO AMORTIZADO",
            "DEPÓSITOS",
            "CAPTAÇÕES NO MERCADO ABERTO",
            "RECURSOS MERCADO INTERFINANCEIRO",
            "OUTRAS CAPTAÇÕES",
            "OBRIGAÇÕES POR EMISSÃO DE TÍTULOS E VALORES MOBILIÁRIOS E OUTRAS OBRIGAÇÕES",
            "OUTROS PASSIVOS FINANCEIROS",
            "PROVISÕES",
            "PROVISÕES TRABALHISTAS, FISCAIS E CÍVEIS",
        }
        passivo_circ = _serie(df_bpp, df_bpp.get("DS_CONTA_UP", "").isin(itens))

    patrimonio = _serie(df_bpp, df_bpp.get("DS_CONTA_UP", "").isin({"PATRIMÔNIO LÍQUIDO CONSOLIDADO", "PATRIMONIO LÍQUIDO CONSOLIDADO"}))
    passivo_total_bpp2 = _serie(df_bpp, df_bpp["CD_CONTA"].astype(str) == "2")

    # Dívida: 2.01.04* e 2.02.01*
    cd_bpp = df_bpp["CD_CONTA"].astype(str) if df_bpp is not None and not df_bpp.empty else pd.Series(dtype="object")
    passivo_circ_fin = _serie(df_bpp, cd_bpp.str.startswith("2.01.04", na=False))
    passivo_nc_fin = _serie(df_bpp, cd_bpp.str.startswith("2.02.01", na=False))

    # ========= DFC =========
    # Preferência: DFC_MI e DFC_MD já virão como df_dfc (consolidado do ano)
    # Dividendos por DS_CONTA candidatos
    div = _serie(df_dfc, df_dfc.get("DS_CONTA_UP", "").isin(DIVIDENDOS_DS_CANDIDATOS))
    div_nctrl = _serie(df_dfc, df_dfc.get("DS_CONTA_UP", "").isin(DIVIDENDOS_NCTRL_DS))

    # Caixa Líquido: usar CD 6.01 (mantém o padrão do DFP)
    fco = _serie(df_dfc, df_dfc["CD_CONTA"].astype(str) == "6.01")

    # ========= União de índices =========
    idx = receita.index
    for s in [ebit, lucro, lpa, ativo_total, ativo_circ, passivo_circ, patrimonio, passivo_total_bpp2, passivo_circ_fin, passivo_nc_fin, caixa_eq, div, div_nctrl, fco]:
        idx = idx.union(s.index)

    if len(idx) == 0:
        return pd.DataFrame()

    out = pd.DataFrame(index=idx).reset_index()
    out.columns = ["CD_CVM", "DT_REFER"]

    # Atribuições
    out["Receita Líquida"] = receita.reindex(idx).values
    out["Ebit"] = ebit.reindex(idx).values
    out["Lucro Líquido"] = lucro.reindex(idx).values
    out["Lucro por Ação"] = pd.Series(lpa, index=lpa.index).reindex(idx).values

    out["Ativo Total"] = ativo_total.reindex(idx).values
    out["Ativo Circulante"] = ativo_circ.reindex(idx).values
    out["Passivo Circulante"] = passivo_circ.reindex(idx).values
    out["Patrimônio Líquido"] = patrimonio.reindex(idx).values
    out["Passivo Total (BPP2)"] = passivo_total_bpp2.reindex(idx).values

    out["Passivo Circ Financeiro"] = passivo_circ_fin.reindex(idx).values
    out["Passivo NC Financeiro"] = passivo_nc_fin.reindex(idx).values

    out["Caixa e Equivalentes"] = caixa_eq.reindex(idx).values

    out["Dividendos"] = div.reindex(idx).values
    out["Dividendos Nctrl"] = div_nctrl.reindex(idx).values
    out["Caixa Líquido"] = fco.reindex(idx).values

    # Limpeza
    out["DT_REFER"] = pd.to_datetime(out["DT_REFER"], errors="coerce")
    out = out.dropna(subset=["DT_REFER"])

    # numéricos
    num2 = [
        "Receita Líquida", "Ebit", "Lucro Líquido",
        "Ativo Total", "Ativo Circulante", "Passivo Circulante",
        "Patrimônio Líquido", "Passivo Total (BPP2)",
        "Passivo Circ Financeiro", "Passivo NC Financeiro",
        "Caixa e Equivalentes", "Dividendos", "Dividendos Nctrl",
        "Caixa Líquido"
    ]
    for c in num2:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).round(2)

    out["Lucro por Ação"] = pd.to_numeric(out["Lucro por Ação"], errors="coerce").fillna(0).round(6)

    # Derivadas (igual DFP)
    out["Passivo Total"] = (out["Passivo Total (BPP2)"] - out["Patrimônio Líquido"]).round(2)
    out["Divida Total"] = (out["Passivo Circ Financeiro"] + out["Passivo NC Financeiro"]).round(2)
    out["Divida Liquida"] = (out["Divida Total"] - out["Caixa e Equivalentes"]).round(2)
    out["Dividendos Totais"] = (out["Dividendos"] + out["Dividendos Nctrl"]).abs().round(2)

    # Seleção final coerente com o schema TRI
    out = out[[
        "CD_CVM", "DT_REFER",
        "Receita Líquida", "Ebit", "Lucro Líquido", "Lucro por Ação",
        "Ativo Total", "Ativo Circulante", "Passivo Circulante", "Passivo Total",
        "Divida Total", "Patrimônio Líquido", "Dividendos Totais", "Caixa Líquido", "Divida Liquida"
    ]].drop_duplicates(subset=["CD_CVM", "DT_REFER"], keep="last")

    return out


# =========================
# TICKER
# =========================
def _carregar_mapa_cvm_ticker() -> pd.DataFrame:
    if not TICKER_PATH.exists():
        raise FileNotFoundError(f"Não encontrei cvm_to_ticker.csv em: {TICKER_PATH}")
    mapa = pd.read_csv(TICKER_PATH, sep=",", encoding="utf-8")
    if "CVM" not in mapa.columns or "Ticker" not in mapa.columns:
        raise ValueError("cvm_to_ticker.csv precisa ter colunas: CVM, Ticker")
    mapa["CVM"] = pd.to_numeric(mapa["CVM"], errors="coerce")
    mapa = mapa.dropna(subset=["CVM"])
    mapa["CVM"] = mapa["CVM"].astype(int)
    return mapa


def _adicionar_ticker(df: pd.DataFrame, mapa: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    out["CD_CVM"] = pd.to_numeric(out["CD_CVM"], errors="coerce")
    out = out.dropna(subset=["CD_CVM"])
    out["CD_CVM"] = out["CD_CVM"].astype(int)

    out = out.merge(mapa, left_on="CD_CVM", right_on="CVM", how="inner")
    out = out.drop(columns=["CD_CVM", "CVM"])

    out["Data"] = pd.to_datetime(out["DT_REFER"], errors="coerce").dt.date
    out = out.drop(columns=["DT_REFER"])

    return out


# =========================
# UPSERT TRI (schema completo)
# =========================
def _upsert_supabase_tri(df_tick: pd.DataFrame) -> int:
    if df_tick is None or df_tick.empty:
        return 0

    if not SUPABASE_DB_URL:
        raise RuntimeError("Defina SUPABASE_DB_URL (Postgres connection string do Supabase).")

    df_db = pd.DataFrame({
        "Ticker": df_tick["Ticker"],
        "Data": df_tick["Data"],
        "Receita_Liquida": pd.to_numeric(df_tick["Receita Líquida"], errors="coerce").fillna(0).round(2),
        "EBIT": pd.to_numeric(df_tick["Ebit"], errors="coerce").fillna(0).round(2),
        "Lucro_Liquido": pd.to_numeric(df_tick["Lucro Líquido"], errors="coerce").fillna(0).round(2),
        "LPA": _normalizar_lpa_series(df_tick["Lucro por Ação"]),
        "Ativo_Total": pd.to_numeric(df_tick["Ativo Total"], errors="coerce").fillna(0).round(2),
        "Ativo_Circulante": pd.to_numeric(df_tick["Ativo Circulante"], errors="coerce").fillna(0).round(2),
        "Passivo_Circulante": pd.to_numeric(df_tick["Passivo Circulante"], errors="coerce").fillna(0).round(2),
        "Passivo_Total": pd.to_numeric(df_tick["Passivo Total"], errors="coerce").fillna(0).round(2),
        "Divida_Total": pd.to_numeric(df_tick["Divida Total"], errors="coerce").fillna(0).round(2),
        "Patrimonio_Liquido": pd.to_numeric(df_tick["Patrimônio Líquido"], errors="coerce").fillna(0).round(2),
        "Dividendos": pd.to_numeric(df_tick["Dividendos Totais"], errors="coerce").fillna(0).round(2),
        "Caixa_Liquido": pd.to_numeric(df_tick["Caixa Líquido"], errors="coerce").fillna(0).round(2),
        "Divida_Liquida": pd.to_numeric(df_tick["Divida Liquida"], errors="coerce").fillna(0).round(2),
    })

    df_db = (
        df_db.sort_values(["Ticker", "Data"])
             .drop_duplicates(subset=["Ticker", "Data"], keep="last")
             .reset_index(drop=True)
    )

    max_lpa = float(pd.to_numeric(df_db["LPA"], errors="coerce").abs().max())
    if max_lpa >= LPA_ABS_MAX_DB:
        raise ValueError(f"LPA fora do limite do banco (max abs={max_lpa}). Abortando para proteção.")

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
            execute_values(cur, sql, values, page_size=UPSERT_PAGE_SIZE)
        conn.commit()

    return len(df_db)


# =========================
# MAIN — ano a ano (anti-queda do Streamlit)
# =========================
def main():
    mapa = _carregar_mapa_cvm_ticker()

    total = 0
    anos = list(_anos_processamento())
    if not anos:
        print("[WARN] Intervalo de anos vazio. Verifique ANO_INICIAL/ULTIMO_ANO.")
        return

    print(f"[INFO] ITR: processando anos {anos[0]}..{anos[-1]} (ULTIMO_ANO incluso)")

    for ano in anos:
        zip_bytes = _baixar_zip_itr(ano)
        if not zip_bytes:
            continue

        # Lê por demonstrativo (mantém leve; não acumula anos)
        df_dre = _ler_csvs_consolidados(zip_bytes, "DRE")
        df_bpa = _ler_csvs_consolidados(zip_bytes, "BPA")
        df_bpp = _ler_csvs_consolidados(zip_bytes, "BPP")

        # DFC: tenta MI e MD (usa o que existir)
        df_dfc_mi = _ler_csvs_consolidados(zip_bytes, "DFC_MI")
        df_dfc_md = _ler_csvs_consolidados(zip_bytes, "DFC_MD")
        if not df_dfc_mi.empty:
            df_dfc = df_dfc_mi
        else:
            df_dfc = df_dfc_md

        if df_dre.empty and df_bpa.empty and df_bpp.empty and df_dfc.empty:
            print(f"[WARN] ITR {ano}: sem arquivos consolidados esperados")
            continue

        df_cons = _consolidar_itr_ano(df_dre, df_bpa, df_bpp, df_dfc)
        if df_cons.empty:
            print(f"[WARN] ITR {ano}: consolidação vazia")
            continue

        df_tick = _adicionar_ticker(df_cons, mapa)
        if df_tick.empty:
            print(f"[WARN] ITR {ano}: nenhum ticker após merge CVM->Ticker")
            continue

        n = _upsert_supabase_tri(df_tick)
        total += n
        print(f"[OK] ITR {ano}: upsert {n} linhas (acumulado={total})")

        # libera memória (importante em deploy)
        del zip_bytes, df_dre, df_bpa, df_bpp, df_dfc_mi, df_dfc_md, df_dfc, df_cons, df_tick

    print(f"[OK] ITR concluído: total upsertado={total}")


if __name__ == "__main__":
    main()
