# auditoria_dados/source_recon.py
#
# Reconciliação FONTE → BANCO.
#
# Compara o que as fontes externas dizem existir com o que realmente
# foi ingerido no Supabase.  Não baixa ZIPs completos — usa apenas
# arquivos leves de cadastro/índice.
#
# Uso:
#   python -m auditoria_dados.source_recon [--all] [--check NOME]
#
# Checks disponíveis:
#   cvm_cadastro      -- CVM cadastro de cias abertas vs cvm_to_ticker
#   cvm_dfp_coverage  -- companias que entregaram DFP ao CVM vs banco
#   cvm_itr_coverage  -- companias que entregaram ITR ao CVM vs banco
#   bcb_series        -- séries BCB/SGS esperadas vs info_economica
#
# Requer: SUPABASE_DB_URL ou DATABASE_URL no ambiente.
# Depende: requests, pandas, sqlalchemy
#
from __future__ import annotations

import argparse
import io
import os
import sys
import time
from datetime import date, datetime
from functools import lru_cache
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine, text


# ─────────────────────────────────────────────────────────────────────────────
# Configuração
# ─────────────────────────────────────────────────────────────────────────────

# Cadastro de companhias abertas (CSV leve, ~500 KB)
CVM_CADASTRO_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"

# Index de arquivos DFP por ano (lista de CSVs dentro do ZIP; baixamos apenas
# o arquivo de demonstrações consolidadas que é o menor: BPA_con)
CVM_DFP_BASE = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
CVM_ITR_BASE = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"

# Séries BCB/SGS que esperamos ter no banco
BCB_EXPECTED_SERIES: Dict[str, str] = {
    "selic":          "Selic anualizada (série 432)",
    "ipca":           "IPCA acumulado anual (série 13522)",
    "cambio":         "Câmbio BRL/USD final de período (série 3698)",
    "pib":            "PIB nominal (série 4380)",
    "divida_publica": "Dívida bruta do governo (série 13762)",
}

# Ano inicial da ingestão (configurável via env)
ANO_INICIAL = int(os.getenv("ANO_INICIAL", "2018"))
ANO_CORRENTE = date.today().year

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))


# ─────────────────────────────────────────────────────────────────────────────
# Infraestrutura
# ─────────────────────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _get_engine():
    url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("Defina SUPABASE_DB_URL ou DATABASE_URL no ambiente.")
    return create_engine(url, pool_pre_ping=True)


def _query(sql: str, params: dict | None = None) -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})


@lru_cache(maxsize=1)
def _build_session() -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers["User-Agent"] = "auditoria-dados-source-recon/1.0"
    session.mount("https://", adapter)
    return session


def _fetch_csv(url: str, encoding: str = "latin-1", sep: str = ";") -> Optional[pd.DataFrame]:
    """Baixa um CSV remoto e retorna como DataFrame.  Retorna None em caso de erro."""
    try:
        resp = _build_session().get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return pd.read_csv(io.BytesIO(resp.content), sep=sep, encoding=encoding, dtype=str)
    except Exception as e:
        print(f"  [ERRO] Falha ao baixar {url}: {e}")
        return None


def _header(title: str) -> None:
    print(f"\n{'─' * 70}")
    print(f"  {title}")
    print(f"{'─' * 70}")


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 1: CVM cadastro vs cvm_to_ticker
# ─────────────────────────────────────────────────────────────────────────────

def check_cvm_cadastro() -> None:
    """
    Baixa o cadastro oficial de companhias abertas da CVM e compara com
    a tabela cvm_to_ticker no banco.

    Detecta:
    - Empresas ativas na CVM sem mapeamento para ticker no banco
    - Mapeamentos no banco para empresas canceladas/inaptas na CVM
    """
    _header("RECONCILIAÇÃO FONTE→BANCO: CVM Cadastro vs cvm_to_ticker")

    print("  Baixando cadastro de cias abertas da CVM...")
    df_cvm = _fetch_csv(CVM_CADASTRO_URL)
    if df_cvm is None:
        print("  [FALHA] Não foi possível obter o cadastro CVM. Verifique conectividade.")
        return

    # Normalizar coluna CNPJ_CIA → código CVM (CD_CVM) é a chave de join
    # O cadastro tem colunas: CD_CVM, DENOM_SOCIAL, CNPJ_CIA, SIT_REG, DT_REG, DT_CANCEL, TP_REG
    needed_cols = {"CD_CVM", "DENOM_SOCIAL", "SIT_REG", "TP_REG"}
    missing = needed_cols - set(df_cvm.columns)
    if missing:
        print(f"  [ERRO] Colunas ausentes no cadastro CVM: {missing}")
        print(f"  Colunas disponíveis: {list(df_cvm.columns)}")
        return

    df_cvm["CD_CVM"] = df_cvm["CD_CVM"].str.strip()
    df_cvm["SIT_REG"] = df_cvm["SIT_REG"].str.strip().str.upper()
    df_cvm["TP_REG"] = df_cvm["TP_REG"].str.strip().str.upper()

    # Apenas empresas registradas e ativas
    ativas = df_cvm[
        (df_cvm["SIT_REG"].str.contains("ATIVO", na=False)) &
        (df_cvm["TP_REG"].str.contains("CIA ABERTA", na=False))
    ].copy()

    print(f"  Total de registros no cadastro CVM:          {len(df_cvm)}")
    print(f"  Cias abertas ativas (SIT_REG=ATIVO):         {len(ativas)}")

    # Buscar tabela cvm_to_ticker no banco
    try:
        df_db = _query("SELECT cvm, ticker FROM public.cvm_to_ticker")
    except Exception as e:
        print(f"  [ERRO] Falha ao carregar cvm_to_ticker: {e}")
        return

    db_cvm_codes: Set[str] = set(df_db["cvm"].astype(str).str.strip())
    cvm_ativas_codes: Set[str] = set(ativas["CD_CVM"].dropna())

    # Empresas ativas na CVM sem mapeamento no banco
    sem_mapeamento = cvm_ativas_codes - db_cvm_codes
    # Mapeamentos no banco para códigos não encontrados nas ativas
    somente_db = db_cvm_codes - cvm_ativas_codes

    print(f"\n  Códigos CVM mapeados no banco:               {len(db_cvm_codes)}")
    print(f"  Cias ativas na CVM:                          {len(cvm_ativas_codes)}")

    pct_cobertura = round(100 * len(db_cvm_codes & cvm_ativas_codes) / max(len(cvm_ativas_codes), 1), 1)
    print(f"  Cobertura (mapeados / ativas):               {pct_cobertura}%")

    if sem_mapeamento:
        # Enriquecer com nome da empresa
        sem_map_df = ativas[ativas["CD_CVM"].isin(sem_mapeamento)][["CD_CVM", "DENOM_SOCIAL"]].head(30)
        print(f"\n  [ALERTA] {len(sem_mapeamento)} cias ativas na CVM SEM mapeamento no banco:")
        print(f"  (exibindo até 30)")
        for _, row in sem_map_df.iterrows():
            print(f"    CD_CVM={row['CD_CVM']:<8}  {str(row['DENOM_SOCIAL'])[:60]}")
        print(f"  → Execute cvm_to_ticker_sync.py para atualizar mapeamentos.")
    else:
        print("\n  [OK] Todas as cias ativas na CVM têm mapeamento no banco.")

    if somente_db:
        somente_db_info = df_db[df_db["cvm"].astype(str).str.strip().isin(somente_db)].head(20)
        print(f"\n  [INFO] {len(somente_db)} códigos no banco não encontrados entre cias ativas na CVM:")
        print(f"  (pode ser cias canceladas, fundos ou nomenclatura diferente)")
        for _, row in somente_db_info.iterrows():
            print(f"    CD_CVM={str(row['cvm']):<8}  ticker={row['ticker']}")


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 2 & 3: CVM DFP/ITR index por ano vs banco
# ─────────────────────────────────────────────────────────────────────────────

def _check_cvm_financial_coverage(doc_type: str) -> None:
    """
    Para cada ano disponível na CVM, baixa o arquivo de índice leve
    (apenas as colunas CD_CVM e DT_REFER do arquivo BPA_con) e compara
    com a tabela Demonstracoes_Financeiras ou Demonstracoes_Financeiras_TRI.

    doc_type: 'dfp' ou 'itr'
    """
    label = doc_type.upper()
    base_url = CVM_DFP_BASE if doc_type == "dfp" else CVM_ITR_BASE
    db_table = "Demonstracoes_Financeiras" if doc_type == "dfp" else "Demonstracoes_Financeiras_TRI"

    _header(f"RECONCILIAÇÃO FONTE→BANCO: CVM {label} por ano vs {db_table}")

    # Anos a verificar: ANO_INICIAL até o ano anterior (o corrente pode estar incompleto)
    anos = list(range(ANO_INICIAL, ANO_CORRENTE))  # CVM só tem anos fechados

    # Carregar tickers no banco para a tabela de destino
    try:
        df_db_tickers = _query(
            f'SELECT "Ticker", EXTRACT(YEAR FROM "Data"::date) as ano '
            f'FROM public."{db_table}"'
        )
        db_pairs: Set[Tuple[str, int]] = set(
            zip(df_db_tickers["Ticker"].str.upper(), df_db_tickers["ano"].astype(int))
        )
        db_tickers_all: Set[str] = set(df_db_tickers["Ticker"].str.upper())
    except Exception as e:
        print(f"  [ERRO] Falha ao carregar {db_table}: {e}")
        return

    # Carregar mapeamento CVM → ticker do banco
    try:
        df_map = _query("SELECT cvm, ticker FROM public.cvm_to_ticker")
        cvm_to_ticker: Dict[str, str] = {
            str(row["cvm"]).strip(): str(row["ticker"]).strip().upper()
            for _, row in df_map.iterrows()
            if row["ticker"]
        }
    except Exception as e:
        print(f"  [ERRO] Falha ao carregar cvm_to_ticker: {e}")
        return

    print(f"  Anos a verificar: {anos[0]}–{anos[-1]}")
    print(f"  Pares (Ticker, Ano) no banco:   {len(db_pairs)}")
    print()

    total_fonte = 0
    total_mapeavel = 0
    total_no_banco = 0
    anos_criticos: List[Tuple[int, int, int, int, float]] = []  # (ano, fonte, mapeavel, banco, %)

    for ano in anos:
        filename = f"{doc_type}_cia_aberta_BPA_con_{ano}.csv"
        url = f"{base_url}{filename}"

        df_fonte = _fetch_csv(url)
        if df_fonte is None:
            print(f"  {ano}: [SKIP] arquivo não encontrado em {url}")
            continue

        if "CD_CVM" not in df_fonte.columns:
            print(f"  {ano}: [SKIP] coluna CD_CVM ausente (colunas: {list(df_fonte.columns)[:6]})")
            continue

        empresas_fonte: Set[str] = set(df_fonte["CD_CVM"].dropna().astype(str).str.strip())

        # Apenas empresas que temos mapeamento para ticker
        mapeaveis = {
            cvm_to_ticker[c] for c in empresas_fonte if c in cvm_to_ticker
        }
        no_banco = {t for t in mapeaveis if (t, ano) in db_pairs}

        pct_cobertura = round(100 * len(no_banco) / max(len(mapeaveis), 1), 1)
        flag = ""
        if pct_cobertura < 60:
            flag = "  ← CRÍTICO"
        elif pct_cobertura < 85:
            flag = "  ← ALERTA"

        print(
            f"  {ano}: {len(empresas_fonte):>5} empresas na CVM | "
            f"{len(mapeaveis):>4} mapeáveis | "
            f"{len(no_banco):>4} no banco ({pct_cobertura}%){flag}"
        )

        total_fonte += len(empresas_fonte)
        total_mapeavel += len(mapeaveis)
        total_no_banco += len(no_banco)

        if pct_cobertura < 85:
            anos_criticos.append((ano, len(empresas_fonte), len(mapeaveis), len(no_banco), pct_cobertura))

        time.sleep(0.1)  # respeitar rate limit da CVM

    if total_mapeavel > 0:
        pct_total = round(100 * total_no_banco / total_mapeavel, 1)
        print(f"\n  COBERTURA GLOBAL: {total_no_banco}/{total_mapeavel} ({pct_total}%) "
              f"dos pares mapeáveis estão no banco.")

    if anos_criticos:
        print(f"\n  Anos com cobertura < 85%:")
        for ano, fonte, mapeavel, banco, pct in anos_criticos:
            print(f"    {ano}: {banco}/{mapeavel} ({pct}%) — {mapeavel - banco} tickers faltando")
        print(f"  → Execute o pipeline {label} para esses anos.")
    else:
        print(f"\n  [OK] Cobertura ≥ 85% em todos os anos verificados.")


def check_cvm_dfp_coverage() -> None:
    _check_cvm_financial_coverage("dfp")


def check_cvm_itr_coverage() -> None:
    _check_cvm_financial_coverage("itr")


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 4: BCB séries esperadas vs info_economica
# ─────────────────────────────────────────────────────────────────────────────

def check_bcb_series() -> None:
    """
    Verifica se as séries BCB/SGS que o pipeline macro deveria ter ingerido
    estão presentes e atualizadas em info_economica.

    Não bate na API do BCB — apenas verifica cobertura temporal no banco.
    """
    _header("RECONCILIAÇÃO FONTE→BANCO: Séries BCB/SGS vs info_economica")

    try:
        df = _query("SELECT * FROM public.info_economica ORDER BY data DESC LIMIT 1")
    except Exception as e:
        print(f"  [ERRO] Falha ao carregar info_economica: {e}")
        return

    if df.empty:
        print("  [CRÍTICO] Tabela info_economica está vazia — pipeline macro nunca rodou.")
        return

    colunas_presentes = set(df.columns)
    latest_row = df.iloc[0]

    try:
        latest_date = pd.to_datetime(latest_row["data"])
        anos_atras = (pd.Timestamp.now() - latest_date).days / 365
        print(f"  Último registro em info_economica: {latest_date.date()}")
        if anos_atras > 1.5:
            print(f"  [ALERTA] Dados desatualizados há {anos_atras:.1f} anos — rode o pipeline macro.")
        else:
            print(f"  [OK] Dados recentes (há {anos_atras:.1f} anos).")
    except Exception:
        print(f"  [WARN] Não foi possível interpretar a data do último registro.")

    print(f"\n  {'Série':<20} {'Coluna no banco':<22} {'Presente?':>10} {'Último valor':>15}")
    print("  " + "─" * 72)

    ausentes: List[str] = []
    for col, descricao in BCB_EXPECTED_SERIES.items():
        presente = col in colunas_presentes
        if not presente:
            ausentes.append(col)
            ultimo_valor = "N/A"
        else:
            val = latest_row.get(col)
            ultimo_valor = f"{val}" if val is not None else "NULL"

        status = "SIM" if presente else "NÃO ← FALTA"
        print(f"  {descricao[:18]:<20} {col:<22} {status:>10} {str(ultimo_valor)[:15]:>15}")

    # Verificar cobertura temporal: anos com dados vs anos esperados
    print()
    try:
        df_anos = _query(
            "SELECT EXTRACT(YEAR FROM data::date) as ano, COUNT(*) as n "
            "FROM public.info_economica GROUP BY ano ORDER BY ano"
        )
        anos_banco: Set[int] = set(df_anos["ano"].astype(int))
        anos_esperados: Set[int] = set(range(ANO_INICIAL, ANO_CORRENTE + 1))
        anos_faltando = anos_esperados - anos_banco

        print(f"  Anos esperados ({ANO_INICIAL}–{ANO_CORRENTE}): {len(anos_esperados)}")
        print(f"  Anos no banco:                             {len(anos_banco)}")

        if anos_faltando:
            print(f"  [ALERTA] Anos ausentes em info_economica: {sorted(anos_faltando)}")
            print(f"  → Execute pickup/dados_macro_brasil.py para os anos faltantes.")
        else:
            print(f"  [OK] Todos os anos esperados estão presentes.")
    except Exception as e:
        print(f"  [WARN] Não foi possível verificar cobertura temporal: {e}")

    # Verificar info_economica_mensal
    print()
    try:
        df_mensal = _query(
            "SELECT EXTRACT(YEAR FROM data::date) as ano, "
            "COUNT(*) as n_meses "
            "FROM public.info_economica_mensal GROUP BY ano ORDER BY ano DESC LIMIT 5"
        )
        if df_mensal.empty:
            print("  [ALERTA] info_economica_mensal está vazia.")
        else:
            print("  info_economica_mensal — últimos 5 anos:")
            for _, row in df_mensal.iterrows():
                n = int(row["n_meses"])
                flag = "" if n >= 10 else "  ← INCOMPLETO"
                print(f"    {int(row['ano'])}: {n} meses{flag}")
    except Exception as e:
        print(f"  [WARN] Não foi possível checar info_economica_mensal: {e}")

    if ausentes:
        print(f"\n  [CRÍTICO] {len(ausentes)} série(s) BCB ausentes no banco: {ausentes}")
        print(f"  → Execute pickup/dados_macro_brasil.py e verifique o mapeamento de colunas.")
    else:
        print(f"\n  [OK] Todas as séries BCB esperadas estão presentes no banco.")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

ALL_CHECKS = {
    "cvm_cadastro":     check_cvm_cadastro,
    "cvm_dfp_coverage": check_cvm_dfp_coverage,
    "cvm_itr_coverage": check_cvm_itr_coverage,
    "bcb_series":       check_bcb_series,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconciliação fonte → banco: CVM e BCB vs Supabase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Checks disponíveis:
  cvm_cadastro      CVM cadastro de cias abertas vs cvm_to_ticker
  cvm_dfp_coverage  Empresas que entregaram DFP ao CVM vs Demonstracoes_Financeiras
  cvm_itr_coverage  Empresas que entregaram ITR ao CVM vs Demonstracoes_Financeiras_TRI
  bcb_series        Séries BCB/SGS esperadas vs info_economica

Variáveis de ambiente:
  SUPABASE_DB_URL   URL PostgreSQL do Supabase (obrigatório)
  ANO_INICIAL       Ano de início da análise (padrão: 2018)
  REQUEST_TIMEOUT   Timeout HTTP em segundos (padrão: 30)
""",
    )
    parser.add_argument("--all", action="store_true", help="Rodar todos os checks")
    parser.add_argument(
        "--check",
        choices=list(ALL_CHECKS.keys()),
        metavar="NOME",
        help="Rodar apenas o check especificado",
    )
    args = parser.parse_args()

    if not args.all and not args.check:
        parser.print_help()
        sys.exit(1)

    checks_to_run = list(ALL_CHECKS.keys()) if args.all else [args.check]

    print(f"\nReconciliação Fonte → Banco — {date.today()}")
    print(f"Checks: {', '.join(checks_to_run)}")

    erros = 0
    for name in checks_to_run:
        try:
            ALL_CHECKS[name]()
        except Exception as e:
            print(f"\n  [ERRO FATAL em {name}] {e}")
            erros += 1

    print(f"\n{'─' * 70}")
    if erros:
        print(f"  {erros} check(s) falharam com erro fatal.")
        sys.exit(1)
    else:
        print("  Reconciliação concluída.")


if __name__ == "__main__":
    main()
