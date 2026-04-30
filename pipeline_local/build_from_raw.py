"""
pipeline_local/build_from_raw.py
Raw DFP/ITR → financials_annual_final_local / financials_quarterly_final_local

Abordagem direta: usa os mesmos account codes do pickup/dados_cvm_dfp.py existente.
NÃO depende de cvm_account_map nem de canonical_key no enriched.

Etapas:
  1. Carrega cvm_to_ticker do Supabase → registra no DuckDB
  2. UPDATE ticker nas tabelas raw (DFP + ITR)
  3. Para cada ano, executa INSERT INTO financials usando pivot SQL com
     as regras de account codes/DS_CONTA do pipeline legado

Uso:
  python -m pipeline_local.build_from_raw
"""
from __future__ import annotations

import os
import pathlib
import sys
import uuid

_PROJ = pathlib.Path(__file__).parent.parent
_ENV = _PROJ / ".env"
if _ENV.exists():
    for _line in _ENV.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and "=" in _line and not _line.startswith("#"):
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

_DB_PATH = str(_PROJ / "data" / "local_pipeline.duckdb")
_SUPABASE_URL = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or ""

if not _SUPABASE_URL:
    print("ERRO: SUPABASE_DB_URL nao definida.")
    sys.exit(1)

import duckdb
import pandas as pd
from sqlalchemy import create_engine, text

_ANNUAL = "pipeline_local.financials_annual_final_local"
_QTR    = "pipeline_local.financials_quarterly_final_local"
_DFP    = "pipeline_local.cvm_dfp_raw_local"
_ITR    = "pipeline_local.cvm_itr_raw_local"


# ---------------------------------------------------------------------------
# Helpers DuckDB
# ---------------------------------------------------------------------------
def _open(db_path: str) -> duckdb.DuckDBPyConnection:
    local_temp = pathlib.Path("C:/DuckDBTemp")
    local_temp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)
    con.execute(f"PRAGMA temp_directory='{local_temp.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute("SET memory_limit='3GB'")
    con.execute("PRAGMA max_temp_directory_size='10GiB'")
    return con


# ---------------------------------------------------------------------------
# 1. Carrega ticker map e corrige tabelas raw
# ---------------------------------------------------------------------------
def load_ticker_map(con: duckdb.DuckDBPyConnection) -> int:
    """Carrega cvm_to_ticker do Supabase e registra no DuckDB como _ticker_map."""
    print("[build_from_raw] Carregando cvm_to_ticker do Supabase...")
    engine = create_engine(_SUPABASE_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        df = pd.read_sql(
            text('SELECT "CVM"::INTEGER AS cd_cvm, "Ticker" AS ticker FROM public.cvm_to_ticker WHERE "Ticker" IS NOT NULL'),
            conn,
        )
    print(f"  -> {len(df)} tickers (ex: {df['ticker'].head(3).tolist()})")
    con.register("_ticker_map", df)
    return len(df)


# ---------------------------------------------------------------------------
# 2. SQL de pivot anual (DFP)
# ---------------------------------------------------------------------------
# Escala: MIL → 1000, MILHAO → 1000000, outros → 1
_SCALE = """
    CASE UPPER(TRIM(COALESCE(escala_moeda,'')))
        WHEN 'MIL'    THEN 1000.0
        WHEN 'MILHAO' THEN 1000000.0
        WHEN 'MILHÃO' THEN 1000000.0
        ELSE 1.0
    END
"""

# Seleciona a melhor linha por (ticker, dt_refer, cd_conta):
# consolida > individual; versao maior; ULTIMO exercicio
_RANKED_DFP = """
WITH src AS (
    SELECT r.*,
        m.ticker                               AS _ticker,
        {scale} AS _scale,
        ROW_NUMBER() OVER (
            PARTITION BY m.ticker, r.dt_refer, r.cd_conta
            ORDER BY
                CASE WHEN UPPER(COALESCE(r.grupo_demo,'')) LIKE '%CONSO%' THEN 1 ELSE 0 END DESC,
                COALESCE(r.versao, 0) DESC,
                CASE WHEN UPPER(COALESCE(r.ordem_exerc,'')) LIKE '%LTIM%' THEN 1 ELSE 0 END DESC
        ) AS _rn
    FROM {table} r
    INNER JOIN _ticker_map m ON r.cd_cvm = m.cd_cvm
    WHERE EXTRACT(YEAR FROM r.dt_refer)::INTEGER = {ano}
      AND r.dt_refer IS NOT NULL
),
best AS (SELECT * FROM src WHERE _rn = 1)
"""

def _annual_sql(ano: int) -> str:
    ranked = _RANKED_DFP.format(scale=_SCALE, table=_DFP, ano=ano)
    cols = ", ".join([
        "ticker", "cd_cvm", "denom_cia", "dt_refer", "period_label", "source_doc",
        "receita_liquida", "ebit", "resultado_financeiro", "ir_csll",
        "lucro_antes_ir", "lucro_liquido", "lpa",
        "ativo_total", "ativo_circulante", "caixa_equivalentes", "aplicacoes_financeiras",
        "contas_receber", "estoques", "imobilizado", "intangivel", "investimentos",
        "passivo_circulante", "passivo_nao_circulante",
        "divida_cp", "divida_lp", "fornecedores",
        "passivo_total", "patrimonio_liquido", "participacao_n_controladores",
        "fco", "fci", "fcf",
        "divida_bruta", "divida_liquida", "quality_score", "row_hash",
    ])
    return f"""
{ranked}
INSERT INTO {_ANNUAL} ({cols})
SELECT
    _ticker                                  AS ticker,
    ANY_VALUE(cd_cvm)::INTEGER               AS cd_cvm,
    ANY_VALUE(denom_cia)                     AS denom_cia,
    dt_refer,
    CAST(EXTRACT(YEAR FROM dt_refer) AS VARCHAR) || 'A'  AS period_label,
    'DFP'                                    AS source_doc,

    -- DRE
    MAX(CASE WHEN cd_conta = '3.01' THEN vl_conta * _scale END)  AS receita_liquida,
    MAX(CASE WHEN cd_conta = '3.05' THEN vl_conta * _scale END)  AS ebit,
    MAX(CASE WHEN cd_conta = '3.06' THEN vl_conta * _scale END)  AS resultado_financeiro,
    MAX(CASE WHEN cd_conta = '3.08' THEN vl_conta * _scale END)  AS ir_csll,
    MAX(CASE WHEN cd_conta = '3.07' THEN vl_conta * _scale END)  AS lucro_antes_ir,
    -- Lucro Liquido: codigo 3.11 OU nome da conta
    MAX(CASE WHEN cd_conta = '3.11'
              OR (cd_conta LIKE '3.%' AND (
                  LOWER(COALESCE(ds_conta,'')) LIKE '%lucro/preju%zo consolidado do per%odo%'
                  OR LOWER(COALESCE(ds_conta,'')) LIKE '%lucro ou preju%zo l%quido consolidado%'
                  OR LOWER(COALESCE(ds_conta,'')) LIKE '%resultado l%quido do per%odo%'
              ))
         THEN vl_conta * _scale END)         AS lucro_liquido,
    MAX(CASE WHEN cd_conta IN ('3.99.01.01','3.99.01') THEN vl_conta END) AS lpa,

    -- BPA
    MAX(CASE WHEN cd_conta = '1'     THEN vl_conta * _scale END) AS ativo_total,
    MAX(CASE WHEN cd_conta = '1.01'  THEN vl_conta * _scale END) AS ativo_circulante,
    MAX(CASE WHEN cd_conta = '1.01.01'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%caixa e equivalentes%')
         THEN vl_conta * _scale END)         AS caixa_equivalentes,
    MAX(CASE WHEN cd_conta = '1.01.02'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%aplica%es financeiras%')
         THEN vl_conta * _scale END)         AS aplicacoes_financeiras,
    MAX(CASE WHEN cd_conta = '1.01.03'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%contas a receber%')
         THEN vl_conta * _scale END)         AS contas_receber,
    MAX(CASE WHEN cd_conta = '1.01.04'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%estoques%')
         THEN vl_conta * _scale END)         AS estoques,
    MAX(CASE WHEN cd_conta = '1.02.03'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%imobilizado%')
         THEN vl_conta * _scale END)         AS imobilizado,
    MAX(CASE WHEN cd_conta = '1.02.04'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%intang%vel%')
         THEN vl_conta * _scale END)         AS intangivel,
    MAX(CASE WHEN cd_conta = '1.02.02'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%investimentos%'
                  AND LENGTH(cd_conta) <= 7)
         THEN vl_conta * _scale END)         AS investimentos,

    -- BPP
    MAX(CASE WHEN cd_conta = '2.01'  THEN vl_conta * _scale END) AS passivo_circulante,
    MAX(CASE WHEN cd_conta = '2.02'  THEN vl_conta * _scale END) AS passivo_nao_circulante,
    MAX(CASE WHEN cd_conta = '2.01.04'
              OR (cd_conta LIKE '2.01.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%empr%stimos%')
         THEN vl_conta * _scale END)         AS divida_cp,
    MAX(CASE WHEN cd_conta = '2.02.01'
              OR (cd_conta LIKE '2.02.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%empr%stimos%')
         THEN vl_conta * _scale END)         AS divida_lp,
    MAX(CASE WHEN cd_conta LIKE '2.01.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%fornecedor%'
         THEN vl_conta * _scale END)         AS fornecedores,
    MAX(CASE WHEN cd_conta = '2'     THEN vl_conta * _scale END) AS passivo_total,
    MAX(CASE WHEN cd_conta = '2.03'
              OR LOWER(COALESCE(ds_conta,'')) LIKE '%patrim%nio l%quido consolidado%'
         THEN vl_conta * _scale END)         AS patrimonio_liquido,
    MAX(CASE WHEN LOWER(COALESCE(ds_conta,'')) LIKE '%participac%o de n%o controladores%'
              OR LOWER(COALESCE(ds_conta,'')) LIKE '%participa%es de n%o controladores%'
         THEN vl_conta * _scale END)         AS participacao_n_controladores,

    -- DFC
    MAX(CASE WHEN cd_conta = '6.01'  THEN vl_conta * _scale END) AS fco,
    MAX(CASE WHEN cd_conta = '6.02'  THEN vl_conta * _scale END) AS fci,
    MAX(CASE WHEN cd_conta = '6.03'  THEN vl_conta * _scale END) AS fcf,

    -- Derivados (calculados depois do GROUP BY)
    NULL AS divida_bruta,
    NULL AS divida_liquida,
    NULL AS quality_score,
    sha256(_ticker || '|' || dt_refer::TEXT || '|DFP')  AS row_hash

FROM best
GROUP BY _ticker, dt_refer
ON CONFLICT (ticker, dt_refer) DO UPDATE SET
    cd_cvm = EXCLUDED.cd_cvm,
    denom_cia = EXCLUDED.denom_cia,
    period_label = EXCLUDED.period_label,
    source_doc = EXCLUDED.source_doc,
    receita_liquida = EXCLUDED.receita_liquida,
    ebit = EXCLUDED.ebit,
    resultado_financeiro = EXCLUDED.resultado_financeiro,
    ir_csll = EXCLUDED.ir_csll,
    lucro_antes_ir = EXCLUDED.lucro_antes_ir,
    lucro_liquido = EXCLUDED.lucro_liquido,
    lpa = EXCLUDED.lpa,
    ativo_total = EXCLUDED.ativo_total,
    ativo_circulante = EXCLUDED.ativo_circulante,
    caixa_equivalentes = EXCLUDED.caixa_equivalentes,
    aplicacoes_financeiras = EXCLUDED.aplicacoes_financeiras,
    contas_receber = EXCLUDED.contas_receber,
    estoques = EXCLUDED.estoques,
    imobilizado = EXCLUDED.imobilizado,
    intangivel = EXCLUDED.intangivel,
    investimentos = EXCLUDED.investimentos,
    passivo_circulante = EXCLUDED.passivo_circulante,
    passivo_nao_circulante = EXCLUDED.passivo_nao_circulante,
    divida_cp = EXCLUDED.divida_cp,
    divida_lp = EXCLUDED.divida_lp,
    fornecedores = EXCLUDED.fornecedores,
    passivo_total = EXCLUDED.passivo_total,
    patrimonio_liquido = EXCLUDED.patrimonio_liquido,
    participacao_n_controladores = EXCLUDED.participacao_n_controladores,
    fco = EXCLUDED.fco,
    fci = EXCLUDED.fci,
    fcf = EXCLUDED.fcf,
    row_hash = EXCLUDED.row_hash,
    updated_at = now()
"""


def _qtr_sql(ano: int) -> str:
    ranked = _RANKED_DFP.format(scale=_SCALE, table=_ITR, ano=ano)
    cols = ", ".join([
        "ticker", "cd_cvm", "denom_cia", "dt_refer", "period_label",
        "period_quarter", "period_year", "source_doc",
        "receita_liquida", "ebit", "resultado_financeiro", "ir_csll",
        "lucro_antes_ir", "lucro_liquido", "lpa",
        "ativo_total", "ativo_circulante", "caixa_equivalentes", "aplicacoes_financeiras",
        "contas_receber", "estoques", "imobilizado", "intangivel", "investimentos",
        "passivo_circulante", "passivo_nao_circulante",
        "divida_cp", "divida_lp", "fornecedores",
        "passivo_total", "patrimonio_liquido", "participacao_n_controladores",
        "fco", "fci", "fcf",
        "divida_bruta", "divida_liquida", "quality_score", "row_hash",
    ])
    return f"""
{ranked}
INSERT INTO {_QTR} ({cols})
SELECT
    _ticker                                  AS ticker,
    ANY_VALUE(cd_cvm)::INTEGER               AS cd_cvm,
    ANY_VALUE(denom_cia)                     AS denom_cia,
    dt_refer,
    CAST(EXTRACT(YEAR FROM dt_refer) AS VARCHAR) || 'T'
        || CAST(EXTRACT(QUARTER FROM dt_refer) AS VARCHAR) AS period_label,
    EXTRACT(QUARTER FROM dt_refer)::INTEGER  AS period_quarter,
    EXTRACT(YEAR FROM dt_refer)::INTEGER     AS period_year,
    'ITR'                                    AS source_doc,

    MAX(CASE WHEN cd_conta = '3.01' THEN vl_conta * _scale END)  AS receita_liquida,
    MAX(CASE WHEN cd_conta = '3.05' THEN vl_conta * _scale END)  AS ebit,
    MAX(CASE WHEN cd_conta = '3.06' THEN vl_conta * _scale END)  AS resultado_financeiro,
    MAX(CASE WHEN cd_conta = '3.08' THEN vl_conta * _scale END)  AS ir_csll,
    MAX(CASE WHEN cd_conta = '3.07' THEN vl_conta * _scale END)  AS lucro_antes_ir,
    MAX(CASE WHEN cd_conta = '3.11'
              OR (cd_conta LIKE '3.%' AND (
                  LOWER(COALESCE(ds_conta,'')) LIKE '%lucro/preju%zo consolidado do per%odo%'
                  OR LOWER(COALESCE(ds_conta,'')) LIKE '%resultado l%quido do per%odo%'
              ))
         THEN vl_conta * _scale END)         AS lucro_liquido,
    MAX(CASE WHEN cd_conta IN ('3.99.01.01','3.99.01') THEN vl_conta END) AS lpa,

    MAX(CASE WHEN cd_conta = '1'     THEN vl_conta * _scale END) AS ativo_total,
    MAX(CASE WHEN cd_conta = '1.01'  THEN vl_conta * _scale END) AS ativo_circulante,
    MAX(CASE WHEN cd_conta = '1.01.01'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%caixa e equivalentes%')
         THEN vl_conta * _scale END)         AS caixa_equivalentes,
    MAX(CASE WHEN cd_conta = '1.01.02'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%aplica%es financeiras%')
         THEN vl_conta * _scale END)         AS aplicacoes_financeiras,
    MAX(CASE WHEN cd_conta = '1.01.03'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%contas a receber%')
         THEN vl_conta * _scale END)         AS contas_receber,
    MAX(CASE WHEN cd_conta = '1.01.04'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%estoques%')
         THEN vl_conta * _scale END)         AS estoques,
    MAX(CASE WHEN cd_conta = '1.02.03'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%imobilizado%')
         THEN vl_conta * _scale END)         AS imobilizado,
    MAX(CASE WHEN cd_conta = '1.02.04'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%intang%vel%')
         THEN vl_conta * _scale END)         AS intangivel,
    MAX(CASE WHEN cd_conta = '1.02.02'
              OR (cd_conta LIKE '1.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%investimentos%'
                  AND LENGTH(cd_conta) <= 7)
         THEN vl_conta * _scale END)         AS investimentos,

    MAX(CASE WHEN cd_conta = '2.01'  THEN vl_conta * _scale END) AS passivo_circulante,
    MAX(CASE WHEN cd_conta = '2.02'  THEN vl_conta * _scale END) AS passivo_nao_circulante,
    MAX(CASE WHEN cd_conta = '2.01.04'
              OR (cd_conta LIKE '2.01.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%empr%stimos%')
         THEN vl_conta * _scale END)         AS divida_cp,
    MAX(CASE WHEN cd_conta = '2.02.01'
              OR (cd_conta LIKE '2.02.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%empr%stimos%')
         THEN vl_conta * _scale END)         AS divida_lp,
    MAX(CASE WHEN cd_conta LIKE '2.01.%' AND LOWER(COALESCE(ds_conta,'')) LIKE '%fornecedor%'
         THEN vl_conta * _scale END)         AS fornecedores,
    MAX(CASE WHEN cd_conta = '2'     THEN vl_conta * _scale END) AS passivo_total,
    MAX(CASE WHEN cd_conta = '2.03'
              OR LOWER(COALESCE(ds_conta,'')) LIKE '%patrim%nio l%quido consolidado%'
         THEN vl_conta * _scale END)         AS patrimonio_liquido,
    MAX(CASE WHEN LOWER(COALESCE(ds_conta,'')) LIKE '%participac%o de n%o controladores%'
         THEN vl_conta * _scale END)         AS participacao_n_controladores,

    MAX(CASE WHEN cd_conta = '6.01'  THEN vl_conta * _scale END) AS fco,
    MAX(CASE WHEN cd_conta = '6.02'  THEN vl_conta * _scale END) AS fci,
    MAX(CASE WHEN cd_conta = '6.03'  THEN vl_conta * _scale END) AS fcf,

    NULL AS divida_bruta,
    NULL AS divida_liquida,
    NULL AS quality_score,
    sha256(_ticker || '|' || dt_refer::TEXT || '|ITR')  AS row_hash

FROM best
GROUP BY _ticker, dt_refer
ON CONFLICT (ticker, dt_refer) DO UPDATE SET
    cd_cvm = EXCLUDED.cd_cvm,
    denom_cia = EXCLUDED.denom_cia,
    period_label = EXCLUDED.period_label,
    period_quarter = EXCLUDED.period_quarter,
    period_year = EXCLUDED.period_year,
    source_doc = EXCLUDED.source_doc,
    receita_liquida = EXCLUDED.receita_liquida,
    ebit = EXCLUDED.ebit,
    resultado_financeiro = EXCLUDED.resultado_financeiro,
    ir_csll = EXCLUDED.ir_csll,
    lucro_antes_ir = EXCLUDED.lucro_antes_ir,
    lucro_liquido = EXCLUDED.lucro_liquido,
    lpa = EXCLUDED.lpa,
    ativo_total = EXCLUDED.ativo_total,
    ativo_circulante = EXCLUDED.ativo_circulante,
    caixa_equivalentes = EXCLUDED.caixa_equivalentes,
    aplicacoes_financeiras = EXCLUDED.aplicacoes_financeiras,
    contas_receber = EXCLUDED.contas_receber,
    estoques = EXCLUDED.estoques,
    imobilizado = EXCLUDED.imobilizado,
    intangivel = EXCLUDED.intangivel,
    investimentos = EXCLUDED.investimentos,
    passivo_circulante = EXCLUDED.passivo_circulante,
    passivo_nao_circulante = EXCLUDED.passivo_nao_circulante,
    divida_cp = EXCLUDED.divida_cp,
    divida_lp = EXCLUDED.divida_lp,
    fornecedores = EXCLUDED.fornecedores,
    passivo_total = EXCLUDED.passivo_total,
    patrimonio_liquido = EXCLUDED.patrimonio_liquido,
    participacao_n_controladores = EXCLUDED.participacao_n_controladores,
    fco = EXCLUDED.fco,
    fci = EXCLUDED.fci,
    fcf = EXCLUDED.fcf,
    row_hash = EXCLUDED.row_hash,
    updated_at = now()
"""


# ---------------------------------------------------------------------------
# 3. Atualiza derivados (divida_bruta, divida_liquida, quality_score)
# ---------------------------------------------------------------------------
def _update_derivados(con: duckdb.DuckDBPyConnection, table: str) -> None:
    con.execute(f"""
        UPDATE {table} SET
            divida_bruta = CASE
                WHEN COALESCE(divida_cp, 0) + COALESCE(divida_lp, 0) = 0 THEN NULL
                ELSE COALESCE(divida_cp, 0) + COALESCE(divida_lp, 0)
            END,
            divida_liquida = CASE
                WHEN COALESCE(divida_cp, 0) + COALESCE(divida_lp, 0) = 0 THEN NULL
                ELSE COALESCE(divida_cp, 0) + COALESCE(divida_lp, 0)
                     - COALESCE(caixa_equivalentes, 0)
                     - COALESCE(aplicacoes_financeiras, 0)
            END,
            quality_score = ROUND(
                (CASE WHEN receita_liquida  IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN ebit             IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN lucro_liquido    IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN ativo_total      IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN ativo_circulante IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN passivo_total    IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN patrimonio_liquido IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN fco              IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / 8,
                2
            ),
            updated_at = now()
        WHERE divida_bruta IS NULL OR quality_score IS NULL
    """)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(source: str = "ALL") -> None:
    run_id = str(uuid.uuid4())
    print(f"[build_from_raw] run_id={run_id} source={source}")

    con = _open(_DB_PATH)
    try:
        # --- Carrega ticker map (JOIN inline nos SQLs de build) ---
        load_ticker_map(con)

        # --- Annual (DFP) ---
        if source in ("DFP", "ALL"):
            anos = [r[0] for r in con.execute(f"""
                SELECT DISTINCT EXTRACT(YEAR FROM dt_refer)::INTEGER AS yr
                FROM {_DFP} WHERE dt_refer IS NOT NULL ORDER BY yr
            """).fetchall()]
            print(f"\n[build_from_raw] DFP: {len(anos)} anos -> {anos}")

            for ano in anos:
                before = con.execute(
                    f"SELECT COUNT(*) FROM {_ANNUAL} WHERE EXTRACT(YEAR FROM dt_refer)::INTEGER = {ano}"
                ).fetchone()[0]
                con.execute(_annual_sql(ano))
                after = con.execute(
                    f"SELECT COUNT(*) FROM {_ANNUAL} WHERE EXTRACT(YEAR FROM dt_refer)::INTEGER = {ano}"
                ).fetchone()[0]
                print(f"  DFP {ano}: {before} -> {after} linhas (+{after-before})")

            _update_derivados(con, _ANNUAL)
            total_ann = con.execute(f"SELECT COUNT(*) FROM {_ANNUAL}").fetchone()[0]
            print(f"  Annual total: {total_ann:,} linhas")

        # --- Quarterly (ITR) ---
        if source in ("ITR", "ALL"):
            anos_itr = [r[0] for r in con.execute(f"""
                SELECT DISTINCT EXTRACT(YEAR FROM dt_refer)::INTEGER AS yr
                FROM {_ITR} WHERE dt_refer IS NOT NULL ORDER BY yr
            """).fetchall()]
            print(f"\n[build_from_raw] ITR: {len(anos_itr)} anos -> {anos_itr}")

            for ano in anos_itr:
                before = con.execute(
                    f"SELECT COUNT(*) FROM {_QTR} WHERE EXTRACT(YEAR FROM dt_refer)::INTEGER = {ano}"
                ).fetchone()[0]
                con.execute(_qtr_sql(ano))
                after = con.execute(
                    f"SELECT COUNT(*) FROM {_QTR} WHERE EXTRACT(YEAR FROM dt_refer)::INTEGER = {ano}"
                ).fetchone()[0]
                print(f"  ITR {ano}: {before} -> {after} linhas (+{after-before})")

            _update_derivados(con, _QTR)
            total_qtr = con.execute(f"SELECT COUNT(*) FROM {_QTR}").fetchone()[0]
            print(f"  Quarterly total: {total_qtr:,} linhas")

        # --- Resumo ---
        print("\n[build_from_raw] === RESUMO ===")
        for tbl in [_ANNUAL, _QTR]:
            n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            nok = con.execute(f"SELECT COUNT(*) FROM {tbl} WHERE receita_liquida IS NOT NULL").fetchone()[0]
            print(f"  {tbl.split('.')[-1]}: {n:,} linhas | {nok:,} com receita_liquida")

        print("\n[build_from_raw] Concluido! Proximo passo:")
        print("  python -m pipeline_local.publish_to_supabase")

    finally:
        con.close()


def main() -> None:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--source", default="ALL", choices=["DFP", "ITR", "ALL"])
    args = p.parse_args()
    run(source=args.source)


if __name__ == "__main__":
    main()
