-- =============================================================================
-- pipeline_local/sql/create_local_tables_duckdb.sql
-- Schema para DuckDB 1.x (banco local sem servidor).
-- Aplicado automaticamente por pipeline_local/setup_local_db.py
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS pipeline_local;

-- =============================================================================
-- CONTROLE: execucoes de pipeline
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.pipeline_runs_local (
    run_id          TEXT            PRIMARY KEY,
    pipeline_name   TEXT            NOT NULL,
    stage           TEXT,
    status          TEXT            NOT NULL,
    started_at      TIMESTAMPTZ     NOT NULL DEFAULT current_timestamp,
    finished_at     TIMESTAMPTZ,
    rows_read       BIGINT,
    rows_written    BIGINT,
    rows_skipped    BIGINT,
    metrics         JSON,
    params          JSON,
    error_message   TEXT
);

-- =============================================================================
-- CONTROLE: quality checks
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.pipeline_quality_checks_local (
    id              TEXT            PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
    run_id          TEXT,
    check_name      TEXT            NOT NULL,
    table_name      TEXT            NOT NULL,
    status          TEXT            NOT NULL,
    expected        JSON,
    actual          JSON,
    delta           JSON,
    checked_at      TIMESTAMPTZ     NOT NULL DEFAULT current_timestamp
);

-- =============================================================================
-- CONTROLE: log de publicacoes
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.pipeline_publish_log_local (
    id              TEXT            PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
    run_id          TEXT,
    target_table    TEXT            NOT NULL,
    publish_mode    TEXT            NOT NULL,
    rows_published  BIGINT          DEFAULT 0,
    rows_skipped    BIGINT          DEFAULT 0,
    rows_error      BIGINT          DEFAULT 0,
    key_columns     TEXT[],
    published_at    TIMESTAMPTZ     NOT NULL DEFAULT current_timestamp,
    metrics         JSON
);

-- =============================================================================
-- CAMADA RAW: DFP (anual)
-- Chave primaria: row_hash (hash SHA-256 dos campos identificadores)
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.cvm_dfp_raw_local (
    row_hash        TEXT            PRIMARY KEY,
    source_doc      TEXT            NOT NULL,
    tipo_demo       TEXT,
    grupo_demo      TEXT,
    arquivo_origem  TEXT,
    cd_cvm          INTEGER,
    cnpj_cia        TEXT,
    denom_cia       TEXT,
    ticker          TEXT,
    versao          INTEGER,
    ordem_exerc     TEXT,
    dt_refer        DATE,
    dt_ini_exerc    DATE,
    dt_fim_exerc    DATE,
    cd_conta        TEXT,
    ds_conta        TEXT,
    nivel_conta     INTEGER,
    conta_pai       TEXT,
    vl_conta        DOUBLE,
    escala_moeda    TEXT,
    moeda           TEXT,
    st_conta_fixa   TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT current_timestamp
);

CREATE INDEX IF NOT EXISTS idx_dfp_raw_ticker    ON pipeline_local.cvm_dfp_raw_local (ticker);
CREATE INDEX IF NOT EXISTS idx_dfp_raw_dt_refer  ON pipeline_local.cvm_dfp_raw_local (dt_refer);
CREATE INDEX IF NOT EXISTS idx_dfp_raw_cd_conta  ON pipeline_local.cvm_dfp_raw_local (cd_conta);
CREATE INDEX IF NOT EXISTS idx_dfp_raw_cd_cvm    ON pipeline_local.cvm_dfp_raw_local (cd_cvm);

-- =============================================================================
-- CAMADA RAW: ITR (trimestral)
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.cvm_itr_raw_local (
    row_hash        TEXT            PRIMARY KEY,
    source_doc      TEXT            NOT NULL,
    tipo_demo       TEXT,
    grupo_demo      TEXT,
    arquivo_origem  TEXT,
    cd_cvm          INTEGER,
    cnpj_cia        TEXT,
    denom_cia       TEXT,
    ticker          TEXT,
    versao          INTEGER,
    ordem_exerc     TEXT,
    dt_refer        DATE,
    dt_ini_exerc    DATE,
    dt_fim_exerc    DATE,
    cd_conta        TEXT,
    ds_conta        TEXT,
    nivel_conta     INTEGER,
    conta_pai       TEXT,
    vl_conta        DOUBLE,
    escala_moeda    TEXT,
    moeda           TEXT,
    st_conta_fixa   TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT current_timestamp
);

CREATE INDEX IF NOT EXISTS idx_itr_raw_ticker    ON pipeline_local.cvm_itr_raw_local (ticker);
CREATE INDEX IF NOT EXISTS idx_itr_raw_dt_refer  ON pipeline_local.cvm_itr_raw_local (dt_refer);
CREATE INDEX IF NOT EXISTS idx_itr_raw_cd_conta  ON pipeline_local.cvm_itr_raw_local (cd_conta);

-- =============================================================================
-- CAMADA ENRICHED: dados normalizados e enriquecidos
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.cvm_raw_enriched_local (
    row_hash                TEXT        PRIMARY KEY,
    source_doc              TEXT,
    tipo_demo               TEXT,
    grupo_demo              TEXT,
    arquivo_origem          TEXT,
    cd_cvm                  INTEGER,
    cnpj_cia                TEXT,
    denom_cia               TEXT,
    ticker                  TEXT,
    dt_refer                DATE,
    cd_conta                TEXT,
    ds_conta                TEXT,
    conta_pai               TEXT,
    nivel_conta             INTEGER,
    vl_conta                DOUBLE,
    period_year             INTEGER,
    period_quarter          INTEGER,
    period_month            INTEGER,
    period_label            TEXT,
    fiscal_period_type      TEXT,
    account_depth           INTEGER,
    top_account_code        TEXT,
    account_code_root       TEXT,
    is_leaf_account         BOOLEAN,
    normalized_ds_conta     TEXT,
    normalized_denom_cia    TEXT,
    is_consolidated         BOOLEAN,
    is_individual           BOOLEAN,
    is_annual               BOOLEAN,
    is_quarterly            BOOLEAN,
    unit_scale_factor       DOUBLE,
    value_normalized_brl    DOUBLE,
    canonical_key           TEXT,
    qualidade_mapeamento    TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE INDEX IF NOT EXISTS idx_enriched_ticker_dt  ON pipeline_local.cvm_raw_enriched_local (ticker, dt_refer);
CREATE INDEX IF NOT EXISTS idx_enriched_canonical  ON pipeline_local.cvm_raw_enriched_local (canonical_key);
CREATE INDEX IF NOT EXISTS idx_enriched_period     ON pipeline_local.cvm_raw_enriched_local (period_year, period_quarter);
CREATE INDEX IF NOT EXISTS idx_enriched_source     ON pipeline_local.cvm_raw_enriched_local (source_doc);

-- =============================================================================
-- CAMADA FINAL: financeiros anuais
-- Chave primaria: (ticker, dt_refer)
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.financials_annual_final_local (
    ticker                          TEXT        NOT NULL,
    cd_cvm                          INTEGER,
    denom_cia                       TEXT,
    dt_refer                        DATE        NOT NULL,
    period_label                    TEXT,
    source_doc                      TEXT        DEFAULT 'DFP',
    receita_bruta                   DOUBLE,
    deducoes_receita                DOUBLE,
    receita_liquida                 DOUBLE,
    custo                           DOUBLE,
    lucro_bruto                     DOUBLE,
    despesa_vendas                  DOUBLE,
    despesa_geral_admin             DOUBLE,
    depreciacao_amortizacao         DOUBLE,
    ebit                            DOUBLE,
    ebitda                          DOUBLE,
    resultado_financeiro            DOUBLE,
    ir_csll                         DOUBLE,
    lucro_antes_ir                  DOUBLE,
    lucro_liquido                   DOUBLE,
    lpa                             DOUBLE,
    ativo_total                     DOUBLE,
    ativo_circulante                DOUBLE,
    caixa_equivalentes              DOUBLE,
    aplicacoes_financeiras          DOUBLE,
    contas_receber                  DOUBLE,
    estoques                        DOUBLE,
    imobilizado                     DOUBLE,
    intangivel                      DOUBLE,
    investimentos                   DOUBLE,
    passivo_circulante              DOUBLE,
    fornecedores                    DOUBLE,
    divida_cp                       DOUBLE,
    passivo_nao_circulante          DOUBLE,
    divida_lp                       DOUBLE,
    provisoes                       DOUBLE,
    passivo_total                   DOUBLE,
    patrimonio_liquido              DOUBLE,
    participacao_n_controladores    DOUBLE,
    fco                             DOUBLE,
    fci                             DOUBLE,
    fcf                             DOUBLE,
    capex                           DOUBLE,
    juros_pagos                     DOUBLE,
    dividendos_jcp_contabeis        DOUBLE,
    dividendos_declarados           DOUBLE,
    divida_bruta                    DOUBLE,
    divida_liquida                  DOUBLE,
    quality_score                   DOUBLE,
    row_hash                        TEXT,
    published_at                    TIMESTAMPTZ,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (ticker, dt_refer)
);

CREATE INDEX IF NOT EXISTS idx_fin_annual_ticker ON pipeline_local.financials_annual_final_local (ticker);

-- =============================================================================
-- CAMADA FINAL: financeiros trimestrais
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.financials_quarterly_final_local (
    ticker                          TEXT        NOT NULL,
    cd_cvm                          INTEGER,
    denom_cia                       TEXT,
    dt_refer                        DATE        NOT NULL,
    period_label                    TEXT,
    period_quarter                  INTEGER,
    period_year                     INTEGER,
    source_doc                      TEXT        DEFAULT 'ITR',
    receita_bruta                   DOUBLE,
    deducoes_receita                DOUBLE,
    receita_liquida                 DOUBLE,
    custo                           DOUBLE,
    lucro_bruto                     DOUBLE,
    despesa_vendas                  DOUBLE,
    despesa_geral_admin             DOUBLE,
    depreciacao_amortizacao         DOUBLE,
    ebit                            DOUBLE,
    ebitda                          DOUBLE,
    resultado_financeiro            DOUBLE,
    ir_csll                         DOUBLE,
    lucro_antes_ir                  DOUBLE,
    lucro_liquido                   DOUBLE,
    lpa                             DOUBLE,
    ativo_total                     DOUBLE,
    ativo_circulante                DOUBLE,
    caixa_equivalentes              DOUBLE,
    aplicacoes_financeiras          DOUBLE,
    contas_receber                  DOUBLE,
    estoques                        DOUBLE,
    imobilizado                     DOUBLE,
    intangivel                      DOUBLE,
    investimentos                   DOUBLE,
    passivo_circulante              DOUBLE,
    fornecedores                    DOUBLE,
    divida_cp                       DOUBLE,
    passivo_nao_circulante          DOUBLE,
    divida_lp                       DOUBLE,
    provisoes                       DOUBLE,
    passivo_total                   DOUBLE,
    patrimonio_liquido              DOUBLE,
    participacao_n_controladores    DOUBLE,
    fco                             DOUBLE,
    fci                             DOUBLE,
    fcf                             DOUBLE,
    capex                           DOUBLE,
    juros_pagos                     DOUBLE,
    divida_bruta                    DOUBLE,
    divida_liquida                  DOUBLE,
    quality_score                   DOUBLE,
    row_hash                        TEXT,
    published_at                    TIMESTAMPTZ,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (ticker, dt_refer)
);

CREATE INDEX IF NOT EXISTS idx_fin_qtr_ticker  ON pipeline_local.financials_quarterly_final_local (ticker);
CREATE INDEX IF NOT EXISTS idx_fin_qtr_period  ON pipeline_local.financials_quarterly_final_local (period_year, period_quarter);
