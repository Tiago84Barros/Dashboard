-- =============================================================================
-- pipeline_local/sql/create_local_tables_v2.sql
-- Schema completo do banco local PostgreSQL.
-- Aplique no banco local (não no Supabase).
-- Uso: psql $LOCAL_DB_URL -f create_local_tables_v2.sql
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS pipeline_local;

-- =============================================================================
-- CONTROLE: execuções de pipeline
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.pipeline_runs_local (
    run_id          text            PRIMARY KEY,
    pipeline_name   text            NOT NULL,
    stage           text,                           -- extract | transform | publish | audit
    status          text            NOT NULL,        -- running | success | error | skipped
    started_at      timestamptz     NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    rows_read       bigint,
    rows_written    bigint,
    rows_skipped    bigint,
    metrics         jsonb,
    params          jsonb,
    error_message   text
);

CREATE INDEX IF NOT EXISTS idx_runs_pipeline_name
    ON pipeline_local.pipeline_runs_local (pipeline_name, started_at DESC);

-- =============================================================================
-- CONTROLE: quality checks
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.pipeline_quality_checks_local (
    id              bigserial       PRIMARY KEY,
    run_id          text,
    check_name      text            NOT NULL,
    table_name      text            NOT NULL,
    status          text            NOT NULL,        -- pass | fail | warn
    expected        jsonb,
    actual          jsonb,
    delta           jsonb,
    checked_at      timestamptz     NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_quality_check_name
    ON pipeline_local.pipeline_quality_checks_local (check_name, checked_at DESC);

-- =============================================================================
-- CONTROLE: log de publicações no Supabase
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.pipeline_publish_log_local (
    id              bigserial       PRIMARY KEY,
    run_id          text,
    target_table    text            NOT NULL,        -- ex: public.Demonstracoes_Financeiras
    publish_mode    text            NOT NULL,        -- upsert | dry_run
    rows_published  bigint          DEFAULT 0,
    rows_skipped    bigint          DEFAULT 0,
    rows_error      bigint          DEFAULT 0,
    key_columns     text[],                         -- chave natural usada no upsert
    published_at    timestamptz     NOT NULL DEFAULT now(),
    metrics         jsonb
);

CREATE INDEX IF NOT EXISTS idx_publish_log_target
    ON pipeline_local.pipeline_publish_log_local (target_table, published_at DESC);

-- =============================================================================
-- CAMADA RAW: DFP (Demonstrações Financeiras Padronizadas — anual)
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.cvm_dfp_raw_local (
    id              bigserial       PRIMARY KEY,
    source_doc      text            NOT NULL,        -- DFP
    tipo_demo       text,                           -- DRE | BPA | BPP | DFC_MI | DFC_MD | DMPL | DVA
    grupo_demo       text,
    arquivo_origem  text,
    cd_cvm          integer,
    cnpj_cia        text,
    denom_cia       text,
    ticker          text,
    versao          integer,
    ordem_exerc     text,
    dt_refer        date,
    dt_ini_exerc    date,
    dt_fim_exerc    date,
    cd_conta        text,
    ds_conta        text,
    nivel_conta     integer,
    conta_pai       text,
    vl_conta        numeric,
    escala_moeda    text,
    moeda           text,
    st_conta_fixa   text,
    row_hash        text            UNIQUE,
    payload         jsonb,
    created_at      timestamptz     NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dfp_raw_ticker       ON pipeline_local.cvm_dfp_raw_local (ticker);
CREATE INDEX IF NOT EXISTS idx_dfp_raw_dt_refer     ON pipeline_local.cvm_dfp_raw_local (dt_refer);
CREATE INDEX IF NOT EXISTS idx_dfp_raw_cd_conta     ON pipeline_local.cvm_dfp_raw_local (cd_conta);
CREATE INDEX IF NOT EXISTS idx_dfp_raw_tipo_demo    ON pipeline_local.cvm_dfp_raw_local (tipo_demo);
CREATE INDEX IF NOT EXISTS idx_dfp_raw_cd_cvm       ON pipeline_local.cvm_dfp_raw_local (cd_cvm);

-- =============================================================================
-- CAMADA RAW: ITR (Informações Trimestrais)
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.cvm_itr_raw_local (
    id              bigserial       PRIMARY KEY,
    source_doc      text            NOT NULL,        -- ITR
    tipo_demo       text,
    grupo_demo       text,
    arquivo_origem  text,
    cd_cvm          integer,
    cnpj_cia        text,
    denom_cia       text,
    ticker          text,
    versao          integer,
    ordem_exerc     text,
    dt_refer        date,
    dt_ini_exerc    date,
    dt_fim_exerc    date,
    cd_conta        text,
    ds_conta        text,
    nivel_conta     integer,
    conta_pai       text,
    vl_conta        numeric,
    escala_moeda    text,
    moeda           text,
    st_conta_fixa   text,
    row_hash        text            UNIQUE,
    payload         jsonb,
    created_at      timestamptz     NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_itr_raw_ticker       ON pipeline_local.cvm_itr_raw_local (ticker);
CREATE INDEX IF NOT EXISTS idx_itr_raw_dt_refer     ON pipeline_local.cvm_itr_raw_local (dt_refer);
CREATE INDEX IF NOT EXISTS idx_itr_raw_cd_conta     ON pipeline_local.cvm_itr_raw_local (cd_conta);
CREATE INDEX IF NOT EXISTS idx_itr_raw_tipo_demo    ON pipeline_local.cvm_itr_raw_local (tipo_demo);
CREATE INDEX IF NOT EXISTS idx_itr_raw_cd_cvm       ON pipeline_local.cvm_itr_raw_local (cd_cvm);

-- =============================================================================
-- CAMADA ENRICHED: dados normalizados e enriquecidos (DFP + ITR combinados)
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.cvm_raw_enriched_local (
    id                      bigserial   PRIMARY KEY,
    source_doc              text        NOT NULL,       -- DFP | ITR
    tipo_demo               text,
    grupo_demo               text,
    arquivo_origem          text,
    cd_cvm                  integer,
    cnpj_cia                text,
    denom_cia               text,
    ticker                  text,
    dt_refer                date,
    cd_conta                text,
    ds_conta                text,
    conta_pai               text,
    nivel_conta             integer,
    vl_conta                numeric,
    -- Dimensões temporais derivadas
    period_year             integer,
    period_quarter          integer,
    period_month            integer,
    period_label            text,
    fiscal_period_type      text,
    -- Metadados de conta
    account_depth           integer,
    top_account_code        text,
    account_code_root       text,
    is_leaf_account         boolean,
    normalized_ds_conta     text,
    normalized_denom_cia    text,
    -- Flags de consolidação
    is_consolidated         boolean,
    is_individual           boolean,
    is_annual               boolean,
    is_quarterly            boolean,
    -- Valor normalizado
    unit_scale_factor       numeric(20, 4),
    value_normalized_brl    numeric,
    -- Mapeamento de conta canônica
    canonical_key           text,
    qualidade_mapeamento    text,                       -- exact | regex | manual | derived | fallback
    -- Controle
    row_hash                text        UNIQUE,
    payload                 jsonb,
    created_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_enriched_ticker_dt   ON pipeline_local.cvm_raw_enriched_local (ticker, dt_refer);
CREATE INDEX IF NOT EXISTS idx_enriched_cd_conta    ON pipeline_local.cvm_raw_enriched_local (cd_conta);
CREATE INDEX IF NOT EXISTS idx_enriched_period      ON pipeline_local.cvm_raw_enriched_local (period_year, period_quarter);
CREATE INDEX IF NOT EXISTS idx_enriched_source      ON pipeline_local.cvm_raw_enriched_local (source_doc, tipo_demo);
CREATE INDEX IF NOT EXISTS idx_enriched_canonical   ON pipeline_local.cvm_raw_enriched_local (canonical_key);

-- =============================================================================
-- CAMADA FINAL: financeiros anuais prontos para publish
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.financials_annual_final_local (
    id                              bigserial   PRIMARY KEY,
    ticker                          text        NOT NULL,
    cd_cvm                          integer,
    denom_cia                       text,
    dt_refer                        date        NOT NULL,
    period_label                    text,
    source_doc                      text        DEFAULT 'DFP',
    -- DRE
    receita_bruta                   numeric,
    deducoes_receita                numeric,
    receita_liquida                 numeric,
    custo                           numeric,
    lucro_bruto                     numeric,
    despesa_vendas                  numeric,
    despesa_geral_admin             numeric,
    depreciacao_amortizacao         numeric,
    ebit                            numeric,
    ebitda                          numeric,
    resultado_financeiro            numeric,
    ir_csll                         numeric,
    lucro_antes_ir                  numeric,
    lucro_liquido                   numeric,
    lpa                             numeric,
    -- Balanço Patrimonial
    ativo_total                     numeric,
    ativo_circulante                numeric,
    caixa_equivalentes              numeric,
    aplicacoes_financeiras          numeric,
    contas_receber                  numeric,
    estoques                        numeric,
    imobilizado                     numeric,
    intangivel                      numeric,
    investimentos                   numeric,
    passivo_circulante              numeric,
    fornecedores                    numeric,
    divida_cp                       numeric,
    passivo_nao_circulante          numeric,
    divida_lp                       numeric,
    provisoes                       numeric,
    passivo_total                   numeric,
    patrimonio_liquido              numeric,
    participacao_n_controladores    numeric,
    -- Fluxo de Caixa
    fco                             numeric,
    fci                             numeric,
    fcf                             numeric,
    capex                           numeric,
    juros_pagos                     numeric,
    dividendos_jcp_contabeis        numeric,
    dividendos_declarados           numeric,
    -- Derivados
    divida_bruta                    numeric,
    divida_liquida                  numeric,
    -- Qualidade do mapeamento
    quality_score                   numeric,
    -- Controle
    row_hash                        text,
    published_at                    timestamptz,
    created_at                      timestamptz NOT NULL DEFAULT now(),
    updated_at                      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (ticker, dt_refer)
);

CREATE INDEX IF NOT EXISTS idx_fin_annual_ticker    ON pipeline_local.financials_annual_final_local (ticker);
CREATE INDEX IF NOT EXISTS idx_fin_annual_dt        ON pipeline_local.financials_annual_final_local (dt_refer);
CREATE INDEX IF NOT EXISTS idx_fin_annual_pub       ON pipeline_local.financials_annual_final_local (published_at);

-- =============================================================================
-- CAMADA FINAL: financeiros trimestrais prontos para publish
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_local.financials_quarterly_final_local (
    id                              bigserial   PRIMARY KEY,
    ticker                          text        NOT NULL,
    cd_cvm                          integer,
    denom_cia                       text,
    dt_refer                        date        NOT NULL,
    period_label                    text,
    period_quarter                  integer,
    period_year                     integer,
    source_doc                      text        DEFAULT 'ITR',
    -- DRE
    receita_bruta                   numeric,
    deducoes_receita                numeric,
    receita_liquida                 numeric,
    custo                           numeric,
    lucro_bruto                     numeric,
    despesa_vendas                  numeric,
    despesa_geral_admin             numeric,
    depreciacao_amortizacao         numeric,
    ebit                            numeric,
    ebitda                          numeric,
    resultado_financeiro            numeric,
    ir_csll                         numeric,
    lucro_antes_ir                  numeric,
    lucro_liquido                   numeric,
    lpa                             numeric,
    -- Balanço Patrimonial
    ativo_total                     numeric,
    ativo_circulante                numeric,
    caixa_equivalentes              numeric,
    aplicacoes_financeiras          numeric,
    contas_receber                  numeric,
    estoques                        numeric,
    imobilizado                     numeric,
    intangivel                      numeric,
    investimentos                   numeric,
    passivo_circulante              numeric,
    fornecedores                    numeric,
    divida_cp                       numeric,
    passivo_nao_circulante          numeric,
    divida_lp                       numeric,
    provisoes                       numeric,
    passivo_total                   numeric,
    patrimonio_liquido              numeric,
    participacao_n_controladores    numeric,
    -- Fluxo de Caixa
    fco                             numeric,
    fci                             numeric,
    fcf                             numeric,
    capex                           numeric,
    juros_pagos                     numeric,
    -- Derivados
    divida_bruta                    numeric,
    divida_liquida                  numeric,
    -- Qualidade do mapeamento
    quality_score                   numeric,
    -- Controle
    row_hash                        text,
    published_at                    timestamptz,
    created_at                      timestamptz NOT NULL DEFAULT now(),
    updated_at                      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (ticker, dt_refer)
);

CREATE INDEX IF NOT EXISTS idx_fin_qtr_ticker       ON pipeline_local.financials_quarterly_final_local (ticker);
CREATE INDEX IF NOT EXISTS idx_fin_qtr_dt           ON pipeline_local.financials_quarterly_final_local (dt_refer);
CREATE INDEX IF NOT EXISTS idx_fin_qtr_period       ON pipeline_local.financials_quarterly_final_local (period_year, period_quarter);
CREATE INDEX IF NOT EXISTS idx_fin_qtr_pub          ON pipeline_local.financials_quarterly_final_local (published_at);
